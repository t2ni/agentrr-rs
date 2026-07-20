//! axum-based reverse proxy for `agentrr` — record and replay.
//!
//! Path-preserving: an incoming request to `/v1/chat/completions` is forwarded
//! to `<upstream>/v1/chat/completions` (record), or served from the recorded
//! cache by match key (replay).
//!
//! Storage policy (see `DECISIONS.md` D0009):
//! - **Request blobs** are stored **redacted**; replay never re-sends them.
//! - **Response blobs** are stored **verbatim** — byte-exact replay needs them.
//! - **Headers are never stored**. They *are* forwarded to the live upstream.
//! - The `match_key` is computed on the **unredacted** request (D0004).

#![forbid(unsafe_code)]

use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use agentrr_core::{Event, EventKind, RunId, RunManifest, Step};
use agentrr_match::{match_key, MatchMode, Provider, ReplayCursor};
use agentrr_store::{RunReader, Store};
use axum::body::{Body, Bytes};
use axum::extract::State;
use axum::http::{HeaderMap, HeaderName, Method, Response, StatusCode, Uri};
use axum::response::IntoResponse;
use axum::Router;
use serde_json::Value;
use tokio::net::TcpListener;
use tokio::sync::{Mutex, Notify};
use url::Url;

// --------------------------------------------------------------------------- //
// Record
// --------------------------------------------------------------------------- //

#[derive(Clone)]
struct RecordState {
    upstream: Url,
    client: reqwest::Client,
    writer: Arc<Mutex<Option<agentrr_store::RunWriter>>>,
    provider_override: Option<Provider>,
}

/// Run the record proxy until `shutdown` completes, then finalize the run.
pub async fn serve_record(
    upstream: Url,
    provider_override: Option<Provider>,
    listener: TcpListener,
    writer: agentrr_store::RunWriter,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> Result<RunManifest, agentrr_core::AgentrrError> {
    let state = RecordState {
        upstream,
        client: build_client()?,
        writer: Arc::new(Mutex::new(Some(writer))),
        provider_override,
    };
    let app = Router::new()
        .fallback(record_handler)
        .with_state(state.clone());

    axum::serve(listener, app.into_make_service())
        .with_graceful_shutdown(shutdown)
        .await
        .map_err(|e| agentrr_core::AgentrrError::Other(format!("serve: {e}")))?;

    let mut guard = state.writer.lock().await;
    let Some(w) = guard.take() else {
        return Err(agentrr_core::AgentrrError::Other(
            "run already finalized".into(),
        ));
    };
    w.finalize()
}

async fn record_handler(
    State(state): State<RecordState>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response<Body>, (StatusCode, String)> {
    let started = Instant::now();
    let path = uri.path();

    let req_json: Option<Value> = serde_json::from_slice(&body).ok();
    let provider = state
        .provider_override
        .clone()
        .unwrap_or_else(|| Provider::from_endpoint(path));
    let key = match_key_from(&provider, path, &body, req_json.as_ref(), MatchMode::Strict);
    let req_bytes = body.clone();

    let forward_url = forward_url(&state.upstream, &uri)?;
    let (status, resp_headers, resp_bytes) =
        forward_upstream(&state.client, method, forward_url, &headers, body).await?;

    let resp_ct = content_type(&resp_headers);
    let is_stream = detect_stream(&req_bytes, &resp_ct);

    {
        let mut guard = state.writer.lock().await;
        if let Some(w) = guard.as_mut() {
            record_event(
                w,
                &req_bytes,
                &resp_bytes,
                &key,
                &provider,
                req_json.as_ref(),
                status,
                &resp_ct,
                is_stream,
                started.elapsed().as_millis() as u64,
            );
        }
    }

    Ok(rebuild_response(status, &resp_headers, resp_bytes))
}

/// Shared by record + passthrough: persist a captured request/response pair.
#[allow(clippy::too_many_arguments)]
fn record_event(
    w: &mut agentrr_store::RunWriter,
    req_bytes: &[u8],
    resp_bytes: &[u8],
    key: &str,
    provider: &Provider,
    req_json: Option<&Value>,
    status: StatusCode,
    resp_ct: &str,
    is_stream: bool,
    latency_ms: u64,
) {
    let request_blob = match w.store_blob(req_bytes, true) {
        Ok(h) => Some(h),
        Err(e) => {
            tracing::warn!(error = %e, "storing request blob");
            None
        }
    };
    let response_blob = match w.store_blob(resp_bytes, false) {
        Ok(h) => Some(h),
        Err(e) => {
            tracing::warn!(error = %e, "storing response blob");
            None
        }
    };
    let meta = serde_json::json!({
        "model": req_json.and_then(|v| v.get("model")).and_then(|m| m.as_str()),
        "status": status.as_u16(),
        "latency_ms": latency_ms,
        "content_type": resp_ct,
        "provider": provider.as_str(),
    });
    let ev = Event {
        step: Step::new(0),
        kind: EventKind::LlmCompletion,
        ts_wall_ns: 0,
        ts_mono_ns: 0,
        match_key: Some(key.to_string()),
        request_blob,
        response_blob,
        is_stream,
        meta,
    };
    if let Err(e) = w.write_event(ev) {
        tracing::error!(error = %e, "writing event");
    }
}

// --------------------------------------------------------------------------- //
// Replay
// --------------------------------------------------------------------------- //

/// What to do when a live request has no recorded match.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OnMiss {
    /// Fail loudly (HTTP 502 to the client; the server stops and the CLI exits 2).
    Strict,
    /// Forward to the upstream live and record the new event into the run.
    Passthrough,
}

#[derive(Debug, Clone)]
pub struct ReplayOutcome {
    pub requests: u64,
    pub misses: u64,
}

#[derive(Clone)]
struct ReplayState {
    reader: Arc<Mutex<RunReader>>,
    cursor: Arc<Mutex<ReplayCursor>>,
    on_miss: OnMiss,
    match_mode: MatchMode,
    provider_override: Option<Provider>,
    upstream: Option<Url>,
    client: Option<reqwest::Client>,
    store: Store,
    run_id: RunId,
    miss_notify: Arc<Notify>,
    requests: Arc<AtomicU64>,
    misses: Arc<AtomicU64>,
    write_lock: Arc<Mutex<()>>,
}

/// Configuration for [`serve_replay`].
pub struct ReplayConfig {
    pub store: Store,
    pub run_id: RunId,
    pub on_miss: OnMiss,
    pub match_mode: MatchMode,
    pub provider_override: Option<Provider>,
    /// Required when `on_miss` is [`OnMiss::Passthrough`].
    pub upstream: Option<Url>,
}

/// Run the replay proxy until `shutdown` (or a strict miss). Returns counters.
pub async fn serve_replay(
    cfg: ReplayConfig,
    listener: TcpListener,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> Result<ReplayOutcome, agentrr_core::AgentrrError> {
    let ReplayConfig {
        store,
        run_id,
        on_miss,
        match_mode,
        provider_override,
        upstream,
    } = cfg;
    let reader = store.open_run(&run_id)?;
    let miss_notify = Arc::new(Notify::new());
    let state = ReplayState {
        reader: Arc::new(Mutex::new(reader)),
        cursor: Arc::new(Mutex::new(ReplayCursor::new())),
        on_miss,
        match_mode,
        provider_override,
        client: upstream.as_ref().map(|_| build_client()).transpose()?,
        upstream,
        store,
        run_id,
        miss_notify: miss_notify.clone(),
        requests: Arc::new(AtomicU64::new(0)),
        misses: Arc::new(AtomicU64::new(0)),
        write_lock: Arc::new(Mutex::new(())),
    };
    let app = Router::new()
        .fallback(replay_handler)
        .with_state(state.clone());

    let stop = async move {
        tokio::select! {
            _ = shutdown => {}
            _ = miss_notify.notified() => {}
        }
    };

    axum::serve(listener, app.into_make_service())
        .with_graceful_shutdown(stop)
        .await
        .map_err(|e| agentrr_core::AgentrrError::Other(format!("serve: {e}")))?;

    Ok(ReplayOutcome {
        requests: state.requests.load(Ordering::Relaxed),
        misses: state.misses.load(Ordering::Relaxed),
    })
}

async fn replay_handler(
    State(state): State<ReplayState>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response<Body>, (StatusCode, String)> {
    let path = uri.path();
    let req_json: Option<Value> = serde_json::from_slice(&body).ok();
    let provider = state
        .provider_override
        .clone()
        .unwrap_or_else(|| Provider::from_endpoint(path));
    let key = match_key_from(&provider, path, &body, req_json.as_ref(), state.match_mode);
    state.requests.fetch_add(1, Ordering::Relaxed);

    let recorded = {
        let r = state.reader.lock().await;
        r.events_for_key(&key).unwrap_or_default()
    };
    let picked: Option<Event> = {
        let mut c = state.cursor.lock().await;
        c.next_index(&key, recorded.len())
            .map(|i| recorded[i].clone())
    };

    let Some(ev) = picked else {
        state.misses.fetch_add(1, Ordering::Relaxed);
        return handle_miss(&state, method, uri, headers, body, &key, &provider).await;
    };

    let status = ev
        .meta
        .get("status")
        .and_then(|v| v.as_u64())
        .and_then(|s| StatusCode::from_u16(s as u16).ok())
        .unwrap_or(StatusCode::OK);
    let mut ct = ev
        .meta
        .get("content_type")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .unwrap_or("application/json")
        .to_string();
    if ev.is_stream {
        ct = "text/event-stream".to_string();
    }
    let blob_hex = ev.response_blob.clone().unwrap_or_default();
    let bytes = {
        let r = state.reader.lock().await;
        r.read_blob(&blob_hex).map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("read response blob: {e}"),
            )
        })?
    };
    Ok(Response::builder()
        .status(status)
        .header("content-type", ct)
        .body(Body::from(bytes))
        .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response()))
}

async fn handle_miss(
    state: &ReplayState,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
    key: &str,
    provider: &Provider,
) -> Result<Response<Body>, (StatusCode, String)> {
    match state.on_miss {
        OnMiss::Strict => {
            // Fail loudly: wake the shutdown selector so the server stops and the
            // CLI can exit with code 2.
            state.miss_notify.notify_one();
            Err((
                StatusCode::BAD_GATEWAY,
                format!(
                    "agentrr strict cache miss (match_key={})",
                    &key[..key.len().min(16)]
                ),
            ))
        }
        OnMiss::Passthrough => {
            let upstream = state.upstream.clone().ok_or_else(|| {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "passthrough miss but no --upstream configured".to_string(),
                )
            })?;
            let client = state.client.clone().ok_or_else(|| {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "no http client".to_string(),
                )
            })?;
            let started = Instant::now();
            let req_bytes = body.clone();
            let forward_url = forward_url(&upstream, &uri)?;
            let (status, resp_headers, resp_bytes) =
                forward_upstream(&client, method, forward_url, &headers, body).await?;
            let resp_ct = content_type(&resp_headers);
            let is_stream = detect_stream(&req_bytes, &resp_ct);

            // Record the new event into the run being replayed (append).
            let _guard = state.write_lock.lock().await;
            if let Ok(mut w) = state.store.open_run_for_append(&state.run_id) {
                record_event(
                    &mut w,
                    &req_bytes,
                    &resp_bytes,
                    key,
                    provider,
                    serde_json::from_slice::<Value>(&req_bytes).ok().as_ref(),
                    status,
                    &resp_ct,
                    is_stream,
                    started.elapsed().as_millis() as u64,
                );
                if let Err(e) = w.finalize() {
                    tracing::warn!(error = %e, "finalizing passthrough append");
                }
            }
            Ok(rebuild_response(status, &resp_headers, resp_bytes))
        }
    }
}

// --------------------------------------------------------------------------- //
// Shared helpers
// --------------------------------------------------------------------------- //

fn build_client() -> Result<reqwest::Client, agentrr_core::AgentrrError> {
    reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .map_err(|e| agentrr_core::AgentrrError::Other(format!("http client: {e}")))
}

async fn forward_upstream(
    client: &reqwest::Client,
    method: Method,
    url: Url,
    headers: &HeaderMap,
    body: Bytes,
) -> Result<(StatusCode, HeaderMap, Bytes), (StatusCode, String)> {
    let resp = client
        .request(method, url)
        .headers(forward_request_headers(headers))
        .body(body)
        .send()
        .await
        .map_err(|e| (StatusCode::BAD_GATEWAY, format!("upstream: {e}")))?;
    let status = resp.status();
    let hdrs = resp.headers().clone();
    let bytes = resp
        .bytes()
        .await
        .map_err(|e| (StatusCode::BAD_GATEWAY, format!("read upstream: {e}")))?;
    Ok((status, hdrs, bytes))
}

fn match_key_from(
    provider: &Provider,
    path: &str,
    body: &[u8],
    req_json: Option<&Value>,
    mode: MatchMode,
) -> String {
    if let Some(v) = req_json {
        match_key(provider, path, v, mode)
    } else {
        let mut pre = Vec::new();
        pre.extend_from_slice(provider.as_str().as_bytes());
        pre.push(0);
        pre.extend_from_slice(path.as_bytes());
        pre.push(0);
        pre.extend_from_slice(body);
        blake3::hash(&pre).to_hex().to_string()
    }
}

fn forward_url(upstream: &Url, uri: &Uri) -> Result<Url, (StatusCode, String)> {
    let mut out = upstream.clone();
    let p = uri.path();
    out.set_path(if p.is_empty() { "/" } else { p });
    out.set_query(uri.query());
    Ok(out)
}

fn forward_request_headers(headers: &HeaderMap) -> HeaderMap {
    const DROP: &[&str] = &[
        "host",
        "content-length",
        "connection",
        "transfer-encoding",
        "keep-alive",
        "proxy-authorization",
        "proxy-authenticate",
        "te",
        "trailer",
        "upgrade",
    ];
    let mut out = HeaderMap::new();
    for (name, value) in headers.iter() {
        let name_lower = name.as_str().to_ascii_lowercase();
        if DROP.iter().any(|d| *d == name_lower) {
            continue;
        }
        out.append(name.clone(), value.clone());
    }
    out
}

fn rebuild_response(status: StatusCode, headers: &HeaderMap, body: Bytes) -> Response<Body> {
    const DROP: &[&str] = &[
        "content-length",
        "transfer-encoding",
        "connection",
        "keep-alive",
    ];
    let mut builder = Response::builder().status(status);
    for (name, value) in headers.iter() {
        let name_lower = name.as_str().to_ascii_lowercase();
        if DROP.iter().any(|d| *d == name_lower) {
            continue;
        }
        if let Ok(n) = HeaderName::from_bytes(name.as_ref()) {
            builder = builder.header(n, value.clone());
        }
    }
    builder
        .body(Body::from(body))
        .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response())
}

fn content_type(headers: &HeaderMap) -> String {
    headers
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string()
}

fn detect_stream(body: &[u8], resp_content_type: &str) -> bool {
    if resp_content_type.contains("text/event-stream") {
        return true;
    }
    serde_json::from_slice::<Value>(body)
        .ok()
        .as_ref()
        .and_then(|v| v.get("stream"))
        .and_then(|s| s.as_bool())
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use agentrr_core::RunManifest;
    use agentrr_match::match_key;
    use agentrr_store::{verify_run, Store};
    use serde_json::json;
    use std::path::Path;
    use tempfile::TempDir;
    use wiremock::{
        matchers::{method, path},
        Mock, MockServer, ResponseTemplate,
    };

    /// Record a single completion against `upstream_uri`; returns (run_id, handle, port_addr).
    async fn record_one(
        upstream_uri: &str,
        store_root: &Path,
    ) -> (RunId, tokio::task::JoinHandle<()>, std::net::SocketAddr) {
        let store = Store::open(store_root).unwrap();
        let writer = store.create_run(RunManifest::new().unwrap()).unwrap();
        let run_id = writer.id();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let upstream_url = Url::parse(upstream_uri).unwrap();
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        let handle = tokio::spawn(async move {
            let _ = serve_record(upstream_url, None, listener, writer, async move {
                let _ = rx.await;
            })
            .await;
        });

        let resp = reqwest::Client::new()
            .post(format!("http://{addr}/v1/chat/completions"))
            .header("authorization", "Bearer sk-testsecret0123456789")
            .json(&json!({"model":"gpt-x","messages":[{"role":"user","content":"hi"}]}))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
        let _ = tx.send(());
        (run_id, handle, addr)
    }

    #[tokio::test]
    async fn record_completes_and_redacts() {
        let mock = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/chat/completions"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "id": "chatcmpl-1",
                "choices": [{"message": {"role":"assistant","content":"hi"}}]
            })))
            .mount(&mock)
            .await;
        let td = TempDir::new().unwrap();
        let (run_id, handle, _) = record_one(&mock.uri(), td.path()).await;
        handle.await.unwrap();

        let store = Store::open(td.path()).unwrap();
        let reader = store.open_run(&run_id).unwrap();
        let events = reader.events().unwrap();
        assert_eq!(events.len(), 1);
        let ev = &events[0];
        for hex in ev.request_blob.iter().chain(ev.response_blob.iter()) {
            let blob = reader.read_blob(hex).unwrap();
            assert!(!String::from_utf8_lossy(&blob).contains("sk-testsecret"));
        }
    }

    #[tokio::test]
    async fn record_then_replay_identical_bytes() {
        let mock = MockServer::start().await;
        let canned =
            json!({"id":"x","choices":[{"message":{"role":"assistant","content":"reply"}}]});
        Mock::given(method("POST"))
            .and(path("/v1/chat/completions"))
            .respond_with(ResponseTemplate::new(200).set_body_json(canned.clone()))
            .mount(&mock)
            .await;

        let td = TempDir::new().unwrap();
        let (run_id, handle, _) = record_one(&mock.uri(), td.path()).await;
        handle.await.unwrap();

        // Recorded response blob (verbatim bytes) for later comparison.
        let store = Store::open(td.path()).unwrap();
        let reader = store.open_run(&run_id).unwrap();
        let ev = reader.events().unwrap().into_iter().next().unwrap();
        let recorded_bytes = reader
            .read_blob(ev.response_blob.as_ref().unwrap())
            .unwrap();

        // Replay the same request through the replay proxy.
        let store2 = Store::open(td.path()).unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        let replay = tokio::spawn(async move {
            serve_replay(
                ReplayConfig {
                    store: store2,
                    run_id,
                    on_miss: OnMiss::Strict,
                    match_mode: MatchMode::Strict,
                    provider_override: None,
                    upstream: None,
                },
                listener,
                async move {
                    let _ = rx.await;
                },
            )
            .await
        });

        let resp = reqwest::Client::new()
            .post(format!("http://{addr}/v1/chat/completions"))
            .json(&json!({"model":"gpt-x","messages":[{"role":"user","content":"hi"}]}))
            .send()
            .await
            .unwrap();
        let status = resp.status();
        let served_bytes = resp.bytes().await.unwrap();
        let _ = tx.send(());
        let outcome = replay.await.unwrap().unwrap();

        assert_eq!(status, 200);
        assert_eq!(
            &served_bytes[..],
            &recorded_bytes[..],
            "replay bytes must match record"
        );
        assert_eq!(outcome.requests, 1);
        assert_eq!(outcome.misses, 0);
    }

    #[tokio::test]
    async fn strict_miss_reports_miss() {
        let td = TempDir::new().unwrap();
        // Empty run: no recorded events.
        let store = Store::open(td.path()).unwrap();
        let writer = store.create_run(RunManifest::new().unwrap()).unwrap();
        let run_id = writer.id();
        writer.finalize().unwrap();

        let store2 = Store::open(td.path()).unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        let replay = tokio::spawn(async move {
            serve_replay(
                ReplayConfig {
                    store: store2,
                    run_id,
                    on_miss: OnMiss::Strict,
                    match_mode: MatchMode::Strict,
                    provider_override: None,
                    upstream: None,
                },
                listener,
                async move {
                    let _ = rx.await;
                },
            )
            .await
        });

        // A request the empty run cannot answer → strict miss → 502, server stops.
        let resp = reqwest::Client::new()
            .post(format!("http://{addr}/v1/chat/completions"))
            .json(&json!({"model":"gpt-x","messages":[]}))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 502);
        let _ = tx.send(());
        let outcome = replay.await.unwrap().unwrap();
        assert_eq!(outcome.misses, 1);
    }

    #[tokio::test]
    async fn verify_run_passes_after_record() {
        let mock = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/chat/completions"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({"choices":[]})))
            .mount(&mock)
            .await;
        let td = TempDir::new().unwrap();
        let (run_id, handle, _) = record_one(&mock.uri(), td.path()).await;
        handle.await.unwrap();
        let store = Store::open(td.path()).unwrap();
        let reader = store.open_run(&run_id).unwrap();
        let report = verify_run(&reader).unwrap();
        assert_eq!(report.events, 1);
    }

    #[test]
    fn forward_url_preserves_path_and_query() {
        let up = Url::parse("https://api.example.com").unwrap();
        let u: Uri = "/v1/chat/completions?stream=1".parse().unwrap();
        let out = forward_url(&up, &u).unwrap();
        assert_eq!(
            out.as_str(),
            "https://api.example.com/v1/chat/completions?stream=1"
        );
    }

    #[test]
    fn match_key_helper_matches_engine() {
        let body = br#"{"model":"gpt","messages":[]}"#;
        let v: Value = serde_json::from_slice(body).unwrap();
        let a = match_key_from(&Provider::OpenAi, "/e", body, Some(&v), MatchMode::Strict);
        let b = match_key(&Provider::OpenAi, "/e", &v, MatchMode::Strict);
        assert_eq!(a, b);
    }
}
