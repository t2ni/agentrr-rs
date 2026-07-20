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
use std::time::{Duration, Instant};

use agentrr_core::{Event, EventKind, RunId, RunManifest, Step};
use agentrr_match::{match_key, MatchMode, Provider, ReplayCursor};
use agentrr_store::{RunReader, Store};
use axum::body::{Body, Bytes};
use axum::extract::State;
use axum::http::{HeaderMap, HeaderName, Method, Response, StatusCode, Uri};
use axum::response::IntoResponse;
use axum::Router;
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::net::TcpListener;
use tokio::sync::{Mutex, Notify};
use url::Url;

/// One captured SSE chunk: its size in bytes and the ms elapsed since the
/// previous chunk arrived (chunk 0 = ms to first byte).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChunkTiming {
    pub size: u64,
    pub dt_ms: u64,
}

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
    let provider = detect_provider(state.provider_override.as_ref(), path, &headers);
    let key = match_key_from(&provider, path, &body, req_json.as_ref(), MatchMode::Strict);
    let req_bytes = body.clone();

    let forward_url = forward_url(&state.upstream, &uri)?;
    let resp = state
        .client
        .request(method, forward_url)
        .headers(forward_request_headers(&headers))
        .body(body)
        .send()
        .await
        .map_err(|e| (StatusCode::BAD_GATEWAY, format!("upstream: {e}")))?;

    let status = resp.status();
    let resp_headers = resp.headers().clone();
    let resp_ct = content_type(&resp_headers);
    let is_stream = detect_stream(req_json.as_ref(), &resp_ct);

    let (resp_bytes, chunks) = if is_stream {
        collect_stream(resp).await?
    } else {
        let b = resp
            .bytes()
            .await
            .map_err(|e| (StatusCode::BAD_GATEWAY, format!("read upstream: {e}")))?;
        (b, Vec::new())
    };

    {
        let mut guard = state.writer.lock().await;
        if let Some(w) = guard.as_mut() {
            let meta = build_meta(
                req_json.as_ref(),
                status,
                &resp_ct,
                &provider,
                started.elapsed().as_millis() as u64,
                &chunks,
            );
            record_event(w, &req_bytes, &resp_bytes, &key, is_stream, meta);
        }
    }

    Ok(rebuild_response(status, &resp_headers, resp_bytes))
}

/// Read an SSE (`text/event-stream`) body chunk-by-chunk, capturing byte sizes
/// and inter-chunk timing while concatenating into a single byte buffer. The
/// concatenation is byte-identical to the full stream.
async fn collect_stream(
    resp: reqwest::Response,
) -> Result<(Bytes, Vec<ChunkTiming>), (StatusCode, String)> {
    let mut buf = Vec::new();
    let mut chunks = Vec::new();
    let start = Instant::now();
    let mut last = start;
    let mut stream = resp.bytes_stream();
    while let Some(item) = stream.next().await {
        let b = item.map_err(|e| (StatusCode::BAD_GATEWAY, format!("stream read: {e}")))?;
        let dt = last.elapsed().as_millis() as u64;
        chunks.push(ChunkTiming {
            size: b.len() as u64,
            dt_ms: dt,
        });
        buf.extend_from_slice(&b);
        last = Instant::now();
    }
    Ok((Bytes::from(buf), chunks))
}

#[allow(clippy::too_many_arguments)]
fn build_meta(
    req_json: Option<&Value>,
    status: StatusCode,
    resp_ct: &str,
    provider: &Provider,
    latency_ms: u64,
    chunks: &[ChunkTiming],
) -> Value {
    let mut m = serde_json::json!({
        "model": req_json.and_then(|v| v.get("model")).and_then(|m| m.as_str()),
        "status": status.as_u16(),
        "latency_ms": latency_ms,
        "content_type": resp_ct,
        "provider": provider.as_str(),
    });
    if !chunks.is_empty() {
        m["chunks"] = serde_json::to_value(chunks).unwrap_or(Value::Null);
    }
    m
}

/// Shared by record + passthrough: persist a captured request/response pair.
fn record_event(
    w: &mut agentrr_store::RunWriter,
    req_bytes: &[u8],
    resp_bytes: &[u8],
    key: &str,
    is_stream: bool,
    meta: Value,
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
    realtime: bool,
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
    /// Re-produce recorded inter-chunk delays for streamed responses.
    pub realtime: bool,
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
        realtime,
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
        realtime,
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
    let provider = detect_provider(state.provider_override.as_ref(), path, &headers);
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
    let body = if ev.is_stream {
        let chunks = parse_chunks(&ev.meta);
        streaming_body(Bytes::from(bytes), &chunks, state.realtime)
    } else {
        Body::from(bytes)
    };
    Ok(Response::builder()
        .status(status)
        .header("content-type", ct)
        .body(body)
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
            let req_json = serde_json::from_slice::<Value>(&req_bytes).ok();
            let forward_url = forward_url(&upstream, &uri)?;
            let (status, resp_headers, resp_bytes) =
                forward_upstream(&client, method, forward_url, &headers, body).await?;
            let resp_ct = content_type(&resp_headers);
            let is_stream = detect_stream(req_json.as_ref(), &resp_ct);

            // Record the new event into the run being replayed (append).
            let _guard = state.write_lock.lock().await;
            if let Ok(mut w) = state.store.open_run_for_append(&state.run_id) {
                let meta = build_meta(
                    req_json.as_ref(),
                    status,
                    &resp_ct,
                    provider,
                    started.elapsed().as_millis() as u64,
                    &[],
                );
                record_event(&mut w, &req_bytes, &resp_bytes, key, is_stream, meta);
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

/// Resolve the provider for a request: explicit override wins, else path-based
/// detection, else header hints (`anthropic-version` ⇒ Anthropic). Used by both
/// record and replay so the match key is consistent across the two passes.
fn detect_provider(override_: Option<&Provider>, path: &str, headers: &HeaderMap) -> Provider {
    if let Some(p) = override_ {
        return p.clone();
    }
    let by_path = Provider::from_endpoint(path);
    if !matches!(by_path, Provider::Other(_)) {
        return by_path;
    }
    if headers.contains_key("anthropic-version") {
        return Provider::Anthropic;
    }
    by_path
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

fn detect_stream(req_json: Option<&Value>, resp_content_type: &str) -> bool {
    if resp_content_type.contains("text/event-stream") {
        return true;
    }
    req_json
        .and_then(|v| v.get("stream"))
        .and_then(|s| s.as_bool())
        .unwrap_or(false)
}

/// Build a response [`Body`] for a recorded streaming event. With `realtime`,
/// re-emit chunks with their recorded inter-chunk delays; otherwise emit the
/// full byte buffer at once (near-zero delay). Either way the bytes are identical.
fn streaming_body(bytes: Bytes, chunks: &[ChunkTiming], realtime: bool) -> Body {
    if !realtime || chunks.is_empty() {
        return Body::from(bytes);
    }
    // Split the buffer into the recorded chunk sizes.
    let mut pieces: Vec<Bytes> = Vec::with_capacity(chunks.len());
    let mut off = 0usize;
    for c in chunks {
        let end = (off + c.size as usize).min(bytes.len());
        pieces.push(bytes.slice(off..end));
        off = end;
    }
    if off < bytes.len() {
        pieces.push(bytes.slice(off..));
    }
    let dts: Vec<u64> = chunks.iter().map(|c| c.dt_ms).collect();
    let stream = async_stream::stream! {
        for (i, piece) in pieces.into_iter().enumerate() {
            if i < dts.len() && dts[i] > 0 {
                tokio::time::sleep(Duration::from_millis(dts[i])).await;
            }
            yield Ok::<Bytes, std::io::Error>(piece);
        }
    };
    Body::from_stream(stream)
}

/// Parse `meta.chunks` back into [`ChunkTiming`].
fn parse_chunks(meta: &Value) -> Vec<ChunkTiming> {
    meta.get("chunks")
        .and_then(|v| serde_json::from_value(v.clone()).ok())
        .unwrap_or_default()
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
                    realtime: false,
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
                    realtime: false,
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

    async fn record_stream(
        upstream_uri: &str,
        store_root: &Path,
    ) -> (RunId, tokio::task::JoinHandle<()>) {
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
            .json(
                &json!({"model":"gpt-x","stream":true,"messages":[{"role":"user","content":"hi"}]}),
            )
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
        let _ = resp.bytes().await;
        let _ = tx.send(());
        (run_id, handle)
    }

    #[tokio::test]
    async fn sse_records_and_replays_byte_identical() {
        let sse: &[u8] =
            b"data: {\"content\":\"a\"}\n\ndata: {\"content\":\"b\"}\n\ndata: [DONE]\n\n";
        let mock = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/chat/completions"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("content-type", "text/event-stream")
                    .set_body_raw(sse.to_vec(), "text/event-stream"),
            )
            .mount(&mock)
            .await;

        let td = TempDir::new().unwrap();
        let (run_id, handle) = record_stream(&mock.uri(), td.path()).await;
        handle.await.unwrap();

        // Recorded: is_stream set, chunks present, blob == sse.
        let store = Store::open(td.path()).unwrap();
        let reader = store.open_run(&run_id).unwrap();
        let ev = reader.events().unwrap().into_iter().next().unwrap();
        assert!(ev.is_stream, "event should be marked streaming");
        assert!(ev.meta.get("chunks").is_some(), "chunks should be recorded");
        let recorded = reader
            .read_blob(ev.response_blob.as_ref().unwrap())
            .unwrap();
        assert_eq!(
            &recorded[..],
            sse,
            "recorded blob must equal the SSE stream"
        );

        // Replay byte-identical, both near-zero and realtime.
        for realtime in [false, true] {
            let s = Store::open(td.path()).unwrap();
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            let (tx, rx) = tokio::sync::oneshot::channel::<()>();
            let join = tokio::spawn(async move {
                serve_replay(
                    ReplayConfig {
                        store: s,
                        run_id,
                        on_miss: OnMiss::Strict,
                        match_mode: MatchMode::Strict,
                        provider_override: None,
                        upstream: None,
                        realtime,
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
                .json(&json!({"model":"gpt-x","stream":true,"messages":[{"role":"user","content":"hi"}]}))
                .send()
                .await
                .unwrap();
            let bytes = resp.bytes().await.unwrap();
            let _ = tx.send(());
            join.await.unwrap().unwrap();
            assert_eq!(
                &bytes[..],
                sse,
                "replay (realtime={realtime}) must be byte-identical"
            );
            assert!(String::from_utf8_lossy(&bytes).contains("[DONE]"));
        }
    }

    // ------------------------------------------------------------------ //
    // Anthropic /v1/messages (provider auto-detect + wire format)
    // ------------------------------------------------------------------ //

    fn anthropic_req() -> serde_json::Value {
        json!({
            "model":"claude-3",
            "max_tokens":16,
            "system":"be brief",
            "messages":[{"role":"user","content":"hi"}]
        })
    }

    async fn record_anthropic(
        upstream_uri: &str,
        store_root: &Path,
        body_json: serde_json::Value,
    ) -> (
        RunId,
        tokio::task::JoinHandle<()>,
        Bytes,
        std::net::SocketAddr,
    ) {
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
            .post(format!("http://{addr}/v1/messages"))
            .header("x-api-key", "sk-ant-testsecret0123456789012")
            .header("anthropic-version", "2023-06-01")
            .json(&body_json)
            .send()
            .await
            .unwrap();
        let served = resp.bytes().await.unwrap();
        let _ = tx.send(());
        (run_id, handle, served, addr)
    }

    #[tokio::test]
    async fn anthropic_non_stream_round_trip() {
        let mock = MockServer::start().await;
        let canned = json!({
            "id":"msg_1","type":"message","role":"assistant",
            "content":[{"type":"text","text":"hi"}],
            "model":"claude-3","stop_reason":"end_turn"
        });
        Mock::given(method("POST"))
            .and(path("/v1/messages"))
            .respond_with(ResponseTemplate::new(200).set_body_json(canned))
            .mount(&mock)
            .await;

        let td = TempDir::new().unwrap();
        let (run_id, handle, served, _addr) =
            record_anthropic(&mock.uri(), td.path(), anthropic_req()).await;
        handle.await.unwrap();

        // Recorded: provider auto-detected as anthropic; no secret stored.
        let store = Store::open(td.path()).unwrap();
        let reader = store.open_run(&run_id).unwrap();
        let ev = reader.events().unwrap().into_iter().next().unwrap();
        assert_eq!(ev.meta["provider"], "anthropic");
        assert_eq!(
            &reader
                .read_blob(ev.response_blob.as_ref().unwrap())
                .unwrap()[..],
            &served[..]
        );
        for hex in ev.request_blob.iter().chain(ev.response_blob.iter()) {
            let b = reader.read_blob(hex).unwrap();
            assert!(!String::from_utf8_lossy(&b).contains("sk-ant-testsecret"));
        }

        // Replay byte-identical.
        let s = Store::open(td.path()).unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        let join = tokio::spawn(async move {
            serve_replay(
                ReplayConfig {
                    store: s,
                    run_id,
                    on_miss: OnMiss::Strict,
                    match_mode: MatchMode::Strict,
                    provider_override: None,
                    upstream: None,
                    realtime: false,
                },
                listener,
                async move {
                    let _ = rx.await;
                },
            )
            .await
        });
        let replayed = reqwest::Client::new()
            .post(format!("http://{addr}/v1/messages"))
            .header("x-api-key", "sk-ant-testsecret0123456789012")
            .header("anthropic-version", "2023-06-01")
            .json(&anthropic_req())
            .send()
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        let _ = tx.send(());
        join.await.unwrap().unwrap();
        assert_eq!(
            &replayed[..],
            &served[..],
            "anthropic replay must be byte-identical"
        );
    }

    #[tokio::test]
    async fn anthropic_stream_round_trip() {
        let sse: &[u8] = b"event: content_block_delta\ndata: {\"type\":\"text_delta\",\"text\":\"a\"}\n\nevent: message_stop\ndata: {\"type\":\"message_stop\"}\n\n";
        let mock = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/messages"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("content-type", "text/event-stream")
                    .set_body_raw(sse.to_vec(), "text/event-stream"),
            )
            .mount(&mock)
            .await;

        let td = TempDir::new().unwrap();
        let mut req = anthropic_req();
        req["stream"] = json!(true);
        let (run_id, handle, _served, _) = record_anthropic(&mock.uri(), td.path(), req).await;
        handle.await.unwrap();

        let store = Store::open(td.path()).unwrap();
        let reader = store.open_run(&run_id).unwrap();
        let ev = reader.events().unwrap().into_iter().next().unwrap();
        assert!(ev.is_stream);
        assert_eq!(ev.meta["provider"], "anthropic");
        assert_eq!(
            &reader
                .read_blob(ev.response_blob.as_ref().unwrap())
                .unwrap()[..],
            sse
        );

        // Replay the stream byte-identical.
        let s = Store::open(td.path()).unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        let join = tokio::spawn(async move {
            serve_replay(
                ReplayConfig {
                    store: s,
                    run_id,
                    on_miss: OnMiss::Strict,
                    match_mode: MatchMode::Strict,
                    provider_override: None,
                    upstream: None,
                    realtime: false,
                },
                listener,
                async move {
                    let _ = rx.await;
                },
            )
            .await
        });
        let mut replay_req = anthropic_req();
        replay_req["stream"] = json!(true);
        let replayed = reqwest::Client::new()
            .post(format!("http://{addr}/v1/messages"))
            .header("anthropic-version", "2023-06-01")
            .json(&replay_req)
            .send()
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        let _ = tx.send(());
        join.await.unwrap().unwrap();
        assert_eq!(
            &replayed[..],
            sse,
            "anthropic stream replay must be byte-identical"
        );
    }
}
