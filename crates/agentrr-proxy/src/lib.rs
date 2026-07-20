//! axum-based reverse proxy for `agentrr` — record mode.
//!
//! Path-preserving forwarder: an incoming request to `/v1/chat/completions` is
//! forwarded to `<upstream>/v1/chat/completions`. While forwarding, the request
//! and response payloads are captured as content-addressed blobs and an
//! [`EventKind::LlmCompletion`] event is appended to the active run.
//!
//! Storage policy (see `DECISIONS.md` D0009):
//! - **Request blobs** are stored **redacted** (agent-authored; may carry pasted
//!   secrets) — replay never re-sends them, so redaction is safe.
//! - **Response blobs** are stored **verbatim** — byte-exact replay needs them.
//! - **Headers are never stored** (`Authorization`, `x-api-key`, cookies dropped
//!   before any blob write). They *are* forwarded to the live upstream.
//! - The `match_key` is computed on the **unredacted** request so live replay
//!   still aligns (D0004).

#![forbid(unsafe_code)]

use std::future::Future;
use std::sync::Arc;
use std::time::Instant;

use agentrr_core::{Event, EventKind, RunManifest};
use agentrr_match::{match_key, MatchMode, Provider};
use agentrr_store::RunWriter;
use axum::body::{Body, Bytes};
use axum::extract::State;
use axum::http::{HeaderMap, HeaderName, Method, Response, StatusCode, Uri};
use axum::response::IntoResponse;
use axum::Router;
use serde_json::Value;
use tokio::net::TcpListener;
use tokio::sync::Mutex;
use url::Url;

/// Shared record-mode state cloned into every handler.
#[derive(Clone)]
struct RecordState {
    upstream: Url,
    client: reqwest::Client,
    writer: Arc<Mutex<Option<RunWriter>>>,
    provider_override: Option<Provider>,
}

/// Run the record proxy until `shutdown` completes (e.g. Ctrl-C), then finalize
/// the run and return its manifest.
pub async fn serve_record(
    upstream: Url,
    provider_override: Option<Provider>,
    listener: TcpListener,
    writer: RunWriter,
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

    // No new connections accepted; finalize the writer.
    let mut guard = state.writer.lock().await;
    let Some(w) = guard.take() else {
        return Err(agentrr_core::AgentrrError::Other(
            "run already finalized".into(),
        ));
    };
    w.finalize()
}

fn build_client() -> Result<reqwest::Client, agentrr_core::AgentrrError> {
    reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .map_err(|e| agentrr_core::AgentrrError::Other(format!("http client: {e}")))
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
    let key = match_key_from(&provider, path, &body, req_json.as_ref());

    // Keep a cheap clone for storage; the original `body` moves into the forward.
    let req_bytes = body.clone();

    // Forward to upstream (auth headers included so the real API works).
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
    let resp_bytes = resp
        .bytes()
        .await
        .map_err(|e| (StatusCode::BAD_GATEWAY, format!("read upstream: {e}")))?;

    let resp_ct = content_type(&resp_headers);
    let is_stream = is_stream(req_json.as_ref(), &resp_ct);

    // Record the event (short lock; blob writes are sync file I/O).
    {
        let mut guard = state.writer.lock().await;
        if let Some(w) = guard.as_mut() {
            let request_blob = match w.store_blob(req_bytes.as_ref(), true) {
                Ok(h) => Some(h),
                Err(e) => {
                    tracing::warn!(error = %e, "storing request blob");
                    None
                }
            };
            let response_blob = match w.store_blob(&resp_bytes, false) {
                Ok(h) => Some(h),
                Err(e) => {
                    tracing::warn!(error = %e, "storing response blob");
                    None
                }
            };
            let meta = serde_json::json!({
                "model": req_json.as_ref()
                    .and_then(|v| v.get("model"))
                    .and_then(|m| m.as_str()),
                "status": status.as_u16(),
                "latency_ms": started.elapsed().as_millis() as u64,
                "content_type": resp_ct,
                "provider": provider.as_str(),
            });
            let ev = Event {
                step: agentrr_core::Step::new(0),
                kind: EventKind::LlmCompletion,
                ts_wall_ns: 0,
                ts_mono_ns: 0,
                match_key: Some(key),
                request_blob,
                response_blob,
                is_stream,
                meta,
            };
            if let Err(e) = w.write_event(ev) {
                tracing::error!(error = %e, "writing event");
            }
        }
    }

    Ok(rebuild_response(status, &resp_headers, resp_bytes))
}

fn match_key_from(
    provider: &Provider,
    path: &str,
    body: &[u8],
    req_json: Option<&Value>,
) -> String {
    if let Some(v) = req_json {
        match_key(provider, path, v, MatchMode::Strict)
    } else {
        // Non-JSON body: fall back to a content hash keyed by provider+path.
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

/// Headers to forward client→upstream. Drops hop-by-hop and framing headers;
/// keeps `Authorization` / `x-api-key` so the real provider authenticates.
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

/// Headers to copy upstream→client. Drops framing so axum recomputes them.
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

fn is_stream(req_json: Option<&Value>, resp_content_type: &str) -> bool {
    if resp_content_type.contains("text/event-stream") {
        return true;
    }
    req_json
        .and_then(|v| v.get("stream"))
        .and_then(|s| s.as_bool())
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use agentrr_core::RunManifest;
    use agentrr_match::match_key;
    use agentrr_store::Store;
    use serde_json::json;
    use std::path::Path;
    use tempfile::TempDir;
    use wiremock::{
        matchers::{method, path},
        Mock, MockServer, ResponseTemplate,
    };

    async fn record_one(
        upstream_uri: &str,
        store_root: &Path,
    ) -> (agentrr_core::RunId, tokio::task::JoinHandle<()>) {
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
            .header("x-api-key", "sk-ant-testsecret0123456789012")
            .json(&json!({"model":"gpt-x","messages":[{"role":"user","content":"hi"}]}))
            .send()
            .await
            .unwrap();
        let body: Value = resp.json().await.unwrap();
        assert_eq!(body["choices"][0]["message"]["content"], "hi");
        let _ = tx.send(());
        (run_id, handle)
    }

    #[tokio::test]
    async fn record_completion_against_mock() {
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
        let (run_id, handle) = record_one(&mock.uri(), td.path()).await;
        handle.await.unwrap();

        let store = Store::open(td.path()).unwrap();
        let reader = store.open_run(&run_id).unwrap();
        let events = reader.events().unwrap();
        assert_eq!(events.len(), 1, "exactly one event recorded");
        let ev = &events[0];
        assert_eq!(ev.kind, EventKind::LlmCompletion);
        assert!(!ev.is_stream);
        let mk = ev.match_key.clone().expect("event has match key");

        // No stored blob leaks either secret.
        for hex in ev.request_blob.iter().chain(ev.response_blob.iter()) {
            let blob = reader.read_blob(hex).unwrap();
            let s = String::from_utf8_lossy(&blob);
            assert!(!s.contains("sk-testsecret"), "auth leaked into stored blob");
            assert!(!s.contains("sk-ant-testsecret"), "api key leaked");
        }

        // Response blob is verbatim.
        let resp_bytes = reader
            .read_blob(ev.response_blob.as_ref().unwrap())
            .unwrap();
        assert!(String::from_utf8_lossy(&resp_bytes).contains("chatcmpl-1"));

        // match_key recomputes identically from the unredacted request.
        let recomputed = match_key(
            &Provider::OpenAi,
            "/v1/chat/completions",
            &json!({"model":"gpt-x","messages":[{"role":"user","content":"hi"}]}),
            MatchMode::Strict,
        );
        assert_eq!(mk, recomputed);
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
}
