# Architecture

## Workspace

```
agentrr/
  crates/
    agentrr-core/   # domain types: RunId, Event, Step, RunManifest, AgentrrError
                    #   — no I/O, no async, trivially testable
    agentrr-store/  # SQLite + BLAKE3 blob store; create/open/list/append;
                    #   redaction; verify; diff; fork; bundle/import
    agentrr-match/  # canonical JSON, match keys, FIFO cursor, strict/loose
    agentrr-proxy/  # axum reverse proxy — record + replay (one strategy per mode)
    agentrr-sdk/    # optional in-process Rust SDK (Recorder / Replayer)
    agentrr-cli/    # clap binary wiring everything together
  docs/ examples/ tests/
```

## Data flow

```
agent ──HTTP──▶ agentrr proxy ──▶ upstream (record)        ──▶ store
                  │                    │
                  │  (replay)          └─ capture request (redacted) + response (verbatim)
                  └──▶ store ──▶ agent     as a content-addressed event
```

- **Record**: forward to the real upstream, capture request/response into the
  active run as an `LlmCompletion` event keyed by a canonical match key.
- **Replay**: compute the incoming request's match key, advance a per-key FIFO
  cursor, and serve the *k*-th recorded response verbatim — no network.
- **Verify**: offline self-check that every response blob hashes back to its
  content address and that a fresh cursor maps each event to itself in FIFO order.

## Key invariants

1. **Determinism is the product.** Replays reproduce the same bytes. Where
   determinism and convenience conflict, determinism wins.
2. **Match key on the unredacted request.** Redaction mutates only the *stored*
   request blob, never the `match_key` (D0004), so live replay still aligns.
3. **Response blobs verbatim.** Redacting them would break byte-exact replay.
4. **Ordering by FIFO cursor.** Identical requests in a retry loop map to the
   *k*-th recorded response (D0008).

## Concurrency

The record proxy serializes event appends behind a `tokio::sync::Mutex` around
the single writer; SQLite uses rollback journal (serialized writes). The replay
proxy serves concurrent reads via a `Mutex<RunReader>`; passthrough appends go
through `open_run_for_append` guarded by a run-level write lock. Adequate for
local single-agent use; not tuned for high-throughput multi-tenant serving.

## Error model

All fallible library functions return `Result<T, AgentrrError>` (a `thiserror`
enum in `agentrr-core`). The CLI wraps this in a `CliError` carrying the process
exit code (`0` ok, `1` error, `2` strict-replay miss, `3` verify failure).
