# Changelog

## v0.1.0

First release.

- **Record/replay proxy** (`agentrr record` / `replay`) — OpenAI Chat Completions
  + Responses and Anthropic Messages, streaming and non-streaming, byte-exact.
- **Deterministic matching** — canonical JSON (sorted keys, NFC, number
  normalization) + BLAKE3 match key + per-key FIFO cursor.
- **`verify`** — offline determinism self-check (exit 3 on failure).
- **Time-travel** — `diff` two runs, `fork` a run with a step override.
- **Bundles** — `bundle` / `import` portable `.agentrr` zips with `--scrub`.
- **Rust SDK** — in-process `Recorder` / `Replayer` for non-proxy flows.
- **Privacy** — request blobs redacted, response blobs verbatim, auth headers
  never stored; bundles warn on residual secrets.
- Local-first, no telemetry, Apache-2.0.
