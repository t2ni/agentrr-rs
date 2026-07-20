# agentrr

> Deterministic record & replay for AI agents — `rr`/time-travel debugging, but for LLM agents.

`agentrr` records every non-deterministic boundary of an AI-agent run (LLM
completions, tool-call results, timestamps, random seeds) and lets you **replay**
the exact same run bit-for-bit from cache (zero tokens, zero network),
**time-travel** to any step, **fork** an alternate branch, and **export** a
self-contained bug bundle so anyone can reproduce a failure with one command.

It plugs into existing agents by acting as an **OpenAI-/Anthropic-compatible
reverse proxy** — change one base URL, not your code.

**Status:** early work-in-progress (scaffold). See `prompt.md` for the full
specification and milestone plan.

## Workspace layout

| Crate | Role |
| --- | --- |
| `agentrr-core` | Pure domain types: `Run`, `Event`, `Step`, `RunManifest`, errors. No I/O. |
| `agentrr-store` | SQLite event index + BLAKE3 content-addressed blob store, redaction, bundle/import. |
| `agentrr-match` | Canonical request normalization, match keys, FIFO cursors, strict/loose. |
| `agentrr-proxy` | axum reverse proxy (record & replay) speaking OpenAI + Anthropic. |
| `agentrr-sdk` | Optional in-process Rust SDK for non-proxy flows. |
| `agentrr-cli` | The `agentrr` binary. |

## Build

```sh
cargo build --workspace
cargo test --workspace
```

## License

Apache-2.0. See `LICENSE`.
