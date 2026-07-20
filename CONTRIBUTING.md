# Contributing

`agentrr` is a small Cargo workspace. Keep the build green and commits tidy.

## Setup

```sh
cargo build --workspace
cargo fmt --all
cargo clippy --all-targets --all-features -- -D warnings
cargo test --workspace
```

CI runs all of the above. No network in tests — everything is offline against
fixtures and a mock upstream.

## Layout

| Crate | Role |
| --- | --- |
| `agentrr-core` | Pure types (no I/O). |
| `agentrr-store` | SQLite + blobs, redaction, verify, diff, fork, bundle. |
| `agentrr-match` | Canonical JSON, match keys, FIFO cursor. |
| `agentrr-proxy` | axum record/replay proxy. |
| `agentrr-sdk` | In-process SDK. |
| `agentrr-cli` | The `agentrr` binary. |

## Conventions

- Milestone-sized commits, each compiling and passing tests.
- `DECISIONS.md` records every unspecified design choice (with a `D00xx` id).
- Public items get `///` docs.
- Keep `agentrr-core` free of I/O and async.

## Adding a provider

The proxy is wire-format-agnostic — a new OpenAI/Anthropic-compatible provider
usually needs only a `Provider` variant and a path/header hint in
`agentrr-match::Provider::from_endpoint` / `agentrr-proxy::detect_provider`.
