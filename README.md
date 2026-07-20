# agentrr

> Deterministic record & replay for AI agents — `rr`/time-travel debugging, but for LLM agents.

`agentrr` records every non-deterministic boundary of an AI-agent run (LLM
completions, tool-call results, timestamps, random draws) and lets you **replay**
the exact same run bit-for-bit from cache — zero tokens, zero network —
**time-travel** to any step, **fork** an alternate branch, and **export** a
self-contained bug bundle anyone can reproduce with one command.

It plugs into existing agents as an **OpenAI-/Anthropic-compatible reverse
proxy**. Change one base URL, not your code.

- **Local-first.** Nothing leaves your machine. No telemetry, ever.
- **Deterministic by construction.** Replays reproduce the *same bytes*; `verify`
  asserts it.
- **Provider-agnostic.** OpenAI Chat Completions + Responses, Anthropic Messages,
  streaming and non-streaming.
- Apache-2.0.

---

## Quickstart

```sh
cargo install --path crates/agentrr-cli     # or: cargo build --release
```

### 1. Record a session

```sh
agentrr record --provider openai            # → https://api.openai.com
```

`agentrr` prints the env to export and a `run_id`, then proxies traffic to the
upstream while capturing it:

```sh
export OPENAI_BASE_URL=http://127.0.0.1:8080/v1
# run_id: 019f7d8b-… (recording -> ~/.agentrr/019f7d8b-…)
# (Ctrl-C to stop and finalize the run)
```

In another shell (or after `eval`-ing the exports), run your agent normally.
Stop with **Ctrl-C** when done; the run is finalized.

### 2. Replay offline

```sh
agentrr replay --run 019f7d8b-…
```

Re-export the base URL, re-run the agent — it gets the **exact same responses**
from cache, with no network calls. A cache miss in strict mode fails loudly
(HTTP 502, exit code `2`); use `--on-miss passthrough --upstream …` to fall back
to live and keep recording.

### 3. Verify determinism

```sh
agentrr verify --run 019f7d8b-…            # exit 3 if any blob/cursor is off
```

### 4. Time-travel: diff and fork

```sh
agentrr steps --run 019f7d8b-…
agentrr diff --run 019f7d8b-… --against 019fa011-…

# Override step 2's response, then continue recording live from step 3:
echo '{"response_path": "new_answer.json"}' > override.json
agentrr fork --run 019f7d8b-… --at 2 --override override.json
```

### 5. Share a bug bundle

```sh
agentrr bundle --run 019f7d8b-… --out bug.agentrr      # add --scrub to redact
agentrr import bug.agentrr                              # on another machine
agentrr verify --run 019f7d8b-…                         # reproduces byte-identical
```

---

## Record a Claude Code session

`agentrr` works with **Claude Code** with a single env change. In one shell:

```sh
agentrr record --provider anthropic
# export ANTHROPIC_BASE_URL=http://127.0.0.1:8080
# run_id: 019f… (recording -> ~/.agentrr/019f…)
```

Then launch Claude Code against the proxy:

```sh
export ANTHROPIC_BASE_URL=http://127.0.0.1:8080
claude        # use Claude Code normally; everything is captured
```

Ctrl-C the `agentrr record` process when finished. To replay that session
offline:

```sh
agentrr replay --run 019f…
export ANTHROPIC_BASE_URL=http://127.0.0.1:8080
claude        # identical responses, zero network
```

> Streaming is captured and replayed byte-identically (add `--realtime` to
> reproduce original chunk timings).

---

## Commands

| Command | What it does |
| --- | --- |
| `record` | Proxy + capture to a new run. |
| `replay` | Serve a run from cache (`--on-miss strict\|passthrough`, `--realtime`). |
| `verify` | Self-check: blobs intact + FIFO self-consistent (exit 3 on failure). |
| `ls` / `steps` / `show` | Inspect runs, their steps, a single event. |
| `diff` | Structural diff of two runs, step by step. |
| `fork` | Copy a run's prefix, override a step, continue live. |
| `bundle` / `import` | Portable `.agentrr` zip; `--scrub` redacts. |
| `env` | Print the env exports for the detected proxy. |

Global flags: `--store <DIR>` (default `~/.agentrr`), `--json`, `-V/--version`.

**Exit codes:** `0` ok · `1` error · `2` cache miss (strict replay) · `3`
verification/determinism failure.

Recipes for **LangGraph, CrewAI, OpenAI Agents SDK, Vercel AI SDK** live in
[`docs/integrations.md`](docs/integrations.md) — they're all the same one-line
base-URL swap.

---

## How it works

Every request through the proxy is hashed into a **match key**
(`blake3(provider ‖ endpoint ‖ canonical_json)`) and its response is stored
verbatim, content-addressed by BLAKE3. On replay, the same request maps to the
same response — with a per-key FIFO cursor so identical requests in a retry loop
still line up. See [`docs/matching.md`](docs/matching.md),
[`docs/format.md`](docs/format.md), [`docs/architecture.md`](docs/architecture.md).

**Privacy:** request blobs are stored redacted, response blobs verbatim (needed
for byte-exact replay), and **no auth headers or cookies are ever stored.**
`bundle --scrub` redacts residual secrets before sharing. See
[`DECISIONS.md`](DECISIONS.md).

## Workspace

| Crate | Role |
| --- | --- |
| `agentrr-core` | Pure domain types (no I/O). |
| `agentrr-store` | SQLite + BLAKE3 blob store, redaction, verify, diff, fork, bundle. |
| `agentrr-match` | Canonical JSON, match keys, FIFO cursor. |
| `agentrr-proxy` | axum record/replay reverse proxy. |
| `agentrr-sdk` | Optional in-process Rust SDK. |
| `agentrr-cli` | The `agentrr` binary. |

## License

Apache-2.0. See [`LICENSE`](LICENSE).
