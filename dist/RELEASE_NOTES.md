## v0.1.0 — first release

**Deterministic record & replay for AI agents.** Record once, replay byte-identical,
offline, zero tokens.

### Highlights

- **Record / replay proxy** for **OpenAI** (Chat Completions + Responses) and
  **Anthropic Messages**, streaming and non-streaming — byte-exact.
- **Deterministic matching** — canonical JSON (sorted keys, NFC, number
  normalization) + BLAKE3 match key + per-key FIFO cursor so retries still align.
- **`verify`** — offline determinism self-check (exit 3 on failure).
- **Time-travel** — `diff` two runs, `fork` a run with a step override.
- **Bundles** — portable `.agentrr` zip with `--scrub` secret redaction.
- **Rust SDK** — in-process `Recorder` / `Replayer`.
- **Local-first, private, no telemetry.** Apache-2.0.

### Install

**Download** the Windows binary below (`agentrr-x86_64-windows.zip`), unzip, and:

```sh
agentrr --help
```

**Or build from source:**

```sh
git clone https://github.com/t2ni/agentrr-rs
cd agentrr-rs
cargo install --path crates/agentrr-cli
```

### Quickstart

```sh
agentrr record --provider anthropic            # Claude Code / Anthropic gateway
# export ANTHROPIC_BASE_URL=http://127.0.0.1:8080  (run your agent)
# Ctrl-C when done
agentrr replay --run <run_id>                   # offline, byte-identical
agentrr verify --run <run_id>                   # exit 0 = deterministic
```

### Downloads

- `agentrr-x86_64-windows.zip` — Windows x86_64 binary (built locally, this release).
- Linux (x86_64) and macOS (aarch64) binaries will be attached automatically by CI
  once GitHub Actions billing is enabled on the repo.

See the [README](https://github.com/t2ni/agentrr-rs#agentrr) for the full guide.
