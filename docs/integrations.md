# Integrations

`agentrr` is an OpenAI-/Anthropic-compatible reverse proxy. **Every integration
is a one-line base-URL swap** — no code changes to the agent. The general flow:

1. Start `agentrr record` (or `replay --run <id>`).
2. Point the agent's base URL at the agentrr port.
3. Run the agent normally; stop with Ctrl-C.

Need the exports?

```sh
eval "$(agentrr env --port 8080)"
```

---

## Claude Code

```sh
# shell 1
agentrr record --provider anthropic
# shell 2
export ANTHROPIC_BASE_URL=http://127.0.0.1:8080
claude
```

Replay:

```sh
agentrr replay --run <id>
export ANTHROPIC_BASE_URL=http://127.0.0.1:8080
claude            # same responses, zero network
```

Streaming is captured and replayed byte-identically. Add `--realtime` to
reproduce original chunk timings.

## OpenAI Agents SDK / OpenAI Python SDK

```sh
agentrr record --provider openai
export OPENAI_BASE_URL=http://127.0.0.1:8080/v1
# then your normal: python -m agents … / python your_script.py
```

Most OpenAI SDKs honor `OPENAI_BASE_URL` (or `OPENAI_API_BASE`) with no code
change.

## Vercel AI SDK

The Vercel AI SDK lets you pass a `baseURL` to its OpenAI-compatible provider:

```ts
import { createOpenAI } from '@ai-sdk/openai';
const openai = createOpenAI({ baseURL: 'http://127.0.0.1:8080/v1' });
```

Then run `agentrr record` alongside.

## LangGraph / LangChain

LangChain's `ChatOpenAI` accepts `openai_api_base` / `base_url`:

```python
from langchain_openai import ChatOpenAI
llm = ChatOpenAI(base_url="http://127.0.0.1:8080/v1", api_key="dummy")
```

```sh
agentrr record --provider openai
```

## CrewAI

CrewAI leans on the underlying litellm/OpenAI client; set the env var before
launching your crew:

```sh
agentrr record --provider openai
export OPENAI_API_BASE=http://127.0.0.1:8080/v1
export OPENAI_BASE_URL=http://127.0.0.1:8080/v1
python -m your_crew
```

---

## Notes & caveats

- Both `OPENAI_BASE_URL` (path includes `/v1`) and `ANTHROPIC_BASE_URL` (no `/v1`)
  can point at the **same** agentrr port — the proxy is path-preserving and
  auto-detects the provider from the path and `anthropic-version` header.
- Strict replay returns HTTP 502 on a cache miss and exits `2`. Use
  `--on-miss passthrough --upstream <URL>` to fall back to live and keep
  recording into the same run.
- `agentrr verify --run <id>` is the fastest way to confirm a run (or an imported
  bundle) is byte-identical and replayable.
