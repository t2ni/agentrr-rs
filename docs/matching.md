# Match key algorithm

> The heart of deterministic replay. This document specifies how a *live* request
> is mapped to a *recorded* response, with worked examples.

## Why a match key

During replay we serve cached responses **without touching the network**. To do
that we must answer, for every incoming request: *which recorded response did
the agent originally get for this request?* We hash the request into a stable
**match key** and look it up. The hash must be invariant under every
non-semantic difference (key ordering, float spelling, Unicode form) so that a
request that *means* the same thing matches.

## The algorithm

Given the provider, the endpoint path, and the parsed JSON body:

1. **(Optional field stripping — [`MatchMode::Loose`] only)** remove sampling
   fields (`temperature`, `top_p`, `seed`) at every object level.
2. **Canonicalize the JSON body** ([`canonical_json`]):
   - Object keys sorted recursively, ascending byte order.
   - Strings NFC-normalized.
   - Numbers canonicalized: integer-valued floats collapse to integers
     (`1.0` → `1`); other floats use shortest round-trip `Display`.
   - Arrays keep their order (order is *semantic* for a message history).
3. **Hash** with BLAKE3:

   ```text
   match_key = blake3( provider || NUL || endpoint || NUL || canonical_json )
   ```

   NUL separators prevent ambiguity when provider/endpoint happen to share bytes.

## Worked examples

### 1. Key order is irrelevant

```text
body A = {"b": 2, "a": 1}
body B = {"a": 1, "b": 2}

canonical(A) == canonical(B) == {"a":1,"b":2}
→ same match_key
```

### 2. Nested objects sort; array order does **not**

```jsonc
{
  "model": "gpt-4",
  "messages": [{"role":"user","content":"hi"}],
  "b": 2, "a": 1,
  "nested": {"y": 1, "x": [3, 2, 1]}
}
```

canonicalizes to (see snapshot `nested_unsorted.snap`):

```text
{"a":1,"b":2,"messages":[{"content":"hi","role":"user"}],"model":"gpt-4","nested":{"x":[3,2,1],"y":1}}
```

Note `"x":[3,2,1]` keeps its array order — message histories are order-sensitive.

### 3. Integer-valued floats collapse

```text
{"n": 1.0}  →  {"n":1}     // matches {"n": 1}
```

### 4. Unicode NFC

```text
"e\u{0301}lan"  ("e" + combining acute)  →  "élan"   (single codepoint U+00E9)
```

(see snapshot `nfc_string.snap`). Two clients sending equivalent text in
different normalization forms still match.

### 5. Provider + endpoint matter

The *same* body at `/v1/chat/completions` (OpenAI) and `/v1/messages` (Anthropic)
produces **different** keys — they are different wire formats and must not collide.

## Ordering: repeated identical requests

An agent may issue the *same* request multiple times (retry loops, repeated
turns). The match key alone is not enough — the *k*-th live occurrence must map
to the *k*-th recorded response, in record order.

[`ReplayCursor`] tracks a per-key counter. On each live request the caller:

1. computes `key = match_key(provider, endpoint, body, mode)`,
2. loads `recorded = reader.events_for_key(key)` (already step-ordered),
3. asks `cursor.next_index(key, recorded.len())`.

If `Some(i)`, serve `recorded[i]`. If `None`, the key's occurrences are
exhausted → **cache miss** (strict mode errors; passthrough falls back to live
and records the new response).

## Strict vs Loose

| Mode     | Behavior |
|----------|----------|
| `Strict` (default) | Exact canonical match. |
| `Loose`  | Sampling params stripped before hashing — lets you tweak `temperature`/`top_p`/`seed` and still replay old responses. |

## Redaction subtelty

`match_key` is computed on the **unredacted** canonical request, so live replay
aligns with recorded responses. **Stored blobs are redacted** (see
`DECISIONS.md` D0004 and `agentrr-store::Redactor`). The SQLite `match_key`
column therefore is *not* a hash of the (redacted) stored request blob — by
design.
