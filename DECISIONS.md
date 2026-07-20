# DECISIONS.md

Record of unspecified-detail decisions, per `prompt.md` §0 ("Ask nothing, assume
sensibly... record the decision"). Newest at the bottom.

---

## D0001 — Toolchain & edition
- Rust stable, edition 2021, MSRV 1.79 (per spec §0). Verified rustc 1.95.
- `Cargo.lock` is committed: the workspace ships a binary (`agentrr-cli`).

## D0002 — TLS backend
- Use `rustls` everywhere (`reqwest` with `rustls-tls`, default-features off).
  Avoids OpenSSL system-dependency, keeps builds reproducible offline-ish and on
  Windows. No native-tls.

## D0003 — SQLite
- `rusqlite` with the `bundled` feature → ships its own SQLite, no system lib.
  One less host dependency; deterministic across platforms.

## D0004 — Redaction vs. match key (spec §9 subpoint)
- `match_key` is computed on the **unredacted** canonical request so live replay
  still aligns with recorded responses. **Stored blobs are redacted.** The
  SQLite `match_key` column therefore may not match a hash of the (redacted)
  stored request blob — this is intentional and documented in `docs/matching.md`
  (land in M2).

## D0005 — zstd/zlib for bundles
- `zip` crate with `default-features = false, features = ["deflate"]`. Smaller
  dependency surface; deflate is universally readable. No zstd for v0.1.

## D0006 — Schema versioning
- `schema_version` starts at `1`. Stored in `manifest.json` and in the SQLite
  `meta` table. `import`/`open` reject incompatible versions with a clear error.

## D0007 — Run id
- `UUIDv7` (`uuid` crate, `v7` feature). Time-ordered → `ls` sorts naturally by
  creation time and directory listing is monotonic.

## D0008 — Ordering of identical requests
- Per-`match_key` FIFO cursor (spec §7). The *k*-th live occurrence of a key
  returns the *k*-th recorded response. Exhausting occurrences = cache miss.

## D0009 — Redaction vs. byte-exact replay (resolves a spec §9 tension)
Spec §9 says both "store redacted bodies" *and* that replay reproduces the exact
bytes. Redacting a **response** blob would mutate what replay serves, breaking
determinism (the golden rule). Policy:
- **Request blobs**: stored **redacted** — they are agent-authored (may carry
  pasted secrets) and replay never re-sends them, so redaction cannot affect a
  replay. `match_key` is still computed on the *unredacted* request (D0004).
- **Response blobs**: stored **verbatim** — needed for byte-exact replay.
- **Headers**: never stored (`Authorization`, `x-api-key`, cookies dropped before
  any blob write); they are forwarded to the live upstream during recording only.
- Residual-secret **scanning** over response blobs is a *warning* surface
  (`bundle --scrub`, M8), not silent mutation.

## D0010 — Strict-miss server behavior
`--on-miss strict`: on a cache miss the proxy returns HTTP 502 to the client and
**stops the server**, so the process exits with code 2 (spec §5). This is
intentional "fail loudly" behavior — use `--on-miss passthrough` (with
`--upstream`) to keep serving.
