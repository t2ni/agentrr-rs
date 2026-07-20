# On-disk format

A store is a directory (default `~/.agentrr`). Each run is one subdir named after
its UUIDv7 `run_id`, so a directory listing is naturally time-ordered.

```
<run_id>/
  manifest.json          # RunManifest: id, created_at, schema_version,
                         #   tool_version, provider, name, parent/fork_at, event_count
  events.sqlite          # ordered events + match keys (source of truth)
  blobs/<blake3hex>.bin  # content-addressed payloads (deduped)
  notes.md               # optional human notes (not written by the tool)
```

## `manifest.json`

The run descriptor. `schema_version` starts at `1`; `import`/`open` reject
mismatched versions. Forks record `parent` (the parent `RunId`) and `fork_at`
(the step where the fork diverged).

## `events.sqlite`

```sql
CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE events (
  step          INTEGER PRIMARY KEY,   -- monotonic, 0-based
  kind          TEXT NOT NULL,         -- LlmCompletion|ToolCall|Clock|Random|Env
  ts_wall_ns    INTEGER NOT NULL,
  ts_mono_ns    INTEGER NOT NULL,
  match_key     TEXT,                  -- NULL for Clock/Random (no replay alignment)
  request_blob  TEXT,                  -- blake3 hex (redacted)
  response_blob TEXT,                  -- blake3 hex (verbatim)
  is_stream     INTEGER NOT NULL DEFAULT 0,
  meta_json     TEXT                   -- model, status, endpoint, latency_ms, chunks, …
);
CREATE INDEX idx_events_matchkey ON events(match_key);
```

SQLite uses the **default rollback journal** (not WAL), so each run directory is
self-contained at rest — no `-wal`/`-shm` sidecars, which keeps `bundle`/zip
portable.

## Blobs

Content-addressed with **BLAKE3**; identical payloads dedupe automatically.
Filenames are `<blake3-hex>.bin`. Request blobs are stored **redacted**;
response blobs **verbatim** (byte-exact replay); auth headers/cookies are never
stored (see `DECISIONS.md` D0009).

## Bundles (`.agentrr`)

A bug bundle is a zip of a run directory's *contents* plus a `bundle.json`
descriptor:

```
manifest.json     # the run manifest (also at archive root)
events.sqlite
blobs/…
bundle.json       # { format: "agentrr-bundle", format_version: 1, tool_version,
                  #   run_id, scrubbed, schema_version, event_count, blob_count }
```

`agentrr import` reads `bundle.json`, checks `schema_version`, and extracts into
the store with zip path-traversal sanitization. `--scrub` redacts residual
secrets (note: a scrubbed bundle is no longer byte-exact-replayable).
