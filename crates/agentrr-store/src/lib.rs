//! On-disk storage for `agentrr` runs.
//!
//! One run = one directory under the store root:
//!
//! ```text
//! <run_id>/
//!   manifest.json         # RunManifest
//!   events.sqlite         # ordered events + match keys (source of truth)
//!   blobs/<blake3hex>.bin # content-addressed payloads (deduped)
//! ```
//!
//! Design notes:
//! - SQLite uses the default rollback journal (not WAL) so each run directory is
//!   self-contained at rest — no `-wal`/`-shm` sidecars, which keeps `bundle`/zip
//!   simple and portable.
//! - Blobs are content-addressed with BLAKE3; identical payloads dedupe.
//! - The store assumes a single writer per run (the recording proxy).

#![forbid(unsafe_code)]

use std::fs;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use agentrr_core::{AgentrrError, Event, EventKind, RunId, RunManifest, Step, SCHEMA_VERSION};
use agentrr_match::{match_key, MatchMode, Provider, ReplayCursor};
use regex::Regex;
use rusqlite::{params, Connection};
use serde::Serialize;

// --------------------------------------------------------------------------- //
// Store
// --------------------------------------------------------------------------- //

/// The top-level store: a directory containing one subdir per run.
#[derive(Debug, Clone)]
pub struct Store {
    root: PathBuf,
}

impl Store {
    /// Open (creating if missing) a store at `root`.
    pub fn open(root: &Path) -> Result<Self, AgentrrError> {
        fs::create_dir_all(root)?;
        Ok(Self {
            root: root.to_path_buf(),
        })
    }

    pub fn root(&self) -> &Path {
        &self.root
    }
    /// Path to a run's directory.
    pub fn run_dir(&self, id: &RunId) -> PathBuf {
        self.root.join(id.to_string())
    }

    /// Create a new run. Fails if the run id already exists in this store.
    pub fn create_run(&self, manifest: RunManifest) -> Result<RunWriter, AgentrrError> {
        let dir = self.run_dir(&manifest.id);
        if dir.exists() {
            return Err(AgentrrError::Other(format!(
                "run directory already exists: {}",
                dir.display()
            )));
        }
        fs::create_dir_all(&dir)?;
        let blobs_dir = dir.join("blobs");
        fs::create_dir_all(&blobs_dir)?;
        let db_path = dir.join("events.sqlite");
        let conn = open_db(&db_path)?;
        init_meta(&conn, &manifest)?;
        write_manifest(&dir, &manifest)?;
        RunWriter::new(dir, blobs_dir, conn, manifest)
    }

    /// Open an existing run read-only. Checks the schema version.
    pub fn open_run(&self, id: &RunId) -> Result<RunReader, AgentrrError> {
        let dir = self.run_dir(id);
        if !dir.exists() {
            return Err(AgentrrError::RunNotFound(*id));
        }
        let manifest = read_manifest(&dir)?;
        if manifest.schema_version != SCHEMA_VERSION {
            return Err(AgentrrError::SchemaVersion {
                store: manifest.schema_version,
                supported: SCHEMA_VERSION,
            });
        }
        let conn = Connection::open(dir.join("events.sqlite")).map_err(sql_err)?;
        RunReader::new(dir, manifest, conn)
    }

    /// Reopen a finalized run for appending (used by `--on-miss passthrough` to
    /// record additional events into the run being replayed). Continues step
    /// numbering from where the run left off.
    pub fn open_run_for_append(&self, id: &RunId) -> Result<RunWriter, AgentrrError> {
        let dir = self.run_dir(id);
        if !dir.exists() {
            return Err(AgentrrError::RunNotFound(*id));
        }
        let manifest = read_manifest(&dir)?;
        if manifest.schema_version != SCHEMA_VERSION {
            return Err(AgentrrError::SchemaVersion {
                store: manifest.schema_version,
                supported: SCHEMA_VERSION,
            });
        }
        let db_path = dir.join("events.sqlite");

        let conn = Connection::open(&db_path).map_err(sql_err)?;
        // Idempotent: tables already exist (CREATE … IF NOT EXISTS).
        conn.execute_batch(SCHEMA_SQL).map_err(sql_err)?;
        let max_step: i64 = conn
            .query_row("SELECT COALESCE(MAX(step),-1) FROM events", [], |r| {
                r.get(0)
            })
            .map_err(sql_err)?;
        let blobs_dir = dir.join("blobs");
        let mut writer = RunWriter::new(dir, blobs_dir, conn, manifest)?;
        writer.next_step = (max_step + 1).max(0) as u64;
        Ok(writer)
    }

    /// Fork `parent_id` into a new run: copy events `[0, at)` verbatim, apply an
    /// override at step `at`, and return an open writer positioned at step `at+1`
    /// ready to record the live tail. Blobs are copied so the fork is
    /// self-contained.
    pub fn fork_from(
        &self,
        parent_id: &RunId,
        at: u64,
        override_resp: Option<&[u8]>,
        override_req: Option<&[u8]>,
    ) -> Result<RunWriter, AgentrrError> {
        let parent = self.open_run(parent_id)?;
        let parent_events = parent.events()?;
        let at_idx = at as usize;
        if at_idx >= parent_events.len() {
            return Err(AgentrrError::Other(format!(
                "fork point {at} out of range (parent has {} events)",
                parent_events.len()
            )));
        }
        let mut manifest = RunManifest::new_fork(*parent_id, Step::new(at))?;
        manifest.provider = parent.manifest().provider.clone();
        let mut writer = self.create_run(manifest)?;
        let src_blobs = parent.blobs_dir().to_path_buf();

        // Prefix [0, at): copy blobs + event rows verbatim.
        for ev in parent_events.iter().take(at_idx) {
            copy_blob(&src_blobs, writer.blobs_dir(), ev.request_blob.as_deref())?;
            copy_blob(&src_blobs, writer.blobs_dir(), ev.response_blob.as_deref())?;
            writer.write_event(ev.clone())?;
        }

        // Override step `at`.
        let mut ov = parent_events[at_idx].clone();
        if let Some(bytes) = override_resp {
            ov.response_blob = Some(write_blob(writer.blobs_dir(), bytes)?);
        }
        if let Some(bytes) = override_req {
            ov.request_blob = Some(write_blob(writer.blobs_dir(), bytes)?);
            let provider = ov
                .meta
                .get("provider")
                .and_then(|v| v.as_str())
                .map(parse_provider)
                .unwrap_or_else(|| Provider::Other("auto".into()));
            let endpoint = ov
                .meta
                .get("endpoint")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            ov.match_key = Some(content_or_match_key(&provider, endpoint, bytes));
        }
        writer.write_event(ov)?;

        // Detach from the parent reader before handing the writer back.
        drop(parent);
        Ok(writer)
    }

    /// Resolve `<RUN_ID|PATH>` against this store, returning a canonical run id.
    pub fn resolve(&self, run: &str) -> Result<RunId, AgentrrError> {
        if let Ok(id) = RunId::from_str(run) {
            return Ok(id);
        }
        // Treat as a path; the directory basename must be a run id.
        let p = Path::new(run);
        let canon = p.canonicalize()?;
        let name = canon
            .file_name()
            .and_then(|s| s.to_str())
            .ok_or_else(|| AgentrrError::InvalidRunId(run.to_string()))?;
        RunId::from_str(name)
    }

    /// List manifests for every run in the store, oldest first (UUIDv7 order).
    pub fn list_runs(&self) -> Result<Vec<RunManifest>, AgentrrError> {
        let mut out = Vec::new();
        if !self.root.exists() {
            return Ok(out);
        }
        for entry in fs::read_dir(&self.root)? {
            let entry = entry?;
            let path = entry.path();
            if !path.is_dir() {
                continue;
            }
            let name = match path.file_name().and_then(|s| s.to_str()) {
                Some(n) => n,
                None => continue,
            };
            if RunId::from_str(name).is_err() {
                continue; // not a run dir
            }
            if let Ok(m) = read_manifest(&path) {
                out.push(m);
            }
        }
        out.sort_by_key(|m| m.id.as_uuid());
        Ok(out)
    }
}

// --------------------------------------------------------------------------- //
// RunWriter
// --------------------------------------------------------------------------- //

/// Append-only handle to a recording run. Drop without [`Self::finalize`] leaves
/// the run un-finalized (manifest without final `event_count`).
pub struct RunWriter {
    dir: PathBuf,
    blobs_dir: PathBuf,
    conn: Connection,
    manifest: RunManifest,
    next_step: u64,
    start_mono: Instant,
    redactor: Redactor,
}

impl RunWriter {
    fn new(
        dir: PathBuf,
        blobs_dir: PathBuf,
        conn: Connection,
        manifest: RunManifest,
    ) -> Result<Self, AgentrrError> {
        Ok(Self {
            dir,
            blobs_dir,
            conn,
            manifest,
            next_step: 0,
            start_mono: Instant::now(),
            redactor: Redactor::default_secrets(),
        })
    }

    pub fn id(&self) -> RunId {
        self.manifest.id
    }

    pub fn manifest(&self) -> &RunManifest {
        &self.manifest
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    pub fn blobs_dir(&self) -> &Path {
        &self.blobs_dir
    }

    /// Replace the active redactor (e.g. with [`Redactor::none`] to disable).
    pub fn set_redactor(&mut self, redactor: Redactor) {
        self.redactor = redactor;
    }

    /// Store `bytes` as a content-addressed blob, returning its BLAKE3 hex.
    /// If `redact`, the default secret-redaction rules are applied first.
    pub fn store_blob(&self, bytes: &[u8], redact: bool) -> Result<String, AgentrrError> {
        let stored = if redact {
            self.redactor.redact_bytes(bytes)?
        } else {
            bytes.to_vec()
        };
        write_blob(&self.blobs_dir, &stored)
    }

    /// Append an event. Assigns the next monotonic step and fills zero
    /// timestamps (wall = now, mono = offset from run start).
    pub fn write_event(&mut self, mut ev: Event) -> Result<Step, AgentrrError> {
        let step = Step::new(self.next_step);
        ev.step = step;
        if ev.ts_wall_ns == 0 {
            ev.ts_wall_ns = unix_nanos();
        }
        if ev.ts_mono_ns == 0 {
            ev.ts_mono_ns = self.start_mono.elapsed().as_nanos() as i64;
        }
        insert_event(&self.conn, &ev)?;
        self.next_step += 1;
        Ok(step)
    }

    /// Finalize: record `event_count`, persist the manifest, checkpoint.
    pub fn finalize(mut self) -> Result<RunManifest, AgentrrError> {
        self.manifest.event_count = self.next_step;
        self.conn
            .execute(
                "INSERT OR REPLACE INTO meta(key,value) VALUES('event_count',?1)",
                params![self.manifest.event_count.to_string()],
            )
            .map_err(sql_err)?;
        write_manifest(&self.dir, &self.manifest)?;
        Ok(self.manifest)
    }
}

// --------------------------------------------------------------------------- //
// RunReader
// --------------------------------------------------------------------------- //

/// Read-only handle to a recorded run.
pub struct RunReader {
    dir: PathBuf,
    blobs_dir: PathBuf,
    manifest: RunManifest,
    conn: Connection,
}

impl RunReader {
    fn new(dir: PathBuf, manifest: RunManifest, conn: Connection) -> Result<Self, AgentrrError> {
        let blobs_dir = dir.join("blobs");
        Ok(Self {
            dir,
            blobs_dir,
            manifest,
            conn,
        })
    }

    pub fn manifest(&self) -> &RunManifest {
        &self.manifest
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    pub fn blobs_dir(&self) -> &Path {
        &self.blobs_dir
    }

    /// Read a blob by BLAKE3 hex.
    pub fn read_blob(&self, hex: &str) -> Result<Vec<u8>, AgentrrError> {
        validate_hex(hex)?;
        let path = self.blobs_dir.join(format!("{hex}.bin"));
        fs::read(&path).map_err(|e| AgentrrError::Other(format!("blob {hex}: {e}")))
    }

    /// Number of events recorded.
    pub fn event_count(&self) -> Result<u64, AgentrrError> {
        let n: i64 = self
            .conn
            .query_row("SELECT COALESCE(MAX(step),-1)+1 FROM events", [], |r| {
                r.get(0)
            })
            .map_err(sql_err)?;
        Ok(n.max(0) as u64)
    }

    /// The event at `step`, if any.
    pub fn event_at(&self, step: u64) -> Result<Option<Event>, AgentrrError> {
        let sql = format!("{SELECT_EVENT_SQL} WHERE step = ?1");
        let mut stmt = self.conn.prepare(&sql).map_err(sql_err)?;
        let mut rows = stmt.query(params![step as i64]).map_err(sql_err)?;
        match rows.next().map_err(sql_err)? {
            Some(row) => Ok(Some(row_to_event(row)?)),
            None => Ok(None),
        }
    }

    /// All events, ordered by step ascending.
    pub fn events(&self) -> Result<Vec<Event>, AgentrrError> {
        let sql = format!("{SELECT_EVENT_SQL} ORDER BY step ASC");
        let mut stmt = self.conn.prepare(&sql).map_err(sql_err)?;
        let mut rows = stmt.query([]).map_err(sql_err)?;
        let mut out = Vec::new();
        while let Some(row) = rows.next().map_err(sql_err)? {
            out.push(row_to_event(row)?);
        }
        Ok(out)
    }

    /// All events matching `match_key`, in step order (used by replay).
    pub fn events_for_key(&self, match_key: &str) -> Result<Vec<Event>, AgentrrError> {
        let sql = format!("{SELECT_EVENT_SQL} WHERE match_key = ?1 ORDER BY step ASC");
        let mut stmt = self.conn.prepare(&sql).map_err(sql_err)?;
        let mut rows = stmt.query(params![match_key]).map_err(sql_err)?;
        let mut out = Vec::new();
        while let Some(row) = rows.next().map_err(sql_err)? {
            out.push(row_to_event(row)?);
        }
        Ok(out)
    }
}

const SELECT_EVENT_SQL: &str = "SELECT step, kind, ts_wall_ns, ts_mono_ns, \
     match_key, request_blob, response_blob, is_stream, meta_json FROM events";

impl std::fmt::Debug for RunReader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RunReader")
            .field("dir", &self.dir)
            .field("manifest", &self.manifest)
            .finish()
    }
}

impl std::fmt::Debug for RunWriter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RunWriter")
            .field("dir", &self.dir)
            .field("manifest", &self.manifest)
            .field("next_step", &self.next_step)
            .finish()
    }
}

fn row_to_event(row: &rusqlite::Row<'_>) -> Result<Event, AgentrrError> {
    let step: i64 = row.get(0).map_err(sql_err)?;
    let kind_s: String = row.get(1).map_err(sql_err)?;
    let ts_wall_ns: i64 = row.get(2).map_err(sql_err)?;
    let ts_mono_ns: i64 = row.get(3).map_err(sql_err)?;
    let match_key: Option<String> = row.get(4).map_err(sql_err)?;
    let request_blob: Option<String> = row.get(5).map_err(sql_err)?;
    let response_blob: Option<String> = row.get(6).map_err(sql_err)?;
    let is_stream: i64 = row.get(7).map_err(sql_err)?;
    let meta_json: Option<String> = row.get(8).map_err(sql_err)?;

    let kind = EventKind::from_str_kind(&kind_s)
        .ok_or_else(|| AgentrrError::Other(format!("unknown event kind in store: {kind_s}")))?;
    let meta = match meta_json {
        Some(s) if !s.is_empty() => serde_json::from_str(&s)?,
        _ => serde_json::Value::Null,
    };

    Ok(Event {
        step: Step::new(step as u64),
        kind,
        ts_wall_ns,
        ts_mono_ns,
        match_key,
        request_blob,
        response_blob,
        is_stream: is_stream != 0,
        meta,
    })
}

// --------------------------------------------------------------------------- //
// Schema + db helpers
// --------------------------------------------------------------------------- //

const SCHEMA_SQL: &str = "\
CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);\n\
CREATE TABLE IF NOT EXISTS events (\n\
  step         INTEGER PRIMARY KEY,\n\
  kind         TEXT NOT NULL,\n\
  ts_wall_ns   INTEGER NOT NULL,\n\
  ts_mono_ns   INTEGER NOT NULL,\n\
  match_key    TEXT,\n\
  request_blob TEXT,\n\
  response_blob TEXT,\n\
  is_stream    INTEGER NOT NULL DEFAULT 0,\n\
  meta_json    TEXT\n\
);\n\
CREATE INDEX IF NOT EXISTS idx_events_matchkey ON events(match_key);\n\
PRAGMA user_version = 1;\n";

fn open_db(path: &Path) -> Result<Connection, AgentrrError> {
    let conn = Connection::open(path).map_err(sql_err)?;
    conn.execute_batch(SCHEMA_SQL).map_err(sql_err)?;
    Ok(conn)
}

fn init_meta(conn: &Connection, m: &RunManifest) -> Result<(), AgentrrError> {
    let put = |k: &str, v: String| {
        conn.execute(
            "INSERT OR REPLACE INTO meta(key,value) VALUES(?1,?2)",
            params![k, v],
        )
        .map_err(sql_err)
    };
    put("schema_version", m.schema_version.to_string())?;
    put("run_id", m.id.to_string())?;
    put("created_at", m.created_at.clone())?;
    Ok(())
}

fn insert_event(conn: &Connection, ev: &Event) -> Result<(), AgentrrError> {
    conn.execute(
        "INSERT INTO events \
         (step, kind, ts_wall_ns, ts_mono_ns, match_key, request_blob, response_blob, is_stream, meta_json) \
         VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9)",
        params![
            ev.step.get() as i64,
            ev.kind.as_str(),
            ev.ts_wall_ns,
            ev.ts_mono_ns,
            ev.match_key,
            ev.request_blob,
            ev.response_blob,
            ev.is_stream as i64,
            ev.meta.to_string(),
        ],
    )
    .map_err(sql_err)?;
    Ok(())
}

fn write_manifest(dir: &Path, m: &RunManifest) -> Result<(), AgentrrError> {
    let json = serde_json::to_string_pretty(m)?;
    fs::write(dir.join("manifest.json"), json)?;
    Ok(())
}

fn read_manifest(dir: &Path) -> Result<RunManifest, AgentrrError> {
    let s = fs::read_to_string(dir.join("manifest.json"))?;
    Ok(serde_json::from_str(&s)?)
}

fn sql_err(e: rusqlite::Error) -> AgentrrError {
    AgentrrError::Sqlite(e.to_string())
}

fn unix_nanos() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as i64)
        .unwrap_or(0)
}

// --------------------------------------------------------------------------- //
// Blob store (content-addressed, BLAKE3)
// --------------------------------------------------------------------------- //

/// Write `bytes` as `blobs/<hex>.bin` if absent; return the hex. Assumes a
/// single writer per run (no concurrency on the same blob dir).
pub fn write_blob(blobs_dir: &Path, bytes: &[u8]) -> Result<String, AgentrrError> {
    let hex = blake3::hash(bytes).to_hex().to_string();
    let path = blobs_dir.join(format!("{hex}.bin"));
    if !path.exists() {
        fs::write(&path, bytes)?;
    }
    Ok(hex)
}

/// Read a blob by BLAKE3 hex directly from a blobs dir.
pub fn read_blob(blobs_dir: &Path, hex: &str) -> Result<Vec<u8>, AgentrrError> {
    validate_hex(hex)?;
    let path = blobs_dir.join(format!("{hex}.bin"));
    fs::read(&path).map_err(|e| AgentrrError::Other(format!("blob {hex}: {e}")))
}

/// Copy a content-addressed blob from `src_blobs` to `dst_blobs` if present.
fn copy_blob(src_blobs: &Path, dst_blobs: &Path, hex: Option<&str>) -> Result<(), AgentrrError> {
    if let Some(hex) = hex {
        validate_hex(hex)?;
        let src = src_blobs.join(format!("{hex}.bin"));
        let dst = dst_blobs.join(format!("{hex}.bin"));
        if src.exists() && !dst.exists() {
            fs::copy(&src, &dst)?;
        }
    }
    Ok(())
}

fn parse_provider(s: &str) -> Provider {
    match s {
        "openai" => Provider::OpenAi,
        "anthropic" => Provider::Anthropic,
        other => Provider::Other(other.to_string()),
    }
}

/// Match key for a request body, falling back to a content hash for non-JSON.
fn content_or_match_key(provider: &Provider, endpoint: &str, body: &[u8]) -> String {
    if let Ok(v) = serde_json::from_slice::<serde_json::Value>(body) {
        match_key(provider, endpoint, &v, MatchMode::Strict)
    } else {
        let mut pre = Vec::new();
        pre.extend_from_slice(provider.as_str().as_bytes());
        pre.push(0);
        pre.extend_from_slice(endpoint.as_bytes());
        pre.push(0);
        pre.extend_from_slice(body);
        blake3::hash(&pre).to_hex().to_string()
    }
}

fn validate_hex(hex: &str) -> Result<(), AgentrrError> {
    let ok = hex.len() == 64 && hex.as_bytes().iter().all(|b| b.is_ascii_hexdigit());
    if !ok {
        return Err(AgentrrError::Other(format!("invalid blob hash: {hex}")));
    }
    Ok(())
}

// --------------------------------------------------------------------------- //
// Redaction
// --------------------------------------------------------------------------- //

/// A set of `(regex, replacement)` rules applied to UTF-8 payloads before they
/// are written to the blob store. Used so that **stored** blobs are scrubbed even
/// though the replay `match_key` is computed on the *unredacted* request (see
/// `DECISIONS.md` D0004).
#[derive(Clone)]
pub struct Redactor {
    rules: Vec<(Regex, String)>,
}

impl Redactor {
    /// No rules — store payloads verbatim.
    pub fn none() -> Self {
        Self { rules: Vec::new() }
    }

    /// Built-in patterns for common bearer/API-key shapes (OpenAI `sk-…`,
    /// Anthropic `sk-ant-…`, `Bearer …`).
    pub fn default_secrets() -> Self {
        let mut r = Self::none();
        r.add(
            Regex::new(r"sk-ant-[A-Za-z0-9_\-]{20,}").unwrap(),
            "[REDACTED]",
        );
        r.add(Regex::new(r"sk-[A-Za-z0-9_\-]{20,}").unwrap(), "[REDACTED]");
        r.add(
            Regex::new(r"(?i)bearer[ \t]+[A-Za-z0-9_\-\.=]+").unwrap(),
            "bearer [REDACTED]",
        );
        r
    }

    /// Add a rule.
    pub fn add(&mut self, re: Regex, replacement: &str) {
        self.rules.push((re, replacement.to_string()));
    }

    /// Redact a string.
    pub fn redact_str(&self, s: &str) -> String {
        let mut out = s.to_string();
        for (re, repl) in &self.rules {
            out = re.replace_all(&out, repl.as_str()).into_owned();
        }
        out
    }

    /// Redact UTF-8 bytes. Non-UTF-8 payloads are returned unchanged (we don't
    /// pattern-match binary bodies in v0.1).
    pub fn redact_bytes(&self, input: &[u8]) -> Result<Vec<u8>, AgentrrError> {
        match std::str::from_utf8(input) {
            Ok(s) => Ok(self.redact_str(s).into_bytes()),
            Err(_) => Ok(input.to_vec()),
        }
    }
}

impl Default for Redactor {
    fn default() -> Self {
        Self::default_secrets()
    }
}

// --------------------------------------------------------------------------- //
// Verification (offline determinism self-check)
// --------------------------------------------------------------------------- //

/// Summary of a [`verify_run`] pass.
#[derive(Debug, Clone)]
pub struct VerifyReport {
    pub events: usize,
    pub keys: usize,
}

/// Verify a run is internally deterministic: every response blob hashes back to
/// its content address, and every recorded request maps — through a fresh
/// [`ReplayCursor`] — to itself in FIFO order. Returns [`AgentrrError::Verify`]
/// on mismatch (the CLI maps this to exit code 3).
pub fn verify_run(reader: &RunReader) -> Result<VerifyReport, AgentrrError> {
    let events = reader.events()?;
    let mut cursor = ReplayCursor::new();
    let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();

    for ev in &events {
        // Blob integrity: the stored bytes must hash back to their address.
        if let Some(hex) = &ev.response_blob {
            let bytes = reader.read_blob(hex)?;
            let actual = blake3::hash(&bytes).to_hex().to_string();
            if actual != *hex {
                return Err(AgentrrError::Verify {
                    step: ev.step.get(),
                    detail: format!("response blob {hex} corrupted (recomputed {actual})"),
                });
            }
        }

        // FIFO self-consistency: the k-th occurrence of a key must map to itself.
        if let Some(key) = &ev.match_key {
            seen.insert(key.clone());
            let recorded = reader.events_for_key(key)?;
            match cursor.next_index(key, recorded.len()) {
                Some(i) => {
                    if recorded[i].step != ev.step {
                        return Err(AgentrrError::Verify {
                            step: ev.step.get(),
                            detail: format!(
                                "FIFO misorder: key {} occurrence mapped to step {} (expected {})",
                                &key[..key.len().min(12)],
                                recorded[i].step.get(),
                                ev.step.get()
                            ),
                        });
                    }
                }
                None => return Err(AgentrrError::CacheMiss(key.clone())),
            }
        }
    }

    Ok(VerifyReport {
        events: events.len(),
        keys: seen.len(),
    })
}

// --------------------------------------------------------------------------- //
// Diff
// --------------------------------------------------------------------------- //

/// One step in a structural [`diff_runs`] comparison.
#[derive(Debug, Clone, Serialize)]
pub struct StepDiff {
    pub step: u64,
    pub identical: bool,
    pub note: String,
}

/// Structural diff of two runs by step index.
#[derive(Debug, Clone, Serialize)]
pub struct RunDiff {
    pub identical: bool,
    pub a_count: u64,
    pub b_count: u64,
    pub steps: Vec<StepDiff>,
}

/// Compare two runs step-by-step (kind, match_key, request/response blobs).
pub fn diff_runs(a: &RunReader, b: &RunReader) -> Result<RunDiff, AgentrrError> {
    let ea = a.events()?;
    let eb = b.events()?;
    let max = ea.len().max(eb.len());
    let mut steps = Vec::with_capacity(max);
    let mut identical = ea.len() == eb.len();
    for i in 0..max {
        let step = i as u64;
        match (ea.get(i), eb.get(i)) {
            (Some(x), Some(y)) => {
                let mut notes = Vec::new();
                if x.kind != y.kind {
                    notes.push(format!("kind {} vs {}", x.kind.as_str(), y.kind.as_str()));
                }
                if x.match_key != y.match_key {
                    notes.push("match_key".into());
                }
                if x.request_blob != y.request_blob {
                    notes.push("request_blob".into());
                }
                if x.response_blob != y.response_blob {
                    notes.push("response_blob".into());
                }
                let same = notes.is_empty();
                if !same {
                    identical = false;
                }
                steps.push(StepDiff {
                    step,
                    identical: same,
                    note: notes.join(", "),
                });
            }
            (Some(x), None) => {
                identical = false;
                steps.push(StepDiff {
                    step,
                    identical: false,
                    note: format!("only in A: {}", x.kind.as_str()),
                });
            }
            (None, Some(y)) => {
                identical = false;
                steps.push(StepDiff {
                    step,
                    identical: false,
                    note: format!("only in B: {}", y.kind.as_str()),
                });
            }
            (None, None) => {}
        }
    }
    Ok(RunDiff {
        identical,
        a_count: ea.len() as u64,
        b_count: eb.len() as u64,
        steps,
    })
}

// --------------------------------------------------------------------------- //
// Tests
// --------------------------------------------------------------------------- //

#[cfg(test)]
mod tests {
    use super::*;
    use agentrr_core::EventKind;
    use tempfile::TempDir;

    fn fresh_store() -> (TempDir, Store) {
        let td = TempDir::new().unwrap();
        let store = Store::open(td.path()).unwrap();
        (td, store)
    }

    fn sample_event(kind: EventKind, match_key: Option<&str>) -> Event {
        Event {
            step: Step::new(0),
            kind,
            ts_wall_ns: 0,
            ts_mono_ns: 0,
            match_key: match_key.map(str::to_string),
            request_blob: None,
            response_blob: None,
            is_stream: false,
            meta: serde_json::json!({"model": "gpt-x"}),
        }
    }

    #[test]
    fn round_trip_events_and_blobs() {
        let (_td, store) = fresh_store();
        let manifest = RunManifest::new().unwrap();
        let id = manifest.id;

        let mut w = store.create_run(manifest).unwrap();
        let req = w.store_blob(b"{\"hi\":1}", false).unwrap();
        let resp = w.store_blob(b"hello world", false).unwrap();
        let s0 = w
            .write_event(sample_event(EventKind::LlmCompletion, Some("mk1")))
            .unwrap();
        // attach blobs retroactively by writing another event with them:
        let mut ev1 = sample_event(EventKind::ToolCall, Some("mk2"));
        ev1.request_blob = Some(req.clone());
        ev1.response_blob = Some(resp.clone());
        w.write_event(ev1.clone()).unwrap();
        w.write_event(sample_event(EventKind::Clock, None)).unwrap();
        let m = w.finalize().unwrap();
        assert_eq!(m.event_count, 3);
        assert_eq!(s0.get(), 0);

        let r = store.open_run(&id).unwrap();
        assert_eq!(r.event_count().unwrap(), 3);
        let events = r.events().unwrap();
        assert_eq!(events.len(), 3);
        assert_eq!(events[0].kind, EventKind::LlmCompletion);
        assert_eq!(events[1].request_blob.as_deref(), Some(req.as_str()));
        assert_eq!(r.read_blob(&resp).unwrap(), b"hello world");
    }

    #[test]
    fn blob_dedupe() {
        let (_td, store) = fresh_store();
        let manifest = RunManifest::new().unwrap();
        let w = store.create_run(manifest).unwrap();
        let h1 = w.store_blob(b"same", false).unwrap();
        let h2 = w.store_blob(b"same", false).unwrap();
        let h3 = w.store_blob(b"different", false).unwrap();
        assert_eq!(h1, h2);
        assert_ne!(h1, h3);
        let count = fs::read_dir(w.blobs_dir()).unwrap().count();
        assert_eq!(count, 2);
    }

    #[test]
    fn schema_version_mismatch_rejected() {
        let (_td, store) = fresh_store();
        let mut m = RunManifest::new().unwrap();
        m.schema_version = 99;
        let id = m.id;
        let w = store.create_run(m).unwrap();
        w.finalize().unwrap();
        let err = store.open_run(&id).unwrap_err();
        match err {
            AgentrrError::SchemaVersion {
                store: 99,
                supported,
            } => {
                assert_eq!(supported, SCHEMA_VERSION);
            }
            other => panic!("expected SchemaVersion, got {other:?}"),
        }
    }

    #[test]
    fn redaction_scrubs_keys_in_stored_blob() {
        let (_td, store) = fresh_store();
        let m = RunManifest::new().unwrap();
        let w = store.create_run(m).unwrap();
        let body = b"{\"auth\":\"Bearer abc123\", \"key\":\"sk-01234567890123456789abc\"}";
        let hex = w.store_blob(body, true).unwrap();
        let stored = w.blobs_dir().join(format!("{hex}.bin"));
        let on_disk = fs::read(&stored).unwrap();
        assert!(
            !String::from_utf8_lossy(&on_disk).contains("abc123"),
            "bearer token leaked into stored blob"
        );
        assert!(
            String::from_utf8_lossy(&on_disk).contains("[REDACTED]"),
            "redaction marker missing"
        );
        // And the raw redactor is deterministic + idempotent-ish.
        let r = Redactor::default_secrets();
        assert!(r
            .redact_str("sk-01234567890123456789abc")
            .contains("[REDACTED]"));
    }

    #[test]
    fn list_runs_sorted() {
        let (_td, store) = fresh_store();
        let a = store
            .create_run(RunManifest::new().unwrap())
            .unwrap()
            .finalize()
            .unwrap();
        let b = store
            .create_run(RunManifest::new().unwrap())
            .unwrap()
            .finalize()
            .unwrap();
        let runs = store.list_runs().unwrap();
        assert_eq!(runs.len(), 2);
        assert!(a.id.as_uuid() <= b.id.as_uuid());
        assert_eq!(runs[0].id, a.id);
        assert_eq!(runs[1].id, b.id);
    }

    #[test]
    fn resolve_accepts_id_and_path() {
        let (_td, store) = fresh_store();
        let m = store
            .create_run(RunManifest::new().unwrap())
            .unwrap()
            .finalize()
            .unwrap();
        let by_id = store.resolve(&m.id.to_string()).unwrap();
        assert_eq!(by_id, m.id);
        let by_path = store
            .resolve(&store.run_dir(&m.id).to_string_lossy())
            .unwrap();
        assert_eq!(by_path, m.id);
    }

    #[test]
    fn open_missing_run_errors() {
        let (_td, store) = fresh_store();
        let id = RunId::new();
        match store.open_run(&id).unwrap_err() {
            AgentrrError::RunNotFound(_) => {}
            other => panic!("expected RunNotFound, got {other:?}"),
        }
    }

    fn seed_parent(store: &Store, n: u64) -> RunId {
        let manifest = RunManifest::new().unwrap();
        let id = manifest.id;
        let mut w = store.create_run(manifest).unwrap();
        for i in 0..n {
            let resp = w.store_blob(format!("resp{i}").as_bytes(), false).unwrap();
            let mut ev = sample_event(EventKind::LlmCompletion, Some(&format!("key{i}")));
            ev.response_blob = Some(resp);
            ev.meta = serde_json::json!({"endpoint":"/e","provider":"openai"});
            w.write_event(ev).unwrap();
        }
        w.finalize().unwrap();
        id
    }

    #[test]
    fn fork_shares_prefix_and_diverges_at_override() {
        let (_td, store) = fresh_store();
        let parent_id = seed_parent(&store, 3);

        let fw = store
            .fork_from(&parent_id, 1, Some(b"OVERRIDDEN"), None)
            .unwrap();
        let fork_id = fw.id();
        fw.finalize().unwrap();

        let pr = store.open_run(&parent_id).unwrap();
        let fr = store.open_run(&fork_id).unwrap();
        let pe = pr.events().unwrap();
        let fe = fr.events().unwrap();

        assert_eq!(fe.len(), 2, "fork = prefix[0] + override[1]");
        // step 0: identical prefix
        assert_eq!(fe[0].match_key, pe[0].match_key);
        assert_eq!(fe[0].response_blob, pe[0].response_blob);
        // step 1: same key (replay alignment), divergent response
        assert_eq!(fe[1].match_key, pe[1].match_key);
        assert_ne!(fe[1].response_blob, pe[1].response_blob);
        assert_eq!(
            &fr.read_blob(fe[1].response_blob.as_ref().unwrap()).unwrap(),
            b"OVERRIDDEN"
        );
        // manifest records lineage
        assert_eq!(fr.manifest().parent, Some(parent_id));
        assert_eq!(fr.manifest().fork_at, Some(Step::new(1)));
        // the fork is itself replayable + verifiable
        let report = verify_run(&fr).unwrap();
        assert_eq!(report.events, 2);
    }

    #[test]
    fn fork_request_override_recomputes_match_key() {
        let (_td, store) = fresh_store();
        let parent_id = seed_parent(&store, 2);
        // Replace step 1's request body; match_key should change.
        let new_body = br#"{"model":"gpt","messages":[{"role":"user","content":"new"}]}"#;
        let fw = store
            .fork_from(&parent_id, 1, None, Some(new_body))
            .unwrap();
        let fork_id = fw.id();
        fw.finalize().unwrap();

        let pr = store.open_run(&parent_id).unwrap();
        let fr = store.open_run(&fork_id).unwrap();
        let pe = pr.events().unwrap();
        let fe = fr.events().unwrap();
        assert_eq!(fe[0].match_key, pe[0].match_key);
        assert_ne!(
            fe[1].match_key, pe[1].match_key,
            "request override must change the key"
        );
    }

    #[test]
    fn diff_runs_detects_changes() {
        let (_td, store) = fresh_store();
        let parent_id = seed_parent(&store, 2);
        let fw = store.fork_from(&parent_id, 1, Some(b"NEW"), None).unwrap();
        let fork_id = fw.id();
        fw.finalize().unwrap();

        let a = store.open_run(&parent_id).unwrap();
        let b = store.open_run(&fork_id).unwrap();
        assert!(diff_runs(&a, &a).unwrap().identical);
        let d = diff_runs(&a, &b).unwrap();
        assert!(!d.identical);
        assert!(d.steps[0].identical);
        assert!(!d.steps[1].identical);
        assert!(d.steps[1].note.contains("response_blob"));
    }
}
