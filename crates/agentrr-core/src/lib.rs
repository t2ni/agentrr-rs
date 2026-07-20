//! Pure domain types for `agentrr`.
//!
//! Vocabulary shared across crates: [`RunId`], [`EventKind`], [`Event`], [`Step`],
//! [`RunManifest`], and the common [`AgentrrError`] enum. **No I/O, no async** —
//! trivially unit-testable.

#![forbid(unsafe_code)]

use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

/// On-disk schema version. Bump on any breaking change to `events.sqlite` or the
/// manifest layout. `agentrr-store` rejects mismatched versions on open/import.
pub const SCHEMA_VERSION: u32 = 1;

/// Current `agentrr` crate version, recorded in manifests for compat checks.
pub const TOOL_VERSION: &str = env!("CARGO_PKG_VERSION");

/// A run id. UUIDv7, so directory listings are naturally time-ordered.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct RunId(Uuid);

impl RunId {
    /// Mint a fresh time-ordered id.
    pub fn new() -> Self {
        Self(Uuid::now_v7())
    }

    /// Wrap an existing [`Uuid`].
    pub fn from_uuid(u: Uuid) -> Self {
        Self(u)
    }

    /// The inner [`Uuid`].
    pub fn as_uuid(&self) -> Uuid {
        self.0
    }
}

impl Default for RunId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for RunId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Hyphenated lowercase — also the on-disk directory name.
        fmt::Display::fmt(&self.0, f)
    }
}

impl FromStr for RunId {
    type Err = AgentrrError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Uuid::parse_str(s)
            .map(Self)
            .map_err(|e| AgentrrError::InvalidRunId(format!("{s}: {e}")))
    }
}

/// The kind of non-deterministic boundary an [`Event`] records.
///
/// Stored verbatim as the SQLite `kind` column text (see [`EventKind::as_str`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub enum EventKind {
    /// LLM completion: request + response (streaming or not).
    LlmCompletion,
    /// A tool/function call captured through the proxy: name, args, result.
    ToolCall,
    /// A wall/monotonic clock reading.
    Clock,
    /// A random seed or draw.
    Random,
    /// An environment-variable read.
    Env,
}

impl EventKind {
    /// Stable on-disk name (matches spec §6).
    pub fn as_str(self) -> &'static str {
        match self {
            EventKind::LlmCompletion => "LlmCompletion",
            EventKind::ToolCall => "ToolCall",
            EventKind::Clock => "Clock",
            EventKind::Random => "Random",
            EventKind::Env => "Env",
        }
    }

    /// Parse the stable on-disk name.
    pub fn from_str_kind(s: &str) -> Option<Self> {
        Some(match s {
            "LlmCompletion" => EventKind::LlmCompletion,
            "ToolCall" => EventKind::ToolCall,
            "Clock" => EventKind::Clock,
            "Random" => EventKind::Random,
            "Env" => EventKind::Env,
            _ => return None,
        })
    }
}

/// The monotonic index of an event within a run (`u64`, 0-based).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Step(pub u64);

impl Step {
    pub const fn new(n: u64) -> Self {
        Self(n)
    }
    pub const fn get(self) -> u64 {
        self.0
    }
    pub fn next(self) -> Self {
        Self(self.0 + 1)
    }
}

/// A single recorded interaction at a non-deterministic boundary.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Event {
    pub step: Step,
    pub kind: EventKind,
    /// Wall clock nanoseconds since UNIX epoch (display only).
    pub ts_wall_ns: i64,
    /// Monotonic offset from run start, nanoseconds.
    pub ts_mono_ns: i64,
    /// Canonical hash used to align live requests during replay.
    /// `None` for `Clock`/`Random`/`Env` (no replay alignment).
    pub match_key: Option<String>,
    /// BLAKE3 hex of the (redacted) request payload, if any.
    pub request_blob: Option<String>,
    /// BLAKE3 hex of the (redacted) response payload, if any.
    pub response_blob: Option<String>,
    /// Whether this event captured a streaming (`text/event-stream`) response.
    pub is_stream: bool,
    /// Small structured extras (model, tokens, latency_ms, chunk timings, …).
    #[serde(default)]
    pub meta: serde_json::Value,
}

/// Top-level run descriptor, written to `manifest.json`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunManifest {
    pub schema_version: u32,
    pub id: RunId,
    /// RFC3339 creation timestamp.
    pub created_at: String,
    /// `agentrr` version that produced this run.
    pub tool_version: String,
    /// Detected/given provider (`openai`, `anthropic`, …). `None` until known.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider: Option<String>,
    /// Optional human label.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// Parent run, for forks.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent: Option<RunId>,
    /// Fork point in the parent, for forks.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fork_at: Option<Step>,
    /// Total recorded events. Finalized on `RunWriter::finalize`.
    #[serde(default)]
    pub event_count: u64,
}

impl RunManifest {
    /// Build a fresh manifest for a new run (UUIDv7 id, now-UTC created_at).
    pub fn new() -> Result<Self, AgentrrError> {
        use time::format_description::well_known::Rfc3339;
        let created_at = time::OffsetDateTime::now_utc()
            .format(&Rfc3339)
            .map_err(|e| AgentrrError::Other(format!("time format: {e}")))?;
        Ok(Self {
            schema_version: SCHEMA_VERSION,
            id: RunId::new(),
            created_at,
            tool_version: TOOL_VERSION.to_string(),
            provider: None,
            name: None,
            parent: None,
            fork_at: None,
            event_count: 0,
        })
    }

    /// Build a manifest for a fork of `parent`, breaking at `fork_at`.
    pub fn new_fork(parent: RunId, fork_at: Step) -> Result<Self, AgentrrError> {
        let mut m = Self::new()?;
        m.parent = Some(parent);
        m.fork_at = Some(fork_at);
        Ok(m)
    }
}

impl Default for RunManifest {
    fn default() -> Self {
        Self::new().expect("now_utc and RFC3339 formatting are infallible in practice")
    }
}

/// The single error type all fallible library functions return.
#[derive(Debug, Error)]
pub enum AgentrrError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("sqlite: {0}")]
    Sqlite(String),
    #[error("json: {0}")]
    Json(#[from] serde_json::Error),
    #[error("invalid run id: {0}")]
    InvalidRunId(String),
    #[error("run not found: {0}")]
    RunNotFound(RunId),
    #[error("schema version mismatch: store has {store}, tool supports {supported}")]
    SchemaVersion { store: u32, supported: u32 },
    #[error("cache miss in strict replay (match_key={0})")]
    CacheMiss(String),
    #[error("verification failed at step {step}: {detail}")]
    Verify { step: u64, detail: String },
    #[error("redaction: {0}")]
    Redaction(String),
    #[error("{0}")]
    Other(String),
}

// Re-export so downstream crates can construct without a direct `uuid` dep path.
pub use uuid;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn run_id_roundtrips() {
        let id = RunId::new();
        let s = id.to_string();
        let back: RunId = s.parse().unwrap();
        assert_eq!(id, back);
    }

    #[test]
    fn run_id_rejects_garbage() {
        assert!("not-a-uuid".parse::<RunId>().is_err());
    }

    #[test]
    fn event_kind_roundtrips() {
        for k in [
            EventKind::LlmCompletion,
            EventKind::ToolCall,
            EventKind::Clock,
            EventKind::Random,
            EventKind::Env,
        ] {
            assert_eq!(EventKind::from_str_kind(k.as_str()), Some(k));
        }
        assert_eq!(EventKind::from_str_kind("nope"), None);
    }

    #[test]
    fn manifest_new_has_current_schema() {
        let m = RunManifest::new().unwrap();
        assert_eq!(m.schema_version, SCHEMA_VERSION);
        assert_eq!(m.tool_version, TOOL_VERSION);
        assert!(m.parent.is_none());
        // created_at parses as RFC3339.
        use time::format_description::well_known::Rfc3339;
        time::OffsetDateTime::parse(&m.created_at, &Rfc3339).unwrap();
    }

    #[test]
    fn manifest_fork_records_parent_and_point() {
        let parent = RunId::new();
        let m = RunManifest::new_fork(parent, Step::new(3)).unwrap();
        assert_eq!(m.parent, Some(parent));
        assert_eq!(m.fork_at, Some(Step::new(3)));
    }

    #[test]
    fn event_serializes_with_snake_meta() {
        let ev = Event {
            step: Step::new(1),
            kind: EventKind::LlmCompletion,
            ts_wall_ns: 1,
            ts_mono_ns: 2,
            match_key: Some("abc".into()),
            request_blob: None,
            response_blob: None,
            is_stream: false,
            meta: serde_json::json!({"model": "gpt-x"}),
        };
        let v = serde_json::to_value(&ev).unwrap();
        assert_eq!(v["step"], 1);
        assert_eq!(v["kind"], "LlmCompletion");
    }
}
