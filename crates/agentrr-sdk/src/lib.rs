//! Optional in-process Rust SDK for `agentrr`.
//!
//! For agent loops that don't go through the HTTP proxy: capture non-determinism
//! (clock readings, RNG draws, tool results) in step order as a "cassette", then
//! replay the same steps deterministically. [`Recorder`] writes; [`Replayer`]
//! reads back, asserting the agent makes the same calls in the same order.
//!
//! ```text
//! let mut rec = Recorder::start(&store, Some("demo"))?;
//! let t = rec.clock()?;          // record wall-clock nanos
//! let r = rec.random(draw)?;     // record an RNG draw
//! rec.tool("search", args, res)?; // record a tool call + result
//! let manifest = rec.finalize()?;
//!
//! let mut rep = Replayer::open(&store, &manifest.id)?;
//! assert_eq!(rep.clock()?, t);
//! ```

#![forbid(unsafe_code)]

use std::time::{SystemTime, UNIX_EPOCH};

use agentrr_core::{AgentrrError, Event, EventKind, RunId, RunManifest, Step};
use agentrr_store::{RunReader, RunWriter, Store};

pub use agentrr_core;
pub use agentrr_store;

/// Record-side guard: appends Clock/Random/ToolCall events to a new run.
pub struct Recorder {
    w: RunWriter,
}

/// Replay-side guard: walks recorded events in step order.
pub struct Replayer {
    r: RunReader,
    events: Vec<Event>,
    idx: usize,
}

fn unix_nanos() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

fn event(kind: EventKind, meta: serde_json::Value) -> Event {
    let now = unix_nanos();
    Event {
        step: Step::new(0),
        kind,
        ts_wall_ns: now as i64,
        ts_mono_ns: 0,
        match_key: None,
        request_blob: None,
        response_blob: None,
        is_stream: false,
        meta,
    }
}

impl Recorder {
    /// Create a new run and begin recording into it.
    pub fn start(store: &Store, name: Option<&str>) -> Result<Self, AgentrrError> {
        let mut manifest = RunManifest::new()?;
        manifest.name = name.map(str::to_string);
        manifest.provider = Some("sdk".into());
        Ok(Self {
            w: store.create_run(manifest)?,
        })
    }

    /// Record the current wall-clock time (nanos since UNIX epoch) and return it.
    pub fn clock(&mut self) -> Result<u64, AgentrrError> {
        let nanos = unix_nanos();
        let mut ev = event(EventKind::Clock, serde_json::json!({ "nanos": nanos }));
        ev.ts_wall_ns = nanos as i64;
        self.w.write_event(ev)?;
        Ok(nanos)
    }

    /// Record a random draw `value` (the caller owns the RNG) and return it.
    pub fn random(&mut self, value: u64) -> Result<u64, AgentrrError> {
        let ev = event(EventKind::Random, serde_json::json!({ "value": value }));
        self.w.write_event(ev)?;
        Ok(value)
    }

    /// Record a tool call: `name` + `args` (redacted blob) and `result` (verbatim).
    pub fn tool(&mut self, name: &str, args: &[u8], result: &[u8]) -> Result<(), AgentrrError> {
        let request_blob = self.w.store_blob(args, true)?;
        let response_blob = self.w.store_blob(result, false)?;
        let mut ev = event(EventKind::ToolCall, serde_json::json!({ "tool": name }));
        ev.request_blob = Some(request_blob);
        ev.response_blob = Some(response_blob);
        self.w.write_event(ev)?;
        Ok(())
    }

    /// Finalize the run and return its manifest.
    pub fn finalize(self) -> Result<RunManifest, AgentrrError> {
        self.w.finalize()
    }
}

impl Replayer {
    /// Open a recorded run for step-ordered replay.
    pub fn open(store: &Store, id: &RunId) -> Result<Self, AgentrrError> {
        let r = store.open_run(id)?;
        let events = r.events()?;
        Ok(Self { r, events, idx: 0 })
    }

    fn expect(&mut self, kind: EventKind) -> Result<Event, AgentrrError> {
        let ev = self
            .events
            .get(self.idx)
            .ok_or_else(|| AgentrrError::Other("replay cassette exhausted".into()))?;
        if ev.kind != kind {
            return Err(AgentrrError::Other(format!(
                "replay order mismatch at step {}: expected {}, got {}",
                self.idx,
                kind.as_str(),
                ev.kind.as_str()
            )));
        }
        let ev = ev.clone();
        self.idx += 1;
        Ok(ev)
    }

    /// Replay the next recorded clock reading.
    pub fn clock(&mut self) -> Result<u64, AgentrrError> {
        let ev = self.expect(EventKind::Clock)?;
        ev.meta
            .get("nanos")
            .and_then(|v| v.as_u64())
            .ok_or_else(|| AgentrrError::Other("clock event missing nanos".into()))
    }

    /// Replay the next recorded random draw.
    pub fn random(&mut self) -> Result<u64, AgentrrError> {
        let ev = self.expect(EventKind::Random)?;
        ev.meta
            .get("value")
            .and_then(|v| v.as_u64())
            .ok_or_else(|| AgentrrError::Other("random event missing value".into()))
    }

    /// Replay the next recorded tool call, returning `(args, result)` bytes.
    pub fn tool(&mut self) -> Result<(Vec<u8>, Vec<u8>), AgentrrError> {
        let ev = self.expect(EventKind::ToolCall)?;
        let args = ev
            .request_blob
            .as_ref()
            .and_then(|h| self.r.read_blob(h).ok())
            .unwrap_or_default();
        let result = ev
            .response_blob
            .as_ref()
            .and_then(|h| self.r.read_blob(h).ok())
            .unwrap_or_default();
        Ok((args, result))
    }

    /// Whether every recorded step has been consumed.
    pub fn done(&self) -> bool {
        self.idx >= self.events.len()
    }
}

/// Record a non-deterministic value.
///
/// - `record!(rec, clock)` → `rec.clock()`
/// - `record!(rec, random, v)` → `rec.random(v)`
#[macro_export]
macro_rules! record {
    ($rec:expr, clock) => {
        $rec.clock()
    };
    ($rec:expr, random, $v:expr) => {
        $rec.random($v)
    };
}

/// Replay a recorded value.
///
/// - `replay!(rep, clock)` → `rep.clock()`
/// - `replay!(rep, random)` → `rep.random()`
#[macro_export]
macro_rules! replay {
    ($rep:expr, clock) => {
        $rep.clock()
    };
    ($rep:expr, random) => {
        $rep.random()
    };
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn record_replay_deterministic() {
        let td = TempDir::new().unwrap();
        let store = Store::open(td.path()).unwrap();

        let mut rec = Recorder::start(&store, Some("t")).unwrap();
        let t = rec.clock().unwrap();
        let r = rec.random(42).unwrap();
        rec.tool("think", b"query", b"answer").unwrap();
        let m = rec.finalize().unwrap();

        let mut rep = Replayer::open(&store, &m.id).unwrap();
        assert_eq!(rep.clock().unwrap(), t);
        assert_eq!(rep.random().unwrap(), r);
        let (args, res) = rep.tool().unwrap();
        assert_eq!(args, b"query");
        assert_eq!(res, b"answer");
        assert!(rep.done());
    }

    #[test]
    fn replay_order_mismatch_errors() {
        let td = TempDir::new().unwrap();
        let store = Store::open(td.path()).unwrap();
        let mut rec = Recorder::start(&store, None).unwrap();
        rec.clock().unwrap();
        let m = rec.finalize().unwrap();

        let mut rep = Replayer::open(&store, &m.id).unwrap();
        // Expecting a Random but the cassette has a Clock → order mismatch.
        assert!(rep.random().is_err());
    }
}
