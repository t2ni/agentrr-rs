//! Minimal agent loop recorded with the `agentrr` SDK, then replayed
//! deterministically. Run with: `cargo run -p agentrr-sdk --example agent_loop`.

#![forbid(unsafe_code)]

use agentrr_sdk::agentrr_store::Store;
use agentrr_sdk::{Recorder, Replayer};
use tempfile::TempDir;

/// A toy agent "step": a deterministic function of two *non-deterministic*
/// inputs — a clock reading and an RNG draw.
fn agent_step(clock_nanos: u64, rng: u64) -> String {
    format!("decided@{clock_nanos} with luck={rng}")
}

fn main() {
    let td = TempDir::new().unwrap();
    let store = Store::open(td.path()).unwrap();

    // ---- record pass ----
    let mut rec = Recorder::start(&store, Some("demo-loop")).unwrap();
    let t = rec.clock().unwrap();
    let r = rec.random(42).unwrap();
    let decision = agent_step(t, r);
    rec.tool("act", b"do-the-thing", decision.as_bytes())
        .unwrap();
    let manifest = rec.finalize().unwrap();

    // ---- replay pass (offline, deterministic) ----
    let mut rep = Replayer::open(&store, &manifest.id).unwrap();
    let t2 = rep.clock().unwrap();
    let r2 = rep.random().unwrap();
    let (_args, result) = rep.tool().unwrap();
    let decision2 = agent_step(t2, r2);

    assert_eq!(t, t2, "clock reading must reproduce");
    assert_eq!(r, r2, "random draw must reproduce");
    assert_eq!(decision, decision2, "agent decision must be deterministic");
    assert_eq!(decision.as_bytes(), &result[..], "tool result must match");
    assert!(rep.done());

    println!("deterministic replay OK: {decision2}");
    println!("run: {}", manifest.id);
}
