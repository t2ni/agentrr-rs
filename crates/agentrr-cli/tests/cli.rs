//! End-to-end CLI smoke: create a run via the store library, then drive the
//! `agentrr` binary against it. This is the M1 acceptance path.

#![forbid(unsafe_code)]

use std::process::Command;

use agentrr_core::{Event, EventKind, RunManifest, Step};
use agentrr_store::Store;
use assert_cmd::prelude::*;
use tempfile::TempDir;

fn seed_run(store_root: &std::path::Path) -> RunManifest {
    let store = Store::open(store_root).unwrap();
    let manifest = RunManifest::new().unwrap();
    let mut w = store.create_run(manifest.clone()).unwrap();
    let req = w
        .store_blob(br#"{"model":"gpt-x","messages":[]}"#, false)
        .unwrap();
    let resp = w.store_blob(br#"{"choices":[]}"#, false).unwrap();
    let mut ev = Event {
        step: Step::new(0),
        kind: EventKind::LlmCompletion,
        ts_wall_ns: 0,
        ts_mono_ns: 0,
        match_key: Some("deadbeef".repeat(8)),
        request_blob: Some(req),
        response_blob: Some(resp),
        is_stream: false,
        meta: serde_json::json!({"model": "gpt-x"}),
    };
    w.write_event(ev.clone()).unwrap();
    ev.kind = EventKind::Clock;
    ev.request_blob = None;
    ev.response_blob = None;
    ev.match_key = None;
    w.write_event(ev).unwrap();
    w.finalize().unwrap()
}

fn agentrr(store: &std::path::Path) -> Command {
    let mut c = Command::cargo_bin("agentrr").unwrap();
    c.arg("--store").arg(store);
    c
}

#[test]
fn ls_and_steps_and_show() {
    let td = TempDir::new().unwrap();
    let m = seed_run(td.path());

    // ls --json lists the run.
    agentrr(td.path())
        .arg("ls")
        .arg("--json")
        .assert()
        .success()
        .stdout(predicates::str::contains(m.id.to_string()));

    // steps --json shows two events.
    let steps = agentrr(td.path())
        .arg("steps")
        .arg("--run")
        .arg(m.id.to_string())
        .arg("--json")
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let parsed: Vec<serde_json::Value> = serde_json::from_slice(&steps).unwrap();
    assert_eq!(parsed.len(), 2);
    assert_eq!(parsed[0]["kind"], "LlmCompletion");
    assert_eq!(parsed[1]["kind"], "Clock");

    // show --step 0 prints the event's kind.
    agentrr(td.path())
        .arg("show")
        .arg("--run")
        .arg(m.id.to_string())
        .arg("--step")
        .arg("0")
        .assert()
        .success()
        .stdout(predicates::str::contains("LlmCompletion"));

    // show (summary) prints the run id.
    agentrr(td.path())
        .arg("show")
        .arg("--run")
        .arg(m.id.to_string())
        .assert()
        .success()
        .stdout(predicates::str::contains("events    2"));
}

#[test]
fn ls_empty_store_message() {
    let td = TempDir::new().unwrap();
    Store::open(td.path()).unwrap();
    agentrr(td.path())
        .arg("ls")
        .assert()
        .success()
        .stdout(predicates::str::contains("no runs"));
}

#[test]
fn verify_exits_zero_on_good_run() {
    let td = TempDir::new().unwrap();
    let m = seed_run(td.path());
    agentrr(td.path())
        .arg("verify")
        .arg("--run")
        .arg(m.id.to_string())
        .assert()
        .success();
}

#[test]
fn verify_exits_3_on_corrupted_blob() {
    let td = TempDir::new().unwrap();
    let m = seed_run(td.path());

    // Corrupt the recorded response blob for step 0.
    let store = Store::open(td.path()).unwrap();
    let reader = store.open_run(&m.id).unwrap();
    let ev = reader.event_at(0).unwrap().unwrap();
    let hex = ev.response_blob.unwrap();
    let blob_path = reader.blobs_dir().join(format!("{hex}.bin"));
    drop(reader);
    std::fs::write(&blob_path, b"corrupted-bytes").unwrap();

    let out = agentrr(td.path())
        .arg("verify")
        .arg("--run")
        .arg(m.id.to_string())
        .output()
        .unwrap();
    assert!(
        !out.status.success(),
        "verify should fail on corrupted blob"
    );
    assert_eq!(out.status.code(), Some(3));
}
