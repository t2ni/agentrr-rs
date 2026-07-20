//! On-disk storage for `agentrr` runs.
//!
//! One run = one directory under the store root: `manifest.json`, `events.sqlite`
//! (ordered event index + match keys), and `blobs/` (content-addressed payloads
//! via BLAKE3). Handles create/open/list, redaction hooks, and bundle/import.
//! Implemented in M1.
