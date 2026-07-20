//! Pure domain types for `agentrr`.
//!
//! Holds the vocabulary shared across crates: `RunId`, `EventKind`, `Event`,
//! `Step`, `RunManifest`, and the common `AgentrrError` enum. **No I/O, no
//! async** — trivially unit-testable. Concrete implementations land in M1.

#![forbid(unsafe_code)]
