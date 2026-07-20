//! The match engine for `agentrr` replay.
//!
//! Canonicalizes JSON request bodies (recursive key sort, number / UTF-8 NFC
//! normalization), derives BLAKE3 match keys, and tracks per-key FIFO cursors so
//! the *k*-th identical request maps to the *k*-th recorded response. Supports
//! `strict` and `loose` matching. Implemented in M2.
