//! axum-based reverse proxy for `agentrr`.
//!
//! Speaks the OpenAI Chat Completions / Responses and Anthropic Messages wire
//! formats. **Record** mode forwards to a real upstream and captures every
//! non-deterministic boundary; **replay** mode serves cached responses by match
//! key without touching the network. Implemented in M3–M6.
