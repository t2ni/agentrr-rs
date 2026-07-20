//! The match engine for `agentrr` replay.
//!
//! Maps a *live* request to the right *recorded* response deterministically:
//!
//! 1. [`canonical_json`] normalizes a JSON body — recursively sorted keys,
//!    NFC-normalized UTF-8 strings, canonical numbers.
//! 2. [`match_key`] folds in the provider + endpoint and hashes with BLAKE3.
//! 3. [`ReplayCursor`] gives the *k*-th recorded response for the *k*-th live
//!    occurrence of a key, so identical requests in a retry loop still line up.
//!
//! See `docs/matching.md` for worked examples.

#![forbid(unsafe_code)]

use std::collections::HashMap;

use serde_json::Value;
use unicode_normalization::UnicodeNormalization;

/// The provider whose wire format a request uses.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Provider {
    OpenAi,
    Anthropic,
    /// Any other provider; matched verbatim by name.
    Other(String),
}

impl Provider {
    pub fn as_str(&self) -> &str {
        match self {
            Provider::OpenAi => "openai",
            Provider::Anthropic => "anthropic",
            Provider::Other(s) => s.as_str(),
        }
    }

    /// Best-effort auto-detect from the request path (used by `--provider auto`).
    pub fn from_endpoint(endpoint: &str) -> Self {
        let p = endpoint.to_ascii_lowercase();
        if p.contains("/v1/messages") {
            Provider::Anthropic
        } else if p.contains("/chat/completions")
            || p.contains("/responses")
            || p.contains("/completions")
        {
            Provider::OpenAi
        } else {
            Provider::Other("auto".to_string())
        }
    }
}

/// How aggressively to match a live request against recorded ones.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MatchMode {
    /// Exact canonical match (default).
    Strict,
    /// Ignore sampling params (`temperature`, `top_p`, `seed`) — for when a user
    /// tweaks sampling but still wants the old responses.
    Loose,
}

/// Fields stripped under [`MatchMode::Loose`].
const SAMPLING_KEYS: [&str; 3] = ["temperature", "top_p", "seed"];

fn is_sampling_key(k: &str) -> bool {
    SAMPLING_KEYS.iter().any(|s| s == &k)
}

/// Canonicalize a JSON body to a stable string.
///
/// - Object keys are sorted recursively (ascending byte order).
/// - Strings are NFC-normalized.
/// - Numbers are canonicalized: integer-valued floats collapse to integers
///   (`1.0` → `1`), everything else uses Rust's shortest round-trip `Display`.
/// - Under [`MatchMode::Loose`], sampling fields are removed at every level.
pub fn canonical_json(body: &Value, mode: MatchMode) -> String {
    let mut out = String::new();
    write_canonical(body, mode, &mut out);
    out
}

fn write_canonical(v: &Value, mode: MatchMode, out: &mut String) {
    match v {
        Value::Null => out.push_str("null"),
        Value::Bool(b) => out.push_str(if *b { "true" } else { "false" }),
        Value::Number(n) => out.push_str(&format_number(n)),
        Value::String(s) => {
            let nfc: String = s.nfc().collect();
            push_json_string(&nfc, out);
        }
        Value::Array(a) => {
            out.push('[');
            for (i, x) in a.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                write_canonical(x, mode, out);
            }
            out.push(']');
        }
        Value::Object(o) => {
            let mut keys: Vec<&String> = o.keys().collect();
            // Sort by UTF-8 byte order (total, deterministic).
            keys.sort();
            out.push('{');
            let mut first = true;
            for k in keys {
                if mode == MatchMode::Loose && is_sampling_key(k) {
                    continue;
                }
                if !first {
                    out.push(',');
                }
                first = false;
                push_json_string(k, out);
                out.push(':');
                write_canonical(&o[k], mode, out);
            }
            out.push('}');
        }
    }
}

fn format_number(n: &serde_json::Number) -> String {
    if let Some(i) = n.as_i64() {
        return i.to_string();
    }
    if let Some(u) = n.as_u64() {
        return u.to_string();
    }
    if let Some(f) = n.as_f64() {
        // Collapse integer-valued floats to plain integers for stable matching.
        if f.is_finite() && f.fract() == 0.0 && f.abs() < 9_007_199_254_740_992.0 {
            return format!("{}", f as i64);
        }
        return format!("{f}");
    }
    // Arbitrary-precision fallback (shouldn't happen for normal payloads).
    n.to_string()
}

fn push_json_string(s: &str, out: &mut String) {
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '\u{08}' => out.push_str("\\b"),
            '\u{0c}' => out.push_str("\\f"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out.push('"');
}

/// The deterministic match key: `blake3(provider || NUL || endpoint || NUL || canonical_json)`.
pub fn match_key(provider: &Provider, endpoint: &str, body: &Value, mode: MatchMode) -> String {
    let canon = canonical_json(body, mode);
    let mut pre = Vec::with_capacity(provider.as_str().len() + endpoint.len() + canon.len() + 2);
    pre.extend_from_slice(provider.as_str().as_bytes());
    pre.push(0);
    pre.extend_from_slice(endpoint.as_bytes());
    pre.push(0);
    pre.extend_from_slice(canon.as_bytes());
    blake3::hash(&pre).to_hex().to_string()
}

/// Per-`match_key` FIFO cursor for replay ordering.
///
/// An agent may issue identical requests several times (retries, loops). Each
/// live occurrence of a key must map to the next recorded response for that key,
/// in record order. Exhausting the recorded set is a cache miss.
#[derive(Debug, Default)]
pub struct ReplayCursor {
    counts: HashMap<String, usize>,
}

impl ReplayCursor {
    pub fn new() -> Self {
        Self::default()
    }

    /// Advance and return the 0-based index of the next occurrence of `match_key`.
    /// `recorded_len` is how many recorded events share this key. Returns `None`
    /// when occurrences are exhausted (a replay miss).
    pub fn next_index(&mut self, match_key: &str, recorded_len: usize) -> Option<usize> {
        let entry = self.counts.entry(match_key.to_string()).or_insert(0);
        if *entry < recorded_len {
            let i = *entry;
            *entry += 1;
            Some(i)
        } else {
            None
        }
    }

    /// Reset all cursors.
    pub fn reset(&mut self) {
        self.counts.clear();
    }
}

// --------------------------------------------------------------------------- //
// Tests
// --------------------------------------------------------------------------- //

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshot_nested_unsorted() {
        let v = serde_json::json!({
            "model": "gpt-4",
            "messages": [{"role":"user","content":"hi"}],
            "b": 2,
            "a": 1,
            "nested": {"y": 1, "x": [3, 2, 1]}
        });
        insta::assert_snapshot!("nested_unsorted", canonical_json(&v, MatchMode::Strict));
    }

    #[test]
    fn snapshot_nfc_strings() {
        // "é" as 'e' + combining acute (U+0301) — NFC folds to a single codepoint.
        let decomposed = "e\u{0301}lan";
        let v = serde_json::json!({"q": decomposed});
        insta::assert_snapshot!("nfc_string", canonical_json(&v, MatchMode::Strict));
    }

    #[test]
    fn numbers_collapse_integer_floats() {
        let a = canonical_json(&serde_json::json!({"n": 1.0}), MatchMode::Strict);
        let b = canonical_json(&serde_json::json!({"n": 1}), MatchMode::Strict);
        assert_eq!(a, b);
        assert_eq!(a, r#"{"n":1}"#);
    }

    #[test]
    fn match_key_is_deterministic_and_order_independent() {
        let body = serde_json::json!({"b": 2, "a": 1});
        let shuffled = serde_json::json!({"a": 1, "b": 2});
        let k1 = match_key(
            &Provider::OpenAi,
            "/v1/chat/completions",
            &body,
            MatchMode::Strict,
        );
        let k2 = match_key(
            &Provider::OpenAi,
            "/v1/chat/completions",
            &shuffled,
            MatchMode::Strict,
        );
        assert_eq!(k1, k2);
        assert_eq!(k1.len(), 64); // blake3 hex
    }

    #[test]
    fn match_key_differs_by_provider_and_endpoint() {
        let body = serde_json::json!({"x": 1});
        let a = match_key(
            &Provider::OpenAi,
            "/v1/chat/completions",
            &body,
            MatchMode::Strict,
        );
        let b = match_key(
            &Provider::Anthropic,
            "/v1/messages",
            &body,
            MatchMode::Strict,
        );
        let c = match_key(&Provider::OpenAi, "/v1/responses", &body, MatchMode::Strict);
        assert_ne!(a, b);
        assert_ne!(a, c);
    }

    #[test]
    fn loose_strips_sampling_params() {
        let body =
            serde_json::json!({"model": "gpt", "temperature": 0.7, "top_p": 0.9, "seed": 42});
        let strict = match_key(&Provider::OpenAi, "/e", &body, MatchMode::Strict);
        let loose = match_key(&Provider::OpenAi, "/e", &body, MatchMode::Loose);
        assert_ne!(strict, loose);
        // Tweaking sampling under loose leaves the key unchanged.
        let body2 =
            serde_json::json!({"model": "gpt", "temperature": 0.1, "top_p": 0.5, "seed": 7});
        let loose2 = match_key(&Provider::OpenAi, "/e", &body2, MatchMode::Loose);
        assert_eq!(loose, loose2);
    }

    #[test]
    fn replay_cursor_orders_repeated_keys() {
        let mut cur = ReplayCursor::new();
        // 3 recorded responses for the same key.
        assert_eq!(cur.next_index("k", 3), Some(0));
        assert_eq!(cur.next_index("k", 3), Some(1));
        assert_eq!(cur.next_index("k", 3), Some(2));
        assert_eq!(cur.next_index("k", 3), None); // exhausted → miss
                                                  // A different key is independent.
        assert_eq!(cur.next_index("other", 1), Some(0));
    }

    #[test]
    fn provider_auto_detect() {
        assert_eq!(Provider::from_endpoint("/v1/messages"), Provider::Anthropic);
        assert_eq!(
            Provider::from_endpoint("/v1/chat/completions"),
            Provider::OpenAi
        );
        assert_eq!(Provider::from_endpoint("/v1/responses"), Provider::OpenAi);
        assert!(matches!(
            Provider::from_endpoint("/weird"),
            Provider::Other(_)
        ));
    }
}
