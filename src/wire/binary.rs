//! Binary-passthrough codec helpers for Phase 59 (TPC-PERF-09).
//!
//! See 59-CONTEXT.md decisions D-A2, D-C1, D-C2.

use crate::error::BeavaError;
use crate::server::protocol::decode_event_binary;

/// Phase 59 D-A2: payload-format tag carried on `ShardEvent`. Determines
/// whether the shard thread decodes via `decode_event_binary` (binary
/// passthrough) or `serde_json::from_slice` (HTTP path + legacy TCP JSON
/// fallback).
///
/// Default is `Binary` per D-C2 — the TCP + Python primary path.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum PayloadFmt {
    /// TCP binary wire shape: `[u16 BE field_count]` + per-field
    /// `[u16 BE key_len][key utf-8][u8 type_tag][value bytes]`. Decoded
    /// via [`crate::server::protocol::decode_event_binary`].
    Binary = 0,
    /// JSON-encoded object. HTTP POST /push + replica `LOG_FMT_JSON`
    /// path + legacy TCP JSON fallback.
    Json = 1,
}

impl Default for PayloadFmt {
    fn default() -> Self {
        // D-C2: Binary is the default (TCP + Python primary path).
        PayloadFmt::Binary
    }
}

/// Phase 59 D-C1: called by `src/shard/thread.rs::process_shard_event` to
/// decode an event's payload bytes into a `serde_json::Value` that the
/// engine (`PipelineEngine::push_with_cascade_on_shard`) can consume.
///
/// Binary path: single call to `decode_event_binary` — the one necessary
/// parse on the TCP hot path after Phase 59 eliminates the WASTE round-trip.
///
/// Json path: `serde_json::from_slice` for HTTP + replica-JSON paths. A
/// JSON decode error is surfaced as `BeavaError::Protocol` so the caller
/// can drop the event and bump the Dropped metric.
pub fn decode_event_on_shard(
    bytes: &[u8],
    fmt: PayloadFmt,
) -> Result<serde_json::Value, BeavaError> {
    match fmt {
        PayloadFmt::Binary => {
            let mut buf: &[u8] = bytes;
            decode_event_binary(&mut buf)
        }
        PayloadFmt::Json => serde_json::from_slice(bytes).map_err(|e| {
            BeavaError::Protocol(format!("JSON decode error on shard: {}", e))
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server::protocol::{write_string, TYPE_I64};

    #[test]
    fn payload_fmt_default_is_binary() {
        assert_eq!(PayloadFmt::default(), PayloadFmt::Binary);
    }

    #[test]
    fn decode_on_shard_binary_roundtrip() {
        // Build a binary-tagged `{amount: 100}` payload.
        let mut buf = Vec::new();
        buf.extend_from_slice(&1u16.to_be_bytes()); // 1 field
        buf.extend_from_slice(&write_string("amount")); // key
        buf.push(TYPE_I64); // type tag
        buf.extend_from_slice(&100i64.to_be_bytes()); // value
        let v = decode_event_on_shard(&buf, PayloadFmt::Binary).unwrap();
        assert_eq!(v["amount"], 100);
    }

    #[test]
    fn decode_on_shard_json_roundtrip() {
        let bytes = br#"{"amount":100}"#;
        let v = decode_event_on_shard(bytes, PayloadFmt::Json).unwrap();
        assert_eq!(v["amount"], 100);
    }

    #[test]
    fn decode_on_shard_binary_truncated_returns_error() {
        let bytes: [u8; 1] = [0u8]; // truncated header (need 2 bytes)
        let err = decode_event_on_shard(&bytes, PayloadFmt::Binary).unwrap_err();
        assert!(matches!(err, BeavaError::Protocol(_)));
    }

    #[test]
    fn decode_on_shard_json_invalid_returns_error() {
        let bytes = br#"not-valid-json"#;
        let err = decode_event_on_shard(bytes, PayloadFmt::Json).unwrap_err();
        assert!(matches!(err, BeavaError::Protocol(_)));
    }
}
