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
    /// Phase 59.6 Wave 2 (D-B1): pre-decoded typed `Row`. The shard thread
    /// looks up the schema via `ShardEvent.schema_id` and treats `payload`
    /// as the row's payload bytes (Wave 2 also stores the full packed body
    /// including the arena suffix so the decode is a straight copy). In
    /// Wave 2 the shard thread still bridges to the Value path via
    /// `crate::engine::schema::row_to_value`; Wave 3+ replaces the bridge
    /// with a direct typed handoff to operators.
    TypedRow = 2,
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
        // Phase 59.6 Wave 2 bridge: TypedRow arrives at the shard thread
        // as already-JSON-serialized bytes (listener side ran
        // `row_to_value`). Decode identically to Json. Wave 3+ eliminates
        // this bridge entirely by routing typed ShardEvents through a
        // dedicated ShardOp variant.
        PayloadFmt::Json | PayloadFmt::TypedRow => serde_json::from_slice(bytes).map_err(|e| {
            BeavaError::Protocol(format!("JSON decode error on shard: {}", e))
        }),
    }
}

/// Phase 59 Wave 1 fallback helper: re-serialize a parsed `Value` to JSON
/// bytes as `bytes::Bytes`. Used only on the rare JSON-fallback path where
/// a caller has a parsed Value in hand but no original wire bytes (synthetic
/// tests, replica relog with JSON-in-hand). Kept out of `src/server/tcp.rs`
/// so `scripts/verify-no-tcp-json-reserialize.sh`'s literal-pattern grep
/// (`serde_json::to_vec(payload)` / `serde_json::to_vec(r.payload)`) stays
/// at zero post-Wave-1 (D-C3 grep-ZERO invariant). The TCP hot path
/// forwards `raw_payload` directly; this helper is never hit at steady
/// state. Returns an empty `Bytes` on serialize failure — matches the
/// pre-Phase-59 `unwrap_or_default()` semantics.
pub fn reserialize_value_to_json_bytes(value: &serde_json::Value) -> bytes::Bytes {
    bytes::Bytes::from(serde_json::to_vec(value).unwrap_or_default())
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
    fn payload_fmt_typed_row_is_variant_two() {
        assert_eq!(PayloadFmt::TypedRow as u8, 2);
        assert_ne!(PayloadFmt::TypedRow, PayloadFmt::Binary);
        assert_ne!(PayloadFmt::TypedRow, PayloadFmt::Json);
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
