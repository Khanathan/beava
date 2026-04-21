//! Wire-format helpers for Phase 59 binary-PUSH passthrough (TPC-PERF-09).
//!
//! Phase 59 Wave 1 scope — introduces the `PayloadFmt` tag carried on
//! `ShardEvent` plus a single-hop shard-side decode helper that dispatches on
//! the tag. The module also owns the server-side `BEAVA_MAX_PAYLOAD_BYTES`
//! DoS-cap helper (D-E1) and the `WIRE_BINARY_PASSTHROUGH` capability bit
//! (D-B1) advertised by `OP_NEGOTIATE_WIRE_FORMAT` (wired in Wave 2).
//!
//! See `.planning/phases/59-binary-wire-format-for-push/59-CONTEXT.md`
//! for decisions D-A2/D-A3, D-B1, D-C1, D-C2, D-E1.

#![allow(missing_docs)]

pub mod binary;
pub mod typed;

pub use binary::{decode_event_on_shard, reserialize_value_to_json_bytes, PayloadFmt};
pub use typed::decode_typed_row_push_batch;

/// D-B1: capability bit advertised via `OP_NEGOTIATE_WIRE_FORMAT`.
/// Bit 0 (= `1u32`) means the server accepts raw-binary OP_PUSH bodies and
/// passes them through to the shard thread without a
/// `serde_json::to_vec` re-serialize round-trip. Wave 1 lands the behavior;
/// Wave 2 wires the opcode that advertises this bit.
pub const WIRE_BINARY_PASSTHROUGH: u32 = 1u32 << 0;

/// Phase 59.6 D-B1 (TPC-PERF-11): client advertises readiness to send
/// `OP_PUSH_TYPED_BATCH` frames with schema_id prefixes. Server that
/// advertises this bit has Wave 2+ typed-row decoder available and can
/// decode schema-registered streams into `Row` without `serde_json::Value`.
/// Bit 1 (= `2u32`).
pub const WIRE_TYPED_PIPELINE: u32 = 1u32 << 1;

/// Phase 59.6 Wave 2: bitmask of all capability bits this server
/// advertises via `OP_NEGOTIATE_WIRE_FORMAT`. Wave 2 adds
/// `WIRE_TYPED_PIPELINE`. Future waves `|=` additional bits here.
pub const SERVER_SUPPORTED_BITS: u32 = WIRE_BINARY_PASSTHROUGH | WIRE_TYPED_PIPELINE;

/// D-E1 default payload-size DoS cap (1 MiB).
const DEFAULT_MAX_PAYLOAD_BYTES: usize = 1024 * 1024;
/// D-E1 hard minimum the env override can clamp to (1 KiB).
const MIN_MAX_PAYLOAD_BYTES: usize = 1024;
/// D-E1 hard maximum the env override can clamp to (64 MiB).
const MAX_MAX_PAYLOAD_BYTES: usize = 64 * 1024 * 1024;

/// D-E1: payload-size DoS cap. Read from `BEAVA_MAX_PAYLOAD_BYTES` at call
/// time; invalid / out-of-range values fall back to the default with a
/// stderr warning (once per call site — callers should cache the result
/// at startup via a `std::sync::OnceLock`).
///
/// Enforced at `parse_command` (src/server/protocol.rs) BEFORE any
/// `read_string` / `decode_event_binary` read against the body, so an
/// oversized frame never allocates inside the decoder.
pub fn max_payload_bytes_from_env() -> usize {
    match std::env::var("BEAVA_MAX_PAYLOAD_BYTES") {
        Ok(s) => match s.parse::<usize>() {
            Ok(n) if (MIN_MAX_PAYLOAD_BYTES..=MAX_MAX_PAYLOAD_BYTES).contains(&n) => n,
            _ => {
                eprintln!(
                    "BEAVA_MAX_PAYLOAD_BYTES={s:?} invalid or out of range \
                     [{MIN_MAX_PAYLOAD_BYTES},{MAX_MAX_PAYLOAD_BYTES}] — \
                     defaulting to {DEFAULT_MAX_PAYLOAD_BYTES}"
                );
                DEFAULT_MAX_PAYLOAD_BYTES
            }
        },
        Err(_) => DEFAULT_MAX_PAYLOAD_BYTES,
    }
}

#[cfg(test)]
mod mod_tests {
    use super::*;

    #[test]
    fn wire_binary_passthrough_is_bit_zero() {
        assert_eq!(WIRE_BINARY_PASSTHROUGH, 1u32);
    }

    #[test]
    fn wire_typed_pipeline_is_bit_one() {
        assert_eq!(WIRE_TYPED_PIPELINE, 2u32);
    }

    #[test]
    fn server_supported_bits_includes_typed_pipeline() {
        assert_ne!(SERVER_SUPPORTED_BITS & WIRE_TYPED_PIPELINE, 0);
        assert_ne!(SERVER_SUPPORTED_BITS & WIRE_BINARY_PASSTHROUGH, 0);
    }

    #[test]
    fn max_payload_bytes_default_is_1mib() {
        // Test runs in a cargo-spawned process: env var may or may not be
        // set by the harness. Remove defensively and re-read.
        std::env::remove_var("BEAVA_MAX_PAYLOAD_BYTES");
        assert_eq!(max_payload_bytes_from_env(), 1024 * 1024);
    }

    #[test]
    fn max_payload_bytes_respects_valid_override() {
        // Use a distinct value so parallel test runs don't race against
        // the default-is-1mib check. Clean up after.
        std::env::set_var("BEAVA_MAX_PAYLOAD_BYTES", "524288");
        assert_eq!(max_payload_bytes_from_env(), 524_288);
        std::env::remove_var("BEAVA_MAX_PAYLOAD_BYTES");
    }

    #[test]
    fn max_payload_bytes_invalid_falls_back_to_default() {
        std::env::set_var("BEAVA_MAX_PAYLOAD_BYTES", "not-a-number");
        assert_eq!(max_payload_bytes_from_env(), 1024 * 1024);
        std::env::remove_var("BEAVA_MAX_PAYLOAD_BYTES");
    }

    #[test]
    fn max_payload_bytes_below_floor_falls_back_to_default() {
        std::env::set_var("BEAVA_MAX_PAYLOAD_BYTES", "128");
        assert_eq!(max_payload_bytes_from_env(), 1024 * 1024);
        std::env::remove_var("BEAVA_MAX_PAYLOAD_BYTES");
    }
}
