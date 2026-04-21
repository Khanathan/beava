//! Phase 59-00 Wave 0 — TPC-PERF-09 D-E3 decoder fuzz regression guard.
//!
//! `decode_event_binary` is already panic-free on current HEAD (per-field
//! truncation guards at protocol.rs:820/825/832/840/849). Phase 59 Wave 1
//! refactors the shard thread to call this decoder directly on
//! `PayloadFmt::Binary`-tagged `ShardEvent.payload` — if that refactor
//! introduces a new decode path or regresses an existing one, this fuzz
//! catches it.
//!
//! Contract (GREEN from Wave 0 onward):
//!   For any byte sequence ≤ 4 KiB that passes through
//!   `decode_event_binary(&mut &bytes[..])`, the call returns EITHER:
//!     - `Ok(Value::Object(_))` (valid binary payload), OR
//!     - `Err(BeavaError::Protocol(_))` (bounded, protocol-level failure).
//!   **NO panics. NO aborts. NO OOMs.**
//!
//! Implemented with proptest (already a dev-dep per Phase 52). 500 cases;
//! Wave 1 can bump to 5,000 if a real fuzz harness lands.
//!
//! Test command: `cargo test --release --test protocol_binary_decode_fuzz`.

use beava::error::BeavaError;
use beava::server::protocol::decode_event_binary;
use proptest::prelude::*;

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 500,
        max_shrink_iters: 256,
        .. ProptestConfig::default()
    })]

    /// TPC-PERF-09 D-E3: random bytes never panic the binary decoder.
    /// Either decoded to Value::Object, or rejected with Protocol error.
    #[test]
    fn decode_event_binary_never_panics_on_arbitrary_bytes(
        bytes in prop::collection::vec(any::<u8>(), 0..=4096)
    ) {
        let mut slice: &[u8] = &bytes;
        let result = decode_event_binary(&mut slice);
        match result {
            Ok(v) => {
                prop_assert!(
                    v.is_object(),
                    "decode_event_binary returned Ok but value is not an Object: {v:?}"
                );
            }
            Err(BeavaError::Protocol(_)) => {
                // Acceptable: bounded protocol-level rejection.
            }
            Err(other) => {
                prop_assert!(
                    false,
                    "decode_event_binary returned non-Protocol error: {other:?}"
                );
            }
        }
    }

    /// Same contract, but bias the input toward small-field-count headers.
    /// A pathological `field_count = u16::MAX` already has the cap-clamp
    /// guard at protocol.rs:820 (`field_count.min(buf.len() / 4)`); this
    /// property pins that guard's behavior.
    #[test]
    fn decode_event_binary_handles_large_field_count_header(
        field_count in any::<u16>(),
        tail in prop::collection::vec(any::<u8>(), 0..=512)
    ) {
        let mut bytes = Vec::with_capacity(2 + tail.len());
        bytes.extend_from_slice(&field_count.to_be_bytes());
        bytes.extend_from_slice(&tail);
        let mut slice: &[u8] = &bytes;
        let result = decode_event_binary(&mut slice);
        match result {
            Ok(v) => {
                prop_assert!(v.is_object());
            }
            Err(BeavaError::Protocol(_)) => { /* ok */ }
            Err(other) => {
                prop_assert!(
                    false,
                    "decode_event_binary returned non-Protocol error on large header: {other:?}"
                );
            }
        }
    }
}
