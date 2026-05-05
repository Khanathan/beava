//! Wire-protocol constants for Beava's binary-framed TCP listener.
//!
//! # Frame envelope
//!
//! ```text
//! [u32 length BE][u16 op BE][u8 content_type][payload: length - 3 bytes]
//! ```
//!
//! `length` = bytes of `op + content_type + payload` (not counting itself).
//! Minimum valid length is 3. All multi-byte integers are big-endian
//! (network byte order).
//!
//! # Opcode table
//!
//! Request and response frames share the same opcode space; the client-server
//! direction is implicit. Errors use the dedicated `OP_ERROR_RESPONSE` (0xFFFF).
//!
//! | Opcode   | Name             | Status      |
//! |----------|------------------|-------------|
//! | 0x0000   | ping             | Implemented |
//! | 0x0001   | register         | Implemented |
//! | 0x0010   | push             | Implemented |
//! | 0x0011   | push_sync        | Reserved    |
//! | 0x0012   | push_many        | Reserved    |
//! | 0x0020   | get              | Implemented |
//! | 0x0021   | mget             | Implemented |
//! | 0x0022   | get_multi        | Implemented |
//! | 0x0023   | get_response     | Implemented |
//! | 0x0024   | batch_get        | Implemented |
//! | 0x0030   | set              | Reserved    |
//! | 0x0031   | mset             | Reserved    |
//! | 0x0040   | reset            | Implemented |
//! | 0xFFFF   | error_response   | Implemented |
//!
//! 0x0013/0x0014 (push_table/delete_table) are removed per the Phase 12.7
//! events-only invariant (`project_v0_events_only_scope`); see CLAUDE.md
//! §"Events-Only Invariant". Tables return in v0.1+ if/when justified.
//! Unknown opcodes (including 0x0013/0x0014 if a stale client sends them)
//! return `OP_ERROR_RESPONSE` with code `unknown_op`.
//!
//! Reserved opcodes return `OP_ERROR_RESPONSE` with payload
//! `{error: {code: "op_not_implemented", message: "..."}, registry_version: N}`.
//! Unknown opcodes (not in this table) return `OP_ERROR_RESPONSE` with code `unknown_op`.
//!
//! # Content types
//!
//! | Byte | Name        | Status            |
//! |------|-------------|-------------------|
//! | 0x01 | JSON        | v0 implemented    |
//! | 0x02 | MessagePack | Reserved          |
//!
//! Frames with unknown content_type return `unsupported_content_type` error.
//! The connection stays open (only that frame is rejected).

use bytes::{Buf, BufMut, Bytes, BytesMut};
use thiserror::Error;

/// Health-check / version-probe opcode. Request payload ignored; response
/// carries `{server_version, registry_version}` JSON.
pub const OP_PING: u16 = 0x0000;

/// Registration opcode. Request payload is the same JSON DAG as `POST /register`;
/// response is the same 200/400/409 body.
pub const OP_REGISTER: u16 = 0x0001;

/// Fire-and-forget push.
pub const OP_PUSH: u16 = 0x0010;

/// Synchronous push with FeatureResult response. Reserved.
pub const OP_PUSH_SYNC: u16 = 0x0011;

/// Batched push (N events in one frame). Reserved.
pub const OP_PUSH_MANY: u16 = 0x0012;

// 0x0013 (push_table) and 0x0014 (delete_table) are excluded by the
// Phase 12.7 events-only invariant (`project_v0_events_only_scope`);
// see CLAUDE.md §"Events-Only Invariant". Tables return v0.1+ if/when
// justified by demand.

/// Single-key feature read.
pub const OP_GET: u16 = 0x0020;

/// Batched feature read (many keys, one feature).
pub const OP_MGET: u16 = 0x0021;

/// Multi-descriptor batched read.
pub const OP_GET_MULTI: u16 = 0x0022;

/// Read response — payload is the JSON body for `OP_GET` / `OP_MGET` /
/// `OP_GET_MULTI` (`{value: ...}` for single-feature, `{result: {...}}`
/// for batch).
pub const OP_GET_RESPONSE: u16 = 0x0023;

/// Batched heterogeneous read — clients send a list of (table, entity_id)
/// tuples in a single frame; server returns a single OP_GET_RESPONSE
/// (0x0023) frame whose JSON body holds per-tuple results. Composes with
/// the empty-string entity_id sentinel for global tables.
pub const OP_BATCH_GET: u16 = 0x0024;

/// Direct feature write. Reserved.
pub const OP_SET: u16 = 0x0030;

/// Batched direct writes. Reserved.
pub const OP_MSET: u16 = 0x0031;

/// Full state + registry clear.
///
/// Gated on server `test_mode` (`BEAVA_TEST_MODE=1` env var OR
/// `Config { test_mode: true }` at server construction). When the gate is
/// open the server clears ALL per-entity aggregation state and ALL
/// registry descriptors, then bumps `registry_version`. When the gate is
/// closed the server returns `reset_disabled_in_production` (HTTP 403 /
/// wire `OP_ERROR_RESPONSE = 0xFFFF`).
///
/// Wire request payload: empty `{}` JSON. Wire success response: framed
/// `OP_GET_RESPONSE (0x0023)` (the generic JSON success frame) with body
/// `{"reset": true, "registry_version": <new>}`.
pub const OP_RESET: u16 = 0x0040;

/// Dedicated error-response opcode. Payload matches the HTTP error body.
pub const OP_ERROR_RESPONSE: u16 = 0xFFFF;

/// JSON content type — the only implementation in v0.
pub const CT_JSON: u8 = 0x01;

/// MessagePack content type — reserved, not implemented in v0.
pub const CT_MSGPACK: u8 = 0x02;

/// Canonical snake_case name for a known opcode, or None if unknown.
pub fn opcode_name(op: u16) -> Option<&'static str> {
    match op {
        OP_PING => Some("ping"),
        OP_REGISTER => Some("register"),
        OP_PUSH => Some("push"),
        OP_PUSH_SYNC => Some("push_sync"),
        OP_PUSH_MANY => Some("push_many"),
        OP_GET => Some("get"),
        OP_MGET => Some("mget"),
        OP_GET_MULTI => Some("get_multi"),
        OP_GET_RESPONSE => Some("get_response"),
        OP_BATCH_GET => Some("batch_get"),
        OP_SET => Some("set"),
        OP_MSET => Some("mset"),
        OP_RESET => Some("reset"),
        OP_ERROR_RESPONSE => Some("error_response"),
        _ => None,
    }
}

/// Phase in which a reserved opcode will be implemented, or None for
/// implemented / unknown opcodes.
pub fn reserved_phase(op: u16) -> Option<&'static str> {
    match op {
        // 0x0013/0x0014 (push_table/delete_table) are excluded by the
        // Phase 12.7 events-only invariant (CLAUDE.md §"Events-Only
        // Invariant") — they fall through to None and are treated as
        // unknown_op by handlers, not Reserved.
        OP_PUSH_SYNC | OP_PUSH_MANY => Some("Phase 12"),
        OP_SET | OP_MSET => Some("Phase 12"),
        _ => None,
    }
}

/// A decoded frame. `payload` is a cheap-to-clone `Bytes` slice.
///
/// Wire layout (big-endian lengths, content_type is a single byte):
/// ```text
/// [u32 length][u16 op][u8 content_type][payload: length - 3 bytes]
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Frame {
    pub op: u16,
    pub content_type: u8,
    pub payload: Bytes,
}

impl Frame {
    /// Build a Frame owning its payload. Convenience for tests and handlers.
    pub fn new(op: u16, content_type: u8, payload: impl Into<Bytes>) -> Self {
        Self {
            op,
            content_type,
            payload: payload.into(),
        }
    }
}

/// Size of the length prefix in bytes.
const LEN_PREFIX_BYTES: usize = 4;
/// Size of the header that follows the length prefix (op + content_type).
const HEADER_AFTER_LEN_BYTES: usize = 3;

/// Codec-level errors. Handler-level errors (`unknown_op`, `op_not_implemented`,
/// `unsupported_content_type`) are policy choices made after a frame parses
/// successfully and live in the server's TCP module.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum FrameError {
    #[error("frame length {declared_len} exceeds limit {limit} (max_frame_bytes + 3 for op+ct)")]
    TooLarge { declared_len: u32, limit: u32 },
    #[error("frame length {declared_len} < 3: cannot cover op + content_type")]
    LengthUnderflow { declared_len: u32 },
}

/// Append a frame to `out`. Never fails in v0 — the payload size is bounded by
/// the caller (the handler; ultimately by `max_frame_bytes`). Writing a frame
/// with payload.len() > u32::MAX - 3 would wrap; handlers check this before
/// calling encode_frame.
pub fn encode_frame(frame: &Frame, out: &mut BytesMut) {
    let payload_len = frame.payload.len();
    debug_assert!(
        payload_len <= (u32::MAX as usize) - HEADER_AFTER_LEN_BYTES,
        "payload too large for u32 length prefix"
    );
    let total_len = (payload_len + HEADER_AFTER_LEN_BYTES) as u32;
    out.reserve(LEN_PREFIX_BYTES + total_len as usize);
    out.put_u32(total_len); // big-endian by default in `bytes`.
    out.put_u16(frame.op);
    out.put_u8(frame.content_type);
    out.extend_from_slice(&frame.payload);
}

/// Attempt to decode one frame from `buf`. Returns:
///   Ok(Some(Frame))  — a complete frame was consumed; buf advanced past it.
///   Ok(None)         — not enough bytes yet; buf unchanged. Caller reads more.
///   Err(FrameError)  — protocol violation; caller writes an error response and
///                      (for TooLarge / LengthUnderflow) closes the connection.
///                      The cursor is NOT advanced on error, so the caller can
///                      decide connection fate without further decode ambiguity.
///
/// `max_frame_bytes` is the configured maximum payload size. The frame's
/// declared length includes op + content_type; a payload of exactly
/// `max_frame_bytes` bytes is accepted (declared_len = max_frame_bytes + 3).
pub fn decode_frame(buf: &mut BytesMut, max_frame_bytes: u32) -> Result<Option<Frame>, FrameError> {
    if buf.len() < LEN_PREFIX_BYTES {
        return Ok(None);
    }

    let declared_len = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]);

    if declared_len < HEADER_AFTER_LEN_BYTES as u32 {
        return Err(FrameError::LengthUnderflow { declared_len });
    }

    let limit = max_frame_bytes.saturating_add(HEADER_AFTER_LEN_BYTES as u32);
    if declared_len > limit {
        return Err(FrameError::TooLarge {
            declared_len,
            limit,
        });
    }

    let total_needed = LEN_PREFIX_BYTES + declared_len as usize;
    if buf.len() < total_needed {
        return Ok(None);
    }

    // Consume the frame atomically.
    buf.advance(LEN_PREFIX_BYTES);
    let op = buf.get_u16();
    let content_type = buf.get_u8();
    let payload_len = declared_len as usize - HEADER_AFTER_LEN_BYTES;
    let payload = buf.split_to(payload_len).freeze();

    Ok(Some(Frame {
        op,
        content_type,
        payload,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn opcode_constants_have_locked_values() {
        assert_eq!(OP_PING, 0x0000);
        assert_eq!(OP_REGISTER, 0x0001);
        assert_eq!(OP_PUSH, 0x0010);
        assert_eq!(OP_PUSH_SYNC, 0x0011);
        assert_eq!(OP_PUSH_MANY, 0x0012);
        // 0x0013 / 0x0014 are excluded by the Phase 12.7 events-only
        // invariant (CLAUDE.md §"Events-Only Invariant").
        assert_eq!(OP_GET, 0x0020);
        assert_eq!(OP_MGET, 0x0021);
        assert_eq!(OP_GET_MULTI, 0x0022);
        assert_eq!(OP_GET_RESPONSE, 0x0023);
        assert_eq!(OP_BATCH_GET, 0x0024);
        assert_eq!(OP_SET, 0x0030);
        assert_eq!(OP_MSET, 0x0031);
        assert_eq!(OP_RESET, 0x0040);
        assert_eq!(OP_ERROR_RESPONSE, 0xFFFF);
    }

    #[test]
    fn content_type_constants_have_locked_values() {
        assert_eq!(CT_JSON, 0x01);
        assert_eq!(CT_MSGPACK, 0x02);
    }

    #[test]
    fn opcode_name_covers_every_constant() {
        assert_eq!(opcode_name(OP_PING), Some("ping"));
        assert_eq!(opcode_name(OP_REGISTER), Some("register"));
        assert_eq!(opcode_name(OP_PUSH), Some("push"));
        assert_eq!(opcode_name(OP_PUSH_SYNC), Some("push_sync"));
        assert_eq!(opcode_name(OP_PUSH_MANY), Some("push_many"));
        // 0x0013 / 0x0014 are excluded by the Phase 12.7 events-only
        // invariant — opcode_name returns None.
        assert_eq!(opcode_name(0x0013), None);
        assert_eq!(opcode_name(0x0014), None);
        assert_eq!(opcode_name(OP_GET), Some("get"));
        assert_eq!(opcode_name(OP_MGET), Some("mget"));
        assert_eq!(opcode_name(OP_GET_MULTI), Some("get_multi"));
        assert_eq!(opcode_name(OP_GET_RESPONSE), Some("get_response"));
        assert_eq!(opcode_name(OP_BATCH_GET), Some("batch_get"));
        assert_eq!(opcode_name(OP_SET), Some("set"));
        assert_eq!(opcode_name(OP_MSET), Some("mset"));
        assert_eq!(opcode_name(OP_RESET), Some("reset"));
        assert_eq!(opcode_name(OP_ERROR_RESPONSE), Some("error_response"));
    }

    #[test]
    fn opcode_name_returns_none_for_unknown() {
        assert_eq!(opcode_name(0x0002), None);
        assert_eq!(opcode_name(0x4242), None);
        assert_eq!(opcode_name(0x7FFE), None);
    }

    #[test]
    fn reserved_phase_covers_every_reserved() {
        assert_eq!(reserved_phase(OP_PUSH), None);
        assert_eq!(reserved_phase(OP_PUSH_SYNC), Some("Phase 12"));
        assert_eq!(reserved_phase(OP_PUSH_MANY), Some("Phase 12"));
        // 0x0013 / 0x0014 are excluded by the Phase 12.7 events-only
        // invariant — they fall through to None (treated as unknown_op).
        assert_eq!(reserved_phase(0x0013), None);
        assert_eq!(reserved_phase(0x0014), None);
        assert_eq!(reserved_phase(OP_GET), None);
        assert_eq!(reserved_phase(OP_MGET), None);
        assert_eq!(reserved_phase(OP_GET_MULTI), None);
        assert_eq!(reserved_phase(OP_GET_RESPONSE), None);
        assert_eq!(reserved_phase(OP_BATCH_GET), None);
        assert_eq!(reserved_phase(OP_SET), Some("Phase 12"));
        assert_eq!(reserved_phase(OP_MSET), Some("Phase 12"));
        assert_eq!(reserved_phase(OP_RESET), None);
    }

    #[test]
    fn reserved_phase_none_for_implemented() {
        assert_eq!(reserved_phase(OP_PING), None);
        assert_eq!(reserved_phase(OP_REGISTER), None);
        assert_eq!(reserved_phase(OP_PUSH), None);
        assert_eq!(reserved_phase(OP_GET), None);
        assert_eq!(reserved_phase(OP_MGET), None);
        assert_eq!(reserved_phase(OP_GET_MULTI), None);
        assert_eq!(reserved_phase(OP_GET_RESPONSE), None);
        assert_eq!(reserved_phase(OP_ERROR_RESPONSE), None);
        assert_eq!(reserved_phase(0x4242), None);
    }

    #[test]
    fn opcodes_are_unique() {
        let ops = [
            OP_PING,
            OP_REGISTER,
            OP_PUSH,
            OP_PUSH_SYNC,
            OP_PUSH_MANY,
            // 0x0013 / 0x0014 are excluded by the Phase 12.7 events-only invariant.
            OP_GET,
            OP_MGET,
            OP_GET_MULTI,
            OP_GET_RESPONSE,
            OP_BATCH_GET,
            OP_SET,
            OP_MSET,
            OP_RESET,
            OP_ERROR_RESPONSE,
        ];
        let set: std::collections::HashSet<u16> = ops.iter().copied().collect();
        assert_eq!(
            set.len(),
            ops.len(),
            "opcodes must be unique — copy-paste drift?"
        );
    }

    #[test]
    fn frame_new_constructs() {
        let f = Frame::new(OP_PING, CT_JSON, vec![1u8, 2, 3]);
        assert_eq!(f.op, OP_PING);
        assert_eq!(f.content_type, CT_JSON);
        assert_eq!(f.payload.as_ref(), &[1u8, 2, 3]);
    }

    #[test]
    fn frame_equality_is_structural() {
        let a = Frame::new(OP_PING, CT_JSON, vec![1u8, 2, 3]);
        let b = Frame::new(OP_PING, CT_JSON, vec![1u8, 2, 3]);
        assert_eq!(a, b);
    }

    #[test]
    fn encode_frame_empty_payload() {
        let f = Frame::new(OP_PING, CT_JSON, Bytes::new());
        let mut buf = BytesMut::new();
        encode_frame(&f, &mut buf);
        assert_eq!(buf.as_ref(), &[0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x01]);
    }

    #[test]
    fn encode_frame_big_endian_byte_layout() {
        let f = Frame::new(0x0102, 0x03, vec![0x04u8]);
        let mut buf = BytesMut::new();
        encode_frame(&f, &mut buf);
        assert_eq!(
            buf.as_ref(),
            &[0x00, 0x00, 0x00, 0x04, 0x01, 0x02, 0x03, 0x04]
        );
    }

    #[test]
    fn encode_frame_large_payload_layout() {
        let payload: Vec<u8> = (0..1000).map(|i| (i % 256) as u8).collect();
        let f = Frame::new(0x00AA, 0x01, payload.clone());
        let mut buf = BytesMut::new();
        encode_frame(&f, &mut buf);
        assert_eq!(buf.len(), 1007);
        let declared = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]);
        assert_eq!(declared, 1003);
        assert_eq!(u16::from_be_bytes([buf[4], buf[5]]), 0x00AA);
        assert_eq!(buf[6], 0x01);
        assert_eq!(&buf[7..1007], &payload[..]);
    }

    #[test]
    fn encode_frame_appends_to_existing_buf() {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(&[0xAAu8]);
        let f = Frame::new(OP_PING, CT_JSON, Bytes::new());
        encode_frame(&f, &mut buf);
        assert_eq!(buf[0], 0xAA);
        assert_eq!(&buf[1..8], &[0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x01]);
    }

    #[test]
    fn frame_error_display_too_large() {
        let e = FrameError::TooLarge {
            declared_len: 100,
            limit: 50,
        };
        let s = e.to_string();
        assert!(s.contains("100"), "got: {s}");
        assert!(s.contains("50"), "got: {s}");
    }

    #[test]
    fn frame_error_display_underflow() {
        let e = FrameError::LengthUnderflow { declared_len: 2 };
        let s = e.to_string();
        assert!(s.contains('2'));
        assert!(s.contains('3'));
    }

    #[test]
    fn decode_empty_buffer_returns_none() {
        let mut buf = BytesMut::new();
        let out = decode_frame(&mut buf, 4 * 1024 * 1024).unwrap();
        assert!(out.is_none());
        assert_eq!(buf.len(), 0);
    }

    #[test]
    fn decode_truncated_length_prefix_returns_none() {
        let mut buf = BytesMut::from(&[0x00u8, 0x01][..]);
        let out = decode_frame(&mut buf, 4 * 1024 * 1024).unwrap();
        assert!(out.is_none());
        assert_eq!(buf.len(), 2, "buf unchanged");
    }

    #[test]
    fn decode_length_prefix_exactly_3_accepted() {
        let mut buf = BytesMut::from(&[0u8, 0, 0, 3, 0, 1, 2][..]);
        let out = decode_frame(&mut buf, 4 * 1024 * 1024).unwrap().unwrap();
        assert_eq!(out.op, 1);
        assert_eq!(out.content_type, 2);
        assert_eq!(out.payload.len(), 0);
        assert_eq!(buf.len(), 0, "buf drained");
    }

    #[test]
    fn decode_length_prefix_of_2_returns_underflow() {
        let mut buf = BytesMut::from(&[0u8, 0, 0, 2, 0, 1][..]);
        let orig_len = buf.len();
        let err = decode_frame(&mut buf, 4 * 1024 * 1024).unwrap_err();
        assert_eq!(err, FrameError::LengthUnderflow { declared_len: 2 });
        assert_eq!(buf.len(), orig_len, "buf unchanged on error");
    }

    #[test]
    fn decode_length_prefix_of_0_returns_underflow() {
        let mut buf = BytesMut::from(&[0u8, 0, 0, 0][..]);
        let err = decode_frame(&mut buf, 4 * 1024 * 1024).unwrap_err();
        assert_eq!(err, FrameError::LengthUnderflow { declared_len: 0 });
        assert_eq!(buf.len(), 4, "buf unchanged on error");
    }

    #[test]
    fn decode_truncated_payload_returns_none() {
        // declared_len=10 (op + ct + 7 payload bytes) but only op+ct+1 in the buffer.
        let mut buf = BytesMut::from(&[0u8, 0, 0, 10, 0, 0, 0x01, 0xAA][..]);
        let orig_len = buf.len();
        let out = decode_frame(&mut buf, 4 * 1024 * 1024).unwrap();
        assert!(out.is_none());
        assert_eq!(buf.len(), orig_len, "buf unchanged when incomplete");
    }

    #[test]
    fn decode_too_large_returns_error() {
        let mut buf = BytesMut::from(&[0u8, 0, 0, 5][..]);
        let orig_len = buf.len();
        let err = decode_frame(&mut buf, 1).unwrap_err();
        assert_eq!(
            err,
            FrameError::TooLarge {
                declared_len: 5,
                limit: 4
            }
        );
        assert_eq!(buf.len(), orig_len, "buf unchanged on error");
    }

    #[test]
    fn decode_at_exactly_limit_accepted() {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(&[0u8, 0, 0, 13, 0, 0, 0x01]);
        buf.extend_from_slice(&[0u8; 10]);
        let out = decode_frame(&mut buf, 10).unwrap().unwrap();
        assert_eq!(out.payload.len(), 10);
    }

    #[test]
    fn decode_one_over_limit_rejected() {
        let mut buf = BytesMut::from(&[0u8, 0, 0, 14][..]);
        let err = decode_frame(&mut buf, 10).unwrap_err();
        assert_eq!(
            err,
            FrameError::TooLarge {
                declared_len: 14,
                limit: 13
            }
        );
    }

    #[test]
    fn decode_saturating_limit_no_overflow() {
        // saturating_add must not overflow when declared_len == u32::MAX;
        // with max_frame_bytes=u32::MAX the limit saturates and the decoder
        // waits for more bytes (Ok(None)) instead of erroring.
        let mut buf = BytesMut::from(&[0xFFu8, 0xFF, 0xFF, 0xFF][..]);
        let out = decode_frame(&mut buf, u32::MAX).unwrap();
        assert!(out.is_none(), "should wait for bytes, not panic");
    }

    #[test]
    fn decode_two_frames_concatenated() {
        let a = Frame::new(OP_PING, CT_JSON, vec![0xAAu8]);
        let b = Frame::new(OP_REGISTER, CT_JSON, vec![0xBBu8, 0xCC]);
        let mut buf = BytesMut::new();
        encode_frame(&a, &mut buf);
        encode_frame(&b, &mut buf);
        let fa = decode_frame(&mut buf, 4 * 1024 * 1024).unwrap().unwrap();
        let fb = decode_frame(&mut buf, 4 * 1024 * 1024).unwrap().unwrap();
        assert_eq!(fa, a);
        assert_eq!(fb, b);
        assert_eq!(buf.len(), 0);
    }

    #[test]
    fn decode_partial_then_complete() {
        let f = Frame::new(OP_PING, CT_JSON, vec![0xAAu8, 0xBB, 0xCC]);
        let mut full = BytesMut::new();
        encode_frame(&f, &mut full);
        let bytes: Vec<u8> = full.to_vec();
        let mid = bytes.len() / 2;

        let mut buf = BytesMut::new();
        buf.extend_from_slice(&bytes[..mid]);
        let out = decode_frame(&mut buf, 4 * 1024 * 1024).unwrap();
        assert!(out.is_none());

        buf.extend_from_slice(&bytes[mid..]);
        let out = decode_frame(&mut buf, 4 * 1024 * 1024).unwrap().unwrap();
        assert_eq!(out, f);
    }

    fn arb_frame() -> impl Strategy<Value = Frame> {
        (
            any::<u16>(),
            any::<u8>(),
            proptest::collection::vec(any::<u8>(), 0..10_000),
        )
            .prop_map(|(op, ct, payload)| Frame {
                op,
                content_type: ct,
                payload: Bytes::from(payload),
            })
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(256))]

        #[test]
        fn round_trip_is_byte_identical(frame in arb_frame()) {
            let mut buf = BytesMut::new();
            encode_frame(&frame, &mut buf);
            let out = decode_frame(&mut buf, 16 * 1024 * 1024).unwrap();
            prop_assert_eq!(out, Some(frame));
            prop_assert_eq!(buf.len(), 0, "buf fully drained");
        }

        #[test]
        fn two_frames_concatenated_decode_in_order(a in arb_frame(), b in arb_frame()) {
            let mut buf = BytesMut::new();
            encode_frame(&a, &mut buf);
            encode_frame(&b, &mut buf);
            let fa = decode_frame(&mut buf, 16 * 1024 * 1024).unwrap();
            let fb = decode_frame(&mut buf, 16 * 1024 * 1024).unwrap();
            prop_assert_eq!(fa, Some(a));
            prop_assert_eq!(fb, Some(b));
            prop_assert_eq!(buf.len(), 0);
        }

        #[test]
        fn streaming_partial_reads_reassemble_correctly(frame in arb_frame()) {
            let mut full = BytesMut::new();
            encode_frame(&frame, &mut full);
            let bytes: Vec<u8> = full.to_vec();

            let mut streaming = BytesMut::new();
            for (i, b) in bytes.iter().enumerate() {
                streaming.extend_from_slice(&[*b]);
                let result = decode_frame(&mut streaming, 16 * 1024 * 1024).unwrap();
                if i + 1 < bytes.len() {
                    prop_assert!(result.is_none(), "premature decode at byte {}", i);
                } else {
                    prop_assert_eq!(result, Some(frame.clone()));
                    prop_assert_eq!(streaming.len(), 0);
                }
            }
        }
    }

    #[test]
    fn wire_module_doc_contains_full_opcode_table() {
        let src = include_str!("wire.rs");
        // Extract top-of-file doc comment (everything before the first non-doc-comment line).
        let doc: String = src
            .lines()
            .take_while(|l| l.starts_with("//!") || l.trim().is_empty())
            .collect::<Vec<_>>()
            .join("\n");
        for mnemonic in [
            "ping",
            "register",
            "push",
            "push_sync",
            "push_many",
            // 0x0013 / 0x0014 (push_table / delete_table) are excluded by
            // the Phase 12.7 events-only invariant; the doc table no longer
            // carries them, so this guard only enforces surviving v0 mnemonics.
            "get",
            "mget",
            "get_multi",
            "get_response",
            "batch_get",
            "set",
            "mset",
            "reset",
            "error_response",
        ] {
            assert!(
                doc.contains(mnemonic),
                "module doc missing mnemonic '{}'",
                mnemonic
            );
        }
    }
}
