//! Wire-protocol constants for Beava's binary-framed TCP listener.
//!
//! # Frame envelope (v0, locked 2026-04-23)
//!
//! ```text
//! [u32 length BE][u16 op BE][u8 content_type][payload: length - 3 bytes]
//! ```
//!
//! `length` = bytes of `op + content_type + payload` (not counting itself).
//! Minimum valid length is 3. All multi-byte integers are big-endian
//! (network byte order).
//!
//! # Opcode table (D-02)
//!
//! Request and response frames share the same opcode space; the client-server
//! direction is implicit. Errors use the dedicated `OP_ERROR_RESPONSE` (0xFFFF).
//!
//! | Opcode   | Name             | Status               | Wired in |
//! |----------|------------------|----------------------|----------|
//! | 0x0000   | ping             | Implemented          | Phase 2.5 |
//! | 0x0001   | register         | Implemented          | Phase 2.5 |
//! | 0x0010   | push             | Reserved             | Phase 6  |
//! | 0x0011   | push_sync        | Reserved             | Phase 12 |
//! | 0x0012   | push_many        | Reserved             | Phase 12 |
//! | 0x0013   | push_table       | Reserved             | Phase 12 |
//! | 0x0014   | delete_table     | Reserved             | Phase 12 |
//! | 0x0020   | get              | Reserved             | Phase 12 |
//! | 0x0021   | mget             | Reserved             | Phase 12 |
//! | 0x0022   | get_multi        | Reserved             | Phase 12 |
//! | 0x0030   | set              | Reserved             | Phase 12 |
//! | 0x0031   | mset             | Reserved             | Phase 12 |
//! | 0xFFFF   | error_response   | Implemented          | Phase 2.5 |
//!
//! Reserved opcodes return `OP_ERROR_RESPONSE` with payload
//! `{error: {code: "op_not_implemented", message: "opcode 0xHHHH (name) reserved for Phase N"}, registry_version: N}`.
//! Unknown opcodes (not in this table) return `OP_ERROR_RESPONSE` with code `unknown_op`.
//!
//! # Content types (D-05)
//!
//! | Byte | Name        | Status                   |
//! |------|-------------|--------------------------|
//! | 0x01 | JSON        | v0 implemented           |
//! | 0x02 | MessagePack | Reserved (Phase 6/12)    |
//!
//! Frames with unknown content_type return `unsupported_content_type` error.
//! The connection stays open (only that frame is rejected).

use bytes::{Buf, BufMut, Bytes, BytesMut};
use thiserror::Error;

// ─── Opcodes ──────────────────────────────────────────────────────────────────

/// Health-check / version-probe opcode. Request payload ignored; response
/// carries `{server_version, registry_version}` JSON (Phase 2.5).
pub const OP_PING: u16 = 0x0000;

/// Registration opcode. Request payload is the same JSON DAG as `POST /register`;
/// response is the same 200/400/409 body (Phase 2.5).
pub const OP_REGISTER: u16 = 0x0001;

/// Fire-and-forget push. Reserved Phase 6.
pub const OP_PUSH: u16 = 0x0010;

/// Synchronous push with FeatureResult response. Reserved Phase 12.
pub const OP_PUSH_SYNC: u16 = 0x0011;

/// Batched push (N events in one frame). Reserved Phase 12.
pub const OP_PUSH_MANY: u16 = 0x0012;

/// Table upsert. Reserved Phase 12.
pub const OP_PUSH_TABLE: u16 = 0x0013;

/// Table tombstone. Reserved Phase 12.
pub const OP_DELETE_TABLE: u16 = 0x0014;

/// Single-key feature read. Reserved Phase 12.
pub const OP_GET: u16 = 0x0020;

/// Batched feature read (many keys, one feature). Reserved Phase 12.
pub const OP_MGET: u16 = 0x0021;

/// Multi-descriptor batched read. Reserved Phase 12.
pub const OP_GET_MULTI: u16 = 0x0022;

/// Direct feature write. Reserved Phase 12.
pub const OP_SET: u16 = 0x0030;

/// Batched direct writes. Reserved Phase 12.
pub const OP_MSET: u16 = 0x0031;

/// Dedicated error-response opcode. Payload matches the HTTP error body.
pub const OP_ERROR_RESPONSE: u16 = 0xFFFF;

// ─── Content types ────────────────────────────────────────────────────────────

/// JSON content type — the only implementation in v0.
pub const CT_JSON: u8 = 0x01;

/// MessagePack content type — reserved, not implemented in v0.
pub const CT_MSGPACK: u8 = 0x02;

// ─── Lookup helpers ───────────────────────────────────────────────────────────

/// Canonical snake_case name for a known opcode, or None if unknown.
/// Used in error payloads ("opcode 0x0010 (push) reserved for Phase 6").
pub fn opcode_name(op: u16) -> Option<&'static str> {
    match op {
        OP_PING => Some("ping"),
        OP_REGISTER => Some("register"),
        OP_PUSH => Some("push"),
        OP_PUSH_SYNC => Some("push_sync"),
        OP_PUSH_MANY => Some("push_many"),
        OP_PUSH_TABLE => Some("push_table"),
        OP_DELETE_TABLE => Some("delete_table"),
        OP_GET => Some("get"),
        OP_MGET => Some("mget"),
        OP_GET_MULTI => Some("get_multi"),
        OP_SET => Some("set"),
        OP_MSET => Some("mset"),
        OP_ERROR_RESPONSE => Some("error_response"),
        _ => None,
    }
}

/// Phase in which a reserved opcode will be implemented, or None for
/// implemented / unknown opcodes.
pub fn reserved_phase(op: u16) -> Option<&'static str> {
    match op {
        OP_PUSH => Some("Phase 6"),
        OP_PUSH_SYNC | OP_PUSH_MANY | OP_PUSH_TABLE | OP_DELETE_TABLE => Some("Phase 12"),
        OP_GET | OP_MGET | OP_GET_MULTI | OP_SET | OP_MSET => Some("Phase 12"),
        _ => None,
    }
}

// ─── Frame codec ──────────────────────────────────────────────────────────────

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

/// Codec-level errors. Handler-level errors (unknown_op / op_not_implemented /
/// unsupported_content_type) are policy choices made AFTER the frame has been
/// parsed successfully; they live in the server's tcp module.
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
    out.put_u32(total_len); // big-endian by default in `bytes`
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
/// `max_frame_bytes` is the configured maximum payload size (CONTEXT.md D-01).
/// The frame's declared length includes op + content_type; a payload of exactly
/// `max_frame_bytes` bytes is accepted (declared_len = max_frame_bytes + 3).
pub fn decode_frame(buf: &mut BytesMut, max_frame_bytes: u32) -> Result<Option<Frame>, FrameError> {
    if buf.len() < LEN_PREFIX_BYTES {
        return Ok(None);
    }

    // Peek the length without advancing
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
        return Ok(None); // wait for more bytes
    }

    // Consume the frame atomically.
    buf.advance(LEN_PREFIX_BYTES); // drop length prefix
    let op = buf.get_u16(); // advances 2
    let content_type = buf.get_u8(); // advances 1
    let payload_len = declared_len as usize - HEADER_AFTER_LEN_BYTES;
    let payload = buf.split_to(payload_len).freeze(); // zero-copy-ish

    Ok(Some(Frame {
        op,
        content_type,
        payload,
    }))
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    // ─── Opcode constant values ───────────────────────────────────────────────

    #[test]
    fn opcode_constants_have_locked_values() {
        assert_eq!(OP_PING, 0x0000);
        assert_eq!(OP_REGISTER, 0x0001);
        assert_eq!(OP_PUSH, 0x0010);
        assert_eq!(OP_PUSH_SYNC, 0x0011);
        assert_eq!(OP_PUSH_MANY, 0x0012);
        assert_eq!(OP_PUSH_TABLE, 0x0013);
        assert_eq!(OP_DELETE_TABLE, 0x0014);
        assert_eq!(OP_GET, 0x0020);
        assert_eq!(OP_MGET, 0x0021);
        assert_eq!(OP_GET_MULTI, 0x0022);
        assert_eq!(OP_SET, 0x0030);
        assert_eq!(OP_MSET, 0x0031);
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
        assert_eq!(opcode_name(OP_PUSH_TABLE), Some("push_table"));
        assert_eq!(opcode_name(OP_DELETE_TABLE), Some("delete_table"));
        assert_eq!(opcode_name(OP_GET), Some("get"));
        assert_eq!(opcode_name(OP_MGET), Some("mget"));
        assert_eq!(opcode_name(OP_GET_MULTI), Some("get_multi"));
        assert_eq!(opcode_name(OP_SET), Some("set"));
        assert_eq!(opcode_name(OP_MSET), Some("mset"));
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
        assert_eq!(reserved_phase(OP_PUSH), Some("Phase 6"));
        assert_eq!(reserved_phase(OP_PUSH_SYNC), Some("Phase 12"));
        assert_eq!(reserved_phase(OP_PUSH_MANY), Some("Phase 12"));
        assert_eq!(reserved_phase(OP_PUSH_TABLE), Some("Phase 12"));
        assert_eq!(reserved_phase(OP_DELETE_TABLE), Some("Phase 12"));
        assert_eq!(reserved_phase(OP_GET), Some("Phase 12"));
        assert_eq!(reserved_phase(OP_MGET), Some("Phase 12"));
        assert_eq!(reserved_phase(OP_GET_MULTI), Some("Phase 12"));
        assert_eq!(reserved_phase(OP_SET), Some("Phase 12"));
        assert_eq!(reserved_phase(OP_MSET), Some("Phase 12"));
    }

    #[test]
    fn reserved_phase_none_for_implemented() {
        assert_eq!(reserved_phase(OP_PING), None);
        assert_eq!(reserved_phase(OP_REGISTER), None);
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
            OP_PUSH_TABLE,
            OP_DELETE_TABLE,
            OP_GET,
            OP_MGET,
            OP_GET_MULTI,
            OP_SET,
            OP_MSET,
            OP_ERROR_RESPONSE,
        ];
        let set: std::collections::HashSet<u16> = ops.iter().copied().collect();
        assert_eq!(
            set.len(),
            ops.len(),
            "opcodes must be unique — copy-paste drift?"
        );
    }

    // ─── Frame construction + equality ────────────────────────────────────────

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

    // ─── encode_frame layout ──────────────────────────────────────────────────

    #[test]
    fn encode_frame_empty_payload() {
        let f = Frame::new(OP_PING, CT_JSON, Bytes::new());
        let mut buf = BytesMut::new();
        encode_frame(&f, &mut buf);
        // [0,0,0,3] length, [0,0] op, [0x01] ct
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
        // 4 (len) + 2 (op) + 1 (ct) + 1000 (payload) = 1007
        assert_eq!(buf.len(), 1007);
        // length field = 1003 (big-endian)
        let declared = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]);
        assert_eq!(declared, 1003);
        // op at bytes [4..6]
        assert_eq!(u16::from_be_bytes([buf[4], buf[5]]), 0x00AA);
        // content_type at byte [6]
        assert_eq!(buf[6], 0x01);
        // payload at bytes [7..1007]
        assert_eq!(&buf[7..1007], &payload[..]);
    }

    #[test]
    fn encode_frame_appends_to_existing_buf() {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(&[0xAAu8]);
        let f = Frame::new(OP_PING, CT_JSON, Bytes::new());
        encode_frame(&f, &mut buf);
        assert_eq!(buf[0], 0xAA);
        // frame starts at offset 1
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

    // ─── decode_frame edge cases ──────────────────────────────────────────────

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
        // declared_len = 10 (i.e., op + ct + 7 payload bytes) but we only have op+ct+1 byte
        let mut buf = BytesMut::from(&[0u8, 0, 0, 10, 0, 0, 0x01, 0xAA][..]);
        let orig_len = buf.len();
        let out = decode_frame(&mut buf, 4 * 1024 * 1024).unwrap();
        assert!(out.is_none());
        assert_eq!(buf.len(), orig_len, "buf unchanged when incomplete");
    }

    #[test]
    fn decode_too_large_returns_error() {
        // max_frame_bytes=1, declared_len=5 → limit=4
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
        // max_frame_bytes=10, payload=10 bytes, declared_len=13
        let mut buf = BytesMut::new();
        buf.extend_from_slice(&[0u8, 0, 0, 13, 0, 0, 0x01]);
        buf.extend_from_slice(&[0u8; 10]);
        let out = decode_frame(&mut buf, 10).unwrap().unwrap();
        assert_eq!(out.payload.len(), 10);
    }

    #[test]
    fn decode_one_over_limit_rejected() {
        // max_frame_bytes=10, payload=11 bytes, declared_len=14, limit=13
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
        // max_frame_bytes=u32::MAX, declared_len=u32::MAX — saturating_add must
        // saturate; declared_len == limit → proceeds; incomplete payload → Ok(None).
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

    // ─── Proptest round-trip ──────────────────────────────────────────────────

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

    // ─── Doc drift guard (Plan 05 Task 3 requirement, landed here) ────────────

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
            "push_table",
            "delete_table",
            "get",
            "mget",
            "get_multi",
            "set",
            "mset",
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
