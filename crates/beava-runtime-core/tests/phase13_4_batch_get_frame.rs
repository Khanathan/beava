//! Plan 13.4-03 Task 3.a (RED) — TCP frame parser MUST recognize op=0x0024
//! (`OP_BATCH_GET`) and produce `WireRequest::TcpBatchGet { body, body_format }`.
//!
//! Wire envelope (Phase 2.5 framing — re-used verbatim):
//! ```text
//! [u32 length BE][u16 op=0x0024 BE][u8 content_type][payload]
//! ```
//!
//! Payload is opaque to the parser — dispatch (`apply_shard.rs::dispatch_batch_get_sync`)
//! handles JSON / MsgPack deserialisation per the `body_format` byte.
//!
//! RED: until Task 3.b adds the `OP_BATCH_GET` constant + `WireRequest::TcpBatchGet`
//! variant + the parser arm, op=0x0024 falls through to `WireRequest::Unknown { op }`.

use beava_core::wire::{encode_frame, Frame, CT_JSON, CT_MSGPACK};
use beava_runtime_core::tcp_listener::parse_wire_request;
use beava_runtime_core::wire_request::WireRequest;
use bytes::{Bytes, BytesMut};

const OP_BATCH_GET: u16 = 0x0024;

/// Build a `[u32 len][u16 op][u8 ct][payload]` frame buffer ready for the parser.
fn make_frame(op: u16, ct: u8, payload: impl Into<Bytes>) -> BytesMut {
    let frame = Frame::new(op, ct, payload.into());
    let mut buf = BytesMut::new();
    encode_frame(&frame, &mut buf);
    buf
}

/// Test 1: a CT_JSON frame with op=0x0024 produces `WireRequest::TcpBatchGet`
/// carrying the original payload bytes verbatim and `body_format: CT_JSON`.
///
/// Empty `requests` body — the dispatch task validates the parsed shape; the
/// parser only cares about the envelope.
#[test]
fn frame_with_op_0x0024_parses_to_tcp_batch_get() {
    let payload = br#"{"requests":[]}"#;
    let mut buf = make_frame(OP_BATCH_GET, CT_JSON, Bytes::copy_from_slice(payload));
    let req = parse_wire_request(&mut buf, 4 * 1024 * 1024)
        .expect("no parse error")
        .expect("complete frame");
    match req {
        WireRequest::TcpBatchGet { body, body_format } => {
            assert_eq!(
                body.as_ref(),
                payload,
                "body bytes preserved verbatim by parser"
            );
            assert_eq!(body_format, CT_JSON);
        }
        other => panic!("expected WireRequest::TcpBatchGet, got {other:?}"),
    }
    assert_eq!(buf.len(), 0, "frame fully consumed from buf");
}

/// Test 2: a CT_MSGPACK frame with op=0x0024 produces `TcpBatchGet { body_format:
/// CT_MSGPACK }`. Dispatch will deserialise the body using rmp_serde at apply
/// time; the parser is content-type agnostic.
#[test]
fn frame_with_op_0x0024_carries_msgpack_body_format() {
    // Sentinel msgpack byte: 0x80 = fixmap of 0 entries. Dispatch validates;
    // parser preserves the byte and the format flag.
    let payload = &[0x80u8][..];
    let mut buf = make_frame(OP_BATCH_GET, CT_MSGPACK, Bytes::copy_from_slice(payload));
    let req = parse_wire_request(&mut buf, 4 * 1024 * 1024)
        .expect("no parse error")
        .expect("complete frame");
    match req {
        WireRequest::TcpBatchGet { body, body_format } => {
            assert_eq!(body.as_ref(), payload);
            assert_eq!(body_format, CT_MSGPACK);
        }
        other => panic!("expected WireRequest::TcpBatchGet (msgpack), got {other:?}"),
    }
}

/// Test 3: Regression guard — adding the OP_BATCH_GET arm MUST NOT alter
/// existing OP_GET (0x0020) routing. Parser still produces `WireRequest::TcpGet`.
#[test]
fn non_batch_get_op_unchanged() {
    use beava_core::wire::OP_GET;
    let payload = br#"{"feature":"cnt","key":"alice"}"#;
    let mut buf = make_frame(OP_GET, CT_JSON, Bytes::copy_from_slice(payload));
    let req = parse_wire_request(&mut buf, 4 * 1024 * 1024)
        .expect("no parse error")
        .expect("complete frame");
    match req {
        WireRequest::TcpGet { body, body_format } => {
            assert_eq!(body.as_ref(), payload);
            assert_eq!(body_format, CT_JSON);
        }
        other => panic!("expected WireRequest::TcpGet (regression guard), got {other:?}"),
    }
}
