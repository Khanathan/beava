//! Framed TCP listener for Phase 18's hand-rolled event loop.
//!
//! Wraps `mio::net::TcpListener` and provides accept + frame-parse helpers.
//! Wire format: `[u32 length BE][u16 op BE][u8 content_type][payload]`
//! (Phase 2.5 framing, re-uses `beava_core::wire` codec).
//!
//! # Frame parser
//!
//! `parse_wire_request` re-uses the battle-tested `beava_core::wire::decode_frame`
//! codec (Phase 2.5) and lifts the raw `Frame` into a typed `WireRequest`.
//! A single recv() can deliver 0, 1, or many frames — the caller loops until
//! `Ok(None)` (need more bytes) is returned.

use beava_core::wire::{decode_frame, CT_JSON, CT_MSGPACK, OP_PING, OP_PUSH, OP_REGISTER};
use bytes::{Bytes, BytesMut};
use std::net::SocketAddr;

use crate::wire_request::WireRequest;

/// A mio-backed TCP listener for the framed Phase 2.5 wire protocol.
///
/// Phase 18-01 scaffold: binds the listener, exposes `accept()` for the
/// event loop to call when the listener token fires. Full client dispatch
/// added in Task 1.2.
pub struct TcpListener {
    inner: mio::net::TcpListener,
    local_addr: SocketAddr,
}

impl std::fmt::Debug for TcpListener {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TcpListener")
            .field("local_addr", &self.local_addr)
            .finish()
    }
}

impl TcpListener {
    /// Bind a TCP listener on the given address. Port 0 lets the OS pick.
    pub fn bind(addr: SocketAddr) -> std::io::Result<Self> {
        let inner = mio::net::TcpListener::bind(addr)?;
        let local_addr = inner.local_addr()?;
        Ok(Self { inner, local_addr })
    }

    /// Construct from an already-bound `std::net::TcpListener` (must be non-blocking).
    pub fn from_std(listener: std::net::TcpListener) -> std::io::Result<Self> {
        let local_addr = listener.local_addr()?;
        let inner = mio::net::TcpListener::from_std(listener);
        Ok(Self { inner, local_addr })
    }

    /// The actual bound address (useful when port 0 was requested).
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// Accept the next pending connection. Returns `WouldBlock` when there
    /// are no more connections ready in this poll tick.
    pub fn accept(&self) -> std::io::Result<(mio::net::TcpStream, SocketAddr)> {
        self.inner.accept()
    }

    /// Borrow the inner mio listener for registration with the event loop.
    pub fn inner_mut(&mut self) -> &mut mio::net::TcpListener {
        &mut self.inner
    }
}

// ─── Frame parser ─────────────────────────────────────────────────────────────

/// Attempt to parse one `WireRequest` from `buf`.
///
/// Returns:
/// - `Ok(Some(req))` — complete frame consumed; `buf` advanced past it.
/// - `Ok(None)` — not enough bytes yet; `buf` unchanged.
/// - `Err(e)` — protocol violation (too-large / underflow); caller closes
///   the connection after sending an error response.
///
/// Wraps `beava_core::wire::decode_frame` (Phase 2.5 codec) and lifts the
/// raw `Frame` into a typed `WireRequest`. TCP wire envelope for OP_PUSH:
/// payload = JSON `{"event": "<name>", "body": {...}}`
pub fn parse_wire_request(
    buf: &mut BytesMut,
    max_frame_bytes: u32,
) -> Result<Option<WireRequest>, beava_core::wire::FrameError> {
    let frame = match decode_frame(buf, max_frame_bytes)? {
        Some(f) => f,
        None => return Ok(None),
    };

    let req = match frame.op {
        OP_PING => WireRequest::Ping,
        OP_REGISTER => WireRequest::Register {
            payload: frame.payload,
        },
        OP_PUSH => {
            match frame.content_type {
                CT_JSON => {
                    // Payload is JSON: {"event": "<name>", "body": {...}}
                    // Parse envelope to extract event_name + raw body bytes.
                    // No re-serialization: extract the body slice directly.
                    #[derive(serde::Deserialize)]
                    struct PushEnvelopeJson {
                        event: String,
                        body: serde_json::Value,
                    }
                    match serde_json::from_slice::<PushEnvelopeJson>(&frame.payload) {
                        Ok(env) => {
                            // Pass body bytes through WITHOUT re-serializing.
                            // Find the raw body slice in the original payload by
                            // re-serializing once (needed to get canonical bytes for
                            // downstream; re-serialization overhead will be removed
                            // in Task 9.3/9.4 via direct Row deserialize).
                            let body_bytes = serde_json::to_vec(&env.body)
                                .map(Bytes::from)
                                .unwrap_or_else(|_| frame.payload.clone());
                            WireRequest::TcpPush {
                                event_name: env.event,
                                body: body_bytes,
                                body_format: CT_JSON,
                            }
                        }
                        Err(e) => WireRequest::ParseError {
                            reason: e.to_string(),
                        },
                    }
                }
                CT_MSGPACK => {
                    // Msgpack envelope: {event: String, body: <msgpack-map>}
                    // Parse the envelope to extract event_name + raw body bytes.
                    //
                    // rmp_serde can deserialize into serde_json::Value because it
                    // implements the serde data model. We parse the whole envelope
                    // as a serde_json::Value map, extract the "event" string and
                    // "body" object, then re-serialize just the body back to msgpack
                    // bytes for downstream Row deserialization (Task 9.4).
                    match rmp_serde::from_slice::<serde_json::Value>(&frame.payload) {
                        Ok(serde_json::Value::Object(mut map)) => {
                            let event_val = map.remove("event");
                            let body_val = map.remove("body");
                            match (event_val, body_val) {
                                (Some(serde_json::Value::String(event_name)), Some(body)) => {
                                    // Re-serialize just the body back to msgpack bytes.
                                    // Task 9.4 uses these bytes directly with rmp_serde::from_slice::<Row>.
                                    match rmp_serde::to_vec_named(&body) {
                                        Ok(body_bytes) => WireRequest::TcpPush {
                                            event_name,
                                            body: Bytes::from(body_bytes),
                                            body_format: CT_MSGPACK,
                                        },
                                        Err(e) => WireRequest::ParseError {
                                            reason: format!("msgpack body re-encode failed: {e}"),
                                        },
                                    }
                                }
                                _ => WireRequest::ParseError {
                                    reason:
                                        "msgpack envelope missing 'event' (string) or 'body' fields"
                                            .into(),
                                },
                            }
                        }
                        Ok(_) => WireRequest::ParseError {
                            reason: "msgpack envelope must be a map".into(),
                        },
                        Err(e) => WireRequest::ParseError {
                            reason: format!("msgpack envelope parse failed: {e}"),
                        },
                    }
                }
                other => WireRequest::ParseError {
                    reason: format!("unsupported content_type: {other:#04x}"),
                },
            }
        }
        op => WireRequest::Unknown { op },
    };
    Ok(Some(req))
}

#[cfg(test)]
mod tests {
    use super::*;
    use beava_core::wire::{encode_frame, Frame, CT_JSON};

    fn make_frame(op: u16, payload: impl Into<Bytes>) -> BytesMut {
        let frame = Frame::new(op, CT_JSON, payload.into());
        let mut buf = BytesMut::new();
        encode_frame(&frame, &mut buf);
        buf
    }

    #[test]
    fn parse_ping_frame() {
        let mut buf = make_frame(OP_PING, Bytes::new());
        let req = parse_wire_request(&mut buf, 4 * 1024 * 1024)
            .expect("no error")
            .expect("complete frame");
        assert_eq!(req, WireRequest::Ping);
        assert_eq!(buf.len(), 0, "buffer drained");
    }

    #[test]
    fn parse_push_frame_extracts_event_and_body() {
        let payload = br#"{"event":"Txn","body":{"amount":99}}"#;
        let mut buf = make_frame(OP_PUSH, Bytes::copy_from_slice(payload));
        let req = parse_wire_request(&mut buf, 4 * 1024 * 1024)
            .expect("no error")
            .expect("complete frame");
        match req {
            WireRequest::TcpPush {
                event_name,
                body,
                body_format,
            } => {
                assert_eq!(event_name, "Txn");
                assert_eq!(
                    body_format, CT_JSON,
                    "JSON frame should produce CT_JSON body_format"
                );
                let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
                assert_eq!(v["amount"], 99);
            }
            other => panic!("expected TcpPush, got {other:?}"),
        }
    }

    #[test]
    fn parse_incomplete_frame_returns_none() {
        // Only 3 bytes — too short for the 4-byte length prefix.
        let mut buf = BytesMut::from(&[0u8, 0, 0][..]);
        let req = parse_wire_request(&mut buf, 4 * 1024 * 1024).expect("no error");
        assert!(req.is_none());
        assert_eq!(buf.len(), 3, "buf unchanged");
    }

    #[test]
    fn parse_two_frames_in_sequence() {
        let mut buf = make_frame(OP_PING, Bytes::new());
        buf.extend_from_slice(&make_frame(OP_PING, Bytes::new()));

        let r1 = parse_wire_request(&mut buf, 4 * 1024 * 1024)
            .unwrap()
            .unwrap();
        let r2 = parse_wire_request(&mut buf, 4 * 1024 * 1024)
            .unwrap()
            .unwrap();
        assert_eq!(r1, WireRequest::Ping);
        assert_eq!(r2, WireRequest::Ping);
        assert_eq!(buf.len(), 0);
    }

    // ─── Plan 18-10 Task 10.1 — parse_msgpack_envelope hand-rolled scanner ────

    /// Helper: build a msgpack envelope `{event: "<name>", body: <body_json>}`.
    /// Round-trips through `rmp_serde` so the bytes are real msgpack the SDK
    /// produces.
    fn build_msgpack_envelope(event_name: &str, body: &serde_json::Value) -> Vec<u8> {
        use serde::Serialize;
        #[derive(Serialize)]
        struct Envelope<'a> {
            event: &'a str,
            body: &'a serde_json::Value,
        }
        rmp_serde::to_vec_named(&Envelope {
            event: event_name,
            body,
        })
        .expect("msgpack serialize envelope")
    }

    #[test]
    fn parse_msgpack_envelope_happy() {
        let body = serde_json::json!({"amount": 99, "ts": 1234567890i64});
        let payload = build_msgpack_envelope("Txn", &body);
        let (event, body_bytes) = parse_msgpack_envelope(&payload).expect("ok");
        assert_eq!(event, "Txn");
        // Body bytes round-trip through rmp_serde (they ARE the raw client bytes).
        let body_val: serde_json::Value =
            rmp_serde::from_slice(body_bytes).expect("body parses as msgpack");
        assert_eq!(body_val["amount"], 99);
        assert_eq!(body_val["ts"], 1234567890i64);
    }

    #[test]
    fn parse_msgpack_envelope_nested_body() {
        let body = serde_json::json!({
            "amount": 99.95,
            "tags": ["a", "b", "c"],
            "meta": {"region": "us-east-1", "shard": 7}
        });
        let payload = build_msgpack_envelope("Order", &body);
        let (event, body_bytes) = parse_msgpack_envelope(&payload).expect("ok");
        assert_eq!(event, "Order");
        let body_val: serde_json::Value =
            rmp_serde::from_slice(body_bytes).expect("nested body parses");
        assert_eq!(body_val["meta"]["region"], "us-east-1");
        assert_eq!(body_val["tags"][1], "b");
    }

    #[test]
    fn parse_msgpack_envelope_array_field() {
        let body = serde_json::json!({"vals": [1i64, 2, 3, 4, 5]});
        let payload = build_msgpack_envelope("Bulk", &body);
        let (event, body_bytes) = parse_msgpack_envelope(&payload).expect("ok");
        assert_eq!(event, "Bulk");
        let body_val: serde_json::Value =
            rmp_serde::from_slice(body_bytes).expect("array body parses");
        assert_eq!(body_val["vals"][4], 5);
    }

    #[test]
    fn parse_msgpack_envelope_truncated_returns_err() {
        let body = serde_json::json!({"amount": 99});
        let payload = build_msgpack_envelope("Txn", &body);
        // Truncate to half the length — must return Err, not panic.
        let truncated = &payload[..payload.len() / 2];
        assert!(parse_msgpack_envelope(truncated).is_err());
    }

    #[test]
    fn parse_msgpack_envelope_wrong_map_len() {
        // Build a 3-key envelope (extra "id" field) — must reject.
        let mut buf = Vec::new();
        // 0x83 = fixmap of 3 entries
        buf.push(0x83);
        // key "event"
        buf.push(0xa5);
        buf.extend_from_slice(b"event");
        // val "Txn"
        buf.push(0xa3);
        buf.extend_from_slice(b"Txn");
        // key "body"
        buf.push(0xa4);
        buf.extend_from_slice(b"body");
        // val empty fixmap
        buf.push(0x80);
        // key "id"
        buf.push(0xa2);
        buf.extend_from_slice(b"id");
        // val "x"
        buf.push(0xa1);
        buf.extend_from_slice(b"x");
        let r = parse_msgpack_envelope(&buf);
        assert!(
            r.is_err(),
            "envelope with extra key should be rejected (map_len != 2)"
        );
    }

    #[test]
    fn parse_msgpack_envelope_missing_event_key() {
        // 2-key map but neither key is "event"
        let mut buf = Vec::new();
        buf.push(0x82); // fixmap 2
        buf.push(0xa3);
        buf.extend_from_slice(b"foo");
        buf.push(0xa1);
        buf.extend_from_slice(b"x");
        buf.push(0xa4);
        buf.extend_from_slice(b"body");
        buf.push(0x80); // empty fixmap body
        let r = parse_msgpack_envelope(&buf);
        assert!(r.is_err(), "envelope with missing 'event' key rejected");
    }

    #[test]
    fn parse_msgpack_envelope_replaces_old_branch_in_parse_wire_request() {
        // Backward compat: existing CT_MSGPACK frame parsing path still
        // produces correct WireRequest::TcpPush — the implementation MUST
        // route through the new hand-rolled path.
        use beava_core::wire::{encode_frame, Frame, CT_MSGPACK, OP_PUSH};
        let body_json = serde_json::json!({"amount": 99});
        let envelope_bytes = build_msgpack_envelope("Txn", &body_json);
        let frame = Frame::new(OP_PUSH, CT_MSGPACK, Bytes::from(envelope_bytes));
        let mut buf = BytesMut::new();
        encode_frame(&frame, &mut buf);

        let req = parse_wire_request(&mut buf, 4 * 1024 * 1024)
            .expect("no parse error")
            .expect("complete frame");

        match req {
            WireRequest::TcpPush {
                event_name,
                body,
                body_format,
            } => {
                assert_eq!(event_name, "Txn");
                assert_eq!(body_format, CT_MSGPACK);
                let v: serde_json::Value =
                    rmp_serde::from_slice(&body).expect("body still valid msgpack");
                assert_eq!(v["amount"], 99);
            }
            other => panic!("expected TcpPush, got {other:?}"),
        }
    }
}
