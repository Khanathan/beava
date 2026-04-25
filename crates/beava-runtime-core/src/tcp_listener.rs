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

use beava_core::wire::{decode_frame, OP_PING, OP_PUSH, OP_REGISTER};
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
        OP_REGISTER => WireRequest::Register { payload: frame.payload },
        OP_PUSH => {
            // Payload is JSON: {"event": "<name>", "body": {...}}
            // Parse the envelope to extract event_name + body bytes.
            #[derive(serde::Deserialize)]
            struct PushEnvelope {
                event: String,
                body: serde_json::Value,
            }
            match serde_json::from_slice::<PushEnvelope>(&frame.payload) {
                Ok(env) => {
                    // Re-serialise just the body so downstream apply gets raw bytes.
                    let body_bytes = serde_json::to_vec(&env.body)
                        .map(Bytes::from)
                        .unwrap_or_else(|_| frame.payload.clone());
                    WireRequest::TcpPush {
                        event_name: env.event,
                        body: body_bytes,
                    }
                }
                Err(e) => WireRequest::ParseError {
                    reason: e.to_string(),
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
            WireRequest::TcpPush { event_name, body } => {
                assert_eq!(event_name, "Txn");
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

        let r1 = parse_wire_request(&mut buf, 4 * 1024 * 1024).unwrap().unwrap();
        let r2 = parse_wire_request(&mut buf, 4 * 1024 * 1024).unwrap().unwrap();
        assert_eq!(r1, WireRequest::Ping);
        assert_eq!(r2, WireRequest::Ping);
        assert_eq!(buf.len(), 0);
    }
}
