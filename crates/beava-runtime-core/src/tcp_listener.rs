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
use bytes::BytesMut;
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

// ─── Plan 18-10: Hand-rolled msgpack envelope scanner ────────────────────────
//
// Skips the rmp_serde + serde_json::Value indirection used in Plan 18-09. The
// envelope is a fixed `{event: str, body: any}` 2-key fixmap; we walk it
// byte-by-byte with rmp::decode primitives and return zero-copy slices.
//
// Target (Apple M4): ≤80 ns/op. Plan 18-09's serde-driven path was 1,928 ns.

/// Errors from `parse_msgpack_envelope`. Owned strings only on the cold error
/// path — the happy path returns borrowed slices.
#[derive(Debug)]
pub enum MsgpackEnvelopeError {
    /// Not enough bytes / malformed marker.
    Truncated,
    /// Top-level shape was not a 2-entry map.
    EnvelopeShape,
    /// Required field missing (e.g. neither key was "event").
    MissingField(&'static str),
    /// Bytes that should have been a UTF-8 string were not.
    InvalidUtf8,
    /// Map key was not a string we recognise.
    UnknownKey,
    /// Underlying rmp decode failed.
    DecodeError,
}

impl std::fmt::Display for MsgpackEnvelopeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MsgpackEnvelopeError::Truncated => f.write_str("truncated msgpack envelope"),
            MsgpackEnvelopeError::EnvelopeShape => {
                f.write_str("msgpack envelope must be a 2-entry map")
            }
            MsgpackEnvelopeError::MissingField(name) => write!(f, "missing field: {name}"),
            MsgpackEnvelopeError::InvalidUtf8 => f.write_str("invalid utf-8 in msgpack envelope"),
            MsgpackEnvelopeError::UnknownKey => {
                f.write_str("unrecognised key in msgpack envelope (expected event/body)")
            }
            MsgpackEnvelopeError::DecodeError => f.write_str("msgpack decode error"),
        }
    }
}

impl std::error::Error for MsgpackEnvelopeError {}

/// Walk one msgpack value of any type starting at `pos`, return the position
/// just past it. Recursive for container types (map/array/ext).
///
/// Implements every msgpack tag variant per the spec:
/// fixint / int8..int64 / uint8..uint64 / float32 / float64 / bool / nil
/// fixstr / str8 / str16 / str32 / bin8 / bin16 / bin32
/// fixarray / array16 / array32 / fixmap / map16 / map32
/// fixext1..16 / ext8 / ext16 / ext32 / reserved
fn skip_msgpack_value(payload: &[u8], pos: usize) -> Result<usize, MsgpackEnvelopeError> {
    if pos >= payload.len() {
        return Err(MsgpackEnvelopeError::Truncated);
    }
    let marker = payload[pos];
    let mut p = pos + 1;
    macro_rules! need {
        ($n:expr) => {
            if p + ($n) > payload.len() {
                return Err(MsgpackEnvelopeError::Truncated);
            }
        };
    }
    match marker {
        // FixPos: 0x00..=0x7f — single byte, value is in marker
        0x00..=0x7f => Ok(p),
        // FixMap: 0x80..=0x8f — len = marker & 0x0f, then 2*len values
        0x80..=0x8f => {
            let len = (marker & 0x0f) as usize;
            for _ in 0..len {
                p = skip_msgpack_value(payload, p)?; // key
                p = skip_msgpack_value(payload, p)?; // value
            }
            Ok(p)
        }
        // FixArray: 0x90..=0x9f
        0x90..=0x9f => {
            let len = (marker & 0x0f) as usize;
            for _ in 0..len {
                p = skip_msgpack_value(payload, p)?;
            }
            Ok(p)
        }
        // FixStr: 0xa0..=0xbf — len = marker & 0x1f
        0xa0..=0xbf => {
            let len = (marker & 0x1f) as usize;
            need!(len);
            Ok(p + len)
        }
        // Nil
        0xc0 => Ok(p),
        // Reserved (never used per spec) — treat as decode error
        0xc1 => Err(MsgpackEnvelopeError::DecodeError),
        // False / True
        0xc2 | 0xc3 => Ok(p),
        // bin8 / bin16 / bin32
        0xc4 => {
            need!(1);
            let len = payload[p] as usize;
            p += 1;
            need!(len);
            Ok(p + len)
        }
        0xc5 => {
            need!(2);
            let len = u16::from_be_bytes([payload[p], payload[p + 1]]) as usize;
            p += 2;
            need!(len);
            Ok(p + len)
        }
        0xc6 => {
            need!(4);
            let len = u32::from_be_bytes([
                payload[p],
                payload[p + 1],
                payload[p + 2],
                payload[p + 3],
            ]) as usize;
            p += 4;
            need!(len);
            Ok(p + len)
        }
        // ext8 / ext16 / ext32 — len bytes + 1 byte type + payload
        0xc7 => {
            need!(2);
            let len = payload[p] as usize;
            p += 2; // skip len + type
            need!(len);
            Ok(p + len)
        }
        0xc8 => {
            need!(3);
            let len = u16::from_be_bytes([payload[p], payload[p + 1]]) as usize;
            p += 3;
            need!(len);
            Ok(p + len)
        }
        0xc9 => {
            need!(5);
            let len = u32::from_be_bytes([
                payload[p],
                payload[p + 1],
                payload[p + 2],
                payload[p + 3],
            ]) as usize;
            p += 5;
            need!(len);
            Ok(p + len)
        }
        // float32
        0xca => {
            need!(4);
            Ok(p + 4)
        }
        // float64
        0xcb => {
            need!(8);
            Ok(p + 8)
        }
        // uint8 / uint16 / uint32 / uint64
        0xcc => {
            need!(1);
            Ok(p + 1)
        }
        0xcd => {
            need!(2);
            Ok(p + 2)
        }
        0xce => {
            need!(4);
            Ok(p + 4)
        }
        0xcf => {
            need!(8);
            Ok(p + 8)
        }
        // int8 / int16 / int32 / int64
        0xd0 => {
            need!(1);
            Ok(p + 1)
        }
        0xd1 => {
            need!(2);
            Ok(p + 2)
        }
        0xd2 => {
            need!(4);
            Ok(p + 4)
        }
        0xd3 => {
            need!(8);
            Ok(p + 8)
        }
        // fixext1..16 — 1 type byte + N data bytes
        0xd4 => {
            need!(2);
            Ok(p + 2)
        }
        0xd5 => {
            need!(3);
            Ok(p + 3)
        }
        0xd6 => {
            need!(5);
            Ok(p + 5)
        }
        0xd7 => {
            need!(9);
            Ok(p + 9)
        }
        0xd8 => {
            need!(17);
            Ok(p + 17)
        }
        // str8 / str16 / str32
        0xd9 => {
            need!(1);
            let len = payload[p] as usize;
            p += 1;
            need!(len);
            Ok(p + len)
        }
        0xda => {
            need!(2);
            let len = u16::from_be_bytes([payload[p], payload[p + 1]]) as usize;
            p += 2;
            need!(len);
            Ok(p + len)
        }
        0xdb => {
            need!(4);
            let len = u32::from_be_bytes([
                payload[p],
                payload[p + 1],
                payload[p + 2],
                payload[p + 3],
            ]) as usize;
            p += 4;
            need!(len);
            Ok(p + len)
        }
        // array16 / array32
        0xdc => {
            need!(2);
            let len = u16::from_be_bytes([payload[p], payload[p + 1]]) as usize;
            p += 2;
            for _ in 0..len {
                p = skip_msgpack_value(payload, p)?;
            }
            Ok(p)
        }
        0xdd => {
            need!(4);
            let len = u32::from_be_bytes([
                payload[p],
                payload[p + 1],
                payload[p + 2],
                payload[p + 3],
            ]) as usize;
            p += 4;
            for _ in 0..len {
                p = skip_msgpack_value(payload, p)?;
            }
            Ok(p)
        }
        // map16 / map32
        0xde => {
            need!(2);
            let len = u16::from_be_bytes([payload[p], payload[p + 1]]) as usize;
            p += 2;
            for _ in 0..len {
                p = skip_msgpack_value(payload, p)?;
                p = skip_msgpack_value(payload, p)?;
            }
            Ok(p)
        }
        0xdf => {
            need!(4);
            let len = u32::from_be_bytes([
                payload[p],
                payload[p + 1],
                payload[p + 2],
                payload[p + 3],
            ]) as usize;
            p += 4;
            for _ in 0..len {
                p = skip_msgpack_value(payload, p)?;
                p = skip_msgpack_value(payload, p)?;
            }
            Ok(p)
        }
        // FixNeg: 0xe0..=0xff — single byte, signed value in marker
        0xe0..=0xff => Ok(p),
    }
}

/// Read a msgpack string header starting at `pos` and return
/// `(string_bytes, position_just_past)`. Supports fixstr, str8, str16, str32.
#[inline]
fn read_msgpack_str(payload: &[u8], pos: usize) -> Result<(&[u8], usize), MsgpackEnvelopeError> {
    if pos >= payload.len() {
        return Err(MsgpackEnvelopeError::Truncated);
    }
    let marker = payload[pos];
    let mut p = pos + 1;
    let len = match marker {
        0xa0..=0xbf => (marker & 0x1f) as usize,
        0xd9 => {
            if p >= payload.len() {
                return Err(MsgpackEnvelopeError::Truncated);
            }
            let l = payload[p] as usize;
            p += 1;
            l
        }
        0xda => {
            if p + 2 > payload.len() {
                return Err(MsgpackEnvelopeError::Truncated);
            }
            let l = u16::from_be_bytes([payload[p], payload[p + 1]]) as usize;
            p += 2;
            l
        }
        0xdb => {
            if p + 4 > payload.len() {
                return Err(MsgpackEnvelopeError::Truncated);
            }
            let l = u32::from_be_bytes([
                payload[p],
                payload[p + 1],
                payload[p + 2],
                payload[p + 3],
            ]) as usize;
            p += 4;
            l
        }
        _ => return Err(MsgpackEnvelopeError::EnvelopeShape),
    };
    if p + len > payload.len() {
        return Err(MsgpackEnvelopeError::Truncated);
    }
    Ok((&payload[p..p + len], p + len))
}

/// Parse a msgpack push envelope `{event: str, body: any}` into borrowed
/// `(event_name, body_bytes)`. Zero-copy: both slices alias `payload`.
///
/// Plan 18-10 D-1 — replaces the rmp_serde::from_slice::<JsonValue> +
/// rmp_serde::to_vec_named round-trip from Plan 18-09.
pub fn parse_msgpack_envelope(payload: &[u8]) -> Result<(&str, &[u8]), MsgpackEnvelopeError> {
    if payload.is_empty() {
        return Err(MsgpackEnvelopeError::Truncated);
    }
    // Top-level must be a 2-entry fixmap (0x82). map16/map32 also legal but
    // the SDK never produces them for the envelope (always fixmap of 2).
    let first = payload[0];
    let map_len = match first {
        0x82 => 2u32,
        // map16 with len 2
        0xde if payload.len() >= 3
            && u16::from_be_bytes([payload[1], payload[2]]) == 2 =>
        {
            2
        }
        // map32 with len 2
        0xdf if payload.len() >= 5
            && u32::from_be_bytes([payload[1], payload[2], payload[3], payload[4]]) == 2 =>
        {
            2
        }
        // Any fixmap that isn't 2 entries
        0x80..=0x8f => return Err(MsgpackEnvelopeError::EnvelopeShape),
        _ => return Err(MsgpackEnvelopeError::EnvelopeShape),
    };
    if map_len != 2 {
        return Err(MsgpackEnvelopeError::EnvelopeShape);
    }

    let mut p = match first {
        0x82 => 1,
        0xde => 3,
        0xdf => 5,
        _ => unreachable!(),
    };

    let mut event_name: Option<&str> = None;
    let mut body_slice: Option<&[u8]> = None;

    for _ in 0..2 {
        let (key_bytes, after_key) = read_msgpack_str(payload, p)?;
        p = after_key;
        match key_bytes {
            b"event" => {
                let (event_bytes, after_event) = read_msgpack_str(payload, p)?;
                p = after_event;
                event_name = Some(
                    std::str::from_utf8(event_bytes).map_err(|_| MsgpackEnvelopeError::InvalidUtf8)?,
                );
            }
            b"body" => {
                let body_start = p;
                p = skip_msgpack_value(payload, p)?;
                body_slice = Some(&payload[body_start..p]);
            }
            _ => return Err(MsgpackEnvelopeError::UnknownKey),
        }
    }

    Ok((
        event_name.ok_or(MsgpackEnvelopeError::MissingField("event"))?,
        body_slice.ok_or(MsgpackEnvelopeError::MissingField("body"))?,
    ))
}

// ─── Plan 18-10: Hand-rolled JSON envelope scanner ────────────────────────────
//
// Plan 18-09's CT_JSON path used `serde_json::from_slice::<PushEnvelope>` to
// decode the envelope into `PushEnvelope { event: String, body: JsonValue }`,
// then re-serialised the body to canonical bytes. That was 583 ns/op.
//
// Plan 18-10 D-2 swaps to sonic-rs LazyValue: the envelope deserialise produces
// borrowed `(&str, raw &str)` pointing into the payload; the body bytes are
// already canonical (the original wire bytes, modulo whitespace). Target ≤150 ns.

/// Errors from `parse_json_envelope`. Cold path only.
#[derive(Debug)]
pub enum JsonEnvelopeError {
    /// sonic-rs failed to deserialise the envelope shape.
    Decode(String),
    /// `event` or `body` missing.
    MissingField(&'static str),
}

impl std::fmt::Display for JsonEnvelopeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            JsonEnvelopeError::Decode(e) => write!(f, "json envelope decode failed: {e}"),
            JsonEnvelopeError::MissingField(name) => write!(f, "missing field: {name}"),
        }
    }
}

impl std::error::Error for JsonEnvelopeError {}

/// Parse a JSON push envelope `{"event":"<name>","body":<any>}` into borrowed
/// `(event_name, body_bytes)`.
///
/// Body bytes are the EXACT canonical bytes that sonic-rs identified as the
/// `body` value's raw text — guaranteed to be a self-contained JSON value
/// (object/array/string/number/bool/null) suitable for `sonic_rs::from_slice`.
///
/// Plan 18-10 D-2 — replaces the serde_json::from_slice::<PushEnvelope> +
/// serde_json::to_vec round-trip from Plan 18-09.
pub fn parse_json_envelope(payload: &[u8]) -> Result<(&str, &[u8]), JsonEnvelopeError> {
    #[derive(serde::Deserialize)]
    struct EnvelopeLazy<'a> {
        #[serde(borrow)]
        event: &'a str,
        #[serde(borrow)]
        body: sonic_rs::LazyValue<'a>,
    }

    let env: EnvelopeLazy<'_> = sonic_rs::from_slice(payload)
        .map_err(|e| JsonEnvelopeError::Decode(e.to_string()))?;
    // as_raw_cow preserves the input lifetime ('a). When the input is borrowed
    // bytes (which is always the case here), the Cow is Borrowed and we can
    // extract the underlying &str slice with the input lifetime.
    let body_slice: &[u8] = match env.body.as_raw_cow() {
        std::borrow::Cow::Borrowed(s) => s.as_bytes(),
        std::borrow::Cow::Owned(_) => {
            // Should not happen for &[u8] input — would only occur for FastStr
            // input. Treat as decode failure.
            return Err(JsonEnvelopeError::Decode(
                "json envelope body produced owned slice (unexpected)".to_string(),
            ));
        }
    };
    Ok((env.event, body_slice))
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
                    // Plan 18-10 D-2: sonic-rs LazyValue zero-copy envelope scan.
                    // Body slice aliases frame.payload directly; no re-serialise.
                    match parse_json_envelope(&frame.payload) {
                        Ok((event_name, body_bytes)) => {
                            // Slice frame.payload to keep the Bytes refcounted view.
                            let body_start = body_bytes.as_ptr() as usize
                                - frame.payload.as_ptr() as usize;
                            let body_end = body_start + body_bytes.len();
                            let body = frame.payload.slice(body_start..body_end);
                            WireRequest::TcpPush {
                                event_name: event_name.to_string(),
                                body,
                                body_format: CT_JSON,
                            }
                        }
                        Err(e) => WireRequest::ParseError {
                            reason: e.to_string(),
                        },
                    }
                }
                CT_MSGPACK => {
                    // Plan 18-10 D-1: hand-rolled scanner via rmp::decode primitives.
                    // No serde, no JsonValue, no body re-encode — body slice aliases
                    // frame.payload directly. Target ≤80 ns vs Plan 18-09's 1,928 ns.
                    match parse_msgpack_envelope(&frame.payload) {
                        Ok((event_name, body_bytes)) => {
                            // Bytes::from triggers a refcount-bump copy out of the
                            // frame.payload Bytes. To stay zero-copy across the
                            // WireRequest boundary we slice the original Bytes.
                            let body_start = body_bytes.as_ptr() as usize
                                - frame.payload.as_ptr() as usize;
                            let body_end = body_start + body_bytes.len();
                            let body = frame.payload.slice(body_start..body_end);
                            WireRequest::TcpPush {
                                event_name: event_name.to_string(),
                                body,
                                body_format: CT_MSGPACK,
                            }
                        }
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
    use bytes::Bytes;

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

    // ─── Plan 18-10 Task 10.2 — parse_json_envelope via sonic-rs LazyValue ────

    #[test]
    fn parse_json_envelope_happy() {
        let payload = br#"{"event":"Txn","body":{"amount":99,"ts":1234567890}}"#;
        let (event, body_bytes) = parse_json_envelope(payload).expect("ok");
        assert_eq!(event, "Txn");
        // body bytes are valid JSON for {"amount":99,"ts":1234567890}
        let body_val: serde_json::Value =
            sonic_rs::from_slice(body_bytes).expect("body parses as json");
        assert_eq!(body_val["amount"], 99);
        assert_eq!(body_val["ts"], 1234567890i64);
    }

    #[test]
    fn parse_json_envelope_nested_body() {
        let payload = br#"{"event":"Order","body":{"amount":99.95,"tags":["a","b","c"],"meta":{"region":"us-east-1","shard":7}}}"#;
        let (event, body_bytes) = parse_json_envelope(payload).expect("ok");
        assert_eq!(event, "Order");
        let body_val: serde_json::Value =
            sonic_rs::from_slice(body_bytes).expect("nested body parses");
        assert_eq!(body_val["meta"]["region"], "us-east-1");
        assert_eq!(body_val["tags"][1], "b");
    }

    #[test]
    fn parse_json_envelope_array_body() {
        // body itself is an array — still valid wire content.
        let payload = br#"{"event":"Bulk","body":[1,2,3,4,5]}"#;
        let (event, body_bytes) = parse_json_envelope(payload).expect("ok");
        assert_eq!(event, "Bulk");
        let body_val: serde_json::Value =
            sonic_rs::from_slice(body_bytes).expect("array body parses");
        assert_eq!(body_val[4], 5);
    }

    #[test]
    fn parse_json_envelope_string_with_braces_in_field() {
        // String fields that contain `{` or `}` must NOT confuse the brace
        // counter in the hand-rolled fallback. (sonic-rs handles this for free
        // via its scanner; the test guards against a regression to a naive impl.)
        let payload = br#"{"event":"Note","body":{"text":"hello {world} }} {{"}}"#;
        let (event, body_bytes) = parse_json_envelope(payload).expect("ok");
        assert_eq!(event, "Note");
        let body_val: serde_json::Value =
            sonic_rs::from_slice(body_bytes).expect("string-with-braces body parses");
        assert_eq!(body_val["text"], "hello {world} }} {{");
    }

    #[test]
    fn parse_json_envelope_escaped_quote_in_string() {
        // Escaped quote inside a string must not terminate string state.
        let payload = br#"{"event":"E","body":{"q":"a\"b"}}"#;
        let (event, body_bytes) = parse_json_envelope(payload).expect("ok");
        assert_eq!(event, "E");
        let body_val: serde_json::Value =
            sonic_rs::from_slice(body_bytes).expect("escaped-quote body parses");
        assert_eq!(body_val["q"], "a\"b");
    }

    #[test]
    fn parse_json_envelope_malformed_returns_err() {
        // Missing closing brace.
        let payload = br#"{"event":"X","body":{"a":1"#;
        assert!(parse_json_envelope(payload).is_err());
    }

    #[test]
    fn parse_json_envelope_missing_event_returns_err() {
        let payload = br#"{"foo":"bar","body":{}}"#;
        assert!(parse_json_envelope(payload).is_err());
    }

    #[test]
    fn parse_json_envelope_missing_body_returns_err() {
        let payload = br#"{"event":"X","foo":"bar"}"#;
        assert!(parse_json_envelope(payload).is_err());
    }

    #[test]
    fn parse_json_envelope_replaces_old_branch_in_parse_wire_request() {
        // Backward compat: CT_JSON frame still produces the right WireRequest.
        let payload = br#"{"event":"Txn","body":{"amount":99}}"#;
        let mut buf = make_frame(OP_PUSH, Bytes::copy_from_slice(payload));
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
                assert_eq!(body_format, CT_JSON);
                let v: serde_json::Value = sonic_rs::from_slice(&body).unwrap();
                assert_eq!(v["amount"], 99);
            }
            other => panic!("expected TcpPush, got {other:?}"),
        }
    }
}
