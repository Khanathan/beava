//! Pre-encoded response templates and the `WireResponse` enum (Phase 18 Plan 04).
//!
//! Hot-path responses must NOT call `serde_json::to_vec` on the apply thread —
//! that allocates and walks the object graph on every request. Instead, common
//! responses are pre-formatted as `&'static [u8]`, and the `WireResponse` enum
//! carries raw response data that I/O threads serialize off-apply via
//! `serialize_into()`.
//!
//! # Plan 18-04 additions
//!
//! - `WireResponse` — raw (unserialised) response queued by the apply thread.
//! - `serialize_into(resp, &mut BytesMut)` — called by I/O worker threads only.
//!
//! Translation table entry #12 (18-rust-translation.md): `addReply(c, bytes)`
//! → `client.output_queue.push_back(WireResponse)` (apply) +
//!   `serialize_into(&resp, &mut write_buf)` (I/O thread).

use bytes::{BufMut, Bytes, BytesMut};

// ─── OP codes used in response frames ────────────────────────────────────────

/// TCP push-ACK response opcode (response-only, never a request opcode).
/// Wire encoding: `[u32 length][u16 OP_ACK][u8 CT_JSON][u64 lsn BE]`
const OP_ACK: u16 = 0x0080;

/// TCP error response opcode (matches Phase 2.5 wire spec OP_ERROR_RESPONSE).
const OP_ERROR_RESPONSE: u16 = 0xFFFF;

/// Content-type byte for JSON (CT_JSON from beava-core wire spec).
const CT_JSON: u8 = 0x01;

// ─── WireResponse ─────────────────────────────────────────────────────────────

/// An unserialised response queued by the apply thread into `Client::output_queue`.
///
/// The apply thread MUST NOT call `serialize_into` — that is the I/O worker's job.
/// Keeping the enum small (no allocated strings on most variants) avoids heap
/// traffic on the apply-thread hot path.
///
/// # Plan 18-04 design decisions
///
/// - `TcpAck { lsn }` — the most common response. 8-byte payload, no allocation.
/// - `TcpError { code, msg }` — error frame. `msg` is a `Bytes` (ref-counted, no copy).
/// - `HttpStatus { status, body, keep_alive }` — HTTP response. `body` is a `Bytes`.
/// - `HttpStaticOk { keep_alive }` — hot path HTTP 204, uses static template (no allocation).
///
/// Adding variants here requires a matching arm in `serialize_into`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WireResponse {
    /// TCP push-ACK: `[u32 len=11][u16 OP_ACK][u8 CT_JSON][u64 lsn BE]`
    TcpAck { lsn: u64 },

    /// TCP error frame: `[u32 len][u16 0xFFFF][u8 CT_JSON][msg bytes]`
    TcpError { code: u16, msg: Bytes },

    /// HTTP response with arbitrary status + body.
    HttpStatus {
        status: u16,
        body: Bytes,
        keep_alive: bool,
    },

    /// HTTP 204 No Content — pre-formatted static template. Zero allocation.
    HttpStaticOk { keep_alive: bool },
}

/// Serialize `resp` into `out`, appending bytes with no intermediate allocation
/// beyond `BytesMut::reserve`.
///
/// # Concurrency
///
/// Called exclusively by I/O worker threads. Never called on the apply thread.
/// No synchronization needed — each I/O worker has exclusive ownership of its
/// `write_buf: BytesMut` for the duration of the work item.
///
/// # Wire format
///
/// - `TcpAck`:        `[u32 length=11 BE][u16 OP_ACK=0x0080 BE][u8 CT_JSON=0x01][u64 lsn BE]`  (15 bytes)
/// - `TcpError`:      `[u32 length BE][u16 0xFFFF BE][u8 CT_JSON][msg bytes]`
/// - `HttpStatus`:    HTTP/1.1 header + body as ASCII bytes
/// - `HttpStaticOk`:  `HTTP/1.1 204 No Content\r\n...` static template
pub fn serialize_into(resp: &WireResponse, out: &mut BytesMut) {
    match resp {
        WireResponse::TcpAck { lsn } => {
            // frame: length(4) + op(2) + ct(1) + lsn(8) = 15 bytes
            // length field = op(2) + ct(1) + lsn(8) = 11
            out.reserve(15);
            out.put_u32(11u32); // length = 11
            out.put_u16(OP_ACK); // op = 0x0080
            out.put_u8(CT_JSON); // content_type = 0x01
            out.put_u64(*lsn); // 8-byte lsn, big-endian
        }

        WireResponse::TcpError { code: _, msg } => {
            // length = op(2) + ct(1) + msg.len()
            let length = (3 + msg.len()) as u32;
            out.reserve(4 + 3 + msg.len());
            out.put_u32(length);
            out.put_u16(OP_ERROR_RESPONSE);
            out.put_u8(CT_JSON);
            out.put_slice(msg);
        }

        WireResponse::HttpStatus {
            status,
            body,
            keep_alive,
        } => {
            let conn = if *keep_alive { "keep-alive" } else { "close" };
            // Formatted header — one allocation for the header string, then copy.
            let header = format!(
                "HTTP/1.1 {status} {reason}\r\nContent-Length: {len}\r\nConnection: {conn}\r\n\r\n",
                status = status,
                reason = http_status_reason(*status),
                len = body.len(),
                conn = conn,
            );
            out.reserve(header.len() + body.len());
            out.put_slice(header.as_bytes());
            out.put_slice(body);
        }

        WireResponse::HttpStaticOk { keep_alive } => {
            let template = if *keep_alive {
                ResponseTemplate::HTTP_204
            } else {
                ResponseTemplate::HTTP_204_CLOSE
            };
            out.reserve(template.len());
            out.put_slice(template);
        }
    }
}

/// Map a numeric HTTP status code to its canonical reason phrase.
fn http_status_reason(status: u16) -> &'static str {
    match status {
        200 => "OK",
        201 => "Created",
        204 => "No Content",
        400 => "Bad Request",
        401 => "Unauthorized",
        403 => "Forbidden",
        404 => "Not Found",
        405 => "Method Not Allowed",
        409 => "Conflict",
        422 => "Unprocessable Entity",
        429 => "Too Many Requests",
        500 => "Internal Server Error",
        503 => "Service Unavailable",
        _ => "Unknown",
    }
}

// ─── ResponseTemplate ─────────────────────────────────────────────────────────

/// Pre-encoded HTTP and TCP response byte strings.
pub struct ResponseTemplate;

impl ResponseTemplate {
    // ─── HTTP responses ────────────────────────────────────────────────────────

    /// HTTP 204 No Content — used for fire-and-forget push ACK.
    pub const HTTP_204: &'static [u8] =
        b"HTTP/1.1 204 No Content\r\nContent-Length: 0\r\nConnection: keep-alive\r\n\r\n";

    /// HTTP 204 No Content, Connection: close variant.
    pub const HTTP_204_CLOSE: &'static [u8] =
        b"HTTP/1.1 204 No Content\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";

    /// HTTP 400 Bad Request — client sent malformed JSON or wire error.
    pub const HTTP_400_HEADER: &'static [u8] =
        b"HTTP/1.1 400 Bad Request\r\nContent-Type: application/json\r\nContent-Length: ";

    /// HTTP 404 Not Found.
    pub const HTTP_404: &'static [u8] = b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n";

    /// HTTP 405 Method Not Allowed.
    pub const HTTP_405: &'static [u8] =
        b"HTTP/1.1 405 Method Not Allowed\r\nContent-Length: 0\r\n\r\n";

    /// HTTP 503 Service Unavailable (apply channel full — back-pressure).
    pub const HTTP_503: &'static [u8] =
        b"HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\nRetry-After: 0\r\n\r\n";

    /// Build an HTTP 200 OK response with a JSON body.
    ///
    /// Allocates once per response — acceptable for low-frequency paths.
    /// Hot-path push ACK uses `HTTP_204` instead.
    pub fn http_200_json(body: &[u8], keep_alive: bool) -> Bytes {
        let conn = if keep_alive { "keep-alive" } else { "close" };
        let header = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: {}\r\n\r\n",
            body.len(),
            conn
        );
        let mut out = Vec::with_capacity(header.len() + body.len());
        out.extend_from_slice(header.as_bytes());
        out.extend_from_slice(body);
        Bytes::from(out)
    }

    /// Build an HTTP 400 Bad Request with a JSON body.
    pub fn http_400_json(body: &[u8]) -> Bytes {
        let header = format!(
            "HTTP/1.1 400 Bad Request\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            body.len()
        );
        let mut out = Vec::with_capacity(header.len() + body.len());
        out.extend_from_slice(header.as_bytes());
        out.extend_from_slice(body);
        Bytes::from(out)
    }

    // ─── TCP framed responses ──────────────────────────────────────────────────

    /// Build a TCP push-ACK frame (op=OP_PUSH, CT_JSON) with the given JSON body.
    ///
    /// Wire: `[u32 length BE][u16 op=0x0010][u8 ct=0x01][payload]`
    pub fn tcp_push_ack(body: &[u8]) -> Bytes {
        use beava_core::wire::{CT_JSON, OP_PUSH};
        Self::tcp_frame(OP_PUSH, CT_JSON, body)
    }

    /// Build a TCP error frame (op=0xFFFF, CT_JSON).
    pub fn tcp_error(body: &[u8]) -> Bytes {
        use beava_core::wire::{CT_JSON, OP_ERROR_RESPONSE};
        Self::tcp_frame(OP_ERROR_RESPONSE, CT_JSON, body)
    }

    /// Low-level framer: `[u32 length][u16 op][u8 ct][payload]`.
    pub fn tcp_frame(op: u16, content_type: u8, payload: &[u8]) -> Bytes {
        // length = op(2) + ct(1) + payload
        let length = (3 + payload.len()) as u32;
        let mut out = Vec::with_capacity(4 + payload.len() + 3);
        out.extend_from_slice(&length.to_be_bytes());
        out.extend_from_slice(&op.to_be_bytes());
        out.push(content_type);
        out.extend_from_slice(payload);
        Bytes::from(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tcp_frame_encoding_matches_wire_spec() {
        // Push ACK for payload b"{}" → length=5, op=0x0010, ct=0x01
        let frame = ResponseTemplate::tcp_push_ack(b"{}");
        assert_eq!(frame.len(), 4 + 3 + 2); // 4 len + 2 op + 1 ct + 2 payload
        assert_eq!(&frame[..4], &5u32.to_be_bytes()); // length = 5
        assert_eq!(&frame[4..6], &0x0010u16.to_be_bytes()); // OP_PUSH
        assert_eq!(frame[6], 0x01); // CT_JSON
        assert_eq!(&frame[7..], b"{}");
    }

    #[test]
    fn http_200_json_contains_content_length() {
        let resp = ResponseTemplate::http_200_json(b"{\"ok\":1}", true);
        let s = std::str::from_utf8(&resp).unwrap();
        assert!(s.contains("Content-Length: 8"));
        assert!(s.contains("200 OK"));
        assert!(s.ends_with("{\"ok\":1}"));
    }

    #[test]
    fn http_404_static_bytes_valid() {
        let s = std::str::from_utf8(ResponseTemplate::HTTP_404).unwrap();
        assert!(s.contains("404"));
    }
}
