//! Pre-encoded response templates for the hand-rolled event loop (Phase 18).
//!
//! Hot-path responses must NOT call `serde_json::to_vec` — that allocates and
//! walks the object graph on every request. Instead, common responses are
//! pre-formatted once as `&'static [u8]` or `Bytes::from_static(...)`.
//!
//! Translation table entry #12 (18-rust-translation.md): `addReply(c, bytes)`
//! → `client.pending_responses.push_back(bytes)` with pre-encoded payload.

use bytes::Bytes;

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
