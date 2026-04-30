//! HTTP/1.1 listener + request parser for Phase 18's hand-rolled event loop.
//!
//! Uses `httparse` for zero-copy header parsing (same library hyper uses).
//! Wraps `mio::net::TcpListener` bound on the HTTP data-plane port.
//!
//! # Parser design
//!
//! `parse_http_request` is a synchronous, resumable parser over a `BytesMut`
//! read buffer. It returns:
//! - `Ok(Some((WireRequest, keep_alive)))` — a complete request was consumed.
//! - `Ok(None)` — not enough bytes yet; caller reads more and retries.
//! - `Err(ParseError)` — malformed request; caller should close the connection.
//!
//! Pipelining is handled naturally: the caller loops calling `parse_http_request`
//! until it returns `Ok(None)`, processing each `WireRequest` inline.
//!
//! # Body handling
//!
//! Two modes:
//! 1. `Content-Length: N` — read exactly N bytes after `\r\n\r\n`.
//! 2. `Transfer-Encoding: chunked` — parse chunk-size lines + data + terminator.
//!
//! For requests with no body (GET, DELETE with no Content-Length), body is empty.

use bytes::{Bytes, BytesMut};
use std::net::SocketAddr;
use thiserror::Error;

use crate::router::Router;
use crate::wire_request::WireRequest;

/// Error from the HTTP request parser.
#[derive(Debug, Error)]
pub enum ParseError {
    #[error("httparse error: {0}")]
    Httparse(#[from] httparse::Error),
    #[error("invalid Content-Length: {0}")]
    InvalidContentLength(String),
    #[error("malformed chunked encoding: {0}")]
    BadChunked(String),
    #[error("request too large")]
    TooLarge,
}

/// Maximum header size (8 KiB — matches nginx default).
const MAX_HEADER_BYTES: usize = 8 * 1024;
/// Maximum number of headers parsed by httparse.
const MAX_HEADERS: usize = 64;
/// Maximum body size (4 MiB — same as TCP max_frame_bytes default).
const MAX_BODY_BYTES: usize = 4 * 1024 * 1024;

/// Attempt to parse one complete HTTP/1.1 request from `buf`.
///
/// Returns `Ok(Some((req, keep_alive)))` when a full request (headers + body)
/// is available, advancing `buf` past the consumed bytes.
/// Returns `Ok(None)` when more bytes are needed.
/// Returns `Err(ParseError)` on protocol violation.
///
/// Supports:
/// - `Content-Length` body framing
/// - `Transfer-Encoding: chunked` body framing
/// - `Connection: keep-alive` / `Connection: close` detection
/// - Pipelining (call repeatedly until `Ok(None)`)
pub fn parse_http_request(buf: &mut BytesMut) -> Result<Option<(WireRequest, bool)>, ParseError> {
    // httparse needs a slice of Header slots pre-allocated on the stack/heap.
    let mut headers = vec![httparse::EMPTY_HEADER; MAX_HEADERS];
    let mut req = httparse::Request::new(&mut headers);

    // Guard against absurdly large headers causing an OOM probe — but ONLY
    // for header bytes. Phase 19.1.1: the cap previously applied to the whole
    // buffer, which (incorrectly) rejected any HTTP request whose headers +
    // body combined exceeded 8 KiB, even when Content-Length was well within
    // MAX_BODY_BYTES = 4 MiB. The body cap below at the Content-Length check
    // was unreachable. Fix: scan for `\r\n\r\n` to detect whether headers are
    // complete. If yes — body bytes follow and are gated by Content-Length.
    // If no — and we already have > MAX_HEADER_BYTES of unfinished headers —
    // it's a genuine header bomb; reject as before.
    let header_end_found = buf.windows(4).any(|w| w == b"\r\n\r\n");
    if !header_end_found && buf.len() > MAX_HEADER_BYTES {
        return Err(ParseError::TooLarge);
    }
    let probe = buf.as_ref();

    let header_end = match req.parse(probe)? {
        httparse::Status::Complete(n) => n,
        httparse::Status::Partial => return Ok(None), // need more header bytes
    };

    // Extract the fields we need before we release the borrow on `headers`.
    let method = req.method.unwrap_or("").to_owned();
    let path = req.path.unwrap_or("").to_owned();

    // Determine keep-alive from the Connection header (default keep-alive for HTTP/1.1).
    let mut keep_alive = true; // HTTP/1.1 default
    let mut content_length: Option<usize> = None;
    let mut is_chunked = false;
    // Plan 12.6-14: capture Content-Type so we can return 415 for POST
    // endpoints when the client sends a non-JSON media type. None means
    // header was absent; Some("") means header was present but empty.
    let mut content_type_header: Option<String> = None;

    for h in req.headers.iter() {
        let name_lower = h.name.to_ascii_lowercase();
        match name_lower.as_str() {
            "connection" => {
                let val = std::str::from_utf8(h.value)
                    .unwrap_or("")
                    .to_ascii_lowercase();
                if val.contains("close") {
                    keep_alive = false;
                } else if val.contains("keep-alive") {
                    keep_alive = true;
                }
            }
            "content-length" => {
                let val = std::str::from_utf8(h.value)
                    .map_err(|e| ParseError::InvalidContentLength(e.to_string()))?
                    .trim();
                content_length = Some(
                    val.parse::<usize>()
                        .map_err(|e| ParseError::InvalidContentLength(e.to_string()))?,
                );
            }
            "transfer-encoding" => {
                let val = std::str::from_utf8(h.value)
                    .unwrap_or("")
                    .to_ascii_lowercase();
                if val.contains("chunked") {
                    is_chunked = true;
                }
            }
            "content-type" => {
                content_type_header = Some(
                    std::str::from_utf8(h.value)
                        .unwrap_or("")
                        .trim()
                        .to_owned(),
                );
            }
            _ => {}
        }
    }

    // Now read the body based on framing mode.
    let body: Bytes = if is_chunked {
        // Parse chunked encoding from buf starting at header_end.
        let body_start = header_end;
        match parse_chunked_body(&buf[body_start..]) {
            ChunkedResult::Complete {
                body,
                bytes_consumed,
            } => {
                // We have a complete chunked body.
                let total = body_start + bytes_consumed;
                // Advance the buffer past headers + body.
                let _ = buf.split_to(total);
                body
            }
            ChunkedResult::Incomplete => return Ok(None),
            ChunkedResult::Error(e) => return Err(ParseError::BadChunked(e)),
        }
    } else if let Some(len) = content_length {
        if len > MAX_BODY_BYTES {
            return Err(ParseError::TooLarge);
        }
        let body_start = header_end;
        let body_end = body_start + len;
        if buf.len() < body_end {
            return Ok(None); // need more body bytes
        }
        // Advance buf past headers + body.
        let mut taken = buf.split_to(body_end);
        taken.advance(body_start);
        taken.freeze()
    } else {
        // No body framing — consume headers only.
        let _ = buf.split_to(header_end);
        Bytes::new()
    };

    // Route the parsed request.
    let route = Router::route(&method, &path);
    use crate::router::Route;
    // HTTP always carries JSON bodies (content_type = CT_JSON = 0x01).
    use beava_core::wire::CT_JSON;

    // Plan 12.6-14: enforce application/json on POST /register only —
    // matches legacy axum `register::post_register` which had the
    // `is_json_content_type` check. Other POST endpoints in legacy axum
    // did not validate Content-Type and just attempted JSON parse,
    // returning 400 on failure. Replicating that posture here keeps
    // every existing test (event_loop_smoke / phase8_tcp_push wire
    // shape / phase18_07_push_and_get) untouched while turning the
    // phase2_smoke success_criterion_5 415 contract green.
    let is_register_post =
        method.eq_ignore_ascii_case("POST") && matches!(route, Route::Register);
    if is_register_post {
        let ct_ok = match &content_type_header {
            None => false,
            Some(ct) => is_json_content_type(ct),
        };
        if !ct_ok {
            let received = content_type_header.clone().unwrap_or_default();
            return Ok(Some((
                WireRequest::HttpUnsupportedMediaType {
                    received,
                    path: path.clone(),
                },
                keep_alive,
            )));
        }
    }

    // Strip query-string from path for routing carriers that don't use it.
    // For Route::TableGet we pass the query through to the dispatcher.
    let query_string = match path.split_once('?') {
        Some((_p, q)) => q.to_owned(),
        None => String::new(),
    };

    let wire_req = match route {
        Route::Push { event_name } => WireRequest::HttpPush {
            event_name,
            body,
            body_format: CT_JSON,
        },
        Route::PushSync { event_name } => WireRequest::HttpPushSync {
            event_name,
            body,
            body_format: CT_JSON,
        },
        Route::PushBatch { event_name } => WireRequest::HttpPushBatch {
            event_name,
            body,
            body_format: CT_JSON,
        },
        Route::Get => WireRequest::HttpGet { body },
        Route::GetSingle { feature, key } => WireRequest::HttpGetSingle { feature, key },
        Route::Upsert { table } => WireRequest::HttpUpsert { table, body },
        Route::Delete { table } => WireRequest::HttpDelete { table, body },
        Route::Retract => WireRequest::HttpRetract { body },
        Route::TableGet { table } => WireRequest::HttpTableGet {
            table,
            query: query_string,
        },
        Route::Register => WireRequest::Register { payload: body },
        // Plan 12-07: /health on the data-plane HTTP port — inline shim, no
        // apply-thread roundtrip. read_bench.py polls /health at startup.
        Route::Health => WireRequest::HttpHealth,
        // Plan 12.6-01: /ready and /registry on the data-plane HTTP port —
        // back-compat shims for TestServer-using tests; the admin sidecar
        // remains the canonical home for both endpoints (per
        // `project_phase18_no_dual_runtime`).
        Route::Ready => WireRequest::HttpReady,
        Route::Registry => WireRequest::HttpRegistry,
        Route::NotFound => WireRequest::HttpNotFound {
            path: path.to_owned(),
        },
        Route::MethodNotAllowed => WireRequest::HttpMethodNotAllowed {
            method: method.to_owned(),
            path: path.to_owned(),
        },
    };

    Ok(Some((wire_req, keep_alive)))
}

/// Plan 12.6-14: returns true iff the Content-Type media type (before `;`)
/// is `application/json` (case-insensitive, trimmed). Mirrors the legacy
/// axum `register::is_json_content_type` semantics so the mio path emits
/// 415 for the same set of bad media types the legacy axum handler did.
fn is_json_content_type(ct: &str) -> bool {
    let media_type = ct.split(';').next().unwrap_or("").trim();
    media_type.eq_ignore_ascii_case("application/json")
}

// ─── Chunked transfer encoding parser ─────────────────────────────────────────

enum ChunkedResult {
    Complete { body: Bytes, bytes_consumed: usize },
    Incomplete,
    Error(String),
}

/// Parse a complete chunked-encoded body from a byte slice.
///
/// Chunk format: `<hex-size>\r\n<data>\r\n` ... `0\r\n\r\n`
/// We accumulate all chunk data into a Vec<u8>.
fn parse_chunked_body(data: &[u8]) -> ChunkedResult {
    let mut pos = 0;
    let mut body = Vec::new();

    loop {
        // Find the \r\n that terminates the chunk-size line.
        let line_end = match find_crlf(data, pos) {
            Some(i) => i,
            None => return ChunkedResult::Incomplete,
        };
        let size_str = match std::str::from_utf8(&data[pos..line_end]) {
            Ok(s) => s.trim(),
            Err(_) => return ChunkedResult::Error("non-UTF8 chunk size line".to_owned()),
        };

        // Strip optional chunk extensions (everything after ';').
        let size_str = size_str.split(';').next().unwrap_or("").trim();

        let chunk_size = match usize::from_str_radix(size_str, 16) {
            Ok(n) => n,
            Err(e) => return ChunkedResult::Error(format!("bad chunk size '{size_str}': {e}")),
        };

        // Advance past the size line + CRLF.
        pos = line_end + 2;

        if chunk_size == 0 {
            // Final chunk — need the trailing \r\n.
            if data.len() < pos + 2 {
                return ChunkedResult::Incomplete;
            }
            // Consume the trailing \r\n.
            pos += 2;
            return ChunkedResult::Complete {
                body: Bytes::from(body),
                bytes_consumed: pos,
            };
        }

        // Need chunk_size bytes + trailing CRLF.
        let data_end = pos + chunk_size;
        if data.len() < data_end + 2 {
            return ChunkedResult::Incomplete;
        }
        body.extend_from_slice(&data[pos..data_end]);
        pos = data_end + 2; // skip trailing \r\n
    }
}

/// Find the byte offset of the next `\r\n` in `data` starting at `from`.
/// Returns the index of `\r` (so data[result..result+2] == b"\r\n").
fn find_crlf(data: &[u8], from: usize) -> Option<usize> {
    let slice = &data[from..];
    for i in 0..slice.len().saturating_sub(1) {
        if slice[i] == b'\r' && slice[i + 1] == b'\n' {
            return Some(from + i);
        }
    }
    None
}

// Needed for split_to + advance.
use bytes::Buf;

/// A mio-backed TCP listener for HTTP/1.1 connections.
///
/// Phase 18-01 scaffold: binds the listener, exposes `accept()` for the
/// event loop when the HTTP listener token fires. Full HTTP state machine
/// (headers + body + keep-alive + chunked TE) added in Task 1.3.
pub struct HttpListener {
    inner: mio::net::TcpListener,
    local_addr: SocketAddr,
}

impl std::fmt::Debug for HttpListener {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HttpListener")
            .field("local_addr", &self.local_addr)
            .finish()
    }
}

impl HttpListener {
    /// Bind an HTTP/1.1 listener. Port 0 = OS-assigned.
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

    /// Actual bound address.
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// Accept the next pending HTTP connection.
    pub fn accept(&self) -> std::io::Result<(mio::net::TcpStream, SocketAddr)> {
        self.inner.accept()
    }

    /// Borrow the inner mio listener for event loop registration.
    pub fn inner_mut(&mut self) -> &mut mio::net::TcpListener {
        &mut self.inner
    }
}
