//! Per-connection client state machine.
//!
//! Modelled after Redis's per-client struct:
//! - `querybuf` → `BytesMut` read buffer
//! - `qb_pos` → tracked via `BytesMut::split_to` consume
//! - `reply` → `VecDeque<bytes::Bytes>` response queue
//!
//! I/O-thread coordination slots (`pending_parse_input`, `parsed_requests`,
//! `parse_error`) are written by I/O workers and read by the main (apply)
//! thread; correctness is guaranteed by the `IoPool::join_all()` Acquire
//! barrier (see `io_pool.rs`).

use bytes::{Bytes, BytesMut};
use std::collections::VecDeque;
use thiserror::Error;

use crate::response::WireResponse;
use crate::wire_request::WireRequest;

/// Error produced when parsing bytes from a client's read buffer fails.
///
/// On `ParseError`, the main thread closes the connection (Redis-style).
#[derive(Debug, Error)]
pub enum ParseError {
    /// The byte stream did not match the expected wire protocol.
    #[error("wire framing error: {0}")]
    FrameError(String),
}

/// Client connection state (one per connected socket).
///
/// Holds the read buffer, a pending-response queue, and a state enum.
/// The `query_buf` is filled by the I/O read phase; the parser consumes
/// from the front via `split_to`. Responses are enqueued as raw `WireResponse`
/// values by the apply thread into `output_queue`, then serialized and drained
/// by the I/O write phase via `write_buf` + `write_offset`.
#[derive(Debug)]
pub struct Client {
    /// Inbound data not yet parsed. Equivalent to Redis's `querybuf`.
    pub query_buf: BytesMut,
    /// Serialized response frames waiting to be written to the socket.
    /// Retained for compatibility with legacy callers; new code should
    /// use `output_queue` instead.
    pub pending_responses: VecDeque<Bytes>,
    /// Current connection lifecycle state.
    pub state: ClientState,

    /// Set by main when this client became readable this event-loop tick.
    /// Cleared after the I/O worker finishes parsing.
    pub pending_parse_input: bool,

    /// Parsed requests produced by the I/O worker this tick.
    /// Drained by the apply thread after `join_all()`.
    pub parsed_requests: Vec<WireRequest>,

    /// Parse error, if any. When `Some`, main closes the connection.
    pub parse_error: Option<ParseError>,

    /// Raw (unserialised) responses enqueued by the apply thread.
    /// I/O workers drain this queue and serialize each item into `write_buf`.
    /// Apply MUST NOT call `serialize_into` — that is the I/O thread's job.
    pub output_queue: VecDeque<WireResponse>,

    /// Staging buffer for serialized response bytes. Populated by the I/O
    /// write worker; flushed to the socket via `write_vectored`.
    pub write_buf: BytesMut,

    /// Number of bytes in `write_buf` that have already been sent to the kernel.
    /// Non-zero indicates a partial write; the I/O worker resumes from this
    /// offset on the next tick. Reset to 0 when `write_buf` is fully drained.
    pub write_offset: usize,
}

/// Per-client lifecycle state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClientState {
    /// Waiting to read more bytes.
    Reading,
    /// Bytes available; parser deciding next frame.
    Parsing,
    /// Response(s) queued; waiting for write-ready.
    Writing,
    /// Connection closing (error or explicit close).
    Closing,
}

impl Client {
    /// Initial read-buffer capacity (8 KiB — covers most single-frame requests).
    const INITIAL_CAPACITY: usize = 8 * 1024;

    /// Create a new client in the `Reading` state.
    pub fn new() -> Self {
        Self {
            query_buf: BytesMut::with_capacity(Self::INITIAL_CAPACITY),
            pending_responses: VecDeque::new(),
            state: ClientState::Reading,
            pending_parse_input: false,
            parsed_requests: Vec::new(),
            parse_error: None,
            output_queue: VecDeque::new(),
            write_buf: BytesMut::new(),
            write_offset: 0,
        }
    }

    /// Queue a response for writing. Transitions state to `Writing` if it
    /// isn't already `Closing`.
    pub fn push_response(&mut self, bytes: Bytes) {
        self.pending_responses.push_back(bytes);
        if self.state != ClientState::Closing {
            self.state = ClientState::Writing;
        }
    }

    /// Pop the next response to write. Returns `None` if the queue is empty.
    pub fn pop_response(&mut self) -> Option<Bytes> {
        let r = self.pending_responses.pop_front();
        if self.pending_responses.is_empty() && self.state == ClientState::Writing {
            self.state = ClientState::Reading;
        }
        r
    }

    /// True if there are bytes waiting to be written.
    pub fn has_pending_writes(&self) -> bool {
        !self.pending_responses.is_empty()
    }

    /// Enqueue a raw `WireResponse` for the I/O write phase.
    ///
    /// Apply thread calls this instead of `push_response`. The I/O worker will
    /// drain `output_queue` into `write_buf` via `serialize_into`, then flush
    /// to the socket. Apply MUST NOT call `serialize_into` itself.
    pub fn enqueue_response(&mut self, resp: WireResponse) {
        self.output_queue.push_back(resp);
        if self.state != ClientState::Closing {
            self.state = ClientState::Writing;
        }
    }

    /// True if the client has pending write work: either unserialized responses
    /// in `output_queue` or partially-flushed bytes in `write_buf`.
    pub fn has_write_work(&self) -> bool {
        !self.output_queue.is_empty() || self.write_offset < self.write_buf.len()
    }

    /// Reset `write_buf` and `write_offset` after fully draining to the socket.
    ///
    /// Called by the I/O write worker when `write_offset == write_buf.len()`.
    /// Clears the buffer so it can be reused next tick without re-allocation
    /// (BytesMut::clear keeps the backing allocation).
    pub fn reset_write_buf(&mut self) {
        self.write_buf.clear();
        self.write_offset = 0;
    }
}

impl Default for Client {
    fn default() -> Self {
        Self::new()
    }
}

/// Parse one framed TCP request from `buf`.
///
/// Executed by I/O worker threads. Operates on a `BytesMut` owned by (or
/// exclusively borrowed from) a single `Client` — no other thread touches
/// the same buffer concurrently.
///
/// Returns `Ok(Some(req))` on a complete frame, `Ok(None)` if more bytes are
/// needed, or `Err(ParseError)` on a protocol violation.
///
/// The full `Client::parse_pending` method (operating on `self.query_buf`)
/// is in `impl Client` below; this free-function variant is used in tests
/// where the buffer is passed directly.
pub fn parse_client_from_buf(buf: &mut BytesMut) -> Result<Option<WireRequest>, ParseError> {
    use crate::tcp_listener::parse_wire_request;

    const MAX_FRAME: u32 = 4 * 1024 * 1024; // 4 MiB
    parse_wire_request(buf, MAX_FRAME).map_err(|e| ParseError::FrameError(e.to_string()))
}

impl Client {
    /// Parse all complete frames from `self.query_buf` and push them into
    /// `self.parsed_requests`. On protocol error, sets `self.parse_error`.
    ///
    /// Called by the I/O worker thread work item. Returns `true` if at
    /// least one frame was parsed.
    pub fn parse_pending(&mut self) -> bool {
        use crate::tcp_listener::parse_wire_request;

        const MAX_FRAME: u32 = 4 * 1024 * 1024;
        let mut parsed_any = false;

        loop {
            match parse_wire_request(&mut self.query_buf, MAX_FRAME) {
                Ok(Some(req)) => {
                    self.parsed_requests.push(req);
                    parsed_any = true;
                }
                Ok(None) => break,
                Err(e) => {
                    self.parse_error = Some(ParseError::FrameError(e.to_string()));
                    break;
                }
            }
        }

        self.pending_parse_input = false;
        parsed_any
    }
}
