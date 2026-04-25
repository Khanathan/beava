//! Per-connection client state machine (Phase 18 Plan 01).
//!
//! Translation table entry #9–#12 (18-rust-translation.md):
//! - `querybuf` → `BytesMut` read buffer
//! - `qb_pos` → tracked via `BytesMut::split_to` consume
//! - `reply` → `VecDeque<bytes::Bytes>` response queue
//!
//! Phase 18-01: scaffold. I/O-thread-aware fields (per-thread assignment,
//! atomic flags) added in Plan 18-03.

use bytes::{Bytes, BytesMut};
use std::collections::VecDeque;

/// Client connection state (one per connected socket).
///
/// Holds the read buffer, a pending-response queue, and a state enum.
/// The `query_buf` is filled by the I/O read phase; the parser consumes
/// from the front via `split_to`. Responses are pushed to `pending_responses`
/// by the apply thread and written out by the I/O write phase.
#[derive(Debug)]
pub struct Client {
    /// Inbound data not yet parsed. Equivalent to Redis's `querybuf`.
    pub query_buf: BytesMut,
    /// Serialized response frames waiting to be written to the socket.
    /// Translation table entry #11: `VecDeque<Bytes>`.
    pub pending_responses: VecDeque<Bytes>,
    /// Current connection lifecycle state.
    pub state: ClientState,
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
}

impl Default for Client {
    fn default() -> Self {
        Self::new()
    }
}
