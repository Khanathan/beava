//! Plan 18-13: Cross-thread ring item type for the IoPool worker → apply
//! thread channel.
//!
//! Replaces the per-tick `MioClient.parsed_requests` + `parsed_rows` Vec batch
//! publish — events flow continuously through a `crossbeam_channel::bounded`
//! channel rather than being collected into per-client Vecs and drained after
//! a `join_all` spin barrier.
//!
//! The channel primitive is `crossbeam-channel` (rather than `rtrb`) because
//! `Sender` is `Send + Sync + Clone` — multiple IoPool workers can share one
//! sender via `clone()` without thread-local hacks. ~80 ns send/recv overhead;
//! adequate at our 1M EPS target (~1 µs/event budget).

use crate::wire_request::WireRequest;
use beava_core::row::Row;

/// A unit of work pushed by an IoPool worker thread into the apply thread's
/// drain channel.
///
/// The `Request` variant is large (~533 bytes — dominated by `Row`'s inline
/// SmallVec storage). Boxing `parsed_row` would trade the size cost for a
/// per-event heap alloc; at our throughputs (~1M EPS) the alloc cost
/// (~30 ns/event) matches the copy cost (~30 ns for 533 bytes through a
/// channel). Net: leave it inline; the compiler-suggested Box<Row> doesn't
/// help in this regime.
#[allow(clippy::large_enum_variant)]
#[derive(Debug)]
pub enum RingItem {
    /// A successfully parsed request (push, register, etc.).
    Request {
        /// Slot index of the originating `MioClient` (for routing the response).
        slot_idx: u32,
        /// Whether the response should mark the connection close-after-write
        /// (HTTP keep-alive=false; TCP always false).
        keep_alive: bool,
        /// The parsed wire request.
        request: WireRequest,
        /// Optional pre-deserialized Row for push variants. `None` for non-push
        /// requests or when body→Row deserialization failed (apply retries
        /// inline and emits `invalid_event` if needed).
        parsed_row: Option<Row>,
    },
    /// A parse error from the read-phase IoPool worker. Apply emits the error
    /// response on the client's output queue and marks the client closed.
    ParseError {
        /// Slot index of the originating `MioClient`.
        slot_idx: u32,
        /// The error classification (TCP frame vs HTTP protocol).
        kind: ParseErrorKind,
    },
}

/// Parse-error classification. Mirrors `MioParseError` in
/// `beava-server/src/server.rs` — duplicated here to keep the apply-side
/// glue layer dep-only on `beava-runtime-core`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParseErrorKind {
    /// Wire-protocol framing error on the TCP path (generic).
    TcpFrame,
    /// Plan 12.6-15: declared frame length exceeded the
    /// `tcp_max_frame_bytes` limit. Carries `(declared, limit)` so the
    /// apply path can frame `frame_too_large` with the precise limit
    /// (criterion 7).
    TcpFrameTooLarge { declared: u32, limit: u32 },
    /// HTTP/1.1 protocol violation.
    HttpProtocol,
}

/// Plan 12-08 (D-B): batch-send extension for the apply → IO worker
/// `write_rx` channel.
///
/// crossbeam_channel doesn't have a native `send_many`; we just send each
/// item individually. The amortization comes from the CALLER firing the
/// worker's `Waker::wake()` ONCE after the batch (instead of once per
/// response). `Sender::send` itself is ~80 ns; firing the mio Waker is
/// ~1 µs (cross-thread eventfd/kqueue write + worker poll wake).
///
/// Net effect under steady-state push load: one `Waker::wake()` per
/// (drain pass × affected worker) ÷ 16 responses, instead of per response.
pub trait WriteRingExt<T> {
    fn send_batch(&self, items: Vec<T>) -> Result<(), crossbeam_channel::SendError<T>>;
}

impl<T> WriteRingExt<T> for crossbeam_channel::Sender<T> {
    fn send_batch(&self, items: Vec<T>) -> Result<(), crossbeam_channel::SendError<T>> {
        for item in items {
            self.send(item)?;
        }
        Ok(())
    }
}
