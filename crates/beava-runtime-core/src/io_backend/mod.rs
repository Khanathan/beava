//! IoBackend trait abstraction (Plan 18-05).
//!
//! Abstracts the polling primitive used by per-worker continuous-loop workers.
//! The two concrete implementations are:
//!  - `MioBackend`: uses `mio::Poll` + `mio::Waker` (macOS kqueue, Linux epoll)
//!  - `IoUringBackend`: uses `io_uring` submission/completion queues (Linux only,
//!    feature-gated behind `--features io-uring`)
//!
//! # Per-worker model (Valkey 8 architecture)
//!
//! Each worker thread owns one `IoBackend` instance. The backend owns the
//! polling fd, the waker, and the per-client connection state. Workers run a
//! continuous loop:
//! ```text
//! loop {
//!     drain new_client_rx → backend.add_client(...)
//!     drain write_rx      → backend cache write_buf for each slot
//!     backend.poll(Some(LONG_TIMEOUT), &mut events)
//!     for ev in events {
//!         Readable(s)  → backend.read(s, &mut buf) → parse → read_tx.send(RingItem)
//!         Writable(s)  → backend.write(s, &write_buf[s])
//!         Closed(s)    → backend.close(s)
//!         Sentinel     → continue (re-drain channels)
//!     }
//! }
//! ```
//!
//! Apply thread sends to workers via `new_client_tx[w]` and `write_tx[w]`,
//! then calls `waker.wake()` to interrupt `poll()`.
//!
//! # Module naming note
//!
//! The submodule is named `mio_backend` (not `mio`) to avoid shadowing the
//! `mio` crate dependency within this module's namespace.

pub mod mio_backend;

#[cfg(all(target_os = "linux", feature = "io-uring"))]
pub mod io_uring;

#[cfg(all(target_os = "linux", feature = "io-uring"))]
pub use io_uring::IoUringBackend;
pub use mio_backend::MioBackend;

use bytes::BytesMut;
// Re-export mio's TcpStream so callers don't need to import mio directly.
pub use mio::net::TcpStream as MioTcpStream;
use std::sync::Arc;
use std::time::Duration;

/// Events produced by `IoBackend::poll()`.
#[derive(Debug, Clone, PartialEq)]
pub enum IoEvent {
    /// The client at `slot_idx` has data to read.
    Readable(u64),
    /// The client at `slot_idx` is ready for writing (edge-triggered).
    Writable(u64),
    /// The client at `slot_idx` was closed (EOF or error).
    Closed(u64),
    /// Waker sentinel — re-check channels; no I/O action needed.
    WakerSentinel,
}

/// Cross-thread waker handle. The apply thread holds one `Arc<dyn WakerHandle>`
/// per worker. Calling `wake()` causes that worker's `poll()` to return early.
pub trait WakerHandle: Send + Sync {
    fn wake(&self) -> std::io::Result<()>;
}

/// Plan 12-08 (D-B) test instrumentation: cumulative count of `wake()` calls
/// on any `WakerHandle` (across all workers). Wave 3 verifies that response
/// batching collapses N response wakes into one wake per batch.
pub static WORKER_WAKE_CALLS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Plan 12-08 test hook. Cumulative count of `WakerHandle::wake()` calls
/// since process start (counts both data-plane worker wakes and the apply-side
/// `apply_waker.wake()` fired by workers — when interpreting test results,
/// hold the shape of traffic constant so the apply-side contribution stays
/// proportional and the response-batch wakes drop visibly).
#[doc(hidden)]
pub fn worker_wake_calls() -> u64 {
    WORKER_WAKE_CALLS.load(std::sync::atomic::Ordering::Relaxed)
}

/// Abstraction over an I/O polling primitive used by one worker thread.
///
/// Each worker owns one `IoBackend` instance exclusively. No sharing.
/// All methods are `&mut self` — only the owning worker calls them.
pub trait IoBackend: Send + 'static {
    /// Create a new backend instance. Called once per worker at startup.
    fn new() -> std::io::Result<Self>
    where
        Self: Sized;

    /// Register a new client TcpStream with this backend.
    ///
    /// `slot_idx` is the deterministic client identifier.
    /// After this call, `poll()` will produce `Readable(slot_idx)` events when
    /// the socket is ready for reading.
    fn add_client(&mut self, stream: MioTcpStream, slot_idx: u64) -> std::io::Result<()>;

    /// Wait for I/O events. Appends ready events to `events_out`.
    ///
    /// Returns after:
    ///  - At least one event fired, OR
    ///  - `timeout` elapsed (if `Some`), OR
    ///  - `waker_handle().wake()` was called from another thread.
    fn poll(
        &mut self,
        timeout: Option<Duration>,
        events_out: &mut Vec<IoEvent>,
    ) -> std::io::Result<()>;

    /// Read available bytes from the client at `slot_idx` into `buf`.
    ///
    /// Reads until `WouldBlock`. Returns total bytes read (0 = EOF/closed).
    fn read(&mut self, slot_idx: u64, buf: &mut BytesMut) -> std::io::Result<usize>;

    /// Write `data` to the client at `slot_idx`.
    ///
    /// Non-blocking: writes as much as possible, returns bytes written.
    fn write(&mut self, slot_idx: u64, data: &[u8]) -> std::io::Result<usize>;

    /// Deregister and close the client at `slot_idx`.
    fn close(&mut self, slot_idx: u64);

    /// Return a clonable waker handle that can be sent to other threads.
    ///
    /// Calling `waker_handle().wake()` from the apply thread causes this
    /// backend's `poll()` to return early (within ~10-50µs on macOS/Linux).
    fn waker_handle(&self) -> Arc<dyn WakerHandle>;

    /// Change the registered write-interest for the client at `slot_idx`.
    ///
    /// When `writable=true`, the backend also fires `IoEvent::Writable` events.
    /// Pass `false` to revert to READABLE-only (avoids busy-looping).
    fn set_interest_writable(&mut self, slot_idx: u64, writable: bool);
}
