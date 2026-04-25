//! Hand-rolled mio-based event loop (Phase 18 Plan 01).
//!
//! Mirrors Redis's `aeMain` / `aeProcessEvents` pattern (see 18-redis-research.md §1).
//! Translation table entry #1 (18-rust-translation.md).
//!
//! # Architecture
//!
//! The `EventLoop` wraps `mio::Poll` and a `mio::Events` buffer. Callers:
//!   1. Create the loop with `EventLoop::new()`.
//!   2. Register listener sources (TCP + HTTP) via `register_listener()`.
//!   3. Drive the loop with `tick()` — one iteration of poll + event dispatch.
//!      The server's main thread calls `tick()` in a `loop {}`.
//!
//! All per-tick work (WAL flush, I/O thread dispatch) will be added in
//! subsequent tasks and plans. This file intentionally stays small.

use std::time::Duration;
use thiserror::Error;

/// Errors produced by the event loop.
#[derive(Debug, Error)]
pub enum EventLoopError {
    #[error("failed to create mio Poll: {0}")]
    PollCreate(#[source] std::io::Error),
    #[error("poll error on tick: {0}")]
    PollTick(#[source] std::io::Error),
    #[error("failed to register source: {0}")]
    Register(#[source] std::io::Error),
}

/// A mio-backed event loop.
///
/// Owns a `mio::Poll` instance and an `Events` buffer. Each call to `tick()`
/// blocks until at least one I/O event is ready (or the timeout expires), then
/// returns the count of events fired.
///
/// Phase 18-01: basic scaffold only. I/O thread dispatch + before_sleep hooks
/// added in 18-03/18-04.
pub struct EventLoop {
    poll: mio::Poll,
    events: mio::Events,
}

impl std::fmt::Debug for EventLoop {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EventLoop").finish_non_exhaustive()
    }
}

impl EventLoop {
    /// Default event buffer capacity. Matches Redis's default (`AE_SETSIZE = 10000`).
    const DEFAULT_EVENT_CAPACITY: usize = 1024;

    /// Create a new event loop backed by `mio::Poll`.
    ///
    /// Returns `Err` if the OS fails to create the epoll/kqueue fd.
    pub fn new() -> Result<Self, EventLoopError> {
        let poll = mio::Poll::new().map_err(EventLoopError::PollCreate)?;
        let events = mio::Events::with_capacity(Self::DEFAULT_EVENT_CAPACITY);
        Ok(Self { poll, events })
    }

    /// Create with a custom event buffer capacity.
    pub fn with_capacity(capacity: usize) -> Result<Self, EventLoopError> {
        let poll = mio::Poll::new().map_err(EventLoopError::PollCreate)?;
        let events = mio::Events::with_capacity(capacity);
        Ok(Self { poll, events })
    }

    /// Register a mio event source with the poll.
    ///
    /// `token` must be unique per source (caller owns the token namespace).
    /// `interest` is typically `Interest::READABLE` for listeners,
    /// `Interest::READABLE | Interest::WRITABLE` for connected client sockets.
    pub fn register<S>(&mut self, source: &mut S, token: mio::Token, interest: mio::Interest)
        -> Result<(), EventLoopError>
    where
        S: mio::event::Source,
    {
        self.poll
            .registry()
            .register(source, token, interest)
            .map_err(EventLoopError::Register)
    }

    /// Re-register a previously registered source with new interest flags.
    pub fn reregister<S>(&mut self, source: &mut S, token: mio::Token, interest: mio::Interest)
        -> Result<(), EventLoopError>
    where
        S: mio::event::Source,
    {
        self.poll
            .registry()
            .reregister(source, token, interest)
            .map_err(EventLoopError::Register)
    }

    /// Deregister a source from the poll.
    pub fn deregister<S>(&mut self, source: &mut S) -> Result<(), EventLoopError>
    where
        S: mio::event::Source,
    {
        self.poll
            .registry()
            .deregister(source)
            .map_err(EventLoopError::Register)
    }

    /// One iteration of the event loop — polls for I/O events, then returns
    /// an iterator over what fired.
    ///
    /// `timeout`: `None` = block indefinitely, `Some(d)` = return after `d` even
    /// if no events fired. Callers driving a spin loop should pass a short
    /// timeout (e.g., `Some(Duration::from_millis(1))`) to avoid starvation.
    pub fn tick(&mut self, timeout: Option<Duration>)
        -> Result<impl Iterator<Item = &mio::event::Event>, EventLoopError>
    {
        self.poll
            .poll(&mut self.events, timeout)
            .map_err(EventLoopError::PollTick)?;
        Ok(self.events.iter())
    }

    /// Borrow the mio registry directly for registering sources that need a
    /// `&Registry` (e.g., `mio::net::TcpListener`).
    pub fn registry(&self) -> &mio::Registry {
        self.poll.registry()
    }
}
