//! Framed TCP listener for Phase 18's hand-rolled event loop.
//!
//! Wraps `mio::net::TcpListener` and provides accept + frame-parse helpers.
//! Wire format: `[u32 length BE][u16 op BE][u8 content_type][payload]`
//! (Phase 2.5 framing, re-uses `beava_core::wire` codec).

use std::net::SocketAddr;

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
