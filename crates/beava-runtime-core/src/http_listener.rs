//! HTTP/1.1 listener for Phase 18's hand-rolled event loop.
//!
//! Uses `httparse` for zero-copy header parsing (same library hyper uses).
//! Wraps `mio::net::TcpListener` bound on the HTTP data-plane port.

use std::net::SocketAddr;

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
