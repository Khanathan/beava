//! Plan 18-05 Task 5.4 — io_uring backend smoke test (Linux only).
//!
//! RED: Fails to compile on Linux until `IoUringBackend` is implemented in
//! `crates/beava-runtime-core/src/io_backend/io_uring.rs`.
//!
//! macOS: this entire file is cfg-gated and produces no code; build is always
//! clean on macOS. The gate is intentional — io_uring is a Linux-specific API.

#![cfg(target_os = "linux")]
#![cfg(feature = "io-uring")]

use beava_runtime_core::io_backend::{IoBackend, IoEvent, IoUringBackend};
use bytes::BytesMut;
use std::io::Write;
use std::time::Duration;

/// Smoke: IoUringBackend constructs, accepts one client, polls, reads bytes.
///
/// Run on Linux CI (will FAIL until 5.4.b implements IoUringBackend).
#[test]
fn test_io_uring_smoke_recv_one_frame() {
    // Create the backend.
    let mut backend = IoUringBackend::new().expect("IoUringBackend::new()");

    // Open a TCP loopback listener.
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("listener");
    let addr = listener.local_addr().unwrap();

    // Connect + send bytes from a background thread.
    let t = std::thread::spawn(move || {
        let mut s = std::net::TcpStream::connect(addr).expect("connect");
        s.write_all(b"hello io_uring").expect("write");
        drop(s);
    });

    let (stream, _) = listener.accept().expect("accept");
    stream.set_nonblocking(true).expect("set_nonblocking");
    let mio_stream = mio::net::TcpStream::from_std(stream);

    // Register with the io_uring backend.
    backend.add_client(mio_stream, 7).expect("add_client");

    // Poll until readable or timeout.
    let mut events: Vec<IoEvent> = Vec::new();
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    loop {
        events.clear();
        backend
            .poll(Some(Duration::from_millis(100)), &mut events)
            .expect("poll");
        let done = events
            .iter()
            .any(|e| matches!(e, IoEvent::Readable(s) | IoEvent::Closed(s) if *s == 7));
        if done || std::time::Instant::now() >= deadline {
            break;
        }
    }

    // Read the bytes.
    let mut buf = BytesMut::new();
    let n = backend.read(7, &mut buf).expect("read");
    assert!(n > 0, "expected bytes from io_uring recv, got 0");
    assert!(
        buf.starts_with(b"hello"),
        "expected 'hello io_uring' prefix, got: {buf:?}"
    );

    backend.close(7);
    t.join().expect("thread join");
}
