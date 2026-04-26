//! Plan 18-05 Task 5.1 — IoBackend trait conformance test.
//!
//! RED: Tests that `IoBackend` trait and `MioBackend` impl exist and conform to
//! the per-worker continuous-loop architecture. Fails to compile until 5.1.b
//! (GREEN) lands the implementation.
//!
//! The test creates a `MioBackend` (the per-worker mio::Poll adapter), adds a
//! client, polls for events, and verifies wake/waker mechanics work.

use beava_runtime_core::io_backend::{IoBackend, IoEvent, MioBackend, WakerHandle};
use bytes::BytesMut;
use std::io::Write;
use std::net::TcpStream;
use std::sync::Arc;
use std::time::Duration;

/// Smoke test: `MioBackend` can be constructed, client added, waker obtained,
/// and poll returns without error.
#[test]
fn test_iobackend_trait_uniform() {
    // MioBackend::new() must work on any OS.
    let mut backend = MioBackend::new().expect("MioBackend::new()");

    // Waker handle must be available before any client is added.
    let waker: Arc<dyn WakerHandle> = backend.waker_handle();
    // wake() with no client registered must not error.
    waker.wake().expect("wake with no clients");

    // Open a TCP loopback listener to get a real connection.
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("listener");
    let addr = listener.local_addr().unwrap();

    // Connect a client from a background thread.
    let t = std::thread::spawn(move || {
        let mut s = TcpStream::connect(addr).expect("connect");
        // Write some data so the socket is readable.
        s.write_all(b"hello").expect("write");
        s
    });
    let (stream, _) = listener.accept().expect("accept");
    // Discard the background TcpStream (keep it alive until thread finishes).

    // Convert std::net::TcpStream → mio::net::TcpStream.
    let mio_stream = mio::net::TcpStream::from_std(stream);

    // add_client must succeed.
    let slot_idx: u64 = 42;
    backend.add_client(mio_stream, slot_idx).expect("add_client");

    // poll() with a short timeout. May get 0 or more events.
    let mut events: Vec<IoEvent> = Vec::new();
    backend
        .poll(Some(Duration::from_millis(50)), &mut events)
        .expect("poll");

    // read() should return data if the socket was readable.
    // (Not always readable in first poll — that's fine; we just assert no panic.)
    let mut buf = BytesMut::new();
    let _n = backend.read(slot_idx, &mut buf).unwrap_or(0);

    // set_interest_writable should not error.
    backend.set_interest_writable(slot_idx, true);
    backend.set_interest_writable(slot_idx, false);

    // close() must succeed.
    backend.close(slot_idx);

    // Join the background thread (it holds the other side).
    let _ = t.join();
}

/// Test that WakerHandle::wake() from a different thread causes poll() to return.
#[test]
fn test_iobackend_waker_cross_thread_wake() {
    let mut backend = MioBackend::new().expect("MioBackend::new()");
    let waker = backend.waker_handle();

    // Spawn a thread that wakes the backend after 20ms.
    let t = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(20));
        waker.wake().expect("cross-thread wake");
    });

    // poll with a long timeout — should return early (via WakerSentinel or within 200ms).
    let mut events: Vec<IoEvent> = Vec::new();
    let start = std::time::Instant::now();
    backend
        .poll(Some(Duration::from_millis(500)), &mut events)
        .expect("poll");
    let elapsed = start.elapsed();

    // Waker should have fired well before the 500ms timeout.
    assert!(
        elapsed < Duration::from_millis(400),
        "poll should return early via waker, elapsed={elapsed:?}"
    );

    t.join().expect("thread join");
}
