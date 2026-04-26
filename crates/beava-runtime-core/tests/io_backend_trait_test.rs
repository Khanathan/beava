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
use std::sync::Arc;
use std::time::Duration;

/// Smoke test: `MioBackend` can be constructed and basic operations succeed.
/// Validates waker, add_client, poll, read, set_interest_writable, close.
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

    // Spawn a client that connects, writes data, then drops.
    let t = std::thread::spawn(move || {
        use std::io::Write;
        let mut s = std::net::TcpStream::connect(addr).expect("connect");
        s.write_all(b"hello").expect("write");
        // Drop immediately so the server sees EOF (readable event).
        drop(s);
    });

    // Accept + set non-blocking before handing to mio.
    let (stream, _) = listener.accept().expect("accept");
    stream.set_nonblocking(true).expect("set_nonblocking");
    let mio_stream = mio::net::TcpStream::from_std(stream);

    // add_client must succeed.
    let slot_idx: u64 = 42;
    backend
        .add_client(mio_stream, slot_idx)
        .expect("add_client");

    // poll() until we get a Readable or Closed event (or timeout).
    let mut events: Vec<IoEvent> = Vec::new();
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    loop {
        events.clear();
        backend
            .poll(Some(Duration::from_millis(50)), &mut events)
            .expect("poll");
        let done = events
            .iter()
            .any(|e| matches!(e, IoEvent::Readable(s) | IoEvent::Closed(s) if *s == slot_idx));
        if done || std::time::Instant::now() >= deadline {
            break;
        }
    }

    // We should have gotten at least a readable or closed event.
    // (On some OSes EOF may show as readable; on others as closed.)
    let got_event = events
        .iter()
        .any(|e| matches!(e, IoEvent::Readable(s) | IoEvent::Closed(s) if *s == slot_idx));
    assert!(
        got_event,
        "expected Readable or Closed for slot 42, got: {events:?}"
    );

    // read() should drain whatever data the client sent (or return 0 on EOF).
    let mut buf = BytesMut::new();
    let _n = backend.read(slot_idx, &mut buf).unwrap_or(0);

    // set_interest_writable should not error.
    backend.set_interest_writable(slot_idx, true);
    backend.set_interest_writable(slot_idx, false);

    // close() must succeed.
    backend.close(slot_idx);

    t.join().expect("thread join");
}

/// Test that WakerHandle::wake() from a different thread causes poll() to return early.
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
