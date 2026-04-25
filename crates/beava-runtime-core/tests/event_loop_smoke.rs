//! Smoke tests for beava-runtime-core (Phase 18 Plan 01).
//!
//! These tests follow strict TDD red-green order per CLAUDE.md §Conventions.
//! Each block is annotated with which task's RED it was first written for.

// ─── Task 1.1 RED: EventLoop::new() constructs ────────────────────────────────

#[test]
fn event_loop_new_constructs_and_returns() {
    let el = beava_runtime_core::EventLoop::new().expect("EventLoop::new()");
    // Just verify it constructed — minimal assertion.
    drop(el);
}

// ─── Task 1.2 RED: TCP framed listener parses a ping frame ────────────────────
//
// The test:
//   1. Creates an EventLoop + TcpListener on a random OS port.
//   2. Spawns a background thread that writes a OP_PING frame to the socket.
//   3. Calls parse_tcp_frame() — asserts it returns WireRequest::Ping.

use beava_runtime_core::wire_request::WireRequest;
use beava_core::wire::{encode_frame, Frame, OP_PING, CT_JSON};
use bytes::BytesMut;
use std::net::SocketAddr;
use std::io::Write;

#[test]
fn tcp_listener_reads_ping_frame_produces_wire_request_ping() {
    // Bind on OS-assigned port.
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let mut tcp_listener = beava_runtime_core::TcpListener::bind(addr).expect("bind");
    let bound_addr = tcp_listener.local_addr();

    // Background thread: connect + write a ping frame.
    let writer = std::thread::spawn(move || {
        let mut conn = std::net::TcpStream::connect(bound_addr).expect("connect");
        // Build a OP_PING frame with empty payload.
        let frame = Frame::new(OP_PING, CT_JSON, bytes::Bytes::new());
        let mut buf = BytesMut::new();
        encode_frame(&frame, &mut buf);
        conn.write_all(&buf).expect("write frame");
    });

    // Accept the connection (blocking — writer guarantees it connects).
    let (mut stream, _peer) = loop {
        match tcp_listener.accept() {
            Ok(c) => break c,
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                std::thread::sleep(std::time::Duration::from_millis(5));
            }
            Err(e) => panic!("accept error: {e}"),
        }
    };
    writer.join().expect("writer thread");

    // Read bytes from the stream into a BytesMut.
    use std::io::Read;
    let mut raw_buf = vec![0u8; 256];
    let n = stream.read(&mut raw_buf).expect("read");
    let mut read_buf = BytesMut::from(&raw_buf[..n]);

    // Parse the frame then convert to WireRequest.
    let req = beava_runtime_core::tcp_listener::parse_wire_request(&mut read_buf, 4 * 1024 * 1024)
        .expect("parse_wire_request")
        .expect("complete frame");

    assert_eq!(req, WireRequest::Ping, "expected WireRequest::Ping");
}
