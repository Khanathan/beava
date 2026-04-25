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

// ─── Task 1.3 RED: HTTP listener parses POST /push/foo + JSON body ────────────
//
// Tests (each a self-contained unit test using the http_listener parser):
//   a. Basic POST /push/foo with Content-Length body → WireRequest::HttpPush
//   b. Chunked transfer encoding body → correct concatenation
//   c. Keep-alive pipelining: 3 requests in one buffer → 3 WireRequests
//   d. Malformed header (missing \r\n\r\n) → None (incomplete)
//   e. Connection: close header → keep_alive=false in parsed request

use beava_runtime_core::http_listener::parse_http_request;

#[test]
fn http_post_push_with_content_length_produces_http_push() {
    let body = br#"{"user_id":"u1","amount":42}"#;
    let raw = format!(
        "POST /push/Transaction HTTP/1.1\r\nHost: localhost\r\nContent-Length: {}\r\nConnection: keep-alive\r\n\r\n",
        body.len()
    );
    let mut buf = BytesMut::new();
    buf.extend_from_slice(raw.as_bytes());
    buf.extend_from_slice(body);

    let result = parse_http_request(&mut buf).expect("parse ok");
    let (req, keep_alive) = result.expect("complete request");
    assert!(keep_alive, "keep-alive expected");
    match req {
        WireRequest::HttpPush { event_name, body: b } => {
            assert_eq!(event_name, "Transaction");
            let v: serde_json::Value = serde_json::from_slice(&b).unwrap();
            assert_eq!(v["user_id"], "u1");
        }
        other => panic!("expected HttpPush, got {other:?}"),
    }
}

#[test]
fn http_post_push_chunked_body_concatenated_correctly() {
    // Send body in 3 chunks using chunked transfer encoding.
    // Body = `{"x":1}` (7 bytes)
    let chunk1 = b"3\r\n{\"x\r\n";     // first 3 bytes of body
    let chunk2 = b"3\r\":1\r\n";        // next 3 bytes
    let chunk3 = b"1\r\n}\r\n";         // last 1 byte
    let terminator = b"0\r\n\r\n";       // final chunk

    let header = "POST /push/Evt HTTP/1.1\r\nHost: localhost\r\nTransfer-Encoding: chunked\r\n\r\n";
    let mut buf = BytesMut::new();
    buf.extend_from_slice(header.as_bytes());
    buf.extend_from_slice(chunk1);
    buf.extend_from_slice(chunk2);
    buf.extend_from_slice(chunk3);
    buf.extend_from_slice(terminator);

    let result = parse_http_request(&mut buf).expect("parse ok");
    let (req, _keep_alive) = result.expect("complete request");
    match req {
        WireRequest::HttpPush { event_name, body: b } => {
            assert_eq!(event_name, "Evt");
            assert_eq!(&b[..], b"{\"x\":1}");
        }
        other => panic!("expected HttpPush, got {other:?}"),
    }
}

#[test]
fn http_three_pipelined_requests_all_parsed() {
    // Build 3 complete POST /push requests back-to-back in one buffer.
    let body = b"{}";
    let single = format!(
        "POST /push/E{idx} HTTP/1.1\r\nHost: x\r\nContent-Length: {len}\r\n\r\n",
        idx = "{idx}",
        len = body.len()
    );
    let mut buf = BytesMut::new();
    for i in 0..3usize {
        let req = format!(
            "POST /push/E{i} HTTP/1.1\r\nHost: x\r\nContent-Length: {}\r\n\r\n",
            body.len()
        );
        buf.extend_from_slice(req.as_bytes());
        buf.extend_from_slice(body);
    }
    let _ = single; // suppress unused

    for i in 0..3usize {
        let result = parse_http_request(&mut buf).expect("parse ok");
        let (req, _) = result.expect("complete");
        match req {
            WireRequest::HttpPush { event_name, .. } => {
                assert_eq!(event_name, format!("E{i}"));
            }
            other => panic!("expected HttpPush #{i}, got {other:?}"),
        }
    }
    assert_eq!(buf.len(), 0, "buffer fully consumed");
}

#[test]
fn http_incomplete_header_returns_none() {
    // No \r\n\r\n boundary yet — parser must return None (need more bytes).
    let mut buf = BytesMut::from("POST /push/Foo HTTP/1.1\r\nHost: x".as_bytes());
    let result = parse_http_request(&mut buf).expect("no error");
    assert!(result.is_none(), "expected None (incomplete header)");
}

#[test]
fn http_connection_close_header_returns_keep_alive_false() {
    let body = b"{}";
    let raw = format!(
        "POST /push/E HTTP/1.1\r\nHost: x\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    let mut buf = BytesMut::new();
    buf.extend_from_slice(raw.as_bytes());
    buf.extend_from_slice(body);

    let result = parse_http_request(&mut buf).expect("parse ok");
    let (_req, keep_alive) = result.expect("complete");
    assert!(!keep_alive, "Connection: close must set keep_alive=false");
}
