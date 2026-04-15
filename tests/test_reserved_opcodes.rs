//! Phase 25-01, Task 1: Reserved opcodes (SCAN / SUBSCRIBE) integration tests.
//!
//! Asserts that `OP_SCAN_RESERVED` (0x10) and `OP_SUBSCRIBE_RESERVED` (0x11)
//! return `STATUS_ERROR` with a "not implemented" message AND leave the TCP
//! connection open so clients can continue issuing subsequent commands
//! (T-25-01-04 DoS mitigation — reserved opcodes must not tear down the
//! session).
//!
//! Uses the same `make_concurrent_state` + `run_tcp_server_with_listener`
//! pattern as `tests/test_op_push_table.rs`.

use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use tally::engine::pipeline::PipelineEngine;
use tally::server::protocol::{self, OP_GET, OP_SCAN_RESERVED, STATUS_ERROR, STATUS_OK};
use tally::server::tcp::{make_concurrent_state, BackfillTracker, SharedState};
use tally::state::store::StateStore;

async fn start_test_server() -> (u16, SharedState) {
    let state: SharedState = make_concurrent_state(
        PipelineEngine::new(),
        StateStore::new(),
        None,
        std::path::PathBuf::from("test_reserved_opcodes.snapshot"),
        Arc::new(BackfillTracker::default()),
        true,
        true,
    );

    let tcp_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let tcp_port = tcp_listener.local_addr().unwrap().port();

    let tcp_state = state.clone();
    tokio::spawn(async move {
        tally::server::tcp::run_tcp_server_with_listener(tcp_listener, tcp_state)
            .await
            .unwrap();
    });

    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    (tcp_port, state)
}

async fn send_frame(stream: &mut TcpStream, opcode: u8, payload: &[u8]) -> (u8, Vec<u8>) {
    let len = (1 + payload.len()) as u32;
    stream.write_u32(len).await.unwrap();
    stream.write_u8(opcode).await.unwrap();
    if !payload.is_empty() {
        stream.write_all(payload).await.unwrap();
    }
    stream.flush().await.unwrap();

    let resp_len = stream.read_u32().await.unwrap() as usize;
    let status = stream.read_u8().await.unwrap();
    let payload_len = resp_len - 1;
    let mut resp_payload = vec![0u8; payload_len];
    if payload_len > 0 {
        stream.read_exact(&mut resp_payload).await.unwrap();
    }
    (status, resp_payload)
}

#[tokio::test]
async fn scan_reserved_returns_error_and_keeps_connection_alive() {
    let (port, _state) = start_test_server().await;
    let mut s = TcpStream::connect(("127.0.0.1", port)).await.unwrap();

    // Send arbitrary bytes — reserved opcodes short-circuit before payload parsing.
    let (status, resp) = send_frame(&mut s, OP_SCAN_RESERVED, b"whatever").await;
    assert_eq!(
        status, STATUS_ERROR,
        "SCAN reserved must return STATUS_ERROR"
    );
    let msg = String::from_utf8_lossy(&resp);
    assert!(
        msg.to_lowercase().contains("not implemented") || msg.contains("SCAN"),
        "error message should mention 'not implemented' / 'SCAN', got: {}",
        msg
    );

    // CRITICAL: connection must still be usable. Issue a plain OP_GET and
    // assert the server still responds (STATUS_OK with empty map for the
    // unknown key).
    let (status, _resp) = send_frame(&mut s, OP_GET, &protocol::write_string("u_probe")).await;
    assert_eq!(
        status, STATUS_OK,
        "connection must survive a reserved-opcode error"
    );
}

// Phase 27-02: OP_SUBSCRIBE (0x11) was promoted from reserved-stub to a
// live-subscribe opcode. The old "returns NotImplemented and keeps the
// connection alive" assertion no longer applies — SUBSCRIBE now takes
// ownership of the connection for the lifetime of the subscription.
// Live-subscribe coverage lives in `tests/test_replica_subscribe.rs`.
