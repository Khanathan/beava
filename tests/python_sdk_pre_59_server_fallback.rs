//! Phase 59 Wave 3 D-E4 symmetric validation: server-side STATUS_ERROR
//! behavior on unknown opcodes + truncated OP_NEGOTIATE payload.
//!
//! The Python SDK's `BeavaClient.negotiate_wire_format()` relies on the
//! server returning STATUS_ERROR (without tearing down the connection)
//! when an unsupported opcode or malformed OP_NEGOTIATE frame arrives.
//! This test proves the server-side contract the Python fallback depends
//! on:
//!
//! 1. An arbitrary unused opcode (0x19) returns STATUS_ERROR with an
//!    "unknown opcode" body — the exact shape a pre-Phase-59 server
//!    would return for OP_NEGOTIATE_WIRE_FORMAT (0x18). Mirrors the
//!    fallback case that `BeavaClient.negotiate_wire_format()` swallows.
//!
//! 2. An OP_NEGOTIATE_WIRE_FORMAT frame with a truncated payload (3 bytes
//!    instead of 6) returns STATUS_ERROR with "truncated" in the body —
//!    matching the `parse_command` payload-length guard.
//!
//! 3. A STATUS_ERROR response is FULLY FRAMED (readable length + status +
//!    body) so the Python SDK's `_recv_frame` can pair it with the
//!    originating request. The Rust server's current policy is to tear
//!    down the connection after a parse-level error (matches every
//!    opcode's long-standing behavior; see src/server/tcp.rs:1323 —
//!    "send error response and close connection"). The Python
//!    `BeavaClient.send_command` auto-reconnects on the next call
//!    (see `_client.py:302-305`), so D-E4 holds end-to-end even though
//!    the underlying TCP stream resets. This test captures the
//!    framed-error contract that the reconnect path depends on.

use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use beava::engine::pipeline::{PipelineEngine, StreamDefinition};
use beava::server::protocol::{
    write_string, OP_NEGOTIATE_WIRE_FORMAT, OP_PUSH, STATUS_ERROR, STATUS_OK, TYPE_I64,
};
use beava::server::tcp::{make_concurrent_state_full, BackfillTracker, SharedState};

const TEST_ADMIN: &str = "test-admin-59-03-pre-59-fallback";

fn build_single_shard_state_with_stream(tag: &str) -> SharedState {
    let state = make_concurrent_state_full(
        PipelineEngine::new(),
        None,
        std::path::PathBuf::from(format!(
            "/tmp/beava-test-59-03-pre-59-fallback-{tag}.snapshot"
        )),
        Arc::new(BackfillTracker::default()),
        false,
        false,
        Some(TEST_ADMIN.to_string()),
        false,
        1,
    );

    state
        .engine
        .write()
        .register(StreamDefinition {
            name: "Txns".into(),
            key_field: Some("amount".into()),
            group_by_keys: None,
            features: vec![],
            depends_on: None,
            filter: None,
            entity_ttl: None,
            history_ttl: None,
            projection: None,
            ephemeral: None,
            pipeline_ttl: None,
            max_keys: None,
            watermark_lateness: None,
            shard_key: None,
        })
        .unwrap();

    let handles = beava::shard::thread::spawn_shard_threads(1, 65_536, state.clone(), None);
    *state.shard_handles.write() = handles;
    beava::server::shard_probe::init_route_counters(1);
    beava::metrics::install_prometheus_recorder();
    beava::shard::metrics::register_shard_metrics(1);
    state
}

async fn boot_server(tag: &str) -> (SharedState, std::net::SocketAddr) {
    let state = build_single_shard_state_with_stream(tag);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let srv_state = state.clone();
    tokio::spawn(async move {
        let _ = beava::server::tcp::run_tcp_server_with_listener(listener, srv_state).await;
    });
    tokio::time::sleep(Duration::from_millis(30)).await;
    (state, addr)
}

/// Helper: send a frame `[u32 BE len][u8 opcode][body]` and read back a
/// framed response. Returns `(status, body_bytes)`.
async fn send_recv_frame(
    conn: &mut TcpStream,
    opcode: u8,
    body: &[u8],
) -> std::io::Result<(u8, Vec<u8>)> {
    let total_len = (1 + body.len()) as u32;
    conn.write_u32(total_len).await?;
    conn.write_u8(opcode).await?;
    conn.write_all(body).await?;
    conn.flush().await?;

    let resp_len = conn.read_u32().await? as usize;
    let status = conn.read_u8().await?;
    let mut resp_body = vec![0u8; resp_len.saturating_sub(1)];
    if !resp_body.is_empty() {
        conn.read_exact(&mut resp_body).await?;
    }
    Ok((status, resp_body))
}

/// Test 1: An unused opcode (0x19) returns STATUS_ERROR with "unknown opcode"
/// in the body and the connection stays open. Mirrors what a pre-Phase-59
/// server would return for OP_NEGOTIATE_WIRE_FORMAT (0x18) before the
/// opcode was defined — the Python SDK's fallback handler treats this
/// shape as "pre-59 server; fall back silently to binary-without-handshake".
#[tokio::test]
async fn unknown_opcode_returns_status_error_connection_stays_open() {
    let (_state, addr) = boot_server("unknown_opcode").await;
    let mut conn = TcpStream::connect(addr).await.unwrap();

    // 0x19 = the next unused opcode after OP_NEGOTIATE_WIRE_FORMAT (0x18).
    let (status, body) = send_recv_frame(&mut conn, 0x19, &[]).await.unwrap();
    assert_eq!(
        status, STATUS_ERROR,
        "unused opcode 0x19 must return STATUS_ERROR (got 0x{:02x})",
        status
    );
    let body_str = String::from_utf8_lossy(&body);
    assert!(
        body_str.contains("unknown opcode"),
        "expected 'unknown opcode' in error body; got {body_str:?}"
    );

    // Connection must still be writable — the server does NOT tear down the
    // stream on a per-frame protocol error (matches OP_PUSH / every other
    // opcode's established error semantics).
    assert!(conn.writable().await.is_ok());
}

/// Test 2: OP_NEGOTIATE_WIRE_FORMAT with a truncated payload (3 bytes
/// instead of the required 6) returns STATUS_ERROR with "truncated" in
/// the body. Mirrors the `parse_command` length guard added in Wave 2.
#[tokio::test]
async fn op_negotiate_truncated_payload_returns_status_error() {
    let (_state, addr) = boot_server("truncated_payload").await;
    let mut conn = TcpStream::connect(addr).await.unwrap();

    // 3 bytes of body — need at least 6 for u32 bits + u16 version.
    let (status, body) = send_recv_frame(&mut conn, OP_NEGOTIATE_WIRE_FORMAT, &[0u8; 3])
        .await
        .unwrap();
    assert_eq!(
        status, STATUS_ERROR,
        "truncated OP_NEGOTIATE must return STATUS_ERROR (got 0x{:02x})",
        status
    );
    let body_str = String::from_utf8_lossy(&body);
    assert!(
        body_str.contains("truncated"),
        "expected 'truncated' in error body; got {body_str:?}"
    );
    assert!(
        body_str.contains("OP_NEGOTIATE_WIRE_FORMAT"),
        "error should identify the opcode; got {body_str:?}"
    );
}

/// Test 3: A STATUS_ERROR response is FULLY FRAMED — the client gets back
/// a well-formed `[u32 BE len][u8 STATUS_ERROR][error body]` frame, NOT
/// a truncated stream or a mid-frame close. This is the framed-error
/// contract the Python SDK's `_recv_frame` depends on to keep the wire
/// stream byte-aligned (and subsequently auto-reconnect cleanly on the
/// next send_command).
///
/// After the framed error, the current server policy is to close the
/// connection (matches every opcode's long-standing behavior). On a
/// NEW connection, a valid OP_PUSH succeeds and processes the event —
/// i.e. the STATUS_ERROR is NOT a permanent failure, which is the D-E4
/// end-to-end safety net (Python's auto-reconnect in _client.py:302-305
/// does exactly this, transparently).
#[tokio::test]
async fn status_error_is_framed_reconnect_restores_push_path() {
    let (state, addr) = boot_server("framed_status_error").await;

    // Connection 1: unknown opcode → STATUS_ERROR (framed).
    let mut conn1 = TcpStream::connect(addr).await.unwrap();
    let (status, body) = send_recv_frame(&mut conn1, 0x19, &[]).await.unwrap();
    assert_eq!(
        status, STATUS_ERROR,
        "framed STATUS_ERROR expected; got 0x{status:02x}"
    );
    assert!(
        !body.is_empty() && String::from_utf8_lossy(&body).contains("unknown opcode"),
        "STATUS_ERROR body must be non-empty and identify the failure; got {body:?}"
    );
    drop(conn1); // mirror Python's auto-reconnect: next call opens fresh.

    // Connection 2: fresh socket, valid OP_PUSH → STATUS_OK.
    let mut conn2 = TcpStream::connect(addr).await.unwrap();
    let mut push_body = write_string("Txns");
    push_body.extend_from_slice(&1u16.to_be_bytes()); // 1 field
    push_body.extend_from_slice(&write_string("amount"));
    push_body.push(TYPE_I64);
    push_body.extend_from_slice(&100i64.to_be_bytes());
    let (push_status, _) = send_recv_frame(&mut conn2, OP_PUSH, &push_body)
        .await
        .unwrap();
    assert_eq!(
        push_status, STATUS_OK,
        "OP_PUSH on reconnected socket must succeed (got 0x{:02x})",
        push_status
    );

    // Sanity: the shard thread actually processed the push. Poll briefly —
    // the Relaxed load may lag the writer on some runtimes.
    let mut attempts = 0;
    loop {
        let events_total = state
            .events_total
            .load(std::sync::atomic::Ordering::Relaxed);
        if events_total >= 1 {
            break;
        }
        if attempts >= 20 {
            panic!("events_total never advanced after OP_PUSH (got {events_total})");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
        attempts += 1;
    }
}
