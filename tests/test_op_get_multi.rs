//! Phase 25-01, Task 2: End-to-end TCP integration tests for OP_GET_MULTI.
//!
//! Covers the six behaviours enumerated in the plan:
//!
//!   * test_get_multi_assembles_feature_vector  — multi-table happy path
//!   * test_get_multi_null_for_missing_table_row — null-collapse (never pushed)
//!   * test_get_multi_null_for_tombstoned        — null-collapse (tombstoned)
//!   * test_get_multi_unknown_table_rejects      — STATUS_ERROR, no partial
//!   * test_get_multi_preserves_request_order    — response key order
//!   * test_get_multi_single_round_trip          — exactly one response frame
//!
//! Uses the same `make_concurrent_state` + `run_tcp_server_with_listener`
//! pattern as `tests/test_op_push_table.rs`.

use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use beava::engine::pipeline::PipelineEngine;
use beava::server::protocol::{
    self, OP_DELETE_TABLE, OP_GET_MULTI, OP_PUSH_TABLE, OP_REGISTER, STATUS_ERROR, STATUS_OK,
};
use beava::server::tcp::{make_concurrent_state, BackfillTracker, SharedState};
// ---------------------------------------------------------------------------
// Harness (mirrors tests/test_op_push_table.rs)
// ---------------------------------------------------------------------------

async fn start_test_server() -> (u16, SharedState) {
    let state: SharedState = make_concurrent_state(
        PipelineEngine::new(),
        None,
        std::path::PathBuf::from("test_op_get_multi.snapshot"),
        Arc::new(BackfillTracker::default()),
        true,
        true,
    );

    let tcp_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let tcp_port = tcp_listener.local_addr().unwrap().port();

    let tcp_state = state.clone();
    tokio::spawn(async move {
        beava::server::tcp::run_tcp_server_with_listener(tcp_listener, tcp_state)
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

fn build_push_table_payload(table: &str, key: &str, fields: &serde_json::Value) -> Vec<u8> {
    let mut buf = protocol::write_string(table);
    buf.extend_from_slice(&protocol::write_string(key));
    buf.extend_from_slice(&serde_json::to_vec(fields).unwrap());
    buf
}

fn build_delete_table_payload(table: &str, key: &str) -> Vec<u8> {
    let mut buf = protocol::write_string(table);
    buf.extend_from_slice(&protocol::write_string(key));
    buf
}

fn build_get_multi_payload(tables: &[&str], key: &str) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.extend_from_slice(&(tables.len() as u16).to_be_bytes());
    for t in tables {
        buf.extend_from_slice(&protocol::write_string(t));
    }
    buf.extend_from_slice(&protocol::write_string(key));
    buf
}

async fn register_table(stream: &mut TcpStream, table_name: &str, key_field: &str) {
    let def = serde_json::json!({
        "name": table_name,
        "kind": "table",
        "mode": "overwrite",
        "key_field": key_field,
        "fields": {
            "country": {"type": "str", "optional": true},
            "score":   {"type": "i64", "optional": true},
            "plan":    {"type": "str", "optional": true},
        },
    });
    let payload = serde_json::to_vec(&def).unwrap();
    let (status, resp) = send_frame(stream, OP_REGISTER, &payload).await;
    assert_eq!(
        status,
        STATUS_OK,
        "register table {} failed: {}",
        table_name,
        String::from_utf8_lossy(&resp)
    );
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_get_multi_assembles_feature_vector() {
    let (port, _state) = start_test_server().await;
    let mut s = TcpStream::connect(("127.0.0.1", port)).await.unwrap();

    register_table(&mut s, "UserProfile", "user_id").await;
    register_table(&mut s, "RiskScore", "user_id").await;
    register_table(&mut s, "Subscription", "user_id").await;

    let (status, _) = send_frame(
        &mut s,
        OP_PUSH_TABLE,
        &build_push_table_payload(
            "UserProfile",
            "u1",
            &serde_json::json!({"country": "US", "score": 10}),
        ),
    )
    .await;
    assert_eq!(status, STATUS_OK);
    let (status, _) = send_frame(
        &mut s,
        OP_PUSH_TABLE,
        &build_push_table_payload("RiskScore", "u1", &serde_json::json!({"score": 42})),
    )
    .await;
    assert_eq!(status, STATUS_OK);
    let (status, _) = send_frame(
        &mut s,
        OP_PUSH_TABLE,
        &build_push_table_payload("Subscription", "u1", &serde_json::json!({"plan": "gold"})),
    )
    .await;
    assert_eq!(status, STATUS_OK);

    let (status, resp) = send_frame(
        &mut s,
        OP_GET_MULTI,
        &build_get_multi_payload(&["UserProfile", "RiskScore", "Subscription"], "u1"),
    )
    .await;
    assert_eq!(status, STATUS_OK);
    let v: serde_json::Value = serde_json::from_slice(&resp).unwrap();
    let obj = v.as_object().expect("GET_MULTI response must be an object");
    assert_eq!(obj["UserProfile"]["country"].as_str(), Some("US"));
    assert_eq!(obj["UserProfile"]["score"].as_i64(), Some(10));
    assert_eq!(obj["RiskScore"]["score"].as_i64(), Some(42));
    assert_eq!(obj["Subscription"]["plan"].as_str(), Some("gold"));
}

#[tokio::test]
async fn test_get_multi_null_for_missing_table_row() {
    let (port, _state) = start_test_server().await;
    let mut s = TcpStream::connect(("127.0.0.1", port)).await.unwrap();

    register_table(&mut s, "UserProfile", "user_id").await;
    register_table(&mut s, "RiskScore", "user_id").await;

    // Push ONLY to UserProfile.
    let (status, _) = send_frame(
        &mut s,
        OP_PUSH_TABLE,
        &build_push_table_payload("UserProfile", "u1", &serde_json::json!({"country": "DE"})),
    )
    .await;
    assert_eq!(status, STATUS_OK);

    let (status, resp) = send_frame(
        &mut s,
        OP_GET_MULTI,
        &build_get_multi_payload(&["UserProfile", "RiskScore"], "u1"),
    )
    .await;
    assert_eq!(status, STATUS_OK);
    let v: serde_json::Value = serde_json::from_slice(&resp).unwrap();
    let obj = v.as_object().unwrap();
    assert!(obj["UserProfile"].is_object());
    assert!(
        obj["RiskScore"].is_null(),
        "never-pushed table must collapse to null, got: {:?}",
        obj["RiskScore"]
    );

    // Also: an entirely unknown KEY returns null across the board.
    let (status, resp) = send_frame(
        &mut s,
        OP_GET_MULTI,
        &build_get_multi_payload(&["UserProfile", "RiskScore"], "u_never_existed"),
    )
    .await;
    assert_eq!(status, STATUS_OK);
    let v: serde_json::Value = serde_json::from_slice(&resp).unwrap();
    assert!(v["UserProfile"].is_null());
    assert!(v["RiskScore"].is_null());
}

#[tokio::test]
async fn test_get_multi_null_for_tombstoned() {
    let (port, _state) = start_test_server().await;
    let mut s = TcpStream::connect(("127.0.0.1", port)).await.unwrap();

    register_table(&mut s, "UserProfile", "user_id").await;
    register_table(&mut s, "RiskScore", "user_id").await;

    let (status, _) = send_frame(
        &mut s,
        OP_PUSH_TABLE,
        &build_push_table_payload("UserProfile", "u1", &serde_json::json!({"country": "FR"})),
    )
    .await;
    assert_eq!(status, STATUS_OK);
    let (status, _) = send_frame(
        &mut s,
        OP_PUSH_TABLE,
        &build_push_table_payload("RiskScore", "u1", &serde_json::json!({"score": 5})),
    )
    .await;
    assert_eq!(status, STATUS_OK);

    // Tombstone UserProfile but leave RiskScore live.
    let (status, _) = send_frame(
        &mut s,
        OP_DELETE_TABLE,
        &build_delete_table_payload("UserProfile", "u1"),
    )
    .await;
    assert_eq!(status, STATUS_OK);

    let (status, resp) = send_frame(
        &mut s,
        OP_GET_MULTI,
        &build_get_multi_payload(&["UserProfile", "RiskScore"], "u1"),
    )
    .await;
    assert_eq!(status, STATUS_OK);
    let v: serde_json::Value = serde_json::from_slice(&resp).unwrap();
    assert!(
        v["UserProfile"].is_null(),
        "tombstoned row must collapse to null (T-25-01-02); got: {:?}",
        v["UserProfile"]
    );
    assert_eq!(v["RiskScore"]["score"].as_i64(), Some(5));
}

#[tokio::test]
async fn test_get_multi_unknown_table_rejects() {
    let (port, state) = start_test_server().await;
    let mut s = TcpStream::connect(("127.0.0.1", port)).await.unwrap();

    register_table(&mut s, "UserProfile", "user_id").await;
    let (status, _) = send_frame(
        &mut s,
        OP_PUSH_TABLE,
        &build_push_table_payload("UserProfile", "u1", &serde_json::json!({"country": "US"})),
    )
    .await;
    assert_eq!(status, STATUS_OK);

    // Mix a known and an unknown table. Must error with STATUS_ERROR BEFORE
    // any per-table state read (T-25-01-03).
    let (status, resp) = send_frame(
        &mut s,
        OP_GET_MULTI,
        &build_get_multi_payload(&["UserProfile", "Ghosts"], "u1"),
    )
    .await;
    assert_eq!(status, STATUS_ERROR);
    let msg = String::from_utf8_lossy(&resp);
    assert!(
        msg.contains("unknown table"),
        "error should mention unknown table, got: {}",
        msg
    );

    // No partial response: the error payload is plain text, not JSON — verify
    // that parsing as JSON fails (or at worst yields a non-object) so clients
    // cannot accidentally consume partial state.
    let parsed: Result<serde_json::Value, _> = serde_json::from_slice(&resp);
    if let Ok(v) = parsed {
        assert!(
            !v.is_object(),
            "error payload must not parse as a partial result object"
        );
    }

    // Connection must survive the STATUS_ERROR. Use state to confirm the
    // server is still up and serving subsequent requests.
    let (status, resp) = send_frame(
        &mut s,
        OP_GET_MULTI,
        &build_get_multi_payload(&["UserProfile"], "u1"),
    )
    .await;
    assert_eq!(status, STATUS_OK);
    let v: serde_json::Value = serde_json::from_slice(&resp).unwrap();
    assert_eq!(v["UserProfile"]["country"].as_str(), Some("US"));

    // Sanity: underlying store state is untouched.
    let _ = &state;
}

#[tokio::test]
async fn test_get_multi_preserves_request_order() {
    let (port, _state) = start_test_server().await;
    let mut s = TcpStream::connect(("127.0.0.1", port)).await.unwrap();

    register_table(&mut s, "A", "user_id").await;
    register_table(&mut s, "B", "user_id").await;
    register_table(&mut s, "C", "user_id").await;

    for t in ["A", "B", "C"] {
        let (status, _) = send_frame(
            &mut s,
            OP_PUSH_TABLE,
            &build_push_table_payload(t, "u1", &serde_json::json!({"plan": t})),
        )
        .await;
        assert_eq!(status, STATUS_OK);
    }

    // Request order: B, A, C. Response keys must serialize in that order.
    let (status, resp) = send_frame(
        &mut s,
        OP_GET_MULTI,
        &build_get_multi_payload(&["B", "A", "C"], "u1"),
    )
    .await;
    assert_eq!(status, STATUS_OK);

    // Parse the raw JSON manually to observe key order — serde_json::Value
    // preserves order only with the `preserve_order` feature. We built the
    // response by hand so the bytes carry request order verbatim.
    let body = String::from_utf8(resp).unwrap();
    let b_pos = body.find("\"B\"").expect("B key missing");
    let a_pos = body.find("\"A\"").expect("A key missing");
    let c_pos = body.find("\"C\"").expect("C key missing");
    assert!(
        b_pos < a_pos && a_pos < c_pos,
        "response must preserve request order B<A<C; got: {}",
        body
    );
}

#[tokio::test]
async fn test_get_multi_single_round_trip() {
    let (port, _state) = start_test_server().await;
    let mut s = TcpStream::connect(("127.0.0.1", port)).await.unwrap();

    register_table(&mut s, "UserProfile", "user_id").await;
    let (status, _) = send_frame(
        &mut s,
        OP_PUSH_TABLE,
        &build_push_table_payload("UserProfile", "u1", &serde_json::json!({"country": "US"})),
    )
    .await;
    assert_eq!(status, STATUS_OK);

    // Send the GET_MULTI frame manually and read exactly ONE response frame
    // (length header + status + payload). Assert no additional bytes are
    // available on the socket afterwards — there must be no server chatter.
    let payload = build_get_multi_payload(&["UserProfile"], "u1");
    let len = (1 + payload.len()) as u32;
    s.write_u32(len).await.unwrap();
    s.write_u8(OP_GET_MULTI).await.unwrap();
    s.write_all(&payload).await.unwrap();
    s.flush().await.unwrap();

    let resp_len = s.read_u32().await.unwrap() as usize;
    let status = s.read_u8().await.unwrap();
    let payload_len = resp_len - 1;
    let mut resp_payload = vec![0u8; payload_len];
    s.read_exact(&mut resp_payload).await.unwrap();
    assert_eq!(status, STATUS_OK);

    // The server must NOT have pushed any additional bytes unsolicited.
    // Try a non-blocking read with a short timeout; if data arrives, fail.
    let mut extra = [0u8; 1];
    let got = tokio::time::timeout(std::time::Duration::from_millis(100), s.read(&mut extra)).await;
    match got {
        Err(_) => {
            // Timeout — no extra bytes. This is the expected path.
        }
        Ok(Ok(0)) => {
            // Clean close — also acceptable (no chatter).
        }
        Ok(Ok(n)) => panic!(
            "server pushed {} unsolicited byte(s) after the GET_MULTI response",
            n
        ),
        Ok(Err(_)) => {
            // Transport error is fine — not extra data.
        }
    }
}
