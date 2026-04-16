//! Phase 24-02, Task 1: TCP integration tests for OP_PUSH_TABLE /
//! OP_DELETE_TABLE and the merged GET view.
//!
//! Spins up the real beava server on random ports and drives end-to-end
//! frames across the wire. Covers the six behaviours enumerated in the plan:
//!
//!   * push_table_creates_live_row
//!   * delete_table_flips_to_tombstone
//!   * push_table_unknown_table_returns_error
//!   * get_returns_merged_view
//!   * get_filters_tombstoned_rows
//!   * push_table_overwrites_prior_live_row
//!
//! Uses the same `make_concurrent_state` + `run_tcp_server_with_listener`
//! pattern as `tests/test_server.rs` so the wiring matches production.

use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use beava::engine::pipeline::PipelineEngine;
use beava::server::protocol::{
    self, OP_DELETE_TABLE, OP_GET, OP_PUSH, OP_PUSH_TABLE, OP_REGISTER, OP_SET, STATUS_ERROR,
    STATUS_OK, TYPE_F64, TYPE_I64, TYPE_STR,
};
use beava::server::tcp::{make_concurrent_state, BackfillTracker, SharedState};
use beava::state::store::{StateStore, TableRowState};

// ---------------------------------------------------------------------------
// Server + frame helpers (copied from test_server.rs)
// ---------------------------------------------------------------------------

async fn start_test_server() -> (u16, SharedState) {
    let state: SharedState = make_concurrent_state(
        PipelineEngine::new(),
        StateStore::new(),
        None,
        std::path::PathBuf::from("test_op_push_table.snapshot"),
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

fn build_get_payload(key: &str) -> Vec<u8> {
    protocol::write_string(key)
}

fn build_set_payload(key: &str, features: &serde_json::Value) -> Vec<u8> {
    let mut buf = protocol::write_string(key);
    buf.extend_from_slice(&serde_json::to_vec(features).unwrap());
    buf
}

/// Build a binary PUSH payload (Phase 11 wire format) for a string-field event.
fn build_push_payload(stream_name: &str, event: &serde_json::Value) -> Vec<u8> {
    let obj = event.as_object().unwrap();
    let mut buf = protocol::write_string(stream_name);
    buf.extend_from_slice(&(obj.len() as u16).to_be_bytes());
    for (k, v) in obj {
        buf.extend_from_slice(&protocol::write_string(k));
        match v {
            serde_json::Value::String(s) => {
                buf.push(TYPE_STR);
                buf.extend_from_slice(&protocol::write_string(s));
            }
            serde_json::Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    buf.push(TYPE_I64);
                    buf.extend_from_slice(&i.to_be_bytes());
                } else if let Some(f) = n.as_f64() {
                    buf.push(TYPE_F64);
                    buf.extend_from_slice(&f.to_be_bytes());
                } else {
                    panic!("unsupported number");
                }
            }
            _ => panic!("unsupported value in test fixture"),
        }
    }
    buf
}

/// Register a v0 Table source named `table_name` with `key_field=key_field`.
/// The register JSON matches what the Python SDK's `@tl.table` emits — the
/// minimal shape accepted by `V0RegisterPayload::parse`.
async fn register_table(stream: &mut TcpStream, table_name: &str, key_field: &str) {
    let def = serde_json::json!({
        "name": table_name,
        "kind": "table",
        "mode": "overwrite",
        "key_field": key_field,
        "fields": {
            "country": {"type": "str", "optional": true},
            "score":   {"type": "i64", "optional": true},
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

/// Register a v0 keyed Stream source with one count(window=1h) feature.
async fn register_clicks_stream(stream: &mut TcpStream) {
    let def = serde_json::json!({
        "name": "Clicks",
        "kind": "stream",
        "key_field": "user_id",
        "fields": {
            "user_id": {"type": "str", "optional": false},
            "page":    {"type": "str", "optional": false},
        },
    });
    let payload = serde_json::to_vec(&def).unwrap();
    let (status, resp) = send_frame(stream, OP_REGISTER, &payload).await;
    assert_eq!(
        status,
        STATUS_OK,
        "register Clicks stream failed: {}",
        String::from_utf8_lossy(&resp)
    );

    // Wrap in an aggregation so a count feature exists under a known name.
    // v0 aggregations are registered as kind=table with an `aggregation` block
    // (Phase 21-03 contract; see test_register_json_v0.rs).
    let agg = serde_json::json!({
        "name": "ClicksAgg",
        "kind": "table",
        "key_field": "user_id",
        "mode": "overwrite",
        "fields": {},
        "aggregation": {
            "source": "Clicks",
            "keys": ["user_id"],
            "features": [
                {"name": "clicks_1h", "type": "count", "supports_retraction": true, "window": "1h"}
            ]
        },
        "depends_on": ["Clicks"]
    });
    let payload = serde_json::to_vec(&agg).unwrap();
    let (status, resp) = send_frame(stream, OP_REGISTER, &payload).await;
    assert_eq!(
        status,
        STATUS_OK,
        "register ClicksAgg failed: {}",
        String::from_utf8_lossy(&resp)
    );
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn push_table_creates_live_row() {
    let (port, state) = start_test_server().await;
    let mut s = TcpStream::connect(("127.0.0.1", port)).await.unwrap();

    register_table(&mut s, "UserProfile", "user_id").await;

    let fields = serde_json::json!({"country": "US", "score": 42});
    let payload = build_push_table_payload("UserProfile", "u123", &fields);
    let (status, resp) = send_frame(&mut s, OP_PUSH_TABLE, &payload).await;
    assert_eq!(status, STATUS_OK, "push_table failed: {:?}", resp);

    let row = state
        .store
        .get_table_row("u123", "UserProfile")
        .expect("row must exist after push");
    assert_eq!(row.state, TableRowState::Live);
    assert_eq!(row.fields.len(), 2);
}

#[tokio::test]
async fn delete_table_flips_to_tombstone() {
    let (port, state) = start_test_server().await;
    let mut s = TcpStream::connect(("127.0.0.1", port)).await.unwrap();

    register_table(&mut s, "UserProfile", "user_id").await;

    let fields = serde_json::json!({"country": "DE"});
    let p = build_push_table_payload("UserProfile", "u1", &fields);
    let (status, _) = send_frame(&mut s, OP_PUSH_TABLE, &p).await;
    assert_eq!(status, STATUS_OK);

    let p = build_delete_table_payload("UserProfile", "u1");
    let (status, _) = send_frame(&mut s, OP_DELETE_TABLE, &p).await;
    assert_eq!(status, STATUS_OK);

    let row = state
        .store
        .get_table_row("u1", "UserProfile")
        .expect("row retained in tombstone grace window");
    assert!(matches!(row.state, TableRowState::Tombstoned { .. }));
}

#[tokio::test]
async fn push_table_unknown_table_returns_error() {
    let (port, state) = start_test_server().await;
    let mut s = TcpStream::connect(("127.0.0.1", port)).await.unwrap();

    // No register call — "Ghosts" is unknown.
    let fields = serde_json::json!({"x": 1});
    let p = build_push_table_payload("Ghosts", "u1", &fields);
    let (status, resp) = send_frame(&mut s, OP_PUSH_TABLE, &p).await;
    assert_eq!(
        status, STATUS_ERROR,
        "unknown table must be rejected with STATUS_ERROR"
    );
    let msg = String::from_utf8_lossy(&resp);
    assert!(
        msg.contains("unknown table"),
        "error message should mention unknown table, got: {}",
        msg
    );
    assert!(
        state.store.get_table_row("u1", "Ghosts").is_none(),
        "state must be untouched after rejected push"
    );

    // Symmetric: OP_DELETE_TABLE against unknown table also rejected.
    let p = build_delete_table_payload("Ghosts", "u1");
    let (status, _) = send_frame(&mut s, OP_DELETE_TABLE, &p).await;
    assert_eq!(status, STATUS_ERROR);
}

#[tokio::test]
async fn get_returns_merged_view() {
    let (port, _state) = start_test_server().await;
    let mut s = TcpStream::connect(("127.0.0.1", port)).await.unwrap();

    register_clicks_stream(&mut s).await;
    register_table(&mut s, "UserProfile", "user_id").await;

    // (a) Drive the stream so the entity picks up a live operator feature.
    let ev = serde_json::json!({"user_id": "u123", "page": "/home"});
    let p = build_push_payload("Clicks", &ev);
    let (status, _) = send_frame(&mut s, OP_PUSH, &p).await;
    assert_eq!(status, STATUS_OK);

    // (b) Push a Table row with two fields.
    let fields = serde_json::json!({"country": "US", "score": 42});
    let p = build_push_table_payload("UserProfile", "u123", &fields);
    let (status, _) = send_frame(&mut s, OP_PUSH_TABLE, &p).await;
    assert_eq!(status, STATUS_OK);

    // (c) Write a static feature via SET.
    let p = build_set_payload("u123", &serde_json::json!({"lifetime_value": 9000}));
    let (status, _) = send_frame(&mut s, OP_SET, &p).await;
    assert_eq!(status, STATUS_OK);

    // GET should return a union of all three.
    let (status, resp) = send_frame(&mut s, OP_GET, &build_get_payload("u123")).await;
    assert_eq!(status, STATUS_OK);
    let v: serde_json::Value = serde_json::from_slice(&resp).unwrap();
    let obj = v.as_object().expect("GET response must be object");

    assert_eq!(
        obj.get("UserProfile.country").and_then(|v| v.as_str()),
        Some("US"),
        "merged view must expose Live Table.field entries. got: {:?}",
        obj
    );
    assert_eq!(
        obj.get("UserProfile.score").and_then(|v| v.as_i64()),
        Some(42)
    );
    assert_eq!(
        obj.get("lifetime_value").and_then(|v| v.as_i64()),
        Some(9000),
        "static_features must still be present in merged view"
    );
    // Operator feature from the stream. clicks_1h must be >= 1 after one push.
    assert!(
        obj.get("clicks_1h")
            .and_then(|v| v.as_i64())
            .map(|n| n >= 1)
            .unwrap_or(false),
        "merged view must include stream operator features. got: {:?}",
        obj
    );
}

#[tokio::test]
async fn get_filters_tombstoned_rows() {
    let (port, _state) = start_test_server().await;
    let mut s = TcpStream::connect(("127.0.0.1", port)).await.unwrap();

    register_table(&mut s, "UserProfile", "user_id").await;

    let fields = serde_json::json!({"country": "FR"});
    let p = build_push_table_payload("UserProfile", "u7", &fields);
    let (status, _) = send_frame(&mut s, OP_PUSH_TABLE, &p).await;
    assert_eq!(status, STATUS_OK);

    let (status, resp) = send_frame(&mut s, OP_GET, &build_get_payload("u7")).await;
    assert_eq!(status, STATUS_OK);
    let v: serde_json::Value = serde_json::from_slice(&resp).unwrap();
    assert_eq!(
        v["UserProfile.country"].as_str(),
        Some("FR"),
        "before delete, field is present"
    );

    // Tombstone the row.
    let p = build_delete_table_payload("UserProfile", "u7");
    let (status, _) = send_frame(&mut s, OP_DELETE_TABLE, &p).await;
    assert_eq!(status, STATUS_OK);

    // GET must NOT expose the tombstoned row (T-24-02-03 info disclosure).
    let (status, resp) = send_frame(&mut s, OP_GET, &build_get_payload("u7")).await;
    assert_eq!(status, STATUS_OK);
    let v: serde_json::Value = serde_json::from_slice(&resp).unwrap();
    assert!(
        v.as_object().unwrap().get("UserProfile.country").is_none(),
        "tombstoned row must be filtered out of GET response. got: {:?}",
        v
    );
}

#[tokio::test]
async fn push_table_overwrites_prior_live_row() {
    let (port, state) = start_test_server().await;
    let mut s = TcpStream::connect(("127.0.0.1", port)).await.unwrap();

    register_table(&mut s, "UserProfile", "user_id").await;

    // First push — two fields.
    let p = build_push_table_payload(
        "UserProfile",
        "u9",
        &serde_json::json!({"country": "US", "score": 10}),
    );
    let (status, _) = send_frame(&mut s, OP_PUSH_TABLE, &p).await;
    assert_eq!(status, STATUS_OK);

    // Second push — different value; whole-row replacement semantics
    // (matching `upsert_table_row` in plan 01).
    let p = build_push_table_payload(
        "UserProfile",
        "u9",
        &serde_json::json!({"country": "CA"}),
    );
    let (status, _) = send_frame(&mut s, OP_PUSH_TABLE, &p).await;
    assert_eq!(status, STATUS_OK);

    let row = state
        .store
        .get_table_row("u9", "UserProfile")
        .expect("row exists");
    assert_eq!(row.state, TableRowState::Live);
    assert_eq!(row.fields.len(), 1, "fields replaced whole; got: {:?}", row.fields);
}
