//! Phase 25-01, Task 3 — End-to-end TCP integration tests for OP_GET_MULTI.
//!
//! Covers the behaviour contract in `25-01-PLAN.md` across the wire:
//!
//!   * degenerate single-table matches per-slice GET
//!   * three-table happy path
//!   * missing-key null-collapse for every table
//!   * mixed present/absent tables
//!   * tombstoned rows collapse to null
//!   * composite-key routing (JSON-array key)
//!   * unknown table name → atomic STATUS_ERROR, no partial JSON
//!   * count=0 and cardinality-cap guards
//!   * prefix-collision safety (`User.` vs `UserProfile.`)
//!   * response body keys in REQUEST order (not sorted)
//!
//! Reserved-opcode (SCAN/SUBSCRIBE) coverage lives in
//! `tests/test_reserved_opcodes.rs`.
//!
//! Uses the same `make_concurrent_state` + `run_tcp_server_with_listener`
//! pattern as `tests/test_op_push_table.rs`.

use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use beava::engine::pipeline::PipelineEngine;
use beava::server::protocol::{
    self, OP_DELETE_TABLE, OP_GET, OP_GET_MULTI, OP_PUSH_TABLE, OP_REGISTER, STATUS_ERROR,
    STATUS_OK,
};
use beava::server::tcp::{make_concurrent_state, BackfillTracker, SharedState};
use beava::state::store::StateStore;

// ---------------------------------------------------------------------------
// Server + frame helpers
// ---------------------------------------------------------------------------

async fn start_test_server(snapshot_tag: &str) -> (u16, SharedState) {
    let state: SharedState = make_concurrent_state(
        PipelineEngine::new(),
        StateStore::new(),
        None,
        std::path::PathBuf::from(format!("test_get_multi_{}.snapshot", snapshot_tag)),
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

/// Build an OP_GET_MULTI payload: [u16 count][count × u16-string name][u16-string key].
fn build_get_multi_payload(tables: &[&str], key: &str) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.extend_from_slice(&(tables.len() as u16).to_be_bytes());
    for t in tables {
        buf.extend_from_slice(&protocol::write_string(t));
    }
    buf.extend_from_slice(&protocol::write_string(key));
    buf
}

/// Register a v0 Table source with the shape the Python SDK's
/// `@tl.table` emits — matches what the server's V0RegisterPayload expects.
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

/// Register a Table whose key is a composite of two fields.
async fn register_composite_table(stream: &mut TcpStream, table_name: &str, key_fields: &[&str]) {
    let def = serde_json::json!({
        "name": table_name,
        "kind": "table",
        "mode": "overwrite",
        "key_fields": key_fields,
        "fields": {
            "payload": {"type": "str", "optional": true},
        },
    });
    let payload = serde_json::to_vec(&def).unwrap();
    let (status, resp) = send_frame(stream, OP_REGISTER, &payload).await;
    assert_eq!(
        status,
        STATUS_OK,
        "register composite table {} failed: {}",
        table_name,
        String::from_utf8_lossy(&resp)
    );
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn get_multi_single_table_degenerate_matches_get_slice() {
    let (port, _state) = start_test_server("single_table").await;
    let mut s = TcpStream::connect(("127.0.0.1", port)).await.unwrap();

    register_table(&mut s, "UserProfile", "user_id").await;

    let fields = serde_json::json!({"country": "US", "score": 42});
    let (status, _) = send_frame(
        &mut s,
        OP_PUSH_TABLE,
        &build_push_table_payload("UserProfile", "u1", &fields),
    )
    .await;
    assert_eq!(status, STATUS_OK);

    let payload = build_get_multi_payload(&["UserProfile"], "u1");
    let (status, resp) = send_frame(&mut s, OP_GET_MULTI, &payload).await;
    assert_eq!(
        status,
        STATUS_OK,
        "get_multi: {}",
        String::from_utf8_lossy(&resp)
    );

    let v: serde_json::Value = serde_json::from_slice(&resp).unwrap();
    let obj = v.as_object().expect("response must be JSON object");
    let up = obj
        .get("UserProfile")
        .expect("UserProfile key present")
        .as_object()
        .expect("UserProfile value is an object (row exists)");

    assert_eq!(up.get("country").and_then(|x| x.as_str()), Some("US"));
    assert_eq!(up.get("score").and_then(|x| x.as_i64()), Some(42));

    // Degenerate-matches-GET check: the slice projected into GET_MULTI must
    // agree with the per-Table slice of a plain GET.
    let (status, get_resp) = send_frame(&mut s, OP_GET, &build_get_payload("u1")).await;
    assert_eq!(status, STATUS_OK);
    let gv: serde_json::Value = serde_json::from_slice(&get_resp).unwrap();
    let gobj = gv.as_object().unwrap();
    assert_eq!(
        gobj.get("UserProfile.country").and_then(|x| x.as_str()),
        up.get("country").and_then(|x| x.as_str()),
    );
    assert_eq!(
        gobj.get("UserProfile.score").and_then(|x| x.as_i64()),
        up.get("score").and_then(|x| x.as_i64()),
    );
}

#[tokio::test]
async fn get_multi_three_tables_happy_path() {
    let (port, _state) = start_test_server("three_tables").await;
    let mut s = TcpStream::connect(("127.0.0.1", port)).await.unwrap();

    register_table(&mut s, "A", "user_id").await;
    register_table(&mut s, "B", "user_id").await;
    register_table(&mut s, "C", "user_id").await;

    for name in ["A", "B", "C"] {
        let fields = serde_json::json!({"country": "US", "score": 1});
        let (status, _) = send_frame(
            &mut s,
            OP_PUSH_TABLE,
            &build_push_table_payload(name, "u1", &fields),
        )
        .await;
        assert_eq!(status, STATUS_OK);
    }

    let payload = build_get_multi_payload(&["A", "B", "C"], "u1");
    let (status, resp) = send_frame(&mut s, OP_GET_MULTI, &payload).await;
    assert_eq!(status, STATUS_OK);

    let v: serde_json::Value = serde_json::from_slice(&resp).unwrap();
    let obj = v.as_object().unwrap();
    for name in ["A", "B", "C"] {
        let row = obj
            .get(name)
            .unwrap_or_else(|| panic!("{} missing", name))
            .as_object()
            .unwrap_or_else(|| panic!("{} was null, expected row", name));
        assert_eq!(row.get("country").and_then(|x| x.as_str()), Some("US"));
    }
}

#[tokio::test]
async fn get_multi_missing_key_all_null_collapse() {
    let (port, _state) = start_test_server("missing_key").await;
    let mut s = TcpStream::connect(("127.0.0.1", port)).await.unwrap();

    register_table(&mut s, "A", "user_id").await;
    register_table(&mut s, "B", "user_id").await;

    // No pushes. GET_MULTI for an unseen key — every table slot must be null.
    let payload = build_get_multi_payload(&["A", "B"], "ghost");
    let (status, resp) = send_frame(&mut s, OP_GET_MULTI, &payload).await;
    assert_eq!(status, STATUS_OK);

    let v: serde_json::Value = serde_json::from_slice(&resp).unwrap();
    let obj = v.as_object().unwrap();
    assert!(
        obj.get("A").unwrap().is_null(),
        "never-seen key must collapse to null for A, got {:?}",
        obj.get("A")
    );
    assert!(
        obj.get("B").unwrap().is_null(),
        "never-seen key must collapse to null for B, got {:?}",
        obj.get("B")
    );
}

#[tokio::test]
async fn get_multi_mixed_present_and_absent() {
    let (port, _state) = start_test_server("mixed").await;
    let mut s = TcpStream::connect(("127.0.0.1", port)).await.unwrap();

    register_table(&mut s, "A", "user_id").await;
    register_table(&mut s, "B", "user_id").await;

    let fields = serde_json::json!({"country": "FR"});
    let (status, _) = send_frame(
        &mut s,
        OP_PUSH_TABLE,
        &build_push_table_payload("A", "u1", &fields),
    )
    .await;
    assert_eq!(status, STATUS_OK);

    let payload = build_get_multi_payload(&["A", "B"], "u1");
    let (status, resp) = send_frame(&mut s, OP_GET_MULTI, &payload).await;
    assert_eq!(status, STATUS_OK);

    let v: serde_json::Value = serde_json::from_slice(&resp).unwrap();
    let obj = v.as_object().unwrap();
    assert_eq!(
        obj.get("A")
            .unwrap()
            .as_object()
            .unwrap()
            .get("country")
            .and_then(|x| x.as_str()),
        Some("FR")
    );
    assert!(obj.get("B").unwrap().is_null(), "B not pushed → null");
}

#[tokio::test]
async fn get_multi_tombstoned_returns_null() {
    let (port, _state) = start_test_server("tombstone").await;
    let mut s = TcpStream::connect(("127.0.0.1", port)).await.unwrap();

    register_table(&mut s, "A", "user_id").await;

    let fields = serde_json::json!({"country": "JP"});
    let (status, _) = send_frame(
        &mut s,
        OP_PUSH_TABLE,
        &build_push_table_payload("A", "u1", &fields),
    )
    .await;
    assert_eq!(status, STATUS_OK);

    // Tombstone
    let (status, _) = send_frame(
        &mut s,
        OP_DELETE_TABLE,
        &build_delete_table_payload("A", "u1"),
    )
    .await;
    assert_eq!(status, STATUS_OK);

    let payload = build_get_multi_payload(&["A"], "u1");
    let (status, resp) = send_frame(&mut s, OP_GET_MULTI, &payload).await;
    assert_eq!(status, STATUS_OK);

    let v: serde_json::Value = serde_json::from_slice(&resp).unwrap();
    let obj = v.as_object().unwrap();
    assert!(
        obj.get("A").unwrap().is_null(),
        "tombstoned row must collapse to null, got {:?}",
        obj.get("A")
    );
}

#[tokio::test]
async fn get_multi_composite_key_routes_correctly() {
    let (port, _state) = start_test_server("composite").await;
    let mut s = TcpStream::connect(("127.0.0.1", port)).await.unwrap();

    register_composite_table(&mut s, "UserRegion", &["user_id", "region"]).await;

    // Composite keys on the wire use the US (\x1f) separator between
    // components (matches v0-restructure-spec §6.2 — same encoding
    // app.get_multi uses for composite keys). Push with that joined key.
    let composite = "u1\x1fAPAC";
    let fields = serde_json::json!({"payload": "hello"});
    let (status, resp) = send_frame(
        &mut s,
        OP_PUSH_TABLE,
        &build_push_table_payload("UserRegion", composite, &fields),
    )
    .await;
    assert_eq!(
        status,
        STATUS_OK,
        "composite push_table failed: {}",
        String::from_utf8_lossy(&resp)
    );

    let payload = build_get_multi_payload(&["UserRegion"], composite);
    let (status, resp) = send_frame(&mut s, OP_GET_MULTI, &payload).await;
    assert_eq!(
        status,
        STATUS_OK,
        "get_multi composite: {}",
        String::from_utf8_lossy(&resp)
    );
    let v: serde_json::Value = serde_json::from_slice(&resp).unwrap();
    let obj = v.as_object().unwrap();
    let row = obj
        .get("UserRegion")
        .expect("UserRegion present")
        .as_object()
        .expect("row is an object");
    assert_eq!(row.get("payload").and_then(|x| x.as_str()), Some("hello"));
}

#[tokio::test]
async fn get_multi_unknown_table_returns_atomic_error() {
    let (port, _state) = start_test_server("unknown_table").await;
    let mut s = TcpStream::connect(("127.0.0.1", port)).await.unwrap();

    register_table(&mut s, "A", "user_id").await;

    // "DoesNotExist" is unregistered. The whole request must fail —
    // no {"A": ..., "DoesNotExist": error} partial.
    let payload = build_get_multi_payload(&["A", "DoesNotExist"], "u1");
    let (status, resp) = send_frame(&mut s, OP_GET_MULTI, &payload).await;
    assert_eq!(
        status,
        STATUS_ERROR,
        "unknown table must abort whole request, got status={} resp={}",
        status,
        String::from_utf8_lossy(&resp)
    );
    let msg = String::from_utf8_lossy(&resp);
    assert!(
        msg.contains("unknown table") || msg.contains("DoesNotExist"),
        "error message should mention unknown table / DoesNotExist, got: {}",
        msg
    );

    // Connection survival: a subsequent GET still works.
    let (status, _) = send_frame(&mut s, OP_GET, &build_get_payload("u1")).await;
    assert_eq!(
        status, STATUS_OK,
        "connection must stay open after GET_MULTI atomic error"
    );
}

#[tokio::test]
async fn get_multi_count_zero_rejected() {
    let (port, _state) = start_test_server("count_zero").await;
    let mut s = TcpStream::connect(("127.0.0.1", port)).await.unwrap();

    // count=0 with an empty key — server must reject at parse.
    let mut payload = Vec::new();
    payload.extend_from_slice(&0u16.to_be_bytes());
    payload.extend_from_slice(&protocol::write_string("u1"));
    let (status, resp) = send_frame(&mut s, OP_GET_MULTI, &payload).await;
    assert_eq!(status, STATUS_ERROR);
    let msg = String::from_utf8_lossy(&resp);
    assert!(
        msg.contains("at least one"),
        "count=0 error should mention 'at least one', got: {}",
        msg
    );
}

#[tokio::test]
async fn get_multi_count_cap_rejected() {
    let (port, _state) = start_test_server("count_cap").await;
    let mut s = TcpStream::connect(("127.0.0.1", port)).await.unwrap();

    // Count=257 > cap(256). Use degenerate 1-byte "X" names to keep payload small.
    let count: u16 = 257;
    let mut payload = Vec::new();
    payload.extend_from_slice(&count.to_be_bytes());
    for _ in 0..count {
        payload.extend_from_slice(&protocol::write_string("X"));
    }
    payload.extend_from_slice(&protocol::write_string("u1"));
    let (status, resp) = send_frame(&mut s, OP_GET_MULTI, &payload).await;
    assert_eq!(status, STATUS_ERROR);
    let msg = String::from_utf8_lossy(&resp);
    assert!(
        msg.to_lowercase().contains("exceeds") || msg.contains("256"),
        "count cap error should mention cap, got: {}",
        msg
    );
}

#[tokio::test]
async fn get_multi_prefix_collision_safety() {
    // Regression guard (plan Risk #1): table `User` must NOT leak into
    // `UserProfile`'s slice via a prefix check. Null-collapse correctness
    // depends on scoping the merge by exact table name, not by string
    // prefix on `T.field`.
    let (port, _state) = start_test_server("prefix_collision").await;
    let mut s = TcpStream::connect(("127.0.0.1", port)).await.unwrap();

    register_table(&mut s, "User", "user_id").await;
    register_table(&mut s, "UserProfile", "user_id").await;

    let user_fields = serde_json::json!({"country": "ZZ"});
    let (status, _) = send_frame(
        &mut s,
        OP_PUSH_TABLE,
        &build_push_table_payload("User", "u1", &user_fields),
    )
    .await;
    assert_eq!(status, STATUS_OK);

    // Only `User` has a row; `UserProfile` has NONE. GET_MULTI must
    // return UserProfile=null (not accidentally inheriting User's fields).
    let payload = build_get_multi_payload(&["User", "UserProfile"], "u1");
    let (status, resp) = send_frame(&mut s, OP_GET_MULTI, &payload).await;
    assert_eq!(status, STATUS_OK);
    let v: serde_json::Value = serde_json::from_slice(&resp).unwrap();
    let obj = v.as_object().unwrap();
    assert_eq!(
        obj.get("User")
            .unwrap()
            .as_object()
            .unwrap()
            .get("country")
            .and_then(|x| x.as_str()),
        Some("ZZ")
    );
    assert!(
        obj.get("UserProfile").unwrap().is_null(),
        "prefix-collision: UserProfile must collapse to null, got {:?}",
        obj.get("UserProfile")
    );
}

#[tokio::test]
async fn get_multi_response_preserves_request_order() {
    // Spec: server serializes response keys in request order (handler
    // writes JSON by hand to guarantee this independent of serde_json's
    // `preserve_order` feature flag).
    let (port, _state) = start_test_server("request_order").await;
    let mut s = TcpStream::connect(("127.0.0.1", port)).await.unwrap();

    register_table(&mut s, "Zeta", "user_id").await;
    register_table(&mut s, "Alpha", "user_id").await;
    register_table(&mut s, "Mu", "user_id").await;

    let payload = build_get_multi_payload(&["Zeta", "Alpha", "Mu"], "u_nobody");
    let (status, resp) = send_frame(&mut s, OP_GET_MULTI, &payload).await;
    assert_eq!(status, STATUS_OK);

    // Text-level assertion: keys appear in request order in the raw body.
    let body = std::str::from_utf8(&resp).unwrap();
    let zeta_at = body.find("\"Zeta\"").expect("Zeta present");
    let alpha_at = body.find("\"Alpha\"").expect("Alpha present");
    let mu_at = body.find("\"Mu\"").expect("Mu present");
    assert!(
        zeta_at < alpha_at && alpha_at < mu_at,
        "response keys must preserve request order; got body={}",
        body
    );
}
