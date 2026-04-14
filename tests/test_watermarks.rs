//! Phase 24-04 Task 1: event-time parsing + per-stream watermark
//! tracking + late-event drop counter.
//!
//! Covers the 8 behaviors locked in the plan:
//!
//!   * event_time_parse_iso8601
//!   * event_time_parse_unix_ms
//!   * event_time_parse_unix_seconds_float
//!   * event_time_absent_uses_wall_clock
//!   * watermark_tracks_max_minus_5s
//!   * late_event_dropped_with_counter_increment
//!   * late_event_within_5s_window_accepted
//!   * per_stream_watermark_isolation
//!
//! Plus a /metrics regression so the exported counter shows up on the wire.
//!
//! The first three tests are pure parser checks (no server). The remaining
//! five drive events through a real TCP listener (same pattern as
//! `tests/test_op_push_table.rs`).

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use axum::body::Body;
use axum::extract::ConnectInfo;
use axum::http::Request;
use tower::ServiceExt;

use tally::engine::event_time::{parse_event_time, WATERMARK_LATENESS};
use tally::engine::pipeline::PipelineEngine;
use tally::server::http::build_router;
use tally::server::protocol::{
    self, OP_PUSH, OP_REGISTER, STATUS_OK, TYPE_I64, TYPE_STR,
};
use tally::server::tcp::{make_concurrent_state, BackfillTracker, SharedState};
use tally::state::store::StateStore;

// ---------------------------------------------------------------------------
// Parser tests (pure unit, no server)
// ---------------------------------------------------------------------------

#[test]
fn event_time_parse_iso8601() {
    let payload = serde_json::json!({"_event_time": "2026-04-14T00:00:00Z"});
    let fallback = UNIX_EPOCH;
    let t = parse_event_time(&payload, fallback);
    let expected = UNIX_EPOCH + Duration::from_secs(20557 * 86_400);
    assert_eq!(t, expected);
}

#[test]
fn event_time_parse_unix_ms() {
    // 3_000_000_000 > 2^31 → interpreted as ms → 3,000,000 seconds since epoch.
    let payload = serde_json::json!({"_event_time": 3_000_000_000i64});
    let t = parse_event_time(&payload, UNIX_EPOCH);
    assert_eq!(t, UNIX_EPOCH + Duration::from_secs(3_000_000));
}

#[test]
fn event_time_parse_unix_seconds_float() {
    let payload = serde_json::json!({"_event_time": 1000.5});
    let t = parse_event_time(&payload, UNIX_EPOCH);
    assert_eq!(t, UNIX_EPOCH + Duration::new(1000, 500_000_000));
}

#[test]
fn event_time_absent_uses_wall_clock() {
    let fallback = UNIX_EPOCH + Duration::from_secs(42);
    let payload = serde_json::json!({"user_id": "u1"});
    assert_eq!(parse_event_time(&payload, fallback), fallback);
}

// ---------------------------------------------------------------------------
// Server integration tests — drive events through real TCP.
// ---------------------------------------------------------------------------

async fn start_test_server() -> (u16, SharedState) {
    let state: SharedState = make_concurrent_state(
        PipelineEngine::new(),
        StateStore::new(),
        None,
        std::path::PathBuf::from("test_watermarks.snapshot"),
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

    tokio::time::sleep(Duration::from_millis(20)).await;
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

async fn register_clicks_stream(stream: &mut TcpStream, name: &str) {
    let def = serde_json::json!({
        "name": name,
        "kind": "stream",
        "key_field": "user_id",
        "fields": {
            "user_id":     {"type": "str", "optional": false},
            "_event_time": {"type": "i64", "optional": true},
        },
    });
    let payload = serde_json::to_vec(&def).unwrap();
    let (status, resp) = send_frame(stream, OP_REGISTER, &payload).await;
    assert_eq!(
        status,
        STATUS_OK,
        "register {} failed: {}",
        name,
        String::from_utf8_lossy(&resp)
    );
}

/// Build an OP_PUSH binary payload with an `_event_time` unix-seconds i64 field.
fn build_push_with_et(stream_name: &str, user_id: &str, event_time_secs: i64) -> Vec<u8> {
    let mut buf = protocol::write_string(stream_name);
    // 2 fields: user_id (str), _event_time (i64).
    buf.extend_from_slice(&(2u16).to_be_bytes());
    // user_id
    buf.extend_from_slice(&protocol::write_string("user_id"));
    buf.push(TYPE_STR);
    buf.extend_from_slice(&protocol::write_string(user_id));
    // _event_time (interpreted as unix seconds when < 2^31)
    buf.extend_from_slice(&protocol::write_string("_event_time"));
    buf.push(TYPE_I64);
    buf.extend_from_slice(&event_time_secs.to_be_bytes());
    buf
}

fn sec(s: u64) -> SystemTime {
    UNIX_EPOCH + Duration::from_secs(s)
}

#[tokio::test]
async fn watermark_tracks_max_minus_5s() {
    let (port, state) = start_test_server().await;
    let mut s = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    register_clicks_stream(&mut s, "Clicks").await;

    // Push three events with event_times 100, 110, 105.
    for et in [100, 110, 105] {
        let payload = build_push_with_et("Clicks", "u1", et);
        let (status, _) = send_frame(&mut s, OP_PUSH, &payload).await;
        assert_eq!(status, STATUS_OK);
    }

    let engine = state.engine.read();
    let wm = engine.watermarks.read().watermark("Clicks").unwrap();
    let observed_max = engine.watermarks.read().observed_max("Clicks").unwrap();
    assert_eq!(observed_max, sec(110));
    assert_eq!(wm, sec(110) - WATERMARK_LATENESS);
    // 110 − 5 = 105.
    assert_eq!(wm, sec(105));
}

#[tokio::test]
async fn late_event_dropped_with_counter_increment() {
    let (port, state) = start_test_server().await;
    let mut s = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    register_clicks_stream(&mut s, "Clicks").await;

    // First event at t=100 → wm=95.
    let (status, _) = send_frame(
        &mut s,
        OP_PUSH,
        &build_push_with_et("Clicks", "u1", 100),
    )
    .await;
    assert_eq!(status, STATUS_OK);

    // Now push at t=94 (< watermark=95) → dropped. Response is still OK
    // (silent drop per plan); the counter captures the event.
    let (status, _) = send_frame(
        &mut s,
        OP_PUSH,
        &build_push_with_et("Clicks", "u1", 94),
    )
    .await;
    assert_eq!(status, STATUS_OK);

    let engine = state.engine.read();
    let count = engine.late_drops.read().get("Clicks");
    assert_eq!(count, 1, "late-drop counter should have incremented once");
}

#[tokio::test]
async fn late_event_within_5s_window_accepted() {
    let (port, state) = start_test_server().await;
    let mut s = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    register_clicks_stream(&mut s, "Clicks").await;

    // First event at t=100 → wm=95.
    let (status, _) = send_frame(
        &mut s,
        OP_PUSH,
        &build_push_with_et("Clicks", "u1", 100),
    )
    .await;
    assert_eq!(status, STATUS_OK);

    // Out-of-order event at t=96 (>= wm=95) → accepted.
    let (status, _) = send_frame(
        &mut s,
        OP_PUSH,
        &build_push_with_et("Clicks", "u1", 96),
    )
    .await;
    assert_eq!(status, STATUS_OK);

    let engine = state.engine.read();
    assert_eq!(engine.late_drops.read().get("Clicks"), 0);
    // observed_max stays at 100 (96 < 100).
    assert_eq!(
        engine.watermarks.read().observed_max("Clicks"),
        Some(sec(100))
    );
}

#[tokio::test]
async fn per_stream_watermark_isolation() {
    let (port, state) = start_test_server().await;
    let mut s = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    register_clicks_stream(&mut s, "StreamA").await;
    register_clicks_stream(&mut s, "StreamB").await;

    // StreamA advances to t=1000 → wm=995.
    let (status, _) = send_frame(
        &mut s,
        OP_PUSH,
        &build_push_with_et("StreamA", "u1", 1000),
    )
    .await;
    assert_eq!(status, STATUS_OK);

    // StreamB at t=50 — only the first observation; wm set to 50-5=45.
    let (status, _) = send_frame(
        &mut s,
        OP_PUSH,
        &build_push_with_et("StreamB", "u1", 50),
    )
    .await;
    assert_eq!(status, STATUS_OK);

    // A second StreamA push at t=500 must be dropped (500 < 995); StreamB
    // is unaffected.
    let (status, _) = send_frame(
        &mut s,
        OP_PUSH,
        &build_push_with_et("StreamA", "u1", 500),
    )
    .await;
    assert_eq!(status, STATUS_OK);

    // A StreamB push at t=60 must be accepted (60 > 45).
    let (status, _) = send_frame(
        &mut s,
        OP_PUSH,
        &build_push_with_et("StreamB", "u1", 60),
    )
    .await;
    assert_eq!(status, STATUS_OK);

    let engine = state.engine.read();
    assert_eq!(engine.late_drops.read().get("StreamA"), 1);
    assert_eq!(engine.late_drops.read().get("StreamB"), 0);
    assert_eq!(engine.watermarks.read().watermark("StreamA"), Some(sec(995)));
    assert_eq!(engine.watermarks.read().watermark("StreamB"), Some(sec(55)));
}

#[tokio::test]
async fn late_drop_counter_visible_in_metrics_endpoint() {
    // Use the same ConcurrentAppState but drive through the HTTP router
    // (oneshot).
    let (port, state) = start_test_server().await;
    let mut s = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    register_clicks_stream(&mut s, "Clicks").await;

    // Seed watermark, then drop a late event.
    send_frame(&mut s, OP_PUSH, &build_push_with_et("Clicks", "u1", 100)).await;
    send_frame(&mut s, OP_PUSH, &build_push_with_et("Clicks", "u1", 94)).await;

    // Hit /metrics through the admin router.
    let app = build_router(state.clone());
    let mut req = Request::builder()
        .method("GET")
        .uri("/metrics")
        .body(Body::empty())
        .unwrap();
    let loopback: SocketAddr = "127.0.0.1:1".parse().unwrap();
    req.extensions_mut().insert(ConnectInfo(loopback));
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), 200);
    let body = axum::body::to_bytes(resp.into_body(), 1024 * 1024)
        .await
        .unwrap();
    let text = String::from_utf8_lossy(&body);
    assert!(
        text.contains("tally_late_events_dropped_total"),
        "metrics body missing late-drop counter: {}",
        text
    );
    assert!(
        text.contains("tally_late_events_dropped_total{stream=\"Clicks\"} 1"),
        "metrics body did not show Clicks=1: {}",
        text
    );
}
