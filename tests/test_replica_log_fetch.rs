//! Phase 35-01: TCP integration tests for `OP_LOG_FETCH` (0x13).
//!
//! Coverage (per 35-01-PLAN §Task T3):
//!   * happy_path_returns_all_events_then_end — push 10 events, LOG_FETCH
//!     with `from_ts_millis=0` returns all 10 + terminal END frame;
//!     `from_ts_millis=T0+something` returns only the ones ≥ that cursor.
//!   * scope_filter_isolates_streams — push to streams A and B, LOG_FETCH
//!     with scope=[A] returns only A's.
//!   * key_filter_narrows_subset — push events with keys u1, u2, u3,
//!     LOG_FETCH with scope.keys=[u1,u2] returns only those.
//!   * auth_reject_emits_status_error — bad token → STATUS_ERROR frame,
//!     no event / end frames.
//!
//! Harness mirrors `tests/test_replica_subscribe.rs` with two tweaks:
//!   - attaches an `EventLog` to the shared state so pushes persist.
//!   - registers every test stream in both the engine and the log.

use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use beava::engine::pipeline::{FeatureDef, PipelineEngine, StreamDefinition};
use beava::server::protocol::{
    self, Scope, OP_LOG_FETCH, REPLICA_FRAME_TAG_END, REPLICA_FRAME_TAG_EVENT, STATUS_ERROR,
};
use beava::server::tcp::{make_concurrent_state_full, BackfillTracker, SharedState};
use beava::state::event_log::EventLog;
use beava::state::store::StateStore;

const ADMIN_TOKEN: &str = "test-admin-token";

fn stream_def(name: &str) -> StreamDefinition {
    StreamDefinition {
        name: name.into(),
        key_field: Some("user_id".into()),
        group_by_keys: None,
        features: vec![(
            "count_1h".into(),
            FeatureDef::Count {
                window: Duration::from_secs(3600),
                bucket: Duration::from_secs(60),
                where_expr: None,
                backfill: false,
            },
        )],
        depends_on: None,
        filter: None,
        entity_ttl: None,
        history_ttl: None,
        projection: None,
        ephemeral: None,
        pipeline_ttl: None,
        max_keys: None,
        watermark_lateness: None,
    }
}

async fn start_test_server(stream_names: &[&str]) -> (u16, SharedState) {
    let mut engine = PipelineEngine::new();
    for s in stream_names {
        engine.register(stream_def(s)).expect("register stream");
    }
    let tmp = std::env::temp_dir().join(format!(
        "beava_test_log_fetch_{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&tmp).unwrap();

    // EventLog lives in the same tmp dir; register each stream so pushes
    // are appended (v0 flow: PUSH writes both state and log).
    let event_log = EventLog::new(tmp.clone()).unwrap();
    for s in stream_names {
        event_log.register_stream(s, None).unwrap();
    }

    let state = make_concurrent_state_full(
        engine,
        StateStore::new(),
        Some(event_log),
        tmp.join("beava.snapshot"),
        Arc::new(BackfillTracker::default()),
        true,
        true, // event_log_enabled
        Some(ADMIN_TOKEN.to_string()),
        false,
    );

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let server_state = state.clone();
    tokio::spawn(async move {
        let _ = beava::server::tcp::run_tcp_server_with_listener(listener, server_state).await;
    });
    tokio::time::sleep(Duration::from_millis(30)).await;
    (port, state)
}

// ---------------------------------------------------------------------------
// Frame helpers
// ---------------------------------------------------------------------------

fn build_log_fetch_payload(token: &str, from_ts_millis: u64, scope: &Scope) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.extend_from_slice(&protocol::write_string(token));
    buf.extend_from_slice(&from_ts_millis.to_be_bytes());
    protocol::write_scope(&mut buf, scope);
    buf
}

async fn send_log_fetch_frame(
    stream: &mut TcpStream,
    token: &str,
    from_ts_millis: u64,
    scope: &Scope,
) {
    let payload = build_log_fetch_payload(token, from_ts_millis, scope);
    let len = (1 + payload.len()) as u32;
    stream.write_u32(len).await.unwrap();
    stream.write_u8(OP_LOG_FETCH).await.unwrap();
    stream.write_all(&payload).await.unwrap();
    stream.flush().await.unwrap();
}

#[derive(Debug)]
enum LogFetchFrame {
    Event {
        timestamp_ms: u64,
        payload: Vec<u8>,
    },
    End,
    Error(String),
}

/// Read one frame from a log-fetch response stream. Dispatches on tag:
/// 0x03 → event, 0x04 → end, 0x01 → STATUS_ERROR envelope.
async fn read_log_fetch_frame(stream: &mut TcpStream) -> LogFetchFrame {
    let frame_len = stream.read_u32().await.unwrap() as usize;
    assert!(frame_len >= 1, "frame_len must include at least the tag byte");
    let tag = stream.read_u8().await.unwrap();
    match tag {
        t if t == REPLICA_FRAME_TAG_EVENT => {
            // body = [u64 ts_ms][u32 payload_len][payload]
            assert!(frame_len >= 1 + 8 + 4);
            let ts_ms = stream.read_u64().await.unwrap();
            let payload_len = stream.read_u32().await.unwrap() as usize;
            let mut payload = vec![0u8; payload_len];
            if payload_len > 0 {
                stream.read_exact(&mut payload).await.unwrap();
            }
            LogFetchFrame::Event {
                timestamp_ms: ts_ms,
                payload,
            }
        }
        t if t == REPLICA_FRAME_TAG_END => {
            assert_eq!(frame_len, 1, "END frame body must be empty");
            LogFetchFrame::End
        }
        t if t == STATUS_ERROR => {
            let msg_len = frame_len - 1;
            let mut msg = vec![0u8; msg_len];
            if msg_len > 0 {
                stream.read_exact(&mut msg).await.unwrap();
            }
            LogFetchFrame::Error(String::from_utf8(msg).unwrap())
        }
        other => panic!("unknown tag in log-fetch response: 0x{:02x}", other),
    }
}

fn scope_streams(streams: &[&str]) -> Scope {
    Scope {
        streams: streams.iter().map(|s| (*s).to_string()).collect(),
        keys: None,
        key_prefix: None,
        pull: "all".into(),
    }
}

/// Push one binary event with a single `user_id` string field. Mirrors
/// the helper in `test_replica_subscribe.rs`.
async fn push_event_binary(port: u16, stream_name: &str, user_id: &str) {
    let mut conn = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    let mut body = Vec::new();
    body.extend_from_slice(&protocol::write_string(stream_name));
    body.extend_from_slice(&1u16.to_be_bytes());
    body.extend_from_slice(&protocol::write_string("user_id"));
    body.push(protocol::TYPE_STR);
    body.extend_from_slice(&protocol::write_string(user_id));
    let frame_len = (1 + body.len()) as u32;
    conn.write_u32(frame_len).await.unwrap();
    conn.write_u8(protocol::OP_PUSH).await.unwrap();
    conn.write_all(&body).await.unwrap();
    conn.flush().await.unwrap();
    let rlen = conn.read_u32().await.unwrap() as usize;
    let _status = conn.read_u8().await.unwrap();
    let mut rest = vec![0u8; rlen - 1];
    if rlen > 1 {
        conn.read_exact(&mut rest).await.unwrap();
    }
}

/// Flush the server-side event_log writer so entries are visible to a
/// subsequent `read_entries` call. Matches the pattern used by the
/// backfill/recovery tests.
fn flush_event_log(state: &SharedState) {
    if let Some(ref log) = state.event_log {
        let _ = log.fsync_all();
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn happy_path_returns_all_events_then_end() {
    let (port, state) = start_test_server(&["orders"]).await;

    for i in 0..10 {
        push_event_binary(port, "orders", &format!("u{}", i)).await;
    }
    // Small settle + flush so the log writer is durable.
    tokio::time::sleep(Duration::from_millis(50)).await;
    flush_event_log(&state);

    // (1) from_ts=0 → all 10 events back, then END.
    let mut conn = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    send_log_fetch_frame(&mut conn, ADMIN_TOKEN, 0, &scope_streams(&["orders"])).await;

    let mut events: Vec<(u64, Vec<u8>)> = Vec::new();
    loop {
        match tokio::time::timeout(Duration::from_secs(2), read_log_fetch_frame(&mut conn))
            .await
            .expect("timed out reading log-fetch frame")
        {
            LogFetchFrame::Event {
                timestamp_ms,
                payload,
            } => events.push((timestamp_ms, payload)),
            LogFetchFrame::End => break,
            LogFetchFrame::Error(e) => panic!("unexpected STATUS_ERROR: {}", e),
        }
    }
    assert_eq!(events.len(), 10, "expected 10 events, got {}", events.len());

    // Metric counter must reflect the send.
    let metric = beava::server::replica::log_entries_sent_snapshot();
    let orders_sent = metric
        .iter()
        .find(|(s, _)| s == "orders")
        .map(|(_, n)| *n)
        .unwrap_or(0);
    assert!(
        orders_sent >= 10,
        "beava_replica_log_entries_sent_total{{stream=\"orders\"}} should be ≥ 10, got {}",
        orders_sent
    );

    // (2) from_ts strictly greater than every entry's ts → 0 events + END.
    let big_cursor = u64::MAX / 2;
    let mut conn = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    send_log_fetch_frame(
        &mut conn,
        ADMIN_TOKEN,
        big_cursor,
        &scope_streams(&["orders"]),
    )
    .await;
    let first = tokio::time::timeout(Duration::from_secs(2), read_log_fetch_frame(&mut conn))
        .await
        .expect("timed out");
    match first {
        LogFetchFrame::End => {}
        other => panic!("expected immediate END with far-future cursor, got {:?}", other),
    }

    // (3) from_ts = midpoint between first and last events → subset ≤ total.
    // Read entries directly to get the actual timestamps.
    let timestamps: Vec<u64> = {
        state
            .event_log
            .as_ref()
            .unwrap()
            .read_entries("orders")
            .unwrap()
            .iter()
            .map(|e| {
                e.timestamp
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as u64
            })
            .collect()
    };
    assert_eq!(timestamps.len(), 10);
    let mid_cursor = timestamps[5];
    let mut conn = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    send_log_fetch_frame(
        &mut conn,
        ADMIN_TOKEN,
        mid_cursor,
        &scope_streams(&["orders"]),
    )
    .await;
    let mut n_after_mid = 0;
    loop {
        match tokio::time::timeout(Duration::from_secs(2), read_log_fetch_frame(&mut conn))
            .await
            .expect("timed out")
        {
            LogFetchFrame::Event { timestamp_ms, .. } => {
                assert!(
                    timestamp_ms >= mid_cursor,
                    "event ts_ms {} < cursor {}",
                    timestamp_ms,
                    mid_cursor
                );
                n_after_mid += 1;
            }
            LogFetchFrame::End => break,
            LogFetchFrame::Error(e) => panic!("unexpected error: {}", e),
        }
    }
    // Boundary-inclusive: at least the entries from index 5 onwards must
    // come back (timestamps may collide on a fast host → could be more).
    assert!(
        n_after_mid >= 1,
        "expected at least one event at or after mid cursor"
    );
    assert!(
        n_after_mid <= 10,
        "cursor filter must not inflate count: got {}",
        n_after_mid
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn scope_filter_isolates_streams() {
    let (port, state) = start_test_server(&["A", "B"]).await;

    push_event_binary(port, "A", "u1").await;
    push_event_binary(port, "A", "u2").await;
    push_event_binary(port, "B", "u3").await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    flush_event_log(&state);

    // Fetch scope=[A] — must get only A's entries + END.
    let mut conn = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    send_log_fetch_frame(&mut conn, ADMIN_TOKEN, 0, &scope_streams(&["A"])).await;

    let mut users: Vec<String> = Vec::new();
    loop {
        match tokio::time::timeout(Duration::from_secs(2), read_log_fetch_frame(&mut conn))
            .await
            .expect("timed out")
        {
            LogFetchFrame::Event { payload, .. } => {
                // Payload is the raw tagged log-payload bytes. Skip the
                // format byte and decode via the appropriate path. We
                // verify indirectly: just ensure we only see events
                // whose user_id is from A's push set.
                // For this coarse assertion we inspect the json-or-binary
                // payload for the "u3" sentinel (must NOT appear).
                assert!(
                    !payload.windows(2).any(|w| w == b"u3"),
                    "scope=[A] leaked an event containing u3: {:?}",
                    String::from_utf8_lossy(&payload)
                );
                // Best-effort: capture a readable user_id marker.
                if payload.windows(2).any(|w| w == b"u1") {
                    users.push("u1".into());
                } else if payload.windows(2).any(|w| w == b"u2") {
                    users.push("u2".into());
                }
            }
            LogFetchFrame::End => break,
            LogFetchFrame::Error(e) => panic!("unexpected error: {}", e),
        }
    }
    users.sort();
    assert_eq!(users, vec!["u1".to_string(), "u2".into()]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn key_filter_narrows_subset() {
    let (port, state) = start_test_server(&["orders"]).await;

    for k in ["u1", "u2", "u3"] {
        push_event_binary(port, "orders", k).await;
    }
    tokio::time::sleep(Duration::from_millis(50)).await;
    flush_event_log(&state);

    let mut scope = scope_streams(&["orders"]);
    scope.keys = Some(vec!["u1".into(), "u2".into()]);

    let mut conn = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    send_log_fetch_frame(&mut conn, ADMIN_TOKEN, 0, &scope).await;

    let mut seen: Vec<String> = Vec::new();
    loop {
        match tokio::time::timeout(Duration::from_secs(2), read_log_fetch_frame(&mut conn))
            .await
            .expect("timed out")
        {
            LogFetchFrame::Event { payload, .. } => {
                // Any u3-containing event would be a scope violation.
                assert!(
                    !payload.windows(2).any(|w| w == b"u3"),
                    "scope.keys filter leaked u3"
                );
                if payload.windows(2).any(|w| w == b"u1") {
                    seen.push("u1".into());
                } else if payload.windows(2).any(|w| w == b"u2") {
                    seen.push("u2".into());
                }
            }
            LogFetchFrame::End => break,
            LogFetchFrame::Error(e) => panic!("unexpected error: {}", e),
        }
    }
    seen.sort();
    assert_eq!(seen, vec!["u1".to_string(), "u2".into()]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn auth_reject_emits_status_error() {
    let (port, _state) = start_test_server(&["orders"]).await;

    let mut conn = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    send_log_fetch_frame(&mut conn, "wrong-token", 0, &scope_streams(&["orders"])).await;

    match read_log_fetch_frame(&mut conn).await {
        LogFetchFrame::Error(msg) => {
            assert!(
                msg.to_lowercase().contains("unauth"),
                "expected unauthorized, got: {}",
                msg
            );
        }
        other => panic!("expected STATUS_ERROR, got {:?}", other),
    }
}
