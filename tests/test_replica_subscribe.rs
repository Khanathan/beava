//! Phase 27-02, Task 3: TCP integration tests for `OP_SUBSCRIBE` (0x11).
//!
//! Covers (locked by 27-02-PLAN §tasks §action.3):
//!   * subscribe_then_push_delivers_events — happy path, 3 events, ordering
//!     preserved, payloads match.
//!   * backpressure_drops_subscriber — slow reader causes the 10_000-slot
//!     mpsc to fill, subscriber gets dropped with reason=backpressure.
//!   * disconnect_cleans_up_registry — closing the socket decrements the
//!     gauge and increments reason=disconnect.
//!   * subscribe_rejects_missing_auth — STATUS_ERROR + safety/error signal
//!     + no registry entry.
//!   * subscribe_rejects_empty_streams_scope — validate_scope smoke test;
//!     full rule matrix is covered in 27-01's tests.
//!
//! Harness mirrors `tests/test_replica_snapshot_fetch.rs`: one server per
//! test with a random port and an admin token baked into AppState.

use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use beava::engine::pipeline::{FeatureDef, PipelineEngine, StreamDefinition};
use beava::server::protocol::{
    self, Scope, OP_SUBSCRIBE, REPLICA_FRAME_TAG_EVENT, STATUS_ERROR,
};
use beava::server::signals::Category;
use beava::server::tcp::{make_concurrent_state_full, BackfillTracker, SharedState};
use beava::state::store::StateStore;

const ADMIN_TOKEN: &str = "test-admin-token";

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

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
    }
}

async fn start_test_server(stream_names: &[&str]) -> (u16, SharedState) {
    let mut engine = PipelineEngine::new();
    for s in stream_names {
        engine.register(stream_def(s)).expect("register stream");
    }
    let tmp = std::env::temp_dir().join(format!(
        "beava_test_subscribe_{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&tmp).unwrap();
    let state = make_concurrent_state_full(
        engine,
        StateStore::new(),
        None,
        tmp.join("beava.snapshot"),
        Arc::new(BackfillTracker::default()),
        true,
        false,
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

fn build_subscribe_payload(token: &str, scope: &Scope) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.extend_from_slice(&protocol::write_string(token));
    protocol::write_scope(&mut buf, scope);
    buf
}

async fn send_subscribe_frame(stream: &mut TcpStream, token: &str, scope: &Scope) {
    let payload = build_subscribe_payload(token, scope);
    let len = (1 + payload.len()) as u32;
    stream.write_u32(len).await.unwrap();
    stream.write_u8(OP_SUBSCRIBE).await.unwrap();
    stream.write_all(&payload).await.unwrap();
    stream.flush().await.unwrap();
}

/// Read one event frame: `[u32 frame_len][u8 tag][u64 ts_secs][u32 ts_nanos]
/// [u32 payload_len][payload]`. Returns (tag, ts_secs, ts_nanos, payload).
async fn read_event_frame(stream: &mut TcpStream) -> (u8, u64, u32, Vec<u8>) {
    let frame_len = stream.read_u32().await.unwrap();
    assert!(frame_len >= 1 + 8 + 4 + 4);
    let tag = stream.read_u8().await.unwrap();
    let ts_secs = stream.read_u64().await.unwrap();
    let ts_nanos = stream.read_u32().await.unwrap();
    let payload_len = stream.read_u32().await.unwrap() as usize;
    let mut payload = vec![0u8; payload_len];
    if payload_len > 0 {
        stream.read_exact(&mut payload).await.unwrap();
    }
    (tag, ts_secs, ts_nanos, payload)
}

/// Read one STATUS_ERROR frame (`[u32 len][u8 status][msg]`). Returns the
/// error message as UTF-8.
async fn read_error_frame(stream: &mut TcpStream) -> String {
    let len = stream.read_u32().await.unwrap() as usize;
    let status = stream.read_u8().await.unwrap();
    assert_eq!(status, STATUS_ERROR);
    let msg_len = len - 1;
    let mut msg = vec![0u8; msg_len];
    if msg_len > 0 {
        stream.read_exact(&mut msg).await.unwrap();
    }
    String::from_utf8(msg).unwrap()
}

fn scope_streams(streams: &[&str]) -> Scope {
    Scope {
        streams: streams.iter().map(|s| (*s).to_string()).collect(),
        keys: None,
        key_prefix: None,
        pull: "all".into(),
    }
}

/// Drive a PUSH over a fresh TCP connection (client_events use the binary
/// PERF-02 encoding). This tiny helper writes a single PUSH frame with
/// one string field `user_id` so the stream's key_field extraction works.
async fn push_event_binary(port: u16, stream_name: &str, user_id: &str) {
    let mut conn = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    let mut body = Vec::new();
    body.extend_from_slice(&protocol::write_string(stream_name));
    // [u16 field_count=1] + [u16 key_len][key bytes] + [u8 type=TYPE_STR]
    // + [u16 value_len][value bytes]
    body.extend_from_slice(&1u16.to_be_bytes());
    body.extend_from_slice(&protocol::write_string("user_id"));
    body.push(protocol::TYPE_STR);
    body.extend_from_slice(&protocol::write_string(user_id));
    let frame_len = (1 + body.len()) as u32;
    conn.write_u32(frame_len).await.unwrap();
    conn.write_u8(protocol::OP_PUSH).await.unwrap();
    conn.write_all(&body).await.unwrap();
    conn.flush().await.unwrap();
    // Read and discard the STATUS_OK response so the server's BufWriter
    // doesn't stall waiting for the client.
    let rlen = conn.read_u32().await.unwrap() as usize;
    let _status = conn.read_u8().await.unwrap();
    let mut rest = vec![0u8; rlen - 1];
    if rlen > 1 {
        conn.read_exact(&mut rest).await.unwrap();
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn subscribe_then_push_delivers_events() {
    let (port, state) = start_test_server(&["orders"]).await;

    let mut sub = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    send_subscribe_frame(&mut sub, ADMIN_TOKEN, &scope_streams(&["orders"])).await;

    // Give the server a tick to register the session before we start pushing.
    // Poll the gauge to deterministically confirm registration.
    for _ in 0..50 {
        if state.subscriber_registry.active_count() == 1 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert_eq!(state.subscriber_registry.active_count(), 1);

    for k in ["u1", "u2", "u3"] {
        push_event_binary(port, "orders", k).await;
    }

    let mut last_ts: u64 = 0;
    let mut keys_seen: Vec<String> = Vec::new();
    for _ in 0..3 {
        let (tag, ts_secs, _ts_nanos, payload) =
            tokio::time::timeout(Duration::from_secs(2), read_event_frame(&mut sub))
                .await
                .expect("timed out reading event frame");
        assert_eq!(tag, REPLICA_FRAME_TAG_EVENT);
        assert!(ts_secs >= last_ts, "timestamps must be monotonic per connection");
        last_ts = ts_secs;
        let v: serde_json::Value = serde_json::from_slice(&payload).unwrap();
        keys_seen.push(v["user_id"].as_str().unwrap().to_string());
    }
    assert_eq!(keys_seen, vec!["u1", "u2", "u3"]);

    drop(sub);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn backpressure_drops_subscriber() {
    let (port, state) = start_test_server(&["orders"]).await;

    // Shrink the client-side recv buffer so the kernel-level TCP buffer
    // stalls *before* we have to push 10 MB of events to saturate the
    // 10_000-slot mpsc. This is the standard trick for exercising server
    // backpressure in an integration test without ballooning the event
    // payload size.
    let sock = tokio::net::TcpSocket::new_v4().unwrap();
    sock.set_recv_buffer_size(4096).unwrap();
    let mut sub = sock
        .connect(format!("127.0.0.1:{}", port).parse().unwrap())
        .await
        .unwrap();
    send_subscribe_frame(&mut sub, ADMIN_TOKEN, &scope_streams(&["orders"])).await;
    for _ in 0..50 {
        if state.subscriber_registry.active_count() == 1 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert_eq!(state.subscriber_registry.active_count(), 1);

    // DO NOT read from `sub`. The mpsc will fill once the server's writes
    // block (recv buffer is 4 KB, BufWriter default is 8 KB, frame is ~50
    // bytes → after ~250 in-flight frames the writer stalls and the mpsc
    // starts accumulating). Push an order of magnitude more than the
    // 10_000-slot capacity to force the Full drop.
    for i in 0..60_000u64 {
        push_event_binary(port, "orders", &format!("u{}", i)).await;
        if i % 1000 == 0 && state.subscriber_registry.active_count() == 0 {
            break;
        }
    }
    for _ in 0..200 {
        if state.subscriber_registry.active_count() == 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(
        state.subscriber_registry.active_count(),
        0,
        "subscriber should have been dropped under backpressure"
    );

    let drops = beava::server::replica::subscribers_dropped_snapshot();
    let bp = drops
        .iter()
        .find(|(r, _)| *r == "backpressure")
        .map(|(_, n)| *n)
        .unwrap_or(0);
    assert!(
        bp >= 1,
        "backpressure drop counter should be >= 1, got {}",
        bp
    );

    drop(sub);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn disconnect_cleans_up_registry() {
    let (port, state) = start_test_server(&["orders"]).await;

    let mut sub = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    send_subscribe_frame(&mut sub, ADMIN_TOKEN, &scope_streams(&["orders"])).await;
    for _ in 0..50 {
        if state.subscriber_registry.active_count() == 1 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert_eq!(state.subscriber_registry.active_count(), 1);

    let disc_before = beava::server::replica::subscribers_dropped_snapshot()
        .into_iter()
        .find(|(r, _)| *r == "disconnect")
        .map(|(_, n)| n)
        .unwrap_or(0);

    drop(sub);

    for _ in 0..100 {
        if state.subscriber_registry.active_count() == 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(state.subscriber_registry.active_count(), 0);
    let disc_after = beava::server::replica::subscribers_dropped_snapshot()
        .into_iter()
        .find(|(r, _)| *r == "disconnect")
        .map(|(_, n)| n)
        .unwrap_or(0);
    assert!(
        disc_after > disc_before,
        "disconnect counter must bump: before={} after={}",
        disc_before,
        disc_after
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn subscribe_rejects_missing_auth() {
    let (port, state) = start_test_server(&["orders"]).await;

    let mut sub = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    send_subscribe_frame(&mut sub, "wrong-token", &scope_streams(&["orders"])).await;
    let msg = read_error_frame(&mut sub).await;
    assert!(
        msg.to_lowercase().contains("unauth"),
        "expected unauthorized message, got {}",
        msg
    );

    // Registry untouched — no session should have been inserted.
    assert_eq!(state.subscriber_registry.active_count(), 0);

    // Safety/error signal must have been emitted.
    let signals = state
        .signals
        .read()
        .snapshot_sorted(std::time::SystemTime::now(), Some(Category::Safety));
    assert!(
        signals.iter().any(|s| s.id.starts_with("replica.auth.failure")),
        "expected a replica.auth.failure safety signal"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn subscribe_rejects_empty_streams_scope() {
    let (port, state) = start_test_server(&["orders"]).await;

    let empty_scope = Scope {
        streams: vec![],
        keys: None,
        key_prefix: None,
        pull: "all".into(),
    };
    let mut sub = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    send_subscribe_frame(&mut sub, ADMIN_TOKEN, &empty_scope).await;
    let msg = read_error_frame(&mut sub).await;
    assert!(
        msg.to_lowercase().contains("scope.streams") || msg.to_lowercase().contains("non-empty"),
        "expected scope.streams rejection message, got {}",
        msg
    );
    assert_eq!(state.subscriber_registry.active_count(), 0);
}
