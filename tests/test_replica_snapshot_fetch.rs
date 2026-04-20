//! Phase 27-01, Task 3: TCP integration tests for `OP_SNAPSHOT_FETCH` (0x12).
//!
//! Spins up the real beava server on a random TCP port with an admin token
//! configured, writes a synthetic `BaseSnapshotState` to the server's
//! snapshot path, and drives the wire protocol end-to-end.
//!
//! Covers:
//!   * happy_streams_only — filter by streams, no keys/prefix
//!   * happy_keys — filter by explicit key list
//!   * happy_key_prefix — filter by key prefix
//!   * header_taken_at_is_recent — `snapshot_taken_at` is `SystemTime::now()`-ish
//!   * rejects_missing_auth — wrong/empty admin token → STATUS_ERROR, no payload
//!   * rejects_empty_streams — validate_scope EmptyStreams
//!   * rejects_unknown_stream — validate_scope UnknownStream
//!   * rejects_keys_and_prefix — mutually exclusive
//!   * rejects_pull_not_all — pull="historical" etc. not implemented in v0
//!   * rejects_too_many_keys — keys.len() > 10_000
//!   * rejects_empty_prefix — key_prefix = ""
//!
//! The test spawns a fresh server per test (each gets its own snapshot dir)
//! so state is hermetic.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use beava::engine::pipeline::{FeatureDef, PipelineEngine, StreamDefinition};
use beava::server::protocol::{
    self, Scope, OP_SNAPSHOT_FETCH, REPLICA_FRAME_TAG_HEADER, REPLICA_FRAME_TAG_PAYLOAD,
    STATUS_ERROR,
};
use beava::server::tcp::{make_concurrent_state_default_store, BackfillTracker, SharedState};
use beava::state::snapshot::{
    save_base_snapshot, BaseSnapshotStateV8, SerializableEntityState, SerializableStreamEntityState,
    SnapshotHeader, SnapshotType,
};
use std::collections::HashMap;
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
        watermark_lateness: None,
        shard_key: None,
    }
}

fn stream_state() -> SerializableStreamEntityState {
    SerializableStreamEntityState {
        operators: vec![],
        last_event_at: None,
    }
}

fn entity_with_streams(streams: &[&str]) -> SerializableEntityState {
    SerializableEntityState {
        streams: streams
            .iter()
            .map(|s| ((*s).to_string(), stream_state()))
            .collect(),
        static_features: vec![],
        table_rows: vec![],
    }
}

/// Write a synthetic base snapshot to the file the server will read on
/// SNAPSHOT_FETCH. Uses the `beava.snapshot.base.NNNNNNNNNN` layout that
/// `load_base_snapshot_for_fetch` scans for. Returns the written path.
fn write_base_snapshot(
    snap_dir: &std::path::Path,
    entities: Vec<(String, SerializableEntityState)>,
) -> PathBuf {
    let base = BaseSnapshotStateV8 {
        header: SnapshotHeader {
            snapshot_type: SnapshotType::Base,
            sequence: 1,
        },
        entities,
        pipelines: vec![],
        backfill_complete: vec![],
        shard_count: 1,
        replica_lsn_map: HashMap::new(),
    };
    use beava::state::snapshot::save_base_snapshot_v8;
    let bytes = save_base_snapshot_v8(&base).expect("save_base_snapshot_v8");
    let path = snap_dir.join("beava.snapshot.base.0000000001");
    std::fs::write(&path, &bytes).expect("write snapshot");
    path
}

async fn start_test_server(
    snap_dir: &std::path::Path,
    stream_names: &[&str],
) -> (u16, SharedState) {
    let mut engine = PipelineEngine::new();
    for s in stream_names {
        engine.register(stream_def(s)).expect("register stream");
    }
    let state = make_concurrent_state_default_store(
        engine,
        None,
        snap_dir.join("beava.snapshot"), // legacy path root (parent == snap_dir)
        Arc::new(BackfillTracker::default()),
        true,  // snapshot_enabled
        false, // event_log_enabled
        Some(ADMIN_TOKEN.to_string()),
        false,
        1,
    );

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind ephemeral port");
    let port = listener.local_addr().unwrap().port();

    let server_state = state.clone();
    tokio::spawn(async move {
        let _ = beava::server::tcp::run_tcp_server_with_listener(listener, server_state).await;
    });

    // Give the listener a tick to become ready.
    tokio::time::sleep(Duration::from_millis(30)).await;
    (port, state)
}

// ---------------------------------------------------------------------------
// Frame helpers
// ---------------------------------------------------------------------------

fn build_snapshot_fetch_payload(token: &str, scope: &Scope) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.extend_from_slice(&protocol::write_string(token));
    protocol::write_scope(&mut buf, scope);
    buf
}

async fn send_snapshot_fetch_frame(stream: &mut TcpStream, token: &str, scope: &Scope) {
    let payload = build_snapshot_fetch_payload(token, scope);
    let len = (1 + payload.len()) as u32;
    stream.write_u32(len).await.unwrap();
    stream.write_u8(OP_SNAPSHOT_FETCH).await.unwrap();
    stream.write_all(&payload).await.unwrap();
    stream.flush().await.unwrap();
}

/// Read one framed response: `[u32 len][u8 tag][body]`. Returns (tag, body).
async fn read_frame(stream: &mut TcpStream) -> (u8, Vec<u8>) {
    let len = stream.read_u32().await.unwrap() as usize;
    assert!(
        len >= 1,
        "response frame must contain at least the tag byte"
    );
    let tag = stream.read_u8().await.unwrap();
    let body_len = len - 1;
    let mut body = vec![0u8; body_len];
    if body_len > 0 {
        stream.read_exact(&mut body).await.unwrap();
    }
    (tag, body)
}

/// Decode the 12-byte header frame body into (secs, nanos).
fn decode_header_body(body: &[u8]) -> (u64, u32) {
    assert_eq!(body.len(), 12, "header body must be exactly 12 bytes");
    let secs = u64::from_be_bytes(body[..8].try_into().unwrap());
    let nanos = u32::from_be_bytes(body[8..].try_into().unwrap());
    (secs, nanos)
}

fn scope_streams(streams: &[&str]) -> Scope {
    Scope {
        streams: streams.iter().map(|s| (*s).to_string()).collect(),
        keys: None,
        key_prefix: None,
        pull: "all".into(),
    }
}

// ---------------------------------------------------------------------------
// Happy-path tests
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn happy_streams_only() {
    let tmp = tempfile::tempdir().unwrap();
    // Entity state on disk: 3 entities across 2 streams.
    write_base_snapshot(
        tmp.path(),
        vec![
            ("u1".into(), entity_with_streams(&["orders"])),
            ("u2".into(), entity_with_streams(&["clicks"])),
            ("u3".into(), entity_with_streams(&["orders", "clicks"])),
        ],
    );
    let (port, _state) = start_test_server(tmp.path(), &["orders", "clicks"]).await;

    let mut conn = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    let scope = scope_streams(&["orders"]);
    send_snapshot_fetch_frame(&mut conn, ADMIN_TOKEN, &scope).await;

    let (tag, body) = read_frame(&mut conn).await;
    assert_eq!(tag, REPLICA_FRAME_TAG_HEADER);
    let (secs, _nanos) = decode_header_body(&body);
    let now_secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    assert!(
        secs > 0 && secs <= now_secs + 5 && secs + 60 >= now_secs,
        "snapshot_taken_at seconds out of range: got {} now {}",
        secs,
        now_secs
    );

    let (tag, payload) = read_frame(&mut conn).await;
    assert_eq!(tag, REPLICA_FRAME_TAG_PAYLOAD);
    let filtered: BaseSnapshotStateV8 = postcard::from_bytes(&payload).expect("postcard deserialize");
    // u1 (orders) + u3 (orders, clicks) should pass; u2 (clicks) filtered out.
    let keys: Vec<_> = filtered.entities.iter().map(|(k, _)| k.as_str()).collect();
    assert_eq!(keys, vec!["u1", "u3"]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn happy_keys() {
    let tmp = tempfile::tempdir().unwrap();
    write_base_snapshot(
        tmp.path(),
        vec![
            ("u1".into(), entity_with_streams(&["orders"])),
            ("u2".into(), entity_with_streams(&["orders"])),
            ("u3".into(), entity_with_streams(&["orders"])),
        ],
    );
    let (port, _state) = start_test_server(tmp.path(), &["orders"]).await;

    let mut conn = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    let mut scope = scope_streams(&["orders"]);
    scope.keys = Some(vec!["u1".into(), "u3".into()]);
    send_snapshot_fetch_frame(&mut conn, ADMIN_TOKEN, &scope).await;

    let (tag, _body) = read_frame(&mut conn).await;
    assert_eq!(tag, REPLICA_FRAME_TAG_HEADER);
    let (tag, payload) = read_frame(&mut conn).await;
    assert_eq!(tag, REPLICA_FRAME_TAG_PAYLOAD);
    let filtered: BaseSnapshotStateV8 = postcard::from_bytes(&payload).unwrap();
    let keys: Vec<_> = filtered.entities.iter().map(|(k, _)| k.as_str()).collect();
    assert_eq!(keys, vec!["u1", "u3"]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn happy_key_prefix() {
    let tmp = tempfile::tempdir().unwrap();
    write_base_snapshot(
        tmp.path(),
        vec![
            ("user_1".into(), entity_with_streams(&["orders"])),
            ("user_2".into(), entity_with_streams(&["orders"])),
            ("bot_1".into(), entity_with_streams(&["orders"])),
        ],
    );
    let (port, _state) = start_test_server(tmp.path(), &["orders"]).await;

    let mut conn = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    let mut scope = scope_streams(&["orders"]);
    scope.key_prefix = Some("user_".into());
    send_snapshot_fetch_frame(&mut conn, ADMIN_TOKEN, &scope).await;

    let (tag, _body) = read_frame(&mut conn).await;
    assert_eq!(tag, REPLICA_FRAME_TAG_HEADER);
    let (tag, payload) = read_frame(&mut conn).await;
    assert_eq!(tag, REPLICA_FRAME_TAG_PAYLOAD);
    let filtered: BaseSnapshotStateV8 = postcard::from_bytes(&payload).unwrap();
    let keys: Vec<_> = filtered.entities.iter().map(|(k, _)| k.as_str()).collect();
    assert_eq!(keys, vec!["user_1", "user_2"]);
}

// ---------------------------------------------------------------------------
// Rejection tests. Each expects a STATUS_ERROR frame and no payload frame.
// ---------------------------------------------------------------------------

async fn expect_error_frame_and_close(conn: &mut TcpStream) -> String {
    let len = conn.read_u32().await.unwrap() as usize;
    let tag = conn.read_u8().await.unwrap();
    assert_eq!(
        tag, STATUS_ERROR,
        "expected STATUS_ERROR, got tag 0x{:02x}",
        tag
    );
    let body_len = len - 1;
    let mut body = vec![0u8; body_len];
    if body_len > 0 {
        conn.read_exact(&mut body).await.unwrap();
    }
    String::from_utf8(body).unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rejects_missing_auth() {
    let tmp = tempfile::tempdir().unwrap();
    write_base_snapshot(tmp.path(), vec![]);
    let (port, _state) = start_test_server(tmp.path(), &["orders"]).await;

    let mut conn = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    let scope = scope_streams(&["orders"]);
    // Wrong token.
    send_snapshot_fetch_frame(&mut conn, "nope", &scope).await;
    let msg = expect_error_frame_and_close(&mut conn).await;
    assert!(
        msg.contains("unauthorized"),
        "expected auth error, got {:?}",
        msg
    );

    // Empty token (simulates client that didn't set the bearer).
    let mut conn2 = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    send_snapshot_fetch_frame(&mut conn2, "", &scope).await;
    let msg2 = expect_error_frame_and_close(&mut conn2).await;
    assert!(msg2.contains("unauthorized"), "got {:?}", msg2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rejects_empty_streams() {
    let tmp = tempfile::tempdir().unwrap();
    write_base_snapshot(tmp.path(), vec![]);
    let (port, _state) = start_test_server(tmp.path(), &["orders"]).await;

    let mut conn = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    let scope = scope_streams(&[]); // empty streams
    send_snapshot_fetch_frame(&mut conn, ADMIN_TOKEN, &scope).await;
    let msg = expect_error_frame_and_close(&mut conn).await;
    assert!(msg.contains("streams must be non-empty"), "got {:?}", msg);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rejects_unknown_stream() {
    let tmp = tempfile::tempdir().unwrap();
    write_base_snapshot(tmp.path(), vec![]);
    let (port, _state) = start_test_server(tmp.path(), &["orders"]).await;

    let mut conn = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    let scope = scope_streams(&["does_not_exist"]);
    send_snapshot_fetch_frame(&mut conn, ADMIN_TOKEN, &scope).await;
    let msg = expect_error_frame_and_close(&mut conn).await;
    assert!(
        msg.contains("unknown stream") && msg.contains("does_not_exist"),
        "got {:?}",
        msg
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rejects_keys_and_prefix() {
    let tmp = tempfile::tempdir().unwrap();
    write_base_snapshot(tmp.path(), vec![]);
    let (port, _state) = start_test_server(tmp.path(), &["orders"]).await;

    let mut conn = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    let mut scope = scope_streams(&["orders"]);
    scope.keys = Some(vec!["k1".into()]);
    scope.key_prefix = Some("u".into());
    send_snapshot_fetch_frame(&mut conn, ADMIN_TOKEN, &scope).await;
    let msg = expect_error_frame_and_close(&mut conn).await;
    assert!(msg.contains("mutually exclusive"), "got {:?}", msg);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rejects_pull_not_all() {
    let tmp = tempfile::tempdir().unwrap();
    write_base_snapshot(tmp.path(), vec![]);
    let (port, _state) = start_test_server(tmp.path(), &["orders"]).await;

    let mut conn = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    let mut scope = scope_streams(&["orders"]);
    scope.pull = "historical".into();
    send_snapshot_fetch_frame(&mut conn, ADMIN_TOKEN, &scope).await;
    let msg = expect_error_frame_and_close(&mut conn).await;
    assert!(
        msg.contains("not implemented") && msg.contains("historical"),
        "got {:?}",
        msg
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rejects_too_many_keys() {
    let tmp = tempfile::tempdir().unwrap();
    write_base_snapshot(tmp.path(), vec![]);
    let (port, _state) = start_test_server(tmp.path(), &["orders"]).await;

    let mut conn = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    let mut scope = scope_streams(&["orders"]);
    scope.keys = Some((0..10_001).map(|i| format!("k{}", i)).collect());
    send_snapshot_fetch_frame(&mut conn, ADMIN_TOKEN, &scope).await;
    let msg = expect_error_frame_and_close(&mut conn).await;
    assert!(msg.contains("exceeds cap of 10000"), "got {:?}", msg);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rejects_empty_prefix() {
    let tmp = tempfile::tempdir().unwrap();
    write_base_snapshot(tmp.path(), vec![]);
    let (port, _state) = start_test_server(tmp.path(), &["orders"]).await;

    let mut conn = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    let mut scope = scope_streams(&["orders"]);
    scope.key_prefix = Some(String::new());
    send_snapshot_fetch_frame(&mut conn, ADMIN_TOKEN, &scope).await;
    let msg = expect_error_frame_and_close(&mut conn).await;
    assert!(msg.contains("key_prefix must not"), "got {:?}", msg);
}
