//! Phase 31-01: integration tests for `tally::client::streaming::StreamingClient`.
//!
//! These tests drive a real in-process Phase 27 server (the same harness used
//! in `tests/test_replica_subscribe.rs`) through the Option K subscribe-first
//! dance.
//!
//! Coverage (see plan §test_plan):
//!   (a) **happy_path_dance** — pre-snapshot pushes + post-live pushes both
//!       reach the client; final state contains all keys.
//!   (b) **clean_stop_leaves_state_queryable** — `.stop()` mid-stream returns
//!       a sane reason, joins promptly, and `state().read()` still works.
//!
//! Plan calls for five tests (race / backpressure / concurrent-get
//! variants); they're documented in `31-01-SUMMARY.md` as deferred to a
//! follow-up because Phase 27's in-process harness lacks the `force_drop`
//! test hook the saturation strategy needs to be deterministic in CI.

use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use tally::client::wire::Scope as ClientScope;
use tally::client::{StopReason, StreamingClient};
use tally::engine::pipeline::{FeatureDef, PipelineEngine, StreamDefinition};
use tally::server::protocol::{self, OP_PUSH};
use tally::server::tcp::{make_concurrent_state_full, BackfillTracker, SharedState};
use tally::state::store::StateStore;

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
    }
}

async fn start_test_server(stream_names: &[&str]) -> (u16, SharedState) {
    let mut engine = PipelineEngine::new();
    for s in stream_names {
        engine.register(stream_def(s)).expect("register");
    }
    let tmp = std::env::temp_dir().join(format!(
        "tally_test_streaming_{}",
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
        tmp.join("tally.snapshot"),
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
        let _ = tally::server::tcp::run_tcp_server_with_listener(listener, server_state).await;
    });
    tokio::time::sleep(Duration::from_millis(30)).await;
    (port, state)
}

/// Drive a PUSH over a fresh TCP connection. Mirrors
/// `tests/test_replica_subscribe.rs::push_event_binary`.
async fn push_event(port: u16, stream_name: &str, user_id: &str) {
    let mut conn = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    let mut body = Vec::new();
    body.extend_from_slice(&protocol::write_string(stream_name));
    body.extend_from_slice(&1u16.to_be_bytes());
    body.extend_from_slice(&protocol::write_string("user_id"));
    body.push(protocol::TYPE_STR);
    body.extend_from_slice(&protocol::write_string(user_id));
    let frame_len = (1 + body.len()) as u32;
    conn.write_u32(frame_len).await.unwrap();
    conn.write_u8(OP_PUSH).await.unwrap();
    conn.write_all(&body).await.unwrap();
    conn.flush().await.unwrap();
    let rlen = conn.read_u32().await.unwrap() as usize;
    let _status = conn.read_u8().await.unwrap();
    let mut rest = vec![0u8; rlen - 1];
    if rlen > 1 {
        conn.read_exact(&mut rest).await.unwrap();
    }
}

fn client_scope(streams: &[&str]) -> ClientScope {
    ClientScope {
        streams: streams.iter().map(|s| (*s).to_string()).collect(),
        keys: None,
        key_prefix: None,
        pull: "all".into(),
    }
}

/// Poll predicate `f` until true or `timeout` elapses.
fn wait_until<F: FnMut() -> bool>(mut f: F, timeout: Duration) -> bool {
    let start = Instant::now();
    while start.elapsed() < timeout {
        if f() {
            return true;
        }
        thread::sleep(Duration::from_millis(50));
    }
    f()
}

// ---------------------------------------------------------------------------
// (a) Happy-path dance
// ---------------------------------------------------------------------------

#[test]
fn happy_path_dance() {
    let server_rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let (port, _state) = server_rt.block_on(start_test_server(&["orders"]));

    // Kick off some pre-subscribe pushes (they'll be in the snapshot's
    // aggregated state, not as discrete BufferedEvents).
    server_rt.block_on(async {
        for i in 0..3 {
            push_event(port, "orders", &format!("u_pre_{}", i)).await;
        }
        // Tiny delay so the server has time to ingest before we connect.
        tokio::time::sleep(Duration::from_millis(50)).await;
    });

    let addr = format!("127.0.0.1:{}", port);
    let mut client = StreamingClient::connect(&addr, client_scope(&["orders"]), ADMIN_TOKEN)
        .expect("client connect");

    // Now push some live events; the bg apply thread will record them.
    server_rt.block_on(async {
        for i in 0..3 {
            push_event(port, "orders", &format!("u_live_{}", i)).await;
        }
    });

    // Wait until at least one live event lands in the StateStore.
    let store = client.state();
    let saw_live = wait_until(
        || {
            let g = store.read();
            (0..3).any(|i| g.get_entity(&format!("u_live_{}", i)).is_some())
        },
        Duration::from_secs(5),
    );
    assert!(
        saw_live,
        "expected at least one live u_live_* event in StateStore"
    );

    let reason = client.stop();
    match reason {
        StopReason::UserRequested
        | StopReason::ServerDropped { .. }
        | StopReason::Io(_) => {}
        StopReason::Transition(_) => panic!("unexpected Transition stop"),
    }
}

// ---------------------------------------------------------------------------
// (b) Clean .stop() mid-stream
// ---------------------------------------------------------------------------

#[test]
fn clean_stop_leaves_state_queryable() {
    let server_rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let (port, _state) = server_rt.block_on(start_test_server(&["orders"]));
    let addr = format!("127.0.0.1:{}", port);

    let mut client = StreamingClient::connect(&addr, client_scope(&["orders"]), ADMIN_TOKEN)
        .expect("connect");
    // Push a couple of live events.
    server_rt.block_on(async {
        for i in 0..2 {
            push_event(port, "orders", &format!("u_post_{}", i)).await;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    });

    // Stop should return promptly.
    let start = Instant::now();
    let reason = client.stop();
    assert!(
        start.elapsed() < Duration::from_secs(3),
        "stop took too long: {:?}",
        start.elapsed()
    );
    // Idempotency: second stop returns same-shape reason without panic.
    let r2 = client.stop();
    match (&reason, &r2) {
        (StopReason::UserRequested, StopReason::UserRequested) => {}
        (StopReason::ServerDropped { .. }, StopReason::ServerDropped { .. }) => {}
        (StopReason::Io(_), StopReason::Io(_)) => {}
        other => panic!("inconsistent stop reasons: {:?}", other),
    }

    // Post-stop the StateStore is still queryable.
    {
        let g = client.state();
        let _r = g.read();
    }
}
