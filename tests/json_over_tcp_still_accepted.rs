//! Phase 59-00 Wave 0 — D-B3 backward-compat contract (flips GREEN at Wave 1).
//!
//! ## Plan deviation (Rule 1 — bug in plan premise, auto-documented)
//!
//! 59-00-PLAN.md Task 4 asserts this test "already passes" on current HEAD
//! because "`parse_command`'s JSON-fallback path has always handled" a
//! JSON body after a 0x7b discriminator. Empirically on this HEAD it does
//! NOT — `src/server/protocol.rs::parse_command` OP_PUSH branch calls
//! `decode_event_binary(&mut buf)?` with `?` propagation and has no JSON
//! fallback. Sending `{"amount":100}` on OP_PUSH yields STATUS_ERROR.
//! See 59-CONTEXT.md D-B2 which *describes* the intended fallback but the
//! wire-level implementation is not on disk today.
//!
//! Phase 59 Wave 1 must (a) preserve the binary-body path (that's the
//! point of the phase — Bytes passthrough) AND (b) add the JSON-body
//! fallback `parse_command` so D-B3 becomes reality. This is net-additive
//! to Wave 1's scope because Wave 1 touches exactly `parse_command`'s
//! OP_PUSH branch. Once landed, this test flips GREEN.
//!
//! Marked `#[ignore = "59-W1"]` and re-labeled "Wave 1 must establish the
//! D-B3 fallback before it becomes a regression guard." For every wave
//! ≥ 1 this test MUST stay GREEN — if it fails on Wave 2/3/4, roll back.
//!
//! Test command: `cargo test --release --test json_over_tcp_still_accepted -- --ignored`.

use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use beava::engine::pipeline::{PipelineEngine, StreamDefinition};
use beava::server::protocol::{write_string, OP_PUSH};
use beava::server::tcp::{make_concurrent_state_full, BackfillTracker, SharedState};

const TEST_ADMIN: &str = "test-admin-59-00-json-over-tcp";

fn build_single_shard_state(tag: &str) -> SharedState {
    let state = make_concurrent_state_full(
        PipelineEngine::new(),
        None,
        std::path::PathBuf::from(format!("/tmp/beava-test-59-00-json-tcp-{tag}.snapshot")),
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
            key_field: Some("user_id".into()),
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

/// Push one event via raw TCP OP_PUSH with a JSON-shaped body (legacy wire
/// shape). Returns the STATUS byte from the server response.
async fn push_one_json_over_tcp(addr: std::net::SocketAddr, stream: &str, json_body: &[u8]) -> u8 {
    let mut conn = TcpStream::connect(addr).await.unwrap();

    // Payload: [u16 BE stream_len][stream_bytes][raw JSON bytes]
    let mut payload = write_string(stream);
    payload.extend_from_slice(json_body);

    let total_len = (1 + payload.len()) as u32;
    conn.write_u32(total_len).await.unwrap();
    conn.write_u8(OP_PUSH).await.unwrap();
    conn.write_all(&payload).await.unwrap();
    conn.flush().await.unwrap();

    let resp_len = conn.read_u32().await.unwrap() as usize;
    let status = conn.read_u8().await.unwrap();
    let mut body = vec![0u8; resp_len - 1];
    if !body.is_empty() {
        conn.read_exact(&mut body).await.unwrap();
    }
    status
}

/// TPC-PERF-09 D-B3: JSON-over-TCP OP_PUSH MUST be accepted on every
/// Phase-59 wave ≥ 1. Wave 1 adds the `parse_command` JSON fallback as
/// part of its Bytes-passthrough reshape (see header docstring — plan
/// premise that this "already passes" was factually wrong on HEAD).
#[tokio::test]
async fn json_over_tcp_op_push_accepted_after_phase_59() {
    let state = build_single_shard_state("accepted");

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let srv_state = state.clone();
    tokio::spawn(async move {
        let _ = beava::server::tcp::run_tcp_server_with_listener(listener, srv_state).await;
    });
    tokio::time::sleep(Duration::from_millis(30)).await;

    // Send a JSON body (not binary). Server's parse_command tries
    // decode_event_binary first — `0x7b` (`{`) is not a valid field_count
    // header — and the fallback JSON parse succeeds.
    let json_body = br#"{"user_id":"u1","amount":100}"#;
    let before = state
        .events_total
        .load(std::sync::atomic::Ordering::Relaxed);

    let status = push_one_json_over_tcp(addr, "Txns", json_body).await;

    assert_eq!(
        status, 0x00,
        "TPC-PERF-09 D-B3: JSON-over-TCP OP_PUSH MUST return STATUS_OK \
         (backward-compat window ≥ 1 release cycle). \
         Got status=0x{:02x}.",
        status
    );

    // Allow shard thread to drain the SPSC inbox.
    tokio::time::sleep(Duration::from_millis(200)).await;

    let after = state
        .events_total
        .load(std::sync::atomic::Ordering::Relaxed);

    assert!(
        after > before,
        "TPC-PERF-09 D-B3: JSON-over-TCP push did not advance events_total \
         (before={before}, after={after}). If this test fails on a Wave 1+ \
         commit, the D-B3 backward-compat contract is broken — roll back \
         and re-plan."
    );
}
