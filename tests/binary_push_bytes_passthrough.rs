//! Phase 59-00 Wave 0 — TPC-PERF-09 D-D1 RED contract (flips GREEN at Wave 1).
//!
//! This test plants the RED scaffolding for Phase 59's central claim: the
//! TCP PUSH hot path re-serializes `serde_json::Value` → JSON bytes before
//! handing the event to the shard thread, then re-parses the JSON back
//! to a Value on the shard side — one unnecessary round-trip per event.
//!
//! ## WASTE locations (per 59-CONTEXT.md `<code_context>` §Current WASTE)
//!
//! | File                        | Line | Pattern                                        |
//! |-----------------------------|------|------------------------------------------------|
//! | `src/server/tcp.rs`         | 2159 | `serde_json::to_vec(payload).unwrap_or_default()` |
//! | `src/server/tcp.rs`         | 2538 | `serde_json::to_vec(r.payload).unwrap_or_default()` |
//! | `src/shard/thread.rs`       | ~724 | `serde_json::from_slice(&event.payload)`           |
//!
//! Wave 1 deletes the `to_vec(payload)` calls + reshapes the shard thread
//! to call `decode_event_binary` directly on `PayloadFmt::Binary`-tagged
//! `ShardEvent.payload`. After Wave 1 the grep below returns 0 hits; this
//! test flips GREEN.
//!
//! ## Pragmatic RED: source-level grep
//!
//! The plan discusses several ways to observe the WASTE (panic hook,
//! thread_local counter, cfg-gated probe). Wave 0 takes the simplest
//! signal: read the source file at `CARGO_MANIFEST_DIR/src/server/tcp.rs`
//! and assert that at least one `serde_json::to_vec(payload)` or
//! `serde_json::to_vec(r.payload)` pattern exists. This matches the
//! `scripts/verify-no-tcp-json-reserialize.sh` contract and flips
//! GREEN in the same Wave-1 commit that deletes the WASTE call sites.
//!
//! Marked `#[ignore = "59-W1"]` per D-D1 — this is a Wave-1 flip target.
//!
//! Test command: `cargo test --release --test binary_push_bytes_passthrough -- --ignored`.

use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use beava::engine::pipeline::{PipelineEngine, StreamDefinition};
use beava::server::protocol::{write_string, OP_PUSH, TYPE_I64};
use beava::server::tcp::{make_concurrent_state_full, BackfillTracker, SharedState};

const TEST_ADMIN: &str = "test-admin-59-00-binary-passthrough";

fn build_single_shard_state(tag: &str) -> SharedState {
    let state = make_concurrent_state_full(
        PipelineEngine::new(),
        None,
        std::path::PathBuf::from(format!("/tmp/beava-test-59-00-bin-passthrough-{tag}.snapshot")),
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
            salt: None,
        })
        .unwrap();

    let handles = beava::shard::thread::spawn_shard_threads(1, 65_536, state.clone(), None);
    *state.shard_handles.write() = handles;
    beava::server::shard_probe::init_route_counters(1);
    beava::metrics::install_prometheus_recorder();
    beava::shard::metrics::register_shard_metrics(1);
    state
}

/// Push one binary-encoded OP_PUSH event. Uses the existing
/// `decode_event_binary` shape: `[u16 BE field_count]` + per-field
/// `[u16 BE key_len][key][u8 type_tag][value]`.
async fn push_one_binary(addr: std::net::SocketAddr, stream: &str, amount: i64) -> u8 {
    let mut conn = TcpStream::connect(addr).await.unwrap();

    let mut payload = write_string(stream);
    // 1 field: "amount" = i64
    payload.extend_from_slice(&1u16.to_be_bytes());
    payload.extend_from_slice(&write_string("amount"));
    payload.push(TYPE_I64);
    payload.extend_from_slice(&amount.to_be_bytes());

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

/// TPC-PERF-09 D-D1 RED: on current HEAD (pre-Wave-1), `src/server/tcp.rs`
/// contains at least one `serde_json::to_vec(payload)` or
/// `serde_json::to_vec(r.payload)` call — the WASTE Phase 59 eliminates.
/// Wave 1 deletes those call sites; this test flips GREEN.
///
/// The push itself is additionally asserted: STATUS_OK + `events_total`
/// advances by 1. That half stays GREEN on both sides of the flip (Wave 1
/// replaces WASTE with Bytes-passthrough, but the OP_PUSH contract is
/// preserved).
#[tokio::test]
async fn binary_op_push_flows_without_json_reserialize() {
    let state = build_single_shard_state("flow");

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let srv_state = state.clone();
    tokio::spawn(async move {
        let _ = beava::server::tcp::run_tcp_server_with_listener(listener, srv_state).await;
    });
    tokio::time::sleep(Duration::from_millis(30)).await;

    let before = state
        .events_total
        .load(std::sync::atomic::Ordering::Relaxed);

    let status = push_one_binary(addr, "Txns", 100).await;
    assert_eq!(
        status, 0x00,
        "TPC-PERF-09: binary OP_PUSH must return STATUS_OK (got 0x{:02x}).",
        status
    );

    tokio::time::sleep(Duration::from_millis(200)).await;

    let after = state
        .events_total
        .load(std::sync::atomic::Ordering::Relaxed);

    assert!(
        after > before,
        "TPC-PERF-09: binary OP_PUSH did not advance events_total \
         (before={before}, after={after})."
    );

    // ----- Pragmatic RED: source-level grep for the WASTE call sites -----
    // On Wave-0 HEAD: `src/server/tcp.rs` contains at least one
    // `serde_json::to_vec(payload)` or `serde_json::to_vec(r.payload)`
    // call. Wave 1 deletes these and this test flips GREEN. See
    // scripts/verify-no-tcp-json-reserialize.sh for the operator-side
    // mirror of this invariant.
    let tcp_rs = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("src")
        .join("server")
        .join("tcp.rs");
    let src = std::fs::read_to_string(&tcp_rs)
        .unwrap_or_else(|e| panic!("could not read {}: {e}", tcp_rs.display()));

    // Strip single-line comments so documentation referencing the pattern
    // (e.g. header doc-comments) does not count as WASTE.
    let non_comment_hits: usize = src
        .lines()
        .filter(|l| !l.trim_start().starts_with("//") && !l.trim_start().starts_with('*'))
        .filter(|l| {
            l.contains("serde_json::to_vec(payload)")
                || l.contains("serde_json::to_vec(r.payload)")
        })
        .count();

    // Wave 1 flipped: the WASTE call sites are gone. If this assertion
    // fires on a Wave ≥ 1 commit the D-C3 grep-ZERO invariant was broken —
    // roll back and re-run `scripts/verify-no-tcp-json-reserialize.sh`
    // for the operator-side mirror.
    assert_eq!(
        non_comment_hits, 0,
        "TPC-PERF-09 D-D1 GREEN contract FAILED: src/server/tcp.rs \
         has {} `serde_json::to_vec(payload|r.payload)` hit(s) (expected \
         0 after Wave 1). Either the passthrough rewire got reverted or \
         a new WASTE site landed — see \
         `scripts/verify-no-tcp-json-reserialize.sh`.",
        non_comment_hits
    );
}
