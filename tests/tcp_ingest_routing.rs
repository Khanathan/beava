//! Phase 54-00 Task 3 — TCP ingest routing RED test (drives Wave 1 Task 1/2).
//!
//! Analogous to `http_ingest_routing.rs` but via the binary TCP protocol.
//! At N=1, a TCP `OP_PUSH` frame MUST transit the shard-thread SPSC inbox,
//! incrementing `beava_shard_events_total{shard="0",outcome="accepted"}`.
//!
//! At Phase 53 HEAD this test FAILS because the N=1 branch of
//! `handle_push_core_ex` (src/server/tcp.rs ~line 1730) falls through to the
//! legacy DashMap-backed `engine.push_with_cascade[_no_features]` without
//! calling `record_shard_event`. Wave 1 plan 54-01 flips it GREEN.
//!
//! Test command: `cargo test --release --test tcp_ingest_routing`.

use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use beava::engine::pipeline::{FeatureDef, PipelineEngine, StreamDefinition};
use beava::server::protocol::{write_string, OP_PUSH, TYPE_STR};
use beava::server::tcp::{make_concurrent_state_default_store, BackfillTracker, SharedState};
const TEST_ADMIN: &str = "test-admin-54-00-tcp-routing";

fn build_single_shard_state(tag: &str) -> SharedState {
    let state = make_concurrent_state_default_store(
        PipelineEngine::new(),
        None,
        std::path::PathBuf::from(format!("/tmp/beava-test-54-00-tcp-{tag}.snapshot")),
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
            name: "tcp_stream".into(),
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
        })
        .unwrap();

    let handles = beava::shard::thread::spawn_shard_threads(1, 65_536, state.clone());
    *state.shard_handles.write() = handles;
    beava::server::shard_probe::init_route_counters(1);

    beava::metrics::install_prometheus_recorder();
    beava::shard::metrics::register_shard_metrics(1);

    state
}

/// Parse `metric{label=value,...} N` from a Prometheus scrape. Returns 0 if
/// not found.
fn parse_counter(scrape: &str, metric: &str, labels: &[(&str, &str)]) -> u64 {
    for line in scrape.lines() {
        if !line.starts_with(metric) {
            continue;
        }
        let after = &line[metric.len()..];
        let Some(lbrace) = after.find('{') else {
            continue;
        };
        let Some(rbrace) = after.find('}') else {
            continue;
        };
        let labels_str = &after[lbrace + 1..rbrace];
        let all_present = labels.iter().all(|(k, v)| {
            let needle = format!("{k}=\"{v}\"");
            labels_str.contains(&needle)
        });
        if !all_present {
            continue;
        }
        let value_str = after[rbrace + 1..].trim();
        if let Ok(n) = value_str.parse::<u64>() {
            return n;
        }
        if let Ok(f) = value_str.parse::<f64>() {
            return f as u64;
        }
    }
    0
}

/// Push one event via raw TCP OP_PUSH and return the status byte.
async fn push_one_tcp(addr: std::net::SocketAddr, stream: &str, user_id: &str) -> u8 {
    let mut conn = TcpStream::connect(addr).await.unwrap();

    let mut payload = write_string(stream);
    payload.extend_from_slice(&1u16.to_be_bytes());
    payload.extend_from_slice(&write_string("user_id"));
    payload.push(TYPE_STR);
    payload.extend_from_slice(&write_string(user_id));

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

/// At N=1, a TCP push via `handle_push_batch`/`handle_push_core_ex` MUST
/// transit shard-0's SPSC inbox.
#[tokio::test]
async fn tcp_push_at_n1_routes_through_spsc() {
    let state = build_single_shard_state("tcp_push");

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let srv_state = state.clone();
    tokio::spawn(async move {
        let _ = beava::server::tcp::run_tcp_server_with_listener(listener, srv_state).await;
    });
    tokio::time::sleep(Duration::from_millis(30)).await;

    let before = beava::metrics::handle()
        .map(|h| {
            parse_counter(
                &h.scrape(),
                "beava_shard_events_total",
                &[("shard", "0"), ("outcome", "accepted")],
            )
        })
        .unwrap_or(0);

    let status = push_one_tcp(addr, "tcp_stream", "u1").await;
    assert_eq!(status, 0x00, "TCP OP_PUSH must return STATUS_OK");

    tokio::time::sleep(Duration::from_millis(200)).await;

    let after = beava::metrics::handle()
        .map(|h| {
            parse_counter(
                &h.scrape(),
                "beava_shard_events_total",
                &[("shard", "0"), ("outcome", "accepted")],
            )
        })
        .unwrap_or(0);

    // Assertion 1 — weak: counter must increment. Phase 53 HEAD passes this
    // trivially because the N=1 legacy branch ALSO calls record_shard_event.
    assert!(
        after > before,
        "TPC-ARCH-01 (TCP routing, weak check): \
         `beava_shard_events_total{{shard=\"0\",outcome=\"accepted\"}}` did not \
         increment (before={before}, after={after})."
    );

    // Assertion 2 — strong SPSC-transit proof: legacy DashMap must be EMPTY
    // for the pushed key. At Phase 53 HEAD this FAILS — the legacy engine
    // path writes 'u1' into state.store. After Wave 1 plan 54-01 Task 2, N=1
    // routes through the shard SPSC and state.store stays empty.
    let legacy_entity = state.store.get_entity("u1");
    assert!(
        legacy_entity.is_none(),
        "TPC-ARCH-01 (TCP routing, strong SPSC-transit check): legacy DashMap \
         `state.store` contains entity 'u1' after a TCP push at N=1. The N=1 \
         branch of handle_push_core_ex is still the legacy hot path. Plan \
         54-01 Task 2 must rewire the TCP handle_push_batch entry through the \
         shard SPSC inbox so state.store stays empty — proving the event \
         transited the shard thread."
    );
}
