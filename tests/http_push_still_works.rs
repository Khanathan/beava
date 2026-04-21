//! Phase 58 Wave 0 — HTTP PUSH regression guard (TPC-PERF-08 D-B3).
//!
//! **No `#[ignore]` marker** — this test runs on every `cargo test`. Its
//! purpose is to catch a well-meaning Wave-1/2/3 touch to `src/server/http.rs`
//! that accidentally breaks the axum ingest path. Phase 58's scope is the
//! TCP PUSH runtime; HTTP stays on tokio+axum (D-B3). Phase 59 handles
//! wire-format changes for TCP — also without disturbing HTTP.
//!
//! Invocation:
//!   cargo test --release --test http_push_still_works
//!
//! Boots an axum router at `BEAVA_SHARDS=4`, posts a batch of events to
//! `POST /push/{stream}`, and asserts:
//!   1. Every HTTP response is 200 OK.
//!   2. `beava_shard_events_total{outcome="accepted"}` (summed across shards)
//!      increments by ≥ N_EVENTS — proves the events actually transited the
//!      shard SPSC, i.e. HTTP ingest is end-to-end live.
//!
//! Shape mirrors `tests/http_ingest_routing.rs` with N=4 shards + a batch of
//! events (not a single push) so an accidentally-regressed routing path
//! surfaces reliably.

use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use tower::ServiceExt;

use beava::engine::pipeline::{FeatureDef, PipelineEngine, StreamDefinition};
use beava::server::http::build_router;
use beava::server::tcp::{make_concurrent_state_full, BackfillTracker, SharedState};

const TEST_ADMIN: &str = "test-admin-58-00-http-regression";
const N_SHARDS: u16 = 4;
const N_EVENTS: usize = 64;

/// Build an N=4-shard SharedState, register a single `Transactions` stream
/// keyed on `user_id`, and spawn the shard threads so the SPSC→shard→fjall
/// path is live.
fn build_four_shard_state(tag: &str) -> SharedState {
    // Ephemeral BEAVA_DATA_DIR so parallel test runs don't clobber fjall.
    let tmp_data = tempfile::tempdir().unwrap();
    std::env::set_var("BEAVA_DATA_DIR", tmp_data.path());
    Box::leak(Box::new(tmp_data));

    let state = make_concurrent_state_full(
        PipelineEngine::new(),
        None,
        std::path::PathBuf::from(format!("/tmp/beava-test-58-00-http-{tag}.snapshot")),
        Arc::new(BackfillTracker::default()),
        false,
        false,
        Some(TEST_ADMIN.to_string()),
        false,
        N_SHARDS,
    );

    state
        .engine
        .write()
        .register(StreamDefinition {
            name: "Transactions".into(),
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

    let shard_count = N_SHARDS as usize;
    let inbox_size = beava::shard::thread::inbox_size_from_env();
    let handles = beava::shard::thread::spawn_shard_threads(shard_count, inbox_size, state.clone(), None);
    *state.shard_handles.write() = handles;
    beava::server::shard_probe::init_route_counters(shard_count);
    beava::metrics::install_prometheus_recorder();
    beava::shard::metrics::register_shard_metrics(shard_count);

    state
}

/// Inject loopback ConnectInfo so `require_loopback_or_token` accepts the
/// request without a bearer token. Same helper as `http_ingest_routing.rs`.
fn inject_loopback(req: &mut Request<Body>) {
    use axum::extract::ConnectInfo;
    let addr: std::net::SocketAddr = "127.0.0.1:1".parse().unwrap();
    req.extensions_mut().insert(ConnectInfo(addr));
}

/// Sum `beava_shard_events_total{outcome="accepted"}` across all shards in a
/// Prometheus scrape. Order-insensitive label matching; `shard` label ignored.
fn sum_accepted(scrape: &str) -> u64 {
    let mut total = 0u64;
    for line in scrape.lines() {
        if !line.starts_with("beava_shard_events_total") {
            continue;
        }
        let Some(rbrace) = line.find('}') else {
            continue;
        };
        if !line.contains("outcome=\"accepted\"") {
            continue;
        }
        let value_str = line[rbrace + 1..].trim();
        if let Ok(n) = value_str.parse::<u64>() {
            total += n;
        } else if let Ok(f) = value_str.parse::<f64>() {
            total += f as u64;
        }
    }
    total
}

/// D-B3 regression guard: HTTP ingest (axum path) must keep accepting PUSHes
/// end-to-end across every Phase 58 wave. Passes today; must keep passing.
#[tokio::test]
async fn http_push_post_events_at_n4_matches_phase57() {
    let state = build_four_shard_state("push_still_works");

    // Snapshot accepted-count BEFORE — other tests in the same proc may
    // have already bumped it.
    let before = beava::metrics::handle()
        .map(|h| sum_accepted(&h.scrape()))
        .unwrap_or(0);

    // Push N_EVENTS events one-by-one to exercise the single-event axum path
    // (most sensitive to accidental-regression touches). Each response built
    // fresh via `oneshot` — a fresh Service per event, same as
    // http_ingest_routing.rs.
    for i in 0..N_EVENTS {
        let body = serde_json::json!({
            "user_id": format!("u{i:04}"),
            "amount": 100 + i,
        });
        let mut req = Request::builder()
            .method("POST")
            .uri("/push/Transactions")
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(&body).unwrap()))
            .unwrap();
        inject_loopback(&mut req);

        let app = build_router(state.clone());
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(
            resp.status(),
            StatusCode::OK,
            "HTTP POST /push/Transactions event #{i} must return 200 \
             (TPC-PERF-08 D-B3 regression guard — HTTP path UNCHANGED across Phase 58)"
        );
    }

    // Give every shard thread time to drain its SPSC inbox.
    tokio::time::sleep(Duration::from_millis(250)).await;

    let after = beava::metrics::handle()
        .map(|h| sum_accepted(&h.scrape()))
        .unwrap_or(0);

    // ≥ N_EVENTS because other concurrent test fixtures in the same proc
    // may also be pushing. We only need to prove N_EVENTS made it through.
    assert!(
        after >= before + N_EVENTS as u64,
        "TPC-PERF-08 D-B3 regression guard: HTTP PUSH did not reach shard SPSC. \
         Expected +{N_EVENTS} on beava_shard_events_total{{outcome=accepted}} sum, \
         got delta {} (before={before}, after={after}). This means an HTTP \
         ingest path has regressed during Phase 58.",
        (after as i64) - (before as i64)
    );

    // Pin state so the shard threads stay live until the asserts complete.
    let _ = &state;
}
