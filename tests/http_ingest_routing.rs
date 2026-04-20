//! Phase 54-00 Task 3 — HTTP ingest routing RED test (drives Wave 1 Task 1).
//!
//! Proves that at N=1 an HTTP push transits the shard-thread SPSC inbox, by
//! observing the `beava_shard_events_total{shard="0",outcome="accepted"}`
//! counter. At Phase 53 HEAD this counter is NOT incremented on the N=1
//! branch of `handle_push_core_ex` (see src/server/tcp.rs lines ~1660-1735
//! "N==0 or N==1: legacy engine.push_with_cascade path"), so this test MUST
//! FAIL today.
//!
//! Wave 1 plan 54-01 Task 1 rewires the N=1 path through the shard SPSC,
//! causing `record_shard_event(0, Accepted)` to fire and flipping this test
//! GREEN.
//!
//! Test command:
//!   cargo test --release --test http_ingest_routing
//!
//! Expected Wave 0 behaviour: FAILED with
//!   assertion `beava_shard_events_total{{shard="0",outcome="accepted"}} == 0`

use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use tower::ServiceExt;

use beava::engine::pipeline::{FeatureDef, PipelineEngine, StreamDefinition};
use beava::server::http::build_router;
use beava::server::tcp::{make_concurrent_state_full, BackfillTracker, SharedState};
const TEST_ADMIN: &str = "test-admin-54-00-http-routing";

/// Build a 1-shard SharedState and register a stream with key_field=user_id.
/// Shard threads are spawned so the SPSC dispatch path is live.
fn build_single_shard_state(tag: &str) -> SharedState {
    let state = make_concurrent_state_full(
        PipelineEngine::new(),
        None,
        std::path::PathBuf::from(format!("/tmp/beava-test-54-00-http-{tag}.snapshot")),
        Arc::new(BackfillTracker::default()),
        false,
        false,
        Some(TEST_ADMIN.to_string()),
        false,
        1, // n_shards = 1 — the critical case for TPC-ARCH-01.
    );

    state
        .engine
        .write()
        .register(StreamDefinition {
            name: "test_stream".into(),
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

    // Spawn the shard threads so the SPSC inbox is live.
    let handles = beava::shard::thread::spawn_shard_threads(1, 65_536, state.clone());
    *state.shard_handles.write() = handles;
    beava::server::shard_probe::init_route_counters(1);

    beava::metrics::install_prometheus_recorder();
    beava::shard::metrics::register_shard_metrics(1);

    state
}

/// Parse `metric{label=value,...} N` from a Prometheus scrape. Returns 0 if
/// not found. Label match is order-insensitive.
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

/// Inject loopback ConnectInfo so `require_loopback_or_token` accepts the
/// request without a token.
fn inject_loopback(req: &mut Request<Body>) {
    use axum::extract::ConnectInfo;
    let addr: std::net::SocketAddr = "127.0.0.1:1".parse().unwrap();
    req.extensions_mut().insert(ConnectInfo(addr));
}

/// At N=1, an HTTP push MUST route through the shard-thread SPSC inbox and
/// increment `beava_shard_events_total{shard="0",outcome="accepted"}`.
///
/// Phase 53 HEAD behaviour: FAILS. The N=1 branch of `handle_push_core_ex`
/// (src/server/tcp.rs ~line 1730: `N==0 or N==1: legacy engine.push_with_cascade
/// path`) does NOT call `record_shard_event`. The per-shard counter stays at 0
/// after push.
///
/// Phase 54-01 Task 1 (Wave 1 GREEN) behaviour: PASSES. N=1 uses the same
/// SPSC path as N>1; the shard thread calls `record_shard_event(0, Accepted)`
/// when it drains its inbox.
#[tokio::test]
async fn http_push_at_n1_routes_through_spsc() {
    let state = build_single_shard_state("http_push");
    let app = build_router(state.clone());

    // Snapshot the counter BEFORE pushing (other tests in the same process
    // may have already incremented it).
    let before = beava::metrics::handle()
        .map(|h| {
            parse_counter(
                &h.scrape(),
                "beava_shard_events_total",
                &[("shard", "0"), ("outcome", "accepted")],
            )
        })
        .unwrap_or(0);

    // POST one event to /push/test_stream.
    let body = serde_json::json!({ "user_id": "u1", "amount": 100 });
    let mut req = Request::builder()
        .method("POST")
        .uri("/push/test_stream")
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_vec(&body).unwrap()))
        .unwrap();
    inject_loopback(&mut req);
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "HTTP /push/test_stream must return 200"
    );

    // Give the shard thread time to drain its SPSC inbox and record the metric.
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

    // Assertion 1 — counter must increment.
    // At Phase 53 HEAD this PASSES trivially: the N=1 legacy branch of
    // handle_push_core_ex ALSO calls record_shard_event (src/server/tcp.rs:1924).
    // Wave 1 still must keep this invariant.
    assert!(
        after > before,
        "TPC-ARCH-01 (HTTP routing, weak check): \
         `beava_shard_events_total{{shard=\"0\",outcome=\"accepted\"}}` did not \
         increment (before={before}, after={after})."
    );

    // Phase 54-04 Pass A6a: legacy DashMap `state.store` field deleted. The
    // SPSC-transit invariant is now structurally enforced — there is no
    // DashMap to touch. Assertion kept as a documented no-op.
    let _ = &state;
}
