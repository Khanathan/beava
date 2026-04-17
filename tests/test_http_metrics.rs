//! Phase 45-04 Task 3: /metrics dual-emit proto-label integration test (A5).
//!
//! Verifies that after N HTTP pushes and M simulated-TCP pushes the `/metrics`
//! endpoint emits exactly the three beava_events_total lines required by A5:
//!
//!   beava_events_total <N+M>           (unlabeled — backward compat)
//!   beava_events_total{proto="http"} N
//!   beava_events_total{proto="tcp"}  M

mod http_common;

use std::sync::Arc;
use std::time::{Duration, SystemTime};

use axum::body::Body;
use axum::http::{Request, StatusCode};
use beava::engine::pipeline::{FeatureDef, PipelineEngine, StreamDefinition};
use beava::server::http::build_router;
use beava::server::tcp::{
    handle_push_core_ex, make_concurrent_state_full, BackfillTracker, SharedState,
};
use http_common::{inject_loopback, TEST_ADMIN_TOKEN};
use serde_json::json;
use tower::ServiceExt;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Build a fresh SharedState and register a simple stream for pushing into.
fn build_state_with_stream() -> SharedState {
    let state = make_concurrent_state_full(
        PipelineEngine::new(),
        beava::state::store::StateStore::new(),
        None,
        std::path::PathBuf::from("/tmp/beava-test-http-metrics.snapshot"),
        Arc::new(BackfillTracker::default()),
        false, // snapshot_enabled
        false, // event_log_enabled
        Some(TEST_ADMIN_TOKEN.to_string()),
        false, // public_mode — /metrics is admin-gated
    );
    state
        .engine
        .write()
        .register(StreamDefinition {
            name: "metrics_test".into(),
            key_field: Some("id".into()),
            group_by_keys: None,
            features: vec![(
                "count".into(),
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
        })
        .unwrap();
    state
}

/// Send a POST /push/{stream} request through the router and return the status.
async fn http_push_one(
    app: axum::Router,
    stream: &str,
    payload: &str,
) -> (axum::Router, StatusCode) {
    let mut req = Request::builder()
        .method("POST")
        .uri(format!("/push/{}", stream))
        .header("content-type", "application/json")
        .body(Body::from(payload.to_string()))
        .unwrap();
    inject_loopback(&mut req);
    let resp = app.clone().oneshot(req).await.unwrap();
    (app, resp.status())
}

/// GET /metrics through the router and return the body as a String.
async fn get_metrics(app: axum::Router) -> String {
    let mut req = Request::builder()
        .method("GET")
        .uri("/metrics")
        .body(Body::empty())
        .unwrap();
    inject_loopback(&mut req);
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK, "GET /metrics should return 200");
    let bytes = axum::body::to_bytes(resp.into_body(), 1024 * 1024)
        .await
        .unwrap();
    String::from_utf8(bytes.to_vec()).unwrap()
}

// ---------------------------------------------------------------------------
// Test 1: N HTTP + M TCP pushes → three beava_events_total lines with correct values
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_proto_labeled_events_total() {
    let state = build_state_with_stream();
    let state_for_tcp = Arc::clone(&state);
    let app = build_router(state);

    // Push 5 events via HTTP.
    let mut cur_app = app;
    for i in 0..5u32 {
        let payload =
            json!({"id": format!("u{}", i), "_event_time": 1700000000000u64 + i as u64})
                .to_string();
        let (next_app, status) = http_push_one(cur_app, "metrics_test", &payload).await;
        cur_app = next_app;
        assert_eq!(status, StatusCode::OK, "HTTP push {} failed", i);
    }

    // Push 3 events via direct handle_push_core_ex (simulating TCP path).
    // handle_push_core_ex bumps events_total internally; we also bump events_tcp
    // to mirror the real TCP sync-push path at tcp.rs.
    let now = SystemTime::now();
    for i in 0..3u32 {
        let payload =
            json!({"id": format!("tcp{}", i), "_event_time": 1700000001000u64 + i as u64});
        let raw = serde_json::to_vec(&payload).unwrap();
        handle_push_core_ex(&state_for_tcp, "metrics_test", &payload, &raw, now, false)
            .expect("TCP push should succeed");
        // Mirror what the TCP sync-push arm does in tcp.rs (handle_sync_command path).
        state_for_tcp
            .events_tcp
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    // Scrape /metrics.
    let body = get_metrics(cur_app).await;

    // Total = 5 HTTP + 3 TCP = 8.
    assert!(
        body.lines().any(|l| l == "beava_events_total 8"),
        "expected 'beava_events_total 8' in metrics body:\n{}",
        body
    );
    assert!(
        body.lines()
            .any(|l| l == "beava_events_total{proto=\"http\"} 5"),
        "expected 'beava_events_total{{proto=\"http\"}} 5' in metrics body:\n{}",
        body
    );
    assert!(
        body.lines()
            .any(|l| l == "beava_events_total{proto=\"tcp\"} 3"),
        "expected 'beava_events_total{{proto=\"tcp\"}} 3' in metrics body:\n{}",
        body
    );
}

// ---------------------------------------------------------------------------
// Test 2: Pure HTTP run — events_tcp must be 0
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_pure_http_run_shows_zero_tcp() {
    let state = build_state_with_stream();
    let app = build_router(state);

    // Push 4 events via HTTP only — no TCP calls at all.
    let mut cur_app = app;
    for i in 0..4u32 {
        let payload = json!({"id": format!("httponly{}", i)}).to_string();
        let (next_app, status) = http_push_one(cur_app, "metrics_test", &payload).await;
        cur_app = next_app;
        assert_eq!(status, StatusCode::OK, "HTTP push {} failed", i);
    }

    let body = get_metrics(cur_app).await;

    assert!(
        body.lines()
            .any(|l| l == "beava_events_total{proto=\"http\"} 4"),
        "expected 'beava_events_total{{proto=\"http\"}} 4' in metrics body:\n{}",
        body
    );
    assert!(
        body.lines()
            .any(|l| l == "beava_events_total{proto=\"tcp\"} 0"),
        "expected 'beava_events_total{{proto=\"tcp\"}} 0' in metrics body:\n{}",
        body
    );
}
