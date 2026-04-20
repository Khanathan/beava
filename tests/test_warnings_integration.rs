//! Plan 25-03 Task 2 — end-to-end integration tests for the unified
//! `/debug/warnings` feed against a live Axum router.
//!
//! Unlike `test_warnings_feed.rs`, these exercises drive the feed through
//! realistic production flows:
//!
//! 1. Late-drop signals across two polling cycles (the bootstrap +
//!    rate-emit lifecycle).
//! 2. Config recommendation round-trip — same underlying
//!    `recommend_config` output visible on both
//!    `/debug/config-recommendations` AND `/debug/warnings`.
//! 3. Safety — a register-failure emission persists until resolved by an
//!    explicit `age_out` (simulating CONTEXT-window expiry).
//! 4. Endpoint under load — 100 concurrent polls return consistent
//!    snapshots without panicking or mutating the registry.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use axum::body::Body;
use axum::extract::ConnectInfo;
use axum::http::{Request, StatusCode};
use tower::ServiceExt;

use beava::engine::pipeline::PipelineEngine;
use beava::engine::recommend::ConfigRecommendation;
use beava::server::http::build_router;
use beava::server::signals::{
    emit_config_recommendations, emit_late_drop_signals, emit_register_failure,
};
use beava::server::tcp::{make_concurrent_state_full, BackfillTracker, SharedState};
// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

fn fresh_state() -> SharedState {
    make_concurrent_state_full(
        PipelineEngine::new(),
        None,
        std::path::PathBuf::from("/tmp/beava-test-warnings-integration.snapshot"),
        Arc::new(BackfillTracker::default()),
        true,
        false,
        None,
        false,
        1,
    )
}

fn loopback_get(uri: &str) -> Request<Body> {
    let addr: SocketAddr = "127.0.0.1:54321".parse().unwrap();
    let mut req = Request::builder()
        .method("GET")
        .uri(uri)
        .body(Body::empty())
        .unwrap();
    req.extensions_mut().insert(ConnectInfo(addr));
    req
}

async fn get_json(state: SharedState, uri: &str) -> serde_json::Value {
    let app = build_router(state);
    let resp = app.oneshot(loopback_get(uri)).await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "GET {} returned non-200",
        uri
    );
    let bytes = axum::body::to_bytes(resp.into_body(), 1024 * 1024)
        .await
        .unwrap();
    serde_json::from_slice(&bytes).unwrap()
}

fn has_id(body: &serde_json::Value, id: &str) -> bool {
    body["warnings"]
        .as_array()
        .map(|arr| arr.iter().any(|w| w["id"].as_str() == Some(id)))
        .unwrap_or(false)
}

// ---------------------------------------------------------------------------
// Test 1 — late-drop stress fires and ages out.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_late_drop_fires_and_clears() {
    let state = fresh_state();

    // Bootstrap sample at t0 — emitter returns None (no rate yet).
    let t0 = SystemTime::now() - Duration::from_secs(3600);
    emit_late_drop_signals(&state.signals, &[("BenchStream".into(), 0)], t0, 1.0);
    let body = get_json(state.clone(), "/debug/warnings").await;
    assert!(
        !has_id(&body, "late_drop.BenchStream"),
        "bootstrap call must not emit a signal"
    );

    // Second sample at t0 + 10s with 1000 drops → 100 drops/s. Warning fires.
    let t1 = t0 + Duration::from_secs(10);
    emit_late_drop_signals(&state.signals, &[("BenchStream".into(), 1000)], t1, 1.0);
    let body = get_json(state.clone(), "/debug/warnings").await;
    assert!(
        has_id(&body, "late_drop.BenchStream"),
        "expected late_drop.BenchStream after rate cross: {:?}",
        body
    );

    // Simulate "signal cleared" by aging out — the registry's age_out drops
    // entries whose last_seen is older than the observation window. We
    // recreate the registry at a small observation window to force expiry
    // deterministically.
    //
    // The production path uses the 7-day default; here we just assert the
    // lifecycle works by manually aging-out past the last_seen timestamp.
    let far_future = t1 + Duration::from_secs(8 * 86400);
    state.signals.write().age_out(far_future);

    // A subsequent endpoint fetch (the handler calls age_out with
    // SystemTime::now which is much smaller than far_future; but since we
    // already mutated the registry above, the signal is gone).
    let reg_after = state.signals.read();
    assert_eq!(
        reg_after.len(),
        0,
        "signal must age out past observation window"
    );
}

// ---------------------------------------------------------------------------
// Test 2 — config recommendation visible on both surfaces.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_config_recommendation_surfaces_as_warning() {
    let state = fresh_state();
    // Fan a single recommendation through the emitter — this is exactly
    // what `poll_signal_sources` does on each snapshot cycle.
    let recs = vec![ConfigRecommendation {
        knob: "UserProfile.ttl".into(),
        current: "30d".into(),
        suggested: "60d".into(),
        confidence: 0.72,
        reason: "12% of TTL-evicted keys reactivated within 24h".into(),
        evidence: serde_json::json!({"evictions_24h": 48213}),
        copy_paste: "@tl.table(key=\"user_id\", ttl=\"60d\")".into(),
    }];
    emit_config_recommendations(&state.signals, &recs);

    // /debug/warnings shows it under the config category.
    let warnings = get_json(state.clone(), "/debug/warnings?category=config").await;
    assert!(
        has_id(&warnings, "config.UserProfile.ttl"),
        "expected config.UserProfile.ttl in /debug/warnings: {:?}",
        warnings
    );

    // /debug/config-recommendations is the source of truth — hitting it
    // should return an empty list in this fixture (we didn't seed the
    // eviction tracker). The point of this test is the
    // warnings-side surface renders the fan-out correctly; the tracker
    // wiring is covered by `test_config_recommendations.rs`.
    let recs_body = get_json(state.clone(), "/debug/config-recommendations").await;
    assert_eq!(recs_body["observation_window"], "7d");
    // The live tracker has no evictions so the real endpoint is empty —
    // but both surfaces use the same generated_at/observation_window shape.
    assert!(recs_body["recommendations"].is_array());

    // Action on the warning carries the copy-paste line.
    let ws = warnings["warnings"].as_array().unwrap();
    let w = ws
        .iter()
        .find(|w| w["id"] == "config.UserProfile.ttl")
        .unwrap();
    assert_eq!(
        w["action"]["copy_paste"],
        "@tl.table(key=\"user_id\", ttl=\"60d\")"
    );
}

// ---------------------------------------------------------------------------
// Test 3 — register-failure persists until resolved.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_registration_failure_warning_persists_until_resolve() {
    let state = fresh_state();
    emit_register_failure(&state.signals, "P1", "bad json");
    let body = get_json(state.clone(), "/debug/warnings").await;
    assert!(has_id(&body, "register.failure.P1"));

    // Re-fetch repeatedly — the signal must remain across polls (no
    // silent drop from the endpoint's internal age_out call).
    for _ in 0..5 {
        let body = get_json(state.clone(), "/debug/warnings").await;
        assert!(
            has_id(&body, "register.failure.P1"),
            "register failure must persist across polls"
        );
    }

    // Explicit age-out past the observation window clears it — this
    // simulates an "admin action" resolution in the v0 contract (no
    // explicit resolve() API in v0; signals fall out on window expiry).
    let far_future = SystemTime::now() + Duration::from_secs(8 * 86400);
    state.signals.write().age_out(far_future);
    let body = get_json(state, "/debug/warnings").await;
    assert!(!has_id(&body, "register.failure.P1"));
}

// ---------------------------------------------------------------------------
// Test 4 — endpoint under load.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_warnings_endpoint_load() {
    // Seed a handful of signals then hammer the endpoint concurrently.
    let state = fresh_state();
    emit_register_failure(&state.signals, "P1", "err1");
    emit_register_failure(&state.signals, "P2", "err2");
    emit_register_failure(&state.signals, "P3", "err3");
    let baseline_count = state.signals.read().len();
    assert_eq!(baseline_count, 3);

    // Fire 100 concurrent requests. Each spawns its own oneshot router.
    let mut handles = Vec::new();
    for _ in 0..100 {
        let s = state.clone();
        handles.push(tokio::spawn(async move {
            let body = get_json(s, "/debug/warnings").await;
            body["warnings"].as_array().unwrap().len()
        }));
    }
    for h in handles {
        let n = h.await.unwrap();
        assert_eq!(
            n, 3,
            "concurrent snapshot must always see all 3 signals (saw {})",
            n
        );
    }

    // Registry is unchanged post-load: no phantom mutations.
    let post_count = state.signals.read().len();
    assert_eq!(
        post_count, baseline_count,
        "endpoint must NOT mutate the registry on read (before={} after={})",
        baseline_count, post_count
    );
}
