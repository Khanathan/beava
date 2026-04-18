//! Plan 25-03 — `/debug/warnings` unified feed: per-category trigger
//! coverage + schema shape assertion per `25-CONTEXT.md §Warnings`.
//!
//! Plan 25-02 already shipped the `SignalRegistry`, the endpoint handler,
//! the dedupe semantics, the severity sort, and the category filter. This
//! file's job is to prove every one of the five signal categories
//! (`config`, `data_quality`, `operational`, `safety`, `performance`) can
//! be triggered through its production emission path — not just a raw
//! `reg.record()` — and that all of those land in a single feed response
//! with the schema the UI depends on.
//!
//! Each test constructs a `SharedState`, drives the corresponding
//! emitter, and then queries `/debug/warnings` through the live Axum
//! router just like the UI does.

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
    emit_config_recommendations, emit_late_drop_signals, emit_perf_p99_signal,
    emit_register_failure, emit_snapshot_failure,
};
use beava::server::tcp::{make_concurrent_state_full, BackfillTracker, SharedState};
use beava::state::store::StateStore;

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

fn test_state() -> SharedState {
    make_concurrent_state_full(
        PipelineEngine::new(),
        StateStore::new(),
        None,
        std::path::PathBuf::from("/tmp/beava-test-warnings-feed.snapshot"),
        Arc::new(BackfillTracker::default()),
        true,
        false,
        None,
        false,
    )
}

fn loopback_request(uri: &str) -> Request<Body> {
    let addr: SocketAddr = "127.0.0.1:54321".parse().unwrap();
    let mut req = Request::builder()
        .method("GET")
        .uri(uri)
        .body(Body::empty())
        .unwrap();
    req.extensions_mut().insert(ConnectInfo(addr));
    req
}

async fn fetch_body(state: SharedState, uri: &str) -> serde_json::Value {
    let app = build_router(state);
    let resp = app.oneshot(loopback_request(uri)).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK, "expected 200 from {}", uri);
    let bytes = axum::body::to_bytes(resp.into_body(), 1024 * 1024)
        .await
        .unwrap();
    serde_json::from_slice(&bytes).unwrap()
}

fn find_warning<'a>(body: &'a serde_json::Value, id: &str) -> Option<&'a serde_json::Value> {
    body["warnings"]
        .as_array()?
        .iter()
        .find(|w| w["id"].as_str() == Some(id))
}

// ---------------------------------------------------------------------------
// Baseline: empty engine emits no warnings.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_warning_feed_returns_empty_on_healthy_engine() {
    let state = test_state();
    let body = fetch_body(state, "/debug/warnings").await;
    assert_eq!(body["warnings"].as_array().unwrap().len(), 0);
    assert_eq!(body["observation_window"], "7d");
}

// ---------------------------------------------------------------------------
// Category: config — from `recommend_config` fan-out (Plan 25-03 emitter).
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_config_category_from_recommendations() {
    let state = test_state();
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

    let body = fetch_body(state, "/debug/warnings?category=config").await;
    let w = find_warning(&body, "config.UserProfile.ttl")
        .expect("expected config.UserProfile.ttl in feed");
    assert_eq!(w["category"], "config");
    assert_eq!(w["severity"], "info");
    assert_eq!(w["title"], "TTL too short");
    // Action carries the copy-paste so the UI can render the button.
    assert_eq!(w["action"]["type"], "config_change");
    assert_eq!(w["action"]["knob"], "UserProfile.ttl");
    assert_eq!(w["action"]["suggested"], "60d");
    assert_eq!(
        w["action"]["copy_paste"],
        "@tl.table(key=\"user_id\", ttl=\"60d\")"
    );
    // Evidence points back at `/debug/config-recommendations#{knob}` so
    // operators can click-through from the warning to the full recommendation.
    assert_eq!(
        w["evidence"]["evidence_url"],
        "/debug/config-recommendations#UserProfile.ttl"
    );
}

#[tokio::test]
async fn test_config_category_covers_history_ttl_knob() {
    let state = test_state();
    let recs = vec![ConfigRecommendation {
        knob: "RawTxns.history_ttl".into(),
        current: "30d".into(),
        suggested: "90d".into(),
        confidence: 1.0,
        reason: "history_ttl is shorter than the max downstream Table ttl".into(),
        evidence: serde_json::json!({}),
        copy_paste: "@tl.stream(history_ttl=\"90d\")".into(),
    }];
    emit_config_recommendations(&state.signals, &recs);

    let body = fetch_body(state, "/debug/warnings").await;
    let w = find_warning(&body, "config.RawTxns.history_ttl")
        .expect("expected history_ttl recommendation as a config warning");
    assert_eq!(w["title"], "history_ttl too short");
    assert_eq!(w["category"], "config");
    assert_eq!(w["severity"], "info");
}

// ---------------------------------------------------------------------------
// Category: data_quality — via `emit_late_drop_signals` and two samples
// (we need the second to compute a rate).
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_data_quality_category_from_late_drops() {
    let state = test_state();
    let t0 = SystemTime::now() - Duration::from_secs(60);
    let t1 = t0 + Duration::from_secs(10);
    // Bootstrap sample — no signal yet.
    emit_late_drop_signals(&state.signals, &[("S".into(), 0)], t0, 1.0);
    // Second sample: 500 drops in 10 s = 50/s — comfortably above 1/s
    // threshold.
    emit_late_drop_signals(&state.signals, &[("S".into(), 500)], t1, 1.0);

    let body = fetch_body(state, "/debug/warnings?category=data_quality").await;
    let w = find_warning(&body, "late_drop.S").expect("expected late_drop.S warning");
    assert_eq!(w["category"], "data_quality");
    assert_eq!(w["severity"], "warning");
    // Evidence carries enough context for the operator to reproduce.
    assert_eq!(w["evidence"]["stream"], "S");
    assert_eq!(w["evidence"]["total_dropped"], 500);
    assert!(w["evidence"]["rate_per_sec"].as_f64().unwrap() > 1.0);
}

// ---------------------------------------------------------------------------
// Category: operational — via `emit_snapshot_failure` (the memory-pressure
// emitter reads /proc/self/statm which is flaky under test harnesses; the
// snapshot-failure path is the deterministic operational trigger).
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_operational_category_from_snapshot_failure() {
    let state = test_state();
    emit_snapshot_failure(&state.signals, "disk full: No space left on device");

    let body = fetch_body(state, "/debug/warnings?category=operational").await;
    let w = find_warning(&body, "snapshot.failure").expect("snapshot.failure missing");
    assert_eq!(w["category"], "operational");
    assert_eq!(w["severity"], "error");
    assert_eq!(w["title"], "Snapshot write failed");
}

#[tokio::test]
async fn test_operational_category_from_memory_pressure_via_record() {
    // Drive the memory-pressure path by recording the exact signal the
    // `emit_memory_pressure_signal` emitter would produce at 90% RSS.
    // We cannot fake RSS in-process, so we record through the same
    // registry with the same id — this validates the id scheme + the
    // serialized shape that the endpoint will emit when the emitter fires.
    use beava::server::signals::{Category, Severity, Signal};
    let state = test_state();
    state.signals.write().record(Signal::new(
        "memory.pressure",
        Severity::Warning,
        Category::Operational,
        "Memory pressure above 85% of configured limit",
        "RSS 7200 MiB / limit 8000 MiB (90.0%)",
        serde_json::json!({
            "rss_bytes": 7_549_747_200u64,
            "limit_bytes": 8_388_608_000u64,
            "ratio": 0.9f64,
        }),
    ));
    // Severity escalation check: re-record at Critical for >95%.
    state.signals.write().record(Signal::new(
        "memory.pressure",
        Severity::Critical,
        Category::Operational,
        "Memory pressure above 85% of configured limit",
        "RSS 8000 MiB / limit 8000 MiB (99.9%)",
        serde_json::json!({
            "rss_bytes": 8_388_000_000u64,
            "limit_bytes": 8_388_608_000u64,
            "ratio": 0.999f64,
        }),
    ));

    let body = fetch_body(state, "/debug/warnings?category=operational").await;
    let w = find_warning(&body, "memory.pressure").expect("memory.pressure missing");
    assert_eq!(w["category"], "operational");
    assert_eq!(w["severity"], "critical", "severity must escalate");
}

// ---------------------------------------------------------------------------
// Category: safety — via `emit_register_failure`.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_safety_category_from_registration_failure() {
    let state = test_state();
    emit_register_failure(
        &state.signals,
        "BadPipeline",
        "schema validation error: unknown field 'foo'",
    );

    let body = fetch_body(state, "/debug/warnings?category=safety").await;
    let w = find_warning(&body, "register.failure.BadPipeline")
        .expect("register.failure.BadPipeline missing");
    assert_eq!(w["category"], "safety");
    assert_eq!(w["severity"], "error");
    assert_eq!(w["title"], "Pipeline registration failed");
    assert_eq!(w["evidence"]["pipeline"], "BadPipeline");
}

// ---------------------------------------------------------------------------
// Category: performance — via `emit_perf_p99_signal`.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_performance_category_from_p99_slo_breach() {
    let state = test_state();
    // Simulate a p99 of 3.5ms against a 1ms SLO.
    emit_perf_p99_signal(&state.signals, 3500.0, 1000.0);

    let body = fetch_body(state, "/debug/warnings?category=performance").await;
    let w = find_warning(&body, "perf.push_p99_slo_breach").expect("perf signal missing");
    assert_eq!(w["category"], "performance");
    assert_eq!(w["severity"], "warning");
    assert!(w["evidence"]["p99_us"].as_f64().unwrap() > 1000.0);
}

// ---------------------------------------------------------------------------
// Cross-category: all five fire together → one feed contains them all,
// severity-sorted. This is the primary UI contract: a single poll returns
// everything the operator needs.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_all_five_categories_fire_in_one_feed() {
    let state = test_state();
    // Safety (Error)
    emit_register_failure(&state.signals, "P1", "validation failed");
    // Operational (Error)
    emit_snapshot_failure(&state.signals, "disk full");
    // Performance (Warning)
    emit_perf_p99_signal(&state.signals, 2000.0, 1000.0);
    // Data-quality (Warning) — two samples for rate.
    let t0 = SystemTime::now() - Duration::from_secs(60);
    let t1 = t0 + Duration::from_secs(10);
    emit_late_drop_signals(&state.signals, &[("S".into(), 0)], t0, 1.0);
    emit_late_drop_signals(&state.signals, &[("S".into(), 100)], t1, 1.0);
    // Config (Info)
    emit_config_recommendations(
        &state.signals,
        &[ConfigRecommendation {
            knob: "T.ttl".into(),
            current: "30d".into(),
            suggested: "60d".into(),
            confidence: 0.8,
            reason: "reinit rate above threshold".into(),
            evidence: serde_json::json!({}),
            copy_paste: "@tl.table(ttl=\"60d\")".into(),
        }],
    );

    let body = fetch_body(state, "/debug/warnings").await;
    let warnings = body["warnings"].as_array().unwrap();
    assert_eq!(
        warnings.len(),
        5,
        "expected one per category, got {:#?}",
        warnings
    );

    // Severity descending: Error, Error, Warning, Warning, Info.
    let sevs: Vec<&str> = warnings
        .iter()
        .map(|w| w["severity"].as_str().unwrap())
        .collect();
    assert_eq!(sevs[0], "error");
    assert_eq!(sevs[1], "error");
    assert_eq!(sevs[2], "warning");
    assert_eq!(sevs[3], "warning");
    assert_eq!(sevs[4], "info");

    // All five category tokens present.
    let cats: std::collections::HashSet<&str> = warnings
        .iter()
        .map(|w| w["category"].as_str().unwrap())
        .collect();
    for expected in [
        "config",
        "data_quality",
        "operational",
        "safety",
        "performance",
    ] {
        assert!(cats.contains(expected), "missing category '{}'", expected);
    }
}

// ---------------------------------------------------------------------------
// Schema contract — every field required by `25-CONTEXT.md §Warnings`
// is present on every warning emitted through the production paths.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_schema_shape_matches_context_md() {
    let state = test_state();
    emit_register_failure(&state.signals, "P1", "err");
    emit_config_recommendations(
        &state.signals,
        &[ConfigRecommendation {
            knob: "T.ttl".into(),
            current: "30d".into(),
            suggested: "60d".into(),
            confidence: 0.8,
            reason: "reason".into(),
            evidence: serde_json::json!({}),
            copy_paste: "@tl.table(ttl=\"60d\")".into(),
        }],
    );

    let body = fetch_body(state, "/debug/warnings").await;
    assert!(body["generated_at"].is_string());
    assert_eq!(body["observation_window"], "7d");
    for w in body["warnings"].as_array().unwrap() {
        // Required fields per CONTEXT schema.
        for f in [
            "id",
            "severity",
            "category",
            "title",
            "detail",
            "first_seen",
            "last_seen",
        ] {
            assert!(
                !w[f].is_null(),
                "required field '{}' missing from warning {:?}",
                f,
                w
            );
            assert!(w[f].is_string() || f == "evidence", "{} must be string", f);
        }
        // Timestamps parse as RFC3339-ish (YYYY-MM-DDTHH:MM:SSZ).
        let first = w["first_seen"].as_str().unwrap();
        assert!(
            first.contains('T') && first.ends_with('Z'),
            "first_seen: {}",
            first
        );
    }
}
