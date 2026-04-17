//! Phase 25-02 — `GET /debug/warnings` endpoint integration tests.
//!
//! Validates the locked response shape from `25-CONTEXT.md §decisions`:
//!
//! ```json
//! { "generated_at": "<RFC3339 UTC>",
//!   "observation_window": "7d",
//!   "warnings": [{ id, severity, category, title, detail, action?,
//!                  first_seen, last_seen, evidence }] }
//! ```
//!
//! Severity sort: `critical > error > warning > info`; stable by
//! `first_seen` ascending within a severity. Dedupe by `id` — second
//! record must not produce a duplicate response entry.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use axum::body::Body;
use axum::extract::ConnectInfo;
use axum::http::{Request, StatusCode};
use tower::ServiceExt;

use beava::engine::pipeline::PipelineEngine;
use beava::server::http::build_router;
use beava::server::signals::{Category, Severity, Signal};
use beava::server::tcp::{make_concurrent_state_full, BackfillTracker, SharedState};
use beava::state::store::StateStore;

fn test_state() -> SharedState {
    make_concurrent_state_full(
        PipelineEngine::new(),
        StateStore::new(),
        None,
        std::path::PathBuf::from("/tmp/beava-test-warnings.snapshot"),
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

async fn fetch_warnings(state: SharedState, uri: &str) -> serde_json::Value {
    let app = build_router(state);
    let resp = app.oneshot(loopback_request(uri)).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK, "expected 200 from {}", uri);
    let bytes = axum::body::to_bytes(resp.into_body(), 1024 * 1024)
        .await
        .unwrap();
    serde_json::from_slice(&bytes).unwrap()
}

fn mk_signal(id: &str, sev: Severity, cat: Category, first_seen: SystemTime) -> Signal {
    let mut s = Signal::new(
        id,
        sev,
        cat,
        format!("{} title", id),
        format!("{} detail", id),
        serde_json::json!({"id": id}),
    );
    s.first_seen = first_seen;
    s.last_seen = first_seen;
    s
}

// ---------------------------------------------------------------------------
// Response-shape + basic behaviour
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_empty_registry_empty_warnings_array() {
    let state = test_state();
    let body = fetch_warnings(state, "/debug/warnings").await;
    assert!(body["generated_at"].is_string(), "generated_at required");
    assert_eq!(body["observation_window"], "7d");
    assert!(body["warnings"].is_array());
    assert_eq!(body["warnings"].as_array().unwrap().len(), 0);
}

#[tokio::test]
async fn test_single_signal_in_response() {
    let state = test_state();
    state.signals.write().record(mk_signal(
        "only.one",
        Severity::Warning,
        Category::Safety,
        SystemTime::now() - Duration::from_secs(60),
    ));
    let body = fetch_warnings(state, "/debug/warnings").await;
    let warnings = body["warnings"].as_array().unwrap();
    assert_eq!(warnings.len(), 1);
    let w = &warnings[0];
    assert_eq!(w["id"], "only.one");
    assert_eq!(w["severity"], "warning");
    assert_eq!(w["category"], "safety");
    assert_eq!(w["title"], "only.one title");
    assert_eq!(w["detail"], "only.one detail");
    assert!(w["first_seen"].is_string(), "first_seen must be a string");
    assert!(w["last_seen"].is_string(), "last_seen must be a string");
    assert!(w["evidence"].is_object());
    assert_eq!(w["evidence"]["id"], "only.one");
}

#[tokio::test]
async fn test_observation_window_field_is_7d() {
    let state = test_state();
    let body = fetch_warnings(state, "/debug/warnings").await;
    assert_eq!(body["observation_window"], "7d");
}

#[tokio::test]
async fn test_severity_sort_order() {
    let state = test_state();
    let t = SystemTime::now() - Duration::from_secs(60);
    {
        let mut reg = state.signals.write();
        reg.record(mk_signal("info", Severity::Info, Category::Config, t));
        reg.record(mk_signal("warn", Severity::Warning, Category::Config, t));
        reg.record(mk_signal("err", Severity::Error, Category::Config, t));
        reg.record(mk_signal("crit", Severity::Critical, Category::Config, t));
    }
    let body = fetch_warnings(state, "/debug/warnings").await;
    let warnings = body["warnings"].as_array().unwrap();
    let ids: Vec<&str> = warnings
        .iter()
        .map(|w| w["id"].as_str().unwrap())
        .collect();
    assert_eq!(ids, vec!["crit", "err", "warn", "info"]);
    let sevs: Vec<&str> = warnings
        .iter()
        .map(|w| w["severity"].as_str().unwrap())
        .collect();
    assert_eq!(sevs, vec!["critical", "error", "warning", "info"]);
}

#[tokio::test]
async fn test_dedupe_visible_single_entry() {
    let state = test_state();
    let t0 = SystemTime::now() - Duration::from_secs(60);
    let t1 = t0 + Duration::from_secs(60);
    {
        let mut reg = state.signals.write();
        reg.record(mk_signal(
            "dupe.me",
            Severity::Warning,
            Category::Operational,
            t0,
        ));
        reg.record(mk_signal(
            "dupe.me",
            Severity::Warning,
            Category::Operational,
            t1,
        ));
    }
    let body = fetch_warnings(state, "/debug/warnings").await;
    let warnings = body["warnings"].as_array().unwrap();
    assert_eq!(warnings.len(), 1, "dedupe must collapse duplicate ids");
    // first_seen should still reflect t0 (preserved), last_seen t1.
    let first = warnings[0]["first_seen"].as_str().unwrap();
    let last = warnings[0]["last_seen"].as_str().unwrap();
    assert_ne!(first, last, "first_seen != last_seen after re-record");
}

#[tokio::test]
async fn test_category_query_param_filters() {
    let state = test_state();
    let t = SystemTime::now() - Duration::from_secs(60);
    {
        let mut reg = state.signals.write();
        reg.record(mk_signal("s1", Severity::Warning, Category::Safety, t));
        reg.record(mk_signal(
            "d1",
            Severity::Warning,
            Category::DataQuality,
            t,
        ));
        reg.record(mk_signal(
            "o1",
            Severity::Warning,
            Category::Operational,
            t,
        ));
    }
    let body = fetch_warnings(state.clone(), "/debug/warnings?category=safety").await;
    let warnings = body["warnings"].as_array().unwrap();
    assert_eq!(warnings.len(), 1);
    assert_eq!(warnings[0]["id"], "s1");

    let body = fetch_warnings(state.clone(), "/debug/warnings?category=data_quality").await;
    let warnings = body["warnings"].as_array().unwrap();
    assert_eq!(warnings.len(), 1);
    assert_eq!(warnings[0]["id"], "d1");

    // Unknown category → full list (filter parses to None).
    let body = fetch_warnings(state, "/debug/warnings?category=bogus").await;
    assert_eq!(body["warnings"].as_array().unwrap().len(), 3);
}

#[tokio::test]
async fn test_all_five_categories_serialize_correctly() {
    let state = test_state();
    let t = SystemTime::now() - Duration::from_secs(60);
    {
        let mut reg = state.signals.write();
        reg.record(mk_signal("c", Severity::Info, Category::Config, t));
        reg.record(mk_signal(
            "d",
            Severity::Info,
            Category::DataQuality,
            t + Duration::from_secs(1),
        ));
        reg.record(mk_signal(
            "o",
            Severity::Info,
            Category::Operational,
            t + Duration::from_secs(2),
        ));
        reg.record(mk_signal(
            "s",
            Severity::Info,
            Category::Safety,
            t + Duration::from_secs(3),
        ));
        reg.record(mk_signal(
            "p",
            Severity::Info,
            Category::Performance,
            t + Duration::from_secs(4),
        ));
    }
    let body = fetch_warnings(state, "/debug/warnings").await;
    let warnings = body["warnings"].as_array().unwrap();
    assert_eq!(warnings.len(), 5);
    let cats: Vec<&str> = warnings
        .iter()
        .map(|w| w["category"].as_str().unwrap())
        .collect();
    // All five serialize as the expected snake_case tokens.
    for expected in [
        "config",
        "data_quality",
        "operational",
        "safety",
        "performance",
    ] {
        assert!(
            cats.contains(&expected),
            "missing category '{}' in {:?}",
            expected,
            cats
        );
    }
}

// ---------------------------------------------------------------------------
// Config category round-trip stub (plan 25-03 will wire an emitter; for now
// we just prove the endpoint path works end-to-end for this category too).
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_config_category_signal_roundtrips_through_endpoint() {
    let state = test_state();
    state.signals.write().record(
        mk_signal(
            "config.ttl.UserProfile",
            Severity::Info,
            Category::Config,
            SystemTime::now() - Duration::from_secs(60),
        )
        .with_action(serde_json::json!({
            "type": "config_change",
            "knob": "UserProfile.ttl",
            "suggested": "60d"
        })),
    );
    let body = fetch_warnings(state, "/debug/warnings?category=config").await;
    let warnings = body["warnings"].as_array().unwrap();
    assert_eq!(warnings.len(), 1);
    assert_eq!(warnings[0]["category"], "config");
    assert_eq!(warnings[0]["action"]["knob"], "UserProfile.ttl");
    assert_eq!(warnings[0]["action"]["suggested"], "60d");
}

// ---------------------------------------------------------------------------
// Within-severity stable order by first_seen (ascending).
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_within_severity_order_by_first_seen_ascending() {
    let state = test_state();
    let base = SystemTime::now() - Duration::from_secs(60);
    {
        let mut reg = state.signals.write();
        reg.record(mk_signal(
            "b",
            Severity::Warning,
            Category::Safety,
            base + Duration::from_secs(20),
        ));
        reg.record(mk_signal(
            "a",
            Severity::Warning,
            Category::Safety,
            base + Duration::from_secs(10),
        ));
        reg.record(mk_signal(
            "c",
            Severity::Warning,
            Category::Safety,
            base + Duration::from_secs(30),
        ));
    }
    let body = fetch_warnings(state, "/debug/warnings").await;
    let warnings = body["warnings"].as_array().unwrap();
    let ids: Vec<&str> = warnings
        .iter()
        .map(|w| w["id"].as_str().unwrap())
        .collect();
    assert_eq!(ids, vec!["a", "b", "c"]);
}

// ---------------------------------------------------------------------------
// Auth gate — the endpoint must be admin-gated (same middleware as other
// /debug/* routes).
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_debug_warnings_is_admin_gated() {
    let state = test_state();
    let app = build_router(state);
    // Non-loopback peer, no token → 401 (HTTP-06 / orchestrator decision A4).
    let addr: SocketAddr = "8.8.8.8:54321".parse().unwrap();
    let mut req = Request::builder()
        .method("GET")
        .uri("/debug/warnings")
        .body(Body::empty())
        .unwrap();
    req.extensions_mut().insert(ConnectInfo(addr));
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
}
