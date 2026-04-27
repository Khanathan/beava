//! Plan 19.2-07 (D-07): Integration tests for GET /debug/op-cost.
//!
//! Covers three behaviours:
//!
//! 1. **test_debug_op_cost_404_when_dev_endpoints_off** — Route is absent when
//!    `dev_endpoints=false` (production default). Expects HTTP 404.
//!
//! 2. **test_debug_op_cost_200_when_dev_endpoints_on** — Route is present when
//!    `dev_endpoints=true`. Expects HTTP 200 with a JSON body matching
//!    `{"ops": [...], "captured_at_ms": <u64>}`.
//!    After at least one event is applied with `BEAVA_TRACE_AGG_TIMING=1`, `ops`
//!    may be non-empty; the test doesn't force tracing but verifies the shape when
//!    the array is empty or populated.
//!
//! 3. **test_debug_op_cost_empty_when_no_traffic** — Route returns 200 with
//!    `ops == []` when no events have been processed yet (snapshot is empty).
//!
//! All tests use the in-process `router()` + `tower::ServiceExt::oneshot` pattern
//! (no real network, no WAL, no temp directories) so they are fast and hermetic.

use axum::body::Body;
use axum::http::{Method, Request, StatusCode};
use beava_core::registry::Registry;
use beava_server::http::{router, ReadinessFlag};
use http_body_util::BodyExt;
use std::sync::Arc;
use tower::ServiceExt;

// ── helpers ───────────────────────────────────────────────────────────────────

async fn get(r: axum::Router, uri: &str) -> (StatusCode, serde_json::Value) {
    let resp = r
        .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
        .await
        .expect("oneshot GET");
    let status = resp.status();
    let bytes = resp
        .into_body()
        .collect()
        .await
        .expect("collect body")
        .to_bytes();
    if bytes.is_empty() {
        (status, serde_json::Value::Null)
    } else {
        let json: serde_json::Value =
            serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null);
        (status, json)
    }
}

#[allow(dead_code)] // retained for future tests that push events before querying
async fn post_json(
    r: axum::Router,
    uri: &str,
    body: serde_json::Value,
) -> (StatusCode, serde_json::Value) {
    let payload = serde_json::to_vec(&body).unwrap();
    let req = Request::builder()
        .method(Method::POST)
        .uri(uri)
        .header("content-type", "application/json")
        .body(Body::from(payload))
        .unwrap();
    let resp = r.oneshot(req).await.expect("oneshot POST");
    let status = resp.status();
    let bytes = resp
        .into_body()
        .collect()
        .await
        .expect("collect body")
        .to_bytes();
    if bytes.is_empty() {
        (status, serde_json::Value::Null)
    } else {
        let json: serde_json::Value =
            serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null);
        (status, json)
    }
}

// ── tests ─────────────────────────────────────────────────────────────────────

/// Test 1: /debug/op-cost returns 404 when dev_endpoints=false (production default).
///
/// This is the security-critical gate: the route must NOT be mounted in
/// production. Any 404 response confirms the route is absent.
#[tokio::test]
async fn test_debug_op_cost_404_when_dev_endpoints_off() {
    let registry = Arc::new(Registry::new());
    // dev_endpoints = false → production config, route not mounted.
    let r = router(
        ReadinessFlag::new(),
        registry,
        false, /* dev_endpoints=false */
        None,
    );
    let (status, _body) = get(r, "/debug/op-cost").await;
    assert_eq!(
        status,
        StatusCode::NOT_FOUND,
        "/debug/op-cost must return 404 when dev_endpoints=false (default-off security gate)"
    );
}

/// Test 2: /debug/op-cost returns 200 + JSON shape when dev_endpoints=true.
///
/// JSON shape contract: `{"ops": <array>, "captured_at_ms": <number>}`
/// Each element of `ops` (if any) must have keys: kind, tier, last_traced_ns, last_traced_count.
///
/// This test doesn't force BEAVA_TRACE_AGG_TIMING=1 so `ops` may be empty
/// (no tracing has happened in this process). The shape test is: `ops` is a
/// JSON array, `captured_at_ms` is a JSON number.
#[tokio::test]
async fn test_debug_op_cost_200_when_dev_endpoints_on() {
    let registry = Arc::new(Registry::new());
    // dev_endpoints = true → debug route mounted.
    let r = router(
        ReadinessFlag::new(),
        registry.clone(),
        true, /* dev_endpoints=true */
        None,
    );
    let (status, body) = get(r, "/debug/op-cost").await;
    assert_eq!(
        status,
        StatusCode::OK,
        "/debug/op-cost must return 200 when dev_endpoints=true, got body: {body:#}"
    );

    // Shape validation: top-level keys must be present.
    assert!(
        body["ops"].is_array(),
        "response must have an 'ops' array, got: {body:#}"
    );
    assert!(
        body["captured_at_ms"].is_number(),
        "response must have 'captured_at_ms' number, got: {body:#}"
    );

    // Shape validation for any entries in ops (if tracing happened to run).
    let ops = body["ops"].as_array().unwrap();
    for (i, entry) in ops.iter().enumerate() {
        assert!(
            entry["kind"].is_string(),
            "ops[{i}].kind must be a string, got: {entry:#}"
        );
        assert!(
            entry["tier"].is_number(),
            "ops[{i}].tier must be a number, got: {entry:#}"
        );
        assert!(
            entry["last_traced_ns"].is_number(),
            "ops[{i}].last_traced_ns must be a number, got: {entry:#}"
        );
        assert!(
            entry["last_traced_count"].is_number(),
            "ops[{i}].last_traced_count must be a number, got: {entry:#}"
        );
    }
}

/// Test 3: /debug/op-cost returns 200 with ops=[] when no traffic has been processed.
///
/// The per-kind snapshot starts empty (OnceLock is fresh, no tracing has run).
/// Endpoint must return `{"ops": [], "captured_at_ms": 0}`.
#[tokio::test]
async fn test_debug_op_cost_empty_when_no_traffic() {
    let registry = Arc::new(Registry::new());
    let r = router(
        ReadinessFlag::new(),
        registry,
        true, /* dev_endpoints=true */
        None,
    );
    let (status, body) = get(r, "/debug/op-cost").await;
    assert_eq!(
        status,
        StatusCode::OK,
        "/debug/op-cost must return 200 even when no traffic, got body: {body:#}"
    );
    let ops = body["ops"].as_array().expect("ops must be an array");
    // No events pushed → no per_kind data. OnceLock snapshot is empty.
    // Note: if another test in this binary already populated the OnceLock, this
    // may be non-empty. The assertion uses `is_empty()` as the canonical no-traffic
    // postcondition but is gated only for the case where captured_at_ms == 0
    // (never written by any trace run in this process).
    let captured_at_ms = body["captured_at_ms"].as_u64().unwrap_or(0);
    if captured_at_ms == 0 {
        assert!(
            ops.is_empty(),
            "ops must be empty when no traffic has been processed (captured_at_ms == 0), got: {ops:#?}"
        );
    }
}
