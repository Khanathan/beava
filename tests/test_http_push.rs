//! Phase 45 — HTTP-01 + HTTP-02: single-event push and batch push tests.
//!
//! Wave 0: the 413 body-limit test passes against the stub handler because
//! RequestBodyLimitLayer rejects oversized requests before the handler runs.
//! Wave 1 (45-03): the remaining tests exercise the live handlers.

mod http_common;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use tower::ServiceExt;

use beava::engine::pipeline::{FeatureDef, PipelineEngine, StreamDefinition};
use beava::server::http::build_router;
use beava::server::tcp::{make_concurrent_state_full, BackfillTracker, SharedState};
use beava::state::store::StateStore;
use http_common::{build_test_state, inject_loopback};
use std::sync::Arc;
use std::time::Duration;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Register a minimal stream with a `sum(amount)` feature on a given state.
fn register_events_stream(state: &SharedState) {
    state
        .engine
        .write()
        .register(StreamDefinition {
            name: "events".into(),
            key_field: Some("user".into()),
            group_by_keys: None,
            features: vec![(
                "total_amount".into(),
                FeatureDef::Sum {
                    field: "amount".into(),
                    window: Duration::from_secs(3600),
                    bucket: Duration::from_secs(60),
                    optional: true,
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
}

// ---------------------------------------------------------------------------
// Wave 0 passing: 17 MiB body → 413 from RequestBodyLimitLayer
// ---------------------------------------------------------------------------

#[tokio::test]
async fn push_returns_413_on_17mib_body() {
    let app = build_router(build_test_state(false));
    let body_data = vec![b'a'; 17 * 1024 * 1024];
    let mut req = Request::builder()
        .method("POST")
        .uri("/push/teststream")
        .header("content-type", "application/json")
        .body(Body::from(body_data))
        .unwrap();
    inject_loopback(&mut req);
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::PAYLOAD_TOO_LARGE,
        "17 MiB body must be rejected with 413 by RequestBodyLimitLayer"
    );
}

// ---------------------------------------------------------------------------
// Wave 1: http_push_single (HTTP-01)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn push_single_returns_200_on_happy() {
    let state = build_test_state(false);
    register_events_stream(&state);
    let app = build_router(state);

    let mut req = Request::builder()
        .method("POST")
        .uri("/push/events")
        .header("content-type", "application/json")
        .body(Body::from(
            r#"{"user":"alice","amount":10,"_event_time":1700000000000}"#,
        ))
        .unwrap();
    inject_loopback(&mut req);
    let resp = app.oneshot(req).await.unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let body = axum::body::to_bytes(resp.into_body(), 1024 * 1024)
        .await
        .unwrap();
    let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(v["ok"], true, "expected ok:true, got: {v}");
}

#[tokio::test]
async fn push_single_returns_400_on_schema() {
    let state = build_test_state(false);
    register_events_stream(&state);
    let app = build_router(state);

    let mut req = Request::builder()
        .method("POST")
        .uri("/push/events")
        .header("content-type", "application/json")
        .body(Body::from("not-json-at-all"))
        .unwrap();
    inject_loopback(&mut req);
    let resp = app.oneshot(req).await.unwrap();

    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let body = axum::body::to_bytes(resp.into_body(), 1024 * 1024)
        .await
        .unwrap();
    let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(v["ok"], false);
    assert_eq!(
        v["error"]["code"], "schema_error",
        "expected schema_error code, got: {v}"
    );
}

/// `?sync=1` triggers `read_features=true` → event is visible in state
/// immediately after the call returns (orchestrator A7 in-memory drain).
#[tokio::test]
async fn push_single_sync_waits_for_visibility() {
    let state = build_test_state(false);
    register_events_stream(&state);
    let app = build_router(state);

    // Push with ?sync=1
    let mut req = Request::builder()
        .method("POST")
        .uri("/push/events?sync=1")
        .header("content-type", "application/json")
        .body(Body::from(r#"{"user":"syncuser","amount":42.0}"#))
        .unwrap();
    inject_loopback(&mut req);
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = axum::body::to_bytes(resp.into_body(), 1024 * 1024)
        .await
        .unwrap();
    let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(v["ok"], true);

    // The event must be immediately visible via GET /features/syncuser
    let mut get_req = Request::builder()
        .method("GET")
        .uri("/features/syncuser")
        .body(Body::empty())
        .unwrap();
    inject_loopback(&mut get_req);
    let get_resp = app.oneshot(get_req).await.unwrap();
    assert_eq!(get_resp.status(), StatusCode::OK);
    let get_body = axum::body::to_bytes(get_resp.into_body(), 1024 * 1024)
        .await
        .unwrap();
    let gv: serde_json::Value = serde_json::from_slice(&get_body).unwrap();
    assert_eq!(gv["ok"], true, "GET /features/syncuser failed: {gv}");
    // The data key must exist with something in tables
    assert!(
        gv["data"]["tables"].is_object(),
        "expected tables object, got: {gv}"
    );
}

// ---------------------------------------------------------------------------
// Wave 1: http_push_batch (HTTP-02)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn push_batch_returns_summary() {
    let state = build_test_state(false);
    register_events_stream(&state);
    let app = build_router(state);

    let mut req = Request::builder()
        .method("POST")
        .uri("/push-batch/events")
        .header("content-type", "application/json")
        .body(Body::from(
            r#"[
                {"user":"a","amount":1.0},
                {"user":"b","amount":2.0},
                {"user":"c","amount":3.0}
            ]"#,
        ))
        .unwrap();
    inject_loopback(&mut req);
    let resp = app.oneshot(req).await.unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let body = axum::body::to_bytes(resp.into_body(), 1024 * 1024)
        .await
        .unwrap();
    let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(v["ok"], true);
    assert_eq!(v["data"]["accepted"], 3, "expected 3 accepted: {v}");
    assert_eq!(v["data"]["rejected"], 0, "expected 0 rejected: {v}");
}

/// Verify per-event event_time capture: two events with distinct _event_times
/// are accepted and the state watermark advances (HTTP-02 client-side assertion).
#[tokio::test]
async fn push_batch_buckets_per_event_time() {
    let state = build_test_state(false);
    register_events_stream(&state);
    let app = build_router(state.clone());

    // event[0]: historical timestamp (2023-11-14)
    // event[1]: near-current timestamp (well within any window)
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    let body = format!(
        r#"[
            {{"user":"et_user","amount":5.0,"_event_time":1700000000000}},
            {{"user":"et_user","amount":7.0,"_event_time":{now_ms}}}
        ]"#
    );

    let mut req = Request::builder()
        .method("POST")
        .uri("/push-batch/events")
        .header("content-type", "application/json")
        .body(Body::from(body))
        .unwrap();
    inject_loopback(&mut req);
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let body = axum::body::to_bytes(resp.into_body(), 1024 * 1024)
        .await
        .unwrap();
    let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(v["ok"], true);
    // At least the near-now event should have been accepted
    assert!(
        v["data"]["accepted"].as_u64().unwrap_or(0) >= 1,
        "expected at least 1 accepted: {v}"
    );

    // Confirm the engine watermark is set and reflects the per-event event_times
    // that the HTTP handler passed via PendingAsync.now.
    //
    // Phase 45 semantics: handle_push_batch uses min(event_times) as the shared
    // bucket `now` (tcp.rs:1731). So with event[0]=1700000000000 and
    // event[1]=now_ms, the watermark will be at 1700000000000 (the min).
    // This is the client-visible half of CORR-01 / HTTP-02: if we had NOT set
    // per-event PendingAsync.now, the watermark would be at the wall-clock time
    // the handler ran, not the client-supplied _event_time.
    // Phase 46 fixes the min-bucket behavior; this assertion captures Phase 45
    // correct behavior.
    let wm = state
        .engine
        .read()
        .watermarks
        .watermark("events");
    assert!(
        wm.is_some(),
        "watermark for 'events' stream must be set after batch push"
    );
    let wm_ts = wm
        .unwrap()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    // The watermark must be at or beyond 1700000000000 (event[0]'s _event_time).
    // If per-event times were NOT passed, wm_ts would be near wall-clock time
    // and could differ significantly from this historical timestamp.
    assert!(
        wm_ts >= 1_700_000_000_000,
        "watermark {wm_ts}ms must be at or beyond historical event time 1700000000000ms"
    );
}

#[tokio::test]
async fn push_batch_empty_array_returns_zero_zero() {
    let state = build_test_state(false);
    register_events_stream(&state);
    let app = build_router(state);

    let mut req = Request::builder()
        .method("POST")
        .uri("/push-batch/events")
        .header("content-type", "application/json")
        .body(Body::from("[]"))
        .unwrap();
    inject_loopback(&mut req);
    let resp = app.oneshot(req).await.unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let body = axum::body::to_bytes(resp.into_body(), 1024 * 1024)
        .await
        .unwrap();
    let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(v["ok"], true);
    assert_eq!(v["data"]["accepted"], 0);
    assert_eq!(v["data"]["rejected"], 0);
}

#[tokio::test]
async fn push_batch_invalid_json_returns_400() {
    let state = build_test_state(false);
    register_events_stream(&state);
    let app = build_router(state);

    // Body is an object, not an array — serde will reject it
    let mut req = Request::builder()
        .method("POST")
        .uri("/push-batch/events")
        .header("content-type", "application/json")
        .body(Body::from(r#"{"not":"an array"}"#))
        .unwrap();
    inject_loopback(&mut req);
    let resp = app.oneshot(req).await.unwrap();

    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let body = axum::body::to_bytes(resp.into_body(), 1024 * 1024)
        .await
        .unwrap();
    let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(v["ok"], false);
    assert_eq!(v["error"]["code"], "schema_error");
}
