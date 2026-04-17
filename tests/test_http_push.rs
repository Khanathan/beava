//! Phase 45 — HTTP-01 + HTTP-02: single-event push and batch push tests.
//!
//! Wave 0: the 413 body-limit test passes against the stub handler because
//! RequestBodyLimitLayer rejects oversized requests before the handler runs.
//! The remaining sub-tests are stubbed until Wave 1 wires real handlers.

mod http_common;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use tower::ServiceExt;

use beava::server::http::build_router;
use http_common::{build_test_state, inject_loopback};

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
// Wave 1 stubs — filled by 45-02
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "Phase 45 Wave 1: http_push_single handler stub"]
async fn push_single_returns_200_on_happy() {
    panic!("MISSING: Wave 1 must implement push_single happy-path");
}

#[tokio::test]
#[ignore = "Phase 45 Wave 1: http_push_single handler stub"]
async fn push_single_returns_400_on_schema() {
    panic!("MISSING: Wave 1 must implement push_single schema-error path");
}

#[tokio::test]
#[ignore = "Phase 45 Wave 1: http_push_batch handler stub"]
async fn push_batch_returns_summary() {
    panic!("MISSING: Wave 1 must implement push_batch summary response");
}

#[tokio::test]
#[ignore = "Phase 45 Wave 1: http_push_batch handler stub"]
async fn push_batch_buckets_per_event_time() {
    panic!("MISSING: Wave 1 must implement push_batch per-event-time bucketing (HTTP-02)");
}
