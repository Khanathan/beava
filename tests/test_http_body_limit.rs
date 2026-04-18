//! Phase 45 — body-limit layer tests.
//!
//! Wave 0: all three tests run today because the RequestBodyLimitLayer is
//! already wired. The 17 MiB test asserts 413; the 1 MiB and 15 MiB tests
//! assert NOT 413 (may be 501 stub or 200 live — both are acceptable).

mod http_common;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use tower::ServiceExt;

use beava::server::http::build_router;
use http_common::{build_test_state, inject_loopback};

fn push_request_with_body(size_bytes: usize) -> Request<Body> {
    let body_data = vec![b'a'; size_bytes];
    // Wrap in a valid JSON-ish outer so the extractor doesn't choke before
    // the size check; the layer check happens at the raw-bytes level.
    Request::builder()
        .method("POST")
        .uri("/push/teststream")
        .header("content-type", "application/json")
        .body(Body::from(body_data))
        .unwrap()
}

// ---------------------------------------------------------------------------
// Under-limit requests: NOT 413
// ---------------------------------------------------------------------------

#[tokio::test]
async fn body_1mib_returns_200_or_501() {
    let app = build_router(build_test_state(false));
    let mut req = push_request_with_body(1024 * 1024);
    inject_loopback(&mut req);
    let resp = app.oneshot(req).await.unwrap();
    assert_ne!(
        resp.status(),
        StatusCode::PAYLOAD_TOO_LARGE,
        "1 MiB body must not be 413 (limit is 16 MiB); got {:?}",
        resp.status()
    );
}

#[tokio::test]
async fn body_15mib_returns_200_or_501() {
    let app = build_router(build_test_state(false));
    let mut req = push_request_with_body(15 * 1024 * 1024);
    inject_loopback(&mut req);
    let resp = app.oneshot(req).await.unwrap();
    assert_ne!(
        resp.status(),
        StatusCode::PAYLOAD_TOO_LARGE,
        "15 MiB body must not be 413 (limit is 16 MiB); got {:?}",
        resp.status()
    );
}

// ---------------------------------------------------------------------------
// Over-limit request: MUST be 413
// ---------------------------------------------------------------------------

#[tokio::test]
async fn body_17mib_returns_413() {
    let app = build_router(build_test_state(false));
    let mut req = push_request_with_body(17 * 1024 * 1024);
    inject_loopback(&mut req);
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::PAYLOAD_TOO_LARGE,
        "17 MiB body must be rejected with 413 by RequestBodyLimitLayer"
    );
}
