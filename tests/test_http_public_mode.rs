//! Phase 45 — HTTP-07: public-mode read-route placement tests.
//!
//! Wave 0:
//! - `writes_always_admin` runs against stubs — write routes are always
//!   admin-mounted regardless of public_mode, confirmed by 401 off-loopback.
//! - The remaining tests are stubbed until Wave 2 has real read handlers.

mod http_common;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use tower::ServiceExt;

use beava::server::http::build_router;
use http_common::{build_test_state, inject_peer, public_addr, TEST_ADMIN_TOKEN};

// ---------------------------------------------------------------------------
// Wave 0 passing: writes are always admin-only regardless of public_mode
// ---------------------------------------------------------------------------

#[tokio::test]
async fn writes_always_admin() {
    let write_routes = [
        ("POST", "/push/s"),
        ("POST", "/push-batch/s"),
        ("POST", "/push/s/ndjson"),
    ];
    // Even with public_mode=true, write routes must still be 401 off-loopback
    for (method, path) in &write_routes {
        let app = build_router(build_test_state(true)); // public_mode = true
        let mut req = Request::builder()
            .method(*method)
            .uri(*path)
            .header("content-type", "application/json")
            .body(Body::from("{}"))
            .unwrap();
        inject_peer(&mut req, public_addr());
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(
            resp.status(),
            StatusCode::UNAUTHORIZED,
            "write route {} {} must be 401 off-loopback even in public_mode (got {:?})",
            method,
            path,
            resp.status()
        );
    }
}

// ---------------------------------------------------------------------------
// Wave 2: HTTP-07 public-mode routing
// ---------------------------------------------------------------------------

#[tokio::test]
async fn reads_on_public_router_when_public_mode() {
    // With public_mode=true, read routes are on the public router — no auth needed.
    // Off-loopback, no Authorization header.

    // GET /features/{key}: key doesn't exist → 404 key_not_found (not 401).
    let app = build_router(build_test_state(true));
    let mut req = Request::builder()
        .method("GET")
        .uri("/features/nokeyneeded")
        .body(Body::empty())
        .unwrap();
    inject_peer(&mut req, public_addr());
    let resp = app.oneshot(req).await.unwrap();
    assert_ne!(
        resp.status(),
        StatusCode::UNAUTHORIZED,
        "GET /features/* must not be 401 when public_mode=true (got {:?})",
        resp.status()
    );
    // Must be 404 key_not_found, not an auth rejection.
    assert_eq!(
        resp.status(),
        StatusCode::NOT_FOUND,
        "GET /features/unknown_key must 404 when public_mode=true and key absent"
    );

    // GET /streams: no key, no auth → 200 with list.
    let app2 = build_router(build_test_state(true));
    let mut req2 = Request::builder()
        .method("GET")
        .uri("/streams")
        .body(Body::empty())
        .unwrap();
    inject_peer(&mut req2, public_addr());
    let resp2 = app2.oneshot(req2).await.unwrap();
    assert_eq!(
        resp2.status(),
        StatusCode::OK,
        "GET /streams must be 200 when public_mode=true (no auth)"
    );
    let body = axum::body::to_bytes(resp2.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["ok"], serde_json::Value::Bool(true));
}

#[tokio::test]
async fn reads_on_admin_router_when_not_public_mode() {
    // With public_mode=false, read routes are on the admin router — 401 off-loopback
    // without auth token.
    let read_routes = ["/features/anykey", "/streams", "/streams/anyname"];
    for path in &read_routes {
        let app = build_router(build_test_state(false)); // public_mode = false
        let mut req = Request::builder()
            .method("GET")
            .uri(*path)
            .body(Body::empty())
            .unwrap();
        inject_peer(&mut req, public_addr());
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(
            resp.status(),
            StatusCode::UNAUTHORIZED,
            "GET {} must be 401 off-loopback when public_mode=false (got {:?})",
            path,
            resp.status()
        );
    }
}
