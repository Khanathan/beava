//! Phase 45 Wave 0: per-endpoint auth sweep.
//!
//! Verifies that `require_loopback_or_token` correctly gates all three write
//! routes and that read routes are ungated when `public_mode = true`.
//!
//! This test MUST pass at end of Wave 0 because the stubs already sit behind
//! the auth layer — 401 is returned before the handler even runs.

mod http_common;

use axum::body::Body;
use axum::http::{Method, Request, StatusCode};
use tower::ServiceExt;

use beava::server::http::build_router;
use http_common::{build_test_state, inject_loopback, inject_peer, public_addr, TEST_ADMIN_TOKEN};

/// Build a oneshot request with optional Authorization header.
fn make_request(method: &str, path: &str, bearer: Option<&str>) -> Request<Body> {
    let mut b = Request::builder()
        .method(method)
        .uri(path)
        .header("content-type", "application/json");
    if let Some(tok) = bearer {
        b = b.header("authorization", format!("Bearer {}", tok));
    }
    b.body(Body::from("{}")).unwrap()
}

// ---------------------------------------------------------------------------
// Write routes — must be admin-gated (401 off-loopback, not-401 on loopback)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_auth_sweep_all_ingest_routes() {
    let write_routes = [
        (Method::POST, "/push/teststream"),
        (Method::POST, "/push-batch/teststream"),
        (Method::POST, "/push/teststream/ndjson"),
    ];

    // --- Write routes: off-loopback without token → 401 ---
    for (method, path) in &write_routes {
        let app = build_router(build_test_state(false));
        let mut req = make_request(method.as_str(), path, None);
        inject_peer(&mut req, public_addr());
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(
            resp.status(),
            StatusCode::UNAUTHORIZED,
            "expected 401 UNAUTHORIZED for off-loopback {} {} (no token)",
            method,
            path
        );
    }

    // --- Write routes: loopback without token → NOT 401 (stub = 501 or other) ---
    for (method, path) in &write_routes {
        let app = build_router(build_test_state(false));
        let mut req = make_request(method.as_str(), path, None);
        inject_loopback(&mut req);
        let resp = app.oneshot(req).await.unwrap();
        assert_ne!(
            resp.status(),
            StatusCode::UNAUTHORIZED,
            "loopback {} {} must NOT be 401 (got {:?})",
            method,
            path,
            resp.status()
        );
    }

    // --- Write routes: off-loopback with valid token → NOT 401 ---
    for (method, path) in &write_routes {
        let app = build_router(build_test_state(false));
        let mut req = make_request(method.as_str(), path, Some(TEST_ADMIN_TOKEN));
        inject_peer(&mut req, public_addr());
        let resp = app.oneshot(req).await.unwrap();
        assert_ne!(
            resp.status(),
            StatusCode::UNAUTHORIZED,
            "valid-token {} {} must NOT be 401 (got {:?})",
            method,
            path,
            resp.status()
        );
    }

    // --- Read routes on public_mode=true server: no auth → NOT 401 ---
    let read_routes = [
        (Method::GET, "/features/alice"),
        (Method::GET, "/streams"),
        (Method::GET, "/streams/teststream"),
    ];
    for (method, path) in &read_routes {
        let app = build_router(build_test_state(true)); // public_mode = true
        let mut req = make_request(method.as_str(), path, None);
        inject_peer(&mut req, public_addr());
        let resp = app.oneshot(req).await.unwrap();
        assert_ne!(
            resp.status(),
            StatusCode::UNAUTHORIZED,
            "public_mode read {} {} must NOT be 401 (got {:?})",
            method,
            path,
            resp.status()
        );
    }

    // --- Read routes on public_mode=false server: no auth → 401 ---
    for (method, path) in &read_routes {
        let app = build_router(build_test_state(false)); // public_mode = false
        let mut req = make_request(method.as_str(), path, None);
        inject_peer(&mut req, public_addr());
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(
            resp.status(),
            StatusCode::UNAUTHORIZED,
            "admin-mode read {} {} must be 401 off-loopback (got {:?})",
            method,
            path,
            resp.status()
        );
    }
}
