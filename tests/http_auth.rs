//! Phase 45 Wave 2: exhaustive per-route × per-case auth sweep (HTTP-06 / D-19).
//!
//! Covers all 6 HTTP routes across 4 case types:
//!   (a) off-loopback, no token           → 401
//!   (b) loopback, no token               → NOT 401
//!   (c) off-loopback, valid Bearer token → NOT 401
//!   (d) public_mode=true, off-loopback   → NOT 401 (reads only; writes still 401)
//!
//! This test is the canonical Pitfall-15 regression guard. If a future refactor
//! moves any write route BELOW `.route_layer(require_loopback_or_token)`, exactly
//! one of the assertions in `test_writes_reject_offloopback_noauth` will fail with
//! a message like:
//!   "route POST /push/s expected 401 got 200 OK"
//!
//! 7 test functions × 3-6 routes each = every route covered by ≥ 3 assertions.

mod http_common;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use tower::ServiceExt;

use beava::server::http::build_router;
use http_common::{build_test_state, inject_loopback, inject_peer, public_addr, TEST_ADMIN_TOKEN};

// ---------------------------------------------------------------------------
// Route definitions
// ---------------------------------------------------------------------------

const WRITE_ROUTES: &[(&str, &str)] = &[
    ("POST", "/push/s"),
    ("POST", "/push-batch/s"),
    ("POST", "/push/s/ndjson"),
];

const READ_ROUTES: &[(&str, &str)] = &[
    ("GET", "/features/k"),
    ("GET", "/streams"),
    ("GET", "/streams/s"),
];

// ---------------------------------------------------------------------------
// Request builder helpers
// ---------------------------------------------------------------------------

fn make_req(method: &str, path: &str, bearer: Option<&str>) -> Request<Body> {
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
// Test 1: Write routes — off-loopback, no token → 401
// ---------------------------------------------------------------------------

/// Structural regression guard (Pitfall 15): if a write route is mounted
/// AFTER `.route_layer(require_loopback_or_token)`, this test will catch it
/// because that route will let the off-loopback unauthenticated request through.
#[tokio::test]
async fn test_writes_reject_offloopback_noauth() {
    for (method, path) in WRITE_ROUTES {
        let app = build_router(build_test_state(false));
        let mut req = make_req(method, path, None);
        inject_peer(&mut req, public_addr());
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(
            resp.status(),
            StatusCode::UNAUTHORIZED,
            "route {} {} expected 401 (off-loopback, no token) got {}",
            method,
            path,
            resp.status()
        );
    }
}

// ---------------------------------------------------------------------------
// Test 2: Write routes — loopback, no token → NOT 401
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_writes_allow_loopback_noauth() {
    for (method, path) in WRITE_ROUTES {
        let app = build_router(build_test_state(false));
        let mut req = make_req(method, path, None);
        inject_loopback(&mut req);
        let resp = app.oneshot(req).await.unwrap();
        assert_ne!(
            resp.status(),
            StatusCode::UNAUTHORIZED,
            "route {} {} must NOT be 401 from loopback (got {})",
            method,
            path,
            resp.status()
        );
    }
}

// ---------------------------------------------------------------------------
// Test 3: Write routes — off-loopback, valid Bearer token → NOT 401
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_writes_allow_offloopback_withtoken() {
    for (method, path) in WRITE_ROUTES {
        let app = build_router(build_test_state(false));
        let mut req = make_req(method, path, Some(TEST_ADMIN_TOKEN));
        inject_peer(&mut req, public_addr());
        let resp = app.oneshot(req).await.unwrap();
        assert_ne!(
            resp.status(),
            StatusCode::UNAUTHORIZED,
            "route {} {} with valid token must NOT be 401 (got {})",
            method,
            path,
            resp.status()
        );
    }
}

// ---------------------------------------------------------------------------
// Test 4: Read routes — off-loopback, no token, public_mode=false → 401
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_reads_reject_offloopback_noauth_when_not_public() {
    for (method, path) in READ_ROUTES {
        let app = build_router(build_test_state(false)); // public_mode = false
        let mut req = make_req(method, path, None);
        inject_peer(&mut req, public_addr());
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(
            resp.status(),
            StatusCode::UNAUTHORIZED,
            "route {} {} in admin-mode must be 401 off-loopback no-auth (got {})",
            method,
            path,
            resp.status()
        );
    }
}

// ---------------------------------------------------------------------------
// Test 5: Read routes — off-loopback, no token, public_mode=true → NOT 401
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_reads_allow_offloopback_noauth_when_public() {
    for (method, path) in READ_ROUTES {
        let app = build_router(build_test_state(true)); // public_mode = true
        let mut req = make_req(method, path, None);
        inject_peer(&mut req, public_addr());
        let resp = app.oneshot(req).await.unwrap();
        assert_ne!(
            resp.status(),
            StatusCode::UNAUTHORIZED,
            "route {} {} in public_mode must NOT be 401 off-loopback no-auth (got {})",
            method,
            path,
            resp.status()
        );
    }
}

// ---------------------------------------------------------------------------
// Test 6: Read routes — loopback, no token, public_mode=false → NOT 401
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_reads_allow_loopback_even_when_not_public() {
    for (method, path) in READ_ROUTES {
        let app = build_router(build_test_state(false)); // public_mode = false
        let mut req = make_req(method, path, None);
        inject_loopback(&mut req);
        let resp = app.oneshot(req).await.unwrap();
        assert_ne!(
            resp.status(),
            StatusCode::UNAUTHORIZED,
            "route {} {} from loopback must NOT be 401 even in admin mode (got {})",
            method,
            path,
            resp.status()
        );
    }
}

// ---------------------------------------------------------------------------
// Test 7: Write routes — public_mode=true, off-loopback, no token → 401
//
// Writes are ALWAYS admin-gated regardless of public_mode. A public server
// must not expose unauthenticated write access.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_writes_always_admin_even_when_public() {
    for (method, path) in WRITE_ROUTES {
        let app = build_router(build_test_state(true)); // public_mode = true
        let mut req = make_req(method, path, None);
        inject_peer(&mut req, public_addr());
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(
            resp.status(),
            StatusCode::UNAUTHORIZED,
            "write route {} {} in public_mode must still be 401 off-loopback no-auth (got {})",
            method,
            path,
            resp.status()
        );
    }
}
