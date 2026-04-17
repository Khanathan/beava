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
use http_common::{build_test_state, inject_peer, public_addr};

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
// Wave 2 stubs — filled by 45-03
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "Phase 45 Wave 2: http_get_features / http_list_streams handler stubs"]
async fn reads_on_public_router_when_public_mode() {
    panic!("MISSING: Wave 2 must verify reads are public when public_mode=true (HTTP-07)");
}

#[tokio::test]
#[ignore = "Phase 45 Wave 2: http_get_features / http_list_streams handler stubs"]
async fn reads_on_admin_router_when_not_public_mode() {
    panic!("MISSING: Wave 2 must verify reads are admin-gated when public_mode=false (HTTP-07)");
}
