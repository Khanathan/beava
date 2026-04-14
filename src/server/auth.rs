//! Phase 20 (TRAC-05): admin-route gate.
//!
//! `require_loopback_or_token` is an axum middleware that admits requests
//! coming from a loopback IP (127.0.0.1 / ::1) unconditionally, and requests
//! from any other address only if they carry an `Authorization: Bearer <token>`
//! header matching `ConcurrentAppState.admin_token`. Everything else gets
//! 403 Forbidden.
//!
//! Mounted by `build_admin_router` in `src/server/http.rs` onto the routes
//! that mutate server state or leak operator internals: POST/DELETE
//! `/pipelines`, DELETE `/pipelines/{name}`, POST `/snapshot`, and every
//! `/debug/*` endpoint. Public read-only routes (`/health`, `/metrics`,
//! `/public/*`, `/`, `/static/*`) are mounted on the public router and
//! deliberately bypass this middleware.
//!
//! `ConnectInfo<SocketAddr>` is populated by axum when the HTTP server is
//! started via `into_make_service_with_connect_info::<SocketAddr>()` — see
//! `run_http_server_with_listener` in `http.rs`. Integration tests that use
//! `tower::ServiceExt::oneshot` inject the same value explicitly with
//! `req.extensions_mut().insert(ConnectInfo(addr))`.

use std::net::SocketAddr;

use axum::{
    body::Body,
    extract::{ConnectInfo, State},
    http::{header::AUTHORIZATION, HeaderMap, Request, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
    Json,
};
use serde_json::json;

use super::tcp::SharedState;

/// Admit loopback requests; admit non-loopback requests only when they carry
/// `Authorization: Bearer <admin_token>`. Reject everything else with 403.
///
/// Security properties (VALIDATION §ASVS V2/V4):
/// - Loopback bypass: `addr.ip().is_loopback()` covers both 127.0.0.0/8 and ::1
///   and cannot be spoofed by a remote client (the kernel stamps the peer
///   address on the TCP socket).
/// - Token bypass: requires `admin_token` to be configured AND an exact
///   `"Bearer "`-prefixed match. Missing token config → all non-loopback
///   requests rejected.
/// - Denial response: application/json body `{"error": ...}` so JSON-speaking
///   clients (the blog widget, smoke.sh) can parse it.
pub async fn require_loopback_or_token(
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    State(state): State<SharedState>,
    headers: HeaderMap,
    req: Request<Body>,
    next: Next,
) -> Response {
    if addr.ip().is_loopback() {
        return next.run(req).await;
    }
    if let Some(expected) = state.admin_token.as_deref() {
        if let Some(h) = headers.get(AUTHORIZATION).and_then(|v| v.to_str().ok()) {
            if h == format!("Bearer {}", expected) {
                return next.run(req).await;
            }
        }
    }
    (
        StatusCode::FORBIDDEN,
        Json(json!({"error": "admin route: loopback or bearer token required"})),
    )
        .into_response()
}

/// Phase 20: pure function for resolving the TCP listener bind address.
/// Precedence: CLI flag > environment variable > loopback default.
/// Called from `main.rs`; unit-tested in `tests/test_tcp_bind.rs`.
///
/// Loopback default is deliberate (CONTEXT.md D-02, TRAC-05): the TCP port
/// exposes `OP_PUSH`/`OP_SET`/`OP_MSET` with no authentication layer, so it
/// must never be reachable from the public internet unless the operator
/// explicitly opts in by passing `--tcp-bind 0.0.0.0`.
pub fn resolve_tcp_bind(env_val: Option<&str>, cli_val: Option<&str>, port: &str) -> String {
    let host = cli_val
        .or(env_val)
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .unwrap_or("127.0.0.1");
    format!("{}:{}", host, port)
}
