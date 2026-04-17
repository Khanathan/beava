//! HTTP ingest + read endpoints (Phase 45).
//!
//! Handlers here are thin wrappers over `crate::server::tcp::handle_push_core_ex`
//! and `crate::server::tcp::handle_push_batch`. Do not duplicate ingest logic.

use std::time::Duration;

use axum::{
    Json, Router,
    extract::{DefaultBodyLimit, Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
};
use serde::Deserialize;
use serde_json::json;
use tower::ServiceBuilder;
use tower_http::limit::RequestBodyLimitLayer;
use tower_http::timeout::TimeoutLayer;

use crate::server::tcp::SharedState;

const DEFAULT_MAX_BODY_BYTES: usize = 16 * 1024 * 1024; // D-05: 16 MiB
const DEFAULT_TIMEOUT_SECS: u64 = 30;

fn max_body_bytes() -> usize {
    std::env::var("BEAVA_HTTP_MAX_BODY")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(DEFAULT_MAX_BODY_BYTES)
}

/// Register the six Phase 45 ingest + read routes onto the provided admin and
/// public routers. Returns the updated (public, admin) pair. Caller must attach
/// `require_loopback_or_token` to the admin router AFTER this fn returns
/// (matches `http.rs:1565-1568` pattern).
///
/// `public_mode = true` mounts /features/* and /streams[/*] on the public
/// router (per D-03 / HTTP-07). When false, reads stay admin-only.
///
/// # Middleware ordering (Pitfall 15)
///
/// `DefaultBodyLimit::disable()` MUST precede `RequestBodyLimitLayer::new`
/// otherwise axum's per-extractor 2 MiB cap silently applies first. The
/// `ServiceBuilder` stack is attached to write routes via `.layer(ingest_layers)`
/// BEFORE `.route_layer(require_loopback_or_token)` is applied by the caller,
/// preventing any auth-bypass on future route additions.
pub fn register_ingest_routes(
    public_router: Router<SharedState>,
    admin_router: Router<SharedState>,
    public_mode: bool,
) -> (Router<SharedState>, Router<SharedState>) {
    // Build the body-limit + timeout layer stack. Per Phase-level Pitfall A
    // `DefaultBodyLimit::disable()` MUST precede `RequestBodyLimitLayer::new`
    // otherwise axum's per-extractor 2 MiB cap silently applies.
    let ingest_layers = ServiceBuilder::new()
        .layer(DefaultBodyLimit::disable())
        .layer(RequestBodyLimitLayer::new(max_body_bytes()))
        // tower-http 0.6: TimeoutLayer::new is deprecated; use with_status_code
        // so the response is a proper 408 Request Timeout instead of 500.
        // Note: with_status_code takes (status_code, duration) order.
        .layer(TimeoutLayer::with_status_code(
            StatusCode::REQUEST_TIMEOUT,
            Duration::from_secs(DEFAULT_TIMEOUT_SECS),
        ));

    // Writes: always admin-mounted.
    let admin_router = admin_router
        .route("/push/{stream}", post(http_push_single))
        .route("/push-batch/{stream}", post(http_push_batch))
        .route("/push/{stream}/ndjson", post(http_push_ndjson))
        .layer(ingest_layers);

    // Reads: public if public_mode, else admin.
    let read_routes: Router<SharedState> = Router::new()
        .route("/features/{key}", get(http_get_features))
        .route("/streams", get(http_list_streams))
        .route("/streams/{name}", get(http_get_stream));

    if public_mode {
        (public_router.merge(read_routes), admin_router)
    } else {
        (public_router, admin_router.merge(read_routes))
    }
}

// -------- Handler stubs. Waves 1-2 replace the bodies. Keep the 501 so that
// TDD-RED tests fail with a clear signal, not a compile error. --------

// Wave 1 will read these fields; suppress dead_code until then.
#[allow(dead_code)]
#[derive(Debug, Deserialize)]
pub(crate) struct SyncQuery {
    #[serde(default)]
    pub sync: Option<u8>,
}

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
pub(crate) struct TableQuery {
    pub table: Option<String>,
}

async fn http_push_single(
    State(_state): State<SharedState>,
    Path(_stream): Path<String>,
    Query(_q): Query<SyncQuery>,
    Json(_payload): Json<serde_json::Value>,
) -> impl IntoResponse {
    stub_501("push_single")
}

async fn http_push_batch(
    State(_state): State<SharedState>,
    Path(_stream): Path<String>,
    Query(_q): Query<SyncQuery>,
    Json(_payload): Json<Vec<serde_json::Value>>,
) -> impl IntoResponse {
    stub_501("push_batch")
}

async fn http_push_ndjson(
    State(_state): State<SharedState>,
    Path(_stream): Path<String>,
    body: axum::body::Body,
) -> impl IntoResponse {
    let _ = body;
    stub_501("push_ndjson")
}

async fn http_get_features(
    State(_state): State<SharedState>,
    Path(_key): Path<String>,
    Query(_q): Query<TableQuery>,
) -> impl IntoResponse {
    stub_501("get_features")
}

async fn http_list_streams(State(_state): State<SharedState>) -> impl IntoResponse {
    stub_501("list_streams")
}

async fn http_get_stream(
    State(_state): State<SharedState>,
    Path(_name): Path<String>,
) -> impl IntoResponse {
    stub_501("get_stream")
}

fn stub_501(handler: &'static str) -> (StatusCode, Json<serde_json::Value>) {
    (
        StatusCode::NOT_IMPLEMENTED,
        Json(json!({
            "ok": false,
            "error": { "code": "not_implemented", "message": format!("handler {handler} not yet wired (Phase 45 Wave 0 stub)") }
        })),
    )
}
