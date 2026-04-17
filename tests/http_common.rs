//! Shared test harness for Phase 45 HTTP integration tests.
//!
//! This file is NOT a test binary itself — it is included by the other
//! `tests/test_http_*.rs` files via `mod http_common;`. It provides:
//!
//! - `spawn_test_server(public_mode)` — binds 127.0.0.1:0, spawns axum
//! - `build_test_state(public_mode)` — in-memory SharedState with admin_token
//! - `inject_loopback(req)` — inserts ConnectInfo(127.0.0.1:1) extension
//! - `inject_peer(req, addr)` — inserts ConnectInfo for arbitrary peers

use std::net::SocketAddr;
use std::sync::Arc;

use axum::body::Body;
use axum::extract::ConnectInfo;
use axum::http::Request;
use tokio::task::JoinHandle;

use beava::engine::pipeline::PipelineEngine;
use beava::server::http::build_router;
use beava::server::tcp::{make_concurrent_state_full, BackfillTracker, SharedState};
use beava::state::store::StateStore;

/// Admin token used in all Phase 45 tests.
pub const TEST_ADMIN_TOKEN: &str = "test-admin";

/// Build a minimal in-memory SharedState.
/// `public_mode = true` mounts read routes on the public router.
pub fn build_test_state(public_mode: bool) -> SharedState {
    make_concurrent_state_full(
        PipelineEngine::new(),
        StateStore::new(),
        None,
        std::path::PathBuf::from("/tmp/beava-test-http-ingest.snapshot"),
        Arc::new(BackfillTracker::default()),
        false, // snapshot_enabled
        false, // event_log_enabled
        Some(TEST_ADMIN_TOKEN.to_string()),
        public_mode,
    )
}

/// Spawn a test server on 127.0.0.1:0 and return (addr, join_handle).
///
/// The JoinHandle runs indefinitely; callers should abort it at test end.
/// Uses `into_make_service_with_connect_info` so `ConnectInfo<SocketAddr>` is
/// populated for the `require_loopback_or_token` middleware.
pub async fn spawn_test_server(public_mode: bool) -> (SocketAddr, JoinHandle<()>) {
    let state = build_test_state(public_mode);
    let app = build_router(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let handle = tokio::spawn(async move {
        axum::serve(
            listener,
            app.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await
        .unwrap();
    });
    (addr, handle)
}

/// Inject a loopback ConnectInfo extension into a request.
///
/// Required when dispatching via `tower::ServiceExt::oneshot` because axum
/// does not populate ConnectInfo in that path.
pub fn inject_loopback(req: &mut Request<Body>) {
    let addr: SocketAddr = "127.0.0.1:1".parse().unwrap();
    req.extensions_mut().insert(ConnectInfo(addr));
}

/// Inject an arbitrary peer address ConnectInfo extension into a request.
pub fn inject_peer(req: &mut Request<Body>, addr: SocketAddr) {
    req.extensions_mut().insert(ConnectInfo(addr));
}

/// A non-loopback address to use in auth-rejection tests.
pub fn public_addr() -> SocketAddr {
    "8.8.8.8:12345".parse().unwrap()
}
