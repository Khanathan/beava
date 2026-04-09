//! HTTP management API: health endpoint, future pipeline CRUD.
//!
//! Runs on a separate port (default 6401) from the TCP hot path.
//! Phase 2 scope: /health only. Phase 4 adds /pipelines, /debug, /metrics.

use axum::{routing::get, Json, Router};
use tokio::net::TcpListener;

use super::tcp::SharedState;

async fn health() -> Json<serde_json::Value> {
    Json(serde_json::json!({"status": "ok"}))
}

/// Start the HTTP management server on the given address.
pub async fn run_http_server(addr: &str, _state: SharedState) -> Result<(), std::io::Error> {
    let app = Router::new().route("/health", get(health));
    let listener = TcpListener::bind(addr).await?;
    axum::serve(listener, app)
        .await
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))
}

/// Start the HTTP management server from a pre-bound listener (for tests).
pub async fn run_http_server_with_listener(
    listener: TcpListener,
    _state: SharedState,
) -> Result<(), std::io::Error> {
    let app = Router::new().route("/health", get(health));
    axum::serve(listener, app)
        .await
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))
}
