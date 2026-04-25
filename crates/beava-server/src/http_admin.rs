//! Admin HTTP plane — tokio/axum app on a dedicated port.
//!
//! Handles: `/health`, `/ready`, `/metrics`, `/registry`.
//! Read-only access to shared state via `Arc<RwLock<RegistrySnapshot>>`.
//! No write-back path — the event-plane state is updated only by the
//! hand-rolled event loop (Plan 18-01, D-01).
//!
//! Plan 18-07: feature flag removed; this module is now unconditionally compiled.

use axum::{
    extract::State,
    http::{header::HeaderName, HeaderValue, StatusCode},
    middleware,
    middleware::Next,
    response::IntoResponse,
    routing::get,
    Json, Router,
};
use std::sync::{Arc, RwLock};
use tokio::net::TcpListener;

/// Axum middleware: stamps every admin response with `X-Runtime: tokio`.
///
/// Plan 18-07 (Task 7.2): identifies admin endpoints as tokio-served (D-01,
/// 18-CONTEXT.md). All data-plane routes return `X-Runtime: hand-rolled`.
async fn stamp_tokio_header(
    req: axum::http::Request<axum::body::Body>,
    next: Next,
) -> impl IntoResponse {
    let mut response = next.run(req).await;
    response.headers_mut().insert(
        HeaderName::from_static("x-runtime"),
        HeaderValue::from_static("tokio"),
    );
    response
}

// ─── Shared admin state ───────────────────────────────────────────────────────

/// A point-in-time snapshot of registry metadata for admin read access.
/// Updated on every successful register call from the event-plane.
#[derive(Clone, Debug, Default)]
pub struct RegistrySnapshot {
    /// Number of registered aggregation nodes.
    pub node_count: usize,
    /// Registry version (monotonic counter).
    pub version: u64,
}

pub type SharedRegistrySnapshot = Arc<RwLock<RegistrySnapshot>>;

// ─── Router ───────────────────────────────────────────────────────────────────

#[derive(Clone)]
struct AdminState {
    snapshot: SharedRegistrySnapshot,
}

pub fn admin_router(snapshot: SharedRegistrySnapshot) -> Router {
    let state = AdminState { snapshot };
    Router::new()
        .route("/health", get(health_handler))
        .route("/ready", get(ready_handler))
        .route("/metrics", get(metrics_handler))
        .route("/registry", get(registry_handler))
        .with_state(state)
        .layer(middleware::from_fn(stamp_tokio_header))
}

// ─── Handlers ─────────────────────────────────────────────────────────────────

async fn health_handler() -> impl IntoResponse {
    (StatusCode::OK, Json(serde_json::json!({"status": "ok"})))
}

async fn ready_handler() -> impl IntoResponse {
    // Always ready once the admin server is running; event-plane recovery
    // happens before ServerV18::bind returns.
    (StatusCode::OK, Json(serde_json::json!({"status": "ready"})))
}

async fn metrics_handler(State(state): State<AdminState>) -> impl IntoResponse {
    let snap = state
        .snapshot
        .read()
        .unwrap_or_else(|p| p.into_inner())
        .clone();

    // Prometheus exposition format (text/plain; version=0.0.4).
    // Plan 18-04.6 Task 4.6.5: add beava_runtime_kind gauge to identify
    // which runtime is serving the data plane.
    let body = format!(
        "# HELP beava_registry_version Registry version (monotonic).\n\
         # TYPE beava_registry_version gauge\n\
         beava_registry_version {registry_version}\n\
         # HELP beava_node_count Number of registered aggregation nodes.\n\
         # TYPE beava_node_count gauge\n\
         beava_node_count {node_count}\n\
         # HELP beava_runtime_kind Data-plane runtime (1=active). Labels: runtime.\n\
         # TYPE beava_runtime_kind gauge\n\
         beava_runtime_kind{{runtime=\"mio\"}} 1\n",
        registry_version = snap.version,
        node_count = snap.node_count,
    );

    (
        StatusCode::OK,
        [(axum::http::header::CONTENT_TYPE, "text/plain; version=0.0.4; charset=utf-8")],
        body,
    )
}

async fn registry_handler(State(state): State<AdminState>) -> impl IntoResponse {
    let snap = state
        .snapshot
        .read()
        .unwrap_or_else(|p| p.into_inner())
        .clone();
    (
        StatusCode::OK,
        Json(serde_json::json!({
            "version": snap.version,
            "node_count": snap.node_count,
        })),
    )
}

// ─── Bound admin server ───────────────────────────────────────────────────────

/// A bound admin HTTP server handle. Holds the local address and a shutdown
/// channel. Dropped when `ServerV18` shuts down.
pub struct BoundAdminServer {
    pub local_addr: std::net::SocketAddr,
    shutdown_tx: tokio::sync::oneshot::Sender<()>,
    join: tokio::task::JoinHandle<()>,
}

impl BoundAdminServer {
    /// Bind and start the admin axum server on `addr`. Returns immediately after
    /// the listener is bound; the serve loop runs on a tokio task.
    pub async fn bind(
        addr: std::net::SocketAddr,
        snapshot: SharedRegistrySnapshot,
    ) -> std::io::Result<Self> {
        let listener = TcpListener::bind(addr).await?;
        let local_addr = listener.local_addr()?;
        let app = admin_router(snapshot);
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        let shutdown_signal = async move {
            let _ = rx.await;
        };
        let join = tokio::spawn(async move {
            let _ = axum::serve(listener, app)
                .with_graceful_shutdown(shutdown_signal)
                .await;
        });
        Ok(Self {
            local_addr,
            shutdown_tx: tx,
            join,
        })
    }

    /// Gracefully stop the admin server.
    pub async fn shutdown(self) {
        let _ = self.shutdown_tx.send(());
        let _ = self.join.await;
    }
}
