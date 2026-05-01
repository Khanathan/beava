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
use beava_core::agg_state::{
    BucketReclaimCounter, ColdEntityEvictionCounter, EntityCountResidentSnapshot, EntropyStateWrap,
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
    // Plan 19.2-06 (D-05a): add beava_entropy_categories_capped_total counter.
    // Plan 12.8-06: add 5 new memory-governance metric families
    //   - beava_cold_entity_evictions_total      (counter; Plan 03 cold-TTL evictions)
    //   - beava_lifetime_op_cap_hit_total        (counter; entropy categories_capped + future top_k/histogram)
    //   - beava_entity_count_resident            (gauge; resident entity count snapshot)
    //   - beava_bucket_reclaim_total             (counter; WindowedOp::evict_oldest_bucket firings)
    //   - beava_bytes_per_entity_p99             (gauge; static v0 estimate per PROJECT.md)
    // All counters are UNLABELED in v0 — per-source labels deferred to v0.0.x per
    // Plan 06 Step 3 (`Claude's Discretion` + the v0 simplicity bias).
    let entropy_capped = EntropyStateWrap::categories_capped_count();
    let cold_evictions = ColdEntityEvictionCounter::count();
    let bucket_reclaims = BucketReclaimCounter::count();
    let entity_count = EntityCountResidentSnapshot::load();
    // Plan 12.8-06: bytes_per_entity_p99 is a STATIC v0 placeholder per
    // PROJECT.md "Memory" budget line ("~7KB per entity for a rich 30-feature
    // pack"). A periodic re-sampler is out of scope for v0; if Phase 13
    // ship-gate needs accuracy, a follow-up plan upgrades to dynamic sampling.
    const BYTES_PER_ENTITY_P99_V0_PLACEHOLDER: u64 = 7000;
    // For v0 the lifetime_op_cap_hit aggregate counter wraps the existing
    // entropy categories_capped_count. top_k displacement + histogram bucket
    // drop hooks aren't currently surfaced as inc()-able sites; they re-join
    // here when the operator internals expose them in v0.0.x.
    let op_cap_hits = entropy_capped;

    let body = format!(
        "# HELP beava_registry_version Registry version (monotonic).\n\
         # TYPE beava_registry_version gauge\n\
         beava_registry_version {registry_version}\n\
         # HELP beava_node_count Number of registered aggregation nodes.\n\
         # TYPE beava_node_count gauge\n\
         beava_node_count {node_count}\n\
         # HELP beava_runtime_kind Data-plane runtime (1=active). Labels: runtime.\n\
         # TYPE beava_runtime_kind gauge\n\
         beava_runtime_kind{{runtime=\"mio\"}} 1\n\
         # HELP beava_entropy_categories_capped_total Total new-category insertions dropped due to max_categories cap.\n\
         # TYPE beava_entropy_categories_capped_total counter\n\
         beava_entropy_categories_capped_total {entropy_capped}\n\
         # HELP beava_cold_entity_evictions_total Total cold-TTL entity evictions since process start (Plan 12.8-03). Increments when an entity arrives after `now_ms - last_seen_ms > cold_after_ms`.\n\
         # TYPE beava_cold_entity_evictions_total counter\n\
         beava_cold_entity_evictions_total {cold_evictions}\n\
         # HELP beava_lifetime_op_cap_hit_total Total cap-hit events across lifetime aggregation operators (Plan 12.8-06). Currently aggregates entropy `categories_capped`; top_k displacements + histogram bucket drops join when their internals expose hooks.\n\
         # TYPE beava_lifetime_op_cap_hit_total counter\n\
         beava_lifetime_op_cap_hit_total {op_cap_hits}\n\
         # HELP beava_entity_count_resident Current resident entity count across all sources / aggregations (Plan 12.8-06). Snapshot refreshed by the apply path post-update — admin reads via process-static atomic load (zero-lock).\n\
         # TYPE beava_entity_count_resident gauge\n\
         beava_entity_count_resident {entity_count}\n\
         # HELP beava_bucket_reclaim_total Total trailing-bucket evictions on windowed operators (Plan 12.8-06). Increments on each WindowedOp::evict_oldest_bucket call (AGG-CORE-09 64-bucket cap).\n\
         # TYPE beava_bucket_reclaim_total counter\n\
         beava_bucket_reclaim_total {bucket_reclaims}\n\
         # HELP beava_bytes_per_entity_p99 Static v0 estimate of per-entity memory footprint (~7 KB for a rich 30-feature pack per PROJECT.md). Phase 13 ship-gate may upgrade to dynamic sampling.\n\
         # TYPE beava_bytes_per_entity_p99 gauge\n\
         beava_bytes_per_entity_p99 {bytes_per_entity_p99}\n",
        registry_version = snap.version,
        node_count = snap.node_count,
        entropy_capped = entropy_capped,
        cold_evictions = cold_evictions,
        op_cap_hits = op_cap_hits,
        entity_count = entity_count,
        bucket_reclaims = bucket_reclaims,
        bytes_per_entity_p99 = BYTES_PER_ENTITY_P99_V0_PLACEHOLDER,
    );

    (
        StatusCode::OK,
        [(
            axum::http::header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )],
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
