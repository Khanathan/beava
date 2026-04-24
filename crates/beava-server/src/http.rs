//! HTTP surface — Phase 1 routes (`/health`, `/ready`).
//!
//! Phase 2+ will add handlers onto this router. Keep this file narrow:
//! route wiring only, no business logic.

use crate::feature_query::{feature_query_router, FeatureQueryState};
use crate::push::push_router;
use crate::register::{register_router, RegisterAppState};
use crate::registry_debug::{
    dev_apply_events_router, dev_apply_ops_router, registry_debug_router, DevAggState,
    RegistryDebugState,
};
use crate::AppState;
use axum::{extract::State, http::StatusCode, response::IntoResponse, routing::get, Json, Router};
use beava_core::registry::Registry;
use serde_json::json;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

/// Shared readiness flag. Clone-cheap (Arc'd AtomicBool). Handed to the /ready handler
/// as axum state. In Phase 1 we flip it after a hardcoded delay; in Phase 5 the
/// recovery path will flip it once snapshot + WAL replay complete.
#[derive(Debug, Clone, Default)]
pub struct ReadinessFlag(Arc<AtomicBool>);

impl ReadinessFlag {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn set_ready(&self) {
        self.0.store(true, Ordering::Release);
    }

    pub fn is_ready(&self) -> bool {
        self.0.load(Ordering::Acquire)
    }
}

/// Build the Phase 2+ router.
/// Merges /health + /ready (Phase 1) with /register (Phase 2).
/// When `dev_endpoints_enabled` is true, also mounts GET /registry (Plan 02-06),
/// POST /dev/apply_ops, POST /dev/apply_events, GET /get/:feature/:key, POST /get.
///
/// `dev_agg_state`: if `Some`, the provided `DevAggState` is shared between
/// `/dev/apply_events` and `/get` so queries reflect pushed events immediately.
/// If `None`, a fresh `DevAggState` is constructed from `registry` (backward
/// compat for callers that don't need shared state).
pub fn router(
    readiness: ReadinessFlag,
    registry: Arc<Registry>,
    dev_endpoints_enabled: bool,
    dev_agg_state: Option<DevAggState>,
) -> Router {
    router_with_push(
        readiness,
        registry,
        dev_endpoints_enabled,
        dev_agg_state,
        None,
    )
}

/// Phase 6 Plan 03 extended router: when `app_state` is `Some`, mounts
/// `POST /push/:event_name`. The Phase 1 callers that pre-date AppState pass
/// `None` and get the historical behavior unchanged.
pub fn router_with_push(
    readiness: ReadinessFlag,
    registry: Arc<Registry>,
    dev_endpoints_enabled: bool,
    dev_agg_state: Option<DevAggState>,
    app_state: Option<Arc<AppState>>,
) -> Router {
    let wal_sink_for_register = app_state.as_ref().map(|a| a.wal_sink.clone());
    let mut r = Router::new()
        .route("/health", get(health))
        .route("/ready", get(ready))
        .with_state(readiness)
        .merge(register_router(RegisterAppState {
            registry: registry.clone(),
            wal_sink: wal_sink_for_register,
        }));

    if let Some(app) = app_state.as_ref() {
        r = r.merge(push_router(Arc::clone(app)));
        // Phase 11.5 — push-table, retract, and table-get sit alongside
        // /push (production API, not gated by dev_endpoints).
        r = r.merge(crate::temporal_http::temporal_router(Arc::clone(app)));
    }

    if dev_endpoints_enabled {
        // Prefer the AppState's DevAggState so /push + /get/… share state.
        let agg_state = match (dev_agg_state, app_state.as_ref()) {
            (Some(s), _) => s,
            (None, Some(app)) => app.dev_agg.clone(),
            (None, None) => DevAggState::new(registry.clone()),
        };
        r = r
            .merge(registry_debug_router(RegistryDebugState {
                registry: registry.clone(),
            }))
            .merge(dev_apply_ops_router(registry.clone()))
            .merge(dev_apply_events_router(agg_state.clone()))
            .merge(feature_query_router(FeatureQueryState::new(agg_state)));
    }
    r
}

async fn health() -> impl IntoResponse {
    (StatusCode::OK, Json(json!({ "status": "ok" })))
}

async fn ready(State(flag): State<ReadinessFlag>) -> impl IntoResponse {
    if flag.is_ready() {
        (StatusCode::OK, Json(json!({ "status": "ready" })))
    } else {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({ "status": "starting" })),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use http_body_util::BodyExt;
    use tower::ServiceExt;

    async fn call(router: Router, uri: &str) -> (StatusCode, serde_json::Value) {
        let resp = router
            .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
            .await
            .expect("oneshot");
        let status = resp.status();
        let bytes = resp
            .into_body()
            .collect()
            .await
            .expect("collect")
            .to_bytes();
        let json: serde_json::Value = if bytes.is_empty() {
            serde_json::Value::Null
        } else {
            serde_json::from_slice(&bytes).expect("json parse")
        };
        (status, json)
    }

    fn test_router() -> Router {
        let registry = Arc::new(Registry::new());
        router(ReadinessFlag::new(), registry, false, None)
    }

    #[tokio::test]
    async fn health_returns_ok() {
        let r = test_router();
        let (status, body) = call(r, "/health").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, serde_json::json!({ "status": "ok" }));
    }

    #[tokio::test]
    async fn ready_returns_starting_before_flag_flip() {
        let flag = ReadinessFlag::new();
        let registry = Arc::new(Registry::new());
        let r = router(flag, registry, false, None);
        let (status, body) = call(r, "/ready").await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(body, serde_json::json!({ "status": "starting" }));
    }

    #[tokio::test]
    async fn ready_returns_ok_after_flag_flip() {
        let flag = ReadinessFlag::new();
        flag.set_ready();
        let registry = Arc::new(Registry::new());
        let r = router(flag, registry, false, None);
        let (status, body) = call(r, "/ready").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, serde_json::json!({ "status": "ready" }));
    }

    #[tokio::test]
    async fn nonexistent_route_returns_404() {
        let r = test_router();
        let (status, _body) = call(r, "/nope").await;
        assert_eq!(status, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn router_accepts_registry_state() {
        // Confirms the 4-arg router signature doesn't break Phase 1 health check
        let registry = Arc::new(Registry::new());
        let r = router(ReadinessFlag::new(), registry, false, None);
        let (status, body) = call(r, "/health").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["status"], "ok");
    }

    #[test]
    fn readiness_flag_is_clone_cheap_and_shares_state() {
        let a = ReadinessFlag::new();
        let b = a.clone();
        assert!(!a.is_ready());
        assert!(!b.is_ready());
        b.set_ready();
        assert!(a.is_ready(), "clones must share inner state via Arc");
    }
}
