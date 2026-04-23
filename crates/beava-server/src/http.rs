//! HTTP surface — Phase 1 routes (`/health`, `/ready`).
//!
//! Phase 2+ will add handlers onto this router. Keep this file narrow:
//! route wiring only, no business logic.

use crate::register::{register_router, RegisterAppState};
use crate::registry_debug::{dev_apply_ops_router, registry_debug_router, RegistryDebugState};
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

/// Build the Phase 2 router.
/// Merges /health + /ready (Phase 1) with /register (Phase 2).
/// When `dev_endpoints_enabled` is true, also mounts GET /registry (Plan 02-06).
pub fn router(
    readiness: ReadinessFlag,
    registry: Arc<Registry>,
    dev_endpoints_enabled: bool,
) -> Router {
    let mut r = Router::new()
        .route("/health", get(health))
        .route("/ready", get(ready))
        .with_state(readiness)
        .merge(register_router(RegisterAppState {
            registry: registry.clone(),
        }));

    if dev_endpoints_enabled {
        r = r
            .merge(registry_debug_router(RegistryDebugState {
                registry: registry.clone(),
            }))
            .merge(dev_apply_ops_router(registry));
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
        router(ReadinessFlag::new(), registry, false)
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
        let r = router(flag, registry, false);
        let (status, body) = call(r, "/ready").await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(body, serde_json::json!({ "status": "starting" }));
    }

    #[tokio::test]
    async fn ready_returns_ok_after_flag_flip() {
        let flag = ReadinessFlag::new();
        flag.set_ready();
        let registry = Arc::new(Registry::new());
        let r = router(flag, registry, false);
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
        // Confirms the 3-arg router signature doesn't break Phase 1 health check
        let registry = Arc::new(Registry::new());
        let r = router(ReadinessFlag::new(), registry, false);
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
