//! HTTP surface — Phase 1 routes (`/health`, `/ready`).
//!
//! Phase 2+ will add handlers onto this router. Keep this file narrow:
//! route wiring only, no business logic.

use axum::{extract::State, http::StatusCode, response::IntoResponse, routing::get, Json, Router};
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

/// Build the Phase 1 router. Phase 2+ will merge additional routers into this.
pub fn router(readiness: ReadinessFlag) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/ready", get(ready))
        .with_state(readiness)
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

    #[tokio::test]
    async fn health_returns_ok() {
        let r = router(ReadinessFlag::new());
        let (status, body) = call(r, "/health").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, serde_json::json!({ "status": "ok" }));
    }

    #[tokio::test]
    async fn ready_returns_starting_before_flag_flip() {
        let flag = ReadinessFlag::new();
        let r = router(flag);
        let (status, body) = call(r, "/ready").await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(body, serde_json::json!({ "status": "starting" }));
    }

    #[tokio::test]
    async fn ready_returns_ok_after_flag_flip() {
        let flag = ReadinessFlag::new();
        flag.set_ready();
        let r = router(flag);
        let (status, body) = call(r, "/ready").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, serde_json::json!({ "status": "ready" }));
    }

    #[tokio::test]
    async fn nonexistent_route_returns_404() {
        let r = router(ReadinessFlag::new());
        let (status, _body) = call(r, "/nope").await;
        assert_eq!(status, StatusCode::NOT_FOUND);
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
