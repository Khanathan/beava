//! GET /registry — dev-only endpoint that dumps the full registry snapshot.
//!
//! Only mounted when `dev_endpoints_enabled = true` in the router call (which
//! reads `BEAVA_DEV_ENDPOINTS=1` from the environment at `Server::bind` time).
//! Default posture: route is NOT mounted → clients receive 404.

use axum::{extract::State, routing::get, Json, Router};
use beava_core::registry::{DerivationDescriptor, EventDescriptor, Registry, TableDescriptor};
use serde::Serialize;
use std::collections::BTreeMap;
use std::sync::Arc;

/// Axum state for the debug router.
#[derive(Clone)]
pub struct RegistryDebugState {
    pub registry: Arc<Registry>,
}

/// Full registry dump. `_dev_only: true` is a permanent wire sentinel so SDK
/// authors know this endpoint is unstable.
#[derive(Debug, Serialize)]
struct RegistryDump {
    version: u64,
    events: BTreeMap<String, EventDescriptor>,
    tables: BTreeMap<String, TableDescriptor>,
    derivations: BTreeMap<String, DerivationDescriptor>,
    _dev_only: bool, // always true
}

/// Build the GET /registry sub-router.  Caller merges this into the main
/// router conditionally based on `dev_endpoints_enabled`.
pub fn registry_debug_router(state: RegistryDebugState) -> Router {
    Router::new()
        .route("/registry", get(get_registry))
        .with_state(state)
}

async fn get_registry(State(state): State<RegistryDebugState>) -> Json<serde_json::Value> {
    let inner = state.registry.snapshot();
    let dump = RegistryDump {
        version: inner.version,
        events: inner.events,
        tables: inner.tables,
        derivations: inner.derivations,
        _dev_only: true,
    };
    Json(serde_json::to_value(dump).unwrap())
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::http::{router, ReadinessFlag};
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use beava_core::registry::{EventDescriptor, Registry};
    use beava_core::registry_diff::PayloadNode;
    use beava_core::schema::{EventSchema, FieldType};
    use http_body_util::BodyExt;
    use std::collections::BTreeMap;
    use tower::ServiceExt;

    async fn get(r: axum::Router, uri: &str) -> (StatusCode, serde_json::Value) {
        let resp = r
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
        if bytes.is_empty() {
            (status, serde_json::Value::Null)
        } else {
            let json: serde_json::Value =
                serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null);
            (status, json)
        }
    }

    fn minimal_event_descriptor(name: &str) -> EventDescriptor {
        let mut fields = BTreeMap::new();
        fields.insert("event_time".to_string(), FieldType::I64);
        fields.insert("x".to_string(), FieldType::F64);
        EventDescriptor {
            name: name.to_string(),
            schema: EventSchema {
                fields,
                optional_fields: vec![],
            },
            event_time_field: "event_time".to_string(),
            idempotency_key: None,
            idempotency_ttl_ms: None,
            history_ttl_ms: None,
            watermark_lateness_ms: None,
            registered_at_version: 0,
        }
    }

    #[tokio::test]
    async fn get_registry_empty_returns_version_0() {
        let registry = Arc::new(Registry::new());
        let r = router(ReadinessFlag::new(), registry, true);
        let (status, body) = get(r, "/registry").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["version"], 0);
        assert_eq!(body["events"], serde_json::json!({}));
        assert_eq!(body["tables"], serde_json::json!({}));
        assert_eq!(body["derivations"], serde_json::json!({}));
        assert_eq!(body["_dev_only"], true);
    }

    #[tokio::test]
    async fn get_registry_after_register_returns_populated() {
        let registry = Arc::new(Registry::new());
        // Seed via apply_registration
        let desc = minimal_event_descriptor("T");
        registry.apply_registration(vec![PayloadNode::Event(desc)]);

        let r = router(ReadinessFlag::new(), registry, true);
        let (status, body) = get(r, "/registry").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["version"], 1);
        assert!(
            body["events"]["T"].is_object(),
            "expected events[T] to be an object"
        );
    }

    #[tokio::test]
    async fn get_registry_when_disabled_returns_404() {
        let registry = Arc::new(Registry::new());
        let r = router(ReadinessFlag::new(), registry, false);
        let (status, _) = get(r, "/registry").await;
        assert_eq!(status, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn get_registry_descriptor_has_registered_at_version_field() {
        let registry = Arc::new(Registry::new());
        let desc = minimal_event_descriptor("T");
        registry.apply_registration(vec![PayloadNode::Event(desc)]);

        let r = router(ReadinessFlag::new(), registry, true);
        let (status, body) = get(r, "/registry").await;
        assert_eq!(status, StatusCode::OK);
        let rav = &body["events"]["T"]["registered_at_version"];
        assert_eq!(rav, 1, "registered_at_version should be 1, got: {rav}");
    }
}
