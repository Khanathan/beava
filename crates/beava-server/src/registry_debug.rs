//! Dev-only endpoints: GET /registry + POST /dev/apply_ops.
//!
//! Only mounted when `dev_endpoints_enabled = true` in the router call (which
//! reads `BEAVA_DEV_ENDPOINTS=1` from the environment at `Server::bind` time).
//! Default posture: routes are NOT mounted → clients receive 404.
//!
//! # POST /dev/apply_ops
//!
//! Applies a registered derivation's compiled op-chain to a synthetic row and
//! returns the result.  Useful for acceptance tests and interactive debugging.
//!
//! Request body: `{"derivation": "<name>", "row": {<field>: <json value>, ...}}`
//!
//! Response variants:
//! - `{"kept": true, "row": {...}}` — filter kept the row (with transforms applied)
//! - `{"kept": false}` — a Filter op dropped the row
//! - HTTP 404 `{"error": "derivation_not_found"}` — derivation name unknown
//! - HTTP 400 `{"error": "no_compiled_chain"}` — derivation has no compiled ops
//!   (should not happen for derivations registered with ops; defensive guard)
//!
//! **JSON ↔ Value conversion rules (doc-inline in handler):**
//! - `bool`             → `Value::Bool`
//! - integer-fitting Number → `Value::I64`
//! - other Number       → `Value::F64`
//! - string             → `Value::Str`
//! - null               → `Value::Null`
//! - array / object     → `Value::Null` (no nested support in v0)
//!
//! **Row → JSON rules:**
//! - `Value::Null`     → `serde_json::Value::Null`
//! - `Value::I64(n)`   → JSON Number (i64)
//! - `Value::F64(f)`   → JSON Number (f64)
//! - `Value::Bool(b)`  → JSON Bool
//! - `Value::Str(s)`   → JSON String
//! - `Value::Bytes(_)` → JSON Null (binary not representable in JSON v0)
//! - `Value::Datetime(ms)` → JSON Number (i64 ms since epoch)

use axum::{extract::State, http::StatusCode, routing::get, Json, Router};
use beava_core::registry::{DerivationDescriptor, EventDescriptor, Registry, TableDescriptor};
use serde::{Deserialize, Serialize};
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

// ─── POST /dev/apply_ops ──────────────────────────────────────────────────────

/// Request body for POST /dev/apply_ops.
#[derive(Debug, Deserialize)]
pub struct ApplyOpsRequest {
    pub derivation: String,
    pub row: BTreeMap<String, serde_json::Value>,
}

/// Build a sub-router for POST /dev/apply_ops. Caller merges this into the main
/// router conditionally (same BEAVA_DEV_ENDPOINTS gate as GET /registry).
pub fn dev_apply_ops_router(_registry: Arc<Registry>) -> axum::Router {
    todo!("red stub: 04-06 impl pending — dev_apply_ops_router")
}

/// POST /dev/apply_ops handler (stub — fails at todo!() until Task 1.b).
#[allow(dead_code)]
async fn post_dev_apply_ops(
    State(_registry): State<Arc<Registry>>,
    Json(_body): Json<ApplyOpsRequest>,
) -> (StatusCode, Json<serde_json::Value>) {
    todo!("red stub: 04-06 impl pending — post_dev_apply_ops")
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::http::{router, ReadinessFlag};
    use axum::body::Body;
    use axum::http::{Method, Request, StatusCode};
    use beava_core::op_node::OpNode;
    use beava_core::registry::OutputKind;
    use beava_core::registry::{EventDescriptor, Registry};
    use beava_core::registry_diff::PayloadNode;
    use beava_core::schema::{DerivedSchema, EventSchema, FieldType};
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
            event_time_field: Some("event_time".to_string()),
            dedupe_key: None,
            dedupe_window_ms: None,
            keep_events_for_ms: None,
            tolerate_delay_ms: None,
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
        registry.apply_registration(vec![PayloadNode::Event(desc)], vec![], vec![]);

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
        registry.apply_registration(vec![PayloadNode::Event(desc)], vec![], vec![]);

        let r = router(ReadinessFlag::new(), registry, true);
        let (status, body) = get(r, "/registry").await;
        assert_eq!(status, StatusCode::OK);
        let rav = &body["events"]["T"]["registered_at_version"];
        assert_eq!(rav, 1, "registered_at_version should be 1, got: {rav}");
    }

    // ─── POST /dev/apply_ops unit tests (04-06 red stubs) ────────────────────

    /// Helper: build a POST request with a JSON body.
    async fn post_json(
        r: axum::Router,
        uri: &str,
        body: serde_json::Value,
    ) -> (StatusCode, serde_json::Value) {
        let payload = serde_json::to_vec(&body).unwrap();
        let req = Request::builder()
            .method(Method::POST)
            .uri(uri)
            .header("content-type", "application/json")
            .body(Body::from(payload))
            .unwrap();
        let resp = r.oneshot(req).await.expect("oneshot");
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

    /// Helper: build a minimal Transaction event descriptor + BigTx filter derivation
    /// and seed them into the registry, returning a router with dev_endpoints=true.
    fn registry_with_filter_derivation(deriv_name: &str, filter_expr: &str) -> Arc<Registry> {
        let registry = Arc::new(Registry::new());

        // 1. Install Transaction event.
        let mut fields = BTreeMap::new();
        fields.insert("event_time".to_string(), FieldType::I64);
        fields.insert("amount".to_string(), FieldType::F64);
        let event = EventDescriptor {
            name: "Transaction".to_string(),
            schema: EventSchema {
                fields,
                optional_fields: vec![],
            },
            event_time_field: Some("event_time".to_string()),
            dedupe_key: None,
            dedupe_window_ms: None,
            keep_events_for_ms: None,
            tolerate_delay_ms: None,
            registered_at_version: 0,
        };

        // 2. Build and compile the derivation.
        let mut schema_fields = BTreeMap::new();
        schema_fields.insert("event_time".to_string(), FieldType::I64);
        schema_fields.insert("amount".to_string(), FieldType::F64);
        let deriv = beava_core::registry::DerivationDescriptor {
            name: deriv_name.to_string(),
            output_kind: OutputKind::Event,
            upstreams: vec!["Transaction".to_string()],
            ops: vec![OpNode::Filter {
                expr: filter_expr.to_string(),
            }],
            schema: DerivedSchema {
                fields: schema_fields,
                optional_fields: vec![],
            },
            table_primary_key: None,
            registered_at_version: 0,
        };

        // Compile the chain.
        use beava_core::op_chain::OpChain;
        use beava_core::schema_propagate::Schema;
        let mut input_fields = BTreeMap::new();
        input_fields.insert("event_time".to_string(), FieldType::I64);
        input_fields.insert("amount".to_string(), FieldType::F64);
        let input_schema = Schema {
            fields: input_fields,
            optional_fields: vec![],
        };
        let (chain, _) = OpChain::compile(&input_schema, &deriv.ops)
            .expect("compile should succeed for valid filter");
        let chain_arc = std::sync::Arc::new(chain);

        registry.apply_registration(
            vec![PayloadNode::Event(event), PayloadNode::Derivation(deriv)],
            vec![(deriv_name.to_string(), chain_arc)],
            vec![],
        );

        registry
    }

    /// W0: POST /dev/apply_ops with unknown derivation → 404.
    /// Fails at `todo!()` until Task 1.b implements the handler.
    #[tokio::test]
    async fn dev_apply_ops_endpoint_returns_404_without_derivation() {
        let registry = Arc::new(Registry::new());
        let r = router(ReadinessFlag::new(), registry, true);
        let (status, _body) = post_json(
            r,
            "/dev/apply_ops",
            serde_json::json!({"derivation": "X", "row": {}}),
        )
        .await;
        assert_eq!(
            status,
            StatusCode::NOT_FOUND,
            "unknown derivation should return 404"
        );
    }

    /// W1: POST /dev/apply_ops where filter drops the row → {"kept": false}.
    /// Fails at `todo!()` until Task 1.b implements the handler.
    #[tokio::test]
    async fn dev_apply_ops_endpoint_filters_drops_row() {
        let registry = registry_with_filter_derivation("BigTx", "(amount > 100)");
        let r = router(ReadinessFlag::new(), registry, true);
        let (status, body) = post_json(
            r,
            "/dev/apply_ops",
            serde_json::json!({"derivation": "BigTx", "row": {"event_time": 1000, "amount": 50.0}}),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            body["kept"], false,
            "amount=50 < 100 should be dropped: {body:#}"
        );
    }

    /// W2: POST /dev/apply_ops where filter keeps the row + with_columns adds field.
    /// Fails at `todo!()` until Task 1.b implements the handler.
    #[tokio::test]
    async fn dev_apply_ops_endpoint_filter_keeps_row_and_returns_transformed() {
        // Register: Transaction + TaggedTx (filter then with_columns)
        let registry = Arc::new(Registry::new());
        let mut fields = BTreeMap::new();
        fields.insert("event_time".to_string(), FieldType::I64);
        fields.insert("amount".to_string(), FieldType::F64);
        let event = EventDescriptor {
            name: "Transaction".to_string(),
            schema: EventSchema {
                fields,
                optional_fields: vec![],
            },
            event_time_field: Some("event_time".to_string()),
            dedupe_key: None,
            dedupe_window_ms: None,
            keep_events_for_ms: None,
            tolerate_delay_ms: None,
            registered_at_version: 0,
        };

        let mut wc_exprs = BTreeMap::new();
        wc_exprs.insert("is_big".to_string(), "(amount > 500)".to_string());
        let ops = vec![
            OpNode::Filter {
                expr: "(amount > 100)".to_string(),
            },
            OpNode::WithColumns { exprs: wc_exprs },
        ];

        let mut schema_fields = BTreeMap::new();
        schema_fields.insert("event_time".to_string(), FieldType::I64);
        schema_fields.insert("amount".to_string(), FieldType::F64);
        schema_fields.insert("is_big".to_string(), FieldType::Bool);
        let deriv = beava_core::registry::DerivationDescriptor {
            name: "TaggedTx".to_string(),
            output_kind: OutputKind::Event,
            upstreams: vec!["Transaction".to_string()],
            ops: ops.clone(),
            schema: DerivedSchema {
                fields: schema_fields,
                optional_fields: vec![],
            },
            table_primary_key: None,
            registered_at_version: 0,
        };

        use beava_core::op_chain::OpChain;
        use beava_core::schema_propagate::Schema;
        let mut input_fields = BTreeMap::new();
        input_fields.insert("event_time".to_string(), FieldType::I64);
        input_fields.insert("amount".to_string(), FieldType::F64);
        let input_schema = Schema {
            fields: input_fields,
            optional_fields: vec![],
        };
        let (chain, _) = OpChain::compile(&input_schema, &ops).expect("compile should succeed");
        let chain_arc = std::sync::Arc::new(chain);

        registry.apply_registration(
            vec![PayloadNode::Event(event), PayloadNode::Derivation(deriv)],
            vec![("TaggedTx".to_string(), chain_arc)],
            vec![],
        );

        let r = router(ReadinessFlag::new(), registry, true);
        let (status, body) = post_json(
            r,
            "/dev/apply_ops",
            serde_json::json!({"derivation": "TaggedTx", "row": {"event_time": 1000, "amount": 1000.0}}),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            body["kept"], true,
            "amount=1000 > 100 should be kept: {body:#}"
        );
        assert_eq!(
            body["row"]["is_big"], true,
            "1000 > 500 should set is_big=true: {body:#}"
        );
        assert_eq!(
            body["row"]["amount"], 1000.0,
            "amount should be preserved: {body:#}"
        );
    }

    /// W3 (gating): POST /dev/apply_ops returns 404 when flag is NOT set.
    /// Asserts the route is strictly gated behind dev_endpoints=true.
    /// Fails at `todo!()` until Task 1.b mounts the route conditionally.
    #[tokio::test]
    async fn dev_apply_ops_not_mounted_without_flag() {
        // Build a router WITHOUT the dev flag (production configuration).
        let registry = Arc::new(Registry::new());
        let r = router(
            ReadinessFlag::new(),
            registry,
            false, /* dev_endpoints=false */
        );
        let (status, _body) = post_json(
            r,
            "/dev/apply_ops",
            serde_json::json!({"derivation": "Any", "row": {}}),
        )
        .await;
        assert_eq!(
            status,
            StatusCode::NOT_FOUND,
            "/dev/apply_ops must not be mounted when dev_endpoints=false"
        );
    }
}
