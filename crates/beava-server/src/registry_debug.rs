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

use axum::{
    extract::State,
    http::StatusCode,
    routing::{get, post},
    Json, Router,
};
use beava_core::agg_apply::apply_event_to_aggregations;
use beava_core::agg_state_table::AggStateTable;
use beava_core::registry::{DerivationDescriptor, EventDescriptor, Registry, TableDescriptor};
use beava_core::row::{Row, Value};
use beava_core::temporal::TemporalStore;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Phase 11.5 D-10/D-12 — runtime side-table that maps an event_id (LSN) to
/// the kind of WAL record at that LSN. The retract handler uses this to
/// route stream events to 501, table writes to MVCC retraction, and unknown
/// IDs to 404 — without re-walking the WAL on the hot path.
#[derive(Debug, Clone)]
pub enum EventIdEntry {
    /// A pushed stream event. Retraction is unimplemented in v0; the
    /// handler returns 501 with `stream_retraction_unimplemented`.
    Stream { event_name: String },
    /// A table write — temporal or non-temporal. The retract handler
    /// inspects the descriptor at retract-time to decide between 400
    /// (table_not_temporal) and the actual MVCC retraction path.
    TableWrite {
        table_name: String,
        entity_key: Vec<u8>,
        retracted: bool,
    },
}

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
pub fn dev_apply_ops_router(registry: Arc<Registry>) -> axum::Router {
    Router::new()
        .route("/dev/apply_ops", post(post_dev_apply_ops))
        .with_state(registry)
}

/// POST /dev/apply_ops handler.
///
/// Looks up the compiled OpChain for `body.derivation`, converts the JSON row
/// to a `Row<Value>`, applies the chain, and returns the result.
///
/// # JSON → Value conversion (per module doc):
/// - bool             → Value::Bool
/// - integer-fitting Number → Value::I64
/// - other Number     → Value::F64
/// - string           → Value::Str
/// - null             → Value::Null
/// - array / object   → Value::Null (no nested support in v0)
///
/// # Row → JSON conversion (per module doc):
/// - Value::Null      → serde_json::Value::Null
/// - Value::I64(n)    → JSON Number (i64)
/// - Value::F64(f)    → JSON Number (f64)
/// - Value::Bool(b)   → JSON Bool
/// - Value::Str(s)    → JSON String
/// - Value::Bytes(_)  → JSON Null  (binary not JSON-representable in v0)
/// - Value::Datetime(ms) → JSON Number (i64 ms since epoch)
async fn post_dev_apply_ops(
    State(registry): State<Arc<Registry>>,
    Json(body): Json<ApplyOpsRequest>,
) -> (StatusCode, Json<serde_json::Value>) {
    // Step 1: look up the compiled chain.
    let chain = match registry.compiled_chain(&body.derivation) {
        Some(c) => c,
        None => {
            return (
                StatusCode::NOT_FOUND,
                Json(serde_json::json!({"error": "derivation_not_found"})),
            );
        }
    };

    // Step 2: convert JSON row → Row<Value>.
    let mut row = Row::new();
    for (field, jv) in body.row {
        let v = json_to_value(&jv);
        row = row.with_field(&field, v);
    }

    // Step 3: apply the chain.
    match chain.apply(row) {
        None => (StatusCode::OK, Json(serde_json::json!({"kept": false}))),
        Some(updated_row) => {
            let mut obj = serde_json::Map::new();
            for (field, v) in updated_row.0 {
                obj.insert(field, value_to_json(v));
            }
            (
                StatusCode::OK,
                Json(serde_json::json!({"kept": true, "row": serde_json::Value::Object(obj)})),
            )
        }
    }
}

// ─── Conversion helpers ───────────────────────────────────────────────────────

/// Convert a `serde_json::Value` to a `beava_core::row::Value`.
/// See module-level doc for the conversion table.
fn json_to_value(jv: &serde_json::Value) -> Value {
    match jv {
        serde_json::Value::Null => Value::Null,
        serde_json::Value::Bool(b) => Value::Bool(*b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Value::I64(i)
            } else if let Some(f) = n.as_f64() {
                Value::F64(f)
            } else {
                Value::Null
            }
        }
        serde_json::Value::String(s) => Value::Str(s.clone()),
        // Array / Object → Null (no nested support in v0)
        _ => Value::Null,
    }
}

/// Convert a `beava_core::row::Value` to a `serde_json::Value`.
/// See module-level doc for the conversion table.
fn value_to_json(v: Value) -> serde_json::Value {
    match v {
        Value::Null => serde_json::Value::Null,
        Value::Bool(b) => serde_json::Value::Bool(b),
        Value::I64(n) => serde_json::Value::Number(n.into()),
        Value::F64(f) => serde_json::Number::from_f64(f)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        Value::Str(s) => serde_json::Value::String(s),
        // Bytes not JSON-representable in v0 → Null
        Value::Bytes(_) => serde_json::Value::Null,
        // Datetime: emit as i64 ms since epoch
        Value::Datetime(ms) => serde_json::Value::Number(ms.into()),
        Value::Json(j) => j,
    }
}

// ─── POST /dev/apply_events ───────────────────────────────────────────────────

/// Shared state for the dev apply-events endpoint and (in Plan 05-06) the dev
/// query endpoint.  Both endpoints share the same `state_tables` so events
/// pushed via `/dev/apply_events` are immediately visible via `/dev/query`.
///
/// Single-writer invariant is preserved at the HTTP layer by the `Mutex` —
/// only the apply handler takes the lock; query handlers use a read-only
/// snapshot.
///
/// # SDK-AGG-02, AGG-CORE-09
#[derive(Clone)]
pub struct DevAggState {
    /// Per-aggregation, per-entity state.  `Mutex` wraps the outer map only;
    /// per-entity `AggOp` state is updated under this single lock (single-writer
    /// invariant per D-06 + project_stateful_architecture.md).
    pub state_tables: Arc<Mutex<BTreeMap<String, AggStateTable>>>,
    /// Registry shared with the main router (read-only from this endpoint).
    pub registry: Arc<Registry>,
    /// Monotonic event-id counter. Feeds `apply_event_to_aggregations`'s
    /// `event_id` parameter; value is ignored in Phase 5 but keeps the
    /// signature stable for Phase 6 WAL (D-08).
    pub next_event_id: Arc<AtomicU64>,
    /// Maximum event_time_ms observed across all applied events.
    ///
    /// Used by the `/get` query handlers as the query time (D-06: deterministic
    /// query time — max observed event_time, NOT wall-clock). Value is 0 until
    /// the first event is applied. Stored as u64 (cast from i64) so it fits in
    /// an AtomicU64; negative event times are treated as 0 for query purposes.
    pub max_event_time_ms: Arc<AtomicU64>,

    /// Phase 11.5 D-01 — per-table MVCC stores. Key = table name. Created
    /// lazily on first push-table for a temporal table.
    pub temporal_stores: Arc<Mutex<HashMap<String, TemporalStore>>>,

    /// Phase 11.5 D-10 — event_id → entry index (see EventIdEntry).
    /// Populated at apply-time by /push/{event_name} and
    /// /push-table/{table_name}; consumed at retract-time.
    pub event_id_index: Arc<Mutex<HashMap<u64, EventIdEntry>>>,
}

impl DevAggState {
    pub fn new(registry: Arc<Registry>) -> Self {
        DevAggState {
            state_tables: Arc::new(Mutex::new(BTreeMap::new())),
            registry,
            next_event_id: Arc::new(AtomicU64::new(0)),
            max_event_time_ms: Arc::new(AtomicU64::new(0)),
            temporal_stores: Arc::new(Mutex::new(HashMap::new())),
            event_id_index: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

/// Request body for `POST /dev/apply_events`.
///
/// ```json
/// {
///   "source": "Transaction",
///   "event_time_ms": 1714000000000,
///   "row": { "user_id": "alice", "amount": 100.0 }
/// }
/// ```
#[derive(Debug, Deserialize)]
pub struct ApplyEventsRequest {
    pub source: String,
    pub event_time_ms: i64,
    pub row: BTreeMap<String, serde_json::Value>,
}

/// Response body for `POST /dev/apply_events`.
///
/// ```json
/// { "applied_to": ["AggTable1", "AggTable2"] }
/// ```
#[derive(Debug, Serialize)]
pub struct ApplyEventsResponse {
    pub applied_to: Vec<String>,
}

/// Build the sub-router for `POST /dev/apply_events`.
/// Caller merges this into the main router conditionally (same
/// `BEAVA_DEV_ENDPOINTS=1` gate).
pub fn dev_apply_events_router(state: DevAggState) -> axum::Router {
    Router::new()
        .route("/dev/apply_events", post(post_dev_apply_events))
        .with_state(state)
}

/// `POST /dev/apply_events` handler.
///
/// Converts the JSON row to `Row<Value>`, looks up the event source in the
/// registry (404 if not found), pulls the next monotonic `event_id`, then
/// calls `apply_event_to_aggregations`. Returns the list of aggregation
/// node_names whose state was touched.
///
/// Gated by `BEAVA_DEV_ENDPOINTS=1` (not mounted in production).
async fn post_dev_apply_events(
    State(dev_state): State<DevAggState>,
    Json(body): Json<ApplyEventsRequest>,
) -> (StatusCode, Json<serde_json::Value>) {
    // Step 1: validate that source exists in the registry.
    {
        let inner = dev_state.registry.read();
        if !inner.events.contains_key(&body.source) {
            return (
                StatusCode::NOT_FOUND,
                Json(serde_json::json!({"error": "source_not_found"})),
            );
        }
    }

    // Step 2: convert JSON row → Row<Value>.
    let mut row = Row::new();
    for (field, jv) in &body.row {
        let v = json_to_value(jv);
        row = row.with_field(field.as_str(), v);
    }

    // Step 3: pull monotonic event_id (ignored in Phase 5; stable for Phase 6).
    let event_id = dev_state.next_event_id.fetch_add(1, Ordering::SeqCst);

    // Step 4: snapshot which agg node_names exist BEFORE and AFTER to build
    // the `applied_to` list.  We report the aggregations that were touched
    // (their source matches the pushed event's source).
    let matching_aggs: Vec<String> = dev_state
        .registry
        .compiled_aggregations_for_source(&body.source)
        .into_iter()
        .map(|d| d.node_name.clone())
        .collect();

    // Step 5: apply under the single-writer lock.
    {
        let mut tables = dev_state.state_tables.lock();
        apply_event_to_aggregations(
            &body.source,
            &row,
            body.event_time_ms,
            event_id,
            &dev_state.registry,
            &mut tables,
        );
    }

    // Step 6: bump max_event_time_ms (D-06 deterministic query time).
    // Use fetch_max to ensure monotonicity even with out-of-order events.
    if body.event_time_ms > 0 {
        dev_state
            .max_event_time_ms
            .fetch_max(body.event_time_ms as u64, Ordering::Relaxed);
    }

    (
        StatusCode::OK,
        Json(
            serde_json::to_value(ApplyEventsResponse {
                applied_to: matching_aggs,
            })
            .unwrap(),
        ),
    )
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
        let r = router(ReadinessFlag::new(), registry, true, None);
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
        registry.apply_registration(vec![PayloadNode::Event(desc)], vec![], vec![], vec![]);

        let r = router(ReadinessFlag::new(), registry, true, None);
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
        let r = router(ReadinessFlag::new(), registry, false, None);
        let (status, _) = get(r, "/registry").await;
        assert_eq!(status, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn get_registry_descriptor_has_registered_at_version_field() {
        let registry = Arc::new(Registry::new());
        let desc = minimal_event_descriptor("T");
        registry.apply_registration(vec![PayloadNode::Event(desc)], vec![], vec![], vec![]);

        let r = router(ReadinessFlag::new(), registry, true, None);
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
            vec![],
        );

        registry
    }

    /// W0: POST /dev/apply_ops with unknown derivation → 404.
    /// Fails at `todo!()` until Task 1.b implements the handler.
    #[tokio::test]
    async fn dev_apply_ops_endpoint_returns_404_without_derivation() {
        let registry = Arc::new(Registry::new());
        let r = router(ReadinessFlag::new(), registry, true, None);
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
        let r = router(ReadinessFlag::new(), registry, true, None);
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
            vec![],
        );

        let r = router(ReadinessFlag::new(), registry, true, None);
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
            None,
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
