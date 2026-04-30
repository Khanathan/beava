//! Feature query endpoints — Plan 05-06.
//!
//! # GET /get/:feature/:key
//!
//! Single-feature lookup. Returns `{"value": <JSON>}` per D-02.
//!
//! - 200 `{"value": <JSON>}` — feature and key found
//! - 404 `{"error": {"code": "feature_not_found"}}` — unknown feature name
//! - 404 `{"error": {"code": "key_not_found"}}` — valid feature, key not seen
//!
//! # POST /get
//!
//! Batch lookup. Body: `{"keys": [...], "features": [...]}`.
//! Returns `{"result": {key: {feature: value}}}` per SRV-API-08.
//!
//! - 200 `{"result": {...}}` — success (missing keys are omitted, not null)
//! - 400 `{"error": {"code": "feature_not_found", "missing": [...]}}` — unknown feature
//! - 400 `{"error": {"code": "batch_too_large"}}` — keys × features > 10_000
//!
//! # D-02 compliance
//!
//! Response envelope is `{"value": ...}` ONLY. v0 ships no metadata envelope.
//! Grep guard test asserts the response wrapper key is exactly "value".
//!
//! # D-06 compliance
//!
//! Query time uses max(event_time_ms observed) or 0 — wall-clock is never read.
//! Grep guard test asserts clock functions are absent from this file's production code.
//!
//! # SDK-AGG-02

use crate::registry_debug::DevAggState;
use axum::{
    extract::{Path, State},
    http::StatusCode,
    routing::{get, post},
    Json, Router,
};
use beava_core::agg_state_table::EntityKey;
use beava_core::registry::Registry;
use beava_core::row::Value;
use serde::Deserialize;
use std::collections::BTreeMap;
use std::sync::atomic::Ordering;
use std::sync::Arc;

// ─── Batch cap (SRV-API-08, T-05-06-01) ─────────────────────────────────────

/// Maximum allowed keys × features in a single POST /get request (SRV-API-08).
const BATCH_CAP: usize = 10_000;

// ─── State ────────────────────────────────────────────────────────────────────

/// Axum state for the feature query router.
///
/// Shares `state_tables` and `registry` with the `/dev/apply_events` handler
/// (both come from the same `DevAggState`), so events pushed via
/// `/dev/apply_events` are immediately visible via `/get`.
///
/// SDK-AGG-02
#[derive(Clone)]
pub struct FeatureQueryState {
    pub registry: Arc<Registry>,
    pub dev_agg_state: DevAggState,
}

impl FeatureQueryState {
    pub fn new(dev_agg_state: DevAggState) -> Self {
        let registry = dev_agg_state.registry.clone();
        FeatureQueryState {
            registry,
            dev_agg_state,
        }
    }
}

// ─── Router ───────────────────────────────────────────────────────────────────

/// Build the sub-router for GET /get/:feature/:key + POST /get.
/// Caller merges this into the main router conditionally (dev gate).
pub fn feature_query_router(state: FeatureQueryState) -> Router {
    Router::new()
        .route("/get/:feature/:key", get(get_feature_handler))
        .route("/get", post(post_get_batch_handler))
        .with_state(state)
}

// ─── Request / response types ─────────────────────────────────────────────────

/// POST /get request body.
#[derive(Debug, Deserialize)]
pub struct BatchGetRequest {
    pub keys: Vec<String>,
    pub features: Vec<String>,
}

// ─── Handlers ─────────────────────────────────────────────────────────────────

/// GET /get/{feature}/{key} handler.
///
/// Resolves the feature name to an aggregation, looks up the entity key in the
/// state table, and returns `{"value": <JSON>}`.  Returns 404 for unknown
/// feature names or keys that have no state.
async fn get_feature_handler(
    Path((feature, key)): Path<(String, String)>,
    State(state): State<FeatureQueryState>,
) -> (StatusCode, Json<serde_json::Value>) {
    // Resolve feature name → (agg_node, feature_index).
    let (agg_node, feature_idx) = match state.registry.resolve_feature(&feature) {
        Some(x) => x,
        None => {
            return (
                StatusCode::NOT_FOUND,
                Json(serde_json::json!({"error": {"code": "feature_not_found"}})),
            );
        }
    };

    // Look up the compiled aggregation descriptor for group_keys.
    let descriptor = match state.registry.compiled_aggregation(&agg_node) {
        Some(d) => d,
        None => {
            // Index consistency invariant violated — should never happen.
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": {"code": "internal_error"}})),
            );
        }
    };

    // Parse entity key (pipe-separated for multi-key group_by).
    // Returns None when segment count mismatches group_keys length (WR-02).
    let entity_key = match parse_entity_key(&key, &descriptor.group_keys) {
        Some(k) => k,
        None => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({"error": {"code": "key_parse_failure"}})),
            );
        }
    };

    // Query state under the lock.
    // Plan 18-16 Task 16.2: state_tables is now Vec<AggStateTable> indexed
    // by agg_id; look up via descriptor.agg_id (already loaded above).
    let tables = state.dev_agg_state.state_tables.lock();
    let query_time_ms = compute_query_time_ms(&state.dev_agg_state);
    let value_opt = tables
        .get(descriptor.agg_id as usize)
        .and_then(|t| t.query_feature(&entity_key, feature_idx, query_time_ms));

    match value_opt {
        Some(v) => (
            StatusCode::OK,
            Json(serde_json::json!({"value": value_to_json(v)})),
        ),
        None => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({"error": {"code": "key_not_found"}})),
        ),
    }
}

/// POST /get batch handler.
///
/// Accepts `{"keys": [...], "features": [...]}`, enforces the 10_000-cell cap
/// (SRV-API-08 / T-05-06-01), returns `{"result": {key: {feature: value}}}`.
/// Missing keys are omitted from the result map (not null).
async fn post_get_batch_handler(
    State(state): State<FeatureQueryState>,
    Json(body): Json<BatchGetRequest>,
) -> (StatusCode, Json<serde_json::Value>) {
    // Cap check (T-05-06-01).
    let cell_count = body.keys.len().saturating_mul(body.features.len());
    if cell_count > BATCH_CAP {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": {"code": "batch_too_large"}})),
        );
    }

    // Validate all feature names upfront — return 400 if any are unknown.
    let mut missing_features: Vec<String> = Vec::new();
    let mut feature_resolutions: Vec<(String, usize)> = Vec::new();
    for feat in &body.features {
        match state.registry.resolve_feature(feat) {
            Some(resolution) => feature_resolutions.push(resolution),
            None => missing_features.push(feat.clone()),
        }
    }
    if !missing_features.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(
                serde_json::json!({"error": {"code": "feature_not_found", "missing": missing_features}}),
            ),
        );
    }

    // Build result map.
    let tables = state.dev_agg_state.state_tables.lock();
    let query_time_ms = compute_query_time_ms(&state.dev_agg_state);

    let mut result: BTreeMap<String, BTreeMap<String, serde_json::Value>> = BTreeMap::new();

    for key_str in &body.keys {
        let mut key_result: BTreeMap<String, serde_json::Value> = BTreeMap::new();

        for (feat_name, (agg_node, feature_idx)) in
            body.features.iter().zip(feature_resolutions.iter())
        {
            let descriptor = match state.registry.compiled_aggregation(agg_node) {
                Some(d) => d,
                None => continue,
            };
            // Skip features where the key is malformed for this group_by arity.
            // Malformed keys (wrong pipe-segment count) are silently omitted from
            // the batch result rather than failing the whole request (WR-02).
            let entity_key = match parse_entity_key(key_str, &descriptor.group_keys) {
                Some(k) => k,
                None => continue,
            };
            if let Some(val) = tables
                .get(descriptor.agg_id as usize)
                .and_then(|t| t.query_feature(&entity_key, *feature_idx, query_time_ms))
            {
                key_result.insert(feat_name.clone(), value_to_json(val));
            }
        }

        // Omit keys with no matching state (SRV-API-08 semantics).
        if !key_result.is_empty() {
            result.insert(key_str.clone(), key_result);
        }
    }

    (StatusCode::OK, Json(serde_json::json!({"result": result})))
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

/// Parse a URL-encoded entity key into an `EntityKey`.
///
/// For multi-key group_by, the key string uses `|` as a separator:
/// e.g., `"alice|merchant1"` → `[("user_id", "alice"), ("merchant_id", "merchant1")]`.
///
/// Returns `None` when the number of pipe-separated segments does not match
/// `group_keys.len()`. Callers should return a `key_parse_failure` error code in
/// that case so it is distinguishable from `key_not_found` (WR-02).
///
/// **Limitation:** pipe characters inside key values must be percent-encoded as `%7C`
/// because `|` is the multi-key separator. Full URL-decoding of `%7C` → `|` is deferred
/// to Phase 12 API completion.
pub(crate) fn parse_entity_key(key_str: &str, group_keys: &[String]) -> Option<EntityKey> {
    use beava_core::row::Value;
    use compact_str::CompactString;
    use smallvec::SmallVec;
    let segments: Vec<&str> = key_str.split('|').collect();
    if segments.len() != group_keys.len() {
        return None;
    }
    // Plan 18-11 D-5: build EntityKey via SmallVec of (CompactString, Value::Str)
    // pairs. URL parameters are always strings — values are stored as Str.
    let pairs: SmallVec<[(CompactString, Value); 2]> = group_keys
        .iter()
        .zip(segments.iter())
        .map(|(k, v)| {
            let key: CompactString = k.as_str().into();
            let val: Value = Value::Str(CompactString::from(*v));
            (key, val)
        })
        .collect();
    Some(EntityKey(pairs))
}

/// Compute the query time as max(event_time_ms) observed across all applied events.
///
/// Returns `0` if no events have been applied yet.
///
/// D-06: Uses the tracked max event_time from DevAggState, never wall-clock.
/// This keeps queries deterministic — the same input event stream always
/// produces the same outputs regardless of when the query executes.
fn compute_query_time_ms(dev_agg_state: &DevAggState) -> i64 {
    dev_agg_state.query_time_ms.load(Ordering::Acquire) as i64
}

/// Convert a `beava_core::row::Value` to a `serde_json::Value`.
///
/// Mirror of the helper in `registry_debug.rs` — duplicated here to keep
/// `feature_query.rs` self-contained (no dep on the debug module).
///
/// Phase 11 (D-01): `Value::List` → JSON array; `Value::Map` → JSON object.
/// Recursive on element values. NaN floats serialise as JSON null.
pub(crate) fn value_to_json(v: Value) -> serde_json::Value {
    match v {
        Value::Null => serde_json::Value::Null,
        Value::Bool(b) => serde_json::Value::Bool(b),
        Value::I64(n) => serde_json::Value::Number(n.into()),
        Value::F64(f) => serde_json::Number::from_f64(f)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        Value::Str(s) => serde_json::Value::String(s.into_string()),
        Value::Bytes(_) => serde_json::Value::Null,
        Value::Datetime(ms) => serde_json::Value::Number(ms.into()),
        Value::Json(j) => j,
        Value::List(items) => {
            serde_json::Value::Array(items.into_iter().map(value_to_json).collect())
        }
        Value::Map(m) => {
            serde_json::Value::Object(m.into_iter().map(|(k, v)| (k, value_to_json(v))).collect())
        }
    }
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::http::{router, ReadinessFlag};
    use axum::body::Body;
    use axum::http::{Method, Request, StatusCode};
    use beava_core::agg_descriptor::{AggregationDescriptor, NamedAggOp};
    use beava_core::agg_op::{AggKind, AggOpDescriptor};
    use beava_core::registry::{DerivationDescriptor, EventDescriptor, OutputKind, Registry};
    use beava_core::registry_diff::PayloadNode;
    use beava_core::schema::{DerivedSchema, EventSchema, FieldType};
    use http_body_util::BodyExt;
    use std::collections::BTreeMap;
    use std::sync::Arc;
    use tower::ServiceExt;

    // ── Test helpers ─────────────────────────────────────────────────────────

    /// Build an HTTP response from the router.
    async fn call_get(r: axum::Router, uri: &str) -> (StatusCode, serde_json::Value) {
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
            (
                status,
                serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null),
            )
        }
    }

    async fn call_post(
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
            (
                status,
                serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null),
            )
        }
    }

    /// Create a registry with a count-5m aggregation over Transaction events
    /// grouped by user_id. Returns the registry and a seeded DevAggState.
    fn make_count_agg_registry() -> (Arc<Registry>, DevAggState) {
        let registry = Arc::new(Registry::new());

        // Event descriptor
        let mut fields = BTreeMap::new();
        fields.insert("event_time".to_string(), FieldType::I64);
        fields.insert("user_id".to_string(), FieldType::Str);
        fields.insert("amount".to_string(), FieldType::F64);
        let event = EventDescriptor {
            name: "Transaction".to_string(),
            schema: EventSchema {
                fields,
                optional_fields: vec![],
            },
            dedupe_key: None,
            dedupe_window_ms: None,
            keep_events_for_ms: None,
            registered_at_version: 0,
            name_arc: Arc::from(""),
            apply_field_names: vec![],
        };

        // Aggregation descriptor
        let agg_desc = Arc::new(AggregationDescriptor {
            node_name: "TxnAgg".to_string(),
            source_node_name: "Transaction".to_string(),
            group_keys: vec!["user_id".to_string()],
            features: vec![
                NamedAggOp {
                    feature_name: "cnt".to_string(),
                    descriptor: AggOpDescriptor {
                        kind: AggKind::Count,
                        field: None,
                        window_ms: Some(300_000),
                        where_expr: None,
                        n: None,
                        half_life_ms: None,
                        sub_window_ms: None,
                        sigma: None,
                        sketch_params: None,
                        ext: Default::default(),
                        field_idx: beava_core::agg_op::FIELD_IDX_NONE,
                        field_idx_into_event_extracted: Vec::new(),
                    },
                },
                NamedAggOp {
                    feature_name: "total".to_string(),
                    descriptor: AggOpDescriptor {
                        kind: AggKind::Sum,
                        field: Some("amount".to_string()),
                        window_ms: None,
                        where_expr: None,
                        n: None,
                        half_life_ms: None,
                        sub_window_ms: None,
                        sigma: None,
                        sketch_params: None,
                        ext: Default::default(),
                        field_idx: beava_core::agg_op::FIELD_IDX_NONE,
                        field_idx_into_event_extracted: Vec::new(),
                    },
                },
            ],
            agg_id: 0, // placeholder; registry overwrites at apply_registration
            field_names: vec![],
            cluster_id: 0,
        });

        // Derivation descriptor
        let deriv = DerivationDescriptor {
            name: "TxnAgg".to_string(),
            output_kind: OutputKind::Table,
            upstreams: vec!["Transaction".to_string()],
            ops: vec![],
            schema: DerivedSchema {
                fields: {
                    let mut m = BTreeMap::new();
                    m.insert("user_id".to_string(), FieldType::Str);
                    m.insert("cnt".to_string(), FieldType::I64);
                    m.insert("total".to_string(), FieldType::F64);
                    m
                },
                optional_fields: vec![],
            },
            table_primary_key: Some(vec!["user_id".to_string()]),
            registered_at_version: 0,
        };

        registry.apply_registration(
            vec![PayloadNode::Event(event), PayloadNode::Derivation(deriv)],
            vec![],
            vec![],
            vec![("TxnAgg".to_string(), agg_desc)],
        );

        let dev_state = DevAggState::new(registry.clone());
        (registry, dev_state)
    }

    /// Push N events to the dev state with the given user_id and amount.
    /// Also bumps `query_time_ms` so query handlers see the correct query time.
    fn push_events(
        dev_state: &DevAggState,
        source: &str,
        user_id: &str,
        amount: f64,
        count: usize,
    ) {
        use beava_core::agg_apply::apply_event_to_aggregations;
        use beava_core::row::{Row, Value};
        use std::sync::atomic::Ordering;

        let mut tables = dev_state.state_tables.lock();
        for i in 0..count {
            let event_id = dev_state.next_event_id.fetch_add(1, Ordering::SeqCst);
            let event_time_ms = 1_000_000_i64 + i as i64;
            let row = Row::new()
                .with_field("user_id", Value::Str(user_id.into()))
                .with_field("amount", Value::F64(amount))
                .with_field("event_time", Value::I64(event_time_ms));
            apply_event_to_aggregations(
                source,
                &row,
                event_time_ms,
                event_id,
                &dev_state.registry,
                &mut tables,
            );
            // Bump query_time_ms so GET /get query handlers use deterministic time.
            dev_state
                .query_time_ms
                .fetch_max(event_time_ms as u64, Ordering::Relaxed);
        }
    }

    // ── registry-level tests (feature_index) ─────────────────────────────────

    // These are also covered in registry.rs unit tests; kept here for proximity
    // to the HTTP tests.

    // ── GET /get/{feature}/{key} tests ────────────────────────────────────────

    /// get_endpoint_returns_value_for_present_entity:
    /// Push 10 events for alice; GET /get/cnt/alice → 200 {"value": 10}
    #[tokio::test]
    async fn get_endpoint_returns_value_for_present_entity() {
        let (registry, dev_state) = make_count_agg_registry();
        push_events(&dev_state, "Transaction", "alice", 5.0, 10);

        let r = router(ReadinessFlag::new(), registry, true, Some(dev_state));
        let (status, body) = call_get(r, "/get/cnt/alice").await;
        assert_eq!(status, StatusCode::OK, "body: {body:#}");
        assert_eq!(body["value"], 10, "expected count=10, body: {body:#}");
    }

    /// get_endpoint_404_on_unknown_feature:
    /// GET /get/nonexistent/alice → 404 {"error": {"code": "feature_not_found"}}
    #[tokio::test]
    async fn get_endpoint_404_on_unknown_feature() {
        let (registry, dev_state) = make_count_agg_registry();
        let r = router(ReadinessFlag::new(), registry, true, Some(dev_state));
        let (status, body) = call_get(r, "/get/nonexistent/alice").await;
        assert_eq!(status, StatusCode::NOT_FOUND, "body: {body:#}");
        assert_eq!(body["error"]["code"], "feature_not_found", "body: {body:#}");
    }

    /// get_endpoint_404_on_unknown_key:
    /// Valid feature, but key "bob" not seen yet → 404 key_not_found
    #[tokio::test]
    async fn get_endpoint_404_on_unknown_key() {
        let (registry, dev_state) = make_count_agg_registry();
        // Push events for alice only
        push_events(&dev_state, "Transaction", "alice", 5.0, 3);
        let r = router(ReadinessFlag::new(), registry, true, Some(dev_state));
        let (status, body) = call_get(r, "/get/cnt/bob").await;
        assert_eq!(status, StatusCode::NOT_FOUND, "body: {body:#}");
        assert_eq!(body["error"]["code"], "key_not_found", "body: {body:#}");
    }

    /// get_endpoint_handles_sum_feature_returning_float:
    /// Push 2 events with amount=50.0; GET /get/total/alice → {"value": 100.0}
    #[tokio::test]
    async fn get_endpoint_handles_sum_feature_returning_float() {
        let (registry, dev_state) = make_count_agg_registry();
        push_events(&dev_state, "Transaction", "alice", 50.0, 2);
        let r = router(ReadinessFlag::new(), registry, true, Some(dev_state));
        let (status, body) = call_get(r, "/get/total/alice").await;
        assert_eq!(status, StatusCode::OK, "body: {body:#}");
        let val = body["value"].as_f64().expect("value must be a number");
        assert!(
            (val - 100.0).abs() < 1e-9,
            "expected total=100.0, got {val}, body: {body:#}"
        );
    }

    /// get_endpoint_respects_envelope_shape:
    /// Response has exactly one top-level key "value" (D-02: no "meta", no "updated_at").
    #[tokio::test]
    async fn get_endpoint_respects_envelope_shape() {
        let (registry, dev_state) = make_count_agg_registry();
        push_events(&dev_state, "Transaction", "alice", 1.0, 1);
        let r = router(ReadinessFlag::new(), registry, true, Some(dev_state));
        let (status, body) = call_get(r, "/get/cnt/alice").await;
        assert_eq!(status, StatusCode::OK, "body: {body:#}");
        let obj = body.as_object().expect("response must be a JSON object");
        assert_eq!(
            obj.len(),
            1,
            "D-02: response must have exactly 1 key, got: {:?}",
            obj.keys().collect::<Vec<_>>()
        );
        assert!(
            obj.contains_key("value"),
            "D-02: response must contain key 'value', got: {:?}",
            obj.keys().collect::<Vec<_>>()
        );
        assert!(
            !obj.contains_key("meta"),
            "D-02: response must NOT contain 'meta'"
        );
        assert!(
            !obj.contains_key("updated_at"),
            "D-02: response must NOT contain 'updated_at'"
        );
    }

    // ── POST /get batch tests ─────────────────────────────────────────────────

    /// post_get_batch_returns_map_of_results:
    /// Push events for alice and bob; POST /get → {"result": {"alice": {...}, "bob": {...}}}
    #[tokio::test]
    async fn post_get_batch_returns_map_of_results() {
        let (registry, dev_state) = make_count_agg_registry();
        push_events(&dev_state, "Transaction", "alice", 10.0, 3);
        push_events(&dev_state, "Transaction", "bob", 20.0, 2);
        let r = router(ReadinessFlag::new(), registry, true, Some(dev_state));

        let (status, body) = call_post(
            r,
            "/get",
            serde_json::json!({"keys": ["alice", "bob"], "features": ["cnt", "total"]}),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "body: {body:#}");
        assert!(body["result"].is_object(), "body: {body:#}");
        assert_eq!(body["result"]["alice"]["cnt"], 3, "body: {body:#}");
        assert_eq!(body["result"]["bob"]["cnt"], 2, "body: {body:#}");
    }

    /// post_get_batch_400_on_unknown_feature:
    /// features contains "unknown" → 400 with missing: ["unknown"]
    #[tokio::test]
    async fn post_get_batch_400_on_unknown_feature() {
        let (registry, dev_state) = make_count_agg_registry();
        let r = router(ReadinessFlag::new(), registry, true, Some(dev_state));

        let (status, body) = call_post(
            r,
            "/get",
            serde_json::json!({"keys": ["alice"], "features": ["cnt", "unknown_feat"]}),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "body: {body:#}");
        assert_eq!(body["error"]["code"], "feature_not_found", "body: {body:#}");
        let missing = body["error"]["missing"]
            .as_array()
            .expect("missing must be an array");
        assert!(
            missing.iter().any(|v| v == "unknown_feat"),
            "missing must include 'unknown_feat', got: {missing:#?}"
        );
    }

    /// post_get_batch_omits_missing_keys:
    /// One of the keys has no state → omitted from result map (not null).
    #[tokio::test]
    async fn post_get_batch_omits_missing_keys() {
        let (registry, dev_state) = make_count_agg_registry();
        push_events(&dev_state, "Transaction", "alice", 1.0, 1);
        // "ghost" has never had events pushed.
        let r = router(ReadinessFlag::new(), registry, true, Some(dev_state));

        let (status, body) = call_post(
            r,
            "/get",
            serde_json::json!({"keys": ["alice", "ghost"], "features": ["cnt"]}),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "body: {body:#}");
        assert!(
            body["result"]["alice"].is_object(),
            "alice must be present, body: {body:#}"
        );
        assert!(
            body["result"]["ghost"].is_null(),
            "ghost must be omitted (null in serde_json), body: {body:#}"
        );
    }

    /// post_get_batch_respects_cap_10k:
    /// keys × features > 10_000 → 400 with batch_too_large
    #[tokio::test]
    async fn post_get_batch_respects_cap_10k() {
        let (registry, dev_state) = make_count_agg_registry();
        let r = router(ReadinessFlag::new(), registry, true, Some(dev_state));

        // 10_001 keys × 1 feature = 10_001 cells
        let keys: Vec<String> = (0..10_001).map(|i| format!("key{i}")).collect();
        let (status, body) = call_post(
            r,
            "/get",
            serde_json::json!({"keys": keys, "features": ["cnt"]}),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "body: {body:#}");
        assert_eq!(body["error"]["code"], "batch_too_large", "body: {body:#}");
    }

    // ── cross-aggregation feature-name collision rule ─────────────────────────

    /// rule11_rejects_cross_aggregation_feature_name_collision:
    /// Two aggregations both define feature "cnt" → register should return 400
    /// with code aggregation_feature_name_collision_across_aggregations.
    #[tokio::test]
    async fn rule11_rejects_cross_aggregation_feature_name_collision() {
        use crate::http::ReadinessFlag;
        use axum::body::Body;
        use axum::http::Request;
        use beava_core::registry::Registry;

        let registry = Arc::new(Registry::new());
        let r = router(ReadinessFlag::new(), registry.clone(), false, None);

        // First registration: Transaction + AggA with feature "cnt"
        let payload1 = serde_json::json!({
            "nodes": [
                {
                    "kind": "event",
                    "name": "Transaction",
                    "schema": {"fields": {"event_time": "i64", "user_id": "str", "amount": "f64"}, "optional_fields": []},
                },
                {
                    "kind": "derivation",
                    "name": "AggA",
                    "output_kind": "table",
                    "upstreams": ["Transaction"],
                    "ops": [{"op": "group_by", "keys": ["user_id"], "agg": {
                        "cnt": {"op": "count", "params": {"window": "5m"}}
                    }}],
                    "schema": {"fields": {"user_id": "str", "cnt": "i64"}, "optional_fields": []},
                    "table_primary_key": ["user_id"]
                }
            ]
        });
        let req1 = Request::builder()
            .method(Method::POST)
            .uri("/register")
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(&payload1).unwrap()))
            .unwrap();
        let r1 = router(ReadinessFlag::new(), registry.clone(), false, None);
        let resp1 = r1.oneshot(req1).await.expect("oneshot");
        assert_eq!(
            resp1.status(),
            StatusCode::OK,
            "first registration must succeed"
        );

        // Second registration: SaleEvent + AggB also with feature "cnt" (collision!)
        let payload2 = serde_json::json!({
            "nodes": [
                {
                    "kind": "event",
                    "name": "SaleEvent",
                    "schema": {"fields": {"event_time": "i64", "merchant_id": "str"}, "optional_fields": []},
                },
                {
                    "kind": "derivation",
                    "name": "AggB",
                    "output_kind": "table",
                    "upstreams": ["SaleEvent"],
                    "ops": [{"op": "group_by", "keys": ["merchant_id"], "agg": {
                        "cnt": {"op": "count", "params": {"window": "1h"}}
                    }}],
                    "schema": {"fields": {"merchant_id": "str", "cnt": "i64"}, "optional_fields": []},
                    "table_primary_key": ["merchant_id"]
                }
            ]
        });
        let req2 = Request::builder()
            .method(Method::POST)
            .uri("/register")
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(&payload2).unwrap()))
            .unwrap();
        let r2 = router(ReadinessFlag::new(), registry.clone(), false, None);
        let resp2 = r2.oneshot(req2).await.expect("oneshot");
        let status2 = resp2.status();
        let bytes2 = resp2
            .into_body()
            .collect()
            .await
            .expect("collect")
            .to_bytes();
        let body2: serde_json::Value = serde_json::from_slice(&bytes2).expect("json");

        assert_eq!(
            status2,
            StatusCode::BAD_REQUEST,
            "cross-agg collision must return 400, body: {body2:#}"
        );
        assert_eq!(
            body2["error"]["code"], "aggregation_feature_name_collision_across_aggregations",
            "expected collision error code, body: {body2:#}"
        );

        let _ = r; // suppress unused
    }

    /// get_windowed_count_uses_max_event_time:
    /// Push events at various event_times; GET /get/cnt/alice is deterministic
    /// across re-queries if no new events arrive (D-06 compliance).
    #[tokio::test]
    async fn get_windowed_count_uses_max_event_time() {
        let (registry, dev_state) = make_count_agg_registry();
        push_events(&dev_state, "Transaction", "alice", 1.0, 5);

        let r1 = router(
            ReadinessFlag::new(),
            registry.clone(),
            true,
            Some(dev_state.clone()),
        );
        let (s1, b1) = call_get(r1, "/get/cnt/alice").await;
        assert_eq!(s1, StatusCode::OK);

        // Second query with no new events — must return same value.
        let r2 = router(
            ReadinessFlag::new(),
            registry.clone(),
            true,
            Some(dev_state),
        );
        let (s2, b2) = call_get(r2, "/get/cnt/alice").await;
        assert_eq!(s2, StatusCode::OK);
        assert_eq!(
            b1["value"], b2["value"],
            "D-06: same events → same query result"
        );
    }

    // ── Grep guard: D-02 envelope purity ─────────────────────────────────────

    /// Asserts the production code in this file does NOT contain the string "meta"
    /// as a JSON response key (D-02 envelope must be {value} only).
    #[test]
    fn envelope_purity_no_meta_key_in_production_code() {
        // Read the source of this file up to the test module boundary.
        let src = include_str!("feature_query.rs");
        let test_marker = "#[cfg(test)]";
        let production_src = src.split(test_marker).next().unwrap_or("");
        // "meta" must not appear as a JSON key in production code.
        // We look for `"meta"` (with quotes, as it would appear in json! macros).
        assert!(
            !production_src.contains("\"meta\""),
            "D-02: production code in feature_query.rs must not contain '\"meta\"' key"
        );
    }

    /// Asserts the production code in this file does NOT contain `SystemTime::now`
    /// (D-06: query time must not use wall-clock).
    #[test]
    fn d06_no_system_time_now_in_production_code() {
        let src = include_str!("feature_query.rs");
        let test_marker = "#[cfg(test)]";
        let production_src = src.split(test_marker).next().unwrap_or("");
        assert!(
            !production_src.contains("SystemTime::now"),
            "D-06: production code in feature_query.rs must not call SystemTime::now"
        );
    }
}
