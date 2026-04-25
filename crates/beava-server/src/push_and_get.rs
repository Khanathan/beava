//! Plan 18-07 Task 7.3 — POST /push-and-get/:event and /push-sync-and-get/:event
//!
//! Combined atomic push + feature-query endpoint (Phase 12.5 SC1, SC2, SC5).
//!
//! Atomicity: push applies, then feature query runs, both under the same
//! `state_tables.lock()` scope. Read-your-writes by construction (D-01).
//!
//! WAL durability modes:
//! - `/push-and-get` → `SyncMode::Periodic` (acks=1; matches `/push`)
//! - `/push-sync-and-get` → `SyncMode::PerEvent` (acks=all; matches `/push-sync`)
//!   The fsync `.await` happens BEFORE `state_tables.lock()` to avoid holding
//!   the lock across an await (Phase 12.5 GA1 resolution).
//!
//! Request body:
//! ```json
//! {
//!   "row": { ...event fields... },
//!   "query": {
//!     "entity_key": { "group_key_name": "value", ... },
//!     "features": ["f1", "f2"]
//!   }
//! }
//! ```
//!
//! Response body:
//! ```json
//! {
//!   "ack_lsn": 12345,
//!   "registry_version": 42,
//!   "features": { "f1": 17, "f2": null },
//!   "warnings": ["unknown_feature: f2"]
//! }
//! ```

use crate::push::execute_push;
use crate::push::PushOutcome;
use crate::AppState;
use axum::{
    body::Bytes,
    extract::{Path, State},
    http::{header, StatusCode},
    response::Response,
    routing::post,
    Router,
};
use beava_core::agg_state_table::EntityKey;
use beava_core::row::Value;
use beava_persistence::SyncMode;
use serde::Deserialize;
use serde_json::Value as JsonValue;
use std::collections::BTreeMap;
use std::sync::atomic::Ordering;
use std::sync::Arc;

// EntityKey is Vec<(String, String)> — pairs of (group_key_name, value).

// ─── Request / response types ─────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
pub struct PushAndGetRequest {
    pub row: serde_json::Value,
    pub query: PushAndGetQuery,
}

#[derive(Debug, Deserialize)]
pub struct PushAndGetQuery {
    pub entity_key: BTreeMap<String, String>,
    pub features: Vec<String>,
}

fn error_response(status: StatusCode, body: serde_json::Value) -> Response {
    let bytes = serde_json::to_vec(&body).unwrap_or_default();
    Response::builder()
        .status(status)
        .header(header::CONTENT_TYPE, "application/json")
        .body(axum::body::Body::from(bytes))
        .unwrap()
}

fn ok_response(body: serde_json::Value) -> Response {
    let bytes = serde_json::to_vec(&body).unwrap_or_default();
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/json")
        .body(axum::body::Body::from(bytes))
        .unwrap()
}

/// Convert an aggregation `Value` to a `serde_json::Value`.
fn value_to_json(v: Value) -> serde_json::Value {
    match v {
        Value::Null => JsonValue::Null,
        Value::Bool(b) => JsonValue::Bool(b),
        Value::I64(n) => JsonValue::Number(n.into()),
        Value::F64(f) => serde_json::Number::from_f64(f)
            .map(JsonValue::Number)
            .unwrap_or(JsonValue::Null),
        Value::Str(s) => JsonValue::String(s),
        Value::Bytes(_) | Value::List(_) | Value::Map(_) => JsonValue::Null,
        Value::Datetime(ms) => JsonValue::Number(ms.into()),
        Value::Json(j) => j,
    }
}

/// Execute a combined push + feature query atomically.
///
/// - `sync_mode::Periodic`: acks=1; WAL append returns before fsync.
/// - `sync_mode::PerEvent`: acks=all; fsync completes BEFORE the state lock
///   is acquired (GA1 compliance — no await inside state_tables.lock()).
pub async fn execute_push_and_get(
    app: &Arc<AppState>,
    event_name: &str,
    body_bytes: &[u8],
    sync_mode: SyncMode,
) -> Response {
    let registry_version = app.dev_agg.registry.version() as u32;

    // Parse the combined request body.
    let req: PushAndGetRequest = match serde_json::from_slice(body_bytes) {
        Ok(r) => r,
        Err(_) => {
            return error_response(
                StatusCode::BAD_REQUEST,
                serde_json::json!({
                    "error": {"code": "invalid_json"},
                    "registry_version": registry_version,
                }),
            );
        }
    };

    // Re-serialize just the row part as the push body.
    let row_bytes = match serde_json::to_vec(&req.row) {
        Ok(b) => b,
        Err(_) => {
            return error_response(
                StatusCode::BAD_REQUEST,
                serde_json::json!({
                    "error": {"code": "invalid_row"},
                    "registry_version": registry_version,
                }),
            );
        }
    };

    // Execute push. For SyncMode::PerEvent the WAL fsync completes inside
    // execute_push before returning — no await is held across state_tables.lock().
    let ack_lsn;
    let updated_registry_version;
    match execute_push(app, event_name, &row_bytes, sync_mode).await {
        PushOutcome::Ok { ack, .. } => {
            ack_lsn = ack.ack_lsn;
            updated_registry_version = ack.registry_version;
        }
        PushOutcome::IdempotentReplay { cached_response_bytes } => {
            // Idempotent replay: parse ack_lsn from cached bytes; still run query.
            let cached: serde_json::Value =
                serde_json::from_slice(&cached_response_bytes).unwrap_or_default();
            ack_lsn = cached["ack_lsn"].as_u64().unwrap_or(0);
            updated_registry_version = cached["registry_version"].as_u64().unwrap_or(registry_version as u64) as u32;
        }
        PushOutcome::Error {
            http_status,
            code,
            registry_version: rv,
        } => {
            return error_response(
                http_status,
                serde_json::json!({
                    "error": {"code": code},
                    "registry_version": rv,
                }),
            );
        }
    }

    // Query features. The push has already applied — state_tables contains
    // the just-pushed event. Lock scope spans the entire query batch.
    let query_time_ms = {
        let raw = app
            .dev_agg
            .max_event_time_ms
            .load(Ordering::Acquire);
        if raw == 0 {
            0i64
        } else {
            raw as i64
        }
    };

    let mut features: BTreeMap<String, JsonValue> = BTreeMap::new();
    let mut warnings: Vec<String> = Vec::new();

    {
        let registry = &app.dev_agg.registry;
        let tables = app.dev_agg.state_tables.lock();

        for feat_name in &req.query.features {
            match registry.resolve_feature(feat_name) {
                None => {
                    features.insert(feat_name.clone(), JsonValue::Null);
                    warnings.push(format!("unknown_feature: {feat_name}"));
                }
                Some((agg_node, feat_idx)) => {
                    // Build entity key from query.entity_key using the agg descriptor's group_keys.
                    let descriptor = match registry.compiled_aggregation(&agg_node) {
                        Some(d) => d,
                        None => {
                            features.insert(feat_name.clone(), JsonValue::Null);
                            continue;
                        }
                    };

                    // Match entity_key map to group_key order.
                    let entity_key = build_entity_key(&req.query.entity_key, &descriptor.group_keys);

                    let val = tables
                        .get(&agg_node)
                        .and_then(|t| t.query_feature(&entity_key, feat_idx, query_time_ms));

                    features.insert(
                        feat_name.clone(),
                        val.map(value_to_json).unwrap_or(JsonValue::Null),
                    );
                }
            }
        }
    }

    let mut resp_body = serde_json::json!({
        "ack_lsn": ack_lsn,
        "registry_version": updated_registry_version,
        "features": features,
    });

    if !warnings.is_empty() {
        resp_body["warnings"] = serde_json::json!(warnings);
    }

    ok_response(resp_body)
}

/// Build an `EntityKey` from the query's `entity_key` map using the descriptor's
/// group_keys ordering. Missing keys default to empty string.
fn build_entity_key(
    key_map: &BTreeMap<String, String>,
    group_keys: &[String],
) -> EntityKey {
    // EntityKey is Vec<(String, String)> — pairs of (group_key_name, value).
    let pairs: Vec<(String, String)> = group_keys
        .iter()
        .map(|gk| {
            let val = key_map.get(gk).cloned().unwrap_or_default();
            (gk.clone(), val)
        })
        .collect();
    EntityKey(pairs)
}

// ─── Axum handlers ────────────────────────────────────────────────────────────

pub async fn push_and_get_handler(
    State(app): State<Arc<AppState>>,
    Path(event_name): Path<String>,
    body: Bytes,
) -> Response {
    execute_push_and_get(&app, &event_name, &body, SyncMode::Periodic).await
}

pub async fn push_sync_and_get_handler(
    State(app): State<Arc<AppState>>,
    Path(event_name): Path<String>,
    body: Bytes,
) -> Response {
    execute_push_and_get(&app, &event_name, &body, SyncMode::PerEvent).await
}

/// Build the push-and-get sub-router (Phase 12.5 / Plan 18-07).
pub fn push_and_get_router(state: Arc<AppState>) -> Router {
    Router::new()
        .route(
            "/push-and-get/:event_name",
            post(push_and_get_handler),
        )
        .route(
            "/push-sync-and-get/:event_name",
            post(push_sync_and_get_handler),
        )
        .with_state(state)
}
