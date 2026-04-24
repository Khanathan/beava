//! Phase 6 Plan 03 — POST /push/{event_name} handler.
//!
//! Flow (D-11, D-12 refinement — apply-after-fsync; see 06-CONTEXT.md):
//! 1. Content-type check → 415 if missing.
//! 2. Parse JSON body → 400 on parse error.
//! 3. Lookup event descriptor → 404 on miss.
//! 4. Schema validation (field presence + type compatibility) → 400 on failure.
//! 5. If descriptor has `dedupe_key`, extract the value and consult the
//!    IdemCache. On hit, return the cached bytes with the
//!    `X-Beava-Idempotent-Replay: 1` header set.
//! 6. Convert body to Row<Value>.
//! 7. WAL-append the serialized event payload. `append_event(...).await`
//!    resolves after fsync.
//! 8. Apply the event to aggregations under the single-writer lock.
//! 9. Bump max_event_time_ms.
//! 10. Build response `{ack_lsn, idempotent_replay: false, registry_version}`.
//! 11. On dedupe-enabled path, insert the cached entry with
//!     (now_ms + dedupe_window).
//! 12. Return 200 with `application/json` body.

use crate::AppState;
use axum::{
    body::Bytes,
    extract::{Path, State},
    http::{header, StatusCode},
    response::{IntoResponse, Response},
    Router,
};
use beava_core::agg_apply::apply_event_to_aggregations;
use beava_core::defaults::DEFAULT_DEDUPE_WINDOW_MS;
use beava_core::registry::EventDescriptor;
use beava_core::row::{Row, Value};
use beava_core::schema::FieldType;
use serde::Serialize;
use serde_json::Value as JsonValue;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Serialize)]
pub struct PushAck {
    pub ack_lsn: u64,
    pub idempotent_replay: bool,
    pub registry_version: u32,
}

pub fn push_router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/push/:event_name", axum::routing::post(push_handler))
        .with_state(state)
}

/// Wall-clock helper. Kept private so tests can mock via injecting event_time.
fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

fn error_response(status: StatusCode, code: &str, registry_version: u32) -> Response {
    let body = serde_json::json!({
        "error": {"code": code},
        "registry_version": registry_version,
    });
    let bytes = serde_json::to_vec(&body).unwrap_or_default();
    Response::builder()
        .status(status)
        .header(header::CONTENT_TYPE, "application/json")
        .body(axum::body::Body::from(bytes))
        .unwrap()
}

/// Validate a JSON body against the event descriptor schema.
///
/// Each required field in `descriptor.schema.fields` (that is not in
/// `optional_fields`) must be present and type-compatible:
/// - Str     → JSON string
/// - I64     → JSON number (integer-coercible)
/// - F64     → JSON number
/// - Bool    → JSON boolean
/// - Datetime → JSON number (ms since epoch)
/// - Bytes    → reject in v0 (JSON has no binary)
fn validate_body(descriptor: &EventDescriptor, body: &serde_json::Map<String, JsonValue>) -> bool {
    for (field, ty) in &descriptor.schema.fields {
        if descriptor.schema.optional_fields.contains(field) {
            continue;
        }
        let jv = match body.get(field) {
            Some(v) => v,
            None => return false,
        };
        let ok = match ty {
            FieldType::Str => jv.is_string(),
            FieldType::I64 => jv.is_i64() || jv.is_u64(),
            FieldType::F64 => jv.is_f64() || jv.is_i64() || jv.is_u64(),
            FieldType::Bool => jv.is_boolean(),
            FieldType::Datetime => jv.is_i64() || jv.is_u64(),
            FieldType::Bytes => false,
        };
        if !ok {
            return false;
        }
    }
    true
}

/// Convert JSON → Row<Value> for the apply path.
fn json_object_to_row(body: &serde_json::Map<String, JsonValue>) -> Row {
    let mut row = Row::new();
    for (field, jv) in body {
        let v = match jv {
            JsonValue::Null => Value::Null,
            JsonValue::Bool(b) => Value::Bool(*b),
            JsonValue::Number(n) => {
                if let Some(i) = n.as_i64() {
                    Value::I64(i)
                } else if let Some(f) = n.as_f64() {
                    Value::F64(f)
                } else {
                    Value::Null
                }
            }
            JsonValue::String(s) => Value::Str(s.clone()),
            _ => Value::Null,
        };
        row = row.with_field(field.as_str(), v);
    }
    row
}

/// Extract the dedupe key value as a string. Numbers and bools are coerced.
fn extract_dedupe_str(body: &serde_json::Map<String, JsonValue>, key: &str) -> Option<String> {
    match body.get(key)? {
        JsonValue::String(s) => Some(s.clone()),
        JsonValue::Number(n) => Some(n.to_string()),
        JsonValue::Bool(b) => Some(b.to_string()),
        _ => None,
    }
}

pub async fn push_handler(
    State(app): State<Arc<AppState>>,
    Path(event_name): Path<String>,
    body_bytes: Bytes,
) -> Response {
    let registry_version = app.dev_agg.registry.version() as u32;

    // 1. Parse JSON body.
    let parsed: JsonValue = match serde_json::from_slice(&body_bytes) {
        Ok(v) => v,
        Err(_) => {
            return error_response(StatusCode::BAD_REQUEST, "invalid_event", registry_version)
        }
    };
    let obj = match parsed.as_object() {
        Some(o) => o.clone(),
        None => return error_response(StatusCode::BAD_REQUEST, "invalid_event", registry_version),
    };

    // 2. Lookup event descriptor.
    let descriptor = {
        let inner = app.dev_agg.registry.read();
        match inner.events.get(&event_name).cloned() {
            Some(d) => d,
            None => {
                return error_response(StatusCode::NOT_FOUND, "event_not_found", registry_version);
            }
        }
    };

    // 3. Schema validate.
    if !validate_body(&descriptor, &obj) {
        return error_response(StatusCode::BAD_REQUEST, "invalid_event", registry_version);
    }

    let now = now_ms();

    // 4. Dedupe lookup.
    let dedupe_str = descriptor
        .dedupe_key
        .as_deref()
        .and_then(|k| extract_dedupe_str(&obj, k));

    if let (Some(_), Some(key_str)) = (descriptor.dedupe_key.as_ref(), dedupe_str.as_ref()) {
        if let Some(cached) = app.idem_cache.get(&event_name, key_str, now) {
            return Response::builder()
                .status(StatusCode::OK)
                .header(header::CONTENT_TYPE, "application/json")
                .header("x-beava-idempotent-replay", "1")
                .body(axum::body::Body::from(cached))
                .unwrap();
        }
    }

    // 5. Extract event_time_ms.
    let event_time_ms = descriptor
        .event_time_field
        .as_deref()
        .and_then(|f| obj.get(f))
        .and_then(|jv| jv.as_i64())
        .unwrap_or(now as i64);

    // 6. Serialize the WAL payload.
    let payload = serde_json::json!({
        "v": 1,
        "rv": registry_version,
        "s": event_name,
        "et": event_time_ms,
        "b": parsed,
    });
    let payload_bytes = match serde_json::to_vec(&payload) {
        Ok(b) => b,
        Err(_) => {
            return error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "serialize_failed",
                registry_version,
            );
        }
    };

    // 7. Durable WAL append (resolves after fsync).
    let ack_lsn = match app.wal_sink.append_event(payload_bytes).await {
        Ok(lsn) => lsn,
        Err(_) => {
            return error_response(
                StatusCode::SERVICE_UNAVAILABLE,
                "wal_unavailable",
                registry_version,
            );
        }
    };

    // 8. Apply to aggregations under the single-writer lock.
    let row = json_object_to_row(&obj);
    {
        let mut tables = app.dev_agg.state_tables.lock();
        apply_event_to_aggregations(
            &event_name,
            &row,
            event_time_ms,
            ack_lsn,
            &app.dev_agg.registry,
            &mut tables,
        );
    }

    // 9. Bump monotonic event counters.
    app.dev_agg
        .next_event_id
        .fetch_max(ack_lsn, Ordering::Relaxed);
    if event_time_ms > 0 {
        app.dev_agg
            .max_event_time_ms
            .fetch_max(event_time_ms as u64, Ordering::Relaxed);
    }

    // Phase 11.5 D-10/D-12 — record this LSN as a stream event so a future
    // POST /retract can route to 501 (stream retraction is v1).
    {
        use crate::registry_debug::EventIdEntry;
        let mut idx = app.dev_agg.event_id_index.lock();
        idx.insert(
            ack_lsn,
            EventIdEntry::Stream {
                event_name: event_name.clone(),
            },
        );
    }

    // 10. Build response.
    let ack = PushAck {
        ack_lsn,
        idempotent_replay: false,
        registry_version,
    };
    let body_vec = serde_json::to_vec(&ack).unwrap_or_default();
    let body_bytes_out = Bytes::from(body_vec);

    // 11. Cache on dedupe path.
    if let Some(key_str) = dedupe_str {
        let window_ms = descriptor
            .dedupe_window_ms
            .unwrap_or(DEFAULT_DEDUPE_WINDOW_MS);
        app.idem_cache.put(
            event_name.clone(),
            key_str,
            crate::idem_cache::CachedEntry {
                response_bytes: body_bytes_out.clone(),
                ack_lsn,
                inserted_at_ms: now,
                expires_at_ms: now.saturating_add(window_ms),
            },
        );
    }

    // 12. Return 200.
    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "application/json")],
        body_bytes_out,
    )
        .into_response()
}
