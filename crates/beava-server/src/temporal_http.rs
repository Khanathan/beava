//! Phase 11.5 + Plan 18-07 — HTTP handlers for temporal-table writes, `as_of`
//! queries, and the `app.retract(event_id)` primitive.
//!
//! Routes (Plan 18-07 Phase 16 rename applied):
//!
//! - `POST /upsert/{table_name}` — body is the row JSON. Writes a
//!   `TableUpsert` WAL record + applies it to the per-table MVCC store
//!   (when the table is temporal). Renamed from `/push-table` per Phase 16 D-09.
//!
//! - `POST /delete/{table_name}` — body is `{"key": {…}}`. Retracts the
//!   identified row from the temporal store. Added in Plan 18-07.
//!
//! - `POST /retract` — body `{"event_id": <u64>}`. Looks up the LSN in
//!   `event_id_index`, dispatches:
//!     - missing → 404 `event_id_not_found`
//!     - stream → 501 `stream_retraction_unimplemented` (D-12)
//!     - non-temporal table → 400 `table_not_temporal` (D-17)
//!     - temporal table → write `Retract` WAL record + insert the
//!       `Retracted{undo_of}` marker via `TemporalStore::retract` (D-04)
//!
//! - `GET /table/{table_name}?key=<v>&as_of=<lsn>` — point lookup. With
//!   `as_of` set on a non-temporal table → 400 `as_of_requires_temporal`
//!   (D-08). Without `as_of`, returns the current visible row.
//!
//! v0 simplifying constraints:
//! - Single-field primary key only — composite keys are out of scope for
//!   the smoke (multi-field encoding stays D-03; handler extension is a
//!   follow-up).
//! - Old routes `/push-table/{name}` and `/delete-table/{name}` removed per
//!   Phase 16 GA-2 (hard break — no deprecation aliases).

use crate::registry_debug::EventIdEntry;
use crate::AppState;
use beava_core::registry::TableDescriptor;
use beava_core::row::{Row, Value};
use beava_core::temporal::RetractError;
use beava_persistence::RecordType;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

/// Bare-bones query-string parser — avoids pulling in `axum/query` feature
/// (workspace builds with `default-features = false` and a minimal feature
/// set). Returns the first occurrence of each key. Unsuitable for repeated
/// keys but matches v0 query-string surface.
fn parse_query_pairs(q: &str) -> std::collections::HashMap<String, String> {
    let mut out = std::collections::HashMap::new();
    for pair in q.split('&') {
        if pair.is_empty() {
            continue;
        }
        let mut it = pair.splitn(2, '=');
        let k = it.next().unwrap_or("");
        let v = it.next().unwrap_or("");
        // Minimal percent-decode: assume sane test inputs (no %xx in v0).
        out.entry(k.to_string()).or_insert_with(|| v.to_string());
    }
    out
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

// Plan 12.6-07: legacy axum `error_response` / `ok_response` helpers
// deleted along with the four axum handlers below. Mio dispatch uses the
// `*_via_mio` helpers which return `(u16, Vec<u8>)` directly.

/// D-03 byte encoding of the entity key from the table descriptor and a
/// JSON object body. v0: single-field primary keys only.
fn entity_key_from_body(
    descriptor: &TableDescriptor,
    body: &serde_json::Map<String, JsonValue>,
) -> Option<Vec<u8>> {
    if descriptor.primary_key.len() != 1 {
        // v0 supports single-field PK only — composite keys arrive in a
        // follow-up plan (CONTEXT.md notes this constraint).
        return None;
    }
    let field = &descriptor.primary_key[0];
    let v = body.get(field)?;
    let s = match v {
        JsonValue::String(s) => s.clone(),
        JsonValue::Number(n) => n.to_string(),
        JsonValue::Bool(b) => b.to_string(),
        _ => return None,
    };
    // Length-prefix encoding for forward-compat with composite keys (D-03).
    let mut out = Vec::with_capacity(4 + s.len());
    out.extend_from_slice(&(s.len() as u32).to_le_bytes());
    out.extend_from_slice(s.as_bytes());
    Some(out)
}

fn entity_key_from_str(value: &str, primary_key_len: usize) -> Option<Vec<u8>> {
    if primary_key_len != 1 {
        return None;
    }
    let mut out = Vec::with_capacity(4 + value.len());
    out.extend_from_slice(&(value.len() as u32).to_le_bytes());
    out.extend_from_slice(value.as_bytes());
    Some(out)
}

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
            JsonValue::String(s) => Value::Str(s.clone().into()),
            _ => Value::Null,
        };
        row = row.with_field(field.as_str(), v);
    }
    row
}

fn row_to_json(row: &Row) -> serde_json::Value {
    let mut obj = serde_json::Map::new();
    for (k, v) in &row.0 {
        let jv = match v {
            Value::Null => JsonValue::Null,
            Value::Bool(b) => JsonValue::Bool(*b),
            Value::I64(n) => JsonValue::Number((*n).into()),
            Value::F64(f) => serde_json::Number::from_f64(*f)
                .map(JsonValue::Number)
                .unwrap_or(JsonValue::Null),
            Value::Str(s) => JsonValue::String(s.to_string()),
            Value::Bytes(_) => JsonValue::Null,
            Value::Datetime(ms) => JsonValue::Number((*ms).into()),
            Value::Json(j) => j.clone(),
            // Phase 11: structured outputs not expected in row payloads.
            Value::List(_) | Value::Map(_) => JsonValue::Null,
        };
        obj.insert(k.to_string(), jv);
    }
    JsonValue::Object(obj)
}

#[derive(Debug, Serialize)]
struct PushTableAck {
    ack_lsn: u64,
    registry_version: u32,
}

/// Pure helper that performs an upsert against `AppState`, returning the
/// HTTP status code and a pre-serialised body. Called from
/// `apply_shard.rs::dispatch_one` on the mio data plane.
///
/// Returns `{"ack_lsn", "registry_version"}` on success; `{"error":...}`
/// on failure. Async because `wal_sink.append_record` is async; the mio
/// path runs this on a temp current-thread tokio runtime (same approach
/// as the existing register dispatch on the apply thread).
pub async fn upsert_via_mio(
    app: &Arc<AppState>,
    table_name: &str,
    body_bytes: &[u8],
) -> (u16, Vec<u8>) {
    let registry_version = app.dev_agg.registry.version() as u32;

    // 1. Parse JSON.
    let parsed: JsonValue = match serde_json::from_slice(body_bytes) {
        Ok(v) => v,
        Err(_) => {
            return (
                400,
                serde_json::to_vec(&serde_json::json!({
                    "error": "invalid_json",
                    "registry_version": registry_version,
                }))
                .unwrap_or_default(),
            );
        }
    };
    let obj = match parsed.as_object() {
        Some(o) => o.clone(),
        None => {
            return (
                400,
                serde_json::to_vec(&serde_json::json!({
                    "error": "expected_object",
                    "registry_version": registry_version,
                }))
                .unwrap_or_default(),
            );
        }
    };

    // 2. Lookup table descriptor.
    let descriptor = {
        let inner = app.dev_agg.registry.read();
        match inner.tables.get(table_name).cloned() {
            Some(d) => d,
            None => {
                return (
                    404,
                    serde_json::to_vec(&serde_json::json!({
                        "error": "table_not_found",
                        "table": table_name,
                    }))
                    .unwrap_or_default(),
                );
            }
        }
    };

    // 3. Encode entity key.
    let entity_key = match entity_key_from_body(&descriptor, &obj) {
        Some(k) => k,
        None => {
            return (
                400,
                serde_json::to_vec(&serde_json::json!({
                    "error": "missing_or_unsupported_primary_key",
                    "primary_key": descriptor.primary_key,
                }))
                .unwrap_or_default(),
            );
        }
    };

    // 4. Build WAL payload.
    let payload = serde_json::json!({
        "v": 1,
        "rv": registry_version,
        "t": table_name,
        "k": entity_key,
        "b": parsed,
    });
    let payload_bytes = match serde_json::to_vec(&payload) {
        Ok(b) => b,
        Err(_) => {
            return (
                500,
                serde_json::to_vec(&serde_json::json!({"error": "serialize_failed"}))
                    .unwrap_or_default(),
            );
        }
    };

    // 5. Durable WAL append.
    let ack_lsn = match app
        .wal_sink
        .append_record(RecordType::TableUpsert, payload_bytes)
        .await
    {
        Ok(lsn) => lsn,
        Err(_) => {
            return (
                503,
                serde_json::to_vec(&serde_json::json!({"error": "wal_unavailable"}))
                    .unwrap_or_default(),
            );
        }
    };

    // 6. Apply to MVCC store (only if temporal). Single-writer lock.
    let now = now_ms();
    let row = json_object_to_row(&obj);
    if descriptor.temporal {
        let mut stores = app.dev_agg.temporal_stores.lock();
        let store = stores.entry(table_name.to_owned()).or_default();
        store.upsert(entity_key.clone(), ack_lsn, row, now);
    }

    // 7. Update event_id_index.
    {
        let mut idx = app.dev_agg.event_id_index.lock();
        idx.insert(
            ack_lsn,
            EventIdEntry::TableWrite {
                table_name: table_name.to_owned(),
                entity_key,
                retracted: false,
            },
        );
    }

    // 8. Bump monotonic counter.
    app.dev_agg
        .next_event_id
        .fetch_max(ack_lsn, std::sync::atomic::Ordering::Relaxed);

    let ack = PushTableAck {
        ack_lsn,
        registry_version,
    };
    (200, serde_json::to_vec(&ack).unwrap_or_default())
}

// ─── DELETE handler ───────────────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
struct DeleteRequest {
    key: serde_json::Map<String, JsonValue>,
}

/// Helper for `POST /delete/:table` on the mio data-plane dispatch.
/// Returns `(status, body_bytes)`.
pub async fn delete_via_mio(
    app: &Arc<AppState>,
    table_name: &str,
    body_bytes: &[u8],
) -> (u16, Vec<u8>) {
    let registry_version = app.dev_agg.registry.version() as u32;

    // 1. Parse JSON.
    let parsed: JsonValue = match serde_json::from_slice(body_bytes) {
        Ok(v) => v,
        Err(_) => {
            return (
                400,
                serde_json::to_vec(&serde_json::json!({
                    "error": "invalid_json",
                    "registry_version": registry_version,
                }))
                .unwrap_or_default(),
            );
        }
    };
    let req: DeleteRequest = match serde_json::from_value(parsed) {
        Ok(r) => r,
        Err(_) => {
            return (
                400,
                serde_json::to_vec(&serde_json::json!({
                    "error": "expected_key_object",
                    "registry_version": registry_version,
                }))
                .unwrap_or_default(),
            );
        }
    };

    // 2. Lookup table descriptor.
    let descriptor = {
        let inner = app.dev_agg.registry.read();
        match inner.tables.get(table_name).cloned() {
            Some(d) => d,
            None => {
                return (
                    404,
                    serde_json::to_vec(&serde_json::json!({
                        "error": "table_not_found",
                        "table": table_name,
                    }))
                    .unwrap_or_default(),
                );
            }
        }
    };

    if !descriptor.temporal {
        return (
            400,
            serde_json::to_vec(&serde_json::json!({
                "error": "table_not_temporal",
                "table": table_name,
            }))
            .unwrap_or_default(),
        );
    }

    // 3. Encode entity key.
    let entity_key = match entity_key_from_body(&descriptor, &req.key) {
        Some(k) => k,
        None => {
            return (
                400,
                serde_json::to_vec(&serde_json::json!({
                    "error": "missing_or_unsupported_primary_key",
                    "primary_key": descriptor.primary_key,
                }))
                .unwrap_or_default(),
            );
        }
    };

    // 4. Find the most recent upsert LSN for this key in event_id_index.
    let target_event_id = {
        let idx = app.dev_agg.event_id_index.lock();
        idx.iter()
            .filter_map(|(lsn, entry)| match entry {
                EventIdEntry::TableWrite {
                    table_name: t,
                    entity_key: k,
                    retracted,
                } if t == table_name && k == &entity_key && !retracted => Some(*lsn),
                _ => None,
            })
            .max()
    };

    let target_event_id = match target_event_id {
        Some(lsn) => lsn,
        None => {
            return (
                404,
                serde_json::to_vec(&serde_json::json!({
                    "error": "key_not_found",
                    "table": table_name,
                }))
                .unwrap_or_default(),
            );
        }
    };

    // 5. Build retract WAL payload.
    let payload = serde_json::json!({
        "v": 1,
        "t": table_name,
        "target": target_event_id,
        "k": entity_key,
    });
    let payload_bytes = match serde_json::to_vec(&payload) {
        Ok(b) => b,
        Err(_) => {
            return (
                500,
                serde_json::to_vec(&serde_json::json!({"error": "serialize_failed"}))
                    .unwrap_or_default(),
            );
        }
    };
    let retract_lsn = match app
        .wal_sink
        .append_record(beava_persistence::RecordType::Retract, payload_bytes)
        .await
    {
        Ok(lsn) => lsn,
        Err(_) => {
            return (
                503,
                serde_json::to_vec(&serde_json::json!({"error": "wal_unavailable"}))
                    .unwrap_or_default(),
            );
        }
    };

    // 6. Apply retraction to MVCC store.
    let now = now_ms();
    {
        let mut stores = app.dev_agg.temporal_stores.lock();
        let store = stores.entry(table_name.to_owned()).or_default();
        let _ = store.retract(&entity_key, target_event_id, retract_lsn, now);
    }

    // 7. Mark the entry retracted.
    {
        let mut idx = app.dev_agg.event_id_index.lock();
        if let Some(EventIdEntry::TableWrite { retracted, .. }) = idx.get_mut(&target_event_id) {
            *retracted = true;
        }
    }

    let ack = PushTableAck {
        ack_lsn: retract_lsn,
        registry_version,
    };
    (200, serde_json::to_vec(&ack).unwrap_or_default())
}

// ─── Retract handler ──────────────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
struct RetractRequest {
    event_id: u64,
}

#[derive(Debug, Serialize)]
struct RetractAck {
    ack_lsn: u64,
    target_event_id: u64,
    table: String,
    restored_to_lsn: Option<u64>,
}

/// Helper for `POST /retract` on the mio data-plane dispatch.
/// D-12 / D-17 error shapes (event_id_not_found 404, stream_retraction_unimplemented 501,
/// table_not_temporal 400, event_id_already_retracted 409).
pub async fn retract_via_mio(app: &Arc<AppState>, body_bytes: &[u8]) -> (u16, Vec<u8>) {
    // 1. Parse body.
    let req: RetractRequest = match serde_json::from_slice(body_bytes) {
        Ok(r) => r,
        Err(_) => {
            return (
                400,
                serde_json::to_vec(&serde_json::json!({"error": "invalid_json"}))
                    .unwrap_or_default(),
            );
        }
    };

    // 2. Look up the event_id in the index.
    let entry = {
        let idx = app.dev_agg.event_id_index.lock();
        idx.get(&req.event_id).cloned()
    };

    let entry = match entry {
        Some(e) => e,
        None => {
            return (
                404,
                serde_json::to_vec(&serde_json::json!({
                    "error": "event_id_not_found",
                    "event_id": req.event_id,
                }))
                .unwrap_or_default(),
            );
        }
    };

    match entry {
        EventIdEntry::Stream { .. } => (
            501,
            serde_json::to_vec(&serde_json::json!({
                "error": "stream_retraction_unimplemented",
                "see": "phase-11.5-summary",
                "event_id": req.event_id,
            }))
            .unwrap_or_default(),
        ),
        EventIdEntry::TableWrite {
            table_name,
            entity_key,
            retracted,
        } => {
            if retracted {
                return (
                    409,
                    serde_json::to_vec(&serde_json::json!({
                        "error": "event_id_already_retracted",
                        "event_id": req.event_id,
                    }))
                    .unwrap_or_default(),
                );
            }
            // Lookup descriptor.
            let descriptor = {
                let inner = app.dev_agg.registry.read();
                match inner.tables.get(&table_name).cloned() {
                    Some(d) => d,
                    None => {
                        return (
                            404,
                            serde_json::to_vec(&serde_json::json!({
                                "error": "table_not_found",
                                "table": table_name,
                            }))
                            .unwrap_or_default(),
                        );
                    }
                }
            };
            if !descriptor.temporal {
                return (
                    400,
                    serde_json::to_vec(&serde_json::json!({
                        "error": "table_not_temporal",
                        "table": table_name,
                    }))
                    .unwrap_or_default(),
                );
            }

            // Build retract WAL payload.
            let payload = serde_json::json!({
                "v": 1,
                "t": table_name,
                "target": req.event_id,
                "k": entity_key,
            });
            let payload_bytes = match serde_json::to_vec(&payload) {
                Ok(b) => b,
                Err(_) => {
                    return (
                        500,
                        serde_json::to_vec(&serde_json::json!({"error": "serialize_failed"}))
                            .unwrap_or_default(),
                    );
                }
            };
            let retract_lsn = match app
                .wal_sink
                .append_record(RecordType::Retract, payload_bytes)
                .await
            {
                Ok(lsn) => lsn,
                Err(_) => {
                    return (
                        503,
                        serde_json::to_vec(&serde_json::json!({"error": "wal_unavailable"}))
                            .unwrap_or_default(),
                    );
                }
            };

            // Apply to MVCC under lock.
            let now = now_ms();
            let restored = {
                let mut stores = app.dev_agg.temporal_stores.lock();
                let store = stores.entry(table_name.clone()).or_default();
                store.retract(&entity_key, req.event_id, retract_lsn, now)
            };
            match restored {
                Ok(restored_to_lsn) => {
                    // Mark the entry as retracted.
                    {
                        let mut idx = app.dev_agg.event_id_index.lock();
                        if let Some(EventIdEntry::TableWrite { retracted, .. }) =
                            idx.get_mut(&req.event_id)
                        {
                            *retracted = true;
                        }
                    }
                    let ack = RetractAck {
                        ack_lsn: retract_lsn,
                        target_event_id: req.event_id,
                        table: table_name,
                        restored_to_lsn,
                    };
                    (200, serde_json::to_vec(&ack).unwrap_or_default())
                }
                Err(RetractError::TargetNotFound) => (
                    409,
                    serde_json::to_vec(&serde_json::json!({
                        "error": "event_id_outside_retention",
                        "event_id": req.event_id,
                    }))
                    .unwrap_or_default(),
                ),
                Err(RetractError::AlreadyRetracted) => (
                    409,
                    serde_json::to_vec(&serde_json::json!({
                        "error": "event_id_already_retracted",
                        "event_id": req.event_id,
                    }))
                    .unwrap_or_default(),
                ),
            }
        }
    }
}

/// Pure helper for `GET /table/:table?key=...&as_of=...` on the mio
/// data-plane dispatch. Sync — no WAL or async I/O. Returns
/// `(status, body_bytes)`.
pub fn table_get_via_mio(app: &Arc<AppState>, table_name: &str, raw_query: &str) -> (u16, Vec<u8>) {
    let pairs = parse_query_pairs(raw_query);
    let key = match pairs.get("key") {
        Some(k) if !k.is_empty() => k.clone(),
        _ => {
            return (
                400,
                serde_json::to_vec(&serde_json::json!({"error": "missing_key_query_param"}))
                    .unwrap_or_default(),
            );
        }
    };
    let as_of: Option<u64> = pairs.get("as_of").and_then(|s| s.parse::<u64>().ok());

    // 1. Lookup descriptor.
    let descriptor = {
        let inner = app.dev_agg.registry.read();
        match inner.tables.get(table_name).cloned() {
            Some(d) => d,
            None => {
                return (
                    404,
                    serde_json::to_vec(&serde_json::json!({
                        "error": "table_not_found",
                        "table": table_name,
                    }))
                    .unwrap_or_default(),
                );
            }
        }
    };

    // 2. Reject as_of on non-temporal tables.
    if as_of.is_some() && !descriptor.temporal {
        return (
            400,
            serde_json::to_vec(&serde_json::json!({
                "error": "as_of_requires_temporal",
                "table": table_name,
            }))
            .unwrap_or_default(),
        );
    }

    // 3. Build entity_key from query param.
    let entity_key = match entity_key_from_str(&key, descriptor.primary_key.len()) {
        Some(k) => k,
        None => {
            return (
                400,
                serde_json::to_vec(&serde_json::json!({
                    "error": "unsupported_primary_key_arity",
                    "primary_key": descriptor.primary_key,
                }))
                .unwrap_or_default(),
            );
        }
    };

    // 4. Lookup. v0: only temporal tables have an in-memory store.
    if !descriptor.temporal {
        return (
            501,
            serde_json::to_vec(&serde_json::json!({
                "error": "non_temporal_table_get_v0_deferred",
                "see": "phase-11.5-summary",
            }))
            .unwrap_or_default(),
        );
    }

    let lookup_lsn = as_of.unwrap_or(u64::MAX);
    let row_json = {
        let stores_guard = app.dev_agg.temporal_stores.lock();
        match stores_guard.get(table_name) {
            Some(store) => store
                .lookup_at_lsn(&entity_key, lookup_lsn)
                .map(row_to_json),
            None => None,
        }
    };

    match row_json {
        Some(jv) => (
            200,
            serde_json::to_vec(&serde_json::json!({"row": jv})).unwrap_or_default(),
        ),
        None => (
            404,
            serde_json::to_vec(&serde_json::json!({"error": "key_not_found", "key": key}))
                .unwrap_or_default(),
        ),
    }
}
