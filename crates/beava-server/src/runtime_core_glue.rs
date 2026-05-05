//! Glue layer between `beava-runtime-core` `WireRequest` values and the
//! existing `AppState` apply + query path. The mio event loop parses raw
//! bytes into `WireRequest`s; this module dispatches them to the
//! wire-agnostic helpers in `apply_shard`, `register`, and `feature_query`.
//! No business logic lives here.
//!
//! The apply thread runs synchronously: it returns a `GlueResponse` directly
//! to the I/O thread, which writes the response bytes during its next write
//! phase.

use crate::feature_query::{parse_entity_key, value_to_json};
use crate::AppState;
use beava_runtime_core::wal_buffer::WalBufferRing;
use beava_runtime_core::wal_lsn::WalLsn;
use bytes::Bytes;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Effective query time for `time_since` / `age` / windowed ops: the max of
/// the apply-side watermark (last event-time seen, monotonic across pushes)
/// and wall-clock now. The watermark guarantees replay-deterministic queries
/// against advanced state; the wall-clock floor keeps `time_since` honest
/// when the caller sleeps + queries an entity that hasn't received fresh
/// events.
#[inline]
fn effective_query_time_ms(watermark: u64) -> i64 {
    let wall = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0);
    (watermark as i64).max(wall)
}

/// The result of dispatching a `WireRequest` through the apply path. The
/// caller (I/O thread or event-loop write phase) serialises this into the
/// appropriate wire bytes (TCP frame or HTTP response).
#[derive(Debug)]
pub enum GlueResponse {
    /// Register response — both success (200) and error (400/409/503/415)
    /// paths funnel here. `body` is pre-serialised by
    /// `register::register_outcome_to_glue` so the wire shape is identical
    /// across HTTP and TCP. The TCP encoder uses `tcp_op` (`OP_REGISTER` on
    /// success, `OP_ERROR_RESPONSE` on failure); the HTTP encoder uses
    /// `http_status`.
    Register {
        http_status: u16,
        body: Bytes,
        tcp_op: u16,
    },
    /// Push accepted; `ack_lsn` is the WAL LSN.
    PushAck { ack_lsn: u64, registry_version: u32 },
    /// Push idempotent-replay. `cached_body` carries the byte-identical
    /// response from the original push (HTTP byte-identical-replay
    /// criterion); `ack_lsn` is the original push's ack so the TCP encoder
    /// can build a `{ack_lsn, idempotent_replay: true, ...}` envelope —
    /// TCP has no idempotent-replay header, the body flag IS the
    /// discriminator.
    PushReplay {
        registry_version: u32,
        ack_lsn: Option<u64>,
        cached_body: Option<Bytes>,
    },
    /// Push rejected (unknown event, schema failure, etc.).
    PushError {
        code: &'static str,
        registry_version: u32,
    },
    /// Verb-style request body received in a legacy shape (`{keys,
    /// features}` for `/get` and `/batch_get`, `{feature, key}` for
    /// `/get`). HTTP returns 400 with `{"error":{"code":
    /// "unsupported_request_shape","message":<hint>}}`; TCP emits an
    /// `OP_ERROR_RESPONSE` (0xFFFF) frame with the same body. `hint`
    /// points the caller at `docs/http-api.md`.
    UnsupportedRequestShape { hint: String },
    /// Feature query result (`{"value": ...}` or batch result). `format`
    /// is the wire content-type byte of the response payload (`CT_JSON`
    /// or `CT_MSGPACK`). The HTTP encoder ignores this byte and always
    /// emits `Content-Type: application/json` — HTTP `/get` is JSON-only.
    QueryResult { body: Bytes, format: u8 },
    /// Feature not found or key not found.
    QueryNotFound { code: &'static str },
    /// Internal error (serialisation failure, etc.).
    InternalError { reason: String },
    /// Ping response.
    Pong { registry_version: u32 },
    /// `/health` response — always 200 once the listener is up.
    HealthOk,
    /// `/ready` shim on the data-plane port. Body matches the admin
    /// sidecar's `/ready` (`{"status":"ready"}`); no apply-thread state is
    /// consulted — the admin port is canonical.
    ReadyOk,
    /// Data-plane `/registry` shim. Body is the dev-endpoint registry
    /// snapshot, serialised by the apply-thread reader.
    RegistrySnapshot { body: Bytes },
    /// HTTP path did not match any route — encoded as 404 Not Found.
    HttpRouteNotFound { path: String },
    /// HTTP path matched a route with the wrong method — encoded as
    /// 405 Method Not Allowed.
    HttpMethodNotAllowed { method: String, path: String },
    /// 415 Unsupported Media Type for POST endpoints whose Content-Type
    /// header was missing or not `application/json`. Body matches the
    /// `RegisterErrorBody` shape so callers can pattern-match on
    /// `error.code == "unsupported_media_type"`.
    HttpUnsupportedMediaType { received: String, path: String },
    /// Rich TCP error frame: `{"error": {"code": <code>, "message": <msg>,
    /// ...extras}}`. `extras` is merged into the error body so callers can
    /// carry context (`frame_too_large.limit`, etc.) without new variants.
    ///
    /// Concrete uses:
    /// - `op_not_implemented` — known-but-deferred opcode. Connection
    ///   stays open.
    /// - `unknown_op` — opcode the server doesn't recognise. Connection
    ///   stays open.
    /// - `unsupported_content_type` — `OP_REGISTER` with a content-type
    ///   byte the server can't parse. Connection stays open.
    /// - `frame_too_large` — frame length exceeded `tcp_max_frame_bytes`.
    ///   Connection MUST be closed by the listener after the error frame.
    TcpError {
        code: &'static str,
        message: String,
        extras: serde_json::Value,
    },
    /// `OP_RESET` succeeded — server is in `effective_test_mode`. Body
    /// shape `{"reset": true, "registry_version": <new>}`. HTTP returns
    /// 200; TCP emits an `OP_GET_RESPONSE` (0x0023) frame.
    ResetOk { registry_version: u64 },
    /// `OP_RESET` rejected — server is not in test mode. HTTP returns
    /// 403; TCP emits `OP_ERROR_RESPONSE` (0xFFFF). Body shape
    /// `{"error": {"code": "reset_disabled_in_production", "reason":
    /// ...}}`.
    ResetForbidden,
    /// Unrecognised request — caller maps to 404 / error frame.
    Unsupported,
}

fn dispatch_get_single(
    app: &Arc<AppState>,
    feature: &str,
    key: &str,
    body_format: u8,
) -> GlueResponse {
    use beava_core::wire::{CT_JSON, CT_MSGPACK};
    // Reject unsupported codecs before we'd stamp a bogus format byte on
    // `QueryResult`; the single-key path doesn't otherwise parse a body.
    match body_format {
        CT_JSON | CT_MSGPACK => {}
        other => {
            return GlueResponse::InternalError {
                reason: format!("unsupported content_type: {other:#04x}"),
            };
        }
    }
    let registry = &app.dev_agg.registry;

    let query_time_ms = effective_query_time_ms(
        app.dev_agg
            .query_time_ms
            .load(std::sync::atomic::Ordering::Acquire),
    );

    // Case 1: `feature` resolves to an individual feature name. Return
    // `{"value": <single-value>}`.
    if let Some((agg_node, feature_idx)) = registry.resolve_feature(feature) {
        let descriptor = match registry.compiled_aggregation(&agg_node) {
            Some(d) => d,
            None => {
                return GlueResponse::InternalError {
                    reason: "internal_error".to_owned(),
                }
            }
        };
        let entity_key = match parse_entity_key(key, &descriptor.group_keys) {
            Some(k) => k,
            None => {
                return GlueResponse::QueryNotFound {
                    code: "key_parse_failure",
                }
            }
        };
        let tables = app.dev_agg.state_tables.lock();
        let value_opt = tables
            .get(descriptor.agg_id as usize)
            .and_then(|t| t.query_feature(&entity_key, feature_idx, query_time_ms));
        return match value_opt {
            Some(v) => {
                let json_val = serde_json::json!({ "value": value_to_json(v) });
                let resp_bytes_res: Result<Vec<u8>, String> = match body_format {
                    CT_JSON => serde_json::to_vec(&json_val).map_err(|e| e.to_string()),
                    CT_MSGPACK => rmp_serde::to_vec_named(&json_val).map_err(|e| e.to_string()),
                    _ => unreachable!("validated above"),
                };
                match resp_bytes_res {
                    Ok(b) => GlueResponse::QueryResult {
                        body: Bytes::from(b),
                        format: body_format,
                    },
                    Err(reason) => GlueResponse::InternalError { reason },
                }
            }
            None => GlueResponse::QueryNotFound {
                code: "key_not_found",
            },
        };
    }

    // Case 2: `feature` is an aggregation node name. Return every feature
    // for the entity as `{feat_name: value, ...}`.
    if let Some(descriptor) = registry.compiled_aggregation(feature) {
        let entity_key = match parse_entity_key(key, &descriptor.group_keys) {
            Some(k) => k,
            None => {
                return GlueResponse::QueryNotFound {
                    code: "key_parse_failure",
                }
            }
        };
        let tables = app.dev_agg.state_tables.lock();
        let table = match tables.get(descriptor.agg_id as usize) {
            Some(t) => t,
            None => {
                return GlueResponse::QueryNotFound {
                    code: "key_not_found",
                }
            }
        };
        let mut result = serde_json::Map::new();
        for (idx, named_op) in descriptor.features.iter().enumerate() {
            if let Some(v) = table.query_feature(&entity_key, idx, query_time_ms) {
                result.insert(named_op.feature_name.clone(), value_to_json(v));
            }
        }
        if result.is_empty() {
            return GlueResponse::QueryNotFound {
                code: "key_not_found",
            };
        }
        let json_val = serde_json::Value::Object(result);
        let resp_bytes_res: Result<Vec<u8>, String> = match body_format {
            CT_JSON => serde_json::to_vec(&json_val).map_err(|e| e.to_string()),
            CT_MSGPACK => rmp_serde::to_vec_named(&json_val).map_err(|e| e.to_string()),
            _ => unreachable!("validated above"),
        };
        return match resp_bytes_res {
            Ok(b) => GlueResponse::QueryResult {
                body: Bytes::from(b),
                format: body_format,
            },
            Err(reason) => GlueResponse::InternalError { reason },
        };
    }

    GlueResponse::QueryNotFound {
        code: "feature_not_found",
    }
}

/// Sync wrapper for `dispatch_get_single` — called from `ApplyShard` on the
/// apply thread. The response `QueryResult.format` mirrors the request
/// format byte (msgpack-in → msgpack-out, json-in → json-out); the HTTP
/// encoder ignores the byte and always emits JSON.
pub fn dispatch_get_single_sync(
    app: &Arc<AppState>,
    feature: &str,
    key: &str,
    body_format: u8,
) -> GlueResponse {
    dispatch_get_single(app, feature, key, body_format)
}

/// Verb-style single-row GET dispatch (`POST /get` and `OP_GET`).
///
/// `table` is an aggregation node name; `key` is the per-entity key.
/// `features = None` returns the full row; `Some(filter)` narrows it to
/// those names. Names not in the descriptor surface `feature_not_found`
/// upfront; absent values are omitted from the row.
///
/// Cold-start returns an empty flat row `{}` per the locked wire-fixtures
/// (NOT `QueryNotFound`). This differs from `dispatch_get_single` which
/// routes via either feature-name (Case 1) or aggregation-node-name
/// (Case 2).
pub fn dispatch_get_single_verb_style_sync(
    app: &Arc<AppState>,
    table: &str,
    key: &str,
    features: Option<&[String]>,
    body_format: u8,
) -> GlueResponse {
    use beava_core::wire::{CT_JSON, CT_MSGPACK};
    match body_format {
        CT_JSON | CT_MSGPACK => {}
        other => {
            return GlueResponse::InternalError {
                reason: format!("unsupported content_type: {other:#04x}"),
            };
        }
    }
    let registry = &app.dev_agg.registry;
    let query_time_ms = effective_query_time_ms(
        app.dev_agg
            .query_time_ms
            .load(std::sync::atomic::Ordering::Acquire),
    );

    let descriptor = match registry.compiled_aggregation(table) {
        Some(d) => d,
        None => {
            return GlueResponse::QueryNotFound {
                code: "unknown_table",
            };
        }
    };

    // Reject the whole call upfront when any requested feature isn't
    // registered, matching the batch path's whole-batch-reject disposition.
    if let Some(filter) = features {
        let mut unknown: Vec<String> = Vec::new();
        for name in filter {
            if !descriptor.features.iter().any(|f| &f.feature_name == name) {
                unknown.push(name.clone());
            }
        }
        if !unknown.is_empty() {
            return GlueResponse::InternalError {
                reason: format!("feature_not_found: missing={:?} table={}", unknown, table),
            };
        }
    }

    let entity_key = match parse_entity_key(key, &descriptor.group_keys) {
        Some(k) => k,
        None => {
            return GlueResponse::QueryNotFound {
                code: "key_parse_failure",
            };
        }
    };
    let tables = app.dev_agg.state_tables.lock();
    let table_st = match tables.get(descriptor.agg_id as usize) {
        Some(t) => t,
        None => {
            return GlueResponse::QueryNotFound {
                code: "key_not_found",
            };
        }
    };

    let mut result = serde_json::Map::new();
    for (idx, named_op) in descriptor.features.iter().enumerate() {
        if let Some(filter) = features {
            if !filter.iter().any(|f| f == &named_op.feature_name) {
                continue;
            }
        }
        if let Some(v) = table_st.query_feature(&entity_key, idx, query_time_ms) {
            result.insert(named_op.feature_name.clone(), value_to_json(v));
        }
    }
    drop(tables);

    let json_val = serde_json::Value::Object(result);
    let resp_bytes_res: Result<Vec<u8>, String> = match body_format {
        CT_JSON => serde_json::to_vec(&json_val).map_err(|e| e.to_string()),
        CT_MSGPACK => rmp_serde::to_vec_named(&json_val).map_err(|e| e.to_string()),
        _ => unreachable!("validated above"),
    };
    match resp_bytes_res {
        Ok(b) => GlueResponse::QueryResult {
            body: Bytes::from(b),
            format: body_format,
        },
        Err(reason) => GlueResponse::InternalError { reason },
    }
}

/// Sync wrapper for `dispatch_get_batch` — called from `ApplyShard` on the
/// apply thread. See `dispatch_get_single_sync` for the `body_format`
/// contract.
pub fn dispatch_get_batch_sync(app: &Arc<AppState>, body: &Bytes, body_format: u8) -> GlueResponse {
    dispatch_get_batch(app, body, body_format)
}

/// Batch GET dispatch.
///
/// 1. Cell-cap: `keys × features > 10_000` → `batch_too_large`.
/// 2. Resolve every feature upfront — any missing → `feature_not_found`.
/// 3. Compute `query_time_ms` once.
/// 4. Take `state_tables.lock()` ONCE and reuse the guard across every
///    `(key, feature)` cell — one critical section for the whole batch.
/// 5. Iterate `for key { for feature }`. The
///    `project_no_same_key_batching` invariant forbids sketch coalescing
///    across cells.
/// 6. Omit keys with no matching state (do NOT insert null / empty obj).
fn dispatch_get_batch(app: &Arc<AppState>, body: &Bytes, body_format: u8) -> GlueResponse {
    use beava_core::wire::{CT_JSON, CT_MSGPACK};
    const BATCH_CAP: usize = 10_000;

    // Stage timing toggled by `BEAVA_TRACE_APPLY_TIMING=1`. Reading the
    // OnceLock is ~5-10 ns when off.
    fn trace_get_enabled() -> bool {
        use std::sync::OnceLock;
        static FLAG: OnceLock<bool> = OnceLock::new();
        *FLAG.get_or_init(|| std::env::var("BEAVA_TRACE_APPLY_TIMING").ok().as_deref() == Some("1"))
    }
    let trace = trace_get_enabled();
    let t0 = if trace {
        Some(std::time::Instant::now())
    } else {
        None
    };

    #[derive(serde::Deserialize)]
    struct BatchGetBody {
        keys: Vec<String>,
        features: Vec<String>,
    }
    let req: BatchGetBody = match body_format {
        CT_JSON => match serde_json::from_slice(body) {
            Ok(r) => r,
            Err(e) => {
                return GlueResponse::InternalError {
                    reason: e.to_string(),
                };
            }
        },
        CT_MSGPACK => match rmp_serde::from_slice(body) {
            Ok(r) => r,
            Err(e) => {
                return GlueResponse::InternalError {
                    reason: e.to_string(),
                };
            }
        },
        other => {
            return GlueResponse::InternalError {
                reason: format!("unsupported content_type: {other:#04x}"),
            };
        }
    };
    let t_parse = t0.map(|t| t.elapsed());

    let cell_count = req.keys.len().saturating_mul(req.features.len());
    if cell_count > BATCH_CAP {
        return GlueResponse::InternalError {
            reason: format!("batch_too_large: cells={} cap={}", cell_count, BATCH_CAP),
        };
    }

    let registry = &app.dev_agg.registry;
    let mut missing_features: Vec<String> = Vec::new();
    let mut feature_resolutions: Vec<(String, usize)> = Vec::new();
    for feat in &req.features {
        match registry.resolve_feature(feat) {
            Some(resolution) => feature_resolutions.push(resolution),
            None => missing_features.push(feat.clone()),
        }
    }
    if !missing_features.is_empty() {
        return GlueResponse::InternalError {
            reason: format!("feature_not_found: missing={:?}", missing_features),
        };
    }
    let t_resolve = t0.map(|t| t.elapsed());

    let query_time_ms = effective_query_time_ms(
        app.dev_agg
            .query_time_ms
            .load(std::sync::atomic::Ordering::Acquire),
    );

    let tables = app.dev_agg.state_tables.lock();
    let t_lock = t0.map(|t| t.elapsed());

    let mut result: std::collections::BTreeMap<
        String,
        std::collections::BTreeMap<String, serde_json::Value>,
    > = std::collections::BTreeMap::new();

    for key_str in &req.keys {
        let mut key_result: std::collections::BTreeMap<String, serde_json::Value> =
            std::collections::BTreeMap::new();
        for (feat_name, (agg_node, feature_idx)) in
            req.features.iter().zip(feature_resolutions.iter())
        {
            let descriptor = match registry.compiled_aggregation(agg_node) {
                Some(d) => d,
                None => continue,
            };
            // Skip features whose group-by arity disagrees with the
            // requested key (silent per the WR-02 omit-on-mismatch rule).
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
        if !key_result.is_empty() {
            result.insert(key_str.clone(), key_result);
        }
    }
    let t_loop = t0.map(|t| t.elapsed());
    drop(tables);

    // Wire shape: flat per-entity dict `{entity_id: {feature: value}}` —
    // no `{"result": ...}` envelope. Cold-start (no entities matched) is
    // `{}`. The single-feature `GET /get/:feature/:key` path stays at
    // `{"value": <val>}` and lives elsewhere.
    let body_json = serde_json::Value::Object(
        result
            .into_iter()
            .map(|(k, v)| (k, serde_json::Value::Object(v.into_iter().collect())))
            .collect(),
    );
    // SHAPE-PARITY CONTRACT: msgpack response uses
    // `rmp_serde::to_vec_named`, not plain `to_vec`. `to_vec_named` writes
    // `Map<String, Value>` as a msgpack `map<str, *>`, matching JSON's
    // string-keyed object shape so a round-tripped `serde_json::Value`
    // from the msgpack response equals the JSON-side value exactly.
    // Plain `to_vec` would emit sequential-integer keys (treating
    // `Value::Object`'s `BTreeMap` as a positional sequence), breaking
    // SDK decoders that expect string-keyed maps.
    let resp_bytes_res: Result<Vec<u8>, String> = match body_format {
        CT_JSON => serde_json::to_vec(&body_json).map_err(|e| e.to_string()),
        CT_MSGPACK => rmp_serde::to_vec_named(&body_json).map_err(|e| e.to_string()),
        _ => unreachable!("validated at body parse"),
    };
    let resp = match resp_bytes_res {
        Ok(b) => GlueResponse::QueryResult {
            body: Bytes::from(b),
            format: body_format,
        },
        Err(reason) => GlueResponse::InternalError { reason },
    };
    if let Some(t0_inst) = t0 {
        let total = t0_inst.elapsed();
        eprintln!(
            "TRACE_APPLY ns get_batch: cells={} parse={} resolve={} lock={} loop={} TOTAL={}",
            cell_count,
            t_parse.map(|d| d.as_nanos()).unwrap_or(0),
            t_resolve.map(|d| d.as_nanos()).unwrap_or(0),
            t_lock.map(|d| d.as_nanos()).unwrap_or(0),
            t_loop.map(|d| d.as_nanos()).unwrap_or(0),
            total.as_nanos()
        );
    }
    resp
}

/// Thin bridge between the apply path and the WAL ring.
///
/// Provides two append modes mirroring `/push` (Periodic) and `/push-sync`
/// (PerEvent):
///
/// - `wal_append_periodic`: appends a serialized record into the ring and
///   returns immediately at `committed_lsn` (no blocking on I/O). Used by
///   the normal `/push` path.
///
/// - `wal_append_per_event`: appends then blocks until `synced_lsn` reaches
///   the request LSN or the timeout fires (returns `PushError` on timeout).
///   Used by the `/push-sync` path.
///
/// Both methods are synchronous — they live on the apply thread (or test
/// thread). The WAL ring itself is lock-free on the append hot path.
pub struct WalGlue {
    ring: Arc<WalBufferRing>,
    lsn: Arc<WalLsn>,
}

impl WalGlue {
    /// Create a new `WalGlue` wrapping `ring` and `lsn`.
    pub fn new(ring: Arc<WalBufferRing>, lsn: Arc<WalLsn>) -> Self {
        Self { ring, lsn }
    }

    /// Append `record_bytes` to the WAL ring and return `PushAck` immediately
    /// at `committed_lsn` (Periodic / `/push` mode).
    ///
    /// Does NOT wait for `written_lsn` or `synced_lsn` to advance.
    pub fn wal_append_periodic(&self, record_bytes: &[u8]) -> GlueResponse {
        let ack_lsn = self.ring.append(record_bytes);
        GlueResponse::PushAck {
            ack_lsn,
            registry_version: 0,
        }
    }

    /// Append `record_bytes` to the WAL ring then block until
    /// `synced_lsn >= request_lsn` (PerEvent / `/push-sync` mode).
    ///
    /// Returns `PushAck` once durable, or `PushError(wal_sync_timeout)` if
    /// `synced_lsn` does not advance within `timeout`.
    pub fn wal_append_per_event(&self, record_bytes: &[u8], timeout: Duration) -> GlueResponse {
        let request_lsn = self.ring.append(record_bytes);
        match self.lsn.wait_for_synced(request_lsn, timeout) {
            Ok(()) => GlueResponse::PushAck {
                ack_lsn: request_lsn,
                registry_version: 0,
            },
            Err(_timeout) => GlueResponse::PushError {
                code: "wal_sync_timeout",
                registry_version: 0,
            },
        }
    }
}

impl std::fmt::Debug for WalGlue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WalGlue")
            .field("committed_lsn", &self.lsn.committed())
            .field("synced_lsn", &self.lsn.synced())
            .finish()
    }
}
