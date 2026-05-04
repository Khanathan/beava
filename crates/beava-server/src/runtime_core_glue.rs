//! Glue layer bridging `beava-runtime-core` `WireRequest` to the existing
//! `AppState` apply + query path (Phase 18 Plan 01, Task 1.4).
//!
//! # Responsibility
//!
//! The hand-rolled event loop (in `beava-runtime-core`) parses raw bytes from
//! TCP and HTTP connections into typed `WireRequest` values. This module is
//! the bridge: it takes those requests and calls the wire-agnostic helpers
//! (`apply_shard::dispatch_*_sync`, `register::register_outcome_to_glue`,
//! `feature_query::parse_entity_key`). No business logic lives here.
//!
//! # Architecture (D-10, 18-CONTEXT.md)
//!
//! Apply thread processes all parsed commands inline after the read phase.
//! Responses are `GlueResponse` values returned synchronously (the apply
//! thread is the caller). I/O threads write the response bytes out
//! in their next write phase (added in Plan 18-03/18-04).
//!
//! # TODO(phase-18-followup)
//!
//! - Wire full cross-thread dispatch: I/O threads hand off `WireRequest` via
//!   SPSC channel to the apply thread; apply thread returns `GlueResponse` via
//!   per-client oneshot. This file is single-threaded for Plan 18-01.

use crate::feature_query::{parse_entity_key, value_to_json};
use crate::AppState;
use beava_runtime_core::wal_buffer::WalBufferRing;
use beava_runtime_core::wal_lsn::WalLsn;
use bytes::Bytes;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// The result of dispatching a `WireRequest` through the apply path.
///
/// The caller (I/O thread or event-loop write phase) serialises this into
/// the appropriate wire bytes (TCP framed or HTTP response).
#[derive(Debug)]
pub enum GlueResponse {
    /// Plan 12.6-01: register response — both success (200) and error
    /// (400/409/503/415) paths funnel here.  The body is pre-serialised by
    /// `crate::register::register_outcome_to_glue` so the wire shape
    /// matches legacy axum's `register::map_outcome_to_http` exactly
    /// (used by ~30 phase2/4/5/etc. tests that assert on `error.code`,
    /// `error.path`, `error.reason`, `error.diff.added/removed/changed`,
    /// `registered_descriptors`, `added`, `already_present`, etc).  The
    /// TCP encoder uses `tcp_op` (OP_REGISTER on success,
    /// OP_ERROR_RESPONSE on failure); the HTTP encoder uses `http_status`.
    Register {
        http_status: u16,
        body: Bytes,
        tcp_op: u16,
    },
    /// Push accepted; `ack_lsn` is the WAL LSN.
    PushAck { ack_lsn: u64, registry_version: u32 },
    /// Push idempotent-replay.
    ///
    /// Plan 12.6-15: `cached_body` carries the byte-identical response from
    /// the original push (HTTP success criterion #2 byte-identical replay).
    /// `ack_lsn` is the original push's ack — used by the TCP encoder to
    /// build a `{ack_lsn, idempotent_replay: true, ...}` envelope (TCP has
    /// no idempotent-replay header; the body flag IS the discriminator).
    PushReplay {
        registry_version: u32,
        ack_lsn: Option<u64>,
        cached_body: Option<Bytes>,
    },
    /// Push rejected (unknown event, schema failure, etc.)
    PushError {
        code: &'static str,
        registry_version: u32,
    },
    /// Feature query result (`{"value": ...}` or batch result).
    ///
    /// `format` is the wire content_type byte of the response payload — Plan 12-09:
    /// `CT_JSON` (HTTP path always; TCP /get when caller sent CT_JSON) or
    /// `CT_MSGPACK` (TCP /get when caller sent CT_MSGPACK). The HTTP encoder
    /// IGNORES this byte and always emits `Content-Type: application/json`
    /// (locked decision D-D — HTTP /get is JSON-only).
    QueryResult { body: Bytes, format: u8 },
    /// Feature not found or key not found.
    QueryNotFound { code: &'static str },
    /// Internal error (serialisation failure, etc.)
    InternalError { reason: String },
    /// Ping response.
    Pong { registry_version: u32 },
    /// /health response — always 200 once the listener is up. Plan 12-07.
    HealthOk,
    /// /ready response on the data-plane port — Plan 12.6-01 back-compat
    /// shim for TestServer tests. Identical body shape to the admin
    /// sidecar's /ready (`{"status":"ready"}`).  No apply-thread state
    /// consulted; the admin port is canonical.
    ReadyOk,
    /// /registry response on the data-plane port — Plan 12.6-01 back-compat
    /// shim. Body is the registry snapshot (legacy axum dev endpoint
    /// shape) serialised by the apply-thread reader.
    RegistrySnapshot { body: Bytes },
    /// Plan 12.6-01: HTTP path did not match any route.  Encoded as
    /// `404 Not Found` to match the legacy axum NotFoundLayer.
    HttpRouteNotFound { path: String },
    /// Plan 12.6-01: HTTP path matched a route but with the wrong method.
    /// Encoded as `405 Method Not Allowed`.
    HttpMethodNotAllowed { method: String, path: String },
    /// Plan 12.6-14: 415 Unsupported Media Type for POST endpoints whose
    /// Content-Type header was missing or not `application/json`. Body
    /// matches the legacy axum register handler `RegisterErrorBody`
    /// shape (so the `success_criterion_5_malformed_returns_400_with_path`
    /// assertion at `error.code == "unsupported_media_type"` passes).
    HttpUnsupportedMediaType { received: String, path: String },
    /// Plan 12.6-15: rich TCP error frame.
    ///
    /// Body shape: `{"error": {"code": <code>, "message": <msg>, ...extras}}`.
    /// `extras` is a JSON object merged into the error body — used to carry
    /// `frame_too_large.limit` (criterion 7) etc. without proliferating
    /// variants.
    ///
    /// Three concrete uses today:
    /// - `op_not_implemented` — known-but-deferred opcode (e.g. OP_PUSH_SYNC,
    ///   reserved for Phase 12). Connection stays open.
    /// - `unknown_op` — opcode the server doesn't recognise. Connection
    ///   stays open.
    /// - `unsupported_content_type` — OP_REGISTER with a content_type byte
    ///   the server can't parse (e.g. CT_MSGPACK). Connection stays open.
    /// - `frame_too_large` — frame length exceeded `tcp_max_frame_bytes`.
    ///   Connection MUST be closed by the listener after the error frame.
    TcpError {
        code: &'static str,
        message: String,
        extras: serde_json::Value,
    },
    /// Unrecognised request type — caller maps to 404 / error frame.
    Unsupported,
}

// ─── Query helpers ────────────────────────────────────────────────────────────
//
// Plan 12.6-07: legacy async `dispatch_wire_request` deleted. The mio data
// plane uses the sync helpers below via `apply_shard::ApplyShard`.

fn dispatch_get_single(
    app: &Arc<AppState>,
    feature: &str,
    key: &str,
    body_format: u8,
) -> GlueResponse {
    use beava_core::wire::{CT_JSON, CT_MSGPACK};
    // Validate body_format upfront — single-key path doesn't parse a body, but
    // we must reject unsupported codecs before stamping a bogus format byte on
    // QueryResult.
    match body_format {
        CT_JSON | CT_MSGPACK => {}
        other => {
            return GlueResponse::InternalError {
                reason: format!("unsupported content_type: {other:#04x}"),
            };
        }
    }
    let registry = &app.dev_agg.registry;

    let query_time_ms = {
        let raw = app
            .dev_agg
            .query_time_ms
            .load(std::sync::atomic::Ordering::Acquire);
        if raw == 0 {
            // Fall back to wall clock when no events have been pushed yet.
            Instant::now().elapsed().as_millis() as i64
        } else {
            raw as i64
        }
    };

    // Case 1: `feature` is an individual feature name (e.g. "cnt").
    // Return `{"value": <single-value>}`.
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
        // Plan 18-16 Task 16.2: state_tables is Vec<AggStateTable> indexed by agg_id.
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

    // Case 2: `feature` is an aggregation node name (e.g. "TxnAgg").
    // Return all features for the entity as `{feat_name: value, ...}`.
    if let Some(descriptor) = registry.compiled_aggregation(feature) {
        let entity_key = match parse_entity_key(key, &descriptor.group_keys) {
            Some(k) => k,
            None => {
                return GlueResponse::QueryNotFound {
                    code: "key_parse_failure",
                }
            }
        };
        // Plan 18-16 Task 16.2: index by agg_id, not by name.
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

/// Sync wrapper for `dispatch_get_single` — called from `ApplyShard` on the apply thread.
///
/// `body_format` is the wire content_type byte from the request frame:
/// - `CT_JSON` (HTTP path always; TCP /get when caller sent CT_JSON)
/// - `CT_MSGPACK` (TCP /get when caller sent CT_MSGPACK)
///
/// The response `QueryResult.format` mirrors the request format byte (msgpack-in →
/// msgpack-out, json-in → json-out — locked decision D-B). HTTP encoder ignores
/// the byte and always emits JSON (D-D).
pub fn dispatch_get_single_sync(
    app: &Arc<AppState>,
    feature: &str,
    key: &str,
    body_format: u8,
) -> GlueResponse {
    dispatch_get_single(app, feature, key, body_format)
}

/// Sync wrapper for `dispatch_get_batch` — called from `ApplyShard` on the apply thread.
///
/// See `dispatch_get_single_sync` for the `body_format` contract.
pub fn dispatch_get_batch_sync(app: &Arc<AppState>, body: &Bytes, body_format: u8) -> GlueResponse {
    dispatch_get_batch(app, body, body_format)
}

/// Plan 12-07 Wave 4: real batch GET dispatch.
///
/// Mirrors the axum-side `post_get_batch_handler` (feature_query.rs:169-238):
///
/// 1. Cell-cap (SRV-API-08): keys × features > 10_000 -> InternalError
///    "batch_too_large: cells={n} cap={cap}".
/// 2. Resolve all features upfront — any missing -> InternalError
///    "feature_not_found: missing=[...]" with the exact axum-side semantics.
/// 3. Compute query_time_ms once via D-06 max-event-time tracking.
/// 4. Acquire `state_tables.lock()` ONCE for the whole batch (single critical
///    section; reuse the guard for all keys × features cells). Per-cell:
///    `query_feature(&entity_key, feature_idx, query_time_ms)`.
/// 5. Iteration order is `for key { for feature }` — per memory
///    `project_no_same_key_batching`, NO sketch coalescing across the keys ×
///    features cells.
/// 6. Omit keys with no matching state (NOT inserted with null / empty obj).
///    Mirrors axum-side feature_query.rs:232-234 semantics.
fn dispatch_get_batch(app: &Arc<AppState>, body: &Bytes, body_format: u8) -> GlueResponse {
    use beava_core::wire::{CT_JSON, CT_MSGPACK};
    /// Cell-count cap enforced by SRV-API-08 / T-05-06-01. Mirrors
    /// `feature_query::BATCH_CAP`.
    const BATCH_CAP: usize = 10_000;

    // Plan 12-07 stage timing — same `BEAVA_TRACE_APPLY_TIMING=1` env var
    // gate as the push path uses. Reading the OnceLock is ~5-10 ns when off.
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
    // Plan 12-09 D-A/D-B: body_format byte selects parse codec.
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

    // 1. Cell-cap check.
    let cell_count = req.keys.len().saturating_mul(req.features.len());
    if cell_count > BATCH_CAP {
        return GlueResponse::InternalError {
            reason: format!("batch_too_large: cells={} cap={}", cell_count, BATCH_CAP),
        };
    }

    // 2. Resolve all features upfront.
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

    // 3. Compute query_time_ms (D-06 — never wall clock).
    let query_time_ms = {
        let raw = app
            .dev_agg
            .query_time_ms
            .load(std::sync::atomic::Ordering::Acquire);
        if raw == 0 {
            0i64
        } else {
            raw as i64
        }
    };

    // 4. Single state_tables lock for the whole batch.
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
            // Skip features where the key is malformed for this group_by arity.
            // Mirrors feature_query.rs:217-222 (WR-02 silent omission).
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
        // Omit keys with no matching state (SRV-API-08).
        if !key_result.is_empty() {
            result.insert(key_str.clone(), key_result);
        }
    }
    let t_loop = t0.map(|t| t.elapsed());
    drop(tables);

    // Phase 13.4 Plan 02 / Phase 13.0-15 wire-spec: drop the `{"result": ...}`
    // envelope. The multi-feature batched read now emits the flat per-entity
    // dict directly — `{entity_id: {feature: value}}` instead of
    // `{"result": {entity_id: {feature: value}}}`. Cold-start (no entities
    // matched) is `{}`. Both transports (HTTP /get + TCP OP_GET_MULTI / OP_MGET)
    // share this body shape; the single-feature `GET /get/:feature/:key` path
    // is unchanged at `{"value": <val>}`.
    let body_json = serde_json::Value::Object(
        result
            .into_iter()
            .map(|(k, v)| (k, serde_json::Value::Object(v.into_iter().collect())))
            .collect(),
    );
    // Plan 12-09 D-B: response format mirrors request format (msgpack-in → msgpack-out,
    // json-in → json-out).
    //
    // ┌─ SHAPE-PARITY CONTRACT (Plan 12-09 Wave 2) ──────────────────────────┐
    // │ Use `rmp_serde::to_vec_named` (NOT plain `rmp_serde::to_vec`).       │
    // │                                                                      │
    // │ `to_vec_named` writes Map<String, Value> as a msgpack `map<str, *>`  │
    // │ — string-keyed, mirroring JSON's object shape so the round-tripped   │
    // │ `serde_json::Value` from the msgpack response equals the JSON-side   │
    // │ value exactly.                                                       │
    // │                                                                      │
    // │ Plain `to_vec` would emit sequential-integer keys (treating the      │
    // │ Value::Object's BTreeMap as a positional sequence), breaking SDK     │
    // │ decoders that expect string-keyed maps and breaking the locked       │
    // │ JSON-equivalent shape contract per memory `project_v2_devex_first`.  │
    // │                                                                      │
    // │ Same precedent: Plan 18-09 / 18-10 push-side msgpack body parsing.   │
    // │                                                                      │
    // │ Test guard: `phase12_09_dispatch_msgpack_test::                      │
    // │              test_msgpack_and_json_responses_are_shape_equivalent`.  │
    // └──────────────────────────────────────────────────────────────────────┘
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

// ─── WAL Glue (Plan 18-02 Task 2.4) ──────────────────────────────────────────

/// Thin bridge between the hand-rolled apply path and the WAL ring.
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
            registry_version: 0, // caller may override with actual registry_version
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
