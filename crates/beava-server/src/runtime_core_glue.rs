//! Glue layer bridging `beava-runtime-core` `WireRequest` to the existing
//! `AppState` apply + query path (Phase 18 Plan 01, Task 1.4).
//!
//! # Responsibility
//!
//! The hand-rolled event loop (in `beava-runtime-core`) parses raw bytes from
//! TCP and HTTP connections into typed `WireRequest` values. This module is
//! the bridge: it takes those requests and calls the same wire-agnostic
//! `execute_push`, `execute_register`, and feature-query functions that the
//! existing tokio/axum handlers call. No business logic lives here.
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
use crate::push::{execute_push, PushOutcome};
use crate::register::{execute_register_with_wal, RegisterOutcome, RegisterPayload};
use crate::AppState;
use beava_persistence::SyncMode;
use beava_runtime_core::wal_buffer::WalBufferRing;
use beava_runtime_core::wal_lsn::WalLsn;
use beava_runtime_core::wire_request::WireRequest;
use bytes::Bytes;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// The result of dispatching a `WireRequest` through the apply path.
///
/// The caller (I/O thread or event-loop write phase) serialises this into
/// the appropriate wire bytes (TCP framed or HTTP response).
#[derive(Debug)]
pub enum GlueResponse {
    /// Successful registration. `version` is the new registry_version.
    RegisterOk { version: u64 },
    /// Registration produced a validation error or conflict.
    RegisterError { code: String, message: String },
    /// Push accepted; `ack_lsn` is the WAL LSN.
    PushAck { ack_lsn: u64, registry_version: u32 },
    /// Push idempotent-replay; identical to the original ACK.
    PushReplay { registry_version: u32 },
    /// Push rejected (unknown event, schema failure, etc.)
    PushError { code: &'static str, registry_version: u32 },
    /// Feature query result (JSON-encoded `{"value": ...}` or batch result).
    QueryResult { body: Bytes },
    /// Feature not found or key not found.
    QueryNotFound { code: &'static str },
    /// Internal error (serialisation failure, etc.)
    InternalError { reason: String },
    /// Ping response.
    Pong { registry_version: u32 },
    /// Unrecognised request type — caller maps to 404 / error frame.
    Unsupported,
}

/// Dispatch a `WireRequest` to the appropriate handler and return a `GlueResponse`.
///
/// This is an async function because `execute_push` and `execute_register_with_wal`
/// are async (they drive the WAL sink channel). In Plan 18-02, the WAL calls
/// become synchronous `std::io::Write` — at that point this function can become
/// synchronous too. Until then, callers on the tokio runtime (including tests)
/// can await directly.
///
/// # TODO(phase-18-followup): replace tokio WAL calls with sync Write
pub async fn dispatch_wire_request(
    app: &Arc<AppState>,
    req: WireRequest,
) -> GlueResponse {
    match req {
        // ─── Ping ─────────────────────────────────────────────────────────────
        WireRequest::Ping => GlueResponse::Pong {
            registry_version: app.dev_agg.registry.version() as u32,
        },

        // ─── Register ─────────────────────────────────────────────────────────
        WireRequest::Register { payload } => {
            let reg_payload: RegisterPayload = match serde_json::from_slice(&payload) {
                Ok(p) => p,
                Err(e) => {
                    return GlueResponse::RegisterError {
                        code: "invalid_registration".to_owned(),
                        message: e.to_string(),
                    };
                }
            };
            let outcome =
                execute_register_with_wal(&app.dev_agg.registry, reg_payload, &app.wal_sink)
                    .await;
            match outcome {
                RegisterOutcome::Success { version, .. } => GlueResponse::RegisterOk { version },
                RegisterOutcome::EmptyPayload { version } => GlueResponse::RegisterOk { version },
                RegisterOutcome::Noop { version, .. } => GlueResponse::RegisterOk { version },
                RegisterOutcome::ValidationFailed {
                    first_error_path,
                    first_error_reason,
                    ..
                } => GlueResponse::RegisterError {
                    code: "invalid_registration".to_owned(),
                    message: format!("{first_error_path}: {first_error_reason}"),
                },
                RegisterOutcome::Conflict { .. } => GlueResponse::RegisterError {
                    code: "registration_conflict".to_owned(),
                    message: "descriptor conflict".to_owned(),
                },
                RegisterOutcome::WalUnavailable { .. } => GlueResponse::RegisterError {
                    code: "wal_unavailable".to_owned(),
                    message: "WAL unavailable".to_owned(),
                },
            }
        }

        // ─── TCP push ─────────────────────────────────────────────────────────
        WireRequest::TcpPush { event_name, body } | WireRequest::HttpPush { event_name, body } => {
            dispatch_push(app, &event_name, body, SyncMode::Periodic).await
        }

        WireRequest::HttpPushSync { event_name, body } => {
            dispatch_push(app, &event_name, body, SyncMode::PerEvent).await
        }

        WireRequest::HttpPushBatch { event_name, body } => {
            // Batch push: body is a JSON array of event objects.
            // TODO(phase-18-followup): implement batch dispatch properly.
            // For now, treat as a single push for scaffold correctness.
            dispatch_push(app, &event_name, body, SyncMode::Periodic).await
        }

        // ─── GET single feature/key ───────────────────────────────────────────
        WireRequest::HttpGetSingle { feature, key } => {
            dispatch_get_single(app, &feature, &key)
        }

        // ─── GET batch ────────────────────────────────────────────────────────
        WireRequest::HttpGet { body } => {
            dispatch_get_batch(app, &body)
        }

        // ─── Upsert / delete / retract ────────────────────────────────────────
        WireRequest::HttpUpsert { .. }
        | WireRequest::HttpDelete { .. }
        | WireRequest::HttpRetract { .. } => {
            // TODO(phase-18-followup): wire table upsert/delete/retract paths
            GlueResponse::Unsupported
        }

        WireRequest::Unknown { .. } | WireRequest::ParseError { .. } => {
            GlueResponse::Unsupported
        }
    }
}

// ─── Push helper ──────────────────────────────────────────────────────────────

async fn dispatch_push(
    app: &Arc<AppState>,
    event_name: &str,
    body: Bytes,
    sync_mode: SyncMode,
) -> GlueResponse {
    match execute_push(app, event_name, &body, sync_mode).await {
        PushOutcome::Ok { ack, .. } => GlueResponse::PushAck {
            ack_lsn: ack.ack_lsn,
            registry_version: ack.registry_version,
        },
        PushOutcome::IdempotentReplay { .. } => GlueResponse::PushReplay {
            registry_version: app.dev_agg.registry.version() as u32,
        },
        PushOutcome::Error {
            code,
            registry_version,
            ..
        } => GlueResponse::PushError {
            code,
            registry_version,
        },
    }
}

// ─── Query helpers ────────────────────────────────────────────────────────────

fn dispatch_get_single(app: &Arc<AppState>, feature: &str, key: &str) -> GlueResponse {
    let registry = &app.dev_agg.registry;

    let query_time_ms = {
        let raw = app.dev_agg.max_event_time_ms.load(std::sync::atomic::Ordering::Acquire);
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
            None => return GlueResponse::InternalError { reason: "internal_error".to_owned() },
        };
        let entity_key = match parse_entity_key(key, &descriptor.group_keys) {
            Some(k) => k,
            None => return GlueResponse::QueryNotFound { code: "key_parse_failure" },
        };
        let tables = app.dev_agg.state_tables.lock();
        let value_opt = tables
            .get(&agg_node)
            .and_then(|t| t.query_feature(&entity_key, feature_idx, query_time_ms));
        return match value_opt {
            Some(v) => {
                let json_val = serde_json::json!({ "value": value_to_json(v) });
                match serde_json::to_vec(&json_val) {
                    Ok(b) => GlueResponse::QueryResult { body: Bytes::from(b) },
                    Err(e) => GlueResponse::InternalError { reason: e.to_string() },
                }
            }
            None => GlueResponse::QueryNotFound { code: "key_not_found" },
        };
    }

    // Case 2: `feature` is an aggregation node name (e.g. "TxnAgg").
    // Return all features for the entity as `{feat_name: value, ...}`.
    if let Some(descriptor) = registry.compiled_aggregation(feature) {
        let entity_key = match parse_entity_key(key, &descriptor.group_keys) {
            Some(k) => k,
            None => return GlueResponse::QueryNotFound { code: "key_parse_failure" },
        };
        let tables = app.dev_agg.state_tables.lock();
        let table = match tables.get(feature) {
            Some(t) => t,
            None => return GlueResponse::QueryNotFound { code: "key_not_found" },
        };
        let mut result = serde_json::Map::new();
        for (idx, named_op) in descriptor.features.iter().enumerate() {
            if let Some(v) = table.query_feature(&entity_key, idx, query_time_ms) {
                result.insert(named_op.feature_name.clone(), value_to_json(v));
            }
        }
        if result.is_empty() {
            return GlueResponse::QueryNotFound { code: "key_not_found" };
        }
        return match serde_json::to_vec(&serde_json::Value::Object(result)) {
            Ok(b) => GlueResponse::QueryResult { body: Bytes::from(b) },
            Err(e) => GlueResponse::InternalError { reason: e.to_string() },
        };
    }

    GlueResponse::QueryNotFound { code: "feature_not_found" }
}

fn dispatch_get_batch(_app: &Arc<AppState>, body: &Bytes) -> GlueResponse {
    // TODO(phase-18-followup): implement full batch GET dispatch.
    // Stub: delegate to the parse path and return an empty result for now.
    #[derive(serde::Deserialize)]
    struct BatchGetBody {
        #[allow(dead_code)]
        keys: Vec<String>,
        #[allow(dead_code)]
        features: Vec<String>,
    }
    let _req: BatchGetBody = match serde_json::from_slice(body) {
        Ok(r) => r,
        Err(e) => {
            return GlueResponse::InternalError { reason: e.to_string() };
        }
    };
    // Stub return — Plan 18-01 only requires GetSingle for the integration test.
    let empty = serde_json::json!({ "result": {} });
    match serde_json::to_vec(&empty) {
        Ok(b) => GlueResponse::QueryResult { body: Bytes::from(b) },
        Err(e) => GlueResponse::InternalError { reason: e.to_string() },
    }
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
