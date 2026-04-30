//! POST /register endpoint — parse, validate, diff, install, respond.
//!
//! Pipeline (8 steps):
//! 1. Content-Type check → 415
//! 2. JSON parse → 400
//! 3. Snapshot current registry for validation + diff
//! 4. validate_payload → 400
//! 5. compute_diff
//! 6. Conflict (diff.changed != []) → 409
//! 7. No-op (diff.added == []) → 200 same version
//! 8. Additive install → 200 new version

use beava_core::{
    register_validate::validate_payload,
    register_validate::ErrorCode,
    registry::Registry,
    registry_diff::{compute_diff, ConflictDetail, PayloadNode},
};
use beava_persistence::{PersistError, RecordType, WalSink};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tracing::{info, warn};

/// Phase 7 Plan 03: WAL record carrying a registration bump. Persisted in
/// the WAL before the in-memory registry is mutated.
///
/// Phase 7.5 Plan 01: encoded with `serde_json` rather than `bincode`.
/// PayloadNode contains `serde_json::Value` fields (`AggSpec.params`,
/// `OpNode::Fillna.defaults`) which bincode 1.x cannot deserialize
/// (`DeserializeAnyNotSupported`). RegistryBump records are emitted only on
/// `/register` (cold-path), so the JSON-vs-bincode size delta is irrelevant
/// to the hot path. JSON also gives recovery a self-describing payload that
/// is forward/backward compatible with descriptor-shape evolution.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RegistryBumpPayload {
    /// The new version number (post-bump). For replay diagnostics; not used
    /// to override the registry's monotonic version assignment.
    pub new_version: u64,
    /// Validated PayloadNodes that produced this bump.
    pub payload_nodes: Vec<PayloadNode>,
}

impl RegistryBumpPayload {
    pub fn encode(&self) -> Result<Vec<u8>, serde_json::Error> {
        serde_json::to_vec(self)
    }
    pub fn decode(bytes: &[u8]) -> Result<Self, serde_json::Error> {
        serde_json::from_slice(bytes)
    }
}

/// Re-apply a recovered RegistryBump record to the in-memory registry.
///
/// Re-runs validation + compile to rebuild caches, then calls
/// `apply_registration`. Idempotent on the descriptor set: if a node is
/// already present, it is left in place.
pub fn apply_registry_bump(
    registry: &Arc<Registry>,
    bump: RegistryBumpPayload,
) -> Result<(), String> {
    let snapshot = registry.snapshot();
    let validated = match validate_payload(&snapshot, bump.payload_nodes) {
        Ok(v) => v,
        Err(errs) => {
            return Err(format!(
                "validation failed during recovery (first error: {:?})",
                errs.first().map(|e| &e.reason)
            ));
        }
    };
    let (nodes, compiled_chains, propagated_schemas, compiled_aggregations) =
        validated.into_parts();
    registry.apply_registration(
        nodes,
        compiled_chains,
        propagated_schemas,
        compiled_aggregations,
    );
    Ok(())
}

/// Errors specific to the WAL-backed register pipeline.
#[derive(Debug)]
pub enum RegisterWalError {
    Encode(String),
    Persist(PersistError),
}

impl std::fmt::Display for RegisterWalError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RegisterWalError::Encode(s) => write!(f, "encode: {s}"),
            RegisterWalError::Persist(e) => write!(f, "persist: {e}"),
        }
    }
}

// ─── Wire types ───────────────────────────────────────────────────────────────

/// Wire shape of `POST /register` request body.
#[derive(Debug, Deserialize)]
pub struct RegisterPayload {
    pub nodes: Vec<PayloadNode>,
}

/// Shared axum state.
#[derive(Clone)]
pub struct RegisterAppState {
    pub registry: Arc<Registry>,
    /// Phase 7 Plan 03: when Some, /register writes a RegistryBump WAL record
    /// before mutating the in-memory registry. None for legacy callers
    /// (Phase 1/2 tests) where WAL plumbing is not yet wired.
    pub wal_sink: Option<WalSink>,
}

#[derive(Debug, Serialize)]
pub struct RegisterSuccess {
    pub status: &'static str, // always "ok"
    pub registry_version: u64,
    pub registered_descriptors: Vec<String>, // input order
    pub added: Vec<String>,
    pub already_present: Vec<String>,
}

#[derive(Debug, Serialize)]
pub struct RegisterErrorBody {
    pub error: RegisterError,
    pub registry_version: u64,
}

#[derive(Debug, Serialize)]
#[serde(untagged)]
pub enum RegisterError {
    Validation {
        code: &'static str, // "invalid_registration"
        path: String,
        reason: String,
    },
    Conflict {
        code: &'static str, // "registration_conflict"
        message: &'static str,
        diff: ResponseDiff,
    },
    UnsupportedMediaType {
        code: &'static str, // "unsupported_media_type"
        path: String,
        reason: String,
    },
}

#[derive(Debug, Serialize)]
pub struct ResponseDiff {
    pub added: Vec<String>,
    pub removed: Vec<String>, // always [] in v0
    pub changed: Vec<ConflictDetail>,
}

// ─── Transport-agnostic register core (Phase 2.5 Plan 03) ─────────────────────

/// Outcome of executing a registration, independent of transport (HTTP / TCP).
///
/// HTTP's `post_register` maps this to `(StatusCode, Json<Value>)`; the TCP
/// `handle_register` (Phase 2.5 Plan 04) maps the same cases to response frames
/// with op=OP_REGISTER (success) or op=OP_ERROR_RESPONSE (validation/conflict).
#[derive(Debug)]
pub enum RegisterOutcome {
    /// Additive install succeeded; version bumped.
    Success {
        version: u64,
        registered_descriptors: Vec<String>,
        added: Vec<String>,
        already_present: Vec<String>,
    },
    /// Payload was empty (zero nodes). 200 OK, version unchanged.
    EmptyPayload { version: u64 },
    /// Every node already present, none added. 200 OK, version unchanged.
    Noop {
        version: u64,
        registered_descriptors: Vec<String>,
        already_present: Vec<String>,
    },
    /// validate_payload returned at least one error. 400.
    /// v0 exposes "first-error-wins" on the wire; full Vec is logged at WARN.
    ValidationFailed {
        version: u64,
        #[allow(dead_code)]
        first_error_code: ErrorCode,
        first_error_path: String,
        first_error_reason: String,
        #[allow(dead_code)]
        all_errors_count: usize,
    },
    /// compute_diff found changed descriptors. 409.
    Conflict {
        version: u64,
        added: Vec<String>,
        changed: Vec<ConflictDetail>,
    },
    /// Phase 7 Plan 03: WAL append for the RegistryBump record failed. 503.
    WalUnavailable { version: u64 },
}

/// Phase 7 Plan 03: WAL-backed entry point. When `wal_sink` is `Some`, a
/// `RegistryBump` record is written + fsynced BEFORE the in-memory registry
/// is mutated (apply-AFTER-fsync invariant for registration). On WAL failure,
/// returns a `WalUnavailable` outcome so the HTTP layer maps to 503.
pub(crate) async fn execute_register_with_wal(
    registry: &Arc<Registry>,
    payload: RegisterPayload,
    wal_sink: &WalSink,
) -> RegisterOutcome {
    execute_register_inner(registry, payload, Some(wal_sink)).await
}

async fn execute_register_inner(
    registry: &Arc<Registry>,
    payload: RegisterPayload,
    wal_sink: Option<&WalSink>,
) -> RegisterOutcome {
    // 1. Empty-payload fast path (matches HTTP handler §pre-validation)
    if payload.nodes.is_empty() {
        let version = registry.version();
        info!(
            kind = "register.noop",
            version,
            nodes = 0,
            "register empty payload"
        );
        return RegisterOutcome::EmptyPayload { version };
    }

    // 2. Snapshot for validation + diff
    let current_snapshot = registry.snapshot();

    // 3. Validate (fail-soft: collects all errors)
    let validated = match validate_payload(&current_snapshot, payload.nodes) {
        Ok(v) => v,
        Err(errs) => {
            let first = &errs[0];
            warn!(
                kind = "register.validation_failed",
                path = %first.path,
                code = ?first.code,
                error_count = errs.len(),
                "register validation failed"
            );
            return RegisterOutcome::ValidationFailed {
                version: current_snapshot.version,
                first_error_code: first.code,
                first_error_path: first.path.clone(),
                first_error_reason: first.reason.clone(),
                all_errors_count: errs.len(),
            };
        }
    };

    let (nodes, compiled_chains, propagated_schemas, compiled_aggregations) =
        validated.into_parts();
    let registered_descriptors: Vec<String> = nodes.iter().map(|n| n.name().to_string()).collect();

    // 4. Diff
    let diff = compute_diff(&current_snapshot, &nodes);

    // 5. Conflict → no mutation
    if !diff.changed.is_empty() {
        warn!(
            kind = "register.conflict",
            version = current_snapshot.version,
            changed = ?diff.changed.iter().map(|c| &c.name).collect::<Vec<_>>(),
            "register conflict"
        );
        return RegisterOutcome::Conflict {
            version: current_snapshot.version,
            added: diff.added,
            changed: diff.changed,
        };
    }

    // 6. No-op (only already_present, no added)
    if diff.added.is_empty() {
        info!(
            kind = "register.noop",
            version = current_snapshot.version,
            nodes = registered_descriptors.len(),
            "register no-op"
        );
        return RegisterOutcome::Noop {
            version: current_snapshot.version,
            registered_descriptors,
            already_present: diff.already_present,
        };
    }

    // 7. Phase 7 Plan 03: WAL-append a RegistryBump record BEFORE applying.
    //    Apply-AFTER-fsync invariant — recovery sees the descriptors only after
    //    they are durable on disk.
    if let Some(sink) = wal_sink {
        let bump = RegistryBumpPayload {
            new_version: current_snapshot.version + 1,
            payload_nodes: nodes.clone(),
        };
        let encoded = match bump.encode() {
            Ok(b) => b,
            Err(e) => {
                warn!(
                    kind = "register.wal_encode_failed",
                    error = %e,
                    "RegistryBump encode failed"
                );
                return RegisterOutcome::WalUnavailable {
                    version: current_snapshot.version,
                };
            }
        };
        if let Err(e) = sink.append_record(RecordType::RegistryBump, encoded).await {
            warn!(
                kind = "register.wal_append_failed",
                error = %e,
                "RegistryBump WAL append failed"
            );
            return RegisterOutcome::WalUnavailable {
                version: current_snapshot.version,
            };
        }
    }

    // 8. Additive install (Phase 4: compiled chains + propagated schemas;
    //    Phase 5 Plan 04: compiled aggregations)
    let new_version = registry.apply_registration(
        nodes,
        compiled_chains,
        propagated_schemas,
        compiled_aggregations,
    );
    info!(
        kind = "register.success",
        version = new_version,
        added = ?diff.added,
        already_present_count = diff.already_present.len(),
        "register succeeded"
    );
    RegisterOutcome::Success {
        version: new_version,
        registered_descriptors,
        added: diff.added,
        already_present: diff.already_present,
    }
}

/// Serialize a response struct to `serde_json::Value`.
///
/// All response types (`RegisterSuccess`, `RegisterErrorBody`) contain only
/// `&'static str`, `u64`, `Vec<String>`, and `bool` fields — serialization of
/// these types via `#[derive(Serialize)]` is infallible. This helper documents
/// that invariant and provides an explicit 500-style fallback rather than an
/// unwrap panic, so a future change that accidentally adds a non-serializable
/// field fails gracefully instead of killing the Tokio runtime thread.
fn to_json_value<T: serde::Serialize>(v: T) -> serde_json::Value {
    serde_json::to_value(v).unwrap_or_else(|e| {
        // This branch is unreachable with the current response types.
        // If it is ever hit, log at error level and return a safe sentinel.
        tracing::error!(
            kind = "register.serialization_error",
            error = %e,
            "BUG: response struct failed to serialize — returning 500 sentinel"
        );
        serde_json::json!({
            "status": "error",
            "error": {"code": "internal_error", "reason": "response serialization failed"}
        })
    })
}

/// Serialise a `RegisterOutcome` into the
/// `GlueResponse::Register { http_status, body, tcp_op }` triple consumed
/// by the mio-path HTTP and TCP encoders. `tcp_op` is `OP_REGISTER` on
/// success, `OP_ERROR_RESPONSE` on failure. Used by
/// `apply_shard.rs::dispatch_one` and the data-plane glue.
pub fn register_outcome_to_glue(outcome: RegisterOutcome) -> (u16, bytes::Bytes, u16) {
    use beava_core::wire::{OP_ERROR_RESPONSE, OP_REGISTER};

    let (status, value) = map_outcome_to_response(outcome);
    let body_bytes = bytes::Bytes::from(serde_json::to_vec(&value).unwrap_or_default());
    let tcp_op = if status == 200 {
        OP_REGISTER
    } else {
        OP_ERROR_RESPONSE
    };
    (status, body_bytes, tcp_op)
}

/// Map a `RegisterOutcome` to `(http_status_code_u16, json_body)`. Plan
/// 12.6-07: replaced legacy `map_outcome_to_http` (which returned axum
/// `(StatusCode, Json<...>)`) with this transport-agnostic equivalent.
fn map_outcome_to_response(outcome: RegisterOutcome) -> (u16, serde_json::Value) {
    match outcome {
        RegisterOutcome::Success {
            version,
            registered_descriptors,
            added,
            already_present,
        } => {
            let resp = RegisterSuccess {
                status: "ok",
                registry_version: version,
                registered_descriptors,
                added,
                already_present,
            };
            (200, to_json_value(resp))
        }
        RegisterOutcome::EmptyPayload { version } => {
            let resp = RegisterSuccess {
                status: "ok",
                registry_version: version,
                registered_descriptors: vec![],
                added: vec![],
                already_present: vec![],
            };
            (200, to_json_value(resp))
        }
        RegisterOutcome::Noop {
            version,
            registered_descriptors,
            already_present,
        } => {
            let resp = RegisterSuccess {
                status: "ok",
                registry_version: version,
                registered_descriptors,
                added: vec![],
                already_present,
            };
            (200, to_json_value(resp))
        }
        RegisterOutcome::ValidationFailed {
            version,
            first_error_code,
            first_error_path,
            first_error_reason,
            ..
        } => {
            let wire_code = error_code_to_wire_str(first_error_code);
            let body = RegisterErrorBody {
                error: RegisterError::Validation {
                    code: wire_code,
                    path: first_error_path,
                    reason: first_error_reason,
                },
                registry_version: version,
            };
            (400, to_json_value(body))
        }
        RegisterOutcome::Conflict {
            version,
            added,
            changed,
        } => {
            let body = RegisterErrorBody {
                error: RegisterError::Conflict {
                    code: "registration_conflict",
                    message: "Registration would change or remove existing descriptors",
                    diff: ResponseDiff {
                        added,
                        removed: vec![],
                        changed,
                    },
                },
                registry_version: version,
            };
            (409, to_json_value(body))
        }
        RegisterOutcome::WalUnavailable { version } => {
            let body = serde_json::json!({
                "error": {
                    "code": "wal_unavailable",
                    "reason": "WAL append for registry bump failed; registry not mutated"
                },
                "registry_version": version,
            });
            (503, body)
        }
    }
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

/// Map an `ErrorCode` to its wire string.
///
/// Phase 4 (Plan 04-05): `InvalidExpression`, `UnknownFieldReference`,
/// `SchemaPropagationFailure`, and `InvalidCastTarget` all surface as
/// `"invalid_expression"` on the wire (distinct from `"invalid_registration"`
/// for structural rules 1-9).
///
/// Phase 5 Plan 04 (Rule 11): aggregation-specific error codes map to their
/// own wire strings so clients can distinguish aggregation failures from
/// general registration/expression failures.
pub(crate) fn error_code_to_wire_str(code: ErrorCode) -> &'static str {
    match code {
        ErrorCode::InvalidExpression
        | ErrorCode::UnknownFieldReference
        | ErrorCode::SchemaPropagationFailure
        | ErrorCode::InvalidCastTarget => "invalid_expression",
        // Rule 11 aggregation codes
        ErrorCode::AggregationOnTableNotSupported => "aggregation_on_table_not_supported",
        ErrorCode::AggregationUnknownField => "aggregation_unknown_field",
        ErrorCode::AggregationInvalidWhere => "aggregation_invalid_where",
        ErrorCode::AggregationInvalidWindow => "aggregation_invalid_window",
        ErrorCode::AggregationUnknownOp => "aggregation_unknown_op",
        ErrorCode::AggregationDuplicateFeatureName => "aggregation_duplicate_feature_name",
        ErrorCode::AggregationGroupKeyCollidesWithFeature => {
            "aggregation_group_key_collides_with_feature"
        }
        ErrorCode::AggregationFeatureNameCollisionAcrossAggregations => {
            "aggregation_feature_name_collision_across_aggregations"
        }
        // Plan 10-05: sketch-op error codes
        ErrorCode::WindowNotSupported => "window_not_supported",
        ErrorCode::InvalidPercentileQ => "invalid_percentile_q",
        ErrorCode::InvalidTopKK => "invalid_top_k_k",
        ErrorCode::InvalidBloomFpr => "invalid_bloom_fpr",
        _ => "invalid_registration",
    }
}

/// v0: returns `("<body>", err.to_string())`. Richer JSON-pointer paths are
/// Phase 3+ work. Used by the mio glue layer to produce `error.path` +
/// `error.reason` pairs for malformed `/register` request bodies.
pub fn format_serde_error_public(e: &serde_json::Error) -> (String, String) {
    ("<body>".to_string(), e.to_string())
}
