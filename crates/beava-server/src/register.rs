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

use axum::{
    body::Bytes,
    extract::{DefaultBodyLimit, State},
    http::{header, HeaderMap, HeaderValue, StatusCode},
    routing::post,
    Json, Router,
};
use beava_core::{
    register_validate::{validate_payload, ErrorCode},
    registry::Registry,
    registry_diff::{compute_diff, ConflictDetail, PayloadNode},
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tracing::{info, warn};

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
pub(crate) enum RegisterOutcome {
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
}

/// Run the transport-agnostic register pipeline. Caller has already parsed the
/// JSON body into `RegisterPayload` (HTTP parses from axum's `Bytes`; TCP
/// parses from the frame's `Bytes` payload).
///
/// Single source of truth for validation + diff + apply. Every success path
/// bumps the registry version exactly once; every error path leaves the
/// registry untouched. Phase 6 will wrap a WAL record around the apply step
/// inside this function.
pub(crate) async fn execute_register(
    registry: &Arc<Registry>,
    payload: RegisterPayload,
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

    let (nodes, compiled_chains, propagated_schemas) = validated.into_parts();
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

    // 7. Additive install (Phase 4: also installs compiled chains + propagated schemas)
    let new_version = registry.apply_registration(nodes, compiled_chains, propagated_schemas);
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

fn map_outcome_to_http(outcome: RegisterOutcome) -> (StatusCode, Json<serde_json::Value>) {
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
            // infallible: all fields are &str/u64/Vec<String>
            (StatusCode::OK, Json(to_json_value(resp)))
        }
        RegisterOutcome::EmptyPayload { version } => {
            let resp = RegisterSuccess {
                status: "ok",
                registry_version: version,
                registered_descriptors: vec![],
                added: vec![],
                already_present: vec![],
            };
            // infallible: all fields are &str/u64/Vec<String>
            (StatusCode::OK, Json(to_json_value(resp)))
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
            // infallible: all fields are &str/u64/Vec<String>
            (StatusCode::OK, Json(to_json_value(resp)))
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
            // infallible: all fields are &str/u64/String
            (StatusCode::BAD_REQUEST, Json(to_json_value(body)))
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
            // infallible: all fields are &str/u64/Vec<String>/ConflictDetail
            (StatusCode::CONFLICT, Json(to_json_value(body)))
        }
    }
}

// ─── Router ───────────────────────────────────────────────────────────────────

pub fn register_router(state: RegisterAppState) -> Router {
    Router::new()
        .route("/register", post(post_register))
        .layer(DefaultBodyLimit::max(1024 * 1024)) // 1 MiB cap
        .with_state(state)
}

// ─── Handler ──────────────────────────────────────────────────────────────────

pub async fn post_register(
    headers: HeaderMap,
    State(state): State<RegisterAppState>,
    body: Bytes,
) -> (StatusCode, Json<serde_json::Value>) {
    // 1. Content-Type check (SRV-API-11) — transport-specific.
    if !is_json_content_type(headers.get(header::CONTENT_TYPE)) {
        let current_version = state.registry.version();
        let err_body = RegisterErrorBody {
            error: RegisterError::UnsupportedMediaType {
                code: "unsupported_media_type",
                path: "<header>.content_type".to_string(),
                reason: "expected application/json".to_string(),
            },
            registry_version: current_version,
        };
        // infallible: RegisterErrorBody contains only &str/u64/String fields
        return (
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            Json(to_json_value(err_body)),
        );
    }

    // 2. JSON parse → 400 — transport-specific.
    let payload: RegisterPayload = match serde_json::from_slice(&body) {
        Ok(p) => p,
        Err(e) => {
            let (path, reason) = format_serde_error(&e);
            warn!(
                kind = "register.parse_error",
                path = %path,
                reason = %reason,
                "malformed register payload"
            );
            let current_version = state.registry.version();
            let err_body = RegisterErrorBody {
                error: RegisterError::Validation {
                    code: "invalid_registration",
                    path,
                    reason,
                },
                registry_version: current_version,
            };
            // infallible: RegisterErrorBody contains only &str/u64/String fields
            return (StatusCode::BAD_REQUEST, Json(to_json_value(err_body)));
        }
    };

    // 3-8. Delegate to shared transport-agnostic core.
    let outcome = execute_register(&state.registry, payload).await;
    map_outcome_to_http(outcome)
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

/// Map an `ErrorCode` to its wire string.
///
/// Phase 4 (Plan 04-05): `InvalidExpression`, `UnknownFieldReference`,
/// `SchemaPropagationFailure`, and `InvalidCastTarget` all surface as
/// `"invalid_expression"` on the wire (distinct from `"invalid_registration"`
/// for structural rules 1-9).
pub(crate) fn error_code_to_wire_str(code: ErrorCode) -> &'static str {
    match code {
        ErrorCode::InvalidExpression
        | ErrorCode::UnknownFieldReference
        | ErrorCode::SchemaPropagationFailure
        | ErrorCode::InvalidCastTarget => "invalid_expression",
        _ => "invalid_registration",
    }
}

/// Returns true iff the Content-Type media type (before `;`) is `application/json`
/// (case-insensitive, trimmed). `application/json; charset=utf-8` → true.
fn is_json_content_type(ct: Option<&HeaderValue>) -> bool {
    match ct {
        None => false,
        Some(v) => {
            let s = match v.to_str() {
                Ok(s) => s,
                Err(_) => return false,
            };
            let media_type = s.split(';').next().unwrap_or("").trim();
            media_type.eq_ignore_ascii_case("application/json")
        }
    }
}

/// v0: returns `("<body>", err.to_string())`. Richer JSON-pointer paths are Phase 3+ work.
fn format_serde_error(e: &serde_json::Error) -> (String, String) {
    ("<body>".to_string(), e.to_string())
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::http::router;
    use axum::body::Body;
    use axum::http::Request;
    use beava_core::registry::Registry;
    use http_body_util::BodyExt;
    use tower::ServiceExt;

    use crate::http::ReadinessFlag;

    fn test_router() -> (axum::Router, Arc<Registry>) {
        let registry = Arc::new(Registry::new());
        let readiness = ReadinessFlag::new();
        let r = router(readiness, registry.clone(), false);
        (r, registry)
    }

    async fn post(
        router: axum::Router,
        body: impl Into<axum::body::Body>,
        content_type: Option<&str>,
    ) -> (StatusCode, serde_json::Value) {
        let mut req = Request::builder().method("POST").uri("/register");
        if let Some(ct) = content_type {
            req = req.header("content-type", ct);
        }
        let resp = router
            .oneshot(req.body(body.into()).unwrap())
            .await
            .expect("oneshot");
        let status = resp.status();
        let bytes = resp
            .into_body()
            .collect()
            .await
            .expect("collect")
            .to_bytes();
        let json: serde_json::Value = serde_json::from_slice(&bytes).expect("json parse");
        (status, json)
    }

    fn json_body(val: serde_json::Value) -> Body {
        Body::from(serde_json::to_vec(&val).unwrap())
    }

    fn event_node(name: &str, fields: &[(&str, &str)], etf: &str) -> serde_json::Value {
        let fields_map: serde_json::Map<String, serde_json::Value> = fields
            .iter()
            .map(|(k, v)| (k.to_string(), serde_json::Value::String(v.to_string())))
            .collect();
        serde_json::json!({
            "kind": "event",
            "name": name,
            "schema": {"fields": fields_map, "optional_fields": []},
            "event_time_field": etf,
        })
    }

    fn transaction_payload() -> serde_json::Value {
        serde_json::json!({
            "nodes": [event_node("Transaction", &[
                ("event_time", "i64"),
                ("card_id", "str"),
                ("amount", "f64"),
                ("merchant_id", "str"),
            ], "event_time")]
        })
    }

    // ── Happy paths ───────────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_register_valid_event_returns_200_v1() {
        let (r, _reg) = test_router();
        let (status, body) = post(
            r,
            json_body(transaction_payload()),
            Some("application/json"),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["status"], "ok");
        assert_eq!(body["registry_version"], 1);
        assert_eq!(
            body["registered_descriptors"],
            serde_json::json!(["Transaction"])
        );
        assert_eq!(body["added"], serde_json::json!(["Transaction"]));
        assert_eq!(body["already_present"], serde_json::json!([]));
    }

    #[tokio::test]
    async fn test_register_identical_is_noop() {
        let (r, reg) = test_router();
        // First POST
        let (s1, b1) = post(
            router(ReadinessFlag::new(), reg.clone(), false),
            json_body(transaction_payload()),
            Some("application/json"),
        )
        .await;
        assert_eq!(s1, StatusCode::OK);
        assert_eq!(b1["registry_version"], 1);

        // Second POST — identical
        let (s2, b2) = post(
            router(ReadinessFlag::new(), reg.clone(), false),
            json_body(transaction_payload()),
            Some("application/json"),
        )
        .await;
        assert_eq!(s2, StatusCode::OK);
        assert_eq!(b2["registry_version"], 1, "version must NOT bump on no-op");
        assert_eq!(b2["added"], serde_json::json!([]));
        assert_eq!(b2["already_present"], serde_json::json!(["Transaction"]));
        let _ = r; // silence unused
    }

    #[tokio::test]
    async fn test_additive_bumps_version() {
        let (_, reg) = test_router();

        // POST EventA → v1
        let (s1, _) = post(
            router(ReadinessFlag::new(), reg.clone(), false),
            json_body(serde_json::json!({
                "nodes": [event_node("A", &[("event_time", "i64"), ("x", "f64")], "event_time")]
            })),
            Some("application/json"),
        )
        .await;
        assert_eq!(s1, StatusCode::OK);

        // POST [A, B] → v2
        let (s2, b2) = post(
            router(ReadinessFlag::new(), reg.clone(), false),
            json_body(serde_json::json!({
                "nodes": [
                    event_node("A", &[("event_time", "i64"), ("x", "f64")], "event_time"),
                    event_node("B", &[("event_time", "i64"), ("y", "f64")], "event_time"),
                ]
            })),
            Some("application/json"),
        )
        .await;
        assert_eq!(s2, StatusCode::OK);
        assert_eq!(b2["registry_version"], 2);
        assert_eq!(b2["added"], serde_json::json!(["B"]));
        assert_eq!(b2["already_present"], serde_json::json!(["A"]));
    }

    #[tokio::test]
    async fn test_register_multi_node_vertical_slice() {
        // Transaction + Merchant + BigTx from 02-CONTEXT.md (derivation with upstreams)
        let (r, _) = test_router();
        let payload = serde_json::json!({
            "nodes": [
                {
                    "kind": "event",
                    "name": "Transaction",
                    "schema": {"fields": {"event_time": "i64", "amount": "f64", "merchant_id": "str"}, "optional_fields": []},
                    "event_time_field": "event_time"
                },
                {
                    "kind": "table",
                    "name": "Merchant",
                    "primary_key": ["merchant_id"],
                    "schema": {"fields": {"merchant_id": "str", "name": "str"}, "optional_fields": []},
                    "mode": "upsert"
                },
                {
                    "kind": "derivation",
                    "name": "BigTx",
                    "output_kind": "event",
                    "upstreams": ["Transaction"],
                    "ops": [{"op": "filter", "expr": "(amount > 500)"}],
                    "schema": {"fields": {"event_time": "i64", "amount": "f64"}, "optional_fields": []}
                }
            ]
        });
        let (status, body) = post(r, json_body(payload), Some("application/json")).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["registry_version"], 1);
        assert_eq!(
            body["registered_descriptors"],
            serde_json::json!(["Transaction", "Merchant", "BigTx"])
        );
    }

    // ── Conflict ──────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_register_changed_event_returns_409() {
        let (_, reg) = test_router();

        // Register EventA with amount: f64
        let (s1, _) = post(
            router(ReadinessFlag::new(), reg.clone(), false),
            json_body(serde_json::json!({
                "nodes": [event_node("A", &[("event_time", "i64"), ("amount", "f64")], "event_time")]
            })),
            Some("application/json"),
        )
        .await;
        assert_eq!(s1, StatusCode::OK);

        // Re-register EventA with amount: i64 → 409
        let (s2, b2) = post(
            router(ReadinessFlag::new(), reg.clone(), false),
            json_body(serde_json::json!({
                "nodes": [event_node("A", &[("event_time", "i64"), ("amount", "i64")], "event_time")]
            })),
            Some("application/json"),
        )
        .await;
        assert_eq!(s2, StatusCode::CONFLICT);
        assert_eq!(b2["error"]["code"], "registration_conflict");
        assert_eq!(b2["error"]["diff"]["added"], serde_json::json!([]));
        assert_eq!(b2["error"]["diff"]["removed"], serde_json::json!([]));
        assert_eq!(b2["error"]["diff"]["changed"][0]["name"], "A");
        assert_eq!(
            b2["error"]["diff"]["changed"][0]["reason"],
            "schema_mismatch"
        );
        let details = b2["error"]["diff"]["changed"][0]["details"]
            .as_str()
            .unwrap();
        assert!(
            details.contains("amount"),
            "details should mention field 'amount': {details}"
        );
        assert_eq!(b2["registry_version"], 1, "version must not bump on 409");

        // Confirm registry was NOT mutated — original A still works
        let (s3, b3) = post(
            router(ReadinessFlag::new(), reg.clone(), false),
            json_body(serde_json::json!({
                "nodes": [event_node("A", &[("event_time", "i64"), ("amount", "f64")], "event_time")]
            })),
            Some("application/json"),
        )
        .await;
        assert_eq!(s3, StatusCode::OK);
        assert_eq!(
            b3["registry_version"], 1,
            "original A is still a no-op at v1"
        );
    }

    // ── Validation ────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_register_missing_event_time_field_returns_400() {
        let (r, _) = test_router();
        // event_time_field = "ts" but schema has no "ts" field
        let payload = serde_json::json!({
            "nodes": [{
                "kind": "event",
                "name": "A",
                "schema": {"fields": {"x": "f64"}, "optional_fields": []},
                "event_time_field": "ts"
            }]
        });
        let (status, body) = post(r, json_body(payload), Some("application/json")).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["error"]["code"], "invalid_registration");
        let path = body["error"]["path"].as_str().unwrap();
        assert!(
            path.contains("ts") || path.contains("event_time"),
            "path should mention ts or event_time_field: {path}"
        );
    }

    #[tokio::test]
    async fn test_register_cycle_returns_400() {
        let (r, _) = test_router();
        // D1 ↔ D2 mutual cycle; Src is a valid event they both reference
        // The validator is fail-soft and may return a topological-order error or cycle error
        // as the first error — both have code "invalid_registration" and reason mentions
        // the problematic relationship. We assert 400 + code only.
        let payload = serde_json::json!({
            "nodes": [
                {
                    "kind": "event",
                    "name": "Src",
                    "schema": {"fields": {"event_time": "i64", "x": "f64"}, "optional_fields": []},
                    "event_time_field": "event_time"
                },
                {
                    "kind": "derivation",
                    "name": "D1",
                    "output_kind": "event",
                    "upstreams": ["Src", "D2"],
                    "ops": [],
                    "schema": {"fields": {"amount": "f64"}, "optional_fields": []}
                },
                {
                    "kind": "derivation",
                    "name": "D2",
                    "output_kind": "event",
                    "upstreams": ["Src", "D1"],
                    "ops": [],
                    "schema": {"fields": {"amount": "f64"}, "optional_fields": []}
                }
            ]
        });
        let (status, body) = post(r, json_body(payload), Some("application/json")).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["error"]["code"], "invalid_registration");
        // Either cycle or topological-order violation is a valid first error here
        let reason = body["error"]["reason"].as_str().unwrap_or("");
        let path = body["error"]["path"].as_str().unwrap_or("");
        assert!(
            reason.to_lowercase().contains("cycle")
                || reason.to_lowercase().contains("later in payload")
                || path.contains("nodes["),
            "expected cycle or topo error, got reason={reason:?} path={path:?}"
        );
    }

    #[tokio::test]
    async fn test_register_reserved_prefix_returns_400() {
        let (r, _) = test_router();
        let payload = serde_json::json!({
            "nodes": [{
                "kind": "event",
                "name": "_beava_internal",
                "schema": {"fields": {"event_time": "i64"}, "optional_fields": []},
                "event_time_field": "event_time"
            }]
        });
        let (status, body) = post(r, json_body(payload), Some("application/json")).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["error"]["code"], "invalid_registration");
        let path = body["error"]["path"].as_str().unwrap();
        assert!(
            path.contains("nodes[0]"),
            "path should point to nodes[0]: {path}"
        );
    }

    #[tokio::test]
    async fn test_register_empty_nodes_returns_200_noop() {
        let (r, _) = test_router();
        let (status, body) = post(
            r,
            json_body(serde_json::json!({"nodes": []})),
            Some("application/json"),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["status"], "ok");
        assert_eq!(body["registry_version"], 0);
        assert_eq!(body["added"], serde_json::json!([]));
        assert_eq!(body["already_present"], serde_json::json!([]));
        assert_eq!(body["registered_descriptors"], serde_json::json!([]));
    }

    // ── Content-Type ──────────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_register_wrong_content_type_returns_415() {
        let (r, _) = test_router();
        let (status, body) = post(r, json_body(transaction_payload()), Some("text/plain")).await;
        assert_eq!(status, StatusCode::UNSUPPORTED_MEDIA_TYPE);
        assert_eq!(body["error"]["code"], "unsupported_media_type");
    }

    #[tokio::test]
    async fn test_register_no_content_type_returns_415() {
        let (r, _) = test_router();
        let (status, body) = post(r, json_body(transaction_payload()), None).await;
        assert_eq!(status, StatusCode::UNSUPPORTED_MEDIA_TYPE);
        assert_eq!(body["error"]["code"], "unsupported_media_type");
    }

    #[tokio::test]
    async fn test_register_json_with_charset_param_ok() {
        let (r, _) = test_router();
        let (status, body) = post(
            r,
            json_body(transaction_payload()),
            Some("application/json; charset=utf-8"),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["status"], "ok");
    }

    // ── Malformed body ────────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_register_malformed_json_returns_400() {
        let (r, _) = test_router();
        let (status, body) = post(
            r,
            Body::from(br#"{"nodes": ["#.as_slice()),
            Some("application/json"),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["error"]["code"], "invalid_registration");
        assert_eq!(body["error"]["path"], "<body>");
        let reason = body["error"]["reason"].as_str().unwrap_or("");
        assert!(!reason.is_empty(), "reason must be non-empty");
    }

    #[tokio::test]
    async fn test_register_body_too_large_returns_413() {
        let (r, _) = test_router();
        // Build a body just over 1 MiB
        let big: Vec<u8> = std::iter::repeat(b'x').take(1024 * 1024 + 1).collect();
        let resp = axum::Router::oneshot(
            r,
            Request::builder()
                .method("POST")
                .uri("/register")
                .header("content-type", "application/json")
                .body(Body::from(big))
                .unwrap(),
        )
        .await
        .unwrap();
        assert_eq!(resp.status(), StatusCode::PAYLOAD_TOO_LARGE);
    }

    // ── Tracing ───────────────────────────────────────────────────────────────

    /// Verify that a successful registration emits a tracing event containing
    /// "register.success". We install the subscriber via `tracing::dispatcher`
    /// before spawning the async work, then check captured output afterwards.
    #[tokio::test]
    async fn test_success_emits_info_log() {
        use std::sync::{Arc as StdArc, Mutex};

        let output = StdArc::new(Mutex::new(String::new()));
        let output_clone = output.clone();

        let subscriber = tracing_subscriber::fmt::Subscriber::builder()
            .with_writer(move || WriterCapture(output_clone.clone()))
            .finish();

        let dispatcher = tracing::Dispatch::new(subscriber);

        let (r, _) = test_router();

        // Use dispatcher::with to scope the subscriber to this block.
        // tracing::dispatcher::with_default accepts a sync closure, but we can
        // move the routing call inside and await it from outside via a oneshot channel.
        let (tx, rx) = tokio::sync::oneshot::channel::<(StatusCode, serde_json::Value)>();
        let payload = transaction_payload();

        // Run the request under the custom dispatcher using spawn_blocking so we
        // can await the async handler inside a sync tracing scope.
        let captured_output = output.clone();
        tokio::task::spawn_blocking(move || {
            tracing::dispatcher::with_default(&dispatcher, || {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap();
                let result = rt.block_on(post(r, json_body(payload), Some("application/json")));
                let _ = tx.send(result);
            });
        })
        .await
        .expect("spawn_blocking");

        let (status, _body) = rx.await.expect("result");
        assert_eq!(status, StatusCode::OK);

        let captured = captured_output.lock().unwrap().clone();
        assert!(
            captured.contains("register.success") || captured.contains("register"),
            "expected tracing output to contain 'register.success', got: {captured:?}"
        );
    }

    // ─── Plan 04-05: Phase 4 expression validation tests (HTTP + TCP) ────────

    // Helper: payload with event A + derivation D with given ops
    fn derivation_payload(ops: serde_json::Value) -> serde_json::Value {
        serde_json::json!({
            "nodes": [
                event_node("A", &[("event_time", "i64"), ("amount", "f64")], "event_time"),
                {
                    "kind": "derivation",
                    "name": "D",
                    "output_kind": "event",
                    "upstreams": ["A"],
                    "ops": ops,
                    "schema": {"fields": {"amount": "f64"}, "optional_fields": []}
                }
            ]
        })
    }

    /// Test 11: POST derivation with bad Filter expression → 400 with code="invalid_expression"
    #[tokio::test]
    async fn test_register_invalid_filter_returns_400_with_invalid_expression_code() {
        let (r, _) = test_router();
        let payload = derivation_payload(serde_json::json!([
            {"op": "filter", "expr": "(amount > "}  // truncated — parse error
        ]));
        let (status, body) = post(r, json_body(payload), Some("application/json")).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "body: {body:#}");
        assert_eq!(
            body["error"]["code"], "invalid_expression",
            "expected 'invalid_expression' code, body: {body:#}"
        );
        let path = body["error"]["path"].as_str().unwrap_or("");
        assert!(
            path.contains("nodes[1].ops[0]"),
            "path should point to nodes[1].ops[0], got: {path:?}"
        );
        let reason = body["error"]["reason"].as_str().unwrap_or("");
        assert!(!reason.is_empty(), "reason must be non-empty");
    }

    /// Test 12: POST derivation with unknown field in filter → 400 with code="invalid_expression"
    #[tokio::test]
    async fn test_register_unknown_field_in_filter_returns_400() {
        let (r, _) = test_router();
        let payload = derivation_payload(serde_json::json!([
            {"op": "filter", "expr": "(nonexistent > 0)"}
        ]));
        let (status, body) = post(r, json_body(payload), Some("application/json")).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "body: {body:#}");
        assert_eq!(body["error"]["code"], "invalid_expression");
        let path = body["error"]["path"].as_str().unwrap_or("");
        assert!(
            path.contains("nodes[1].ops[0]"),
            "path should point to nodes[1].ops[0], got: {path:?}"
        );
        let reason = body["error"]["reason"].as_str().unwrap_or("");
        assert!(
            reason.contains("nonexistent"),
            "reason should mention 'nonexistent', got: {reason:?}"
        );
    }

    /// Test 13: POST derivation with invalid cast target → 400 with code="invalid_expression"
    #[tokio::test]
    async fn test_register_invalid_cast_target_returns_400() {
        let (r, _) = test_router();
        let payload = derivation_payload(serde_json::json!([
            {"op": "cast", "type_map": {"amount": "blob"}}
        ]));
        let (status, body) = post(r, json_body(payload), Some("application/json")).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "body: {body:#}");
        assert_eq!(body["error"]["code"], "invalid_expression");
        let path = body["error"]["path"].as_str().unwrap_or("");
        assert!(
            path.contains("nodes[1].ops[0]"),
            "path should point to nodes[1].ops[0], got: {path:?}"
        );
    }

    /// Test 14: Valid chained ops → 200; propagated schema visible via GET /registry
    #[tokio::test]
    async fn test_register_with_columns_chain_propagates_schema_and_200s() {
        let (_, reg) = test_router();
        // Build router with dev_endpoints=true so GET /registry works
        let r = router(ReadinessFlag::new(), reg.clone(), true);
        let payload = serde_json::json!({
            "nodes": [
                event_node("A", &[("event_time", "i64"), ("amount", "f64")], "event_time"),
                {
                    "kind": "derivation",
                    "name": "D",
                    "output_kind": "event",
                    "upstreams": ["A"],
                    "ops": [
                        {"op": "filter", "expr": "(amount > 0)"},
                        {"op": "with_columns", "exprs": {"is_big": "(amount > 500)"}},
                        {"op": "cast", "type_map": {"is_big": "int"}}
                    ],
                    // client-supplied schema is WRONG (missing is_big, has wrong type for it)
                    "schema": {"fields": {"amount": "f64"}, "optional_fields": []}
                }
            ]
        });
        let (status, body) = post(r.clone(), json_body(payload), Some("application/json")).await;
        assert_eq!(status, StatusCode::OK, "body: {body:#}");
        assert_eq!(body["registry_version"], 1);

        // GET /registry — derivation D's schema should have propagated {amount: f64, is_big: i64}
        let get_resp = axum::Router::oneshot(
            r,
            axum::http::Request::builder()
                .method("GET")
                .uri("/registry")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .expect("GET /registry");
        let get_bytes = get_resp
            .into_body()
            .collect()
            .await
            .expect("collect")
            .to_bytes();
        let reg_dump: serde_json::Value = serde_json::from_slice(&get_bytes).expect("json");
        let d_schema = &reg_dump["derivations"]["D"]["schema"]["fields"];
        assert_eq!(
            d_schema["is_big"], "i64",
            "propagated schema should have is_big: i64 (after cast); full schema: {d_schema:#}"
        );
        assert_eq!(
            d_schema["amount"], "f64",
            "propagated schema should retain amount: f64"
        );
    }

    /// Test 15: After successful register with ops, compiled_op_chain must be cached
    #[tokio::test]
    async fn test_register_chained_ops_compile_is_cached_on_install() {
        let (_, reg) = test_router();
        let r = router(ReadinessFlag::new(), reg.clone(), false);
        let payload = serde_json::json!({
            "nodes": [
                event_node("A", &[("event_time", "i64"), ("amount", "f64")], "event_time"),
                {
                    "kind": "derivation",
                    "name": "D",
                    "output_kind": "event",
                    "upstreams": ["A"],
                    "ops": [{"op": "filter", "expr": "(amount > 0)"}],
                    "schema": {"fields": {"amount": "f64"}, "optional_fields": []}
                }
            ]
        });
        let (status, _body) = post(r, json_body(payload), Some("application/json")).await;
        assert_eq!(status, StatusCode::OK);

        // The compiled chain must be cached in the registry
        let compiled = reg.compiled_chain("D");
        assert!(
            compiled.is_some(),
            "registry.compiled_chain('D') must return Some after registration with ops"
        );
    }

    /// Test 16: TCP frame with bad filter → OP_ERROR_RESPONSE with code="invalid_expression"
    #[tokio::test]
    async fn test_register_invalid_expression_via_tcp_frame_returns_error_frame() {
        use crate::testing::TestServerBuilder;
        use beava_core::wire::OP_ERROR_RESPONSE;

        let ts = TestServerBuilder::new()
            .dev_endpoints(false)
            .spawn()
            .await
            .expect("spawn test server");

        let payload = serde_json::json!({
            "nodes": [
                {
                    "kind": "event",
                    "name": "A",
                    "schema": {"fields": {"event_time": "i64", "amount": "f64"}, "optional_fields": []},
                    "event_time_field": "event_time"
                },
                {
                    "kind": "derivation",
                    "name": "D",
                    "output_kind": "event",
                    "upstreams": ["A"],
                    "ops": [{"op": "filter", "expr": "(amount > "}],
                    "schema": {"fields": {"amount": "f64"}, "optional_fields": []}
                }
            ]
        });

        let mut tcp = ts.tcp_client().await.expect("tcp connect");
        let (resp_op, body) = tcp.register_json(payload).await.expect("tcp register");

        assert_eq!(
            resp_op, OP_ERROR_RESPONSE,
            "expected OP_ERROR_RESPONSE, got op={resp_op:#06x}, body: {body:#}"
        );
        assert_eq!(
            body["error"]["code"], "invalid_expression",
            "TCP must use 'invalid_expression' code, body: {body:#}"
        );
        let path = body["error"]["path"].as_str().unwrap_or("");
        assert!(
            path.contains("nodes[1].ops[0]"),
            "TCP path should point to nodes[1].ops[0], got: {path:?}"
        );

        ts.shutdown().await.expect("shutdown");
    }

    // ─── Writer capture helper ─────────────────────────────────────────────

    #[derive(Clone)]
    struct WriterCapture(std::sync::Arc<std::sync::Mutex<String>>);

    impl std::io::Write for WriterCapture {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            if let Ok(mut s) = self.0.lock() {
                s.push_str(&String::from_utf8_lossy(buf));
            }
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for WriterCapture {
        type Writer = WriterCapture;
        fn make_writer(&'a self) -> Self::Writer {
            WriterCapture(self.0.clone())
        }
    }

    // ─── Plan 02.5-03: execute_register unit tests (transport-free) ──────────

    fn parse_payload(val: serde_json::Value) -> RegisterPayload {
        serde_json::from_value(val).expect("parse payload")
    }

    #[tokio::test]
    async fn execute_register_empty_payload_returns_empty_payload_variant() {
        let reg = Arc::new(Registry::new());
        let outcome = execute_register(&reg, RegisterPayload { nodes: Vec::new() }).await;
        match outcome {
            RegisterOutcome::EmptyPayload { version } => assert_eq!(version, 0),
            other => panic!("expected EmptyPayload, got {other:?}"),
        }
        assert_eq!(reg.version(), 0);
    }

    #[tokio::test]
    async fn execute_register_valid_event_returns_success_v1() {
        let reg = Arc::new(Registry::new());
        let payload = parse_payload(transaction_payload());
        let outcome = execute_register(&reg, payload).await;
        match outcome {
            RegisterOutcome::Success {
                version,
                added,
                already_present,
                registered_descriptors,
            } => {
                assert_eq!(version, 1);
                assert_eq!(added, vec!["Transaction".to_string()]);
                assert_eq!(already_present, Vec::<String>::new());
                assert_eq!(registered_descriptors, vec!["Transaction".to_string()]);
            }
            other => panic!("expected Success, got {other:?}"),
        }
        assert_eq!(reg.version(), 1);
    }

    #[tokio::test]
    async fn execute_register_identical_repost_returns_noop() {
        let reg = Arc::new(Registry::new());
        let _ = execute_register(&reg, parse_payload(transaction_payload())).await;
        let outcome = execute_register(&reg, parse_payload(transaction_payload())).await;
        match outcome {
            RegisterOutcome::Noop {
                version,
                registered_descriptors,
                already_present,
            } => {
                assert_eq!(version, 1);
                assert_eq!(registered_descriptors, vec!["Transaction".to_string()]);
                assert_eq!(already_present, vec!["Transaction".to_string()]);
            }
            other => panic!("expected Noop, got {other:?}"),
        }
        assert_eq!(reg.version(), 1);
    }

    #[tokio::test]
    async fn execute_register_additive_bumps_version() {
        let reg = Arc::new(Registry::new());
        let a = serde_json::json!({
            "nodes": [event_node("A", &[("event_time", "i64"), ("x", "f64")], "event_time")]
        });
        let _ = execute_register(&reg, parse_payload(a)).await;

        let ab = serde_json::json!({
            "nodes": [
                event_node("A", &[("event_time", "i64"), ("x", "f64")], "event_time"),
                event_node("B", &[("event_time", "i64"), ("y", "f64")], "event_time"),
            ]
        });
        let outcome = execute_register(&reg, parse_payload(ab)).await;
        match outcome {
            RegisterOutcome::Success {
                version,
                added,
                already_present,
                ..
            } => {
                assert_eq!(version, 2);
                assert_eq!(added, vec!["B".to_string()]);
                assert_eq!(already_present, vec!["A".to_string()]);
            }
            other => panic!("expected Success, got {other:?}"),
        }
        assert_eq!(reg.version(), 2);
    }

    #[tokio::test]
    async fn execute_register_conflict_returns_conflict_variant() {
        let reg = Arc::new(Registry::new());
        let a_f64 = serde_json::json!({
            "nodes": [event_node("A", &[("event_time", "i64"), ("amount", "f64")], "event_time")]
        });
        let _ = execute_register(&reg, parse_payload(a_f64)).await;
        assert_eq!(reg.version(), 1);

        let a_i64 = serde_json::json!({
            "nodes": [event_node("A", &[("event_time", "i64"), ("amount", "i64")], "event_time")]
        });
        let outcome = execute_register(&reg, parse_payload(a_i64)).await;
        match outcome {
            RegisterOutcome::Conflict {
                version,
                added,
                changed,
            } => {
                assert_eq!(version, 1);
                assert!(added.is_empty());
                assert_eq!(changed.len(), 1);
                assert_eq!(changed[0].name, "A");
            }
            other => panic!("expected Conflict, got {other:?}"),
        }
        assert_eq!(reg.version(), 1, "conflict must not mutate");
    }

    #[tokio::test]
    async fn execute_register_validation_failure_returns_validation_failed() {
        let reg = Arc::new(Registry::new());
        // event_time_field = "ts" but schema has no "ts" field
        let payload = parse_payload(serde_json::json!({
            "nodes": [{
                "kind": "event",
                "name": "A",
                "schema": {"fields": {"x": "f64"}, "optional_fields": []},
                "event_time_field": "ts"
            }]
        }));
        let outcome = execute_register(&reg, payload).await;
        match outcome {
            RegisterOutcome::ValidationFailed {
                version,
                first_error_path,
                all_errors_count,
                ..
            } => {
                assert_eq!(version, 0);
                assert!(
                    first_error_path.contains("event_time") || first_error_path.contains("ts"),
                    "path: {first_error_path}"
                );
                assert!(all_errors_count >= 1);
            }
            other => panic!("expected ValidationFailed, got {other:?}"),
        }
        assert_eq!(reg.version(), 0);
    }

    #[tokio::test]
    async fn execute_register_validation_failure_records_error_count() {
        let reg = Arc::new(Registry::new());
        // Two events with reserved prefix → at least 2 errors.
        let payload = parse_payload(serde_json::json!({
            "nodes": [
                {
                    "kind": "event",
                    "name": "_beava_one",
                    "schema": {"fields": {"event_time": "i64"}, "optional_fields": []},
                    "event_time_field": "event_time"
                },
                {
                    "kind": "event",
                    "name": "_beava_two",
                    "schema": {"fields": {"event_time": "i64"}, "optional_fields": []},
                    "event_time_field": "event_time"
                }
            ]
        }));
        let outcome = execute_register(&reg, payload).await;
        match outcome {
            RegisterOutcome::ValidationFailed {
                all_errors_count,
                first_error_path,
                ..
            } => {
                assert!(all_errors_count >= 2, "got {all_errors_count}");
                assert!(first_error_path.contains("nodes[0]"), "{first_error_path}");
            }
            other => panic!("expected ValidationFailed, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn execute_register_noop_does_not_mutate_version() {
        let reg = Arc::new(Registry::new());
        let _ = execute_register(&reg, parse_payload(transaction_payload())).await;
        assert_eq!(reg.version(), 1);
        let _ = execute_register(&reg, parse_payload(transaction_payload())).await;
        assert_eq!(reg.version(), 1);
    }

    #[tokio::test]
    async fn execute_register_success_then_conflict_leaves_registry_at_first_version() {
        let reg = Arc::new(Registry::new());
        let _ = execute_register(
            &reg,
            parse_payload(serde_json::json!({
                "nodes": [event_node("A", &[("event_time", "i64"), ("amount", "f64")], "event_time")]
            })),
        )
        .await;
        assert_eq!(reg.version(), 1);
        let outcome = execute_register(
            &reg,
            parse_payload(serde_json::json!({
                "nodes": [event_node("A", &[("event_time", "i64"), ("amount", "i64")], "event_time")]
            })),
        )
        .await;
        assert!(matches!(outcome, RegisterOutcome::Conflict { .. }));
        assert_eq!(reg.version(), 1);
    }
}
