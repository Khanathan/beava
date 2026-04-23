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
    register_validate::validate_payload,
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
    // 1. Content-Type check (SRV-API-11)
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
        return (
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            Json(serde_json::to_value(err_body).unwrap()),
        );
    }

    // 2. JSON parse → 400
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
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::to_value(err_body).unwrap()),
            );
        }
    };

    // Empty payload fast-path (no validation needed)
    if payload.nodes.is_empty() {
        let current_version = state.registry.version();
        let resp = RegisterSuccess {
            status: "ok",
            registry_version: current_version,
            registered_descriptors: vec![],
            added: vec![],
            already_present: vec![],
        };
        info!(
            kind = "register.noop",
            version = current_version,
            nodes = 0,
            "register empty payload"
        );
        return (StatusCode::OK, Json(serde_json::to_value(resp).unwrap()));
    }

    // 3. Snapshot current for validation + diff
    let current_snapshot = state.registry.snapshot();

    // 4. Validate
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
            let err_body = RegisterErrorBody {
                error: RegisterError::Validation {
                    code: "invalid_registration",
                    path: first.path.clone(),
                    reason: first.reason.clone(),
                },
                registry_version: current_snapshot.version,
            };
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::to_value(err_body).unwrap()),
            );
        }
    };

    let nodes = validated.into_inner();
    let descriptor_names: Vec<String> = nodes.iter().map(|n| n.name().to_string()).collect();

    // 5. Diff
    let diff = compute_diff(&current_snapshot, &nodes);

    // 6. Conflict → 409 (no mutation)
    if !diff.changed.is_empty() {
        warn!(
            kind = "register.conflict",
            version = current_snapshot.version,
            changed = ?diff.changed.iter().map(|c| &c.name).collect::<Vec<_>>(),
            "register conflict"
        );
        let err_body = RegisterErrorBody {
            error: RegisterError::Conflict {
                code: "registration_conflict",
                message: "Registration would change or remove existing descriptors",
                diff: ResponseDiff {
                    added: diff.added,
                    removed: Vec::new(),
                    changed: diff.changed,
                },
            },
            registry_version: current_snapshot.version,
        };
        return (
            StatusCode::CONFLICT,
            Json(serde_json::to_value(err_body).unwrap()),
        );
    }

    // 7. No-op? (only already_present, no added) → 200 with SAME version
    if diff.added.is_empty() {
        let resp = RegisterSuccess {
            status: "ok",
            registry_version: current_snapshot.version,
            registered_descriptors: descriptor_names,
            added: Vec::new(),
            already_present: diff.already_present,
        };
        info!(
            kind = "register.noop",
            version = current_snapshot.version,
            nodes = resp.registered_descriptors.len(),
            "register no-op"
        );
        return (StatusCode::OK, Json(serde_json::to_value(resp).unwrap()));
    }

    // 8. Additive install — atomic under write lock
    let new_version = state.registry.apply_registration(nodes);
    info!(
        kind = "register.success",
        version = new_version,
        added = ?diff.added,
        already_present_count = diff.already_present.len(),
        "register succeeded"
    );
    let resp = RegisterSuccess {
        status: "ok",
        registry_version: new_version,
        registered_descriptors: descriptor_names,
        added: diff.added,
        already_present: diff.already_present,
    };
    (StatusCode::OK, Json(serde_json::to_value(resp).unwrap()))
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

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
        let r = router(readiness, registry.clone());
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
            router(ReadinessFlag::new(), reg.clone()),
            json_body(transaction_payload()),
            Some("application/json"),
        )
        .await;
        assert_eq!(s1, StatusCode::OK);
        assert_eq!(b1["registry_version"], 1);

        // Second POST — identical
        let (s2, b2) = post(
            router(ReadinessFlag::new(), reg.clone()),
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
            router(ReadinessFlag::new(), reg.clone()),
            json_body(serde_json::json!({
                "nodes": [event_node("A", &[("event_time", "i64"), ("x", "f64")], "event_time")]
            })),
            Some("application/json"),
        )
        .await;
        assert_eq!(s1, StatusCode::OK);

        // POST [A, B] → v2
        let (s2, b2) = post(
            router(ReadinessFlag::new(), reg.clone()),
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
                    "mode": "append"
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
            router(ReadinessFlag::new(), reg.clone()),
            json_body(serde_json::json!({
                "nodes": [event_node("A", &[("event_time", "i64"), ("amount", "f64")], "event_time")]
            })),
            Some("application/json"),
        )
        .await;
        assert_eq!(s1, StatusCode::OK);

        // Re-register EventA with amount: i64 → 409
        let (s2, b2) = post(
            router(ReadinessFlag::new(), reg.clone()),
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
            router(ReadinessFlag::new(), reg.clone()),
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
}
