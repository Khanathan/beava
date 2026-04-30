//! Phase 12.6 Plan 06 — Event-time hard rip (D-03).
//!
//! End-to-end integration tests that pin the strict-deny boundary on **both**
//! wire surfaces:
//!
//! 1. **Push payload (POST /push/{event})** — body cannot carry `event_time_ms`
//!    or any other field absent from the registered EventDescriptor schema.
//!    Per CONTEXT D-03 verbatim — "Hard rip everywhere — zero `event_time_ms`
//!    compat at any layer. … No deprecation window, no parse-and-strip, no
//!    warn-then-error." Silent-strip is parse-and-strip; explicitly forbidden.
//!
//! 2. **Register payload (POST /register)** — payload struct cannot carry
//!    legacy `event_time_field` / `tolerate_delay_ms` JSON keys on event
//!    nodes.  Same D-03 verbatim posture.
//!
//! Both surfaces emit structured 400 error envelopes with `error.code` strings
//! consumed by the Python SDK + Rust client to surface a clear "this is the
//! no-event-time pivot" message — rather than an opaque serde "unknown field"
//! string or a 200 with silent-strip.
//!
//! Architectural commitment per `project_redis_shaped_no_event_time_ever`
//! (locked 2026-04-30): event_time / watermarks / joins / PIT removed from v0
//! permanently. Reviving any of these requires explicit user override + a new
//! ADR.

#![cfg(feature = "testing")]

use beava_server::testing::TestServer;
use serde_json::json;

/// Helper — register a clean `Tx` event with `user_id: str` + `amount: f64`
/// schema. No `event_time_field`. Used as the baseline for the push-side
/// strict-deny tests (1 + 2).
async fn register_clean_tx(ts: &TestServer) {
    let payload = json!({
        "nodes": [
            {
                "kind": "event",
                "name": "Tx",
                "schema": {
                    "fields": {"user_id": "str", "amount": "f64"},
                    "optional_fields": []
                }
            }
        ]
    });
    let resp = ts.post_json("/register", &payload).await.expect("register");
    assert_eq!(
        resp.status().as_u16(),
        200,
        "clean register must succeed: {}",
        resp.text().await.unwrap_or_default()
    );
}

/// Test 1 — POST /push/Tx with a body carrying `event_time_ms` returns
/// HTTP 400 with `error.code == "unknown_field_event_time_v0"`.
///
/// **D-03 verbatim:** "no parse-and-strip, no warn-then-error" — server MUST
/// reject, not silently drop the field.
#[tokio::test]
async fn push_payload_with_event_time_ms_returns_400_unknown_field() {
    let ts = TestServer::builder()
        .dev_endpoints(true)
        .spawn()
        .await
        .expect("spawn TestServer");
    register_clean_tx(&ts).await;

    let body = json!({
        "user_id": "u1",
        "amount": 42.0,
        "event_time_ms": 1234567890_i64
    });
    let resp = ts.post_json("/push/Tx", &body).await.expect("push");
    let status = resp.status().as_u16();
    let text = resp.text().await.unwrap_or_default();
    assert_eq!(
        status, 400,
        "push with event_time_ms must return 400 (got {status}): body={text}"
    );
    let parsed: serde_json::Value =
        serde_json::from_str(&text).unwrap_or_else(|_| serde_json::Value::Null);
    let code = parsed
        .get("error")
        .and_then(|e| e.get("code"))
        .and_then(|c| c.as_str())
        .unwrap_or("");
    assert_eq!(
        code, "unknown_field_event_time_v0",
        "push with event_time_ms must surface code unknown_field_event_time_v0; \
         got: {text}"
    );
}

/// Test 2 — POST /push/Tx with a clean body (no `event_time_ms`) returns 200.
///
/// Sanity: the strict-deny only triggers on the legacy field; clean pushes
/// continue to land.
#[tokio::test]
async fn push_payload_clean_succeeds() {
    let ts = TestServer::builder()
        .dev_endpoints(true)
        .spawn()
        .await
        .expect("spawn TestServer");
    register_clean_tx(&ts).await;

    let body = json!({"user_id": "u1", "amount": 42.0});
    let resp = ts.post_json("/push/Tx", &body).await.expect("push");
    let status = resp.status().as_u16();
    let text = resp.text().await.unwrap_or_default();
    assert_eq!(
        status, 200,
        "clean push must return 200 (got {status}): body={text}"
    );
}

/// Test 3 — POST /register payload that includes `"event_time_field": "ts"`
/// on an event node returns HTTP 400 with
/// `error.code == "unknown_field_event_time_v0"`.
///
/// **D-03 strict-deny on register:** legacy decorator keys MUST raise a
/// structured error, NOT silently strip into `EventDescriptor`-with-null-fields.
#[tokio::test]
async fn register_payload_with_legacy_event_time_field_returns_400_unknown_field() {
    let ts = TestServer::builder()
        .dev_endpoints(true)
        .spawn()
        .await
        .expect("spawn TestServer");

    let payload = json!({
        "nodes": [
            {
                "kind": "event",
                "name": "WithEventTime",
                "schema": {
                    "fields": {"user_id": "str", "ts": "i64"},
                    "optional_fields": []
                },
                "event_time_field": "ts"
            }
        ]
    });
    let resp = ts.post_json("/register", &payload).await.expect("register");
    let status = resp.status().as_u16();
    let text = resp.text().await.unwrap_or_default();
    assert_eq!(
        status, 400,
        "register with event_time_field must return 400 (got {status}): body={text}"
    );
    let parsed: serde_json::Value =
        serde_json::from_str(&text).unwrap_or_else(|_| serde_json::Value::Null);
    let code = parsed
        .get("error")
        .and_then(|e| e.get("code"))
        .and_then(|c| c.as_str())
        .unwrap_or("");
    assert_eq!(
        code, "unknown_field_event_time_v0",
        "register with event_time_field must surface code unknown_field_event_time_v0; \
         got: {text}"
    );
}

/// Test 4 — POST /register payload that includes `"tolerate_delay_ms": 1000`
/// on an event node returns HTTP 400 with
/// `error.code == "unknown_field_tolerate_delay_v0"`.
#[tokio::test]
async fn register_payload_with_legacy_tolerate_delay_ms_returns_400_unknown_field() {
    let ts = TestServer::builder()
        .dev_endpoints(true)
        .spawn()
        .await
        .expect("spawn TestServer");

    let payload = json!({
        "nodes": [
            {
                "kind": "event",
                "name": "WithTolerateDelay",
                "schema": {
                    "fields": {"user_id": "str"},
                    "optional_fields": []
                },
                "tolerate_delay_ms": 1000
            }
        ]
    });
    let resp = ts.post_json("/register", &payload).await.expect("register");
    let status = resp.status().as_u16();
    let text = resp.text().await.unwrap_or_default();
    assert_eq!(
        status, 400,
        "register with tolerate_delay_ms must return 400 (got {status}): body={text}"
    );
    let parsed: serde_json::Value =
        serde_json::from_str(&text).unwrap_or_else(|_| serde_json::Value::Null);
    let code = parsed
        .get("error")
        .and_then(|e| e.get("code"))
        .and_then(|c| c.as_str())
        .unwrap_or("");
    assert_eq!(
        code, "unknown_field_tolerate_delay_v0",
        "register with tolerate_delay_ms must surface code unknown_field_tolerate_delay_v0; \
         got: {text}"
    );
}
