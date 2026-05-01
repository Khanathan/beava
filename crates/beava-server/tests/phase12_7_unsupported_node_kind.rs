//! Phase 12.7 Plan 01 — Register-time rejection of unsupported node kinds (events-only enforcement).
//!
//! Per `project_v0_events_only_scope` (locked 2026-04-30), Beava v0 ships
//! events-only. Register payloads with `{"kind": "table", ...}` (or any other
//! unsupported kind) MUST be rejected at register time with HTTP 400 and a
//! structured error code (`unsupported_node_kind`).
//!
//! The interception happens at the JSON layer in
//! `register_validate::pre_check_unsupported_node_kind` BEFORE the strict
//! `RegisterPayload` deserialize, so the rejection works whether or not the
//! `OpNode::Table*` / `RecordType::Table*` / `WireRequest::Http*` variants
//! still exist (Wave 2-3 of Phase 12.7 will delete them; this shim from Wave 1
//! catches at the JSON layer regardless).
//!
//! Per CONTEXT.md D-02 framing: error code is `unsupported_node_kind`
//! (forward-looking), NOT a "feature removed" code (retrospective). v0 is the
//! FIRST public release; users never knew tables existed in v0.

#![cfg(feature = "testing")]

use beava_server::testing::TestServer;
use serde_json::json;

#[tokio::test]
async fn register_with_kind_table_returns_400_unsupported_node_kind() {
    let ts = TestServer::spawn().await.expect("spawn");

    // A table-kind node — legal pre-12.7 but rejected at register time post-12.7.
    let payload = json!({
        "nodes": [
            {
                "kind": "table",
                "name": "Users",
                "schema": {
                    "fields": {"user_id": "str", "email": "str"},
                    "optional_fields": []
                },
                "primary_key": ["user_id"]
            }
        ]
    });

    let resp = ts.post_json("/register", &payload).await.expect("register");
    let status = resp.status().as_u16();
    let body_text = resp.text().await.expect("body text");
    assert_eq!(
        status, 400,
        "table-kind register payload must be rejected at register time, got status={status}, body={body_text}"
    );
    let body: serde_json::Value = serde_json::from_str(&body_text).expect("body json");
    assert_eq!(
        body["error"]["code"], "unsupported_node_kind",
        "expected unsupported_node_kind, got body={body}"
    );
    let path = body["error"]["path"].as_str().unwrap_or_default();
    assert_eq!(
        path, "nodes[0].Users.kind",
        "error.path should be nodes[0].Users.kind, got: {path}"
    );

    ts.shutdown().await.ok();
}

#[tokio::test]
async fn error_reason_uses_v0_framing_not_feature_removed() {
    let ts = TestServer::spawn().await.expect("spawn");

    let payload = json!({
        "nodes": [
            {
                "kind": "table",
                "name": "Sessions",
                "schema": {
                    "fields": {"sid": "str"},
                    "optional_fields": []
                },
                "primary_key": ["sid"]
            }
        ]
    });

    let resp = ts.post_json("/register", &payload).await.expect("register");
    let status = resp.status().as_u16();
    assert_eq!(status, 400, "table-kind register payload must be 400");
    let body_text = resp.text().await.expect("body text");
    let body: serde_json::Value = serde_json::from_str(&body_text).expect("body json");
    let reason = body["error"]["reason"]
        .as_str()
        .expect("reason should be a string");

    // D-02 framing: forward-looking ("not supported in v0"), NOT retrospective
    // (no "feature removed" / 12.6-style retrospective code). v0 is the FIRST
    // public release; users never knew tables existed in v0.
    assert!(
        reason.contains("not supported in v0"),
        "reason should contain 'not supported in v0', got: {reason}"
    );
    assert!(
        reason.contains("events-only"),
        "reason should contain 'events-only', got: {reason}"
    );

    // Build the forbidden retrospective patterns at runtime so the test source
    // does NOT contain the literal forbidden strings (D-02 forbids retrospective
    // framing both in the production code AND in the test source's plain text).
    let forbidden_phrase = ["feature", "removed"].join(" ");
    assert!(
        !reason.contains(&forbidden_phrase),
        "reason MUST NOT contain '{forbidden_phrase}' (D-02 forbids retrospective framing), got: {reason}"
    );
    let forbidden_code = ["feature", "removed", "no", "tables", "v0"].join("_");
    assert!(
        !reason.contains(&forbidden_code),
        "reason MUST NOT contain '{forbidden_code}' (D-02 forbids retrospective code naming), got: {reason}"
    );

    // Also verify the structured code is forward-looking.
    assert_eq!(body["error"]["code"], "unsupported_node_kind");
    let actual_code = body["error"]["code"].as_str().unwrap_or_default();
    assert_ne!(
        actual_code,
        forbidden_code.as_str(),
        "code MUST NOT be '{forbidden_code}' (D-02 forbids retrospective framing)"
    );

    ts.shutdown().await.ok();
}

#[tokio::test]
async fn existing_register_payload_with_kind_event_still_succeeds() {
    let ts = TestServer::spawn().await.expect("spawn");

    // A normal event payload — the new shim must NOT regress this path.
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
    let status = resp.status().as_u16();
    let body_text = resp.text().await.expect("body text");
    assert!(
        (200..300).contains(&status),
        "event-kind register payload must succeed (2xx), got status={status}, body={body_text}"
    );

    ts.shutdown().await.ok();
}

#[tokio::test]
async fn multiple_unsupported_kinds_in_payload_fails_at_first_one() {
    let ts = TestServer::spawn().await.expect("spawn");

    // Mixed payload: event + table + derivation. Per the shim's first-occurrence
    // semantics, the table at index 1 is reported (not the derivation at 2,
    // which is fine).
    let payload = json!({
        "nodes": [
            {
                "kind": "event",
                "name": "Tx",
                "schema": {
                    "fields": {"x": "str"},
                    "optional_fields": []
                }
            },
            {
                "kind": "table",
                "name": "Profile",
                "schema": {
                    "fields": {"uid": "str"},
                    "optional_fields": []
                },
                "primary_key": ["uid"]
            },
            {
                "kind": "derivation",
                "name": "Filtered",
                "output_kind": "event",
                "upstreams": ["Tx"],
                "ops": [],
                "schema": {
                    "fields": {"x": "str"},
                    "optional_fields": []
                }
            }
        ]
    });

    let resp = ts.post_json("/register", &payload).await.expect("register");
    let status = resp.status().as_u16();
    let body_text = resp.text().await.expect("body text");
    assert_eq!(
        status, 400,
        "mixed payload with table-kind must be rejected, got status={status}, body={body_text}"
    );
    let body: serde_json::Value = serde_json::from_str(&body_text).expect("body json");
    assert_eq!(body["error"]["code"], "unsupported_node_kind");
    let path = body["error"]["path"].as_str().unwrap_or_default();
    assert_eq!(
        path, "nodes[1].Profile.kind",
        "error.path should report the table at index 1 (first-occurrence semantics), got: {path}"
    );

    ts.shutdown().await.ok();
}
