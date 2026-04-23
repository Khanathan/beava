//! Phase 2 acceptance gate.
//!
//! Exercises all 5 ROADMAP success criteria via real HTTP against a live TestServer:
//!  1. Valid JSON DAG → 200 with registry_version:1 + registered_descriptors
//!  2. Identical re-post = no-op, version unchanged
//!  3. Additive DAG (new nodes) → 200 with version bump
//!  4. Conflicting DAG → 409 with structured diff
//!  5. Malformed payload → 400 with {error: {code, path, reason}}
//!
//! Plus GET /registry dev endpoint tests.
//!
//! Required-features: testing

use beava_server::testing::TestServer;
use serde_json::{json, Value};

// ─── Helpers ──────────────────────────────────────────────────────────────────

fn valid_event_payload(name: &str, extra_field: &str, field_type: &str) -> Value {
    json!({
        "nodes": [{
            "kind": "event",
            "name": name,
            "schema": {
                "fields": {"event_time": "i64", extra_field: field_type},
                "optional_fields": []
            },
            "event_time_field": "event_time"
        }]
    })
}

fn transaction_payload() -> Value {
    valid_event_payload("Transaction", "amount", "f64")
}

async fn post_register(ts: &TestServer, body: &Value) -> reqwest::Response {
    ts.post_json("/register", body).await.expect("post_json")
}

async fn post_register_json(ts: &TestServer, body: &Value) -> Value {
    post_register(ts, body)
        .await
        .json()
        .await
        .expect("json body")
}

// ─── Success criterion 1 ──────────────────────────────────────────────────────

#[tokio::test]
async fn success_criterion_1_valid_register_returns_200_v1() {
    let ts = TestServer::spawn().await.expect("spawn");
    let body = transaction_payload();
    let resp = post_register(&ts, &body).await;
    assert_eq!(resp.status().as_u16(), 200);
    let val: Value = resp.json().await.expect("json");
    assert_eq!(val["status"], "ok");
    assert_eq!(val["registry_version"], 1);
    assert_eq!(val["registered_descriptors"], json!(["Transaction"]));
    assert_eq!(val["added"], json!(["Transaction"]));
    assert_eq!(val["already_present"], json!([]));
    ts.shutdown().await.expect("shutdown");
}

// ─── Success criterion 2 ──────────────────────────────────────────────────────

#[tokio::test]
async fn success_criterion_2_identical_repost_is_noop() {
    let ts = TestServer::spawn().await.expect("spawn");
    let body = transaction_payload();

    // First post → v1
    let v1 = post_register_json(&ts, &body).await;
    assert_eq!(v1["registry_version"], 1);

    // Second post (identical) → still v1, no-op
    let v2 = post_register_json(&ts, &body).await;
    assert_eq!(v2["status"], "ok");
    assert_eq!(v2["registry_version"], 1, "version must not bump on no-op");
    assert_eq!(v2["added"], json!([]));
    assert_eq!(v2["already_present"], json!(["Transaction"]));

    ts.shutdown().await.expect("shutdown");
}

// ─── Success criterion 3 ──────────────────────────────────────────────────────

#[tokio::test]
async fn success_criterion_3_additive_bumps_version() {
    let ts = TestServer::spawn().await.expect("spawn");

    // POST Transaction → v1
    let r1 = post_register_json(&ts, &transaction_payload()).await;
    assert_eq!(r1["registry_version"], 1);

    // POST [Transaction, Merchant (table)] → v2
    let additive = json!({
        "nodes": [
            {
                "kind": "event",
                "name": "Transaction",
                "schema": {"fields": {"event_time": "i64", "amount": "f64"}, "optional_fields": []},
                "event_time_field": "event_time"
            },
            {
                "kind": "table",
                "name": "Merchant",
                "primary_key": ["merchant_id"],
                "schema": {"fields": {"merchant_id": "str", "name": "str"}, "optional_fields": []},
                "mode": "append"
            }
        ]
    });
    let r2 = post_register_json(&ts, &additive).await;
    assert_eq!(r2["status"], "ok");
    assert_eq!(r2["registry_version"], 2);
    assert_eq!(r2["added"], json!(["Merchant"]));
    assert_eq!(r2["already_present"], json!(["Transaction"]));

    ts.shutdown().await.expect("shutdown");
}

// ─── Success criterion 4 ──────────────────────────────────────────────────────

#[tokio::test]
async fn success_criterion_4_conflict_returns_409_with_diff() {
    let ts = TestServer::spawn().await.expect("spawn");

    // Register Transaction with amount: f64 → v1
    let r1 = post_register_json(&ts, &transaction_payload()).await;
    assert_eq!(r1["registry_version"], 1);

    // Re-register Transaction with amount: i64 → 409
    let conflict_body = valid_event_payload("Transaction", "amount", "i64");
    let resp = post_register(&ts, &conflict_body).await;
    assert_eq!(resp.status().as_u16(), 409);
    let val: Value = resp.json().await.expect("json");
    assert_eq!(val["error"]["code"], "registration_conflict");
    assert_eq!(val["error"]["diff"]["added"], json!([]));
    assert_eq!(val["error"]["diff"]["removed"], json!([]));
    let changed = &val["error"]["diff"]["changed"];
    assert!(changed.is_array() && !changed.as_array().unwrap().is_empty());
    assert_eq!(changed[0]["name"], "Transaction");
    assert_eq!(changed[0]["reason"], "schema_mismatch");
    let details = changed[0]["details"].as_str().unwrap_or("");
    assert!(
        details.contains("amount"),
        "details should mention 'amount': {details}"
    );
    assert_eq!(
        val["registry_version"], 1,
        "registry_version must not bump on 409"
    );

    // Confirm registry was NOT mutated: original Transaction still resolves as no-op
    let recheck = post_register_json(&ts, &transaction_payload()).await;
    assert_eq!(recheck["registry_version"], 1, "original still valid at v1");
    assert_eq!(recheck["added"], json!([]));

    ts.shutdown().await.expect("shutdown");
}

// ─── Success criterion 5 ──────────────────────────────────────────────────────

#[tokio::test]
async fn success_criterion_5_malformed_returns_400_with_path() {
    let ts = TestServer::spawn().await.expect("spawn");

    // (a) Missing event_time_field value in schema
    let bad_event = json!({
        "nodes": [{
            "kind": "event",
            "name": "T",
            "schema": {"fields": {"x": "f64"}, "optional_fields": []},
            "event_time_field": "event_time"
        }]
    });
    let resp = post_register(&ts, &bad_event).await;
    assert_eq!(resp.status().as_u16(), 400);
    let val: Value = resp.json().await.expect("json");
    assert_eq!(val["error"]["code"], "invalid_registration");
    let path = val["error"]["path"].as_str().unwrap_or("");
    assert!(
        path.contains("event_time") || path.contains("nodes[0]"),
        "path should reference event_time field: {path}"
    );
    let reason = val["error"]["reason"].as_str().unwrap_or("");
    assert!(!reason.is_empty(), "reason must be non-empty");

    // (b) Malformed JSON (unclosed brace) — send raw bytes
    let malformed_resp = ts
        .post_json("/register", &json!(null)) // will be overridden below
        .await;
    // Actually send the raw malformed body using reqwest directly
    let raw_resp = reqwest::Client::new()
        .post(&format!("{}/register", ts.base_url()))
        .header("Content-Type", "application/json")
        .body(r#"{"nodes": ["#)
        .timeout(std::time::Duration::from_secs(5))
        .send()
        .await
        .expect("send");
    assert_eq!(raw_resp.status().as_u16(), 400);
    let raw_val: Value = raw_resp.json().await.expect("json");
    assert_eq!(raw_val["error"]["code"], "invalid_registration");
    assert_eq!(raw_val["error"]["path"], "<body>");
    let _ = malformed_resp; // consumed above — suppress warning

    // (c) Wrong Content-Type → 415
    let ct_resp = reqwest::Client::new()
        .post(&format!("{}/register", ts.base_url()))
        .header("Content-Type", "text/plain")
        .body(serde_json::to_vec(&transaction_payload()).unwrap())
        .timeout(std::time::Duration::from_secs(5))
        .send()
        .await
        .expect("send");
    assert_eq!(ct_resp.status().as_u16(), 415);
    let ct_val: Value = ct_resp.json().await.expect("json");
    assert_eq!(ct_val["error"]["code"], "unsupported_media_type");

    // (d) Unknown op kind → 400
    let unknown_op = json!({
        "nodes": [{
            "kind": "event",
            "name": "Src",
            "schema": {"fields": {"event_time": "i64", "x": "f64"}, "optional_fields": []},
            "event_time_field": "event_time"
        }, {
            "kind": "derivation",
            "name": "D",
            "output_kind": "event",
            "upstreams": ["Src"],
            "ops": [{"op": "delete", "fields": []}],
            "schema": {"fields": {"x": "f64"}, "optional_fields": []}
        }]
    });
    let op_resp = post_register(&ts, &unknown_op).await;
    assert_eq!(op_resp.status().as_u16(), 400);
    let op_val: Value = op_resp.json().await.expect("json");
    assert_eq!(op_val["error"]["code"], "invalid_registration");
    let op_reason = op_val["error"]["reason"].as_str().unwrap_or("");
    assert!(
        op_reason.contains("unknown variant") || op_reason.contains("delete"),
        "reason should mention unknown op 'delete': {op_reason}"
    );

    ts.shutdown().await.expect("shutdown");
}

// ─── Dev endpoint: enabled ────────────────────────────────────────────────────

#[tokio::test]
async fn get_registry_dev_endpoint_works_when_enabled() {
    let ts = TestServer::builder()
        .dev_endpoints(true)
        .spawn()
        .await
        .expect("spawn");

    // Register one event
    let r = post_register_json(&ts, &transaction_payload()).await;
    assert_eq!(r["registry_version"], 1);

    // GET /registry → 200 with full dump
    let registry = ts.get_json("/registry").await;
    assert_eq!(registry["version"], 1);
    assert!(
        registry["events"]["Transaction"].is_object(),
        "expected events[Transaction] to be an object"
    );
    assert_eq!(registry["_dev_only"], true);

    ts.shutdown().await.expect("shutdown");
}

// ─── Dev endpoint: disabled ───────────────────────────────────────────────────

#[tokio::test]
async fn get_registry_dev_endpoint_returns_404_when_disabled() {
    let ts = TestServer::spawn().await.expect("spawn"); // dev_endpoints defaults to false
    let resp = ts.get_raw("/registry").await;
    assert_eq!(
        resp.status().as_u16(),
        404,
        "GET /registry must 404 when BEAVA_DEV_ENDPOINTS is not set"
    );
    ts.shutdown().await.expect("shutdown");
}
