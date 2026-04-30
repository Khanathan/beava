//! Phase 12.6 Plan 14 — Mio data-plane temporal HTTP surface tests.
//!
//! These tests pin the gap-closure work for the mio data plane:
//!   - POST /upsert/{table} returns 200 + ack_lsn (matches legacy axum)
//!   - POST /delete/{table} returns 200 + ack_lsn (matches legacy axum)
//!   - POST /retract returns 200 / 501 / 404 / 400 per D-12 / D-17
//!   - GET /table/{table}?key=... returns 200 + row (or 404 on miss)
//!   - GET /registry returns 404 when dev_endpoints not set
//!   - POST /register with non-JSON Content-Type returns 415
//!
//! The plan-author chose Option (a) per CLAUDE.md TDD §Note 4 — a NEW unit
//! test file, written RED first, paired with the GREEN impl.  The existing
//! integration tests in phase11_5_temporal_smoke / phase18_07 / phase2_smoke
//! also serve as the integration RED contract; we re-run them post-impl.

#![cfg(feature = "testing")]

use beava_server::testing::TestServer;
use serde_json::json;

fn temporal_table_node(name: &str, retention_ms: u64) -> serde_json::Value {
    json!({
        "kind": "table",
        "name": name,
        "primary_key": ["k"],
        "schema": {
            "fields": {"k": "str", "v": "i64"},
            "optional_fields": []
        },
        "mode": "upsert",
        "temporal": true,
        "retention_ms": retention_ms
    })
}

fn non_temporal_table_node(name: &str) -> serde_json::Value {
    json!({
        "kind": "table",
        "name": name,
        "primary_key": ["k"],
        "schema": {
            "fields": {"k": "str", "v": "i64"},
            "optional_fields": []
        },
        "mode": "upsert"
    })
}

// ─── Task 1: /upsert dispatch ────────────────────────────────────────────────

#[tokio::test]
async fn t1_mio_upsert_returns_200_and_ack_lsn() {
    let ts = TestServer::builder()
        .dev_endpoints(true)
        .spawn()
        .await
        .expect("spawn");

    let reg = json!({"nodes": [temporal_table_node("t1", 60_000)]});
    let r = ts.post_json("/register", &reg).await.expect("register");
    assert_eq!(r.status().as_u16(), 200);

    // POST /upsert/t1 with single-field PK row.
    let resp = ts
        .post_json("/upsert/t1", &json!({"k": "a", "v": 7}))
        .await
        .expect("upsert");
    assert_eq!(
        resp.status().as_u16(),
        200,
        "/upsert must 200 on mio: {}",
        resp.text().await.unwrap()
    );
    let ack: serde_json::Value = resp.json().await.expect("json");
    assert!(
        ack["ack_lsn"].as_u64().is_some(),
        "ack must carry ack_lsn: {ack:?}"
    );
    assert!(
        ack["registry_version"].as_u64().is_some(),
        "ack must carry registry_version: {ack:?}"
    );
    ts.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn t1_mio_upsert_unknown_table_returns_404() {
    let ts = TestServer::builder()
        .dev_endpoints(true)
        .spawn()
        .await
        .expect("spawn");
    let resp = ts
        .post_json("/upsert/no_such", &json!({"k": "a", "v": 1}))
        .await
        .expect("upsert");
    assert_eq!(resp.status().as_u16(), 404);
    let body: serde_json::Value = resp.json().await.expect("json");
    assert_eq!(body["error"], "table_not_found");
    ts.shutdown().await.expect("shutdown");
}

// ─── Task 2: /delete dispatch ────────────────────────────────────────────────

#[tokio::test]
async fn t2_mio_delete_returns_200() {
    let ts = TestServer::builder()
        .dev_endpoints(true)
        .spawn()
        .await
        .expect("spawn");

    let reg = json!({"nodes": [temporal_table_node("t2", 60_000)]});
    ts.post_json("/register", &reg).await.expect("register");
    ts.post_json("/upsert/t2", &json!({"k": "a", "v": 9}))
        .await
        .expect("upsert");

    let resp = ts
        .post_json("/delete/t2", &json!({"key": {"k": "a"}}))
        .await
        .expect("delete");
    assert_eq!(
        resp.status().as_u16(),
        200,
        "/delete must 200 on mio: {}",
        resp.text().await.unwrap()
    );
    let ack: serde_json::Value = resp.json().await.expect("json");
    assert!(ack["ack_lsn"].as_u64().is_some());
    ts.shutdown().await.expect("shutdown");
}

// ─── Task 3: /retract dispatch ───────────────────────────────────────────────

#[tokio::test]
async fn t3_mio_retract_unknown_event_id_returns_404() {
    let ts = TestServer::builder()
        .dev_endpoints(true)
        .spawn()
        .await
        .expect("spawn");
    let resp = ts
        .post_json("/retract", &json!({"event_id": 999_999}))
        .await
        .expect("retract");
    assert_eq!(resp.status().as_u16(), 404);
    let body: serde_json::Value = resp.json().await.expect("json");
    assert_eq!(body["error"], "event_id_not_found");
    ts.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn t3_mio_retract_stream_event_returns_501() {
    let ts = TestServer::builder()
        .dev_endpoints(true)
        .spawn()
        .await
        .expect("spawn");

    let reg = json!({
        "nodes": [{
            "kind": "event",
            "name": "click",
            "schema": {"fields": {"u": "str"}, "optional_fields": []}
        }]
    });
    ts.post_json("/register", &reg).await.expect("register");
    let r = ts
        .post_json("/push/click", &json!({"u": "u1"}))
        .await
        .expect("push");
    let ack: serde_json::Value = r.json().await.expect("ack");
    let lsn = ack["ack_lsn"].as_u64().expect("ack_lsn");

    let resp = ts
        .post_json("/retract", &json!({"event_id": lsn}))
        .await
        .expect("retract");
    assert_eq!(resp.status().as_u16(), 501, "stream retract → 501 per D-12");
    let body: serde_json::Value = resp.json().await.expect("json");
    assert_eq!(body["error"], "stream_retraction_unimplemented");
    assert!(body["see"].as_str().is_some());
    ts.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn t3_mio_retract_non_temporal_table_returns_400() {
    let ts = TestServer::builder()
        .dev_endpoints(true)
        .spawn()
        .await
        .expect("spawn");
    let reg = json!({"nodes": [non_temporal_table_node("u")]});
    ts.post_json("/register", &reg).await.expect("register");
    let r = ts
        .post_json("/upsert/u", &json!({"k": "a", "v": 1}))
        .await
        .expect("upsert");
    let ack: serde_json::Value = r.json().await.unwrap();
    let lsn = ack["ack_lsn"].as_u64().unwrap();

    let rr = ts
        .post_json("/retract", &json!({"event_id": lsn}))
        .await
        .expect("retract");
    assert_eq!(rr.status().as_u16(), 400);
    let body: serde_json::Value = rr.json().await.expect("json");
    assert_eq!(body["error"], "table_not_temporal");
    ts.shutdown().await.expect("shutdown");
}

// ─── Task 3b: /table/{name} GET dispatch (gap-closure ride-along) ─────────────

#[tokio::test]
async fn t3b_mio_table_get_after_upsert_returns_200_with_row() {
    let ts = TestServer::builder()
        .dev_endpoints(true)
        .spawn()
        .await
        .expect("spawn");
    let reg = json!({"nodes": [temporal_table_node("tg", 60_000)]});
    ts.post_json("/register", &reg).await.expect("register");
    ts.post_json("/upsert/tg", &json!({"k": "alice", "v": 42}))
        .await
        .expect("upsert");
    let resp = ts.get_raw("/table/tg?key=alice").await;
    assert_eq!(
        resp.status().as_u16(),
        200,
        "table get must 200 after upsert"
    );
    let body: serde_json::Value = resp.json().await.expect("json");
    assert_eq!(body["row"]["v"], 42);
    ts.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn t3b_mio_table_get_as_of_on_non_temporal_returns_400() {
    let ts = TestServer::builder()
        .dev_endpoints(true)
        .spawn()
        .await
        .expect("spawn");
    let reg = json!({"nodes": [non_temporal_table_node("u")]});
    ts.post_json("/register", &reg).await.expect("register");
    let resp = ts.get_raw("/table/u?key=x&as_of=10").await;
    assert_eq!(resp.status().as_u16(), 400);
    let body: serde_json::Value = resp.json().await.expect("json");
    assert_eq!(body["error"], "as_of_requires_temporal");
    ts.shutdown().await.expect("shutdown");
}

// ─── Task 4: dev_endpoints gating on /registry ───────────────────────────────

#[tokio::test]
async fn t4_mio_registry_returns_404_when_dev_endpoints_disabled() {
    // Default builder: dev_endpoints = false (matches legacy axum default)
    let ts = TestServer::spawn().await.expect("spawn");
    let resp = ts.get_raw("/registry").await;
    assert_eq!(
        resp.status().as_u16(),
        404,
        "/registry must 404 when dev_endpoints disabled (matches BEAVA_DEV_ENDPOINTS!=1)"
    );
    ts.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn t4_mio_registry_returns_200_when_dev_endpoints_enabled() {
    let ts = TestServer::builder()
        .dev_endpoints(true)
        .spawn()
        .await
        .expect("spawn");
    let resp = ts.get_raw("/registry").await;
    assert_eq!(
        resp.status().as_u16(),
        200,
        "/registry must 200 when dev_endpoints enabled"
    );
    let body: serde_json::Value = resp.json().await.expect("json");
    assert_eq!(body["_dev_only"], true);
    ts.shutdown().await.expect("shutdown");
}

// ─── Task 5: Content-Type 415 rejection on POST /register ────────────────────

#[tokio::test]
async fn t5_mio_register_text_plain_returns_415() {
    let ts = TestServer::builder()
        .dev_endpoints(true)
        .spawn()
        .await
        .expect("spawn");

    let resp = reqwest::Client::new()
        .post(format!("{}/register", ts.base_url()))
        .header("Content-Type", "text/plain")
        .body(r#"{"nodes":[]}"#)
        .timeout(std::time::Duration::from_secs(5))
        .send()
        .await
        .expect("send");
    assert_eq!(
        resp.status().as_u16(),
        415,
        "wrong Content-Type → 415 (matches legacy axum register handler)"
    );
    let body: serde_json::Value = resp.json().await.expect("json");
    assert_eq!(body["error"]["code"], "unsupported_media_type");
    ts.shutdown().await.expect("shutdown");
}
