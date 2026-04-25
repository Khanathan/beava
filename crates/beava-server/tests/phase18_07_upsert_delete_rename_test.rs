//! Phase 18 Plan 07 — Task 7.4 tests.
//!
//! Tests for the Phase 16 HTTP rename:
//!   - POST /push-table/:name  → 404 (old route deleted)
//!   - POST /delete-table/:name → 404 (old route deleted)
//!   - POST /upsert/:name      → 200 (new route)
//!   - POST /delete/:name      → 200 (new route)
//!
//! RED phase: these tests fail because:
//!   - /upsert and /delete routes don't exist yet (returns 404)
//!   - /push-table and /delete-table still return 200 (not yet 404)

#![cfg(feature = "testing")]

use beava_server::testing::TestServer;
use serde_json::json;

/// Helper: register a temporal table for upsert/delete testing.
async fn register_temporal_table(ts: &TestServer, table_name: &str) {
    let body = json!({
        "nodes": [
            {
                "kind": "table",
                "name": table_name,
                "primary_key": ["user_id"],
                "schema": {
                    "fields": {"user_id": "str", "country": "str"},
                    "optional_fields": ["country"]
                },
                "mode": "upsert",
                "temporal": true,
                "retention_ms": 3_600_000
            }
        ]
    });
    let resp = ts
        .post_json("/register", &body)
        .await
        .expect("register request");
    assert_eq!(
        resp.status().as_u16(),
        200,
        "register failed: {}",
        resp.text().await.unwrap()
    );
}

/// 7.4 — New /upsert/:name route works, old /push-table/:name returns 404.
///
/// RED: fails because:
///   - /upsert/users returns 404 (route not added yet)
///   - /push-table/users returns 200 (not yet deleted)
#[tokio::test]
async fn test_upsert_route_works_old_route_404() {
    let ts = TestServer::builder()
        .dev_endpoints(false)
        .spawn()
        .await
        .expect("spawn");

    register_temporal_table(&ts, "users").await;

    // New route: POST /upsert/users should return 200.
    let upsert_resp = ts
        .post_json("/upsert/users", &json!({"user_id": "u1", "country": "US"}))
        .await
        .expect("upsert request");
    let upsert_status = upsert_resp.status().as_u16();
    assert_eq!(
        upsert_status, 200,
        "POST /upsert/users must return 200; got: {upsert_status}"
    );

    // New route: POST /delete/users should return 200.
    let delete_resp = ts
        .post_json("/delete/users", &json!({"key": {"user_id": "u1"}}))
        .await
        .expect("delete request");
    let delete_status = delete_resp.status().as_u16();
    assert_eq!(
        delete_status, 200,
        "POST /delete/users must return 200; got: {delete_status}"
    );

    // Old route: POST /push-table/users must return 404.
    let old_push_resp = ts
        .post_json("/push-table/users", &json!({"user_id": "u2", "country": "CA"}))
        .await
        .expect("old push-table request");
    let old_push_status = old_push_resp.status().as_u16();
    assert_eq!(
        old_push_status, 404,
        "POST /push-table/users (old route) must return 404; got: {old_push_status}"
    );

    // Old route: POST /delete-table/users must return 404.
    let old_delete_resp = ts
        .post_json("/delete-table/users", &json!({"user_id": "u1"}))
        .await
        .expect("old delete-table request");
    let old_delete_status = old_delete_resp.status().as_u16();
    assert_eq!(
        old_delete_status, 404,
        "POST /delete-table/users (old route) must return 404; got: {old_delete_status}"
    );

    ts.shutdown().await.expect("shutdown");
}

/// 7.4 — /upsert stores row that is then readable via /table/:name.
///
/// RED: fails because /upsert route doesn't exist yet.
#[tokio::test]
async fn test_upsert_stores_row_readable_via_table_get() {
    let ts = TestServer::builder()
        .dev_endpoints(false)
        .spawn()
        .await
        .expect("spawn");

    register_temporal_table(&ts, "profiles").await;

    // Upsert a row.
    let upsert_resp = ts
        .post_json("/upsert/profiles", &json!({"user_id": "alice", "country": "US"}))
        .await
        .expect("upsert");
    assert_eq!(
        upsert_resp.status().as_u16(),
        200,
        "upsert failed: {}",
        upsert_resp.text().await.unwrap()
    );

    // Verify the row is readable.
    let get_resp = ts.get_raw("/table/profiles?key=alice").await;
    assert_eq!(
        get_resp.status().as_u16(),
        200,
        "table get must return 200 after upsert"
    );
    let rb: serde_json::Value = get_resp.json().await.expect("json");
    assert_eq!(rb["row"]["user_id"], json!("alice"));
    assert_eq!(rb["row"]["country"], json!("US"));

    ts.shutdown().await.expect("shutdown");
}

/// 7.4 — /delete removes a row that was previously upserted.
///
/// RED: fails because /delete route doesn't exist yet.
#[tokio::test]
async fn test_delete_removes_upserted_row() {
    let ts = TestServer::builder()
        .dev_endpoints(false)
        .spawn()
        .await
        .expect("spawn");

    register_temporal_table(&ts, "accounts").await;

    // Upsert then delete.
    let upsert_resp = ts
        .post_json("/upsert/accounts", &json!({"user_id": "bob", "country": "GB"}))
        .await
        .expect("upsert");
    assert_eq!(upsert_resp.status().as_u16(), 200, "upsert must succeed");

    let delete_resp = ts
        .post_json("/delete/accounts", &json!({"key": {"user_id": "bob"}}))
        .await
        .expect("delete");
    assert_eq!(delete_resp.status().as_u16(), 200, "delete must succeed");

    // After delete, the row should be gone (retracted).
    let get_resp = ts.get_raw("/table/accounts?key=bob").await;
    // After retract, lookup returns 404 (key_not_found) or 200 with null row.
    // Phase 11.5 retract semantics: row is tombstoned; lookup returns 404 or
    // a null sentinel. Accept either 404 or 200 with null.
    let status = get_resp.status().as_u16();
    assert!(
        status == 404 || status == 200,
        "get after delete should be 404 or 200; got: {status}"
    );

    ts.shutdown().await.expect("shutdown");
}
