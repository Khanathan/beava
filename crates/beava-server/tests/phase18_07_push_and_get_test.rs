//! Phase 18 Plan 07 — Task 7.3 tests.
//!
//! Tests for the new /push-and-get/:event and /push-sync-and-get/:event routes.
//! These routes atomically push an event and query features in a single request
//! (read-your-writes by construction).
//!
//! RED phase: these tests fail because the routes don't exist yet.
//! GREEN phase (Task 7.3.b): routes added to runtime-core router + glue dispatch.

#![cfg(feature = "testing")]

use beava_server::testing::TestServer;
use serde_json::json;

/// Helper: register a simple event + count aggregation.
async fn register_count_pipeline(ts: &TestServer, event_name: &str, feature_name: &str) {
    let body = json!({
        "nodes": [
            {
                "kind": "event",
                "name": event_name,
                "schema": {
                    "fields": {
                        "entity": "str",
                        "amount": "f64",
                        "event_time": "i64"
                    },
                    "optional_fields": ["amount"]
                },
                "event_time_field": "event_time"
            },
            {
                "kind": "derivation",
                "name": format!("{event_name}Agg"),
                "output_kind": "table",
                "upstreams": [event_name],
                "ops": [{"op": "group_by", "keys": ["entity"], "agg": {
                    feature_name: {"op": "count", "params": {}}
                }}],
                "schema": {
                    "fields": {"entity": "str", feature_name: "i64"},
                    "optional_fields": []
                },
                "table_primary_key": ["entity"]
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

/// 7.3 — Atomic push-and-get: single POST returns both ack and feature values
/// that include the just-pushed event (read-your-writes).
///
/// RED: fails because /push-and-get route doesn't exist yet (returns 404).
#[tokio::test]
async fn test_push_and_get_atomic_read_your_writes() {
    let ts = TestServer::builder()
        .dev_endpoints(false)
        .spawn()
        .await
        .expect("spawn");

    register_count_pipeline(&ts, "transaction", "count").await;

    let body = json!({
        "row": {"entity": "u1", "amount": 10.0, "event_time": 1_000_000},
        "query": {
            "entity_key": {"entity": "u1"},
            "features": ["count"]
        }
    });

    let resp = ts
        .post_json("/push-and-get/transaction", &body)
        .await
        .expect("push-and-get request");
    let status = resp.status().as_u16();
    let resp_body: serde_json::Value = resp.json().await.expect("json body");

    assert_eq!(
        status, 200,
        "push-and-get should return 200; got: {status} {resp_body:#}"
    );
    assert!(
        resp_body["ack_lsn"].as_u64().unwrap_or(0) > 0,
        "ack_lsn must be > 0; got: {resp_body:#}"
    );
    assert!(
        resp_body["registry_version"].as_u64().is_some(),
        "registry_version must be present; got: {resp_body:#}"
    );
    assert_eq!(
        resp_body["features"]["count"],
        json!(1),
        "count must be 1 after first push (read-your-writes); got: {resp_body:#}"
    );
    let warnings = resp_body["warnings"].as_array();
    assert!(
        warnings.is_none() || warnings.unwrap().is_empty(),
        "warnings must be empty; got: {resp_body:#}"
    );

    ts.shutdown().await.expect("shutdown");
}

/// 7.3 — Repeated push-and-get calls accumulate count correctly.
///
/// RED: fails because /push-and-get route doesn't exist yet.
#[tokio::test]
async fn test_push_and_get_cumulative_across_calls() {
    let ts = TestServer::builder()
        .dev_endpoints(false)
        .spawn()
        .await
        .expect("spawn");

    register_count_pipeline(&ts, "txn", "cnt").await;

    let body = json!({
        "row": {"entity": "alice", "event_time": 1000},
        "query": {
            "entity_key": {"entity": "alice"},
            "features": ["cnt"]
        }
    });

    for expected in 1u64..=5 {
        let resp = ts
            .post_json("/push-and-get/txn", &body)
            .await
            .expect("push-and-get");
        let status = resp.status().as_u16();
        let rb: serde_json::Value = resp.json().await.expect("json");
        assert_eq!(status, 200, "call {expected}: status={status} body={rb:#}");
        assert_eq!(
            rb["features"]["cnt"],
            json!(expected as i64),
            "after {expected} pushes, cnt must be {expected}; got: {rb:#}"
        );
    }

    ts.shutdown().await.expect("shutdown");
}

/// 7.3 — Unknown feature in query.features returns 200 with null + warning.
/// This matches Phase 12.5 SC5.
///
/// RED: fails because /push-and-get route doesn't exist yet.
#[tokio::test]
async fn test_push_and_get_unknown_feature_returns_null_and_warning() {
    let ts = TestServer::builder()
        .dev_endpoints(false)
        .spawn()
        .await
        .expect("spawn");

    register_count_pipeline(&ts, "ev", "cnt").await;

    let body = json!({
        "row": {"entity": "bob", "event_time": 2000},
        "query": {
            "entity_key": {"entity": "bob"},
            "features": ["cnt", "bogus_feature"]
        }
    });

    let resp = ts
        .post_json("/push-and-get/ev", &body)
        .await
        .expect("push-and-get");
    let status = resp.status().as_u16();
    let rb: serde_json::Value = resp.json().await.expect("json");

    assert_eq!(
        status, 200,
        "unknown feature should still return 200; got={rb:#}"
    );
    assert_eq!(
        rb["features"]["cnt"],
        json!(1),
        "cnt should still be present; got: {rb:#}"
    );
    assert!(
        rb["features"]["bogus_feature"].is_null(),
        "unknown feature must be null; got: {rb:#}"
    );
    let warnings = rb["warnings"]
        .as_array()
        .expect("warnings array must be present");
    assert!(
        warnings
            .iter()
            .any(|w| w.as_str().unwrap_or("").contains("bogus_feature")),
        "warnings must mention bogus_feature; got: {rb:#}"
    );

    ts.shutdown().await.expect("shutdown");
}

/// 7.3 — push-sync-and-get returns after fsync completes.
///
/// RED: fails because /push-sync-and-get route doesn't exist yet.
#[tokio::test]
async fn test_push_sync_and_get_returns_200() {
    let ts = TestServer::builder()
        .dev_endpoints(false)
        .spawn()
        .await
        .expect("spawn");

    register_count_pipeline(&ts, "sync_ev", "sync_cnt").await;

    let body = json!({
        "row": {"entity": "charlie", "event_time": 3000},
        "query": {
            "entity_key": {"entity": "charlie"},
            "features": ["sync_cnt"]
        }
    });

    let resp = ts
        .post_json("/push-sync-and-get/sync_ev", &body)
        .await
        .expect("push-sync-and-get");
    let status = resp.status().as_u16();
    let rb: serde_json::Value = resp.json().await.expect("json");

    assert_eq!(
        status, 200,
        "/push-sync-and-get must return 200; got={rb:#}"
    );
    assert_eq!(
        rb["features"]["sync_cnt"],
        json!(1),
        "sync_cnt must be 1 (read-your-writes); got: {rb:#}"
    );

    ts.shutdown().await.expect("shutdown");
}
