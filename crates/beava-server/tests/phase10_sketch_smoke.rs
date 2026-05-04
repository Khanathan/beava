//! Phase 10 Plan 10-05: end-to-end smoke for all 5 sketch operators.
//!
//! Registers a pipeline that exercises n_unique (count_distinct),
//! quantile (percentile), top_k, bloom_member, and entropy. Pushes 100
//! events, then GETs each feature and asserts a successful response with
//! a non-null `value`.
//!
//! Phase 13.4-01 per ADR-002: count_distinct→n_unique, percentile→quantile
//! on the wire.
//!
//! Also verifies that bloom_member with `window=` is rejected at register time
//! with kind=window_not_supported.

#![cfg(feature = "testing")]

use beava_server::testing::TestServerBuilder;
use serde_json::json;

fn sketch_pipeline_payload() -> serde_json::Value {
    json!({
        "nodes": [
            {
                "kind": "event",
                "name": "Tx",
                "schema": {"fields": {
                    "event_time": "i64",
                    "user_id": "str",
                    "merchant_id": "str",
                    "amount": "f64",
                    "device_id": "str",
                    "category": "str"
                }, "optional_fields": []},
            },
            {
                "kind": "derivation",
                "name": "TxFeatures",
                "output_kind": "table",
                "upstreams": ["Tx"],
                "ops": [{"op": "group_by", "keys": ["user_id"], "agg": {
                    "merchants_distinct_1h": {"op": "n_unique",   "params": {"field": "merchant_id", "window": "1h"}},
                    "amount_p99_1h":          {"op": "quantile",   "params": {"field": "amount", "q": 0.99, "window": "1h"}},
                    "top_merchants_1h":       {"op": "top_k",          "params": {"field": "merchant_id", "k": 3, "window": "1h"}},
                    "device_seen":            {"op": "bloom_member",   "params": {"field": "device_id"}},
                    "category_entropy_1h":    {"op": "entropy",        "params": {"field": "category", "window": "1h"}}
                }}],
                "schema": {"fields": {
                    "user_id": "str",
                    "merchants_distinct_1h": "i64",
                    "amount_p99_1h": "f64",
                    "top_merchants_1h": "json",
                    "device_seen": "bool",
                    "category_entropy_1h": "f64"
                }, "optional_fields": []},
                "table_primary_key": ["user_id"]
            }
        ]
    })
}

#[tokio::test]
async fn phase10_sketch_pipeline_register_push_get_works() {
    let wal = tempfile::tempdir().unwrap();
    let ts = TestServerBuilder::new()
        .dev_endpoints(true)
        .wal_dir(wal.path().to_path_buf())
        .fsync_interval_ms(1)
        .spawn()
        .await
        .expect("spawn");
    let resp = ts
        .post_json("/register", &sketch_pipeline_payload())
        .await
        .expect("register");
    assert_eq!(
        resp.status().as_u16(),
        200,
        "register failed: {}",
        resp.text().await.unwrap_or_default()
    );

    // Push 100 events for u1.
    for i in 0..100_i64 {
        let cat = ["A", "B", "C", "D"][(i % 4) as usize];
        let evt = json!({
            "user_id": "u1",
            "merchant_id": format!("m{}", i % 10),
            "amount": (i as f64) * 1.5,
            "device_id": format!("d{}", i % 3),
            "category": cat,
            "event_time": 1_700_000_000_000_i64 + i * 1000,
        });
        let r = ts.post_json("/push/Tx", &evt).await.expect("push");
        assert_eq!(r.status().as_u16(), 200, "push #{i} failed");
    }

    // GET each feature.
    for feat in &[
        "merchants_distinct_1h",
        "amount_p99_1h",
        "top_merchants_1h",
        "device_seen",
        "category_entropy_1h",
    ] {
        let r = ts.get_raw(&format!("/get/{}/u1", feat)).await;
        assert_eq!(r.status().as_u16(), 200, "GET {feat}");
        let body: serde_json::Value = r.json().await.expect("json");
        assert!(
            body.get("value").is_some(),
            "GET {feat} missing value: {body}"
        );
        // None of the queries should return null in this fixture.
        assert!(
            !body["value"].is_null(),
            "GET {feat} returned null value: {body}"
        );
    }

    // Sanity-check specific outputs.
    let cd: serde_json::Value = ts.get_json("/get/merchants_distinct_1h/u1").await;
    let cd_v = cd["value"].as_i64().expect("count_distinct i64");
    assert!(
        (5..=15).contains(&cd_v),
        "expected ~10 distinct merchants, got {cd_v}"
    );

    let bm: serde_json::Value = ts.get_json("/get/device_seen/u1").await;
    assert_eq!(
        bm["value"],
        serde_json::Value::Bool(true),
        "bloom_member should be Bool(true) post-inserts"
    );

    let tk: serde_json::Value = ts.get_json("/get/top_merchants_1h/u1").await;
    assert!(tk["value"].is_array(), "top_k value should be array");

    ts.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn phase10_register_rejects_bloom_member_with_window() {
    let wal = tempfile::tempdir().unwrap();
    let ts = TestServerBuilder::new()
        .wal_dir(wal.path().to_path_buf())
        .fsync_interval_ms(1)
        .spawn()
        .await
        .expect("spawn");

    let payload = json!({
        "nodes": [
            {
                "kind": "event",
                "name": "Tx",
                "schema": {"fields": {"event_time": "i64", "user_id": "str", "device_id": "str"}, "optional_fields": []},
            },
            {
                "kind": "derivation",
                "name": "F",
                "output_kind": "table",
                "upstreams": ["Tx"],
                "ops": [{"op": "group_by", "keys": ["user_id"], "agg": {
                    "bad": {"op": "bloom_member", "params": {"field": "device_id", "window": "1h"}}
                }}],
                "schema": {"fields": {"user_id": "str", "bad": "bool"}, "optional_fields": []},
                "table_primary_key": ["user_id"]
            }
        ]
    });
    let resp = ts.post_json("/register", &payload).await.expect("register");
    let status = resp.status().as_u16();
    let body = resp.text().await.unwrap_or_default();
    assert_eq!(status, 400, "expected 400; body={body}");
    assert!(
        body.contains("window_not_supported"),
        "expected window_not_supported error code; body={body}"
    );

    ts.shutdown().await.expect("shutdown");
}
