//! Phase 12.6 Plan 04 — Register-time rejection of removed `join` and `union` ops.
//!
//! The architectural pivot 2026-04-30 (`project_redis_shaped_no_event_time_ever`)
//! permanently removes joins and unions from the v0 surface. This test pins the
//! contract: stale Python-SDK fixtures or hand-rolled JSON DAGs that include
//! `{"op": "join"}` or `{"op": "union"}` MUST be rejected at register time with
//! HTTP 400 and a structured error code (`feature_removed_no_joins_v0` or
//! `feature_removed_no_unions_v0`).
//!
//! The interception happens at the JSON layer in
//! `register_validate::pre_check_removed_ops` BEFORE the strict OpNode
//! deserialize, so the rejection works whether or not the OpNode variants
//! still exist in the enum.

#![cfg(feature = "testing")]

use beava_server::testing::TestServer;
use serde_json::json;

#[tokio::test]
async fn register_join_returns_feature_removed_no_joins_v0() {
    let ts = TestServer::spawn().await.expect("spawn");

    // Two upstream events + a derivation that tries to join them.
    let payload = json!({
        "nodes": [
            {
                "kind": "event",
                "name": "E1",
                "schema": {"fields": {"x": "str"}, "optional_fields": []}
            },
            {
                "kind": "event",
                "name": "E2",
                "schema": {"fields": {"x": "str"}, "optional_fields": []}
            },
            {
                "kind": "derivation",
                "name": "EJoin",
                "output_kind": "event",
                "upstreams": ["E1", "E2"],
                "ops": [
                    {"op": "join", "other": "E2", "on": ["x"], "join_type": "inner"}
                ],
                "schema": {"fields": {"x": "str"}, "optional_fields": []}
            }
        ]
    });

    let resp = ts.post_json("/register", &payload).await.expect("register");
    let status = resp.status().as_u16();
    let body_text = resp.text().await.expect("body text");
    assert_eq!(
        status, 400,
        "join DAG must be rejected at register time, got status={status}, body={body_text}"
    );
    let body: serde_json::Value = serde_json::from_str(&body_text).expect("body json");
    assert_eq!(
        body["error"]["code"], "feature_removed_no_joins_v0",
        "expected feature_removed_no_joins_v0, got body={body}"
    );
    let path = body["error"]["path"].as_str().unwrap_or_default();
    assert!(
        path.contains("EJoin"),
        "error.path should reference the offending derivation 'EJoin', got: {path}"
    );

    ts.shutdown().await.ok();
}

#[tokio::test]
async fn register_union_returns_feature_removed_no_unions_v0() {
    let ts = TestServer::spawn().await.expect("spawn");

    let payload = json!({
        "nodes": [
            {
                "kind": "event",
                "name": "E1",
                "schema": {"fields": {"x": "str"}, "optional_fields": []}
            },
            {
                "kind": "event",
                "name": "E2",
                "schema": {"fields": {"x": "str"}, "optional_fields": []}
            },
            {
                "kind": "derivation",
                "name": "EUnion",
                "output_kind": "event",
                "upstreams": ["E1", "E2"],
                "ops": [
                    {"op": "union", "others": ["E2"]}
                ],
                "schema": {"fields": {"x": "str"}, "optional_fields": []}
            }
        ]
    });

    let resp = ts.post_json("/register", &payload).await.expect("register");
    let status = resp.status().as_u16();
    let body_text = resp.text().await.expect("body text");
    assert_eq!(
        status, 400,
        "union DAG must be rejected at register time, got status={status}, body={body_text}"
    );
    let body: serde_json::Value = serde_json::from_str(&body_text).expect("body json");
    assert_eq!(
        body["error"]["code"], "feature_removed_no_unions_v0",
        "expected feature_removed_no_unions_v0, got body={body}"
    );
    let path = body["error"]["path"].as_str().unwrap_or_default();
    assert!(
        path.contains("EUnion"),
        "error.path should reference the offending derivation 'EUnion', got: {path}"
    );

    ts.shutdown().await.ok();
}
