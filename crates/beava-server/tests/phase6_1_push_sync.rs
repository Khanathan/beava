//! Phase 6.1 Plans 02 + 03: `POST /push-sync/{event_name}` endpoint and
//! mode-dependent apply order.
//!
//! /push (default): Periodic-mode WAL append, ACK after in-memory append,
//! state mutations applied right after the append (NOT after fsync).
//!
//! /push-sync: PerEvent-mode WAL append, ACK after fsync, state mutations
//! applied AFTER fsync (Phase 6 D-12 behavior preserved).
//!
//! These tests assert the response shape + status codes + observable
//! behavior; the durability semantics themselves are exercised in
//! `phase6_1_crash.rs`.

#![cfg(feature = "testing")]

use beava_server::testing::TestServerBuilder;
use serde_json::json;
use std::time::Duration;
use tempfile::TempDir;

async fn register_transaction(ts: &beava_server::testing::TestServer) {
    let event_node = json!({
        "kind": "event",
        "name": "Transaction",
        "schema": {
            "fields": {
                "event_time": "i64",
                "user_id": "str",
                "amount": "f64"
            },
            "optional_fields": []
        },
    });
    let agg_node = json!({
        "kind": "derivation",
        "name": "TxnAgg",
        "output_kind": "table",
        "upstreams": ["Transaction"],
        "ops": [{"op": "group_by", "keys": ["user_id"], "agg": {
            "cnt": {"op": "count", "params": {}}
        }}],
        "schema": {"fields": {"user_id": "str", "cnt": "i64"}, "optional_fields": []},
        "table_primary_key": ["user_id"]
    });
    let payload = json!({"nodes": [event_node, agg_node]});
    let resp = ts.post_json("/register", &payload).await.expect("register");
    assert_eq!(resp.status().as_u16(), 200, "register must succeed");
}

async fn spawn_with_wal(tmp: &TempDir) -> beava_server::testing::TestServer {
    TestServerBuilder::new()
        .dev_endpoints(true)
        .wal_dir(tmp.path().to_path_buf())
        .fsync_interval_ms(1)
        .spawn()
        .await
        .expect("spawn")
}

async fn post_to(
    ts: &beava_server::testing::TestServer,
    path: &str,
    body: &serde_json::Value,
) -> reqwest::Response {
    let url = format!("{}{}", ts.base_url(), path);
    reqwest::Client::new()
        .post(&url)
        .header("Content-Type", "application/json")
        .body(serde_json::to_vec(body).unwrap())
        .timeout(Duration::from_secs(5))
        .send()
        .await
        .expect("send")
}

#[tokio::test]
async fn push_sync_endpoint_returns_200_with_ack_shape() {
    let tmp = tempfile::tempdir().unwrap();
    let ts = spawn_with_wal(&tmp).await;
    register_transaction(&ts).await;

    let body = json!({"user_id": "alice", "amount": 1.0, "event_time": 1_000_000});
    let resp = post_to(&ts, "/push-sync/Transaction", &body).await;
    assert_eq!(resp.status().as_u16(), 200, "push-sync should 200");

    let parsed: serde_json::Value = resp.json().await.expect("json");
    assert!(parsed.get("ack_lsn").is_some(), "must include ack_lsn");
    assert_eq!(
        parsed.get("idempotent_replay"),
        Some(&serde_json::Value::Bool(false)),
        "first push not a replay"
    );
    assert!(
        parsed.get("registry_version").is_some(),
        "must include registry_version"
    );
}

#[tokio::test]
async fn push_sync_404s_unknown_event() {
    let tmp = tempfile::tempdir().unwrap();
    let ts = spawn_with_wal(&tmp).await;
    let body = json!({"user_id": "alice"});
    let resp = post_to(&ts, "/push-sync/DoesNotExist", &body).await;
    assert_eq!(resp.status().as_u16(), 404);
}

#[tokio::test]
async fn push_sync_validates_schema() {
    let tmp = tempfile::tempdir().unwrap();
    let ts = spawn_with_wal(&tmp).await;
    register_transaction(&ts).await;

    // Missing required `amount`.
    let body = json!({"user_id": "alice", "event_time": 1});
    let resp = post_to(&ts, "/push-sync/Transaction", &body).await;
    assert_eq!(resp.status().as_u16(), 400);
}

/// Apply-after-append on /push: after a successful 200, the aggregation
/// must reflect the event. (Aggregation semantics are unchanged; this
/// is a smoke that we didn't accidentally drop the apply step.)
#[tokio::test]
async fn push_default_applies_to_aggregations() {
    let tmp = tempfile::tempdir().unwrap();
    let ts = spawn_with_wal(&tmp).await;
    register_transaction(&ts).await;

    let body = json!({"user_id": "alice", "amount": 1.0, "event_time": 1_000_000});
    let resp = post_to(&ts, "/push/Transaction", &body).await;
    assert_eq!(resp.status().as_u16(), 200);

    // Query the count.
    let url = format!("{}/get/cnt/alice", ts.base_url());
    let r = reqwest::Client::new()
        .get(&url)
        .timeout(Duration::from_secs(2))
        .send()
        .await
        .expect("send");
    assert_eq!(r.status().as_u16(), 200);
    let body: serde_json::Value = r.json().await.unwrap();
    assert_eq!(
        body.get("value").and_then(|v| v.as_i64()),
        Some(1),
        "default /push must apply to aggregations: {body}"
    );
}

/// Apply-after-fsync on /push-sync: same observable result.
#[tokio::test]
async fn push_sync_applies_to_aggregations() {
    let tmp = tempfile::tempdir().unwrap();
    let ts = spawn_with_wal(&tmp).await;
    register_transaction(&ts).await;

    let body = json!({"user_id": "bob", "amount": 1.0, "event_time": 1_000_000});
    let resp = post_to(&ts, "/push-sync/Transaction", &body).await;
    assert_eq!(resp.status().as_u16(), 200);

    let url = format!("{}/get/cnt/bob", ts.base_url());
    let r = reqwest::Client::new()
        .get(&url)
        .timeout(Duration::from_secs(2))
        .send()
        .await
        .expect("send");
    assert_eq!(r.status().as_u16(), 200);
    let body: serde_json::Value = r.json().await.unwrap();
    assert_eq!(
        body.get("value").and_then(|v| v.as_i64()),
        Some(1),
        "/push-sync must apply to aggregations: {body}"
    );
}
