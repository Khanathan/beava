//! Phase 7.5 Plan 02: end-to-end restart-cycle smokes (closes Phase 7 SC1, SC4).
//!
//! These tests close the PARTIAL success criteria left by Phase 7's
//! 07-VERIFICATION.md. They became writable once Phase 7.5 Plan 01 fixed
//! the RegistryBump WAL codec round-trip.
//!
//! - **SC1**: Snapshot atomic write → reproducible state after restart from
//!   snapshot + WAL-past-LSN.
//! - **SC4**: Schema evolution survives restart — register A → push → register
//!   B → restart → both events queryable.
//!
//! SC2 (crash mid-snapshot preserves committed events) requires a subprocess
//! crash-probe binary modeled on `phase6_crash_probe.rs`. Phase 7's snapshot
//! atomic-rename unit tests (`snapshot_roundtrip.rs`) already prove the disk
//! invariant; the integration-level crash probe is deferred to a Phase 8+
//! follow-up to keep Phase 7.5 focused on the throughput harness.

#![cfg(feature = "testing")]

use beava_server::testing::TestServerBuilder;
use serde_json::json;

fn txn_descriptor() -> serde_json::Value {
    json!({
        "kind": "event",
        "name": "Txn",
        "schema": {"fields": {
            "event_time": "i64",
            "user_id": "str",
            "amount": "f64"
        }, "optional_fields": []},
        "event_time_field": "event_time",
    })
}

fn txn_agg_descriptor() -> serde_json::Value {
    json!({
        "kind": "derivation",
        "name": "TxnAgg",
        "output_kind": "table",
        "upstreams": ["Txn"],
        "ops": [{"op": "group_by", "keys": ["user_id"], "agg": {
            "cnt": {"op": "count", "params": {}}
        }}],
        "schema": {"fields": {"user_id": "str", "cnt": "i64"}, "optional_fields": []},
        "table_primary_key": ["user_id"]
    })
}

fn click_descriptor() -> serde_json::Value {
    json!({
        "kind": "event",
        "name": "Click",
        "schema": {"fields": {
            "event_time": "i64",
            "user_id": "str",
        }, "optional_fields": []},
        "event_time_field": "event_time",
    })
}

fn click_agg_descriptor() -> serde_json::Value {
    json!({
        "kind": "derivation",
        "name": "ClickAgg",
        "output_kind": "table",
        "upstreams": ["Click"],
        "ops": [{"op": "group_by", "keys": ["user_id"], "agg": {
            "click_cnt": {"op": "count", "params": {}}
        }}],
        "schema": {"fields": {"user_id": "str", "click_cnt": "i64"}, "optional_fields": []},
        "table_primary_key": ["user_id"]
    })
}

async fn register(ts: &beava_server::testing::TestServer, nodes: serde_json::Value) {
    let resp = ts
        .post_json("/register", &json!({"nodes": nodes}))
        .await
        .expect("register");
    let status = resp.status().as_u16();
    let body = resp.text().await.unwrap_or_default();
    assert_eq!(status, 200, "register expected 200, got {status}: {body}");
}

async fn push_event(
    ts: &beava_server::testing::TestServer,
    event_name: &str,
    body: serde_json::Value,
) {
    let path = format!("/push/{event_name}");
    let resp = ts.post_json(&path, &body).await.expect("push");
    assert_eq!(
        resp.status().as_u16(),
        200,
        "push to {event_name} expected 200"
    );
}

async fn get_feature(
    ts: &beava_server::testing::TestServer,
    keys: &[&str],
    features: &[&str],
) -> serde_json::Value {
    let url = format!("{}/get", ts.base_url());
    let r = reqwest::Client::new()
        .post(&url)
        .header("Content-Type", "application/json")
        .body(json!({"keys": keys, "features": features}).to_string())
        .send()
        .await
        .expect("post /get");
    let status = r.status().as_u16();
    let body = r.text().await.unwrap_or_default();
    assert_eq!(status, 200, "/get expected 200, got {status}: {body}");
    serde_json::from_str(&body).expect("body json")
}

/// SC1: snapshot atomic write → reproducible state after restart from
/// snapshot + WAL-past-LSN.
///
/// Push 1000 events, force_snapshot_now (truncates WAL up to snapshot LSN),
/// push 250 more events (these stay in the post-snapshot WAL tail), then
/// shutdown. Respawn with same dirs and assert the post-restart server sees
/// 1250 total events.
#[tokio::test]
async fn sc1_snapshot_then_restart_reproduces_state() {
    let wal = tempfile::tempdir().unwrap();
    let snap = tempfile::tempdir().unwrap();

    {
        let ts = TestServerBuilder::new()
            .dev_endpoints(true)
            .wal_dir(wal.path().to_path_buf())
            .snapshot_dir(snap.path().to_path_buf())
            .fsync_interval_ms(1)
            .spawn()
            .await
            .expect("spawn 1st");

        register(&ts, json!([txn_descriptor(), txn_agg_descriptor()])).await;
        for i in 0..1000_i64 {
            push_event(
                &ts,
                "Txn",
                json!({"user_id": "alice", "amount": 1.0, "event_time": 1_000_000 + i}),
            )
            .await;
        }
        ts.force_snapshot_now().await.expect("force snapshot");
        for i in 0..250_i64 {
            push_event(
                &ts,
                "Txn",
                json!({"user_id": "alice", "amount": 1.0, "event_time": 2_000_000 + i}),
            )
            .await;
        }
        let v = get_feature(&ts, &["alice"], &["cnt"]).await;
        assert_eq!(
            v["result"]["alice"]["cnt"], 1250,
            "pre-restart cnt expected 1250, got {v}"
        );
        ts.shutdown().await.expect("shutdown 1st");
    }

    // Verify a snapshot file exists on disk before restart — this is what
    // the cold-start recovery has to install before it touches the WAL tail.
    let snap_files = std::fs::read_dir(snap.path())
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().and_then(|s| s.to_str()) == Some("bvs"))
        .count();
    assert!(
        snap_files >= 1,
        "expected at least one .bvs snapshot before restart, found {snap_files}"
    );

    {
        let ts = TestServerBuilder::new()
            .dev_endpoints(true)
            .wal_dir(wal.path().to_path_buf())
            .snapshot_dir(snap.path().to_path_buf())
            .fsync_interval_ms(1)
            .spawn()
            .await
            .expect("spawn 2nd");

        let v = get_feature(&ts, &["alice"], &["cnt"]).await;
        assert_eq!(
            v["result"]["alice"]["cnt"], 1250,
            "post-restart cnt expected 1250 (snapshot + WAL-past-LSN), got {v}"
        );
        ts.shutdown().await.expect("shutdown 2nd");
    }
}

/// SC4: Schema evolution survives restart — register Txn+TxnAgg, push, then
/// register Click+ClickAgg (additive bump), push to both, shutdown, respawn
/// with same dirs. Both aggregations must be recovered, both per-feature
/// values must match.
#[tokio::test]
async fn sc4_schema_evolution_survives_restart() {
    let wal = tempfile::tempdir().unwrap();
    let snap = tempfile::tempdir().unwrap();

    {
        let ts = TestServerBuilder::new()
            .dev_endpoints(true)
            .wal_dir(wal.path().to_path_buf())
            .snapshot_dir(snap.path().to_path_buf())
            .fsync_interval_ms(1)
            .spawn()
            .await
            .expect("spawn 1st");

        // First registration: Txn + TxnAgg
        register(&ts, json!([txn_descriptor(), txn_agg_descriptor()])).await;
        for i in 0..7_i64 {
            push_event(
                &ts,
                "Txn",
                json!({"user_id": "alice", "amount": 1.0, "event_time": 1_000_000 + i}),
            )
            .await;
        }

        // Schema evolution: ADD Click + ClickAgg in a second /register call.
        register(&ts, json!([click_descriptor(), click_agg_descriptor()])).await;
        for i in 0..3_i64 {
            push_event(
                &ts,
                "Click",
                json!({"user_id": "alice", "event_time": 2_000_000 + i}),
            )
            .await;
        }

        let v = get_feature(&ts, &["alice"], &["cnt", "click_cnt"]).await;
        assert_eq!(v["result"]["alice"]["cnt"], 7);
        assert_eq!(v["result"]["alice"]["click_cnt"], 3);

        ts.shutdown().await.expect("shutdown 1st");
    }

    {
        let ts = TestServerBuilder::new()
            .dev_endpoints(true)
            .wal_dir(wal.path().to_path_buf())
            .snapshot_dir(snap.path().to_path_buf())
            .fsync_interval_ms(1)
            .spawn()
            .await
            .expect("spawn 2nd");

        // Both aggregations must come back, both keys must resolve.
        let v = get_feature(&ts, &["alice"], &["cnt", "click_cnt"]).await;
        assert_eq!(
            v["result"]["alice"]["cnt"], 7,
            "post-restart cnt expected 7 (recovered v1 schema + replayed events), got {v}"
        );
        assert_eq!(
            v["result"]["alice"]["click_cnt"], 3,
            "post-restart click_cnt expected 3 (recovered v2 schema + replayed events), got {v}"
        );

        ts.shutdown().await.expect("shutdown 2nd");
    }
}

/// Bonus: verify SC1 + SC4 combined — snapshot MID-WAY through schema
/// evolution. Specifically: register A → push A → snapshot → register B →
/// push A and B → shutdown → restart. Snapshot covers v1 schema; WAL tail
/// covers v2 RegistryBump + post-snapshot events.
#[tokio::test]
async fn snapshot_then_schema_evolution_then_restart() {
    let wal = tempfile::tempdir().unwrap();
    let snap = tempfile::tempdir().unwrap();

    {
        let ts = TestServerBuilder::new()
            .dev_endpoints(true)
            .wal_dir(wal.path().to_path_buf())
            .snapshot_dir(snap.path().to_path_buf())
            .fsync_interval_ms(1)
            .spawn()
            .await
            .expect("spawn 1st");

        register(&ts, json!([txn_descriptor(), txn_agg_descriptor()])).await;
        for i in 0..5_i64 {
            push_event(
                &ts,
                "Txn",
                json!({"user_id": "alice", "amount": 1.0, "event_time": 1_000_000 + i}),
            )
            .await;
        }

        // Snapshot covers Txn + TxnAgg + 5 events.
        ts.force_snapshot_now().await.expect("force snapshot");

        // Now bump the schema (RegistryBump record lands AFTER snapshot LSN).
        register(&ts, json!([click_descriptor(), click_agg_descriptor()])).await;
        for i in 0..2_i64 {
            push_event(
                &ts,
                "Click",
                json!({"user_id": "alice", "event_time": 2_000_000 + i}),
            )
            .await;
        }
        for i in 0..3_i64 {
            push_event(
                &ts,
                "Txn",
                json!({"user_id": "alice", "amount": 1.0, "event_time": 3_000_000 + i}),
            )
            .await;
        }

        let v = get_feature(&ts, &["alice"], &["cnt", "click_cnt"]).await;
        assert_eq!(v["result"]["alice"]["cnt"], 8);
        assert_eq!(v["result"]["alice"]["click_cnt"], 2);

        ts.shutdown().await.expect("shutdown 1st");
    }

    {
        let ts = TestServerBuilder::new()
            .dev_endpoints(true)
            .wal_dir(wal.path().to_path_buf())
            .snapshot_dir(snap.path().to_path_buf())
            .fsync_interval_ms(1)
            .spawn()
            .await
            .expect("spawn 2nd");

        let v = get_feature(&ts, &["alice"], &["cnt", "click_cnt"]).await;
        assert_eq!(
            v["result"]["alice"]["cnt"], 8,
            "post-restart cnt expected 8 (5 from snapshot + 3 from WAL tail), got {v}"
        );
        assert_eq!(
            v["result"]["alice"]["click_cnt"], 2,
            "post-restart click_cnt expected 2 (RegistryBump + 2 events all from WAL tail), got {v}"
        );

        ts.shutdown().await.expect("shutdown 2nd");
    }
}
