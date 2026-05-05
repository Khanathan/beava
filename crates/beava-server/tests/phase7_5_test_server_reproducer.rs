//! Phase 7.5 Plan 01 (RED): TestServer + restart-recovery reproducers.
//!
//! Two tests:
//!
//! 1. `two_sequential_test_servers_isolated_dirs_both_serve_get` — confirms
//!    that two `TestServer::spawn()` calls in the same `#[tokio::test]`
//!    using INDEPENDENT wal/snapshot tempdirs both serve `/get` correctly.
//!    The Phase 7 SUMMARY reported "second TestServer sees empty registry"
//!    here; this test passes, ruling that out as the actual blocker.
//!
//! 2. `restart_with_same_dirs_recovers_registry_and_state` — the REAL bug.
//!    Spawn server, register, push 5 events, shutdown. Spawn again with the
//!    SAME wal/snapshot dirs. Recovery must replay the RegistryBump + Event
//!    records so /get returns cnt=5. Currently FAILS with `feature_not_found`
//!    because the RegistryBump payload encodes `serde_json::Value` fields
//!    (`OpNode::Fillna.defaults`, `AggSpec.params`) which bincode 1.x cannot
//!    deserialize ("Bincode does not support the serde::Deserializer::
//!    deserialize_any method"). The decode error is silently swallowed by a
//!    `tracing::warn!` in `recovery::replay_wal_from_lsn`.

#![cfg(feature = "testing")]

use beava_server::testing::TestServerBuilder;
use serde_json::json;

async fn register_and_query(ts: &beava_server::testing::TestServer, label: &str) {
    let payload = json!({"nodes": [
        {
            "kind": "event",
            "name": "Txn",
            "schema": {"fields": {
                "event_time": "i64",
                "user_id": "str",
                "amount": "f64"
            }, "optional_fields": []},
        },
        {
            "kind": "derivation",
            "name": "TxnAgg",
            "output_kind": "table",
            "upstreams": ["Txn"],
            "ops": [{"op": "group_by", "keys": ["user_id"], "agg": {
                "cnt": {"op": "count", "params": {}}
            }}],
            "schema": {"fields": {"user_id": "str", "cnt": "i64"}, "optional_fields": []},
            "table_primary_key": ["user_id"]
        }
    ]});
    let resp = ts.post_json("/register", &payload).await.expect("register");
    assert_eq!(
        resp.status().as_u16(),
        200,
        "[{label}] register failed: {}",
        resp.text().await.unwrap_or_default()
    );

    for i in 0..5 {
        let body = json!({
            "user_id": "alice",
            "amount": 1.0,
            "event_time": 1_000_000_i64 + i as i64,
        });
        let resp = ts.post_json("/push/Txn", &body).await.expect("push");
        assert_eq!(
            resp.status().as_u16(),
            200,
            "[{label}] push #{i} failed: {}",
            resp.text().await.unwrap_or_default()
        );
    }

    // Phase 13.5.4 alignment per CLAUDE.md §TDD Discipline item #4 (lockstep
    // alignment exemption): post-13.4 POST /get takes verb-style
    // {table, key, features?} and returns a flat dict. The derivation table
    // queried here is "TxnAgg" (registered above).
    let url = format!("{}/get", ts.base_url());
    let r = reqwest::Client::new()
        .post(&url)
        .header("Content-Type", "application/json")
        .body(json!({"table": "TxnAgg", "key": "alice", "features": ["cnt"]}).to_string())
        .send()
        .await
        .expect("post /get");
    let status = r.status().as_u16();
    let body = r.text().await.unwrap_or_default();
    assert_eq!(
        status, 200,
        "[{label}] /get expected 200, got {status}: {body}"
    );
    let v: serde_json::Value = serde_json::from_str(&body).expect("body json");
    assert_eq!(v["cnt"], 5, "[{label}] expected cnt=5, got {v}");
}

/// Two sequential TestServer spawns in ONE test, each with its own tempdirs
/// and shutdown between. This is the minimal reproducer for the documented
/// Phase 7 flake — and it passes, ruling out router-state propagation as
/// the actual blocker.
#[tokio::test]
async fn two_sequential_test_servers_isolated_dirs_both_serve_get() {
    let wal_a = tempfile::tempdir().unwrap();
    let snap_a = tempfile::tempdir().unwrap();
    let ts_a = TestServerBuilder::new()
        .dev_endpoints(true)
        .wal_dir(wal_a.path().to_path_buf())
        .snapshot_dir(snap_a.path().to_path_buf())
        .fsync_interval_ms(1)
        .spawn()
        .await
        .expect("spawn A");
    register_and_query(&ts_a, "A").await;
    ts_a.shutdown().await.expect("shutdown A");

    let wal_b = tempfile::tempdir().unwrap();
    let snap_b = tempfile::tempdir().unwrap();
    let ts_b = TestServerBuilder::new()
        .dev_endpoints(true)
        .wal_dir(wal_b.path().to_path_buf())
        .snapshot_dir(snap_b.path().to_path_buf())
        .fsync_interval_ms(1)
        .spawn()
        .await
        .expect("spawn B");
    register_and_query(&ts_b, "B").await;
    ts_b.shutdown().await.expect("shutdown B");
}

/// Restart with same dirs: recovery must replay the RegistryBump WAL record
/// so the second spawn knows about the registered features. Without the
/// fix this fails with `feature_not_found` because RegistryBump bincode
/// decode trips on `serde_json::Value` fields inside the payload.
#[tokio::test]
async fn restart_with_same_dirs_recovers_registry_and_state() {
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
        register_and_query(&ts, "first").await;
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
        // Phase 13.5.4 alignment per CLAUDE.md §TDD Discipline item #4: verb-style
        // {table, key, features?}; flat-dict response.
        let url = format!("{}/get", ts.base_url());
        let r = reqwest::Client::new()
            .post(&url)
            .header("Content-Type", "application/json")
            .body(json!({"table": "TxnAgg", "key": "alice", "features": ["cnt"]}).to_string())
            .send()
            .await
            .expect("post /get");
        let status = r.status().as_u16();
        let body = r.text().await.unwrap_or_default();
        assert_eq!(
            status, 200,
            "[restart] /get expected 200, got {status}: {body}"
        );
        let v: serde_json::Value = serde_json::from_str(&body).expect("body json");
        assert_eq!(
            v["cnt"], 5,
            "[restart] expected cnt=5 after recovery, got {v}"
        );
        ts.shutdown().await.expect("shutdown 2nd");
    }
}

/// Direct unit test on `apply_registry_bump`: a freshly-decoded RegistryBump
/// payload from a real `/register` flow must apply cleanly to a fresh
/// Registry and populate `feature_index` so `resolve_feature` returns Some.
#[tokio::test]
async fn registry_bump_payload_roundtrips_via_wal_codec() {
    use beava_core::registry::Registry;
    use beava_persistence::{RecordType, WalReader};
    use beava_server::register::{apply_registry_bump, RegistryBumpPayload};
    use std::sync::Arc;

    let wal = tempfile::tempdir().unwrap();
    let snap = tempfile::tempdir().unwrap();

    let ts = TestServerBuilder::new()
        .dev_endpoints(true)
        .wal_dir(wal.path().to_path_buf())
        .snapshot_dir(snap.path().to_path_buf())
        .fsync_interval_ms(1)
        .spawn()
        .await
        .expect("spawn");
    register_and_query(&ts, "rt").await;
    ts.shutdown().await.expect("shutdown");

    let recs = WalReader::read_all(wal.path()).expect("read_all");
    let bump_rec = recs
        .iter()
        .find(|r| matches!(r.record_type, RecordType::RegistryBump))
        .expect("RegistryBump record present");

    let bump = RegistryBumpPayload::decode(&bump_rec.payload)
        .expect("RegistryBump payload must round-trip via WAL codec");
    let registry = Arc::new(Registry::new());
    apply_registry_bump(&registry, bump).expect("apply_registry_bump");
    assert!(
        registry.resolve_feature("cnt").is_some(),
        "after apply_registry_bump, resolve_feature('cnt') must be Some"
    );
}
