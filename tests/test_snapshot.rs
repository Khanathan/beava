//! Integration tests for snapshot persistence and TTL eviction.
//!
//! Tests PERS-01 (periodic snapshot), PERS-02 (postcard + versioned format),
//! PERS-03 (crash recovery), PERS-04 (non-blocking write), PERS-05 (TTL eviction).

use std::time::{Duration, SystemTime, UNIX_EPOCH};
use beava::engine::pipeline::{FeatureDef, PipelineEngine, StreamDefinition};
use beava::state::eviction::evict_expired_keys;
use beava::state::snapshot::{load_snapshot, save_snapshot, SerializablePipeline, SnapshotState};
use beava::state::store::StateStore;
use beava::types::FeatureValue;

fn ts(secs: u64) -> SystemTime {
    UNIX_EPOCH + Duration::from_secs(secs)
}

fn make_tx_stream() -> StreamDefinition {
    StreamDefinition {
        name: "Transactions".into(),
        key_field: Some("user_id".into()),
        group_by_keys: None,
        features: vec![
            (
                "tx_count_1h".into(),
                FeatureDef::Count {
                    window: Duration::from_secs(3600),
                    bucket: Duration::from_secs(60),
                    where_expr: None,
                    backfill: false,
                },
            ),
            (
                "tx_sum_1h".into(),
                FeatureDef::Sum {
                    field: "amount".into(),
                    window: Duration::from_secs(3600),
                    bucket: Duration::from_secs(60),
                    optional: false,
                    where_expr: None,
                    backfill: false,
                },
            ),
        ],
        depends_on: None,
        filter: None,
        entity_ttl: None,
        history_ttl: None,
        projection: None,
        ephemeral: None,
        pipeline_ttl: None,
        max_keys: None,
        watermark_lateness: None,
    }
}

// ======================== Snapshot Roundtrip ========================

#[test]
fn test_snapshot_roundtrip_preserves_features() {
    let mut engine = PipelineEngine::new();
    let store = StateStore::new();
    engine.register(make_tx_stream()).unwrap();

    let now = ts(60_000);

    // Push 3 events
    for amount in [10.0, 20.0, 30.0] {
        let event = serde_json::json!({
            "user_id": "u123",
            "amount": amount
        });
        engine.push("Transactions", &event, &store, now).unwrap();
    }

    // Clone state for snapshot
    let entities = store.clone_for_snapshot();
    let snapshot = SnapshotState {
        entities,
        pipelines: vec![SerializablePipeline {
            name: "Transactions".into(),
            key_field: "user_id".into(),
            raw_register_json: r#"{"name":"Transactions","key_field":"user_id","features":[{"name":"tx_count_1h","type":"count","window":"1h"},{"name":"tx_sum_1h","type":"sum","field":"amount","window":"1h"}]}"#.to_string(),
        }],
        backfill_complete: vec![],
    };

    // Save and load
    let bytes = save_snapshot(&snapshot).expect("save_snapshot should succeed");
    let restored = load_snapshot(&bytes).expect("load_snapshot should succeed");

    // Restore into a new store
    let new_store = StateStore::new();
    new_store.restore_from_snapshot(restored.entities);

    // Verify features match
    let features = new_store.get_all_features("u123", now);
    assert_eq!(features.get("tx_count_1h"), Some(&FeatureValue::Int(3)));
    assert_eq!(features.get("tx_sum_1h"), Some(&FeatureValue::Float(60.0)));

    // Verify pipeline info preserved
    assert_eq!(restored.pipelines.len(), 1);
    assert_eq!(restored.pipelines[0].name, "Transactions");
    assert_eq!(restored.pipelines[0].key_field, "user_id");
}

// ======================== Version Mismatch ========================

#[test]
fn test_snapshot_version_mismatch_returns_none() {
    let snapshot = SnapshotState {
        entities: vec![],
        pipelines: vec![],
        backfill_complete: vec![],
    };
    let mut bytes = save_snapshot(&snapshot).expect("save_snapshot should succeed");
    // Mutate version byte to invalid value
    bytes[0] = 0xFF;
    assert!(load_snapshot(&bytes).is_none());
}

// ======================== Empty Bytes ========================

#[test]
fn test_snapshot_empty_bytes_returns_none() {
    assert!(load_snapshot(&[]).is_none());
}

// ======================== Corrupt Data ========================

#[test]
fn test_snapshot_corrupt_data_returns_none() {
    // Correct version byte followed by garbage
    let mut bytes = vec![0x04]; // version 4
    bytes.extend_from_slice(b"this is absolutely not valid postcard data!!!");
    assert!(load_snapshot(&bytes).is_none());
}

// ======================== Eviction ========================

#[test]
fn test_eviction_removes_old_entity() {
    let store = StateStore::new();
    let mut engine = PipelineEngine::new();
    engine
        .register(StreamDefinition {
            name: "stream1".into(),
            key_field: Some("user_id".into()),
            group_by_keys: None,
            features: vec![(
                "count".into(),
                FeatureDef::Count {
                    window: Duration::from_secs(1800), // 30m window
                    bucket: Duration::from_secs(60),
                    where_expr: None,
                    backfill: false,
                },
            )],
            depends_on: None,
            filter: None,
            entity_ttl: None,
            history_ttl: None,
            projection: None,
            ephemeral: None,
            pipeline_ttl: None,
            max_keys: None,
        watermark_lateness: None,
        })
        .unwrap();

    // Entity with old last_event_at (strictly older than TTL)
    {
        let mut entity = store.get_or_create_entity("old_user");
        let stream = entity.get_or_create_stream("stream1");
        stream.last_event_at = Some(ts(96_399)); // 100_000 - 96_399 = 3601s > 3600s TTL -> evicted
    }

    // Entity at exactly TTL boundary (should be kept)
    {
        let mut entity = store.get_or_create_entity("boundary_user");
        let stream = entity.get_or_create_stream("stream1");
        stream.last_event_at = Some(ts(96_400)); // 100_000 - 96_400 = 3600s = TTL -> kept
    }

    // Entity with recent last_event_at (1 minute ago)
    {
        let mut entity = store.get_or_create_entity("recent_user");
        let stream = entity.get_or_create_stream("stream1");
        stream.last_event_at = Some(ts(99_940)); // 100_000 - 99_940 = 60s < 3600s TTL -> kept
    }

    let now = ts(100_000);
    // TTL = 2 * 1800 = 3600 seconds (1 hour)
    // old_user: 3601s old > 3600s TTL -> evicted
    // boundary_user: 3600s old = 3600s TTL -> kept (at boundary)
    // recent_user: 60s old < 3600s TTL -> kept
    let evicted = evict_expired_keys(&store, &engine, now, 2);
    assert_eq!(evicted, 1);
    assert!(store.get_entity("old_user").is_none());
    assert!(store.get_entity("boundary_user").is_some());
    assert!(store.get_entity("recent_user").is_some());
}

#[test]
fn test_eviction_preserves_entity_with_no_events() {
    let store = StateStore::new();
    let mut engine = PipelineEngine::new();
    engine
        .register(StreamDefinition {
            name: "stream1".into(),
            key_field: Some("user_id".into()),
            group_by_keys: None,
            features: vec![(
                "count".into(),
                FeatureDef::Count {
                    window: Duration::from_secs(1800),
                    bucket: Duration::from_secs(60),
                    where_expr: None,
                    backfill: false,
                },
            )],
            depends_on: None,
            filter: None,
            entity_ttl: None,
            history_ttl: None,
            projection: None,
            ephemeral: None,
            pipeline_ttl: None,
            max_keys: None,
        watermark_lateness: None,
        })
        .unwrap();

    // Entity with a stream but no last_event_at (never received event)
    {
        let mut entity = store.get_or_create_entity("no_event_user");
        entity.get_or_create_stream("stream1"); // has a stream entry, so not empty
    }

    let now = ts(100_000);
    let evicted = evict_expired_keys(&store, &engine, now, 2);
    assert_eq!(evicted, 0);
    assert!(store.get_entity("no_event_user").is_some());
}

// ======================== Atomic Write Pattern ========================

#[test]
fn test_snapshot_atomic_write() {
    let dir = tempfile::tempdir().expect("failed to create temp dir");
    let final_path = dir.path().join("beava.snapshot");
    let tmp_path = final_path.with_extension("tmp");

    // Build a snapshot
    let snapshot = SnapshotState {
        entities: vec![],
        pipelines: vec![],
        backfill_complete: vec![],
    };
    let bytes = save_snapshot(&snapshot).expect("save_snapshot should succeed");

    // Mimic the atomic rename pattern from main.rs
    std::fs::write(&tmp_path, &bytes).expect("write tmp");
    std::fs::rename(&tmp_path, &final_path).expect("rename");

    // Assert final file exists and tmp file does not
    assert!(final_path.exists(), "final snapshot file should exist");
    assert!(
        !tmp_path.exists(),
        ".tmp file should not exist after rename"
    );

    // Assert load_snapshot works on the final file
    let loaded_bytes = std::fs::read(&final_path).expect("read final");
    assert!(load_snapshot(&loaded_bytes).is_some());
}
