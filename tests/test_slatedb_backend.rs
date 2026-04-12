//! Integration test for SlateDB persistence backend.
//! Only compiled when the `slatedb-backend` feature is enabled.
#![cfg(feature = "slatedb-backend")]

use std::path::Path;
use std::time::{Duration, UNIX_EPOCH};

use tally::state::persistence::{PersistenceBackend, RestoredState};
use tally::state::slate_backend::SlateBackend;
use tally::state::snapshot::{
    BaseSnapshotState, DeltaSnapshotState, SerializableEntityState,
    SerializableStreamEntityState, SerializablePipeline, SnapshotHeader, SnapshotType,
};
use tally::state::store::StaticFeature;
use tally::types::FeatureValue;

fn make_test_entity(name: &str, value: f64) -> (String, SerializableEntityState) {
    (
        name.to_string(),
        SerializableEntityState {
            streams: vec![],
            static_features: vec![(
                "f1".to_string(),
                StaticFeature {
                    value: FeatureValue::Float(value),
                    updated_at: UNIX_EPOCH + Duration::from_secs(1000),
                },
            )],
        },
    )
}

fn make_pipeline(name: &str) -> SerializablePipeline {
    SerializablePipeline {
        name: name.to_string(),
        key_field: "user_id".to_string(),
        raw_register_json: format!(r#"{{"name":"{}","key":"user_id","features":{{}}}}"#, name),
    }
}

#[tokio::test]
async fn test_slatedb_persist_base_and_restore() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("slatedb_test");

    // Open, persist, close, reopen, restore.
    {
        let backend = SlateBackend::open(db_path.to_str().unwrap()).await.unwrap();

        let base = BaseSnapshotState {
            header: SnapshotHeader {
                snapshot_type: SnapshotType::Base,
                sequence: 5,
            },
            entities: vec![
                make_test_entity("user:1", 100.0),
                make_test_entity("user:2", 200.0),
            ],
            pipelines: vec![make_pipeline("transactions")],
            backfill_complete: vec![("transactions".to_string(), "tx_count".to_string())],
        };

        let size = backend.persist_base(&base, dir.path()).unwrap();
        assert!(size > 0, "persist_base should write bytes");

        // Restore from the same open DB.
        let restored = backend
            .restore(dir.path(), &dir.path().join("legacy"))
            .expect("restore should succeed");
        assert_eq!(restored.entities.len(), 2);
        assert_eq!(restored.pipelines.len(), 1);
        assert_eq!(restored.pipelines[0].name, "transactions");
        assert_eq!(restored.backfill_complete.len(), 1);
        assert_eq!(restored.next_seq, 6);

        // Verify entity values.
        let entity_map: std::collections::HashMap<&str, &SerializableEntityState> = restored
            .entities
            .iter()
            .map(|(k, v)| (k.as_str(), v))
            .collect();
        assert!(entity_map.contains_key("user:1"));
        assert!(entity_map.contains_key("user:2"));

        backend.close().await;
    }

    // Reopen and verify persistence across close/open.
    {
        let backend = SlateBackend::open(db_path.to_str().unwrap()).await.unwrap();
        let restored = backend
            .restore(dir.path(), &dir.path().join("legacy"))
            .expect("restore after reopen should succeed");
        assert_eq!(restored.entities.len(), 2);
        assert_eq!(restored.next_seq, 6);
        backend.close().await;
    }
}

#[tokio::test]
async fn test_slatedb_persist_delta() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("slatedb_delta");

    let backend = SlateBackend::open(db_path.to_str().unwrap()).await.unwrap();

    // Base with 2 entities.
    let base = BaseSnapshotState {
        header: SnapshotHeader {
            snapshot_type: SnapshotType::Base,
            sequence: 0,
        },
        entities: vec![
            make_test_entity("key1", 10.0),
            make_test_entity("key2", 20.0),
        ],
        pipelines: vec![],
        backfill_complete: vec![],
    };
    backend.persist_base(&base, dir.path()).unwrap();

    // Delta: add key3, delete key1.
    let delta = DeltaSnapshotState {
        header: SnapshotHeader {
            snapshot_type: SnapshotType::Delta { base_seq: 0 },
            sequence: 1,
        },
        changed_entities: vec![make_test_entity("key3", 30.0)],
        deleted_keys: vec!["key1".to_string()],
    };
    backend.persist_delta(&delta, dir.path()).unwrap();

    // Restore: should have key2 and key3, not key1.
    let restored = backend
        .restore(dir.path(), &dir.path().join("legacy"))
        .expect("restore should succeed");

    let keys: Vec<&str> = restored.entities.iter().map(|(k, _)| k.as_str()).collect();
    assert!(keys.contains(&"key2"), "key2 should be present");
    assert!(keys.contains(&"key3"), "key3 should be present");
    assert!(!keys.contains(&"key1"), "key1 should be deleted");
    assert_eq!(restored.next_seq, 2);

    backend.close().await;
}

#[tokio::test]
async fn test_slatedb_fresh_db_returns_none() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("slatedb_fresh");

    let backend = SlateBackend::open(db_path.to_str().unwrap()).await.unwrap();
    let restored = backend.restore(dir.path(), &dir.path().join("legacy"));
    assert!(restored.is_none(), "fresh DB should return None on restore");

    backend.close().await;
}

#[tokio::test]
async fn test_slatedb_backend_name() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("slatedb_name");

    let backend = SlateBackend::open(db_path.to_str().unwrap()).await.unwrap();
    assert_eq!(backend.name(), "slatedb");
    backend.close().await;
}
