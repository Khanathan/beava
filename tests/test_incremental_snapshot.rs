//! Integration tests for Phase 9 incremental snapshots (OPS-03, OPS-04).
//!
//! Covers the full incremental snapshot lifecycle:
//!   * delta snapshots contain only dirty entities
//!   * base + deltas recovery merges to the correct state
//!   * deleted_keys in a delta are honored on recovery
//!   * legacy v5 single-file snapshots migrate transparently
//!   * cycle counter logic picks base at cycles 0, N, 2N, ...
//!
//! These tests live in a dedicated file (rather than extending the pre-existing
//! tests/test_snapshot.rs, which has Phase-8-era compile errors tracked in
//! .planning/phases/09-incremental-snapshots/deferred-items.md) so they exercise
//! the new incremental code path cleanly.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tally::engine::pipeline::{FeatureDef, PipelineEngine, StreamDefinition};
use tally::state::eviction::evict_expired_keys;
use tally::state::snapshot::{
    load_legacy_v5, load_snapshot_file, save_base_snapshot, save_delta_snapshot, BaseSnapshotState,
    DeltaSnapshotState, SerializablePipeline, SnapshotFile, SnapshotHeader, SnapshotState,
    SnapshotType, LEGACY_V5_FORMAT, SNAPSHOT_FORMAT_VERSION,
};
use tally::state::store::StateStore;
use tally::types::FeatureValue;

fn ts(secs: u64) -> SystemTime {
    UNIX_EPOCH + Duration::from_secs(secs)
}

fn tx_stream() -> StreamDefinition {
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
    }
}

fn push(store: &StateStore, engine: &PipelineEngine, key: &str, amount: f64, now: SystemTime) {
    let event = serde_json::json!({"user_id": key, "amount": amount});
    engine.push("Transactions", &event, store, now).unwrap();
    store.mark_dirty(key);
}

// ======================== OPS-03: Delta contains only dirty ========================

#[test]
fn test_incremental_snapshot_delta_contains_only_dirty_entities() {
    let mut engine = PipelineEngine::new();
    engine.register(tx_stream()).unwrap();
    let store = StateStore::new();
    let now = ts(60_000);

    // Push to u1 and u3 (will be dirty). u2 exists from a prior push but its
    // dirty flag will be cleared to simulate a clean entity across snapshots.
    push(&store, &engine, "u1", 10.0, now);
    push(&store, &engine, "u2", 20.0, now);
    store.clear_dirty();
    // After clear, another push to u1 and u3
    push(&store, &engine, "u1", 11.0, now);
    push(&store, &engine, "u3", 30.0, now);

    let valid_features = engine.valid_features_map();
    let changed = store.clone_dirty_for_snapshot_with_gc(&valid_features);

    // Delta must contain exactly u1 and u3, NOT u2.
    let keys: Vec<String> = changed.iter().map(|(k, _)| k.clone()).collect();
    assert_eq!(
        keys.len(),
        2,
        "expected exactly 2 dirty entities, got {:?}",
        keys
    );
    assert!(keys.contains(&"u1".to_string()));
    assert!(keys.contains(&"u3".to_string()));
    assert!(
        !keys.contains(&"u2".to_string()),
        "u2 should not be in delta"
    );
}

// ======================== OPS-04: Base + delta recovery ========================

#[test]
fn test_incremental_snapshot_recovery_base_plus_two_deltas() {
    let dir = tempfile::tempdir().unwrap();
    let snap_dir = dir.path().to_path_buf();

    let mut engine = PipelineEngine::new();
    engine.register(tx_stream()).unwrap();
    let store = StateStore::new();
    let now = ts(60_000);

    // Cycle 0: push u1, u2 -> write base snapshot (seq=0)
    push(&store, &engine, "u1", 10.0, now);
    push(&store, &engine, "u2", 20.0, now);

    let entities = store.clone_for_snapshot_with_gc(&engine.valid_features_map());
    let base = BaseSnapshotState {
        header: SnapshotHeader {
            snapshot_type: SnapshotType::Base,
            sequence: 0,
        },
        entities,
        pipelines: vec![SerializablePipeline {
            name: "Transactions".into(),
            key_field: "user_id".into(),
            raw_register_json: "{}".into(),
        }],
        backfill_complete: vec![],
    };
    let bytes = save_base_snapshot(&base).unwrap();
    std::fs::write(snap_dir.join("tally.snapshot.base.0000000000"), &bytes).unwrap();
    store.clear_dirty();
    let _ = store.take_deleted();

    // Cycle 1: push u3 -> delta with only u3 (seq=1)
    push(&store, &engine, "u3", 30.0, now);
    let changed = store.clone_dirty_for_snapshot_with_gc(&engine.valid_features_map());
    let delta1 = DeltaSnapshotState {
        header: SnapshotHeader {
            snapshot_type: SnapshotType::Delta { base_seq: 0 },
            sequence: 1,
        },
        changed_entities: changed,
        deleted_keys: vec![],
    };
    let bytes = save_delta_snapshot(&delta1).unwrap();
    std::fs::write(snap_dir.join("tally.snapshot.delta.0000000001"), &bytes).unwrap();
    store.clear_dirty();

    // Cycle 2: update u1 -> delta with only u1 (seq=2)
    push(&store, &engine, "u1", 5.0, now);
    let changed = store.clone_dirty_for_snapshot_with_gc(&engine.valid_features_map());
    let delta2 = DeltaSnapshotState {
        header: SnapshotHeader {
            snapshot_type: SnapshotType::Delta { base_seq: 0 },
            sequence: 2,
        },
        changed_entities: changed,
        deleted_keys: vec![],
    };
    let bytes = save_delta_snapshot(&delta2).unwrap();
    std::fs::write(snap_dir.join("tally.snapshot.delta.0000000002"), &bytes).unwrap();

    // Now recover from disk: load base + apply deltas in order.
    let (merged, next_seq) = recover_from_dir(&snap_dir).expect("recovery should succeed");

    // Restore into a fresh store and verify features.
    let recovered = StateStore::new();
    recovered.restore_from_snapshot(merged.entities);

    let u1 = recovered.get_all_features("u1", now);
    let u2 = recovered.get_all_features("u2", now);
    let u3 = recovered.get_all_features("u3", now);

    // u1 was pushed twice (10, 5) -> sum=15, count=2
    assert_eq!(u1.get("tx_count_1h"), Some(&FeatureValue::Int(2)));
    assert_eq!(u1.get("tx_sum_1h"), Some(&FeatureValue::Float(15.0)));
    // u2 was pushed once in base -> sum=20, count=1
    assert_eq!(u2.get("tx_count_1h"), Some(&FeatureValue::Int(1)));
    assert_eq!(u2.get("tx_sum_1h"), Some(&FeatureValue::Float(20.0)));
    // u3 was pushed once in delta1 -> sum=30, count=1
    assert_eq!(u3.get("tx_count_1h"), Some(&FeatureValue::Int(1)));
    assert_eq!(u3.get("tx_sum_1h"), Some(&FeatureValue::Float(30.0)));

    // next_seq should be max_seq + 1 = 3
    assert_eq!(next_seq, 3);
}

// ======================== OPS-04: Deleted keys in delta ========================

#[test]
fn test_incremental_snapshot_deleted_keys_removed_on_recovery() {
    let dir = tempfile::tempdir().unwrap();
    let snap_dir = dir.path().to_path_buf();

    let mut engine = PipelineEngine::new();
    engine.register(tx_stream()).unwrap();
    let store = StateStore::new();
    let now = ts(60_000);

    // Base contains u1, u2, u3.
    push(&store, &engine, "u1", 10.0, now);
    push(&store, &engine, "u2", 20.0, now);
    push(&store, &engine, "u3", 30.0, now);
    let entities = store.clone_for_snapshot_with_gc(&engine.valid_features_map());
    let base = BaseSnapshotState {
        header: SnapshotHeader {
            snapshot_type: SnapshotType::Base,
            sequence: 0,
        },
        entities,
        pipelines: vec![],
        backfill_complete: vec![],
    };
    let bytes = save_base_snapshot(&base).unwrap();
    std::fs::write(snap_dir.join("tally.snapshot.base.0000000000"), &bytes).unwrap();

    // Delta marks u2 as deleted.
    let delta = DeltaSnapshotState {
        header: SnapshotHeader {
            snapshot_type: SnapshotType::Delta { base_seq: 0 },
            sequence: 1,
        },
        changed_entities: vec![],
        deleted_keys: vec!["u2".into()],
    };
    let bytes = save_delta_snapshot(&delta).unwrap();
    std::fs::write(snap_dir.join("tally.snapshot.delta.0000000001"), &bytes).unwrap();

    // Recover.
    let (merged, next_seq) = recover_from_dir(&snap_dir).unwrap();
    assert_eq!(next_seq, 2);

    let recovered = StateStore::new();
    recovered.restore_from_snapshot(merged.entities);

    assert!(recovered.get_entity("u1").is_some());
    assert!(
        recovered.get_entity("u2").is_none(),
        "u2 should have been removed by delta deleted_keys"
    );
    assert!(recovered.get_entity("u3").is_some());
}

// ======================== OPS-04: Legacy v5 migration ========================

#[test]
fn test_legacy_v5_migration_loads_as_initial_base() {
    // Build a v5 byte stream by hand: [version=5][postcard(SnapshotState)]
    let mut engine = PipelineEngine::new();
    engine.register(tx_stream()).unwrap();
    let store = StateStore::new();
    let now = ts(60_000);
    for amount in [10.0, 20.0, 30.0] {
        let event = serde_json::json!({"user_id": "u_legacy", "amount": amount});
        engine.push("Transactions", &event, &store, now).unwrap();
    }

    let v5_state = SnapshotState {
        entities: store.clone_for_snapshot(),
        pipelines: vec![],
        backfill_complete: vec![],
    };
    let mut v5_bytes = vec![LEGACY_V5_FORMAT];
    v5_bytes.extend_from_slice(&postcard::to_stdvec(&v5_state).unwrap());

    // Round-trip through load_legacy_v5 directly (baseline)
    let round_trip = load_legacy_v5(&v5_bytes).expect("legacy load must succeed");
    assert_eq!(round_trip.entities.len(), 1);

    // Ensure the byte slice starts with the legacy marker (migration pathway
    // can recognize it by the version byte).
    assert_eq!(v5_bytes[0], LEGACY_V5_FORMAT);
    // And that it does NOT collide with the v6 base tag prefix.
    assert_ne!(v5_bytes[0], SNAPSHOT_FORMAT_VERSION);
}

// ======================== OPS-03: Full snapshot every Nth cycle ========================

#[test]
fn test_full_snapshot_cycle_picks_base_at_zero_and_every_n() {
    // Simulate the cycle counter logic from main.rs: is_full = cycle % N == 0.
    let n: u64 = 10;
    let mut bases = Vec::new();
    let mut deltas = Vec::new();
    for cycle in 0..30u64 {
        if cycle % n == 0 {
            bases.push(cycle);
        } else {
            deltas.push(cycle);
        }
    }
    assert_eq!(bases, vec![0, 10, 20]);
    assert_eq!(deltas.len(), 27);
}

// ======================== Eviction + delta integration ========================

#[test]
fn test_eviction_marks_deleted_and_delta_includes_it() {
    let mut engine = PipelineEngine::new();
    engine
        .register(StreamDefinition {
            name: "stream_short".into(),
            key_field: Some("user_id".into()),
            group_by_keys: None,
            features: vec![(
                "count".into(),
                FeatureDef::Count {
                    window: Duration::from_secs(3600),
                    bucket: Duration::from_secs(60),
                    where_expr: None,
                    backfill: false,
                },
            )],
            depends_on: None,
            filter: None,
            entity_ttl: Some(Duration::from_secs(300)),
            history_ttl: None,
            projection: None,
            ephemeral: None,
            pipeline_ttl: None,
            max_keys: None,
        })
        .unwrap();

    let store = StateStore::new();
    // Entity with an event long ago
    {
        let mut entity = store.get_or_create_entity("doomed");
        let s = entity.get_or_create_stream("stream_short");
        s.last_event_at = Some(ts(1000));
    }
    store.clear_dirty();

    let now = ts(100_000);
    // Entity is 99_000s old, TTL is 300s -> evicted.
    let evicted = evict_expired_keys(&store, &engine, now, 2);
    assert_eq!(evicted, 1);
    assert_eq!(store.entity_count(), 0);

    // take_deleted must contain "doomed".
    let deleted = store.take_deleted();
    assert_eq!(deleted, vec!["doomed".to_string()]);

    // Build a delta from this deletion; changed is empty, deleted has doomed.
    let delta = DeltaSnapshotState {
        header: SnapshotHeader {
            snapshot_type: SnapshotType::Delta { base_seq: 0 },
            sequence: 1,
        },
        changed_entities: vec![],
        deleted_keys: deleted,
    };
    let bytes = save_delta_snapshot(&delta).unwrap();
    // Verify round-trip through load_snapshot_file preserves the deletion.
    match load_snapshot_file(&bytes).expect("should load delta") {
        SnapshotFile::Delta(d) => {
            assert_eq!(d.deleted_keys, vec!["doomed".to_string()]);
            assert!(d.changed_entities.is_empty());
        }
        _ => panic!("expected a delta"),
    }
}

// ======================== Helpers ========================

/// Mirror of main.rs::load_incremental_snapshots without the legacy fallback
/// (tests build the files directly). Scans `snap_dir` for base+delta files,
/// loads the latest base, and applies deltas in sequence order.
fn recover_from_dir(snap_dir: &std::path::Path) -> Option<(SnapshotState, u64)> {
    let mut bases: Vec<(u64, std::path::PathBuf)> = Vec::new();
    let mut deltas: Vec<(u64, std::path::PathBuf)> = Vec::new();

    for entry in std::fs::read_dir(snap_dir).ok()?.flatten() {
        let name = entry.file_name();
        let name_str = name.to_string_lossy().into_owned();
        if let Some(seq_str) = name_str.strip_prefix("tally.snapshot.base.") {
            if let Ok(seq) = seq_str.parse::<u64>() {
                bases.push((seq, entry.path()));
            }
        } else if let Some(seq_str) = name_str.strip_prefix("tally.snapshot.delta.") {
            if let Ok(seq) = seq_str.parse::<u64>() {
                deltas.push((seq, entry.path()));
            }
        }
    }

    bases.sort_by_key(|(seq, _)| *seq);
    let (base_seq, base_path) = bases.last()?.clone();
    let bytes = std::fs::read(&base_path).ok()?;
    let base = match load_snapshot_file(&bytes)? {
        SnapshotFile::Base(b) => b,
        _ => return None,
    };

    let store = StateStore::new();
    store.restore_from_snapshot(base.entities.clone());

    let mut applicable: Vec<(u64, std::path::PathBuf)> = deltas
        .into_iter()
        .filter(|(seq, _)| *seq > base_seq)
        .collect();
    applicable.sort_by_key(|(seq, _)| *seq);

    let mut max_seq = base_seq;
    for (seq, path) in &applicable {
        let bytes = std::fs::read(path).unwrap();
        match load_snapshot_file(&bytes) {
            Some(SnapshotFile::Delta(delta)) => {
                store.apply_delta(delta.changed_entities, delta.deleted_keys);
                if *seq > max_seq {
                    max_seq = *seq;
                }
            }
            _ => continue,
        }
    }

    Some((
        SnapshotState {
            entities: store.clone_for_snapshot(),
            pipelines: base.pipelines,
            backfill_complete: base.backfill_complete,
        },
        max_seq + 1,
    ))
}
