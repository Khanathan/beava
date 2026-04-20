//! Phase 52-01: Snapshot v8 migration tests.
//!
//! Verifies:
//! 1. A frozen v7 binary fixture loads with shard_count defaulted to 1 and replica_lsn_map empty.
//! 2. A v8 snapshot round-trips all fields (entities, pipelines, shard_count, replica_lsn_map).
//! 3. v7 bytes passed to the v8 loader promote cleanly: entities intact, shard_count=1.
//! 4. A v8 snapshot with shard_count=3 but BEAVA_SHARDS=8 triggers hard-fail with exact error string.
//! 5. A v8 snapshot with shard_count=1 and BEAVA_SHARDS=1 succeeds (guard skipped).
//! 6. A v7 snapshot (shard_count promoted to 1) with BEAVA_SHARDS=1 succeeds.
//! 7. A v7 snapshot with BEAVA_SHARDS=8 fails — after promotion shard_count=1 ≠ 8.

use std::collections::HashMap;
use std::time::{Duration, UNIX_EPOCH};

use beava::engine::operators::CountOp;
use beava::state::snapshot::{
    load_snapshot_file, save_base_snapshot_v7_for_test, save_base_snapshot_v8, BaseSnapshotStateV7,
    BaseSnapshotStateV8, OperatorState, SerializableEntityState, SerializableStreamEntityState,
    SnapshotFile, SnapshotHeader, SnapshotType, LEGACY_V7_FORMAT, SNAPSHOT_FORMAT_VERSION,
};
use beava::state::store::check_shard_count_guard;
use beava::types::FeatureValue;

fn ts(secs: u64) -> std::time::SystemTime {
    UNIX_EPOCH + Duration::from_secs(secs)
}

fn sample_entity(count: u64, now: std::time::SystemTime) -> (String, SerializableEntityState) {
    let mut op = OperatorState::Count(CountOp::new(
        Duration::from_secs(3600),
        Duration::from_secs(60),
    ));
    for _ in 0..count {
        op.push(&serde_json::json!({}), None, now).unwrap();
    }
    (
        format!("u{}", count),
        SerializableEntityState {
            streams: vec![(
                "Transactions".to_string(),
                SerializableStreamEntityState {
                    operators: vec![("tx_count_1h".to_string(), op)],
                    last_event_at: Some(now),
                },
            )],
            static_features: vec![],
            table_rows: vec![],
        },
    )
}

// ======================== Test 1: v7 fixture loads with shard_count=1 ========================

#[test]
fn test_snapshot_v8_v7_fixture_loads_with_shard_count_1() {
    // Load the frozen v7 binary fixture from disk.
    let fixture_path = std::path::Path::new("tests/fixtures/snapshot_v7_sample.bin");
    let bytes = std::fs::read(fixture_path).expect("v7 fixture must exist at tests/fixtures/snapshot_v7_sample.bin");

    // v7 fixture must start with version byte 7.
    assert_eq!(bytes[0], LEGACY_V7_FORMAT, "fixture must start with v7 version byte");

    let file = load_snapshot_file(&bytes).expect("v7 fixture must load cleanly");
    match file {
        SnapshotFile::Base(snap) => {
            // v7 → v8 promotion: shard_count defaults to 1.
            assert_eq!(snap.shard_count, 1, "v7 snapshot must promote with shard_count=1");
            // replica_lsn_map defaults to empty.
            assert!(snap.replica_lsn_map.is_empty(), "v7 snapshot must promote with empty replica_lsn_map");
            // Entities are intact.
            assert_eq!(snap.entities.len(), 1, "fixture entity must be preserved");
            assert_eq!(snap.entities[0].0, "u3");
        }
        SnapshotFile::Delta(_) => panic!("expected Base snapshot from fixture"),
    }
}

// ======================== Test 2: v8 round-trip ========================

#[test]
fn test_snapshot_v8_roundtrip() {
    let now = ts(60_000);
    let (key, entity) = sample_entity(3, now);

    let mut lsn_map = HashMap::new();
    lsn_map.insert(("MyStream".to_string(), 0u8), 42u64);
    lsn_map.insert(("OtherStream".to_string(), 2u8), 999u64);

    let snap_v8 = BaseSnapshotStateV8 {
        header: SnapshotHeader {
            snapshot_type: SnapshotType::Base,
            sequence: 100,
            schema_version: 9,
        },
        entities: vec![(key.clone(), entity)],
        pipelines: vec![],
        backfill_complete: vec![("Transactions".to_string(), "tx_count_1h".to_string())],
        shard_count: 4,
        replica_lsn_map: lsn_map,
    };

    let bytes = save_base_snapshot_v8(&snap_v8).expect("v8 save must succeed");
    // Phase 55-03: writer now emits V9_FORMAT (0x09) outer byte. The v8 body
    // type (BaseSnapshotStateV8) is unchanged — only the outer byte and the
    // embedded schema_version field differ. v8 bytes on disk are still
    // accepted by the reader (serde-default schema_version=8 triggers
    // rematerialization at boot).
    assert_eq!(bytes[0], SNAPSHOT_FORMAT_VERSION, "writer emits current format version");
    assert_eq!(bytes[0], 0x09, "Phase 55-03: writes advertise V9_FORMAT");

    let file = load_snapshot_file(&bytes).expect("v8 must load cleanly");
    match file {
        SnapshotFile::Base(restored) => {
            assert_eq!(restored.header.sequence, 100);
            assert_eq!(restored.shard_count, 4, "shard_count must round-trip");
            assert_eq!(restored.replica_lsn_map.len(), 2, "replica_lsn_map must round-trip");
            assert_eq!(
                restored.replica_lsn_map.get(&("MyStream".to_string(), 0u8)),
                Some(&42u64)
            );
            assert_eq!(
                restored.replica_lsn_map.get(&("OtherStream".to_string(), 2u8)),
                Some(&999u64)
            );
            assert_eq!(restored.entities.len(), 1);
            assert_eq!(restored.entities[0].0, key);
            assert_eq!(restored.backfill_complete.len(), 1);
            // Verify operator state survived.
            let mut op = restored.entities[0].1.streams[0].1.operators[0].1.clone();
            assert_eq!(op.read(now), FeatureValue::Int(3));
        }
        SnapshotFile::Delta(_) => panic!("expected Base"),
    }
}

// ======================== Test 3: v7 bytes → v8 promotion (entities intact) ========================

#[test]
fn test_snapshot_v8_v7_to_v8_promotion_entities_intact() {
    let now = ts(60_000);
    let (key, entity) = sample_entity(5, now);

    let v7_snap = BaseSnapshotStateV7 {
        header: SnapshotHeader {
            snapshot_type: SnapshotType::Base,
            sequence: 7,
            schema_version: 9,
        },
        entities: vec![(key.clone(), entity)],
        pipelines: vec![],
        backfill_complete: vec![],
    };

    let bytes = save_base_snapshot_v7_for_test(&v7_snap).expect("v7 save must succeed");
    assert_eq!(bytes[0], LEGACY_V7_FORMAT, "v7 test helper must write version byte 7");

    let file = load_snapshot_file(&bytes).expect("v7 bytes must load via v8 dispatch");
    match file {
        SnapshotFile::Base(snap) => {
            assert_eq!(snap.shard_count, 1, "v7 promoted shard_count must be 1");
            assert!(snap.replica_lsn_map.is_empty());
            assert_eq!(snap.entities.len(), 1);
            assert_eq!(snap.entities[0].0, key);
            let mut op = snap.entities[0].1.streams[0].1.operators[0].1.clone();
            assert_eq!(op.read(now), FeatureValue::Int(5));
        }
        SnapshotFile::Delta(_) => panic!("expected Base"),
    }
}

// ======================== Test 4: shard_count mismatch triggers hard-fail ========================

#[test]
fn test_snapshot_v8_shard_count_mismatch_hard_fail() {
    // check_shard_count_guard(snapshot_shard_count, env_beava_shards)
    // BEAVA_SHARDS=8, snapshot has shard_count=3 → must return Err with exact string.
    let result = check_shard_count_guard(3, 8);
    assert!(result.is_err(), "mismatched shard counts must return Err");
    let err = result.unwrap_err();
    assert_eq!(
        err,
        "snapshot shard_count=3 but BEAVA_SHARDS=8 \u{2014} run 'tally reshard --from 3 --to 8' then restart",
        "error string must match TPC-CORR-02 exactly"
    );
}

// ======================== Test 5: matching shard_count=1/BEAVA_SHARDS=1 succeeds ========================

#[test]
fn test_snapshot_v8_matching_shard_count_succeeds() {
    let result = check_shard_count_guard(1, 1);
    assert!(result.is_ok(), "matching shard_count=1 and BEAVA_SHARDS=1 must succeed");
}

// ======================== Test 6: v7 promoted to shard_count=1, BEAVA_SHARDS=1 → ok ========================

#[test]
fn test_snapshot_v8_v7_promoted_shard_count_1_matches_beava_shards_1() {
    // v7 snapshot is promoted to shard_count=1. If BEAVA_SHARDS=1, no error.
    let now = ts(60_000);
    let (key, entity) = sample_entity(2, now);
    let v7_snap = BaseSnapshotStateV7 {
        header: SnapshotHeader {
            snapshot_type: SnapshotType::Base,
            sequence: 1,
            schema_version: 9,
        },
        entities: vec![(key.clone(), entity)],
        pipelines: vec![],
        backfill_complete: vec![],
    };
    let bytes = save_base_snapshot_v7_for_test(&v7_snap).expect("v7 save");
    let file = load_snapshot_file(&bytes).expect("v7 loads");
    match file {
        SnapshotFile::Base(snap) => {
            assert_eq!(snap.shard_count, 1);
            // Guard: BEAVA_SHARDS=1, shard_count=1 → success.
            let result = check_shard_count_guard(snap.shard_count, 1);
            assert!(result.is_ok(), "v7 promoted shard_count=1 with BEAVA_SHARDS=1 must not block boot");
        }
        SnapshotFile::Delta(_) => panic!(),
    }
}

// ======================== Test 7: v7 promoted with BEAVA_SHARDS=8 → guard triggers ========================

#[test]
fn test_snapshot_v8_v7_promoted_shard_count_1_mismatch_beava_shards_8() {
    // v7 snapshot promotes to shard_count=1. With BEAVA_SHARDS=8, guard must fire.
    let now = ts(60_000);
    let (key, entity) = sample_entity(1, now);
    let v7_snap = BaseSnapshotStateV7 {
        header: SnapshotHeader {
            snapshot_type: SnapshotType::Base,
            sequence: 1,
            schema_version: 9,
        },
        entities: vec![(key.clone(), entity)],
        pipelines: vec![],
        backfill_complete: vec![],
    };
    let bytes = save_base_snapshot_v7_for_test(&v7_snap).expect("v7 save");
    let file = load_snapshot_file(&bytes).expect("v7 loads");
    match file {
        SnapshotFile::Base(snap) => {
            assert_eq!(snap.shard_count, 1);
            // Guard: BEAVA_SHARDS=8, shard_count=1 → hard fail.
            let result = check_shard_count_guard(snap.shard_count, 8);
            assert!(result.is_err(), "v7 shard_count=1 vs BEAVA_SHARDS=8 must trigger guard");
            let err = result.unwrap_err();
            assert_eq!(
                err,
                "snapshot shard_count=1 but BEAVA_SHARDS=8 \u{2014} run 'tally reshard --from 1 --to 8' then restart"
            );
        }
        SnapshotFile::Delta(_) => panic!(),
    }
}

// ======================== Verify SNAPSHOT_FORMAT_VERSION is 9 (Phase 55-03) ========================

#[test]
fn test_snapshot_v8_format_version_is_9() {
    // Phase 55-03: SNAPSHOT_FORMAT_VERSION bumped 8 → 9 alongside the
    // SnapshotHeader.schema_version field addition. v8 bytes are STILL
    // accepted by the reader (via wire-compat shim → schema_version=8 →
    // triggers boot rematerialization); only the writer changed.
    assert_eq!(
        SNAPSHOT_FORMAT_VERSION, 9,
        "SNAPSHOT_FORMAT_VERSION must be 9 after Phase 55-03 v9 bump"
    );
}

// ======================== Verify LEGACY_V7_FORMAT constant is 7 ========================

#[test]
fn test_snapshot_v8_legacy_v7_format_constant() {
    assert_eq!(LEGACY_V7_FORMAT, 7);
}

// ======================== Fixture generator (run once, output committed) ========================

/// Generate the frozen v7 binary fixture at `tests/fixtures/snapshot_v7_sample.bin`.
///
/// Run with: `cargo test -p beava generate_v7_fixture -- --include-ignored --nocapture`
///
/// The fixture is a valid v7 snapshot containing one entity ("u3") with 3 events
/// pushed to a CountOp in the "Transactions" stream. Committed as a binary blob;
/// loaded by `test_snapshot_v8_v7_fixture_loads_with_shard_count_1` to prove the
/// v7→v8 promotion path works against a frozen binary artifact.
#[test]
#[ignore]
fn generate_v7_fixture() {
    use beava::state::snapshot::save_base_snapshot_v7_for_test;
    let now = ts(60_000);
    let (key, entity) = sample_entity(3, now);

    let v7_snap = BaseSnapshotStateV7 {
        header: SnapshotHeader {
            snapshot_type: SnapshotType::Base,
            sequence: 1,
            schema_version: 9,
        },
        entities: vec![(key, entity)],
        pipelines: vec![],
        backfill_complete: vec![],
    };

    let bytes = save_base_snapshot_v7_for_test(&v7_snap).expect("v7 save");
    assert_eq!(bytes[0], LEGACY_V7_FORMAT);

    // Create the fixtures directory if it doesn't exist.
    std::fs::create_dir_all("tests/fixtures").expect("create fixtures dir");
    std::fs::write("tests/fixtures/snapshot_v7_sample.bin", &bytes)
        .expect("write v7 fixture");
    println!("Wrote {} bytes to tests/fixtures/snapshot_v7_sample.bin", bytes.len());
}
