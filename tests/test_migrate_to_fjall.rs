//! Phase 53 Plan 04 — TDD RED tests for `tally migrate-to-fjall`.
//!
//! 7 integration tests covering:
//!   1. fresh migration → entities land in fjall partitions matching production routing
//!   2. idempotent second run → no-op without `--force`
//!   3. resume-from-marker → only missing entities migrate
//!   4. `snapshot.v8.bak` preserved unless `--replace`
//!   5. `--replace` deletes bak on success
//!   6. fs2 lock contention → `Err(WouldBlock)`
//!   7. force flag re-migrates over existing fjall
//!
//! Plus (W-2 parity) test 8 — per-stream `shard_key` routing matches production
//! for 3 streams with different key_fields (single-field + keyless).
//!
//! RED invariant: none of the imports resolve yet (module `beava::migrate_to_fjall`
//! does not exist). Task 2's GREEN commit will make them resolve and the tests pass.

#![cfg(not(feature = "state-inmem"))]

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};

use tempfile::TempDir;

// -----------------------------------------------------------------------------
// Module under test — DOES NOT EXIST YET (Task 2 GREEN).
// -----------------------------------------------------------------------------
use beava::migrate_to_fjall::{
    is_migrate_subcommand, migrate_to_fjall, parse_migrate_args, MigrationReport,
};

use beava::routing::shard_hint::shard_hint_for_event;
use beava::shard::fjall_backend::{
    fjall_config_from_env, open_keyspace_from_env, open_shard_partition,
};
use beava::state::snapshot::{
    save_base_snapshot_v8, BaseSnapshotStateV8, SerializableEntityState, SerializablePipeline,
    SerializableStreamEntityState, SnapshotHeader, SnapshotType,
};

// -----------------------------------------------------------------------------
// Env-mutation guard. All tests share one `BEAVA_FJALL_*` process-global lock.
// -----------------------------------------------------------------------------

fn env_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

fn set_determinism_env() {
    std::env::set_var("BEAVA_FJALL_FSYNC_DISABLE", "1");
    std::env::set_var("BEAVA_FJALL_CACHE_MB", "32");
}

// -----------------------------------------------------------------------------
// Fixture builder: synthesize a v8 snapshot at `dir/snapshot.bin` with:
//   - `shard_count` shards
//   - one `SerializablePipeline` per `(stream_name, key_field)` entry
//   - one `SerializableEntityState` per `(entity_key, stream_name)` entry;
//     entities with the same entity_key from multiple streams are merged.
// -----------------------------------------------------------------------------

fn build_v8_fixture(
    dir: &Path,
    shard_count: u16,
    streams: &[(&str, &str)], // (stream_name, key_field)
    entities: &[(&str, &str)], // (entity_key, stream_name)
) {
    let pipelines: Vec<SerializablePipeline> = streams
        .iter()
        .map(|(name, key_field)| SerializablePipeline {
            name: (*name).to_string(),
            key_field: (*key_field).to_string(),
            raw_register_json: format!(
                "{{\"name\":\"{}\",\"key_field\":\"{}\"}}",
                name, key_field
            ),
        })
        .collect();

    // Group entity → streams.
    let mut by_entity: Vec<(String, Vec<(String, SerializableStreamEntityState)>)> = Vec::new();
    for (ekey, sname) in entities {
        let stream_state = SerializableStreamEntityState {
            operators: Vec::new(),
            last_event_at: None,
        };
        if let Some(existing) = by_entity.iter_mut().find(|(k, _)| k == *ekey) {
            existing.1.push((sname.to_string(), stream_state));
        } else {
            by_entity.push((
                (*ekey).to_string(),
                vec![(sname.to_string(), stream_state)],
            ));
        }
    }

    let ents: Vec<(String, SerializableEntityState)> = by_entity
        .into_iter()
        .map(|(k, streams)| {
            (
                k,
                SerializableEntityState {
                    streams,
                    static_features: Vec::new(),
                    table_rows: Vec::new(),
                },
            )
        })
        .collect();

    let snap = BaseSnapshotStateV8 {
        header: SnapshotHeader {
            snapshot_type: SnapshotType::Base,
            sequence: 1,
        },
        entities: ents,
        pipelines,
        backfill_complete: Vec::new(),
        shard_count,
        replica_lsn_map: HashMap::new(),
    };

    let bytes = save_base_snapshot_v8(&snap).expect("save v8");
    fs::write(dir.join("snapshot.bin"), bytes).expect("write snapshot.bin");
}

/// Compute the expected shard index exactly as the migration is required to
/// reproduce production routing. Matches `resolve_shard_key_for_entity`'s
/// contract.
fn expected_shard(entity_key: &str, key_field: &str, shard_count: u16) -> usize {
    if key_field.is_empty() {
        return 0;
    }
    let payload = serde_json::json!({ key_field.to_string(): entity_key });
    (shard_hint_for_event(&payload, Some(key_field)) as usize) % (shard_count as usize)
}

// Helper: read back all keys across N partitions (for assertion).
fn collect_partition_keys(
    data_dir: &Path,
    shard_count: u16,
) -> Vec<std::collections::HashSet<String>> {
    let cfg = fjall_config_from_env(shard_count);
    let ks = open_keyspace_from_env(data_dir, &cfg).expect("open keyspace for readback");
    (0..shard_count as usize)
        .map(|i| {
            let partition = open_shard_partition(&ks, i, &cfg).expect("open partition");
            let mut out = std::collections::HashSet::new();
            for kv in partition.iter() {
                let (k, _v) = kv.expect("partition kv");
                out.insert(String::from_utf8(k.to_vec()).expect("utf8 key"));
            }
            out
        })
        .collect()
}

// =============================================================================
// Test 1 — fresh migration converts entities to fjall at correct shard indices.
// =============================================================================

#[test]
fn fresh_migration_converts_entities_to_fjall() {
    let _g = env_lock().lock().unwrap();
    set_determinism_env();

    let tmp = TempDir::new().unwrap();
    let dir = tmp.path().to_path_buf();
    let shard_count: u16 = 2;
    let streams = &[("txns", "user_id")];
    let entity_keys: Vec<String> = (0..100).map(|i| format!("user-{:03}", i)).collect();
    let entities: Vec<(&str, &str)> = entity_keys.iter().map(|k| (k.as_str(), "txns")).collect();
    build_v8_fixture(&dir, shard_count, streams, &entities);

    let report = migrate_to_fjall(&dir, false, false).expect("migrate");

    assert_eq!(report.entities_migrated, 100);
    assert_eq!(report.entities_skipped, 0);
    assert_eq!(report.streams_resolved, 1);
    assert!(dir.join("fjall").is_dir(), "fjall/ dir must exist");
    assert!(
        dir.join("snapshot.v8.bak").exists(),
        "snapshot.v8.bak must be preserved"
    );

    // Re-read snapshot.bin — it must be metadata-only (entities empty).
    use beava::state::snapshot::{load_snapshot_file, SnapshotFile};
    let snap_bytes = fs::read(dir.join("snapshot.bin")).unwrap();
    match load_snapshot_file(&snap_bytes).expect("load metadata-only snapshot") {
        SnapshotFile::Base(v8) => {
            assert!(
                v8.entities.is_empty(),
                "post-migration snapshot.bin must have empty entities"
            );
            assert_eq!(v8.shard_count, shard_count);
        }
        _ => panic!("expected base snapshot"),
    }

    // Verify routing: each entity lives in the partition matching
    // `shard_hint_for_event({user_id: key}, Some("user_id")) % shard_count`.
    let keys_per_shard = collect_partition_keys(&dir, shard_count);
    for ekey in &entity_keys {
        let expected = expected_shard(ekey, "user_id", shard_count);
        assert!(
            keys_per_shard[expected].contains(ekey),
            "entity {} expected in shard {} but not found; shards = {:?}",
            ekey,
            expected,
            keys_per_shard
                .iter()
                .enumerate()
                .map(|(i, s)| (i, s.len()))
                .collect::<Vec<_>>()
        );
    }
}

// =============================================================================
// Test 2 — idempotent second run without --force is a no-op.
// =============================================================================

#[test]
fn idempotent_second_run_exits_with_already_migrated_and_no_changes() {
    let _g = env_lock().lock().unwrap();
    set_determinism_env();

    let tmp = TempDir::new().unwrap();
    let dir = tmp.path().to_path_buf();
    build_v8_fixture(
        &dir,
        2,
        &[("txns", "user_id")],
        &[("alice", "txns"), ("bob", "txns"), ("carol", "txns")],
    );

    let r1 = migrate_to_fjall(&dir, false, false).expect("migrate 1");
    assert_eq!(r1.entities_migrated, 3);

    // Capture keys after first run.
    let keys_before = collect_partition_keys(&dir, 2);

    let r2 = migrate_to_fjall(&dir, false, false).expect("migrate 2 (idempotent)");
    assert_eq!(
        r2.entities_migrated, 0,
        "second run must not re-migrate entities (got {})",
        r2.entities_migrated
    );

    let keys_after = collect_partition_keys(&dir, 2);
    assert_eq!(keys_before, keys_after, "partitions must be unchanged");
}

// =============================================================================
// Test 3 — resume from marker: pre-inserted entities skipped.
// =============================================================================

#[test]
fn resume_from_marker_inserts_missing_entities_only() {
    let _g = env_lock().lock().unwrap();
    set_determinism_env();

    let tmp = TempDir::new().unwrap();
    let dir = tmp.path().to_path_buf();
    let entity_keys: Vec<String> = (0..20).map(|i| format!("user-{:02}", i)).collect();
    let entities: Vec<(&str, &str)> = entity_keys.iter().map(|k| (k.as_str(), "txns")).collect();
    build_v8_fixture(&dir, 2, &[("txns", "user_id")], &entities);

    // Pre-create the keyspace + partitions with HALF the entities at correct shards.
    {
        let cfg = fjall_config_from_env(2);
        let ks = open_keyspace_from_env(&dir, &cfg).expect("open keyspace");
        let mut partitions = Vec::new();
        for i in 0..2 {
            partitions.push(open_shard_partition(&ks, i, &cfg).expect("open partition"));
        }
        // Insert first 10 entity keys at the correct partitions.
        for ekey in &entity_keys[..10] {
            let shard_idx = expected_shard(ekey, "user_id", 2);
            partitions[shard_idx]
                .insert(ekey.as_bytes(), b"preseeded")
                .expect("preseed insert");
        }
        ks.persist(fjall::PersistMode::SyncData).expect("persist");
    }
    // Write the marker file to trigger resume mode.
    fs::write(dir.join(".migration-in-progress"), b"pid=0 ts=0\n")
        .expect("write marker");

    let report = migrate_to_fjall(&dir, false, false).expect("migrate resume");
    assert_eq!(
        report.entities_skipped, 10,
        "resume must skip 10 pre-seeded entities"
    );
    assert_eq!(
        report.entities_migrated, 10,
        "resume must migrate 10 missing entities"
    );
}

// =============================================================================
// Test 4 — snapshot.v8.bak preserved unless --replace.
// =============================================================================

#[test]
fn bak_preserved_unless_replace_passed() {
    let _g = env_lock().lock().unwrap();
    set_determinism_env();

    // Case A: replace=false → bak exists after.
    let tmp_a = TempDir::new().unwrap();
    let dir_a = tmp_a.path().to_path_buf();
    build_v8_fixture(&dir_a, 1, &[("txns", "user_id")], &[("alice", "txns")]);
    let _ = migrate_to_fjall(&dir_a, false, false).expect("migrate a");
    assert!(
        dir_a.join("snapshot.v8.bak").exists(),
        "bak must exist when replace=false"
    );

    // Case B: replace=true → bak does NOT exist after.
    let tmp_b = TempDir::new().unwrap();
    let dir_b = tmp_b.path().to_path_buf();
    build_v8_fixture(&dir_b, 1, &[("txns", "user_id")], &[("bob", "txns")]);
    let _ = migrate_to_fjall(&dir_b, false, true).expect("migrate b");
    assert!(
        !dir_b.join("snapshot.v8.bak").exists(),
        "bak must NOT exist when replace=true"
    );
}

// =============================================================================
// Test 5 — fs2 lock contention → Err(WouldBlock).
// =============================================================================

#[test]
fn lock_contention_returns_would_block() {
    use fs2::FileExt;
    let _g = env_lock().lock().unwrap();
    set_determinism_env();

    let tmp = TempDir::new().unwrap();
    let dir = tmp.path().to_path_buf();
    build_v8_fixture(&dir, 1, &[("txns", "user_id")], &[("alice", "txns")]);

    // Take the exclusive lock first (simulates a running server).
    let lock_path = dir.join(".beava.lock");
    let held = std::fs::File::create(&lock_path).unwrap();
    held.try_lock_exclusive().expect("lock");

    let err = migrate_to_fjall(&dir, false, false).expect_err("should be blocked");
    assert_eq!(
        err.kind(),
        std::io::ErrorKind::WouldBlock,
        "lock contention must be WouldBlock, got: {:?}",
        err
    );

    FileExt::unlock(&held).ok();
}

// =============================================================================
// Test 6 — --force re-migrates over existing fjall (restores deleted keys).
// =============================================================================

#[test]
fn force_flag_remigrates_overwriting_existing_fjall() {
    let _g = env_lock().lock().unwrap();
    set_determinism_env();

    let tmp = TempDir::new().unwrap();
    let dir = tmp.path().to_path_buf();
    let entity_keys: Vec<String> = (0..10).map(|i| format!("user-{}", i)).collect();
    let entities: Vec<(&str, &str)> = entity_keys.iter().map(|k| (k.as_str(), "txns")).collect();
    build_v8_fixture(&dir, 2, &[("txns", "user_id")], &entities);

    // Migrate once.
    let _ = migrate_to_fjall(&dir, false, false).expect("migrate 1");

    // Delete the entity for `user-3` from its owning partition.
    let target_key = "user-3";
    let target_shard = expected_shard(target_key, "user_id", 2);
    {
        let cfg = fjall_config_from_env(2);
        let ks = open_keyspace_from_env(&dir, &cfg).expect("open keyspace");
        let p = open_shard_partition(&ks, target_shard, &cfg).expect("open partition");
        p.remove(target_key.as_bytes()).expect("remove");
        ks.persist(fjall::PersistMode::SyncData).expect("persist");
    }

    // Sanity: entity is gone.
    let keys_after_delete = collect_partition_keys(&dir, 2);
    assert!(
        !keys_after_delete[target_shard].contains(target_key),
        "precondition: deleted key absent"
    );

    // Re-migrate with force=true. The entity must come back.
    let _ = migrate_to_fjall(&dir, true, false).expect("migrate 2 (force)");
    let keys_after_force = collect_partition_keys(&dir, 2);
    assert!(
        keys_after_force[target_shard].contains(target_key),
        "force=true must restore deleted entity {}",
        target_key
    );
}

// =============================================================================
// Test 7 — W-2 per-stream shard_key routing parity with production.
// =============================================================================

#[test]
fn per_stream_shard_key_routing_matches_production() {
    let _g = env_lock().lock().unwrap();
    set_determinism_env();

    let tmp = TempDir::new().unwrap();
    let dir = tmp.path().to_path_buf();
    let shard_count: u16 = 4;

    let stream_a_keys: Vec<String> = (0..30).map(|i| format!("a-{}", i)).collect();
    let stream_b_keys: Vec<String> = (0..30).map(|i| format!("b-{}", i)).collect();
    let stream_c_keys: Vec<String> = (0..10).map(|i| format!("c-{}", i)).collect();

    let streams = &[
        ("stream_a", "user_id"),
        ("stream_b", "account"),
        ("stream_c", ""), // keyless
    ];

    let mut entities: Vec<(&str, &str)> = Vec::new();
    for k in &stream_a_keys {
        entities.push((k.as_str(), "stream_a"));
    }
    for k in &stream_b_keys {
        entities.push((k.as_str(), "stream_b"));
    }
    for k in &stream_c_keys {
        entities.push((k.as_str(), "stream_c"));
    }

    build_v8_fixture(&dir, shard_count, streams, &entities);

    let report = migrate_to_fjall(&dir, false, false).expect("migrate");

    // W-2 report counters.
    assert_eq!(
        report.streams_resolved, 2,
        "stream_a + stream_b should count as resolved (single-field keys)"
    );
    assert!(
        report.streams_keyless >= 1,
        "stream_c (keyless) should produce at least 1 keyless routing event"
    );
    assert_eq!(
        report.entities_migrated,
        stream_a_keys.len() + stream_b_keys.len() + stream_c_keys.len()
    );

    // Full keys-per-shard map for assertion.
    let keys_per_shard = collect_partition_keys(&dir, shard_count);

    // stream_a: shard_key = "user_id"
    for k in &stream_a_keys {
        let expected = expected_shard(k, "user_id", shard_count);
        assert!(
            keys_per_shard[expected].contains(k),
            "stream_a entity {} expected shard {} (via user_id)",
            k,
            expected
        );
    }

    // stream_b: shard_key = "account" — key_field name MUST NOT be hardcoded.
    for k in &stream_b_keys {
        let expected = expected_shard(k, "account", shard_count);
        assert!(
            keys_per_shard[expected].contains(k),
            "stream_b entity {} expected shard {} (via account); if this fails, migration hardcoded \"user_id\" or \"key\"",
            k,
            expected
        );
    }

    // stream_c: keyless → shard 0
    for k in &stream_c_keys {
        assert!(
            keys_per_shard[0].contains(k),
            "stream_c entity {} (keyless) expected shard 0; if this fails, keyless stream routed via synthesized payload instead of None",
            k
        );
    }
}

// =============================================================================
// Minor sanity: CLI helpers resolve (compile-level probe).
// =============================================================================

#[test]
fn cli_helpers_exist() {
    // Smoke: ensures `is_migrate_subcommand`, `parse_migrate_args`, and the
    // `MigrationReport` type resolve from beava::migrate_to_fjall. Intentionally
    // trivial body — the real compile-level check is the imports at the top of
    // this file.
    let argv: Vec<String> = vec!["tally".to_string(), "migrate-to-fjall".to_string()];
    assert!(is_migrate_subcommand(&argv));
    // parse_migrate_args with missing args should return Err (no panic).
    let _ = parse_migrate_args(&argv);
    // Trivial constructor path — MigrationReport fields are pub per interfaces.
    let _probe_type: Option<MigrationReport> = None;
    let _ = _probe_type; // silence unused
    let _ = PathBuf::new();
}
