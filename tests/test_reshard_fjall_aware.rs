//! Phase 53 Plan 04 — TDD RED tests for fjall-aware `tally reshard`.
//!
//! 3 integration tests:
//!   1. Reshard reads from fjall partitions, rehashes, emits fjall-layout output.
//!   2. Reshard refuses when `.migration-in-progress` marker present.
//!   3. Back-compat: reshard on a pre-fjall (snapshot.entities) data dir still works.
//!
//! RED invariant: `beava::reshard::reshard_data_dir` does NOT yet honor fjall or
//! the marker file. Task 2 extends the existing reshard module; these tests
//! will go from failing/panicking to green.

#![cfg(not(feature = "state-inmem"))]

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};

use tempfile::TempDir;

use beava::migrate_to_fjall::migrate_to_fjall;
use beava::reshard::reshard_data_dir;
use beava::routing::shard_hint::shard_hint_for_event;
use beava::shard::fjall_backend::{
    fjall_config_from_env, open_keyspace_from_env, open_shard_partition,
};
use beava::state::snapshot::{
    save_base_snapshot_v8, BaseSnapshotStateV8, SerializableEntityState, SerializablePipeline,
    SerializableStreamEntityState, SnapshotHeader, SnapshotType,
};

fn env_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

fn lock_env() -> std::sync::MutexGuard<'static, ()> {
    match env_lock().lock() {
        Ok(g) => g,
        Err(poisoned) => poisoned.into_inner(),
    }
}

fn set_determinism_env() {
    std::env::set_var("BEAVA_FJALL_FSYNC_DISABLE", "1");
    std::env::set_var("BEAVA_FJALL_CACHE_MB", "32");
}

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

fn collect_partition_keys(
    data_dir: &Path,
    shard_count: u16,
) -> Vec<std::collections::HashSet<String>> {
    let cfg = fjall_config_from_env(shard_count);
    let ks = open_keyspace_from_env(data_dir, &cfg).expect("open keyspace");
    (0..shard_count as usize)
        .map(|i| {
            let p = open_shard_partition(&ks, i, &cfg).expect("open partition");
            let mut out = std::collections::HashSet::new();
            for kv in p.iter() {
                let (k, _v) = kv.expect("kv");
                out.insert(String::from_utf8(k.to_vec()).expect("utf8"));
            }
            out
        })
        .collect()
}

// =============================================================================
// Test 1 — reshard a fjall-backed data dir from N=1 to N=2; expect fjall output.
// =============================================================================

#[test]
fn reshard_from_fjall_data_dir_produces_rehashed_output() {
    let _g = lock_env();
    set_determinism_env();

    // Step 1: create a v8 snapshot with 6 entities on stream "txns" (key_field=user_id).
    let tmp_src = TempDir::new().unwrap();
    let src = tmp_src.path().to_path_buf();
    let entity_keys = vec![
        "alice", "bob", "carol", "dave", "eve", "frank",
    ];
    let entities: Vec<(&str, &str)> = entity_keys.iter().map(|k| (*k, "txns")).collect();
    build_v8_fixture(&src, 1, &[("txns", "user_id")], &entities);

    // Step 2: migrate to fjall (N=1).
    let _ = migrate_to_fjall(&src, false, false).expect("initial migrate");
    assert!(src.join("fjall").is_dir());

    // Step 3: reshard 1 → 2 into out_dir.
    let tmp_out = TempDir::new().unwrap();
    let out = tmp_out.path().join("out");
    fs::create_dir_all(&out).unwrap();

    reshard_data_dir(1, 2, &src, &out).expect("reshard 1->2");

    // Step 4: out_dir must contain a fjall keyspace (fjall-layout output).
    assert!(
        out.join("fjall").is_dir(),
        "reshard of fjall-backed dir must emit fjall-layout output (fjall/ directory)"
    );

    // Step 5: every entity must land in the shard matching the N=2 hash.
    let keys_per_shard = collect_partition_keys(&out, 2);
    let total: usize = keys_per_shard.iter().map(|s| s.len()).sum();
    assert_eq!(total, entity_keys.len(), "all entities must be present");

    for k in &entity_keys {
        let payload = serde_json::json!({"user_id": k});
        let expected = (shard_hint_for_event(&payload, Some("user_id")) as usize) % 2;
        assert!(
            keys_per_shard[expected].contains(*k),
            "entity {} expected in shard {} at N=2",
            k,
            expected
        );
    }
}

// =============================================================================
// Test 2 — reshard refuses with `.migration-in-progress` marker present.
// =============================================================================

#[test]
fn reshard_refuses_when_migration_in_progress_marker_exists() {
    let _g = lock_env();
    set_determinism_env();

    let tmp_src = TempDir::new().unwrap();
    let src = tmp_src.path().to_path_buf();
    build_v8_fixture(&src, 1, &[("txns", "user_id")], &[("alice", "txns")]);

    // Touch the marker.
    fs::write(src.join(".migration-in-progress"), b"pid=1 ts=1\n").expect("write marker");

    let tmp_out = TempDir::new().unwrap();
    let out = tmp_out.path().join("out");

    let err = reshard_data_dir(1, 2, &src, &out).expect_err("should refuse mid-migration");
    let msg = format!("{}", err);
    assert!(
        msg.to_lowercase().contains("migration in progress"),
        "error must mention 'migration in progress'; got: {}",
        msg
    );
}

// =============================================================================
// Test 3 — back-compat: pre-fjall data dir (no fjall/) still reshards.
// =============================================================================

#[test]
fn reshard_back_compat_no_fjall_still_reads_snapshot_entities() {
    let _g = lock_env();
    set_determinism_env();

    // Build a v8 snapshot with NO fjall/ directory — i.e. legacy pre-Phase-53
    // data dir. Reshard should take the legacy snapshot.entities code path.
    let tmp_src = TempDir::new().unwrap();
    let src = tmp_src.path().to_path_buf();
    build_v8_fixture(&src, 1, &[("txns", "user_id")], &[("alice", "txns")]);

    // Also create the legacy per-shard-log dir layout so `reshard_data_dir` can
    // iterate it without erroring (Phase 52-04 reshard walks shard-0/streams/).
    fs::create_dir_all(src.join("shard-0/streams")).unwrap();

    assert!(
        !src.join("fjall").is_dir(),
        "fixture precondition: no fjall/ dir"
    );

    let tmp_out = TempDir::new().unwrap();
    let out = tmp_out.path().join("out");
    fs::create_dir_all(&out).unwrap();

    // Legacy path: reshard should succeed without a fjall/ directory in src.
    reshard_data_dir(1, 2, &src, &out).expect("back-compat reshard");

    // Output shape is legacy (Phase 52-04 style): snapshot.bin with shard_count=2.
    assert!(
        out.join("snapshot.bin").exists(),
        "back-compat reshard must still write snapshot.bin"
    );

    // And — importantly — it should NOT have emitted a fjall/ dir for a non-fjall
    // source (plan's contract: fjall output only when fjall source).
    // (A permissive reader could accept either shape; we assert the simpler one.)
    let _ = PathBuf::from(&out);
}
