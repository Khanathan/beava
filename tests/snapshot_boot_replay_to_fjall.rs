//! Phase 54 Plan 03 Task 1 — TDD RED test for boot-time snapshot replay
//! writing directly to fjall partitions (bypassing DashMap / `StateStore`).
//!
//! Contract: `beava::state::snapshot::restore_snapshot_to_shards(entities,
//! pipelines, partitions)` routes each `(entity_key, SerializableEntityState)`
//! tuple to `partitions[shard_idx]` where `shard_idx` matches the W-2 per-
//! stream `shard_key` routing used by `migrate_to_fjall` — reproducing
//! production ingest-time hashing EXACTLY. Values are postcard-encoded
//! `SerializableEntityState` bytes so subsequent reads via `Shard::with_partition`
//! round-trip through the normal fjall path.
//!
//! RED invariant: `beava::state::snapshot::restore_snapshot_to_shards`
//! does not exist yet. Task 1 GREEN introduces it.
//!
//! Default-build only — boot-time snapshot replay to fjall partitions is
//! the default build path. Under `state-inmem` the legacy AHashMap-backed
//! `state.store` replay still owns this job; scope of Task 1 is explicitly
//! fjall-first.

#![cfg(not(feature = "state-inmem"))]

use std::sync::{Mutex, OnceLock};

use beava::routing::shard_hint::shard_hint_for_event;
use beava::shard::fjall_backend::{fjall_config_from_env, open_keyspace_from_env, open_shard_partition};
use beava::state::snapshot::{
    restore_snapshot_to_shards, SerializableEntityState, SerializablePipeline,
    SerializableStreamEntityState,
};
use tempfile::TempDir;

// -----------------------------------------------------------------------------
// Env-mutation guard shared with common/mod.rs. `fjall_config_from_env` reads
// BEAVA_FJALL_* on every call; tests must serialize their env writes.
// -----------------------------------------------------------------------------

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

/// Mirror of `migrate_to_fjall::resolve_shard_key_for_entity` contract —
/// entities with an explicit single-field `key_field` route via
/// `hash({key_field: entity_key}) % shard_count`; keyless streams land on 0.
fn expected_shard(entity_key: &str, key_field: &str, shard_count: u16) -> usize {
    if key_field.is_empty() {
        return 0;
    }
    let payload = serde_json::json!({ key_field.to_string(): entity_key });
    (shard_hint_for_event(&payload, Some(key_field)) as usize) % (shard_count as usize)
}

fn make_entity(stream_name: &str) -> SerializableEntityState {
    SerializableEntityState {
        streams: vec![(
            stream_name.to_string(),
            SerializableStreamEntityState {
                operators: Vec::new(),
                last_event_at: None,
            },
        )],
        static_features: Vec::new(),
        table_rows: Vec::new(),
    }
}

/// Happy path — 3 entities across 1 stream with a `user_id` shard_key,
/// routed through N=8 partitions. Every insert must land on the shard
/// computed by `shard_hint_for_event`, and the total entity count must
/// equal the input length.
#[test]
fn restore_snapshot_to_shards_routes_by_per_stream_shard_key() {
    let _g = lock_env();
    set_determinism_env();
    let shard_count: u16 = 8;
    let cfg = fjall_config_from_env(shard_count);

    let tmp = TempDir::new().expect("tempdir");
    let ks = open_keyspace_from_env(tmp.path(), &cfg).expect("open keyspace");
    let partitions: Vec<fjall::PartitionHandle> = (0..shard_count as usize)
        .map(|i| open_shard_partition(&ks, i, &cfg).expect("open partition"))
        .collect();

    let pipelines = vec![SerializablePipeline {
        name: "orders".to_string(),
        key_field: "user_id".to_string(),
        raw_register_json: r#"{"name":"orders","key_field":"user_id"}"#.to_string(),
    }];

    let entity_keys = vec!["u-alpha", "u-bravo", "u-charlie"];
    let entities: Vec<(String, SerializableEntityState)> = entity_keys
        .iter()
        .map(|k| ((*k).to_string(), make_entity("orders")))
        .collect();

    let total = entities.len();
    let counts = restore_snapshot_to_shards(entities, &pipelines, &partitions)
        .expect("restore_snapshot_to_shards");

    // Distribution invariant: sum across shards == total entities.
    assert_eq!(
        counts.iter().sum::<usize>(),
        total,
        "sum of per-shard counts must equal input entity count"
    );
    assert_eq!(counts.len(), shard_count as usize);

    // Per-entity routing invariant: each key lives on exactly the shard
    // computed by the production hash.
    for key in &entity_keys {
        let expected_idx = expected_shard(key, "user_id", shard_count);
        let present = partitions[expected_idx]
            .contains_key(key.as_bytes())
            .expect("fjall contains_key");
        assert!(
            present,
            "entity {} missing from expected shard {}",
            key, expected_idx
        );

        // And it must NOT appear on any other shard.
        for (i, p) in partitions.iter().enumerate() {
            if i == expected_idx {
                continue;
            }
            let stray = p.contains_key(key.as_bytes()).expect("fjall contains_key");
            assert!(
                !stray,
                "entity {} leaked onto shard {} (expected shard {})",
                key, i, expected_idx
            );
        }
    }
}

/// Stored bytes must round-trip through postcard back into
/// `SerializableEntityState` — this proves the helper uses the same wire
/// format as `Shard::with_partition` + `StoreView::Sharded`, so live reads
/// post-boot (before shard threads start writing) will decode cleanly.
#[test]
fn restore_snapshot_to_shards_postcard_roundtrips_entity_state() {
    let _g = lock_env();
    set_determinism_env();
    let shard_count: u16 = 4;
    let cfg = fjall_config_from_env(shard_count);

    let tmp = TempDir::new().expect("tempdir");
    let ks = open_keyspace_from_env(tmp.path(), &cfg).expect("open keyspace");
    let partitions: Vec<fjall::PartitionHandle> = (0..shard_count as usize)
        .map(|i| open_shard_partition(&ks, i, &cfg).expect("open partition"))
        .collect();

    let pipelines = vec![SerializablePipeline {
        name: "events".to_string(),
        key_field: "user_id".to_string(),
        raw_register_json: r#"{"name":"events","key_field":"user_id"}"#.to_string(),
    }];

    let key = "u-42";
    let original = make_entity("events");
    let entities = vec![(key.to_string(), original.clone())];

    restore_snapshot_to_shards(entities, &pipelines, &partitions)
        .expect("restore_snapshot_to_shards");

    let idx = expected_shard(key, "user_id", shard_count);
    let bytes = partitions[idx]
        .get(key.as_bytes())
        .expect("fjall get")
        .expect("entity present");
    let decoded: SerializableEntityState =
        postcard::from_bytes(&bytes).expect("postcard decode SerializableEntityState");
    assert_eq!(decoded.streams.len(), original.streams.len());
    assert_eq!(decoded.streams[0].0, original.streams[0].0);
}

/// Keyless streams (empty `key_field`) route to shard 0. Matches the
/// `shard_hint_for_event(_, None) -> 0` invariant used by ingest and by
/// `migrate_to_fjall`.
#[test]
fn restore_snapshot_to_shards_keyless_routes_to_shard_zero() {
    let _g = lock_env();
    set_determinism_env();
    let shard_count: u16 = 4;
    let cfg = fjall_config_from_env(shard_count);

    let tmp = TempDir::new().expect("tempdir");
    let ks = open_keyspace_from_env(tmp.path(), &cfg).expect("open keyspace");
    let partitions: Vec<fjall::PartitionHandle> = (0..shard_count as usize)
        .map(|i| open_shard_partition(&ks, i, &cfg).expect("open partition"))
        .collect();

    let pipelines = vec![SerializablePipeline {
        name: "keyless".to_string(),
        key_field: String::new(),
        raw_register_json: r#"{"name":"keyless","key_field":""}"#.to_string(),
    }];

    let entities: Vec<(String, SerializableEntityState)> = (0..5)
        .map(|i| (format!("anon-{}", i), make_entity("keyless")))
        .collect();
    let total = entities.len();

    let counts = restore_snapshot_to_shards(entities, &pipelines, &partitions)
        .expect("restore_snapshot_to_shards");

    assert_eq!(counts.iter().sum::<usize>(), total);
    assert_eq!(counts[0], total, "all keyless entities must land on shard 0");
    for c in &counts[1..] {
        assert_eq!(*c, 0, "non-zero shard received a keyless entity");
    }
}
