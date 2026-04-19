//! Phase 53-03 Task 1 — TDD RED integration tests for the fjall-backed
//! `Shard.state` swap and the `read_entity_from_shard` read-only helper.
//!
//! These tests drive post-swap API surface that does NOT yet exist in
//! `src/shard/mod.rs`:
//!   * `Shard::with_partition(PartitionHandle) -> Shard`
//!   * `Shard.state: fjall::PartitionHandle` (not `AHashMap<_, _>`)
//!   * `read_entity_from_shard<F, R>(&Shard, &str, F) -> Option<R>` (W-6)
//!
//! At RED, ALL five tests here fail to compile: the symbols above are absent.
//! Task 2 (GREEN) lands the implementation; this file is the oracle.
//!
//! Scope note (W-1 revision): this file intentionally calls `Shard::with_partition`
//! directly — it does NOT exercise `src/shard/thread.rs`, `src/server/tcp.rs`,
//! or the proptest harness. Those callsites are Plan 03B's job.

use std::sync::{Mutex, OnceLock};
use std::time::SystemTime;
use tempfile::TempDir;

use beava::shard::fjall_backend::{
    fjall_config_from_env, open_keyspace_from_env, open_shard_partition,
};
use beava::shard::{read_entity_from_shard, Shard, StoreView};
use beava::state::store::StaticFeature;
use beava::types::FeatureValue;

/// Process-global lock for tests that mutate `BEAVA_FJALL_*` env vars.
fn env_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

fn clear_fjall_env() {
    for k in [
        "BEAVA_FJALL_FSYNC_MS",
        "BEAVA_FJALL_FSYNC_DISABLE",
        "BEAVA_FJALL_CACHE_MB",
        "BEAVA_FJALL_FLUSH_WORKERS",
        "BEAVA_FJALL_COMPACTION_WORKERS",
        "BEAVA_FJALL_BLOCK_SIZE",
        "BEAVA_FJALL_MAX_MEMTABLE_MB",
    ] {
        std::env::remove_var(k);
    }
}

/// Deterministic test-cfg: disable fsync thread, tiny cache. Callers MUST hold
/// env_lock() while invoking.
fn test_cfg(num_shards: u16) -> beava::shard::fjall_backend::FjallConfig {
    std::env::set_var("BEAVA_FJALL_FSYNC_DISABLE", "1");
    std::env::set_var("BEAVA_FJALL_CACHE_MB", "32");
    fjall_config_from_env(num_shards)
}

// ---------------------------------------------------------------------------
// Test 1 — StoreView::Sharded write-then-read round-trips through fjall
// ---------------------------------------------------------------------------

/// Write `alice.static_features["x"] = Int(7)` via `StoreView::Sharded`; then
/// read it back via the same view. Also asserts a missing key resolves to
/// `None` through `get_entity_ref`.
#[test]
fn storeview_sharded_write_then_read_round_trips_through_fjall() {
    let _g = env_lock().lock().unwrap();
    clear_fjall_env();
    let cfg = test_cfg(1);

    let tmp = TempDir::new().expect("tempdir");
    let ks = open_keyspace_from_env(tmp.path(), &cfg).expect("open keyspace");
    let partition = open_shard_partition(&ks, 0, &cfg).expect("open partition");

    let mut shard = Shard::with_partition(partition);
    let mut view = StoreView::Sharded(&mut shard);

    view.with_entity_mut("alice", |e| {
        e.static_features.insert(
            "x".to_string(),
            StaticFeature {
                value: FeatureValue::Int(7),
                updated_at: SystemTime::UNIX_EPOCH,
            },
        );
    });

    let got_alice_x = view.get_entity_ref("alice", |e| {
        e.static_features.get("x").map(|sf| sf.value.clone())
    });
    assert_eq!(got_alice_x, Some(Some(FeatureValue::Int(7))));

    let got_bob = view.get_entity_ref("bob", |_| 42u32);
    assert_eq!(got_bob, None, "missing key must resolve to None");

    clear_fjall_env();
}

// ---------------------------------------------------------------------------
// Test 2 — StoreView::Sharded survives keyspace reopen (WAL recovery proxy)
// ---------------------------------------------------------------------------

/// Write, sync-persist, drop the keyspace; then re-open the same `data_dir`
/// and assert the written feature is readable. This semantic proof is NOT the
/// SIGKILL crash test (that's Plan 05) — it validates the clean-shutdown
/// round-trip end-to-end through `StoreView::Sharded`.
#[test]
fn storeview_sharded_survives_keyspace_reopen() {
    let _g = env_lock().lock().unwrap();
    clear_fjall_env();
    let cfg = test_cfg(1);

    let tmp = TempDir::new().expect("tempdir");

    // Round 1: open, write, sync-persist, drop.
    {
        let ks = open_keyspace_from_env(tmp.path(), &cfg).expect("open keyspace");
        let partition = open_shard_partition(&ks, 0, &cfg).expect("open partition");
        let mut shard = Shard::with_partition(partition);
        {
            let mut view = StoreView::Sharded(&mut shard);
            view.with_entity_mut("alice", |e| {
                e.static_features.insert(
                    "score".to_string(),
                    StaticFeature {
                        value: FeatureValue::Float(9.5),
                        updated_at: SystemTime::UNIX_EPOCH,
                    },
                );
            });
        }
        ks.persist(fjall::PersistMode::SyncData)
            .expect("persist sync fence");
    }

    // Round 2: reopen, read, assert identity.
    {
        let ks = open_keyspace_from_env(tmp.path(), &cfg).expect("re-open keyspace");
        let partition = open_shard_partition(&ks, 0, &cfg).expect("re-open partition");
        let mut shard = Shard::with_partition(partition);
        let view = StoreView::Sharded(&mut shard);
        let got = view.get_entity_ref("alice", |e| {
            e.static_features.get("score").map(|sf| sf.value.clone())
        });
        assert_eq!(
            got,
            Some(Some(FeatureValue::Float(9.5))),
            "value must survive clean keyspace reopen"
        );
    }

    clear_fjall_env();
}

// ---------------------------------------------------------------------------
// Test 3 — Two shard partitions are isolated; no cross-contention / leakage
// ---------------------------------------------------------------------------

/// Two threads, each owning a distinct partition handle in its own Shard, do
/// 200 inserts with disjoint key prefixes. After persist, each partition must
/// see ONLY its own prefix — proving per-shard isolation at the fjall layer.
#[test]
fn two_shard_partitions_isolated_no_cross_contention() {
    let _g = env_lock().lock().unwrap();
    clear_fjall_env();
    let cfg = test_cfg(2);

    let tmp = TempDir::new().expect("tempdir");
    let ks = open_keyspace_from_env(tmp.path(), &cfg).expect("open keyspace");
    let p0 = open_shard_partition(&ks, 0, &cfg).expect("open p0");
    let p1 = open_shard_partition(&ks, 1, &cfg).expect("open p1");

    const N: usize = 200;

    let t0 = {
        let p0 = p0.clone();
        std::thread::spawn(move || {
            let mut shard = Shard::with_partition(p0);
            for i in 0..N {
                let key = format!("A-{}", i);
                let mut view = StoreView::Sharded(&mut shard);
                view.with_entity_mut(&key, |e| {
                    e.static_features.insert(
                        "v".to_string(),
                        StaticFeature {
                            value: FeatureValue::Int(i as i64),
                            updated_at: SystemTime::UNIX_EPOCH,
                        },
                    );
                });
            }
        })
    };
    let t1 = {
        let p1 = p1.clone();
        std::thread::spawn(move || {
            let mut shard = Shard::with_partition(p1);
            for i in 0..N {
                let key = format!("B-{}", i);
                let mut view = StoreView::Sharded(&mut shard);
                view.with_entity_mut(&key, |e| {
                    e.static_features.insert(
                        "v".to_string(),
                        StaticFeature {
                            value: FeatureValue::Int(i as i64),
                            updated_at: SystemTime::UNIX_EPOCH,
                        },
                    );
                });
            }
        })
    };

    t0.join().expect("t0 joined");
    t1.join().expect("t1 joined");

    ks.persist(fjall::PersistMode::SyncData).expect("persist");

    // Verify p0 holds only "A-*" keys and p1 only "B-*" keys. Iteration via
    // the fjall partition handle should yield zero cross-prefix entries.
    let mut a_count_in_p0 = 0usize;
    let mut b_count_in_p0 = 0usize;
    for kv in p0.iter() {
        let (k, _) = kv.expect("iter ok");
        let k_str = std::str::from_utf8(&k).expect("utf8 key");
        if k_str.starts_with("A-") {
            a_count_in_p0 += 1;
        } else if k_str.starts_with("B-") {
            b_count_in_p0 += 1;
        }
    }
    assert_eq!(a_count_in_p0, N, "partition 0 must see all A- keys");
    assert_eq!(b_count_in_p0, 0, "partition 0 must NOT see any B- keys");

    let mut a_count_in_p1 = 0usize;
    let mut b_count_in_p1 = 0usize;
    for kv in p1.iter() {
        let (k, _) = kv.expect("iter ok");
        let k_str = std::str::from_utf8(&k).expect("utf8 key");
        if k_str.starts_with("A-") {
            a_count_in_p1 += 1;
        } else if k_str.starts_with("B-") {
            b_count_in_p1 += 1;
        }
    }
    assert_eq!(b_count_in_p1, N, "partition 1 must see all B- keys");
    assert_eq!(a_count_in_p1, 0, "partition 1 must NOT see any A- keys");

    clear_fjall_env();
}

// ---------------------------------------------------------------------------
// Test 6 (W-6) — read_entity_from_shard returns None on missing key
// ---------------------------------------------------------------------------

/// The read-only helper MUST return `None` without writing back when the key
/// is absent. Proves the helper is cheaper than `with_entity_mut`, which
/// always does a write-back of `EntityState::default()` on first touch.
#[test]
fn read_entity_from_shard_returns_none_on_missing_key() {
    let _g = env_lock().lock().unwrap();
    clear_fjall_env();
    let cfg = test_cfg(1);

    let tmp = TempDir::new().expect("tempdir");
    let ks = open_keyspace_from_env(tmp.path(), &cfg).expect("open keyspace");
    let partition = open_shard_partition(&ks, 0, &cfg).expect("open partition");
    let shard = Shard::with_partition(partition);

    let got = read_entity_from_shard(&shard, "missing", |e| e.static_features.len());
    assert_eq!(got, None, "absent key must resolve to None");

    clear_fjall_env();
}

// ---------------------------------------------------------------------------
// Test 7 (W-6) — read_entity_from_shard returns deserialized entity
// ---------------------------------------------------------------------------

/// Seed via StoreView::Sharded; verify read_entity_from_shard deserializes
/// the same entity and returns the closure's result. A second read under the
/// same shard must still see the same state — proving the helper does no
/// write-back.
#[test]
fn read_entity_from_shard_returns_deserialized_entity() {
    let _g = env_lock().lock().unwrap();
    clear_fjall_env();
    let cfg = test_cfg(1);

    let tmp = TempDir::new().expect("tempdir");
    let ks = open_keyspace_from_env(tmp.path(), &cfg).expect("open keyspace");
    let partition = open_shard_partition(&ks, 0, &cfg).expect("open partition");
    let mut shard = Shard::with_partition(partition);

    {
        let mut view = StoreView::Sharded(&mut shard);
        view.with_entity_mut("alice", |e| {
            e.static_features.insert(
                "hits".to_string(),
                StaticFeature {
                    value: FeatureValue::Int(1),
                    updated_at: SystemTime::UNIX_EPOCH,
                },
            );
        });
    }

    let got = read_entity_from_shard(&shard, "alice", |e| e.static_features.len());
    assert_eq!(got, Some(1), "seeded entity must roundtrip");

    // Second read — same state; proves no write-back in the helper.
    let got2 = read_entity_from_shard(&shard, "alice", |e| {
        e.static_features.get("hits").map(|sf| sf.value.clone())
    });
    assert_eq!(got2, Some(Some(FeatureValue::Int(1))));

    clear_fjall_env();
}
