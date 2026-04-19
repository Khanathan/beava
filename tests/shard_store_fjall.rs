//! Phase 53-03B Task 1 — TDD RED integration tests for `ShardedStateStoreFjall`
//! and the `ephemeral_test_keyspace` test-keyspace helper.
//!
//! Gated behind `#![cfg(not(feature = "state-inmem"))]` because
//! `store_fjall` is compiled only in the default (fjall) build; under
//! `--features state-inmem` the module is absent.
#![cfg(not(feature = "state-inmem"))]
//!
//! These tests drive the post-Plan-03B API surface that does NOT yet exist in
//! the default (fjall) build:
//!
//!   * `beava::shard::store_fjall::ShardedStateStoreFjall` (new backend
//!     sibling to the now-state-inmem-gated `ShardedStateStoreV1`).
//!   * `ShardedStateStoreFjall::new(n, Arc<Keyspace>, &FjallConfig)`.
//!   * `ShardedStateStoreFjall::shard_count / shard_at / shard_index_for_event
//!     / for_each_shard{_mut}` — the `ShardedStateStore` trait impl.
//!   * `tests/common::ephemeral_test_keyspace(n)` — returns
//!     `(Arc<Keyspace>, Vec<PartitionHandle>, TempDir)` for use by these
//!     tests and Plans 04 / 05.
//!
//! At RED, ALL four tests fail to compile because the `store_fjall` module is
//! absent. Task 2 (GREEN) lands the implementation; this file is the oracle.
//!
//! Scope note: these tests drive the `ShardedStateStoreFjall` surface only —
//! the `src/shard/thread.rs` fjall port + `ConcurrentAppState` plumbing are
//! covered indirectly via the full `cargo test` run in Task 2.

mod common;

use std::sync::{Mutex, OnceLock};

use beava::shard::store_fjall::ShardedStateStoreFjall;

use common::ephemeral_test_keyspace;

/// Process-global lock — must wrap every call to `ephemeral_test_keyspace`
/// because it mutates `BEAVA_FJALL_*` env vars while building the config.
fn env_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

// ---------------------------------------------------------------------------
// Test 1 — ShardedStateStoreFjall::new creates N shards
// ---------------------------------------------------------------------------

#[test]
fn sharded_state_store_fjall_new_creates_n_shards() {
    let _g = env_lock().lock().unwrap();
    let (ks, partitions, _tmp, cfg) = ephemeral_test_keyspace(4);
    // Drop the pre-opened partitions — ShardedStateStoreFjall opens its own.
    drop(partitions);

    let store = ShardedStateStoreFjall::new(4, ks, &cfg).expect("open store");
    use beava::shard::traits::ShardedStateStore;
    assert_eq!(store.shard_count(), 4, "shard_count must equal constructor N");

    // shard_at(i) for i in 0..N must all resolve.
    let _s0 = store.shard_at(0);
    let _s3 = store.shard_at(3);
}

// ---------------------------------------------------------------------------
// Test 2 — shard_index_for_event is deterministic (pure routing)
// ---------------------------------------------------------------------------

#[test]
fn sharded_state_store_fjall_shard_index_for_event_is_deterministic() {
    let _g = env_lock().lock().unwrap();
    let (ks, partitions, _tmp, cfg) = ephemeral_test_keyspace(4);
    drop(partitions);

    let store = ShardedStateStoreFjall::new(4, ks, &cfg).expect("open store");
    let ev_alice = serde_json::json!({"key": "alice"});

    // Same event routes to the same shard across calls — pure fn guarantee.
    let idx_a_1 = store.shard_index_for_event(&ev_alice, Some("key"));
    let idx_a_2 = store.shard_index_for_event(&ev_alice, Some("key"));
    assert_eq!(idx_a_1, idx_a_2, "routing must be deterministic across calls");
    assert!(idx_a_1 < 4, "index must be < shard_count");
}

// ---------------------------------------------------------------------------
// Test 3 — for_each_shard{_mut} visits every shard exactly once
// ---------------------------------------------------------------------------

#[test]
fn sharded_state_store_fjall_for_each_shard_visits_all() {
    let _g = env_lock().lock().unwrap();
    let (ks, partitions, _tmp, cfg) = ephemeral_test_keyspace(8);
    drop(partitions);

    let mut store = ShardedStateStoreFjall::new(8, ks, &cfg).expect("open store");

    use beava::shard::traits::ShardedStateStore;
    let mut visit_ro = 0usize;
    store.for_each_shard(|_s| visit_ro += 1);
    assert_eq!(visit_ro, 8, "for_each_shard must visit all 8 shards");

    let mut visit_mut = 0usize;
    store.for_each_shard_mut(|_s| visit_mut += 1);
    assert_eq!(visit_mut, 8, "for_each_shard_mut must visit all 8 shards");
}

// ---------------------------------------------------------------------------
// Test 4 — ephemeral_test_keyspace helper creates N partitions
// ---------------------------------------------------------------------------

#[test]
fn ephemeral_test_keyspace_helper_creates_n_partitions() {
    let _g = env_lock().lock().unwrap();
    let (ks, partitions, tmp, _cfg) = ephemeral_test_keyspace(3);

    // Keyspace Arc still alive (strong count >= 1).
    assert!(std::sync::Arc::strong_count(&ks) >= 1);
    // N partitions opened.
    assert_eq!(partitions.len(), 3, "helper must open the requested partition count");
    // TempDir path exists until dropped.
    assert!(tmp.path().exists(), "tempdir must be alive while the helper result is held");

    // The partitions are usable — quick insert/get round-trip on partition 0.
    partitions[0]
        .insert(b"probe".as_slice(), b"ok".as_slice())
        .expect("insert into first partition");
    let got = partitions[0]
        .get(b"probe".as_slice())
        .expect("get ok")
        .expect("value present");
    assert_eq!(&*got, b"ok");
}
