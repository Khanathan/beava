//! Phase 53-02 Task 1 — TDD RED integration smoke test for the fjall backend.
//!
//! These tests drive `beava::shard::fjall_backend` — the new module Task 2
//! implements. All symbols referenced from `fjall_backend::*` are deliberately
//! unresolved at this point; the compiler MUST emit "cannot find"
//! diagnostics, which is the RED signal the executor looks for.
//!
//! Coverage:
//! - `smoke_keyspace_open_insert_close_reopen_readback`: proves the full
//!   open-keyspace → open-partitions → insert → sync-persist → drop →
//!   re-open → read-back cycle works on real disk bytes (no in-memory
//!   shortcut). Uses `BEAVA_FJALL_FSYNC_DISABLE=1` + `BEAVA_FJALL_CACHE_MB=32`
//!   so the test is deterministic and cheap.
//! - `smoke_keyspace_is_single_root`: structurally enforces D-01 — ONE
//!   keyspace at `data_dir/fjall/`, not one per shard. If a future refactor
//!   accidentally opens `data_dir/shard-0/fjall/` instead, this test catches it.
//!
//! Both tests run serially via an internal `EnvLock` because they mutate
//! `BEAVA_FJALL_*` process-global env state. Parallel tests in other files
//! that also touch these env vars MUST take the same lock.

use std::sync::{Mutex, OnceLock};
use tempfile::TempDir;

use beava::shard::fjall_backend::{
    fjall_config_from_env, open_keyspace_from_env, open_shard_partition,
};

/// Process-global lock for tests that mutate `BEAVA_FJALL_*` env vars. Parallel
/// test harnesses (`cargo test`) run each integration test binary in its own
/// process, but inside a binary the tests share the process env table.
fn env_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

/// Remove all `BEAVA_FJALL_*` env vars to isolate the test. Call under the
/// env_lock guard.
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

/// Round-trip smoke: open keyspace, open two partitions, insert into each,
/// persist-sync, drop, re-open same data_dir, and assert the bytes we wrote
/// come back unchanged from disk.
#[test]
fn smoke_keyspace_open_insert_close_reopen_readback() {
    let _guard = env_lock().lock().unwrap();
    clear_fjall_env();
    std::env::set_var("BEAVA_FJALL_FSYNC_DISABLE", "1");
    std::env::set_var("BEAVA_FJALL_CACHE_MB", "32");

    let tmp = TempDir::new().expect("tempdir");
    let cfg = fjall_config_from_env(2);
    assert_eq!(cfg.fsync_ms, None, "FSYNC_DISABLE=1 must force fsync_ms=None");
    assert_eq!(cfg.cache_mb, 32, "BEAVA_FJALL_CACHE_MB=32 must be honored");

    // -- Round 1: open, insert, persist-sync, drop.
    {
        let ks = open_keyspace_from_env(tmp.path(), &cfg).expect("open keyspace");
        let p0 = open_shard_partition(&ks, 0, &cfg).expect("open shard-0");
        let p1 = open_shard_partition(&ks, 1, &cfg).expect("open shard-1");
        p0.insert(b"alice", b"{\"v\":1}").expect("insert alice");
        p1.insert(b"bob", b"{\"v\":2}").expect("insert bob");
        ks.persist(fjall::PersistMode::SyncData)
            .expect("persist sync fence");
        // Drop order: p0, p1, ks. Scoped block enforces this.
    }

    // -- Round 2: re-open the same data_dir and read back.
    {
        let ks = open_keyspace_from_env(tmp.path(), &cfg).expect("re-open keyspace");
        let p0 = open_shard_partition(&ks, 0, &cfg).expect("re-open shard-0");
        let p1 = open_shard_partition(&ks, 1, &cfg).expect("re-open shard-1");
        let got_alice = p0
            .get(b"alice")
            .expect("fjall get alice")
            .expect("alice present after reopen");
        let got_bob = p1
            .get(b"bob")
            .expect("fjall get bob")
            .expect("bob present after reopen");
        assert_eq!(&got_alice[..], b"{\"v\":1}");
        assert_eq!(&got_bob[..], b"{\"v\":2}");
    }

    clear_fjall_env();
}

/// Structural enforcement of D-01 (Pitfall 1 from 53-RESEARCH): the fjall
/// keyspace root is `data_dir/fjall/`, NOT `data_dir/shard-N/fjall/`.
///
/// If someone ever refactors `open_keyspace_from_env` to open a keyspace per
/// shard, this test fails immediately — `data_dir/fjall` will not exist and
/// `data_dir/shard-0/fjall` will.
#[test]
fn smoke_keyspace_is_single_root() {
    let _guard = env_lock().lock().unwrap();
    clear_fjall_env();
    std::env::set_var("BEAVA_FJALL_FSYNC_DISABLE", "1");
    std::env::set_var("BEAVA_FJALL_CACHE_MB", "32");

    let tmp = TempDir::new().expect("tempdir");
    let cfg = fjall_config_from_env(2);

    let ks = open_keyspace_from_env(tmp.path(), &cfg).expect("open keyspace");
    let p0 = open_shard_partition(&ks, 0, &cfg).expect("open shard-0");
    let p1 = open_shard_partition(&ks, 1, &cfg).expect("open shard-1");
    p0.insert(b"k", b"v").expect("insert p0");
    p1.insert(b"k", b"v").expect("insert p1");
    ks.persist(fjall::PersistMode::SyncData).expect("persist");
    drop(p0);
    drop(p1);
    drop(ks);

    let fjall_root = tmp.path().join("fjall");
    assert!(
        fjall_root.is_dir(),
        "D-01: the single fjall keyspace root must exist at {:?}",
        fjall_root
    );
    let shard0_root = tmp.path().join("shard-0").join("fjall");
    let shard1_root = tmp.path().join("shard-1").join("fjall");
    assert!(
        !shard0_root.exists(),
        "D-01 violation: per-shard fjall roots must NOT exist (found {:?})",
        shard0_root
    );
    assert!(
        !shard1_root.exists(),
        "D-01 violation: per-shard fjall roots must NOT exist (found {:?})",
        shard1_root
    );

    clear_fjall_env();
}
