//! Shared test helpers for integration tests (Phase 53-03B onwards).
//!
//! Integration tests under `tests/*.rs` that need a fjall keyspace wired up
//! with N partitions should import `ephemeral_test_keyspace` from here rather
//! than hand-rolling the fjall boot dance per test.
//!
//! This module is declared via `mod common;` in each integration test file
//! that needs it. Cargo compiles it into each test binary that mentions it.

use std::sync::{Arc, Mutex, OnceLock};

use fjall::{Keyspace, PartitionHandle};
use tempfile::TempDir;

use beava::shard::fjall_backend::{
    fjall_config_from_env, open_keyspace_from_env, open_shard_partition, FjallConfig,
};

/// Process-global lock for `BEAVA_FJALL_*` env mutation inside this helper.
///
/// Parallel test runners MUST serialize any code that calls `std::env::set_var`
/// on `BEAVA_FJALL_*`. `fjall_config_from_env` reads these vars directly, so
/// this helper takes the lock for its own env-mutation phase. Callers do NOT
/// need to re-take this lock — they hold their own process-global lock for
/// the duration of the test body.
fn env_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

/// Build an ephemeral fjall keyspace rooted under a fresh `TempDir`, pre-open
/// N partitions named `shard-0`, `shard-1`, …, `shard-(N-1)`, and return all
/// four handles so the caller can keep the `TempDir` alive for the duration
/// of the test.
///
/// Returns `(Arc<Keyspace>, Vec<PartitionHandle>, TempDir, FjallConfig)`:
/// - `Arc<Keyspace>` — shared keyspace pointer. Cloned into
///   `ShardedStateStoreFjall::new` or similar.
/// - `Vec<PartitionHandle>` — N pre-opened partition handles. Caller may drop
///   them if they are going to re-open via `ShardedStateStoreFjall::new`.
/// - `TempDir` — data-dir guard. MUST be kept alive for the duration of the
///   test; dropping it removes the on-disk keyspace.
/// - `FjallConfig` — the clamped config used to open the keyspace, also
///   forwarded to `open_shard_partition`. Reused verbatim by callers that
///   want `ShardedStateStoreFjall::new(n, ks, &cfg)` identity-match.
///
/// Tuned for determinism + speed:
/// - `BEAVA_FJALL_FSYNC_DISABLE=1` — no background fsync thread. Writes land
///   in the OS page cache only; SIGKILL recovery is NOT tested here (that is
///   Plan 05's job).
/// - `BEAVA_FJALL_CACHE_MB=32` — small cache; fits inside any CI runner.
///
/// # Env-mutation warning
///
/// This helper temporarily sets the two env vars above. Callers that launch
/// multiple `ephemeral_test_keyspace` calls in parallel threads MUST hold a
/// process-global lock around each call — `fjall_config_from_env` reads the
/// env vars at this moment. The helper itself takes a private mutex for its
/// own env-read phase but that does NOT protect against cross-test racing.
pub fn ephemeral_test_keyspace(
    n: usize,
) -> (Arc<Keyspace>, Vec<PartitionHandle>, TempDir, FjallConfig) {
    let _g = env_lock().lock().unwrap();

    std::env::set_var("BEAVA_FJALL_FSYNC_DISABLE", "1");
    std::env::set_var("BEAVA_FJALL_CACHE_MB", "32");
    let cfg = fjall_config_from_env(n.max(1) as u16);

    let tmp = TempDir::new().expect("tempdir");
    let ks = open_keyspace_from_env(tmp.path(), &cfg).expect("open keyspace");

    let partitions: Vec<PartitionHandle> = (0..n)
        .map(|i| open_shard_partition(&ks, i, &cfg).expect("open partition"))
        .collect();

    (ks, partitions, tmp, cfg)
}

// Phase 55 Wave 0: cascade harness module.
//
// Hosts the two-shard fixture + fake sibling drain thread utilities used
// by Phase 55 RED tests (cross_shard_tt_cascade_ownership.rs,
// cross_shard_backpressure.rs, cross_shard_cascade_recovery.rs,
// cascade_metrics.rs, boot_rematerialization.rs). Wave 1 fills in the
// real implementations; Wave 0 only needs the module on disk so test
// files compile.
pub mod cascade_harness;
