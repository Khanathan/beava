//! `ShardedStateStoreFjall` — fjall-backed per-shard state store (Phase 53-03B).
//!
//! Sibling of the now-state-inmem-gated `ShardedStateStoreV1`. This module is
//! compiled only under the default (fjall) build; `#[cfg(not(feature =
//! "state-inmem"))]` in `src/shard/mod.rs` gates its registration.
//!
//! ## D-01 layout
//!
//! One `fjall::Keyspace` rooted at `data/fjall/` (opened by the boot path via
//! `fjall_backend::open_keyspace_from_env`). Each shard owns one
//! `fjall::PartitionHandle` named `shard-{index}` within that keyspace
//! (`fjall_backend::open_shard_partition`). `ShardedStateStoreFjall` wraps the
//! N partitions into N `Shard` structs via `Shard::with_partition` (Plan 03).
//!
//! ## Single-writer invariant
//!
//! See the module-level note on `src/shard/mod.rs`: `PartitionHandle` ops take
//! `&self`, so the type system does not enforce single-writer. The convention
//! is that the shard thread that owns `Shard` is the only thread that mutates
//! its partition via `StoreView::Sharded`. Cross-shard reads through cloned
//! handles are fine; cross-shard writes are NOT.
//!
//! ## Plan 03B trust boundary (T-53-03B-01)
//!
//! `shard_index_for_event` returns `(shard_hint_for_event % n)`; because `n`
//! is asserted `>= 1 && <= 256` at construction time, the returned index is
//! arithmetically always in-bounds for `self.shards[idx]`.

use std::sync::Arc;

use fjall::Keyspace;

use crate::routing::shard_hint::shard_hint_for_event;
use crate::shard::fjall_backend::{open_shard_partition, FjallConfig};
use crate::shard::traits::ShardedStateStore;
use crate::shard::Shard;

/// Fjall-backed implementation of `ShardedStateStore`.
///
/// Wraps an `Arc<Keyspace>` + `Vec<Shard>` where each `Shard` owns one
/// `PartitionHandle`. The keyspace is shared-owned (cheap `Arc::clone`) so the
/// boot path may hand the same pointer to `ConcurrentAppState.fjall_keyspace`.
pub struct ShardedStateStoreFjall {
    keyspace: Arc<Keyspace>,
    shards: Vec<Shard>,
}

impl std::fmt::Debug for ShardedStateStoreFjall {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ShardedStateStoreFjall")
            .field("shard_count", &self.shards.len())
            .finish()
    }
}

impl ShardedStateStoreFjall {
    /// Open (or create) N partitions inside `ks` and wrap each in a
    /// `Shard::with_partition`.
    ///
    /// # Panics
    /// Asserts `1 <= n <= 256` (T-53-03B-01 mitigation; matches
    /// `ShardedStateStoreV1::new`'s bound so the invariant is identical under
    /// both backends).
    ///
    /// # Errors
    /// Propagates `fjall::Error` from `open_shard_partition` on IO failure.
    pub fn new(n: u16, ks: Arc<Keyspace>, cfg: &FjallConfig) -> fjall::Result<Self> {
        assert!(n >= 1 && n <= 256, "shard count must be 1..=256");
        let mut shards = Vec::with_capacity(n as usize);
        for i in 0..n as usize {
            let partition = open_shard_partition(&ks, i, cfg)?;
            shards.push(Shard::with_partition(partition));
        }
        Ok(Self {
            keyspace: ks,
            shards,
        })
    }

    /// Return the shard index for a given event's routing key.
    ///
    /// At N=1 always returns 0 (fast-path); otherwise
    /// `(shard_hint_for_event(event, key_field) as usize) % self.shards.len()`.
    /// Identical contract to `ShardedStateStoreV1::shard_index_for_event` so
    /// routing is backend-agnostic.
    pub fn shard_index_for_event(
        &self,
        event: &serde_json::Value,
        key_field: Option<&str>,
    ) -> usize {
        let n = self.shards.len();
        if n == 1 {
            return 0;
        }
        (shard_hint_for_event(event, key_field) as usize) % n
    }

    /// Shared read access to shard `idx`.
    pub fn shard_at(&self, idx: usize) -> &Shard {
        &self.shards[idx]
    }

    /// Mutable read/write access to shard `idx`.
    pub fn shard_at_mut(&mut self, idx: usize) -> &mut Shard {
        &mut self.shards[idx]
    }

    /// Shared handle to the underlying keyspace. The boot path stashes a clone
    /// of this `Arc` inside `ConcurrentAppState.fjall_keyspace` so shutdown
    /// code (e.g. SIGTERM handlers) can call `keyspace.persist(SyncAll)`.
    pub fn keyspace(&self) -> &Arc<Keyspace> {
        &self.keyspace
    }
}

impl ShardedStateStore for ShardedStateStoreFjall {
    fn shard_count(&self) -> u16 {
        self.shards.len() as u16
    }

    fn for_each_shard<F: FnMut(&Shard)>(&self, mut f: F) {
        for s in &self.shards {
            f(s);
        }
    }

    fn for_each_shard_mut<F: FnMut(&mut Shard)>(&mut self, mut f: F) {
        for s in &mut self.shards {
            f(s);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::shard::fjall_backend::{fjall_config_from_env, open_keyspace_from_env};
    use std::sync::{Mutex, OnceLock};

    fn env_lock() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
    }

    fn build_store(n: u16) -> (ShardedStateStoreFjall, tempfile::TempDir) {
        std::env::set_var("BEAVA_FJALL_FSYNC_DISABLE", "1");
        std::env::set_var("BEAVA_FJALL_CACHE_MB", "32");
        let cfg = fjall_config_from_env(n);
        let tmp = tempfile::TempDir::new().expect("tempdir");
        let ks = open_keyspace_from_env(tmp.path(), &cfg).expect("open keyspace");
        let store = ShardedStateStoreFjall::new(n, ks, &cfg).expect("open store");
        (store, tmp)
    }

    #[test]
    fn new_allocates_n_shards() {
        let _g = env_lock().lock().unwrap();
        let (store, _tmp) = build_store(4);
        assert_eq!(store.shard_count(), 4);
    }

    #[test]
    fn shard_index_for_event_at_n1_is_zero() {
        let _g = env_lock().lock().unwrap();
        let (store, _tmp) = build_store(1);
        let idx = store.shard_index_for_event(&serde_json::json!({"k": "x"}), Some("k"));
        assert_eq!(idx, 0);
    }
}
