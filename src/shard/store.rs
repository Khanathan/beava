//! ShardedStateStoreV1: Vec<Arc<Mutex<Shard>>> + router (v1.2 TPC Wave 1).
//!
//! At N=1, the router always returns index 0. The `shard_for_event` method
//! computes `shard_hint_for_event(event, key_field) % shard_count` to pick
//! the owning shard. At N=1 this is `hint % 1 == 0` — always shard 0.
//!
//! ## Phase 53-03 (D-03): state-inmem feature gate
//!
//! The entire Phase 49 legacy path is gated behind the `state-inmem` Cargo
//! feature. Production / default builds ship the fjall-backed
//! `ShardedStateStoreFjall` (Plan 03B) instead. This file's file-level
//! attribute below ensures the default build skips compiling this module
//! entirely — avoiding both the `Shard::new()` dependency (now gated) and
//! any accidental routing through AHashMap code in a fjall build.

#![cfg(feature = "state-inmem")]

use std::sync::{Arc, Mutex};

use super::traits::ShardedStateStore;
use super::Shard;
use crate::routing::shard_hint::shard_hint_for_event;

/// Concrete Wave 1 impl: Vec<Arc<Mutex<Shard>>> + hint-based router.
///
/// Arc<Mutex<>> wrapping: necessary because `Vec<Arc<Shard>>` alone does not
/// allow mutation from callers that hold a shared reference to the store.
/// At N=1 on a single thread, the Mutex is uncontended — zero overhead.
/// Wave 2 replaces this with per-shard pinned threads + message passing, at
/// which point the Mutex is removed from the hot path.
pub struct ShardedStateStoreV1 {
    shards: Vec<Arc<Mutex<Shard>>>,
}

impl std::fmt::Debug for ShardedStateStoreV1 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ShardedStateStoreV1")
            .field("shard_count", &self.shards.len())
            .finish()
    }
}

/// Phase 55-02 D-B5 (TPC-SOURCE-01): full-replace upsert wrapper for a
/// source-table row against a `ShardedStateStoreV1` (state-inmem path).
/// Delegates to `Shard::upsert_source_table_row` on the shard owning
/// `hash(key) % N`. Never fires cascade (D-B6).
///
/// This free function exists alongside the `Shard`-level method in
/// `src/shard/mod.rs` to keep a legacy-friendly entry point — routing at
/// the store-level is convenient for harness tests that hold a
/// `ShardedStateStoreV1` directly.
pub fn upsert_source_table_row(
    store: &ShardedStateStoreV1,
    key: &str,
    table_name: &str,
    fields: ahash::AHashMap<String, crate::types::FeatureValue>,
    source_lsn: u64,
    now: std::time::SystemTime,
) {
    use serde_json::json;
    let mut guard = store.shard_for_event(&json!({ "__k": key }), Some("__k"));
    guard.upsert_source_table_row(key, table_name, fields, source_lsn, now);
}

/// Phase 55-02 D-B5: hard-delete wrapper. Caller is responsible for
/// writing the `PendingRetraction` marker via the event log.
pub fn delete_source_table_row(
    store: &ShardedStateStoreV1,
    key: &str,
    table_name: &str,
    now: std::time::SystemTime,
) -> bool {
    use serde_json::json;
    let mut guard = store.shard_for_event(&json!({ "__k": key }), Some("__k"));
    guard.delete_source_table_row(key, table_name, now)
}

impl ShardedStateStoreV1 {
    /// Allocate `n` empty shards.
    pub fn new(n: u16) -> Self {
        assert!(n >= 1 && n <= 256, "shard count must be 1..=256");
        let shards = (0..n as usize)
            .map(|_| Arc::new(Mutex::new(Shard::new())))
            .collect();
        ShardedStateStoreV1 { shards }
    }

    /// Return the shard index for a given event's primary key.
    /// At N=1, always returns 0.
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

    /// Acquire a lock-guarded reference to the shard for a given event.
    /// At N=1, always returns shard 0's guard.
    pub fn shard_for_event(
        &self,
        event: &serde_json::Value,
        key_field: Option<&str>,
    ) -> std::sync::MutexGuard<'_, Shard> {
        let idx = self.shard_index_for_event(event, key_field);
        self.shards[idx].lock().expect("shard mutex poisoned")
    }

    /// Direct access to shard by index (for testing and for_each patterns).
    pub fn shard_at(&self, idx: usize) -> std::sync::MutexGuard<'_, Shard> {
        self.shards[idx].lock().expect("shard mutex poisoned")
    }
}

impl ShardedStateStore for ShardedStateStoreV1 {
    fn shard_count(&self) -> u16 {
        self.shards.len() as u16
    }

    fn for_each_shard<F: FnMut(&Shard)>(&self, mut f: F) {
        for arc in &self.shards {
            let guard = arc.lock().expect("shard mutex poisoned");
            f(&guard);
        }
    }

    fn for_each_shard_mut<F: FnMut(&mut Shard)>(&mut self, mut f: F) {
        for arc in &mut self.shards {
            let mut guard = arc.lock().expect("shard mutex poisoned");
            f(&mut guard);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn new_n1_allocates_one_shard() {
        let store = ShardedStateStoreV1::new(1);
        assert_eq!(store.shards.len(), 1);
    }

    #[test]
    fn shard_count_returns_1() {
        let store = ShardedStateStoreV1::new(1);
        assert_eq!(store.shard_count(), 1);
    }

    #[test]
    fn shard_for_event_at_n1_always_shard0() {
        let store = ShardedStateStoreV1::new(1);
        // Any key must route to shard 0 at N=1.
        let idx = store.shard_index_for_event(&json!({"user_id": "alice"}), Some("user_id"));
        assert_eq!(idx, 0);
        let idx2 = store.shard_index_for_event(&json!({"user_id": "bob"}), Some("user_id"));
        assert_eq!(idx2, 0);
    }

    #[test]
    fn for_each_shard_called_once_at_n1() {
        let store = ShardedStateStoreV1::new(1);
        let mut count = 0usize;
        store.for_each_shard(|_shard| {
            count += 1;
        });
        assert_eq!(count, 1, "for_each_shard visits exactly one shard at N=1");
    }

    #[test]
    fn shard_state_is_ahashmap_not_dashmap() {
        // Structural test: Shard.state must be AHashMap.
        let shard = Shard::new();
        let mut s = shard.state;
        s.insert(
            "k".to_string(),
            crate::state::store::EntityState::default(),
        );
        assert_eq!(s.len(), 1);
    }

    #[test]
    fn shard_dirty_set_is_hashset() {
        let mut shard = Shard::new();
        shard.dirty_set.insert("k".to_string());
        assert!(shard.dirty_set.contains("k"));
    }
}
