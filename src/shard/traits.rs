//! ShardedStateStore trait (v1.2 TPC Wave 1 — D-01).
//!
//! The trait is the flexibility hedge: Waves 2-4 can introduce alternate impls
//! ([Shard; N] fixed-array, sled-backed) without rewriting callers.

use super::Shard;

/// Abstraction over per-shard state storage.
///
/// Implementors: `ShardedStateStoreV1` (Vec<Arc<Mutex<Shard>>> + router).
/// Future impls: fixed-array `[Shard; N]`, sled-backed.
pub trait ShardedStateStore: Send + Sync {
    /// Number of shards allocated.
    fn shard_count(&self) -> u16;

    /// Call `f` for each shard in order.
    fn for_each_shard<F: FnMut(&Shard)>(&self, f: F);

    /// Call `f` for each shard mutably in order.
    fn for_each_shard_mut<F: FnMut(&mut Shard)>(&mut self, f: F);
}
