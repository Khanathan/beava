pub mod event_log;
pub mod eviction;
pub mod eviction_tracker;
pub mod recovery;
pub mod snapshot;
pub mod store;

// Phase 54-04 2026-04-19: the `StreamStore` DashMap re-export has been removed.
// The struct itself is deleted — per-shard fjall partitions replace it on the
// default build; `state-inmem` uses `shard::store::ShardedStateStoreV1`
// (AHashMap, no DashMap).
