//! Shard routing module (v1.2 TPC Wave 0).
//!
//! Wave 0: `shard_hint_for_event` — compute routing slot at ingest, discard immediately.
//! Wave 1+: `ShardRouter`, `ShardDispatcher`, SPSC channels (src/shard/runtime.rs).
pub mod scatter;
pub mod shard_hint;
pub use shard_hint::{
    compute_ingest_shard_hint, random_shard_hint, shard_hint_for_event,
    shard_hint_from_shard_key_json,
};
