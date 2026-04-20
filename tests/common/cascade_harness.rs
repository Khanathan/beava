//! Phase 55 Wave 0 — shared cascade harness for RED tests.
//!
//! This module provides the minimal surface that Wave 0 RED test files
//! reference. Every helper is a stub today: Wave 1 flips them to real
//! implementations when the end-of-batch coalesce buffer + cross-shard
//! delivery cursor land.
//!
//! Contract (called out in 55-00-PLAN.md):
//!   - `spawn_two_shards(inbox_cap)` — build a two-shard fixture with a fake
//!     sibling drain thread, mirroring `tests/cross_shard_tt_cascade.rs`
//!     (Phase 54 Wave 2 template).
//!   - `hash_key_to_shard(key, n)` — compute routing slot via
//!     `shard_hint_for_event` (same as production routing).
//!   - `drain_sibling_inbox(rx, state)` — spawn a drain-thread servicing
//!     `ShardOp::UpsertTableRow` + `TombstoneTableRow` from the fake
//!     sibling's inbox.
//!   - `pick_two_keys_hashing_to_different_shards(n)` — deterministic
//!     search for (k1, k2) where `hash_key_to_shard(k1, n) !=
//!     hash_key_to_shard(k2, n)`. Used by SC-1 RED tests.
//!
//! Wave 1 replaces every `unimplemented!()` below with real wiring; RED
//! tests consume this surface unchanged.

#![allow(dead_code)]

use std::sync::Arc;
use std::thread::JoinHandle;

use beava::shard::thread::{ShardEvent, ShardHandle};

/// Two-shard fixture handle. Wave 1 fills in engine state + shard handles
/// + drain threads; Wave 0 only needs the type to exist so tests compile.
pub struct TwoShardHarness {
    /// Shared engine/app state pointer (Arc<ConcurrentAppState> in
    /// production). Wave 1 wires this to a real `make_concurrent_state_full`.
    pub state: Arc<beava::server::tcp::ConcurrentAppState>,
    /// Per-shard handles — two entries (shard 0, shard 1).
    pub shard_handles: Vec<ShardHandle>,
    /// Fake sibling drain threads — one per sibling (all shards except
    /// the source). Holds the JoinHandle so the test can join at end.
    pub drain_threads: Vec<JoinHandle<()>>,
}

/// Spawn a two-shard harness with bounded(inbox_cap) SPSC inboxes.
///
/// Wave 0: returns `unimplemented!()` — no test calls this at runtime
/// (every caller is `#[ignore = "55-W{N}"]`'d). The function signature
/// is the contract Wave 1 must honor.
pub fn spawn_two_shards(_inbox_cap: usize) -> TwoShardHarness {
    unimplemented!("Wave 1 — spawn_two_shards implementation lands with the coalesce buffer");
}

/// Compute the shard slot for a given key under N-way sharding. Uses the
/// same routing primitive as production ingest (`shard_hint_for_event`)
/// so harness-level shard assignments match the engine's runtime routing.
///
/// Keyed as `{ "__k": key }` so `shard_hint_for_event(.., Some("__k"))`
/// sees the full key string verbatim.
pub fn hash_key_to_shard(key: &str, n: usize) -> usize {
    let ev = serde_json::json!({ "__k": key });
    (beava::routing::shard_hint_for_event(&ev, Some("__k")) as usize) % n.max(1)
}

/// Spawn a drain thread for a fake sibling shard. Reads `ShardEvent`s
/// from `rx`, applies `UpsertTableRow` / `TombstoneTableRow` against a
/// local `Shard`, and replies `ShardResult::SetOk` on the oneshot.
///
/// Wave 0: stub. Wave 1 mirrors the pattern in
/// `tests/cross_shard_tt_cascade.rs::spawn_drain_thread`.
pub fn drain_sibling_inbox(
    _rx: crossbeam_channel::Receiver<ShardEvent>,
    _state: Arc<beava::server::tcp::ConcurrentAppState>,
) -> JoinHandle<()> {
    unimplemented!("Wave 1 — drain_sibling_inbox implementation");
}

/// Deterministic search for a `(k1, k2)` pair whose shard slots differ
/// under N-way sharding. At N=2 the first few candidates hit (~50%
/// probability per pair); loop bounds are generous for N up to 64.
pub fn pick_two_keys_hashing_to_different_shards(n: usize) -> (String, String) {
    for i in 0u32..4096 {
        for j in 0u32..4096 {
            let k1 = format!("k{i:04}");
            let k2 = format!("m{j:04}");
            if hash_key_to_shard(&k1, n) != hash_key_to_shard(&k2, n) {
                return (k1, k2);
            }
        }
    }
    panic!("pick_two_keys_hashing_to_different_shards: no pair found at n={n}");
}

/// Deterministic search for a `(k1, k2)` pair whose shard slots are
/// equal under N-way sharding. Companion to the split-pair finder above;
/// used by the same-shard fast-path RED test.
pub fn pick_two_keys_hashing_to_same_shard(n: usize) -> (String, String) {
    for i in 0u32..4096 {
        for j in 0u32..4096 {
            let k1 = format!("u{i:04}");
            let k2 = format!("v{j:04}");
            if hash_key_to_shard(&k1, n) == hash_key_to_shard(&k2, n) {
                return (k1, k2);
            }
        }
    }
    panic!("pick_two_keys_hashing_to_same_shard: no pair found at n={n}");
}
