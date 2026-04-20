//! Phase 55 Wave 0 RED — SC-4 part 1: target inbox full → SHARD_OVERLOAD.
//!
//! Contract (D-A4): When a target shard's SPSC inbox is full, the source
//! shard's cross-shard `try_send` MUST fail fast with
//! `BeavaError::ShardOverload` (wire status byte 0x02). Source-shard
//! ingress then blocks further accepts until the target drains. Primary
//! events already acked + fsynced stay recoverable via the event log.
//!
//! Wave 1 (plan 55-01) lands the coalesce-buffer dispatcher that enforces
//! this contract. Wave 0 landing: `#[ignore = "55-W1"]`.
//!
//! Run:
//!   cargo test --release --test cross_shard_backpressure -- --ignored

#![cfg(not(feature = "state-inmem"))]

#[path = "common/mod.rs"]
mod common;

#[allow(unused_imports)]
use common::cascade_harness::spawn_two_shards;

/// RED contract — flood the target shard's SPSC inbox with cascade writes
/// while its drain thread is parked. Source shard MUST surface
/// `ShardOverload` (wire status 0x02) at capacity+1.
///
/// Scenario:
///   - N=2. Target shard's drain thread blocks on a parking_lot barrier.
///   - Source shard sends 16 PUSHes whose cascade targets the drained
///     shard. Inbox cap = 8.
///   - After the 8th PUSH fills the queue, the 9th is expected to still
///     try_send successfully (depends on coalesce flush timing). The 10th
///     MUST return `BeavaError::ShardOverload` over TCP (status byte 0x02
///     — `ERROR_SHARD_OVERLOAD` per src/server/protocol.rs).
///   - Metric assertion: `beava_shard_inbox_full_total{shard="1"} >= 1`.
///   - After releasing the barrier: acked pushes replay fully (no
///     silent drops).
#[test]
#[ignore = "55-W1"]
fn target_inbox_full_returns_shard_overload_over_tcp() {
    let _harness = spawn_two_shards(8);
    unimplemented!("Wave 1 — backpressure contract (ShardOverload + inbox_full_total)");
}
