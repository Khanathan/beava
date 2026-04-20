//! Phase 55 Wave 0 RED — SC-1 cross-shard TT cascade ownership (TPC-CORR-07).
//!
//! Contract: Stream→Table downstream rows MUST land on the shard owning
//! `hash(output_key) % N`, NOT on the input event's shard. `shard_key=` on
//! streams becomes a pure source-ingress hint only; every downstream
//! cascade shuffles by its own `key_field`.
//!
//! Wave 1 (plan 55-01) lands the end-of-batch coalesce buffer + cross-shard
//! delivery cursor that flips these tests GREEN.
//!
//! Run:
//!   cargo test --release --test cross_shard_tt_cascade_ownership -- --ignored
//!
//! Wave 0 status: RED — tests compile but `#[ignore = "55-W1"]`'d pending
//! Wave 1 implementation.

#![cfg(not(feature = "state-inmem"))]

#[path = "common/mod.rs"]
mod common;

#[allow(unused_imports)]
use common::cascade_harness::{
    hash_key_to_shard, pick_two_keys_hashing_to_different_shards,
    pick_two_keys_hashing_to_same_shard, spawn_two_shards,
};

/// SC-1 primary assertion — downstream row lives on `hash(output_key) % N`
/// shard at N=8, and NOWHERE else.
///
/// Pipeline:
///   Txn(shard_key=user_id) → MerchantActivity(key=merchant_id, agg=sum(amount))
///
/// Scenario:
///   - u1 and m_X hash to DIFFERENT shards at N=8.
///   - PUSH Txn {user_id: "u1", merchant_id: "m_X", amount: 10.0}.
///   - Assertion: `read_entity_from_shard(hash(m_X)%8, "m_X").is_some()`
///                AND `read_entity_from_shard(other, "m_X").is_none()` for
///                all 7 other shards.
///   - Metric assertion: `beava_cascade_cross_shard_total{src=u_shard,
///     tgt=mX_shard} >= 1`, and intra_shard counter did NOT increment.
#[test]
#[ignore = "55-W1"]
fn tt_cascade_output_key_lands_on_output_shard() {
    let n = 8usize;
    let (u_key, m_key) = pick_two_keys_hashing_to_different_shards(n);
    let u_shard = hash_key_to_shard(&u_key, n);
    let m_shard = hash_key_to_shard(&m_key, n);
    assert_ne!(u_shard, m_shard, "harness precondition: split shards");

    let _harness = spawn_two_shards(65_536);
    unimplemented!("Wave 1 lands cascade — this is the RED contract for SC-1");
}

/// SC-1 corollary — same-shard fast path MUST NOT go through the
/// scatter-gather cross-shard dispatcher.
///
/// Scenario:
///   - u2 and m_Y hash to the SAME shard at N=8.
///   - PUSH Txn {user_id: "u2", merchant_id: "m_Y", amount: 7.0}.
///   - Metric assertion: `beava_cascade_intra_shard_total{shard=u2_shard}`
///     increments; `beava_cascade_cross_shard_total{..}` does NOT
///     increment for this event.
#[test]
#[ignore = "55-W1"]
fn tt_cascade_same_shard_takes_fast_path() {
    let n = 8usize;
    let (u_key, m_key) = pick_two_keys_hashing_to_same_shard(n);
    assert_eq!(
        hash_key_to_shard(&u_key, n),
        hash_key_to_shard(&m_key, n),
        "harness precondition: same-shard"
    );

    let _harness = spawn_two_shards(65_536);
    unimplemented!("Wave 1 — same-shard fast-path assertion");
}
