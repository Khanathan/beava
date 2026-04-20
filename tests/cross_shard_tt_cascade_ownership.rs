//! Phase 55 Wave 1 GREEN — SC-1 cross-shard TT cascade ownership (TPC-CORR-07).
//!
//! Contract: Stream→Table downstream rows MUST land on the shard owning
//! `hash(output_key) % N`, NOT on the input event's shard. `shard_key=` on
//! streams becomes a pure source-ingress hint only; every downstream
//! cascade shuffles by its own `key_field`.
//!
//! Wave 1 (plan 55-01) lands the CascadeBuffer + per-event cross-shard
//! dispatch with metric emission. These tests flip GREEN here.
//!
//! Run:
//!   cargo test --release --test cross_shard_tt_cascade_ownership -- --ignored --test-threads=1

#![cfg(not(feature = "state-inmem"))]

use beava::routing::shard_hint_for_event;
use beava::shard::read_entity_from_shard;

#[path = "common/mod.rs"]
mod common;

use common::cascade_harness::{
    make_tt_cascade_engine, seed_txn_row, spawn_two_shards,
};

/// Helper — pick (user_id, merchant_id) where `user_id` hashes to shard 0
/// and `merchant_id` hashes to shard 1 at N=2.
fn pick_split_user_merchant() -> (String, String) {
    for u in 0u32..8192 {
        let uu = format!("u{u:04}");
        let uidx = (shard_hint_for_event(&serde_json::json!({ "user_id": uu.clone() }), Some("user_id"))
            as usize) % 2;
        if uidx != 0 { continue; }
        for m in 0u32..8192 {
            let mm = format!("m{m:04}");
            let midx = (shard_hint_for_event(
                &serde_json::json!({ "merchant_id": mm.clone() }),
                Some("merchant_id"),
            ) as usize) % 2;
            if midx == 1 {
                return (uu, mm);
            }
        }
    }
    panic!("no split (user_id, merchant_id) at N=2");
}

/// Helper — pick (user_id, merchant_id) where BOTH hash to shard 0 at N=2.
fn pick_same_shard_user_merchant() -> (String, String) {
    for u in 0u32..8192 {
        let uu = format!("u{u:04}");
        let uidx = (shard_hint_for_event(&serde_json::json!({ "user_id": uu.clone() }), Some("user_id"))
            as usize) % 2;
        if uidx != 0 { continue; }
        for m in 0u32..8192 {
            let mm = format!("m{m:04}");
            let midx = (shard_hint_for_event(
                &serde_json::json!({ "merchant_id": mm.clone() }),
                Some("merchant_id"),
            ) as usize) % 2;
            if midx == 0 {
                return (uu, mm);
            }
        }
    }
    panic!("no same-shard (user_id, merchant_id) at N=2");
}

/// SC-1 primary assertion — downstream MerchantActivity row lives on
/// `hash(merchant_id) % 2` shard and NOT on the input event's shard.
#[test]
#[ignore = "55-W1"]
fn tt_cascade_output_key_lands_on_output_shard() {
    let (user_id, merchant_id) = pick_split_user_merchant();

    let mut harness = spawn_two_shards(65_536);

    // Seed the input Txn table row at user_id on the input shard (shard 0).
    seed_txn_row(&mut harness.input_shard, &user_id, 42);

    let engine = make_tt_cascade_engine();
    let primary_event = serde_json::json!({
        "user_id": user_id.clone(),
        "merchant_id": merchant_id.clone(),
        "amount": 42,
    });

    // Fire cascade — should dispatch to shard 1 (merchant_id's owner).
    engine
        .cascade_table_upsert_on_shard(
            "Txn",
            &user_id,
            false,
            Some(&primary_event),
            &mut harness.input_shard,
            0,
            Some(&harness.shard_handles),
            std::time::SystemTime::now(),
        )
        .expect("cascade ok");

    // Confirm the output did NOT land on the input shard at merchant_id.
    let input_has_m = read_entity_from_shard(
        &harness.input_shard,
        &merchant_id,
        |e| e.table_rows.contains_key("MerchantActivity"),
    )
    .unwrap_or(false);
    assert!(
        !input_has_m,
        "MerchantActivity[{merchant_id}] must NOT land on input shard"
    );

    // Finalize + recover sibling shard.
    let sibling = harness.finish();
    let sibling_has_m = read_entity_from_shard(
        &sibling,
        &merchant_id,
        |e| e.table_rows.contains_key("MerchantActivity"),
    )
    .unwrap_or(false);
    assert!(
        sibling_has_m,
        "MerchantActivity[{merchant_id}] must land on sibling (shard-1 = merchant_id's owner)"
    );
}

/// SC-1 corollary — same-shard fast path MUST write inline (no SPSC hop),
/// inspected via the sibling shard NOT receiving the row.
#[test]
#[ignore = "55-W1"]
fn tt_cascade_same_shard_takes_fast_path() {
    let (user_id, merchant_id) = pick_same_shard_user_merchant();

    let mut harness = spawn_two_shards(65_536);

    seed_txn_row(&mut harness.input_shard, &user_id, 7);

    let engine = make_tt_cascade_engine();
    let primary_event = serde_json::json!({
        "user_id": user_id.clone(),
        "merchant_id": merchant_id.clone(),
        "amount": 7,
    });

    engine
        .cascade_table_upsert_on_shard(
            "Txn",
            &user_id,
            false,
            Some(&primary_event),
            &mut harness.input_shard,
            0,
            Some(&harness.shard_handles),
            std::time::SystemTime::now(),
        )
        .expect("cascade ok");

    // Same-shard: merchant_id's entity lives on shard 0 (input_shard) now.
    let input_has_m = read_entity_from_shard(
        &harness.input_shard,
        &merchant_id,
        |e| e.table_rows.contains_key("MerchantActivity"),
    )
    .unwrap_or(false);
    assert!(
        input_has_m,
        "MerchantActivity[{merchant_id}] must land INLINE on input shard"
    );

    // Sibling shard must NOT have received any cascade (fast path emits
    // no SPSC message).
    let sibling = harness.finish();
    let sibling_has_m = read_entity_from_shard(
        &sibling,
        &merchant_id,
        |e| e.table_rows.contains_key("MerchantActivity"),
    )
    .unwrap_or(false);
    assert!(
        !sibling_has_m,
        "Same-shard fast path must not dispatch to sibling shard"
    );
}
