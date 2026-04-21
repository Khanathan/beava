//! Phase 56 SC-1 — EnrichFromTable cross-shard read correctness (TPC-CORR-08).
//!
//! Contract: `EnrichFromTable` MUST return the correct enrichment regardless
//! of which shard the driving event lands on. When the right-side key hashes
//! to a different shard than the current shard, the operator dispatches
//! `ShardOp::ReadEntityAt { target_shard, table_name, key, reply }` (single
//! key) or `ShardOp::ReadEntityBatch { .. }` (per-target coalesced) and blocks
//! the source shard on the oneshot reply. When `hash(key) % N == current_shard`,
//! the operator reads directly from the local `PartitionHandle` (same-shard
//! fast path — zero inbox hop).
//!
//! RED until Wave 2 (plan 56-02) wires `ShardOp::ReadEntityAt` into
//! `src/shard/thread.rs` and `EnrichFromTable` in `src/engine/operators.rs`.
//! Passes at Wave 2.
//!
//! Run:
//!   cargo test --release --test cross_shard_enrich_from_table -- --ignored --test-threads=1

#![cfg(not(feature = "state-inmem"))]

use ahash::AHasher;
use std::hash::{Hash, Hasher};

/// Copy of the helper used in `tests/cross_shard_tt_cascade_ownership.rs` —
/// deterministic shard assignment for a given string key.
#[allow(dead_code)]
fn hash_to_shard(key: &str, n_shards: usize) -> usize {
    let mut h = AHasher::default();
    key.hash(&mut h);
    (h.finish() % n_shards as u64) as usize
}

/// SC-1 primary — a Txn event on shard-J joined against a Countries
/// source-table row owned by shard-K (J≠K) MUST return an enriched output
/// containing the Country columns (gdp_usd=800_000).
///
/// Wave 2 acceptance: the test harness registers a 4-shard engine, UPSERTs
/// `Countries{country_code="CH", gdp_usd=800_000}` via the source-table
/// dispatch (which routes to `hash("CH") % 4 = K`), registers
/// `Txns(shard_key=user_id)` with `enrich_from(Countries, on=country_code)`,
/// then pushes `{user_id: uJ, country_code: "CH"}` where
/// `hash(uJ) % 4 != K`. Expected output feature map contains `gdp_usd == 800_000`.
///
/// Today (pre-Wave-2): `EnrichFromTable` returns `Missing` for cross-shard
/// lookups because the operator reads only its own shard's partition handle
/// — so the assertion `gdp_usd == 800_000` fails. This test is the RED
/// contract that Wave 2 flips GREEN by adding `ShardOp::ReadEntityAt`
/// dispatch in the operator path.
///
/// Wave 2 assertion hooks (executor MUST wire at Wave 2):
///   - `read_entity_from_shard(&shard_K, "CH", ...)` returns Some with
///     gdp_usd=800_000 (source-table row placement).
///   - Output of `engine.push(...)` on shard J contains
///     `features["gdp_usd"] == 800_000`.
///   - `beava_enrich_cross_shard_total{table="Countries"}` counter ≥ 1.
#[test]
#[ignore = "56-W2"]
fn enrich_from_table_crosses_shard_boundary() {
    const N: usize = 4;

    // Find a country_code with known shard assignment and a user_id whose
    // shard differs — these are the ingredients for the cross-shard read.
    let country = "CH";
    let k = hash_to_shard(country, N);
    let mut user = String::new();
    for i in 0u32..8192 {
        let candidate = format!("u_{i}");
        if hash_to_shard(&candidate, N) != k {
            user = candidate;
            break;
        }
    }
    assert!(!user.is_empty(), "no user_id found with shard != {k}");
    let j = hash_to_shard(&user, N);
    assert_ne!(j, k, "test precondition: J != K");

    // Wave 2 wiring goes here:
    //   1. Build 4-shard engine (mirror pattern in tests/cross_shard_tt_cascade_ownership.rs).
    //   2. Register source_table "Countries" with key="country_code".
    //   3. UPSERT_TABLE_ROW{country_code:"CH", gdp_usd:800_000} via the
    //      source-table dispatch (Phase 55 Wave 2 wire path).
    //   4. Register stream "Txns" with shard_key="user_id" and feature
    //      `gdp_usd = enrich_from(Countries, on=country_code).gdp_usd`.
    //   5. Push {user_id: user, country_code: "CH"} on shard J.
    //   6. Assert the stream output features contain gdp_usd == 800_000.
    //   7. Assert `beava_enrich_cross_shard_total{table="Countries"}` ≥ 1.
    todo!(
        "56-W2: wire full 4-shard enrich fixture. Contract asserts \
         output features contain gdp_usd=800_000 for Txn on shard J={j} \
         with country_code=CH on shard K={k}."
    );
}

/// SC-1 corollary — the same-shard fast path MUST still work (no cross-shard
/// dispatch metric increment when J == K).
///
/// Wave 2 assertion hooks:
///   - `beava_enrich_intra_shard_total{table="Countries"}` ≥ 1.
///   - `beava_enrich_cross_shard_total{table="Countries"}` == 0 (for this test).
///   - Output features contain gdp_usd=800_000.
#[test]
#[ignore = "56-W2"]
fn enrich_from_table_same_shard_fast_path() {
    const N: usize = 4;

    // Find a (user_id, country_code) pair where both hash to the SAME shard —
    // this exercises the local-read fast path.
    let country = "CH";
    let k = hash_to_shard(country, N);
    let mut user = String::new();
    for i in 0u32..8192 {
        let candidate = format!("u_{i}");
        if hash_to_shard(&candidate, N) == k {
            user = candidate;
            break;
        }
    }
    assert!(!user.is_empty(), "no user_id found with shard == {k}");

    // Wave 2: same fixture as above, but push event where
    // hash(user_id) % N == hash(country_code) % N. Assert:
    //   - `beava_enrich_intra_shard_total{table="Countries"}` ≥ 1.
    //   - `beava_enrich_cross_shard_total{table="Countries"}` unchanged.
    //   - Output contains gdp_usd=800_000.
    todo!(
        "56-W2: same-shard fast path fixture. Contract asserts zero \
         cross-shard hops for co-located user_id={user} and country_code={country} \
         (both on shard {k})."
    );
}
