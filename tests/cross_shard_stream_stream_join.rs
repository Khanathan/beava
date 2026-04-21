//! Phase 56 SC-2 — StreamStreamJoin buffer lives on `hash(join.on) % N`
//! (TPC-CORR-09).
//!
//! Contract: `StreamStreamJoin` with mismatched left/right `shard_key=`
//! declarations MUST produce correct joined events by routing both sides to
//! the shard owning `hash(join.on) % N`. The buffer lives on the join-owning
//! shard in a dedicated fjall partition `ssj-<join_id>/`. Source-shard
//! dispatch: accumulate per-batch, coalesce, `try_send`
//! `ShardOp::SsjInsert { join_id, side: Left|Right, join_key, event, reply }`
//! to the target shard; the target evaluates the match inline and emits any
//! joined output via the existing (Phase 55) cascade path.
//!
//! When `shard_key=join.on` on both sides (co-located), no relaxation applies
//! — no extra hop.
//!
//! RED until Wave 3 (plan 56-03) relocates the SSJ buffer and adds
//! `ShardOp::SsjInsert`. Passes at Wave 3.
//!
//! Run:
//!   cargo test --release --test cross_shard_stream_stream_join -- --ignored --test-threads=1

#![cfg(not(feature = "state-inmem"))]

use ahash::AHasher;
use std::hash::{Hash, Hasher};

#[allow(dead_code)]
fn hash_to_shard(key: &str, n_shards: usize) -> usize {
    let mut h = AHasher::default();
    key.hash(&mut h);
    (h.finish() % n_shards as u64) as usize
}

/// SC-2 primary — `LeftStream(shard_key=user_id)` × `RightStream(shard_key=session_id)`
/// joined `on user_id` MUST produce the matched output on the shard owning
/// `hash(user_id) % N`, NOT on the shard owning `hash(session_id) % N`.
///
/// Wave 3 acceptance: register L(shard_key=user_id) and R(shard_key=session_id)
/// with StreamStreamJoin on `user_id` and `within_ms=60_000`. Push
/// `L{user_id:"u1", session_id:"s1", payload:"left"}` then
/// `R{user_id:"u1", session_id:"s1", payload:"right"}`. The join output
/// entity MUST live on shard `J = hash("u1") % 4`, and the shard owning
/// `hash("s1") % 4` MUST NOT have a join buffer entry for u1.
///
/// Today (pre-Wave-3): the SSJ buffer is co-resident with the source event's
/// shard; left side writes on `hash(user_id)` (accidentally correct), but
/// right side writes on `hash(session_id)` — so the match never fires
/// because left/right buffers live on different shards. This test RED's
/// that broken behaviour.
///
/// Wave 3 assertion hooks:
///   - `read_entity_from_shard(shard=J, key="u1", ...)` returns the joined
///     output (contains both `payload_left="left"` and `payload_right="right"`).
///   - `read_entity_from_shard(shard=hash("s1")%N, key="s1", ...)` returns None
///     for the join output (no buffer leaked to session_id shard).
///   - `beava_ssj_cross_shard_total{join_id=...}` counter ≥ 2 (one per side).
#[test]
#[ignore = "56-W3"]
fn stream_stream_join_routes_to_join_key_shard() {
    const N: usize = 4;

    let user_id = "u1";
    let session_id = "s1";
    let j = hash_to_shard(user_id, N);
    let s = hash_to_shard(session_id, N);

    // Test precondition — the two keys MUST hash to different shards for
    // the test to be meaningful. If they happen to collide at this fixed
    // pair, pick an alternate session_id that differs.
    let (session_id, s) = if j == s {
        let mut alt = String::new();
        let mut alt_shard = j;
        for i in 0u32..4096 {
            let candidate = format!("s{i}");
            let c_shard = hash_to_shard(&candidate, N);
            if c_shard != j {
                alt = candidate;
                alt_shard = c_shard;
                break;
            }
        }
        assert!(!alt.is_empty(), "no alternate session_id found");
        (alt, alt_shard)
    } else {
        (session_id.to_string(), s)
    };
    assert_ne!(j, s, "test precondition: hash(user_id) != hash(session_id)");

    // Wave 3 wiring goes here:
    //   1. Build 4-shard engine.
    //   2. Register stream L(shard_key=user_id).
    //   3. Register stream R(shard_key=session_id).
    //   4. Register StreamStreamJoin(L, R, on="user_id", within_ms=60_000).
    //   5. Push L{user_id:"u1", session_id:"s1", payload:"left"}
    //      (lands on hash(user_id) % 4 = J per source-ingress routing).
    //   6. Push R{user_id:"u1", session_id:"s1", payload:"right"}
    //      (lands on hash(session_id) % 4 = s per source-ingress routing).
    //   7. Both L and R buffers dispatched to shard J via ShardOp::SsjInsert.
    //   8. Assert read_entity_from_shard(shard=J, key="u1") returns join output.
    //   9. Assert read_entity_from_shard(shard=s, key="s1") returns None for join.
    //  10. Assert beava_ssj_cross_shard_total{join_id=...} >= 2.
    todo!(
        "56-W3: wire 4-shard SSJ fixture. Contract asserts join output on \
         shard J={j} (owner of hash(user_id='{user_id}') % 4), NOT on shard s={s} \
         (owner of hash(session_id='{session_id}') % 4)."
    );
}

/// SC-2 corollary — co-located case (both sides already declare
/// `shard_key=user_id`) MUST produce the match without any cross-shard hop.
///
/// Wave 3 assertion hooks:
///   - Join output on shard J.
///   - `beava_ssj_cross_shard_total{join_id=...}` == 0 (no cross-shard dispatch).
#[test]
#[ignore = "56-W3"]
fn stream_stream_join_colocated_fast_path() {
    const N: usize = 4;

    let user_id = "u1";
    let j = hash_to_shard(user_id, N);

    // Wave 3: same fixture as above but both L and R declare
    // shard_key=user_id. Assert zero cross-shard hops; output on shard J.
    todo!(
        "56-W3: co-located SSJ fixture. Contract asserts \
         beava_ssj_cross_shard_total == 0 for shard_key=user_id on both sides \
         (join output on shard J={j})."
    );
}
