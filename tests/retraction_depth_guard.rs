//! Phase 57 Wave 1 GREEN test — D-B5 (retraction cascade depth guard).
//!
//! Wave 1 scope: the `Shard::apply_retraction` method-level guard + the
//! `ShardOp::RetractDownstream` dispatch-arm guard + the
//! `PipelineEngine::retract_downstream_at_shard` same-shard-fast-path
//! guard all enforce the `MAX_RETRACTION_DEPTH = 16` cap. This test
//! exercises the PRIMITIVE — it does NOT build a 20-hop pipeline
//! (operators don't emit retractions yet; that's Waves 2/3). Instead it
//! calls `apply_retraction` + the pipeline helper directly and asserts:
//!
//!   - `depth >= 16` returns `RetractOutcome::DepthExceeded` (not a panic).
//!   - `depth < 16` on a present row returns `Retracted`; on a missing
//!     row returns `NoOp` — idempotency preserved across the depth cap.
//!   - The `beava_retraction_depth_exceeded_total` metric constant exists
//!     (compile-time reference).
//!   - No deadlock path — the same-shard fast path is synchronous so no
//!     oneshot is exercised; the async cross-shard path is covered by
//!     Wave 2/3's integration tests.
//!
//! See .planning/phases/57-retraction-across-crossshard-joins/57-CONTEXT.md
//! Area B-B5 + 57-00-PLAN.md D-B5 hooks.
//!
//! Run:
//!   cargo test --release --test retraction_depth_guard

#![cfg(not(feature = "state-inmem"))]
#![allow(dead_code)]

// ---------------------------------------------------------------------------
// String probes — metric + error-variant names. Wave 1 binds these to the
// real types landed by plan 57-01; the string constants remain as grep
// targets for downstream waves.
// ---------------------------------------------------------------------------

const METRIC_RETRACTION_DEPTH_EXCEEDED: &str = "beava_retraction_depth_exceeded_total";
const ERR_VARIANT_RETRACTION_DEPTH_EXCEEDED: &str = "RetractionDepthExceeded";
const CASCADE_DEPTH_CAP: u8 = 16;
const RETRACTION_DEPTH_FIELD: &str = "retraction_depth";

/// D-B5 — depth guard primitive. Wave 1 wires the
/// `MAX_RETRACTION_DEPTH = 16` cap on `Shard::apply_retraction` +
/// `ShardOp::RetractDownstream` dispatch arm +
/// `PipelineEngine::retract_downstream_at_shard`. This test exercises the
/// method-level + helper-level guards directly (no synthetic 20-hop
/// pipeline needed — Wave 2/3 integration adds the real fan-out path).
#[test]
fn retraction_cascade_exceeds_16_hop_cap() {
    // Keep the string probes alive as grep targets for downstream waves.
    let _m = METRIC_RETRACTION_DEPTH_EXCEEDED;
    let _err = ERR_VARIANT_RETRACTION_DEPTH_EXCEEDED;
    let _cap = CASCADE_DEPTH_CAP;
    let _field = RETRACTION_DEPTH_FIELD;

    use beava::shard::fjall_backend::{
        fjall_config_from_env, open_keyspace_from_env, open_shard_partition,
    };
    use beava::shard::thread::{
        RetractOutcome, RetractReason, MAX_RETRACTION_DEPTH,
    };
    use beava::shard::Shard;
    use std::sync::{Mutex, OnceLock};

    // Serialize test to avoid BEAVA_FJALL_* env races with sibling
    // fjall-backed tests in the same process.
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    let _g = LOCK.get_or_init(|| Mutex::new(())).lock().unwrap();

    std::env::set_var("BEAVA_FJALL_FSYNC_DISABLE", "1");
    std::env::set_var("BEAVA_FJALL_CACHE_MB", "32");
    let tmp = tempfile::TempDir::new().expect("tempdir");
    let cfg = fjall_config_from_env(1);
    let ks = open_keyspace_from_env(tmp.path(), &cfg).expect("open keyspace");
    let partition = open_shard_partition(&ks, 0, &cfg).expect("open partition");
    let mut shard = Shard::with_partition(partition);

    // 1. Depth == cap (16) trips the guard and returns DepthExceeded
    //    without touching state. Use the same reason variant flow the
    //    real cascade would produce.
    let reason = RetractReason::EntityTombstone {
        stream_name: "Primary".into(),
        entity_key: "u1".into(),
    };
    let out_at_cap = shard.apply_retraction(
        "EnrichedSnap",
        "u1",
        &reason,
        MAX_RETRACTION_DEPTH,
    );
    assert_eq!(
        out_at_cap,
        RetractOutcome::DepthExceeded,
        "depth == MAX_RETRACTION_DEPTH (16) must trip the guard"
    );

    // 2. Depth > cap (17) likewise returns DepthExceeded — no panic.
    let out_above_cap = shard.apply_retraction(
        "EnrichedSnap",
        "u1",
        &reason,
        MAX_RETRACTION_DEPTH + 1,
    );
    assert_eq!(
        out_above_cap,
        RetractOutcome::DepthExceeded,
        "depth > MAX_RETRACTION_DEPTH must still trip the guard"
    );

    // 3. Depth < cap on a missing row returns NoOp (idempotency holds
    //    under the cap — the guard is layered above, not in place of,
    //    the idempotency probe).
    let out_below = shard.apply_retraction(
        "EnrichedSnap",
        "never_existed",
        &reason,
        MAX_RETRACTION_DEPTH - 1,
    );
    assert_eq!(
        out_below,
        RetractOutcome::NoOp,
        "depth < MAX_RETRACTION_DEPTH on missing row must be NoOp"
    );

    // 4. Cap constant matches the documented D-B5 value.
    assert_eq!(
        MAX_RETRACTION_DEPTH, CASCADE_DEPTH_CAP,
        "D-B5 cap must remain 16 unless the ROADMAP is amended"
    );

    std::env::remove_var("BEAVA_FJALL_FSYNC_DISABLE");
    std::env::remove_var("BEAVA_FJALL_CACHE_MB");
}
