//! Phase 57 Wave 0 RED test — D-B5 (retraction cascade depth guard).
//! Flips GREEN at Wave 1 when Plan 57-01 lands the depth-capped
//! `ShardOp::RetractDownstream { depth: u8 }` variant + the source-shard
//! guard that returns `BeavaError::RetractionDepthExceeded` at depth ≥ 16.
//!
//! Contract (TPC-CORR-10, Area B-B5): when a downstream row is retracted,
//! its own `contributing_inputs` is walked and fans out further retractions.
//! The cascade depth is capped at 16 hops. Depth 17 MUST raise
//! `BeavaError::RetractionDepthExceeded` + increment
//! `beava_retraction_depth_exceeded_total`. No panic, no deadlock, source
//! shard returns a typed error.
//!
//! Metric-name + error-variant assertions (string probes — grep targets):
//!   - "beava_retraction_depth_exceeded_total"
//!   - "RetractionDepthExceeded"
//!   - "retraction_depth"  (general grep target; allows the Wave 1 impl
//!     to pick its own field name)
//!
//! See .planning/phases/57-retraction-across-crossshard-joins/57-CONTEXT.md
//! Area B-B5 + 57-00-PLAN.md D-B5 hooks.
//!
//! Run:
//!   cargo test --release --test retraction_depth_guard

#![cfg(not(feature = "state-inmem"))]
#![allow(dead_code)]

// ---------------------------------------------------------------------------
// String probes — metric + error-variant names. Wave 1 lands the real
// types; today these stay as `&str` constants so the test harness builds.
// ---------------------------------------------------------------------------

const METRIC_RETRACTION_DEPTH_EXCEEDED: &str = "beava_retraction_depth_exceeded_total";
const ERR_VARIANT_RETRACTION_DEPTH_EXCEEDED: &str = "RetractionDepthExceeded";
const CASCADE_DEPTH_CAP: u8 = 16;
const RETRACTION_DEPTH_FIELD: &str = "retraction_depth";

/// D-B5 — synthetic 20-hop cascade pipeline. Root tombstone triggers a
/// fan-out that must cap at depth 16. Depth 17 raises
/// `BeavaError::RetractionDepthExceeded`; depths 0..=16 succeed.
///
/// Assertion hooks (Wave 1 must satisfy):
///   - Source shard returns `Err(BeavaError::RetractionDepthExceeded)` — NOT a
///     panic.
///   - Exactly ONE increment of `beava_retraction_depth_exceeded_total`
///     per overflow.
///   - 5s timeout on the blocking `recv()` (via
///     `crossbeam_channel::recv_timeout`) — overflow cannot deadlock.
///   - Depth 16 cascades succeed; depth 17 is where the guard trips.
#[test]
#[ignore = "57-W1"]
// flips GREEN in Plan 57-01 (ShardOp::RetractDownstream + depth guard)
fn retraction_cascade_exceeds_16_hop_cap() {
    let _m = METRIC_RETRACTION_DEPTH_EXCEEDED;
    let _err = ERR_VARIANT_RETRACTION_DEPTH_EXCEEDED;
    let _cap = CASCADE_DEPTH_CAP;
    let _field = RETRACTION_DEPTH_FIELD;

    // Wave 1 wires the following. References APIs that do NOT exist
    // today (ShardOp::RetractDownstream { depth }, RetractReason,
    // BeavaError::RetractionDepthExceeded). The `todo!()` + `#[ignore]`
    // together keep the default suite green.
    //
    // Step 1: construct a synthetic pipeline with 20 cascade hops.
    //         Each stream i has `contributing_inputs` referencing
    //         stream (i-1)'s emitted row; this models a linear chain.
    // Step 2: push one event through the root; observe all 20 hops
    //         materialize.
    // Step 3: issue a root retraction (depth=0) — the fan-out walk
    //         hits depth 17 at the 18th stream.
    // Step 4: assert the source shard returns
    //         Err(BeavaError::RetractionDepthExceeded) with a clean
    //         error message naming the overflowing depth (17).
    // Step 5: assert `beava_retraction_depth_exceeded_total` incremented
    //         exactly once.
    // Step 6: assert no panic, no deadlock — the test harness uses
    //         crossbeam_channel::recv_timeout(5s) on every oneshot to
    //         bound runtime.
    todo!(
        "57-W1: implement retraction depth guard; see 57-CONTEXT.md Area B-B5"
    );
}
