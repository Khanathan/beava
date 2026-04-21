//! Phase 57 Wave 0 RED test — SC-3 (late retractions skip + warn).
//! Flips GREEN at Wave 4 when Plan 57-04 lands the history_ttl guard +
//! `/debug/warnings.retraction_beyond_history` surface + dedup cadence.
//!
//! Contract (TPC-CORR-10, Area C-C1..C-C3): retractions apply only to
//! events whose `primary_event_id` is within `history_ttl` of the target
//! shard's current watermark. Events older than that are NOT retractable —
//! skip with `tracing::warn!("RetractionBeyondHistory: ...")`, increment
//! `beava_retraction_beyond_history_total{operator}`, surface via
//! `GET /debug/warnings` dedup'd at 60s per (operator, reason_class).
//! State on the target shard MUST remain unchanged.
//!
//! Metric-name + surface assertions (string probes — grep targets for Wave 4):
//!   - "beava_retraction_beyond_history_total"
//!   - "RetractionBeyondHistory"
//!   - "retraction_beyond_history"  (JSON field in /debug/warnings)
//!
//! See .planning/phases/57-retraction-across-crossshard-joins/57-CONTEXT.md
//! Area C (D-C1/D-C2/D-C3) + 57-00-PLAN.md SC-3 hooks.
//!
//! Run:
//!   cargo test --release --test late_retraction_warning

#![cfg(not(feature = "state-inmem"))]
#![allow(dead_code)]

// ---------------------------------------------------------------------------
// String probes — metric names + surface fields. Wave 4 asserts these on
// the live metric registry + /debug/warnings response body. Today they are
// just string constants so grep (57-00-PLAN acceptance) finds them.
// ---------------------------------------------------------------------------

const METRIC_RETRACTION_BEYOND_HISTORY: &str = "beava_retraction_beyond_history_total";
const REASON_BEYOND_HISTORY: &str = "RetractionBeyondHistory";
const WARNING_FIELD: &str = "retraction_beyond_history";
const DEDUP_CADENCE_SECS: u64 = 60;

/// SC-3 — push an event with `event_time = watermark - history_ttl - 1s`
/// and attempt to retract it. The retraction MUST be skipped (target
/// shard state unchanged) + the warning surface MUST reflect it.
///
/// Assertion hooks (Wave 4 must satisfy):
///   - `beava_retraction_beyond_history_total{operator="<op>"} ≥ 1` after one
///     call.
///   - `GET /debug/warnings` body has a top-level / nested
///     `retraction_beyond_history: [{operator, reason_class, count}]` array
///     with ≥ 1 entry.
///   - Calling the out-of-window retraction 100× within the 60s dedup
///     window yields exactly 1 warning-feed entry (count aggregated to 100,
///     not 100 separate entries).
///   - Target-shard state for the late event is BYTE-IDENTICAL before and
///     after the retraction attempt.
#[test]
#[ignore = "57-W4"]
// flips GREEN in Plan 57-04 (history_ttl guard + /debug/warnings surface)
fn late_retraction_beyond_history_is_skipped_and_warned() {
    // Compile-time probes — must live on the stack so the compiler doesn't
    // strip them before grep (57-00-PLAN acceptance) sees them in the
    // rlib/bin.
    let _m = METRIC_RETRACTION_BEYOND_HISTORY;
    let _reason = REASON_BEYOND_HISTORY;
    let _field = WARNING_FIELD;
    let _cadence = DEDUP_CADENCE_SECS;

    // Wave 4 wires the following steps. They reference APIs that do NOT
    // exist today (engine.delete_source_table_row_with_event_time,
    // engine.retract_entity_with_event_time, /debug/warnings JSON schema
    // for retraction_beyond_history). The `todo!()` + `#[ignore]` marker
    // together keep the default suite green until Wave 4 implements them.
    //
    // Step 1: build 4-shard engine with StreamDefinition.history_ttl = 60s.
    // Step 2: advance watermark to T = now().
    // Step 3: attempt a retraction for an event whose primary_event_id
    //         maps to event_time = T - 3600s (far beyond 60s history_ttl).
    // Step 4: assert target-shard state unchanged AND the counter
    //         `beava_retraction_beyond_history_total{operator=...}` incremented
    //         by exactly 1.
    // Step 5: repeat Step 3 99 more times within the 60s dedup window.
    //         Assert the `/debug/warnings.retraction_beyond_history`
    //         array still has exactly 1 entry (aggregated; not 100).
    todo!(
        "57-W4: implement late-retraction warning path; see 57-CONTEXT.md Area C"
    );
}
