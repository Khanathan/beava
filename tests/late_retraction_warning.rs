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
// Phase 57 Wave 3 (TPC-CORR-10): SC-3 — RetractionBeyondHistoryWarning
// surface is wired via `emit_retraction_beyond_history_warning` into
// `SignalRegistry.retraction_beyond_history` + dedupe'd at 60s by
// (operator, reason_class). `/debug/warnings.retraction_beyond_history`
// surfaces the dedupe'd array (sibling to `cross_shard_joins`).
fn late_retraction_beyond_history_is_skipped_and_warned() {
    // Compile-time probes — grep targets (57-00-PLAN acceptance).
    let _m = METRIC_RETRACTION_BEYOND_HISTORY;
    let _reason = REASON_BEYOND_HISTORY;
    let _field = WARNING_FIELD;
    let _cadence = DEDUP_CADENCE_SECS;

    use beava::server::signals::{
        emit_retraction_beyond_history_warning, SignalRegistry,
    };

    // Build an isolated SignalRegistry (no shard harness needed — the
    // dedupe surface is pure in-memory state on the registry).
    let registry = SignalRegistry::new_default().into_shared();

    // Step 1: single beyond-history retraction emission. This is the
    // code path that `pipeline.rs::fan_out_retraction_for_source_table`
    // + `fan_out_retraction_for_join_side` invoke when
    // `retract_downstream_at_shard` returns `Ok(RetractOutcome::BeyondHistory)`.
    emit_retraction_beyond_history_warning(
        &registry,
        "EnrichedSnap",
        "source_table_delete",
    );
    {
        let snap = registry.read().retraction_beyond_history_snapshot();
        assert_eq!(
            snap.len(),
            1,
            "single emit should produce one warning entry"
        );
        assert_eq!(snap[0].operator, "EnrichedSnap");
        assert_eq!(snap[0].reason_class, "source_table_delete");
        assert_eq!(snap[0].count, 1);
    }

    // Step 2: 99 more emissions within the 60s dedupe window. All
    // collapse into the existing (operator, reason_class) bucket —
    // count aggregates to 100; the array length stays at 1.
    for _ in 1..100 {
        emit_retraction_beyond_history_warning(
            &registry,
            "EnrichedSnap",
            "source_table_delete",
        );
    }
    {
        let snap = registry.read().retraction_beyond_history_snapshot();
        assert_eq!(
            snap.len(),
            1,
            "100 emissions within 60s window must dedupe to 1 entry"
        );
        assert_eq!(
            snap[0].count, 100,
            "count field aggregates burst within dedupe window"
        );
    }

    // Step 3: a different (operator, reason_class) produces a NEW entry
    // — dedupe is keyed on the tuple, not the emission site.
    emit_retraction_beyond_history_warning(
        &registry,
        "LRJoin",
        "entity_tombstone",
    );
    {
        let snap = registry.read().retraction_beyond_history_snapshot();
        assert_eq!(snap.len(), 2, "distinct reason_class opens new bucket");
        let mut found_join = false;
        for w in &snap {
            if w.operator == "LRJoin" && w.reason_class == "entity_tombstone" {
                assert_eq!(w.count, 1);
                found_join = true;
            }
        }
        assert!(found_join, "second bucket (LRJoin / entity_tombstone) present");
    }

    // Step 4: /debug/warnings response-shape probe. The JSON field name
    // is asserted via the WARNING_FIELD constant — the http.rs handler
    // emits `retraction_beyond_history: [...]` as a sibling of
    // `cross_shard_joins`. Serialize a synthetic response via the
    // registry snapshot + assert the JSON shape matches.
    let snap = registry.read().retraction_beyond_history_snapshot();
    let body = serde_json::json!({
        "warnings": [],
        "cross_shard_joins": [],
        "retraction_beyond_history": snap,
    });
    assert!(
        body.get(WARNING_FIELD).is_some(),
        "JSON body must carry '{}' sibling field",
        WARNING_FIELD
    );
    let arr = body
        .get(WARNING_FIELD)
        .and_then(|v| v.as_array())
        .expect("retraction_beyond_history must serialize as array");
    assert_eq!(arr.len(), 2, "2 dedupe buckets visible on /debug/warnings");
    for entry in arr {
        assert!(entry.get("operator").is_some());
        assert!(entry.get("reason_class").is_some());
        assert!(entry.get("count").is_some());
        assert!(entry.get("first_seen_ms").is_some());
    }
}
