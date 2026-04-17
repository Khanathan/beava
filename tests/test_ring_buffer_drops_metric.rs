// OBS-01/02: beava_ring_buffer_drops_total{stream, operator_kind, reason}
// counter with bounded labels and mutual-exclusivity invariant.
// RED until Phase 46 Wave 4 (D-05/D-06/D-08) adds the metric, caches
// CounterVec handles at operator registration, and adds the mutual-exclusivity
// integration test.
//
// D-05: counter name = beava_ring_buffer_drops_total; reason ∈ {too_old,
//       too_new, pre_epoch} hard enum.
// D-06: cache Counter handle at operator registration; .inc() only on hot path.
// D-07: scrape /metrics; assert expected label-value combos appear.
// D-08: at most one of beava_late_events_dropped_total or
//       beava_ring_buffer_drops_total fires per event.

#[test]
#[ignore = "Phase 46 Wave 4 (D-05/D-06/D-08): beava_ring_buffer_drops_total counter not yet implemented"]
fn bounded_labels() {
    // Arrange: push events that trigger all three drop reasons
    //          (too_old, too_new, pre_epoch) across two streams and two
    //          operator_kinds.
    // Act:     scrape /metrics endpoint.
    // Assert:  label cardinality for beava_ring_buffer_drops_total is bounded
    //          (does not grow with event count — only grows with stream x
    //          operator_kind x reason combinations).
    panic!("MISSING: Wave 4 (D-05/D-06/D-08) must implement beava_ring_buffer_drops_total counter");
}

#[test]
#[ignore = "Phase 46 Wave 4 (D-05/D-06/D-08): mutual-exclusivity invariant not yet enforced"]
fn counters_mutually_exclusive() {
    // Arrange: for a known event sequence that triggers a ring-buffer drop,
    //          record both beava_late_events_dropped_total and
    //          beava_ring_buffer_drops_total before and after each event.
    // Assert:  for each dropped event, exactly one of the two counters
    //          incremented (mutual exclusivity per D-08 / OBS-02).
    panic!("MISSING: Wave 4 (D-08) must enforce mutual-exclusivity between beava_late_events_dropped_total and beava_ring_buffer_drops_total");
}
