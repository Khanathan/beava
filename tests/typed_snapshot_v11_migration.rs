// Phase 59.6 SC-10 — v10 snapshots load; re-serialize to v11 on next cycle.

#![allow(unused_imports)]

#[test]
#[ignore = "59.6-W5"]
fn v10_snapshot_loads_into_v11_writer() {
    // Wave 5: v10 (Value-based) snapshot loads transparently; next snapshot
    // cycle writes v11 (typed-row) format.
    panic!("SC-10 RED: v10→v11 migration path not yet implemented; expected in Wave 5");
}

#[test]
#[ignore = "59.6-W5"]
fn v11_snapshot_round_trip_preserves_typed_rows() {
    // Wave 5: v11 round-trip — write → read → byte-diff.
    panic!("SC-10 RED: v11 round-trip test not yet implemented; expected in Wave 5");
}
