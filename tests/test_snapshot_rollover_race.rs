// CORR-10: busy-racer asserting take_dirty_and_advance_gen() loses no marks.
// RED until Phase 46 Wave 4 (D-21) replaces the Mutex<DashSet<String>> dirty
// container with ArcSwap<DashSet<String>> and exposes take_dirty_and_advance_gen().
//
// Pattern: N writer threads call mark_dirty(key) while a snapshotter thread
// calls take_dirty_and_advance_gen() in a tight loop.  Every marked key must
// appear in exactly one generation's dirty set — no key must be lost.
//
// Once Wave 4 lands:
// - Remove the #[ignore] attribute below.
// - Replace unimplemented!() stubs with real StateStore / mark_dirty calls.

#[test]
#[ignore = "Phase 46 Wave 4 (D-21): dirty_keys still uses Mutex<DashSet<String>>; take_dirty_and_advance_gen not yet available"]
fn busy_racer_no_lost_keys() {
    // Arrange: create a StateStore (or a thin test struct wrapping the same
    //          ArcSwap<DashSet<String>> mechanism).
    //          Prepare a set of 1000 distinct keys.
    //
    // Act:     spawn 8 writer threads, each marking 125 keys via mark_dirty(k).
    //          Concurrently, spawn 1 snapshotter thread calling
    //          take_dirty_and_advance_gen() in a loop until all writers finish.
    //          Collect every generation's dirty set from the snapshotter.
    //          After all threads complete, do one final take to drain any
    //          remaining marks.
    //
    // Assert:  union of all collected dirty sets contains all 1000 keys.
    //          No key appears in more than one generation (no double-count).
    //          snapshot_gen advanced by exactly the number of takes performed.
    panic!("MISSING: Wave 4 (D-21) must add ArcSwap<DashSet<String>> + take_dirty_and_advance_gen() to StateStore");
}
