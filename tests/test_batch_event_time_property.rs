// CORR-01: property test for batch vs single-event event-time bucketing equivalence.
// RED until Phase 46 Wave 2 (D-01/D-02) lands the &[(&Value, SystemTime)] signature.
//
// Once Wave 2 is complete, remove the #[ignore] attribute and fill in the test
// body by wiring a real PipelineEngine, pushing events via push_for_backfill
// (single-event path) and push_batch_with_cascade_no_features (batch path), and
// asserting per-bucket feature equality.
use proptest::prelude::*;

proptest! {
    #[test]
    #[ignore = "Phase 46 Wave 2 (D-01/D-02): push_batch_with_cascade_no_features signature not yet &[(&Value, SystemTime)]"]
    fn batch_path_equals_single_event_path(
        event_time_offsets_secs in proptest::collection::vec(-3600i64..0i64, 2..16)
    ) {
        // Arrange: create a PipelineEngine + StateStore with a count-1h stream.
        // Build events: first event has event_time = now - 1h (bucket boundary
        // stress), remaining events have event_time = now.
        // Act: push via push_for_backfill one-by-one (single-event path),
        //      AND push via push_batch_with_cascade_no_features (batch path) on
        //      a fresh store.
        // Assert: for every entity key and every bucket, features are identical.
        let _ = event_time_offsets_secs;
        panic!("MISSING: Wave 2 must implement group-by-bucket batch primitive (D-01/D-02)");
    }
}
