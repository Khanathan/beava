// CORR-07: eviction clock sources from WatermarkTracker::observed_max(), not
// SystemTime::now().
// RED until Phase 46 Wave 3 (D-17) patches src/state/eviction.rs:63.
//
// Once Wave 3 lands:
// - Remove the #[ignore] attribute below.
// - Push historical events (30 days old) with a stream entity_ttl = 7d.
// - Assert entity is NOT evicted immediately (wall-clock is far ahead of
//   event-time watermark).
// - Push a newer event advancing the watermark past the 7d TTL boundary.
// - Assert entity IS evicted after the watermark advance.

#[test]
#[ignore = "Phase 46 Wave 3 (D-17): eviction.rs:63 still uses SystemTime::now() instead of WatermarkTracker::observed_max()"]
fn ttl_honors_event_time_not_wall_clock() {
    // Arrange: create PipelineEngine + StateStore with stream "S",
    //          entity_ttl = Duration::from_secs(7 * 24 * 3600).
    // Act A:   push event for entity "e1" with event_time = now - 30 days.
    //          Trigger eviction pass.
    // Assert A: "e1" is NOT evicted (watermark = 30 days ago; TTL not crossed
    //           relative to watermark).
    // Act B:   push event for entity "e2" with event_time = now (advances
    //          watermark to present).  Trigger eviction pass.
    // Assert B: "e1" IS now evicted (watermark has advanced past e1's TTL
    //           expiry boundary).
    panic!("MISSING: Wave 3 (D-17) must patch eviction.rs:63 to use WatermarkTracker::observed_max()");
}
