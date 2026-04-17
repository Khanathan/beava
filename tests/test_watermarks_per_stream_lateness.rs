// CORR-03: per-stream watermark_lateness override on StreamDefinition.
// RED until Phase 46 Wave 3 (D-09/D-10) adds StreamDefinition.watermark_lateness
// and WatermarkTracker.lateness_for().
//
// Once Wave 3 lands:
// - Remove both #[ignore] attributes below.
// - Wire StreamDefinition with watermark_lateness: Some(Duration::from_secs(600))
//   and assert WatermarkTracker returns the override instead of the 5s default.

#[test]
#[ignore = "Phase 46 Wave 3 (D-09/D-10): watermark_lateness not yet on StreamDefinition"]
fn per_stream_override_honored() {
    // Arrange: register stream "S" with watermark_lateness = 10m.
    // Act:     retrieve lateness via WatermarkTracker::lateness_for("S").
    // Assert:  returned Duration == 600s (override), not 5s (default).
    panic!("MISSING: Wave 3 (D-09/D-10) must add StreamDefinition.watermark_lateness + WatermarkTracker.lateness_for");
}

#[test]
#[ignore = "Phase 46 Wave 3 (D-09/D-10): watermark_lateness not yet on StreamDefinition"]
fn absent_field_defaults_to_5s() {
    // Arrange: register stream "S" with no watermark_lateness set (None).
    // Act:     retrieve lateness via WatermarkTracker::lateness_for("S").
    // Assert:  returned Duration == 5s (WATERMARK_LATENESS constant fallback).
    panic!("MISSING: Wave 3 (D-09/D-10) must add StreamDefinition.watermark_lateness + WatermarkTracker.lateness_for");
}
