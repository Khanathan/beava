// CORR-04: backward-compatible snapshot migration for watermark_lateness.
// RED until Phase 46 Wave 3 (D-12) adds Option<Duration> watermark_lateness
// to StreamDefinition with serde(default).
//
// Once Wave 3 lands:
// - Remove the #[ignore] attribute below.
// - Load a legacy snapshot JSON (absent watermark_lateness field) and assert
//   the deserialized StreamDefinition has watermark_lateness == None, which
//   resolves to the 5s default via lateness_for().

#[test]
#[ignore = "Phase 46 Wave 3 (D-12): StreamDefinition.watermark_lateness field with serde(default) not yet added"]
fn old_snapshot_loads_with_default_lateness() {
    // Arrange: craft a legacy snapshot JSON blob that does NOT contain
    //          the watermark_lateness key in its stream definitions.
    // Act:     deserialize via serde_json into StreamDefinition.
    // Assert:  watermark_lateness field is None; lateness_for() returns 5s.
    panic!("MISSING: Wave 3 (D-12) must add watermark_lateness: Option<Duration> with #[serde(default)] to StreamDefinition");
}
