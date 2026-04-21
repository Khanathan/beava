// Phase 59.6 SC-6 — stream registered without typed schema continues working
// via Value fallback. Existing tests like test_v0_register_roundtrip stay green.

#![allow(unused_imports)]

#[test]
#[ignore = "59.6-W6"]
fn unschemad_stream_uses_value_fallback() {
    // Wave 6: unschema'd stream → Value path → beava_value_fallback_path_total
    // increments on every event; typed_row_path_total stays at 0.
    panic!("SC-6 RED: Value fallback path preservation not yet verified; expected in Wave 6");
}

#[test]
#[ignore = "59.6-W6"]
fn value_fallback_counter_increments_for_untyped() {
    // D-E2 — beava_value_fallback_path_total{stream=X} bumps when X has no typed schema.
    panic!("SC-6 RED: D-E2 counter wiring not yet implemented; expected in Wave 6");
}
