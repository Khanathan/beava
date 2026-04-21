// Phase 59.6 SC-7 — two streams registered, one typed, one untyped;
// interoperate via cross-shard EnrichFromTable.

#![allow(unused_imports)]

#[test]
#[ignore = "59.6-W5"]
fn mixed_typed_untyped_streams_coexist_via_enrich() {
    // Wave 5: typed stream + untyped stream run side-by-side; EnrichFromTable
    // between them succeeds via Value-fallback on the untyped side.
    panic!("SC-7 RED: mixed-mode coexistence not yet implemented; expected in Wave 5");
}
