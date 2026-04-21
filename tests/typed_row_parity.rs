// Phase 59.6 Wave 0 — parity harness. Runs a stream of events through both
// the typed-row path AND the serde_json::Value fallback path; diffs resulting
// entity state byte-for-byte. Other 59.6 integration tests call into this
// harness conceptually (crate-level test modules in `tests/` cannot share
// code directly — peer test files copy the helper shape).
//
// Wave 0 state: every test panics with an "SC-N RED" message. Wave 4+ replaces
// the stubs with the real typed-path + Value-path drivers and the state diff.
//
// See `.planning/phases/59.6-typed-pipeline-records/59.6-CONTEXT.md` (D-F2)
// for the parity-gate semantics.

#![allow(unused_imports)]

use serde_json::json;

/// SC-4 smoke: Count aggregation via typed + Value paths on same stream.
/// RED until Wave 4 lands typed CountOp — tagged `#[ignore = "59.6-W4"]`
/// so it's skipped by default `cargo test` but runs under `-- --ignored`.
#[test]
#[ignore = "59.6-W4"]
fn typed_and_value_paths_produce_identical_count_state() {
    // Stubs — Wave 1 implements PipelineEngine::register_typed_schema,
    // Wave 4 implements the typed CountOp. Pre-Wave-1, this test `panic!`s
    // with the RED marker below so CI can distinguish "not implemented yet"
    // from "actually broken after implementation".

    // Intended shape (commented until Wave 1+):
    //   let mut engine_typed = PipelineEngine::new();
    //   engine_typed.register_typed_schema("Txns", sample_typed_schema());
    //   let mut engine_value = PipelineEngine::new();
    //   engine_value.register_untyped("Txns", sample_fields_dict());
    //   for i in 0..100 {
    //       let payload = json!({ "user_id": format!("u{}", i % 10), "amount": 1.0 });
    //       engine_typed.push_with_cascade_on_shard_typed("Txns", &payload, ...);
    //       engine_value.push_with_cascade_on_shard("Txns", &payload, ...);
    //   }
    //   assert_eq!(engine_typed.dump_entity_state("Counts"),
    //              engine_value.dump_entity_state("Counts"));

    let _sample_event = json!({ "user_id": "u0", "amount": 1.0 });
    panic!("SC-4 RED: typed path not yet implemented; expected in Wave 4");
}

/// SC-4 harness assertion: diff semantics placeholder.
/// Wave 4 replaces with real proptest-driven diff harness per D-F2.
#[test]
#[ignore = "59.6-W4"]
fn typed_row_parity_harness_diffs_100k_events() {
    panic!("SC-4 RED: 100K-event parity harness not yet implemented; expected in Wave 4");
}
