// Phase 59.6 SC-1 — @bv.stream with typed fields produces a RegisteredSchema
// on the server; engine.is_typed_stream("Txns") returns true.
//
// Wave 0: both tests are RED (panic!). Wave 1 lands
// `PipelineEngine::register_typed_schema` + `SchemaRegistry` and flips these
// to GREEN.

#![allow(unused_imports)]

#[test]
#[ignore = "59.6-W1"]
fn register_typed_stream_populates_schema_registry() {
    // Pre-Wave-1: beava::engine::pipeline::PipelineEngine::register_typed_schema
    // does not exist. This test will fail at compile time once the typed
    // API is referenced; Wave 1 lands the API and this body turns into:
    //   let mut engine = PipelineEngine::new();
    //   engine.register_typed_schema("Txns", typed_schema_for_txns());
    //   assert!(engine.is_typed_stream("Txns"));
    panic!("SC-1 RED: register_typed_schema not yet implemented; expected in Wave 1");
}

#[test]
#[ignore = "59.6-W1"]
fn typed_schema_round_trips_through_register_json() {
    // Wave 1: SDK → REGISTER JSON → server parse → RegisteredSchema → round-trip check.
    panic!("SC-1 RED: REGISTER JSON schema section not yet consumed; expected in Wave 1");
}
