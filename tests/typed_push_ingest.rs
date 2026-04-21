// Phase 59.6 SC-2 — OP_PUSH_BATCH with schema_id header decodes into typed
// Row without allocating a serde_json::Value on the hot path.
//
// Wave 0: RED. Wave 2 lands the wire-codec schema_id prefix and the
// server-side typed decode, flipping these to GREEN.

#![allow(unused_imports)]

#[test]
#[ignore = "59.6-W2"]
fn typed_push_batch_decodes_without_value_alloc() {
    // Verification shape (Wave 2):
    //   1. Register typed stream "Txns" with schema S.
    //   2. Build OP_PUSH_BATCH frame with schema_id(S) + N typed rows.
    //   3. Feed through `parse_command` + shard dispatch.
    //   4. Assert ConcurrentAppState.typed_row_path_total bumped by N.
    //   5. Assert value_fallback_path_total unchanged.
    panic!("SC-2 RED: OP_PUSH_BATCH schema_id prefix not yet implemented; expected in Wave 2");
}

#[test]
#[ignore = "59.6-W2"]
fn typed_push_unknown_schema_id_returns_protocol_error() {
    // Wave 2 D-B1 — server rejects schema_id that isn't in SchemaRegistry
    // with ProtocolError::UnknownSchema.
    panic!("SC-2 RED: unknown schema_id error path not yet implemented; expected in Wave 2");
}
