// Phase 59.6 SC-2 — OP_PUSH_TYPED_BATCH with schema_id header decodes
// into typed Row without allocating a serde_json::Value on the hot path.
//
// Wave 0: RED. Wave 2 lands the wire-codec schema_id prefix and the
// server-side typed decode, flipping these to GREEN (this file).

use beava::engine::pipeline::PipelineEngine;
use beava::engine::schema::{FieldSpec, FieldTy, RegisteredSchema};
use beava::server::protocol::OP_PUSH_TYPED_BATCH;

fn txns_schema() -> RegisteredSchema {
    RegisteredSchema {
        schema_id: 0,
        name: "Txns".into(),
        fields: vec![
            FieldSpec {
                name: "user_id".into(),
                ty: FieldTy::InlineStr,
                offset: 0,
                nullable: false,
            },
            FieldSpec {
                name: "amount".into(),
                ty: FieldTy::F64,
                offset: 16,
                nullable: false,
            },
        ],
        inline_str_cap: 15,
        row_size: 24,
    }
}

/// SC-2 happy path: the Wave 2 typed decoder takes a registered schema +
/// a packed wire body and returns a `Vec<Row>` with the correct field
/// values. The integration test bypasses the TCP dance (that's covered
/// by the binary_push_bytes_passthrough harness) and exercises the
/// decoder directly against an engine-registered schema.
#[test]
fn typed_push_batch_decodes_without_value_alloc() {
    let mut engine = PipelineEngine::new();
    engine.register_typed_schema("Txns", txns_schema());
    let schema = engine.get_schema("Txns").expect("schema registered");
    assert!(engine.is_typed_stream("Txns"));

    // Pack a 2-row OP_PUSH_TYPED_BATCH body (AFTER opcode + stream_name —
    // this is exactly what parse_command hands to the dispatcher).
    let mut body = Vec::new();
    body.extend_from_slice(&schema.schema_id.to_be_bytes()); // u32 BE schema_id
    body.extend_from_slice(&2u32.to_be_bytes()); // u32 BE row_count

    // Row 0: "alice" + 1.5
    let mut row0 = vec![0u8; 24];
    row0[..5].copy_from_slice(b"alice");
    row0[16..24].copy_from_slice(&1.5f64.to_le_bytes());
    body.extend_from_slice(&row0);

    // Row 1: "bob" + 2.5
    let mut row1 = vec![0u8; 24];
    row1[..3].copy_from_slice(b"bob");
    row1[16..24].copy_from_slice(&2.5f64.to_le_bytes());
    body.extend_from_slice(&row1);

    // Arena (empty)
    body.extend_from_slice(&0u32.to_be_bytes());
    // Ack token
    body.extend_from_slice(&0x1234_5678_9ABC_DEF0u64.to_be_bytes());

    let (rows, ack, consumed) =
        beava::wire::typed::decode_typed_row_push_batch(&body, &schema)
            .expect("Wave 2 decode ok");
    assert_eq!(rows.len(), 2, "expected 2 rows");
    assert_eq!(ack, 0x1234_5678_9ABC_DEF0);
    assert_eq!(consumed, body.len(), "decoder should consume full body");

    // Decoded via Row's typed accessors — no serde_json::Value ever
    // allocated for these events; that's the TPC-PERF-11 correctness
    // property Wave 2 unlocks.
    assert_eq!(rows[0].read_inline_str(0, 15), "alice");
    assert!((rows[0].read_f64(16) - 1.5).abs() < 1e-9);
    assert_eq!(rows[1].read_inline_str(0, 15), "bob");
    assert!((rows[1].read_f64(16) - 2.5).abs() < 1e-9);
    for row in &rows {
        assert_eq!(row.schema_id, schema.schema_id);
        assert!(row.arena.is_empty());
    }

    // Spot-check the opcode constant hasn't drifted.
    assert_eq!(OP_PUSH_TYPED_BATCH, 0x19);
}

/// SC-2 error path: server rejects an OP_PUSH_TYPED_BATCH body whose
/// schema_id prefix disagrees with the registered schema's id. Returns a
/// Protocol error with an actionable message naming the offending id so
/// clients can surface a clear "schema mismatch" error to the user.
#[test]
fn typed_push_unknown_schema_id_returns_protocol_error() {
    let mut engine = PipelineEngine::new();
    engine.register_typed_schema("Txns", txns_schema());
    let schema = engine.get_schema("Txns").expect("schema registered");

    // Frame claims schema_id=99 but the registered schema has a real id
    // (>= 1). Non-zero id that disagrees with registration → rejected.
    let mut body = Vec::new();
    body.extend_from_slice(&99u32.to_be_bytes());
    body.extend_from_slice(&1u32.to_be_bytes()); // row_count = 1
    body.extend_from_slice(&vec![0u8; 24]); // zeroed row
    body.extend_from_slice(&0u32.to_be_bytes()); // arena_len = 0
    body.extend_from_slice(&0u64.to_be_bytes()); // ack_token = 0

    let err = beava::wire::typed::decode_typed_row_push_batch(&body, &schema)
        .expect_err("mismatched schema_id must be rejected");
    let msg = format!("{}", err);
    assert!(msg.contains("schema_id 99"), "unexpected error: {msg}");
}
