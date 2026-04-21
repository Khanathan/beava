// Phase 59.6 SC-1 — @bv.stream with typed fields produces a RegisteredSchema
// on the server; engine.is_typed_stream("Txns") returns true.
//
// Wave 0: both tests were RED (panic! body + an ignore attribute tagged
// with the 59.6-W1 wave gate).
// Wave 1: flipped GREEN — `PipelineEngine::register_typed_schema` +
// `SchemaRegistry` + REGISTER JSON `schema:` consumer landed. The ignore
// attributes are gone; bodies assert the contract.

#![allow(unused_imports)]

use beava::engine::pipeline::PipelineEngine;
use beava::engine::register::{RegisterSchemaJson, SourceDescriptor};
use beava::engine::schema::{FieldSpec, FieldTy, RegisteredSchema};

#[test]
fn register_typed_stream_populates_schema_registry() {
    let mut engine = PipelineEngine::new();
    assert!(
        !engine.is_typed_stream("Txns"),
        "bare engine: Txns should not be a typed stream yet"
    );
    assert!(engine.get_schema("Txns").is_none());

    // Layout: user_id InlineStr@0 (slot 16), amount F64@16 → row_size 24.
    let schema = RegisteredSchema {
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
    };
    schema
        .validate_layout()
        .expect("Wave 1: sample Txns schema layout must validate");

    let schema_id = engine.register_typed_schema("Txns", schema);
    assert!(schema_id >= 1, "schema_id must be monotonic >= 1");

    assert!(engine.is_typed_stream("Txns"));
    let got = engine.get_schema("Txns").expect("schema present");
    assert_eq!(got.name, "Txns");
    assert_eq!(got.schema_id, schema_id);
    assert_eq!(got.fields.len(), 2);
    assert_eq!(got.fields[0].offset, 0);
    assert_eq!(got.fields[0].ty, FieldTy::InlineStr);
    assert_eq!(got.fields[1].offset, 16);
    assert_eq!(got.fields[1].ty, FieldTy::F64);
    assert_eq!(got.row_size, 24);
    assert_eq!(got.inline_str_cap, 15);
}

#[test]
fn typed_schema_round_trips_through_register_json() {
    // Full path: REGISTER JSON → SourceDescriptor → RegisterSchemaJson ↩️
    // → RegisteredSchema. Matches the shape the Python SDK emits via
    // `_serialize.py::_compile_source` when `_beava_schema` is attached.
    let json_str = r#"{
        "name": "Txns",
        "kind": "stream",
        "fields": {
            "user_id": {"type": "str", "optional": false},
            "amount": {"type": "float", "optional": false}
        },
        "schema": {
            "inline_str_cap": 15,
            "fields": [
                {"name": "user_id", "ty": "inline_str", "offset": 0, "nullable": false},
                {"name": "amount", "ty": "f64", "offset": 16, "nullable": false}
            ],
            "row_size": 24
        }
    }"#;

    let desc: SourceDescriptor =
        serde_json::from_str(json_str).expect("parse REGISTER JSON");
    assert_eq!(desc.name, "Txns");
    assert_eq!(desc.kind, "stream");
    assert!(desc.schema.is_some(), "schema block must be parsed");

    let schema_json = desc.schema.unwrap();
    assert_eq!(schema_json.inline_str_cap, 15);
    assert_eq!(schema_json.row_size, 24);
    assert_eq!(schema_json.fields.len(), 2);
    assert_eq!(schema_json.fields[0].name, "user_id");
    assert_eq!(schema_json.fields[0].ty, FieldTy::InlineStr);
    assert_eq!(schema_json.fields[0].offset, 0);
    assert!(!schema_json.fields[0].nullable);
    assert_eq!(schema_json.fields[1].name, "amount");
    assert_eq!(schema_json.fields[1].ty, FieldTy::F64);
    assert_eq!(schema_json.fields[1].offset, 16);

    // Round-trip into RegisteredSchema and through validate_layout.
    let registered = schema_json.to_registered_schema(&desc.name);
    registered
        .validate_layout()
        .expect("layout from wire JSON must validate");
    assert_eq!(registered.name, "Txns");
    assert_eq!(registered.row_size, 24);
    assert_eq!(registered.fields.len(), 2);
    assert_eq!(registered.fields[1].ty, FieldTy::F64);

    // Register into a PipelineEngine and confirm the accessors see it.
    let mut engine = PipelineEngine::new();
    let id = engine.register_typed_schema(&desc.name, registered);
    assert!(engine.is_typed_stream("Txns"));
    let got = engine.get_schema("Txns").expect("registered");
    assert_eq!(got.schema_id, id);
    assert_eq!(got.row_size, 24);
}

#[test]
fn source_descriptor_without_schema_is_backward_compatible() {
    // Pre-Phase-59.6 SDKs emit REGISTER JSON without a `schema:` block.
    // Parsing must still succeed and `desc.schema` must be None.
    let legacy_json = r#"{
        "name": "Clicks",
        "kind": "stream",
        "fields": {"user_id": {"type": "str", "optional": false}}
    }"#;
    let desc: SourceDescriptor =
        serde_json::from_str(legacy_json).expect("legacy REGISTER still parses");
    assert_eq!(desc.name, "Clicks");
    assert!(
        desc.schema.is_none(),
        "legacy payload must not synthesize a schema block"
    );

    // An engine that never saw a schema block must report is_typed_stream == false.
    let engine = PipelineEngine::new();
    assert!(!engine.is_typed_stream("Clicks"));
    assert!(engine.get_schema("Clicks").is_none());
}
