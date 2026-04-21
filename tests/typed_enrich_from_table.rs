// Phase 59.6 SC-3 — EnrichFromTable runs end-to-end on typed rows; output is
// byte-identical to the Value-path reference.
//
// Wave 3 flips this file from RED → GREEN. The test exercises the typed
// operator directly against its Value-path sibling (same input row, same
// right-side lookup, same expected output). Full HTTP round-trip parity
// is SC-5 territory (Wave 7 perf gate); SC-3 scope is the operator-level
// output byte-identical check.
//
// See `src/engine/operators_typed.rs` for the EnrichFromTableTyped impl
// and `.planning/phases/59.6-typed-pipeline-records/59.6-CONTEXT.md`
// Area C (D-C2) for the design contract.

use beava::engine::operators_typed::{
    derive_enriched_schema, EnrichFromTableTyped, ProjectedField,
};
use beava::engine::schema::{FieldSpec, FieldTy, RegisteredSchema, Row};
use std::sync::Arc;

/// Build the Txns input schema (user_id: InlineStr @0, amount: F64 @16).
fn txns_schema() -> Arc<RegisteredSchema> {
    let s = RegisteredSchema {
        schema_id: 1,
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
    s.validate_layout().expect("valid");
    Arc::new(s)
}

/// Build the Countries source-table schema (user_id, country, tier, secret).
fn countries_schema() -> Arc<RegisteredSchema> {
    let s = RegisteredSchema {
        schema_id: 2,
        name: "Countries".into(),
        fields: vec![
            FieldSpec {
                name: "user_id".into(),
                ty: FieldTy::InlineStr,
                offset: 0,
                nullable: false,
            },
            FieldSpec {
                name: "country".into(),
                ty: FieldTy::InlineStr,
                offset: 16,
                nullable: false,
            },
            FieldSpec {
                name: "tier".into(),
                ty: FieldTy::InlineStr,
                offset: 32,
                nullable: false,
            },
            FieldSpec {
                name: "secret_field".into(),
                ty: FieldTy::InlineStr,
                offset: 48,
                nullable: false,
            },
        ],
        inline_str_cap: 15,
        row_size: 64,
    };
    s.validate_layout().expect("valid");
    Arc::new(s)
}

/// Build an EnrichFromTableTyped projecting country + tier from Countries.
fn make_enrich_op(
    input: Arc<RegisteredSchema>,
    right: Arc<RegisteredSchema>,
) -> EnrichFromTableTyped {
    let inline_cap = input.inline_str_cap;
    let projections: Vec<(&str, FieldTy)> =
        vec![("country", FieldTy::InlineStr), ("tier", FieldTy::InlineStr)];
    let mut enriched = derive_enriched_schema(&input, &projections, inline_cap);
    enriched.schema_id = 99;
    let enriched = Arc::new(enriched);
    let country_off = input.row_size;
    let tier_off = country_off + FieldTy::InlineStr.fixed_width(inline_cap);
    EnrichFromTableTyped {
        name: "enrich_country".to_string(),
        right_table: Arc::from("Countries"),
        right_key_field_in_input: 0,
        projected: vec![
            ProjectedField {
                right_field_name: "country".into(),
                dst_offset: country_off,
                dst_ty: FieldTy::InlineStr,
            },
            ProjectedField {
                right_field_name: "tier".into(),
                dst_offset: tier_off,
                dst_ty: FieldTy::InlineStr,
            },
        ],
        input_schema: input,
        enriched_schema: enriched,
        right_schema: Some(right),
    }
}

/// Value-path reference implementation: merge primary event dict with
/// projected fields from the right-side row. Mirrors the Phase 23 /
/// Phase 56 EnrichFromTable emission shape (serde_json::Value).
fn value_enrich_reference(
    input: &serde_json::Value,
    right: Option<&serde_json::Value>,
    projected: &[&str],
) -> serde_json::Value {
    let mut out = input.clone();
    if let Some(obj) = out.as_object_mut() {
        if let Some(right_val) = right {
            if let Some(right_obj) = right_val.as_object() {
                for name in projected {
                    if let Some(v) = right_obj.get(*name) {
                        obj.insert((*name).to_string(), v.clone());
                    }
                }
            }
        }
    }
    out
}

#[test]
fn typed_enrich_from_table_byte_identical_to_value_path() {
    // SC-3 parity: the typed operator and the Value reference MUST
    // agree field-by-field on the enriched output when given the same
    // input + right-row pair.
    let input_schema = txns_schema();
    let right_schema = countries_schema();
    let op = make_enrich_op(input_schema.clone(), right_schema.clone());

    // Build the typed input Row (u1, amount=1.5).
    let mut input_row = Row::zeroed(&input_schema);
    input_row.write_inline_str(0, input_schema.inline_str_cap, "u1");
    input_row.write_f64(16, 1.5);

    // Build the typed right Row (u1, US, gold, classified).
    let mut right_row = Row::zeroed(&right_schema);
    right_row.write_inline_str(0, right_schema.inline_str_cap, "u1");
    right_row.write_inline_str(16, right_schema.inline_str_cap, "US");
    right_row.write_inline_str(32, right_schema.inline_str_cap, "gold");
    right_row.write_inline_str(48, right_schema.inline_str_cap, "classified");

    // Typed enrich output (Row).
    let typed_out = op.enrich_from_row(&input_row, Some(&right_row));

    // Value-path reference — same scenario expressed as JSON objects.
    let input_val = serde_json::json!({"user_id": "u1", "amount": 1.5});
    let right_val = serde_json::json!({
        "user_id": "u1", "country": "US", "tier": "gold", "secret_field": "classified"
    });
    let value_out = value_enrich_reference(&input_val, Some(&right_val), &["country", "tier"]);

    // Assert parity: typed output's field readers MUST return the same
    // values as the Value-path reference's dict entries. This is the
    // "byte-identical" equivalence at the operator boundary — the typed
    // output's payload layout is schema-defined (input prefix + packed
    // projections); the Value output is a dict. We compare values.
    let enriched_cap = op.enriched_schema.inline_str_cap;
    assert_eq!(
        typed_out.read_inline_str(0, enriched_cap),
        value_out["user_id"].as_str().unwrap(),
        "SC-3: user_id parity failed"
    );
    assert!(
        (typed_out.read_f64(16) - value_out["amount"].as_f64().unwrap()).abs() < 1e-9,
        "SC-3: amount parity failed"
    );
    assert_eq!(
        typed_out.read_inline_str(24, enriched_cap),
        value_out["country"].as_str().unwrap(),
        "SC-3: country parity failed"
    );
    assert_eq!(
        typed_out.read_inline_str(40, enriched_cap),
        value_out["tier"].as_str().unwrap(),
        "SC-3: tier parity failed"
    );
    // And the scope-boundary check: secret_field MUST NOT leak into
    // the enriched Row even though the right-side has it.
    assert!(
        !op.enriched_schema
            .fields
            .iter()
            .any(|f| f.name == "secret_field"),
        "SC-3: non-projected right-field leaked into enriched schema"
    );
}

#[test]
fn typed_enrich_missing_right_row_byte_identical_to_value_missing_semantics() {
    // D-C2 null-safe: missing right row → input is preserved, projected
    // fields are zero/empty. Value path's reference: dict unchanged.
    let input_schema = txns_schema();
    let right_schema = countries_schema();
    let op = make_enrich_op(input_schema.clone(), right_schema);

    let mut input_row = Row::zeroed(&input_schema);
    input_row.write_inline_str(0, input_schema.inline_str_cap, "u_missing");
    input_row.write_f64(16, 42.0);

    let typed_out = op.enrich_from_row(&input_row, None);

    let input_val = serde_json::json!({"user_id": "u_missing", "amount": 42.0});
    let value_out = value_enrich_reference(&input_val, None, &["country", "tier"]);

    let enriched_cap = op.enriched_schema.inline_str_cap;
    // Input preserved on typed side.
    assert_eq!(
        typed_out.read_inline_str(0, enriched_cap),
        value_out["user_id"].as_str().unwrap()
    );
    assert!(
        (typed_out.read_f64(16) - value_out["amount"].as_f64().unwrap()).abs() < 1e-9
    );
    // Projected fields empty on typed side; Value side omits them.
    assert_eq!(typed_out.read_inline_str(24, enriched_cap), "");
    assert_eq!(typed_out.read_inline_str(40, enriched_cap), "");
    assert!(value_out.get("country").is_none());
    assert!(value_out.get("tier").is_none());
}

#[test]
fn typed_enrich_mixed_mode_value_right_side_parity() {
    // Wave 3 mixed-mode: right side is still stored as Value (Wave 5
    // makes source_tables typed). enrich_from_value MUST emit the same
    // enriched Row as enrich_from_row on the equivalent right Row.
    let input_schema = txns_schema();
    let right_schema = countries_schema();
    let op = make_enrich_op(input_schema.clone(), right_schema.clone());

    let mut input_row = Row::zeroed(&input_schema);
    input_row.write_inline_str(0, input_schema.inline_str_cap, "u1");
    input_row.write_f64(16, 9.5);

    // Row-side right row.
    let mut right_row = Row::zeroed(&right_schema);
    right_row.write_inline_str(0, right_schema.inline_str_cap, "u1");
    right_row.write_inline_str(16, right_schema.inline_str_cap, "CH");
    right_row.write_inline_str(32, right_schema.inline_str_cap, "silver");
    right_row.write_inline_str(48, right_schema.inline_str_cap, "hidden");
    let out_row = op.enrich_from_row(&input_row, Some(&right_row));

    // Value-side right row (mixed mode).
    let right_val = serde_json::json!({
        "user_id": "u1", "country": "CH", "tier": "silver", "secret_field": "hidden"
    });
    let out_val = op.enrich_from_value(&input_row, Some(&right_val));

    let cap = op.enriched_schema.inline_str_cap;
    // Both paths MUST produce byte-identical enriched field values.
    assert_eq!(
        out_row.read_inline_str(0, cap),
        out_val.read_inline_str(0, cap),
        "parity: user_id"
    );
    assert!(
        (out_row.read_f64(16) - out_val.read_f64(16)).abs() < 1e-9,
        "parity: amount"
    );
    assert_eq!(
        out_row.read_inline_str(24, cap),
        out_val.read_inline_str(24, cap),
        "parity: country"
    );
    assert_eq!(
        out_row.read_inline_str(40, cap),
        out_val.read_inline_str(40, cap),
        "parity: tier"
    );
}
