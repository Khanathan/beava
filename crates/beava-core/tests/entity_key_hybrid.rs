//! Plan 19.2-03 Task 1.a (red): EntityKey hybrid (SingleU64 / SingleStr / Multi)
//! + NaN reject integration tests.
//!
//! These tests are RED until Task 1.b implements `EntityKeyShape` in
//! `crates/beava-core/src/agg_state_table.rs`.

use beava_core::agg_state_table::EntityKeyShape;
use beava_core::registry::{DerivationDescriptor, EventDescriptor, OutputKind, Registry};
#[allow(unused_imports)]
use beava_core::registry_diff::PayloadNode;
use beava_core::row::{Row, Value};
use beava_core::schema::{DerivedSchema, EventSchema, FieldType};
use compact_str::CompactString;
use std::collections::BTreeMap;
use std::sync::Arc;

// ── Helpers ───────────────────────────────────────────────────────────────────

fn make_event_schema_with_fields(fields: Vec<(&str, FieldType)>) -> EventSchema {
    let mut map = BTreeMap::new();
    for (k, v) in fields {
        map.insert(k.to_string(), v);
    }
    EventSchema {
        fields: map,
        optional_fields: vec![],
    }
}

fn make_event_with_schema(name: &str, schema: EventSchema) -> EventDescriptor {
    EventDescriptor {
        name: name.to_string(),
        schema,
        dedupe_key: None,
        dedupe_window_ms: None,
        keep_events_for_ms: None,
        cold_after_ms: None,
        registered_at_version: 0,
        name_arc: Arc::from(""),
        apply_field_names: vec![],
    }
}

// ── Test 1: SingleU64 for I64 ─────────────────────────────────────────────────

/// An I64 single-key produces EntityKeyShape::SingleU64.
#[test]
fn test_entity_key_shape_single_u64_for_i64() {
    let row = Row::new().with_field("user_id", Value::I64(42));
    let shape = EntityKeyShape::from_row(&["user_id".to_string()], &row);
    assert!(
        shape.is_some(),
        "I64 single key must produce Some(EntityKeyShape)"
    );
    let shape = shape.unwrap();
    assert!(
        matches!(shape, EntityKeyShape::SingleU64(_)),
        "I64 single key must produce SingleU64 variant, got {:?}",
        shape
    );
}

// ── Test 2: SingleU64 for F64 (non-NaN) ──────────────────────────────────────

/// A non-NaN F64 single-key produces EntityKeyShape::SingleU64.
#[test]
fn test_entity_key_shape_single_u64_for_f64() {
    let row = Row::new().with_field("amount", Value::F64(2.5_f64));
    let shape = EntityKeyShape::from_row(&["amount".to_string()], &row);
    assert!(
        shape.is_some(),
        "Non-NaN F64 single key must produce Some(EntityKeyShape)"
    );
    let shape = shape.unwrap();
    assert!(
        matches!(shape, EntityKeyShape::SingleU64(_)),
        "Non-NaN F64 single key must produce SingleU64 variant, got {:?}",
        shape
    );
}

// ── Test 3: SingleU64 for Bool ────────────────────────────────────────────────

/// A Bool single-key produces EntityKeyShape::SingleU64.
#[test]
fn test_entity_key_shape_single_u64_for_bool() {
    let row = Row::new().with_field("flag", Value::Bool(true));
    let shape = EntityKeyShape::from_row(&["flag".to_string()], &row);
    assert!(
        shape.is_some(),
        "Bool single key must produce Some(EntityKeyShape)"
    );
    let shape = shape.unwrap();
    assert!(
        matches!(shape, EntityKeyShape::SingleU64(_)),
        "Bool single key must produce SingleU64 variant, got {:?}",
        shape
    );
}

// ── Test 4: SingleU64 for Datetime ────────────────────────────────────────────

/// A Datetime single-key produces EntityKeyShape::SingleU64.
#[test]
fn test_entity_key_shape_single_u64_for_datetime() {
    let row = Row::new().with_field("ts", Value::Datetime(1234567890));
    let shape = EntityKeyShape::from_row(&["ts".to_string()], &row);
    assert!(
        shape.is_some(),
        "Datetime single key must produce Some(EntityKeyShape)"
    );
    let shape = shape.unwrap();
    assert!(
        matches!(shape, EntityKeyShape::SingleU64(_)),
        "Datetime single key must produce SingleU64 variant, got {:?}",
        shape
    );
}

// ── Test 5: SingleStr for Str ─────────────────────────────────────────────────

/// A Str single-key produces EntityKeyShape::SingleStr(hash, s).
/// The hash must be the FxHash of the string; the stored string must match.
#[test]
fn test_entity_key_shape_single_str_for_str() {
    use std::hash::{Hash, Hasher};

    let row = Row::new().with_field("user_id", Value::Str(CompactString::from("u1")));
    let shape = EntityKeyShape::from_row(&["user_id".to_string()], &row);
    assert!(
        shape.is_some(),
        "Str single key must produce Some(EntityKeyShape)"
    );
    let shape = shape.unwrap();

    match shape {
        EntityKeyShape::SingleStr(hash, s) => {
            // Hash must equal FxHash of "u1".
            let mut h = fxhash::FxHasher::default();
            "u1".hash(&mut h);
            let expected_hash = h.finish();
            assert_eq!(
                hash, expected_hash,
                "SingleStr hash must be FxHash of the string"
            );
            assert_eq!(s.as_str(), "u1", "SingleStr stored string must match");
        }
        other => panic!("Expected SingleStr, got {:?}", other),
    }
}

// ── Test 6: Multi for compound key ────────────────────────────────────────────

/// A compound key (≥2 group_keys) produces EntityKeyShape::Multi with pairs in
/// declaration order.
#[test]
fn test_entity_key_shape_multi_for_compound() {
    let row = Row::new()
        .with_field("user_id", Value::Str(CompactString::from("u1")))
        .with_field("merchant", Value::Str(CompactString::from("m1")));

    let keys = vec!["user_id".to_string(), "merchant".to_string()];
    let shape = EntityKeyShape::from_row(&keys, &row);
    assert!(
        shape.is_some(),
        "Compound key must produce Some(EntityKeyShape)"
    );

    match shape.unwrap() {
        EntityKeyShape::Multi(pairs) => {
            assert_eq!(pairs.0.len(), 2, "compound key must have 2 pairs");
            // Declaration order: user_id first, merchant second.
            assert_eq!(
                pairs.0[0].0.as_str(),
                "user_id",
                "first pair must be user_id (declaration order)"
            );
            assert_eq!(
                pairs.0[1].0.as_str(),
                "merchant",
                "second pair must be merchant (declaration order)"
            );
        }
        other => panic!("Expected Multi, got {:?}", other),
    }
}

// ── Test 7: NaN rejected at push time ─────────────────────────────────────────

/// A NaN F64 group-key value produces None (event dropped at push time).
#[test]
fn test_entity_key_shape_nan_rejected_at_push() {
    let row = Row::new().with_field("amount", Value::F64(f64::NAN));
    let shape = EntityKeyShape::from_row(&["amount".to_string()], &row);
    assert!(
        shape.is_none(),
        "NaN F64 group-key value must produce None (event dropped)"
    );
}

// ── Test 8: Null rejected ─────────────────────────────────────────────────────

/// A Null group-key value produces None (event dropped).
#[test]
fn test_entity_key_shape_null_rejected() {
    let row = Row::new().with_field("user_id", Value::Null);
    let shape = EntityKeyShape::from_row(&["user_id".to_string()], &row);
    assert!(
        shape.is_none(),
        "Null group-key value must produce None (event dropped)"
    );
}

// ── Test 9: Register-time rejects float-typed group_keys ─────────────────────

/// Registering an aggregation with group_keys=["amount"] where amount is F64
/// must return an error containing "NaN" or "float" — NaN-capable float group
/// keys are rejected at register time per D-03 policy.
#[test]
fn test_register_time_rejects_nan_capable_float_group_key() {
    use beava_core::agg_descriptor::{AggregationDescriptor, NamedAggOp};
    use beava_core::agg_op::{AggKind, AggOpDescriptor, FIELD_IDX_NONE};

    // Build an event schema where "amount" is F64.
    let schema = make_event_schema_with_fields(vec![
        ("user_id", FieldType::Str),
        ("amount", FieldType::F64),
    ]);

    // Aggregation using F64 field "amount" as a group_key.
    let agg = Arc::new(AggregationDescriptor {
        node_name: "FloatKeyAgg".to_string(),
        source_node_name: "Txn".to_string(),
        group_keys: vec!["amount".to_string()], // F64-typed group key
        features: vec![NamedAggOp {
            feature_name: "cnt".to_string(),
            descriptor: AggOpDescriptor {
                kind: AggKind::Count,
                field: None,
                window_ms: None,
                where_expr: None,
                n: None,
                half_life_ms: None,
                sub_window_ms: None,
                sigma: None,
                sketch_params: None,
                ext: Default::default(),
                field_idx: FIELD_IDX_NONE,
                field_idx_into_event_extracted: Vec::new(),
            },
        }],
        agg_id: 0,
        field_names: vec![],
        cluster_id: 0,
    });

    let _event = make_event_with_schema("Txn", schema.clone());
    let _deriv = DerivationDescriptor {
        name: "FloatKeyAgg".to_string(),
        output_kind: OutputKind::Table,
        upstreams: vec!["Txn".to_string()],
        ops: vec![],
        schema: DerivedSchema {
            fields: BTreeMap::new(),
            optional_fields: vec![],
        },
        table_primary_key: None,
        registered_at_version: 0,
    };

    // Validate through the registry's field-index resolver which should now
    // also check for float group keys.
    let registry = Registry::new();
    let result = registry.validate_group_keys_for_agg(&agg, &schema);
    assert!(
        result.is_err(),
        "Registration with float group_key must return Err"
    );
    let err = result.unwrap_err();
    assert!(
        err.to_lowercase().contains("nan")
            || err.to_lowercase().contains("float")
            || err.to_lowercase().contains("f64"),
        "Error message must mention NaN/float, got: {}",
        err
    );
}
