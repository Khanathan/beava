//! Integration tests for Plan 19.2-01 (D-01): register-time field-idx
//! resolution + apply-loop pre-extraction.
//!
//! Test matrix:
//!   1. test_missing_field_rejected_at_register_time — register-time rejects aggs
//!      that reference fields not in the source schema (with clear error message).
//!   2. test_field_idx_resolved_at_register_time — Sum feature gets field_idx
//!      pointing at `amount`; Count gets FIELD_IDX_NONE.
//!   3. test_field_idx_stable_across_features_sharing_field — two features that
//!      both reference `amount` get the SAME field_idx.
//!   4. test_apply_uses_pre_extraction_not_per_op_row_get — apply-loop pre-extracts
//!      ONCE per distinct field (not once per feature).
//!   5. test_apply_with_count_only_no_field_extraction — count-only aggregation
//!      works when the pre-extraction array is empty.

use beava_core::agg_descriptor::{AggregationDescriptor, NamedAggOp};
use beava_core::agg_op::{AggKind, AggOpDescriptor, FIELD_IDX_NONE};
use beava_core::registry::{DerivationDescriptor, EventDescriptor, OutputKind, Registry};
use beava_core::registry_diff::PayloadNode;
use beava_core::row::{Row, Value};
use beava_core::schema::{DerivedSchema, EventSchema, FieldType};
use std::collections::BTreeMap;
use std::sync::Arc;

// ── Helpers ──────────────────────────────────────────────────────────────────

fn make_schema(fields: &[(&str, FieldType)]) -> EventSchema {
    EventSchema {
        fields: fields
            .iter()
            .map(|(k, v)| (k.to_string(), *v))
            .collect::<BTreeMap<_, _>>(),
        optional_fields: vec![],
    }
}

fn make_event(name: &str, schema: EventSchema) -> EventDescriptor {
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

fn make_deriv(name: &str, source: &str) -> DerivationDescriptor {
    DerivationDescriptor {
        name: name.to_string(),
        output_kind: OutputKind::Table,
        upstreams: vec![source.to_string()],
        ops: vec![],
        schema: DerivedSchema {
            fields: BTreeMap::new(),
            optional_fields: vec![],
        },
        table_primary_key: None,
        registered_at_version: 0,
    }
}

fn count_op() -> AggOpDescriptor {
    AggOpDescriptor {
        kind: AggKind::Count,
        field: None,
        ..Default::default()
    }
}

fn sum_op(field: &str) -> AggOpDescriptor {
    AggOpDescriptor {
        kind: AggKind::Sum,
        field: Some(field.to_string()),
        ..Default::default()
    }
}

fn avg_op(field: &str) -> AggOpDescriptor {
    AggOpDescriptor {
        kind: AggKind::Avg,
        field: Some(field.to_string()),
        ..Default::default()
    }
}

/// Build a minimal AggregationDescriptor with given features.
fn make_agg(
    node_name: &str,
    source: &str,
    features: Vec<(&str, AggOpDescriptor)>,
) -> AggregationDescriptor {
    AggregationDescriptor {
        node_name: node_name.to_string(),
        source_node_name: source.to_string(),
        group_keys: vec!["user_id".to_string()],
        features: features
            .into_iter()
            .map(|(name, desc)| NamedAggOp {
                feature_name: name.to_string(),
                descriptor: desc,
            })
            .collect(),
        agg_id: 0,
        field_names: vec![],
        cluster_id: 0,
    }
}

/// Register an event + aggregation in one shot. Returns the registry.
/// Also validates field_idx resolution — returns Err string on missing field.
fn register_with_validation(
    event_name: &str,
    schema: EventSchema,
    agg: AggregationDescriptor,
) -> Result<Registry, String> {
    let registry = Registry::new();
    let agg_node_name = agg.node_name.clone();

    // Plan 19.2-01: resolve_field_indices must be called here before installing.
    // The registry's apply_registration should call resolve_field_indices
    // and return Err if any referenced field is not in the source schema.
    registry
        .resolve_field_indices_for_agg(&agg, &schema)
        .map_err(|e| e.to_string())?;

    registry.apply_registration(
        vec![
            PayloadNode::Event(make_event(event_name, schema)),
            PayloadNode::Derivation(make_deriv(&agg_node_name, event_name)),
        ],
        vec![],
        vec![],
        vec![(agg_node_name.clone(), Arc::new(agg))],
    );
    Ok(registry)
}

// ── Test 1: test_missing_field_rejected_at_register_time ─────────────────────

/// Registration of an aggregation that references a field not in the source
/// schema must return an Err whose message includes both the missing field name
/// and the word "schema".
#[test]
fn test_missing_field_rejected_at_register_time() {
    let schema = make_schema(&[("user_id", FieldType::Str), ("amount", FieldType::F64)]);

    let agg = make_agg(
        "FeatureTable",
        "Transaction",
        vec![("bad_sum", sum_op("nonexistent"))],
    );

    let result = register_with_validation("Transaction", schema, agg);
    assert!(
        result.is_err(),
        "expected Err when field 'nonexistent' is not in schema"
    );

    let err_msg = result.unwrap_err();
    assert!(
        err_msg.contains("nonexistent"),
        "error message must include the missing field name; got: {err_msg}"
    );
    assert!(
        err_msg.contains("schema"),
        "error message must include 'schema' to indicate it's a schema-level rejection; got: {err_msg}"
    );
}

// ── Test 2: test_field_idx_resolved_at_register_time ─────────────────────────

/// After a successful registration, the Sum feature's descriptor.field_idx
/// must be Some-variant (not FIELD_IDX_NONE), and Count's descriptor.field_idx
/// must be FIELD_IDX_NONE.
#[test]
fn test_field_idx_resolved_at_register_time() {
    let schema = make_schema(&[
        ("user_id", FieldType::Str),
        ("amount", FieldType::F64),
        ("status", FieldType::Str),
    ]);

    let mut agg = make_agg(
        "FeatureTable",
        "Transaction",
        vec![("cnt", count_op()), ("total", sum_op("amount"))],
    );

    // Plan 19.2-01: resolve field indices in-place on the agg descriptor.
    let registry = Registry::new();
    registry
        .resolve_field_indices_for_agg_mut(&mut agg, &schema, &[])
        .expect("resolve_field_indices must succeed when fields exist");

    // Count has no field → field_idx must be FIELD_IDX_NONE.
    let count_feat = agg
        .features
        .iter()
        .find(|f| f.feature_name == "cnt")
        .unwrap();
    assert_eq!(
        count_feat.descriptor.field_idx, FIELD_IDX_NONE,
        "Count feature must have field_idx == FIELD_IDX_NONE (no field)"
    );

    // Sum(amount) has a field → field_idx must NOT be FIELD_IDX_NONE.
    let sum_feat = agg
        .features
        .iter()
        .find(|f| f.feature_name == "total")
        .unwrap();
    assert_ne!(
        sum_feat.descriptor.field_idx, FIELD_IDX_NONE,
        "Sum(amount) feature must have a resolved field_idx (not FIELD_IDX_NONE)"
    );
}

// ── Test 3: test_field_idx_stable_across_features_sharing_field ──────────────

/// Two features referencing the same field `amount` (Sum and Avg) must resolve
/// to the SAME field_idx.
#[test]
fn test_field_idx_stable_across_features_sharing_field() {
    let schema = make_schema(&[("user_id", FieldType::Str), ("amount", FieldType::F64)]);

    let mut agg = make_agg(
        "FeatureTable",
        "Transaction",
        vec![("total", sum_op("amount")), ("mean", avg_op("amount"))],
    );

    let registry = Registry::new();
    registry
        .resolve_field_indices_for_agg_mut(&mut agg, &schema, &[])
        .expect("resolve_field_indices must succeed");

    let sum_idx = agg
        .features
        .iter()
        .find(|f| f.feature_name == "total")
        .unwrap()
        .descriptor
        .field_idx;
    let avg_idx = agg
        .features
        .iter()
        .find(|f| f.feature_name == "mean")
        .unwrap()
        .descriptor
        .field_idx;

    assert_ne!(sum_idx, FIELD_IDX_NONE, "sum field_idx must be resolved");
    assert_ne!(avg_idx, FIELD_IDX_NONE, "avg field_idx must be resolved");
    assert_eq!(
        sum_idx, avg_idx,
        "two features referencing the same field must get the SAME field_idx"
    );
}

// ── Test 4: test_apply_uses_pre_extraction_not_per_op_row_get ────────────────

/// After registering an aggregation with 3 features all referencing `amount`,
/// pushing 1 event must result in correct feature values AND the apply path
/// must call `Row::get` at most `distinct_fields_referenced` times (not
/// `n_features * distinct_fields` times).
///
/// The probe is `beava_core::row::_take_get_count()` — a `#[cfg(test)]`
/// thread-local counter incremented on every `Row::get` call.
#[test]
fn test_apply_uses_pre_extraction_not_per_op_row_get() {
    use beava_core::agg_apply::apply_event_to_aggregations;
    use beava_core::agg_state_table::{new_state_tables_for, EntityKey};
    use compact_str::CompactString;
    use smallvec::SmallVec;

    let schema = make_schema(&[
        ("user_id", FieldType::Str),
        ("amount", FieldType::F64),
        ("status", FieldType::Str),
    ]);

    let agg = make_agg(
        "FeatureTable",
        "Transaction",
        vec![
            ("cnt", count_op()),
            ("total", sum_op("amount")),
            ("mean", avg_op("amount")),
        ],
    );

    let registry = Registry::new();
    registry.apply_registration(
        vec![
            PayloadNode::Event(make_event("Transaction", schema)),
            PayloadNode::Derivation(make_deriv("FeatureTable", "Transaction")),
        ],
        vec![],
        vec![],
        vec![("FeatureTable".to_string(), Arc::new(agg))],
    );

    let mut state_tables = new_state_tables_for(&registry);

    let row = Row::new()
        .with_field("user_id", Value::Str("u1".into()))
        .with_field("amount", Value::F64(42.0))
        .with_field("status", Value::Str("ok".into()));

    // Reset the counter before the apply call.
    let _ = beava_core::row::_take_get_count();

    apply_event_to_aggregations("Transaction", &row, 0, 0, &registry, &mut state_tables);

    let get_calls = beava_core::row::_take_get_count();

    // Functional check: count=1, sum=42.0, avg=42.0.
    let agg_desc = registry
        .compiled_aggregation("FeatureTable")
        .expect("FeatureTable must be registered");
    let table =
        beava_core::agg_state_table::lookup_table_by_name(&state_tables, &registry, "FeatureTable")
            .expect("FeatureTable table must exist");

    let key = EntityKey({
        let mut sv: SmallVec<[(CompactString, Value); 2]> = SmallVec::new();
        sv.push(("user_id".into(), Value::Str("u1".into())));
        sv
    });

    let cnt = table.query_feature(&key, 0, 0).expect("cnt must exist");
    assert_eq!(cnt, Value::I64(1), "count must be 1");

    let total = table.query_feature(&key, 1, 0).expect("total must exist");
    assert_eq!(total, Value::F64(42.0), "sum must be 42.0");

    let mean = table.query_feature(&key, 2, 0).expect("mean must exist");
    assert_eq!(mean, Value::F64(42.0), "avg must be 42.0");

    // Architectural check: pre-extraction means Row::get is called at most
    // (n_group_keys + n_distinct_feature_fields) times — NOT (n_features * n_distinct_fields).
    //
    // For our 3 features with 1 distinct field (amount) + 1 group key (user_id):
    //   - WITHOUT pre-extraction: EntityKey::from_row(1 call) + Sum(1 call) + Avg(1 call) = 3 calls
    //   - WITH pre-extraction:    EntityKey::from_row(1 call) + pre-extract(1 call) = 2 calls
    //
    // The bound is ≤ n_group_keys + n_distinct_feature_fields = 1 + 1 = 2.
    // A non-pre-extracting loop WILL exceed this (3 > 2) → test is RED until impl ships.
    let _ = agg_desc; // used above
    assert!(
        get_calls <= 2,
        "apply-loop must use pre-extraction (≤ n_group_keys + n_distinct_fields calls to row.get); \
         expected ≤ 2 (1 group-key + 1 distinct field) but got {get_calls} — \
         without pre-extraction, Sum and Avg each call row.get(\"amount\") separately",
    );
}

// ── Test 5: test_apply_with_count_only_no_field_extraction ───────────────────

/// Aggregation with only bv.count() — no field references.
/// Pre-extraction array is empty. Push 1 event; count must be 1.
#[test]
fn test_apply_with_count_only_no_field_extraction() {
    use beava_core::agg_apply::apply_event_to_aggregations;
    use beava_core::agg_state_table::{new_state_tables_for, EntityKey};
    use compact_str::CompactString;
    use smallvec::SmallVec;

    let schema = make_schema(&[("user_id", FieldType::Str), ("amount", FieldType::F64)]);

    let agg = make_agg("CountOnly", "Login", vec![("cnt", count_op())]);

    let registry = Registry::new();
    registry.apply_registration(
        vec![
            PayloadNode::Event(make_event("Login", schema)),
            PayloadNode::Derivation(make_deriv("CountOnly", "Login")),
        ],
        vec![],
        vec![],
        vec![("CountOnly".to_string(), Arc::new(agg))],
    );

    let mut state_tables = new_state_tables_for(&registry);

    let row = Row::new()
        .with_field("user_id", Value::Str("alice".into()))
        .with_field("amount", Value::F64(99.0));

    apply_event_to_aggregations("Login", &row, 0, 0, &registry, &mut state_tables);

    let table =
        beava_core::agg_state_table::lookup_table_by_name(&state_tables, &registry, "CountOnly")
            .expect("CountOnly table must exist");

    let key = EntityKey({
        let mut sv: SmallVec<[(CompactString, Value); 2]> = SmallVec::new();
        sv.push(("user_id".into(), Value::Str("alice".into())));
        sv
    });

    let cnt = table.query_feature(&key, 0, 0).expect("cnt must exist");
    assert_eq!(
        cnt,
        Value::I64(1),
        "count-only aggregation must count 1 event"
    );
}
