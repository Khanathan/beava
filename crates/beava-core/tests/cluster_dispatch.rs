//! Plan 19.2-03 Task 2.a (red): cluster_id assignment + cluster dispatch tests.
//!
//! Five tests that are RED until Task 2.b wires EntityKeyShape dispatch into
//! the apply loop via `get_or_init_by_shape`.
//!
//! Tests:
//!   1. Two aggregations with identical group_keys receive the same cluster_id
//!      after registration.
//!   2. Two aggregations with different group_keys receive different cluster_ids.
//!   3. apply_event_to_aggregations builds EntityKey ONCE per cluster (not once
//!      per agg) — verified via the `_take_entity_key_build_count()` test hook.
//!   4. SingleU64 path: apply_event_to_aggregations routes through single_u64 map
//!      for an I64-typed group_key (no multi-map allocation).
//!   5. SingleStr path: apply_event_to_aggregations routes through single_str map
//!      for a Str-typed group_key.

use beava_core::agg_apply::apply_event_to_aggregations;
use beava_core::agg_descriptor::{AggregationDescriptor, NamedAggOp};
use beava_core::agg_op::{AggKind, AggOpDescriptor, FIELD_IDX_NONE};
use beava_core::agg_state_table::StateTables;
use beava_core::registry::{DerivationDescriptor, EventDescriptor, OutputKind, Registry};
use beava_core::registry_diff::PayloadNode;
use beava_core::row::{Row, Value};
use beava_core::schema::{DerivedSchema, EventSchema, FieldType};
use std::collections::BTreeMap;
use std::sync::Arc;

// ── Helpers ───────────────────────────────────────────────────────────────────

fn count_desc() -> AggOpDescriptor {
    AggOpDescriptor {
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
    }
}

fn make_event(name: &str, fields: Vec<(&str, FieldType)>) -> EventDescriptor {
    let mut schema_fields = BTreeMap::new();
    for (k, v) in fields {
        schema_fields.insert(k.to_string(), v);
    }
    EventDescriptor {
        name: name.to_string(),
        schema: EventSchema {
            fields: schema_fields,
            optional_fields: vec![],
        },
        dedupe_key: None,
        dedupe_window_ms: None,
        keep_events_for_ms: None,
        cold_after_ms: None,
        registered_at_version: 0,
        name_arc: Arc::from(""),
        apply_field_names: vec![],
    }
}

fn make_derivation(name: &str, upstream: &str) -> DerivationDescriptor {
    DerivationDescriptor {
        name: name.to_string(),
        output_kind: OutputKind::Table,
        upstreams: vec![upstream.to_string()],
        ops: vec![],
        schema: DerivedSchema {
            fields: BTreeMap::new(),
            optional_fields: vec![],
        },
        table_primary_key: None,
        registered_at_version: 0,
    }
}

fn make_agg(name: &str, source: &str, group_keys: Vec<&str>) -> AggregationDescriptor {
    AggregationDescriptor {
        node_name: name.to_string(),
        source_node_name: source.to_string(),
        group_keys: group_keys.into_iter().map(|s| s.to_string()).collect(),
        features: vec![NamedAggOp {
            feature_name: "cnt".to_string(),
            descriptor: count_desc(),
        }],
        agg_id: 0,
        field_names: vec![],
        cluster_id: 0,
    }
}

// ── Test 1: Same group_keys → same cluster_id ─────────────────────────────────

/// Two aggregations that share the same group_keys signature should receive
/// the same cluster_id after registration.
#[test]
fn test_same_group_keys_same_cluster_id() {
    let registry = Registry::new();
    let event = make_event("Txn", vec![("user_id", FieldType::Str)]);
    let agg1 = make_agg("Agg1", "Txn", vec!["user_id"]);
    let agg2 = make_agg("Agg2", "Txn", vec!["user_id"]);

    registry.apply_registration(
        vec![
            PayloadNode::Event(event),
            PayloadNode::Derivation(make_derivation("Agg1", "Txn")),
            PayloadNode::Derivation(make_derivation("Agg2", "Txn")),
        ],
        vec![],
        vec![],
        vec![
            ("Agg1".to_string(), Arc::new(agg1)),
            ("Agg2".to_string(), Arc::new(agg2)),
        ],
    );

    let inner = registry.read();
    let agg1_desc = inner
        .compiled_aggregations
        .get("Agg1")
        .expect("Agg1 must exist");
    let agg2_desc = inner
        .compiled_aggregations
        .get("Agg2")
        .expect("Agg2 must exist");

    assert_eq!(
        agg1_desc.cluster_id, agg2_desc.cluster_id,
        "aggregations with identical group_keys must share a cluster_id"
    );
}

// ── Test 2: Different group_keys → different cluster_ids ──────────────────────

/// Two aggregations with different group_keys signatures should receive
/// distinct cluster_ids after registration.
#[test]
fn test_different_group_keys_different_cluster_id() {
    let registry = Registry::new();
    let event = make_event(
        "Txn",
        vec![("user_id", FieldType::Str), ("merchant", FieldType::Str)],
    );
    let agg1 = make_agg("AggUser", "Txn", vec!["user_id"]);
    let agg2 = make_agg("AggMerchant", "Txn", vec!["merchant"]);

    registry.apply_registration(
        vec![
            PayloadNode::Event(event),
            PayloadNode::Derivation(make_derivation("AggUser", "Txn")),
            PayloadNode::Derivation(make_derivation("AggMerchant", "Txn")),
        ],
        vec![],
        vec![],
        vec![
            ("AggUser".to_string(), Arc::new(agg1)),
            ("AggMerchant".to_string(), Arc::new(agg2)),
        ],
    );

    let inner = registry.read();
    let agg1_desc = inner
        .compiled_aggregations
        .get("AggUser")
        .expect("AggUser must exist");
    let agg2_desc = inner
        .compiled_aggregations
        .get("AggMerchant")
        .expect("AggMerchant must exist");

    assert_ne!(
        agg1_desc.cluster_id, agg2_desc.cluster_id,
        "aggregations with different group_keys must have distinct cluster_ids"
    );
}

// ── Test 3: EntityKey built once per cluster, not once per agg ────────────────

/// When two aggregations share the same cluster_id (same group_keys),
/// apply_event_to_aggregations should build EntityKey ONCE for the cluster,
/// not once per aggregation. Verified via the `_take_entity_key_build_count()`
/// test hook.
#[test]
fn test_entity_key_built_once_per_cluster() {
    let registry = Registry::new();
    let event = make_event("Txn", vec![("user_id", FieldType::Str)]);
    let agg1 = make_agg("Agg1", "Txn", vec!["user_id"]);
    let agg2 = make_agg("Agg2", "Txn", vec!["user_id"]);

    registry.apply_registration(
        vec![
            PayloadNode::Event(event),
            PayloadNode::Derivation(make_derivation("Agg1", "Txn")),
            PayloadNode::Derivation(make_derivation("Agg2", "Txn")),
        ],
        vec![],
        vec![],
        vec![
            ("Agg1".to_string(), Arc::new(agg1)),
            ("Agg2".to_string(), Arc::new(agg2)),
        ],
    );

    let mut state_tables: StateTables =
        beava_core::agg_state_table::new_state_tables_for(&registry);

    // Reset the counter before the event.
    beava_core::agg_state_table::_take_entity_key_build_count();

    let row = Row::new().with_field("user_id", Value::Str("alice".into()));
    apply_event_to_aggregations("Txn", &row, 1000, 0, &registry, &mut state_tables, None);

    let count = beava_core::agg_state_table::_take_entity_key_build_count();
    assert_eq!(
        count, 1,
        "EntityKey must be built ONCE per cluster (got {count}); \
         two aggs sharing group_keys must reuse the key build"
    );
}

// ── Test 4: SingleU64 path for I64-typed group_key ────────────────────────────

/// For an I64-typed single group_key, apply_event dispatches through the
/// single_u64 sub-map (zero SmallVec allocation).
/// After applying one event, single_u64 must have 1 entry and multi must be empty.
#[test]
fn test_apply_routes_single_u64_for_i64_group_key() {
    let registry = Registry::new();
    let event = make_event("Txn", vec![("acct_id", FieldType::I64)]);
    let agg = make_agg("AcctAgg", "Txn", vec!["acct_id"]);

    registry.apply_registration(
        vec![
            PayloadNode::Event(event),
            PayloadNode::Derivation(make_derivation("AcctAgg", "Txn")),
        ],
        vec![],
        vec![],
        vec![("AcctAgg".to_string(), Arc::new(agg))],
    );

    let mut state_tables: StateTables =
        beava_core::agg_state_table::new_state_tables_for(&registry);
    let row = Row::new().with_field("acct_id", Value::I64(42));
    apply_event_to_aggregations("Txn", &row, 1000, 0, &registry, &mut state_tables, None);

    let inner = registry.read();
    let agg_desc = inner.compiled_aggregations.get("AcctAgg").expect("AcctAgg");
    let tbl = &state_tables[agg_desc.agg_id as usize];

    assert_eq!(
        tbl.single_u64.len(),
        1,
        "single_u64 must have 1 entry for I64 group key"
    );
    assert_eq!(
        tbl.multi.len(),
        0,
        "multi map must be empty for I64 group key"
    );
}

// ── Test 5: SingleStr path for Str-typed group_key ────────────────────────────

/// For a Str-typed single group_key, apply_event dispatches through the
/// single_str sub-map.
/// After applying one event, single_str must have 1 entry and multi must be empty.
#[test]
fn test_apply_routes_single_str_for_str_group_key() {
    let registry = Registry::new();
    let event = make_event("Txn", vec![("user_id", FieldType::Str)]);
    let agg = make_agg("UserAgg", "Txn", vec!["user_id"]);

    registry.apply_registration(
        vec![
            PayloadNode::Event(event),
            PayloadNode::Derivation(make_derivation("UserAgg", "Txn")),
        ],
        vec![],
        vec![],
        vec![("UserAgg".to_string(), Arc::new(agg))],
    );

    let mut state_tables: StateTables =
        beava_core::agg_state_table::new_state_tables_for(&registry);
    let row = Row::new().with_field("user_id", Value::Str("alice".into()));
    apply_event_to_aggregations("Txn", &row, 1000, 0, &registry, &mut state_tables, None);

    let inner = registry.read();
    let agg_desc = inner.compiled_aggregations.get("UserAgg").expect("UserAgg");
    let tbl = &state_tables[agg_desc.agg_id as usize];

    assert_eq!(
        tbl.single_str.len(),
        1,
        "single_str must have 1 entry for Str group key"
    );
    assert_eq!(
        tbl.multi.len(),
        0,
        "multi map must be empty for Str group key"
    );
}
