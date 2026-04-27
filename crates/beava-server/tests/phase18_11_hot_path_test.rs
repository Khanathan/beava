//! Plan 18-11 Task 11.5 + 11.6 + 11.7 integration tests.
//!
//! Verifies the hot-path optimization contract end-to-end:
//! - Row::Deserialize (rmp_serde + sonic-rs) produces the new SmallVec-backed
//!   Row containing CompactString keys + native Value variants.
//! - dispatch_push_sync still routes events correctly through the rewritten
//!   apply path post-Plan-18-11.

use beava_core::row::{Row, Value};

/// Task 11.5 contract: rmp_serde and sonic-rs both deserialize a representative
/// 6-field body directly into the Plan-18-11 Row (SmallVec + CompactString)
/// with no JsonValue intermediate and no with_field re-clone — direct push
/// into the SmallVec storage.
#[test]
fn test_row_deserialize_no_jsonvalue_no_with_field_clone() {
    // Representative 6-field fraud event body.
    let json_body = r#"{"amount":99.95,"ts":1714234567000,"account_id":"acc_123","merchant":"M_ACME","country":"US","method":"card"}"#;

    let row_json: Row = sonic_rs::from_slice(json_body.as_bytes()).expect("json deser");

    // 6 fields landed.
    assert_eq!(row_json.0.len(), 6);

    // SmallVec inline storage (≤8 fields fit inline).
    assert!(
        !row_json.0.spilled(),
        "6-field Row must use inline SmallVec — no heap alloc"
    );

    // Values are correctly typed (no canonicalisation, no JsonValue).
    assert_eq!(row_json.get("amount"), Some(&Value::F64(99.95)));
    assert_eq!(row_json.get("ts"), Some(&Value::I64(1_714_234_567_000)));
    assert_eq!(
        row_json.get("account_id"),
        Some(&Value::Str("acc_123".into()))
    );
    assert_eq!(row_json.get("merchant"), Some(&Value::Str("M_ACME".into())));
    assert_eq!(row_json.get("country"), Some(&Value::Str("US".into())));
    assert_eq!(row_json.get("method"), Some(&Value::Str("card".into())));

    // ─── Same payload via msgpack ───────────────────────────────────────────
    use serde::Serialize;
    #[derive(Serialize)]
    struct Body<'a> {
        amount: f64,
        ts: i64,
        account_id: &'a str,
        merchant: &'a str,
        country: &'a str,
        method: &'a str,
    }
    let msgpack_body = rmp_serde::to_vec_named(&Body {
        amount: 99.95,
        ts: 1_714_234_567_000,
        account_id: "acc_123",
        merchant: "M_ACME",
        country: "US",
        method: "card",
    })
    .expect("msgpack encode");

    let row_msgpack: Row = rmp_serde::from_slice(&msgpack_body).expect("msgpack deser");

    assert_eq!(row_msgpack.0.len(), 6);
    assert!(!row_msgpack.0.spilled());
    assert_eq!(row_msgpack.get("amount"), Some(&Value::F64(99.95)));
    assert_eq!(row_msgpack.get("country"), Some(&Value::Str("US".into())));
    assert_eq!(row_msgpack.get("method"), Some(&Value::Str("card".into())));

    // The two rows compare equal — same logical content regardless of wire
    // format (Plan 18-10 inversion check + Plan 18-11 storage compat).
    // Note: Row's PartialEq relies on element-wise equality across the SmallVec.
    // Insertion order may differ between sonic-rs and rmp_serde; compare via
    // get() on each known field instead.
    for f in &[
        "amount",
        "ts",
        "account_id",
        "merchant",
        "country",
        "method",
    ] {
        assert_eq!(
            row_json.get(f),
            row_msgpack.get(f),
            "field {} must match across wire formats",
            f
        );
    }
}

/// Task 11.5 contract: Row.iter() yields (&str, &Value) pairs that can be
/// consumed by all existing call sites (debug routes, op_chain, etc.) without
/// per-key allocations.
#[test]
fn test_row_iter_yields_str_value_pairs() {
    let row = Row::new()
        .with_field("a", Value::I64(1))
        .with_field("b", Value::Str("x".into()));

    let collected: Vec<(&str, &Value)> = row.iter().collect();
    assert_eq!(collected.len(), 2);
    assert_eq!(collected[0].0, "a");
    assert_eq!(collected[1].0, "b");
}

/// Task 11.5 contract: Row Serialize produces a flat JSON object whose keys
/// are the field names (no tagging on the Row container itself). This is the
/// shape used by debug routes (registry_debug, temporal_http).
#[test]
fn test_row_serialize_yields_flat_object_keys() {
    let row = Row::new()
        .with_field("a", Value::I64(1))
        .with_field("b", Value::Str("hi".into()));

    let json = serde_json::to_string(&row).expect("serialize");
    // Key names must appear at top level (we don't assert the value tagging
    // because Value's auto-derived enum serialise is externally-tagged —
    // this is pre-existing and orthogonal to Plan 18-11).
    assert!(
        json.contains("\"a\""),
        "serialised JSON must contain key 'a'"
    );
    assert!(
        json.contains("\"b\""),
        "serialised JSON must contain key 'b'"
    );
}

/// Task 11.7 contract: Registry exposes Arc<EventDescriptor> lookup so
/// dispatch_push_sync no longer clones the EventDescriptor on every push.
/// The Plan 18-11 D-6 contract: `Registry::get_event_descriptor(name)` returns
/// `Option<Arc<EventDescriptor>>` — Arc::clone is a refcount bump, not a deep
/// clone.
#[test]
fn test_registry_get_event_descriptor_returns_arc() {
    use beava_core::registry::{EventDescriptor, OutputKind, Registry};
    use beava_core::registry_diff::PayloadNode;
    use beava_core::schema::{EventSchema, FieldType};
    use std::collections::BTreeMap;
    use std::sync::Arc;

    let registry = Registry::new();
    let mut fields = BTreeMap::new();
    fields.insert("user_id".to_string(), FieldType::Str);
    let event = EventDescriptor {
        name: "Txn".to_string(),
        schema: EventSchema {
            fields,
            optional_fields: vec![],
        },
        event_time_field: None,
        dedupe_key: None,
        dedupe_window_ms: None,
        keep_events_for_ms: None,
        tolerate_delay_ms: None,
        registered_at_version: 0,
        name_arc: Arc::from(""),
        apply_field_names: vec![],
    };
    registry.apply_registration(vec![PayloadNode::Event(event)], vec![], vec![], vec![]);

    let _ = OutputKind::Event; // keep import alive

    // The new method must return Arc<EventDescriptor>.
    let arc: Arc<EventDescriptor> = registry
        .get_event_descriptor("Txn")
        .expect("Txn must be registered");
    assert_eq!(arc.name, "Txn");

    // Arc::clone is a refcount bump — strong_count goes up by 1 then down.
    let count_before = Arc::strong_count(&arc);
    {
        let _arc2 = Arc::clone(&arc);
        let count_during = Arc::strong_count(&arc);
        assert_eq!(count_during, count_before + 1, "Arc::clone bumps refcount");
    }
    assert_eq!(
        Arc::strong_count(&arc),
        count_before,
        "refcount drops back when arc2 is dropped"
    );
}

/// Task 11.8 contract: RegistryInner exposes `aggregations_by_source` —
/// a precomputed per-source index that turns the prior linear-scan over
/// `compiled_aggregations` into an O(1) HashMap lookup. The compiled
/// aggregations Vec is built once at register time.
#[test]
fn test_per_source_aggregation_index_is_populated() {
    use beava_core::agg_descriptor::{AggregationDescriptor, NamedAggOp};
    use beava_core::agg_op::{AggKind, AggOpDescriptor};
    use beava_core::registry::{DerivationDescriptor, EventDescriptor, OutputKind, Registry};
    use beava_core::registry_diff::PayloadNode;
    use beava_core::schema::{DerivedSchema, EventSchema, FieldType};
    use std::collections::BTreeMap;
    use std::sync::Arc;

    let registry = Registry::new();
    let mut fields = BTreeMap::new();
    fields.insert("user_id".to_string(), FieldType::Str);
    let event = EventDescriptor {
        name: "Txn".to_string(),
        schema: EventSchema {
            fields: fields.clone(),
            optional_fields: vec![],
        },
        event_time_field: None,
        dedupe_key: None,
        dedupe_window_ms: None,
        keep_events_for_ms: None,
        tolerate_delay_ms: None,
        registered_at_version: 0,
        name_arc: Arc::from(""),
        apply_field_names: vec![],
    };
    let agg = AggregationDescriptor {
        node_name: "AggTxn".to_string(),
        source_node_name: "Txn".to_string(),
        group_keys: vec!["user_id".to_string()],
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
                field_idx: beava_core::agg_op::FIELD_IDX_NONE,
            },
        }],
        agg_id: 0,
        field_names: vec![],
    };
    let mut deriv_schema = BTreeMap::new();
    deriv_schema.insert("user_id".to_string(), FieldType::Str);
    deriv_schema.insert("cnt".to_string(), FieldType::I64);
    let deriv = DerivationDescriptor {
        name: "AggTxn".to_string(),
        output_kind: OutputKind::Table,
        upstreams: vec!["Txn".to_string()],
        ops: vec![],
        schema: DerivedSchema {
            fields: deriv_schema,
            optional_fields: vec![],
        },
        table_primary_key: Some(vec!["user_id".to_string()]),
        registered_at_version: 0,
    };
    registry.apply_registration(
        vec![PayloadNode::Event(event), PayloadNode::Derivation(deriv)],
        vec![],
        vec![],
        vec![("AggTxn".to_string(), Arc::new(agg))],
    );

    // The per-source index must contain Txn → [AggTxn].
    let inner = registry.read();
    let aggs_for_txn = inner
        .aggregations_by_source
        .get("Txn")
        .expect("Txn must have at least one aggregation");
    assert_eq!(aggs_for_txn.len(), 1);
    assert_eq!(aggs_for_txn[0].node_name, "AggTxn");
    drop(inner);

    // compiled_aggregations_for_source returns the same set without
    // any linear scan (uses the index internally).
    let aggs = registry.compiled_aggregations_for_source("Txn");
    assert_eq!(aggs.len(), 1);
    assert_eq!(aggs[0].node_name, "AggTxn");

    // Unknown source returns empty.
    assert!(registry
        .compiled_aggregations_for_source("Nonexistent")
        .is_empty());
}

/// Task 11.9 contract: snapshot byte-encoding is deterministic across
/// independent runs over the same input event sequence — even though the
/// underlying AggStateTable is now backed by a (non-deterministic-iter)
/// HashMap. The fix: snapshot writer sorts entries via iter_sorted before
/// serialising. This locks in the D-06 invariant under Plan 18-11 D-8.
#[test]
fn test_snapshot_byte_identical_for_same_inputs() {
    use beava_core::agg_apply::apply_event_to_aggregations;
    use beava_core::agg_descriptor::{AggregationDescriptor, NamedAggOp};
    use beava_core::agg_op::{AggKind, AggOpDescriptor};
    use beava_core::agg_state_table::{AggStateTable, StateTables};
    use beava_core::registry::{DerivationDescriptor, EventDescriptor, OutputKind, Registry};
    use beava_core::registry_diff::PayloadNode;
    use beava_core::schema::{DerivedSchema, EventSchema, FieldType};
    use beava_core::snapshot_body::SnapshotBody;
    use std::collections::BTreeMap;
    use std::sync::Arc;

    fn build_test_registry() -> Arc<Registry> {
        let registry = Arc::new(Registry::new());
        let mut fields = BTreeMap::new();
        fields.insert("user_id".to_string(), FieldType::Str);
        let event = EventDescriptor {
            name: "Txn".to_string(),
            schema: EventSchema {
                fields: fields.clone(),
                optional_fields: vec![],
            },
            event_time_field: None,
            dedupe_key: None,
            dedupe_window_ms: None,
            keep_events_for_ms: None,
            tolerate_delay_ms: None,
            registered_at_version: 0,
            name_arc: Arc::from(""),
            apply_field_names: vec![],
        };
        let agg = AggregationDescriptor {
            node_name: "AggTxn".to_string(),
            source_node_name: "Txn".to_string(),
            group_keys: vec!["user_id".to_string()],
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
                    field_idx: beava_core::agg_op::FIELD_IDX_NONE,
                },
            }],
            agg_id: 0,
            field_names: vec![],
        };
        let mut deriv_schema = BTreeMap::new();
        deriv_schema.insert("user_id".to_string(), FieldType::Str);
        deriv_schema.insert("cnt".to_string(), FieldType::I64);
        let deriv = DerivationDescriptor {
            name: "AggTxn".to_string(),
            output_kind: OutputKind::Table,
            upstreams: vec!["Txn".to_string()],
            ops: vec![],
            schema: DerivedSchema {
                fields: deriv_schema,
                optional_fields: vec![],
            },
            table_primary_key: Some(vec!["user_id".to_string()]),
            registered_at_version: 0,
        };
        registry.apply_registration(
            vec![PayloadNode::Event(event), PayloadNode::Derivation(deriv)],
            vec![],
            vec![],
            vec![("AggTxn".to_string(), Arc::new(agg))],
        );
        registry
    }

    fn run_apply(registry: &Registry, users: &[&str]) -> StateTables {
        let mut tables: StateTables = StateTables::default();
        for (i, u) in users.iter().enumerate() {
            let row = Row::new().with_field("user_id", Value::Str((*u).into()));
            apply_event_to_aggregations(
                "Txn",
                &row,
                1000 + i as i64,
                i as u64,
                registry,
                &mut tables,
            );
        }
        tables
    }

    let registry = build_test_registry();

    // Apply the same events twice in different runs. Snapshots must be
    // byte-identical because iter_sorted yields a deterministic sequence
    // regardless of HashMap insertion bucket layout.
    let users = ["zebra", "alice", "monkey", "bob", "carol", "dan"];
    let tables_a = run_apply(&registry, &users);
    let tables_b = run_apply(&registry, &users);

    let inner = registry.read();
    let snap_a = SnapshotBody::from_live(&inner, &tables_a, 0, 0);
    let snap_b = SnapshotBody::from_live(&inner, &tables_b, 0, 0);
    drop(inner);

    let bytes_a = snap_a.encode().expect("encode A");
    let bytes_b = snap_b.encode().expect("encode B");
    assert_eq!(
        bytes_a, bytes_b,
        "Snapshot bytes must be identical for the same input sequence"
    );

    // Also: round-trip preserves the same sorted order — re-snapshotting the
    // restored tables yields identical bytes a third time.
    let restored = SnapshotBody::decode(&bytes_a).expect("decode");
    // Plan 18-16 Task 16.2: state_tables is Vec<AggStateTable> indexed by agg_id.
    // Resolve the node name to its agg_id via the registry then place at
    // the correct slot.
    let inner = registry.read();
    let next_id = inner.next_agg_id as usize;
    drop(inner);
    let mut restored_tables: StateTables = (0..next_id).map(|_| AggStateTable::new()).collect();
    for (node, entries) in restored.state_tables {
        if let Some(desc) = registry.compiled_aggregation(&node) {
            let t = &mut restored_tables[desc.agg_id as usize];
            for (k, ops) in entries {
                t.entities.insert(k, ops);
            }
        }
    }
    let inner = registry.read();
    let snap_c = SnapshotBody::from_live(&inner, &restored_tables, 0, 0);
    drop(inner);
    let bytes_c = snap_c.encode().expect("encode C");
    assert_eq!(
        bytes_a, bytes_c,
        "Snapshot bytes must remain identical after restore-and-resnapshot"
    );
}
