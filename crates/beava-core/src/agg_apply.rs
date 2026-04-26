//! Apply-loop hook: routes a push event to every matching aggregation.
//!
//! # SDK-AGG-02, AGG-CORE-09
//!
//! `apply_event_to_aggregations` is the single-writer entry point for stateful
//! feature updates. It is called:
//! - in Phase 5, by the dev endpoint (`POST /dev/apply_events`);
//! - in Phase 6, by the production push handler (WAL group-commit path).
//!
//! ## D-06 determinism invariants
//!
//! This function is a **pure function** of `(source_name, row, event_time_ms,
//! registry state, prior agg state)`.  No wall-clock reads.  No random sources.
//! Safe for WAL replay (SC4).
//!
//! ## Why `event_id: u64` is in the signature now (Phase 5)
//!
//! Phase 6 WAL will pass the stable event identifier from the WAL record (see
//! D-08 + `memory/project_stateful_architecture.md`).  The parameter is threaded
//! through here so Phase 6 does not need to change the signature of every caller.
//! In Phase 5 it is ignored (prefixed `_event_id`).  Dev-endpoint callers pass a
//! monotonic counter (0, 1, 2, …).

use std::collections::BTreeMap;

use crate::agg_state_table::{AggStateTable, EntityKey};
use crate::registry::Registry;
use crate::row::Row;

/// Apply a single event to every aggregation whose `source_node_name` matches
/// `source_name`.
///
/// # Semantics
///
/// 1. Look up all aggregations for `source_name` via
///    `Registry::compiled_aggregations_for_source`.
/// 2. For each aggregation:
///    - Derive `EntityKey` from `row` + `descriptor.group_keys`.
///      If any group-key field is null/missing → drop the event for this
///      aggregation (continue to the next).
///    - Look up or initialise the entity row in the aggregation's
///      `AggStateTable`.
///    - For each feature: call `AggOp::update_with_row(row, event_time_ms,
///      field, where_expr)`.
///
/// # `event_id` parameter
///
/// `_event_id` is deliberately prefixed with `_` to silence the
/// `unused_variables` lint while preserving the exact parameter name in the
/// signature for Phase 6.  **Do NOT remove this parameter.**  Phase 6 WAL will
/// populate it with the stable WAL event identifier (D-08); callers must not
/// break their signatures.
///
/// # No wall-clock reads
///
/// `event_time_ms` is the only time source.  Wall-clock reads are forbidden
/// in this function (D-06).
pub fn apply_event_to_aggregations(
    source_name: &str,
    row: &Row,
    event_time_ms: i64,
    _event_id: u64, // Phase 5: unused. Phase 6 WAL populates via D-08.
    registry: &Registry,
    state_tables: &mut BTreeMap<String, AggStateTable>,
) {
    // SPIKE: per-substage timing of the agg hot path.
    let trace = std::env::var("BEAVA_TRACE_APPLY_TIMING").ok().as_deref() == Some("1");
    let t0 = if trace { Some(std::time::Instant::now()) } else { None };

    let descs = registry.compiled_aggregations_for_source(source_name);
    let t_registry = t0.map(|t| t.elapsed());

    let mut t_entity_key_total = std::time::Duration::ZERO;
    let mut t_table_lookup_total = std::time::Duration::ZERO;
    let mut t_entity_row_total = std::time::Duration::ZERO;
    let mut t_features_total = std::time::Duration::ZERO;
    let mut feat_updates: u32 = 0;
    let mut desc_count: u32 = 0;

    for desc in descs {
        desc_count += 1;
        let t_a = t0.map(|t| t.elapsed());

        let entity_key = match EntityKey::from_row(&desc.group_keys, row) {
            Some(k) => k,
            None => continue,
        };
        let t_b = t0.map(|t| t.elapsed());

        let table = state_tables.entry(desc.node_name.clone()).or_default();
        let t_c = t0.map(|t| t.elapsed());

        let entity_row = table.get_or_init(&entity_key, &desc);
        let t_d = t0.map(|t| t.elapsed());

        for (i, feat) in desc.features.iter().enumerate() {
            entity_row[i].update_with_row(
                row,
                event_time_ms,
                feat.descriptor.field.as_deref(),
                feat.descriptor.where_expr.as_ref(),
            );
            feat_updates += 1;
        }
        let t_e = t0.map(|t| t.elapsed());

        if let (Some(a), Some(b), Some(c), Some(d), Some(e)) = (t_a, t_b, t_c, t_d, t_e) {
            t_entity_key_total += b - a;
            t_table_lookup_total += c - b;
            t_entity_row_total += d - c;
            t_features_total += e - d;
        }
    }

    if let (Some(t0_inst), Some(reg)) = (t0, t_registry) {
        let total = t0_inst.elapsed();
        eprintln!(
            "TRACE_AGG ns: descs={} feat_updates={} registry_call={} entity_key={} table_lookup={} entity_row_init={} features={} TOTAL={}",
            desc_count,
            feat_updates,
            reg.as_nanos(),
            t_entity_key_total.as_nanos(),
            t_table_lookup_total.as_nanos(),
            t_entity_row_total.as_nanos(),
            t_features_total.as_nanos(),
            total.as_nanos()
        );
    }
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agg_descriptor::{AggregationDescriptor, NamedAggOp};
    use crate::agg_op::{AggKind, AggOpDescriptor};
    use crate::registry::{DerivationDescriptor, EventDescriptor, OutputKind, Registry};
    use crate::registry_diff::PayloadNode;
    use crate::row::{Row, Value};
    use crate::schema::{DerivedSchema, EventSchema, FieldType};
    use std::collections::BTreeMap;
    use std::sync::Arc;

    // ── Helpers ──────────────────────────────────────────────────────────────

    fn simple_event_schema() -> EventSchema {
        let mut fields = BTreeMap::new();
        fields.insert("user_id".to_string(), FieldType::Str);
        fields.insert("amount".to_string(), FieldType::F64);
        fields.insert("status".to_string(), FieldType::Str);
        EventSchema {
            fields,
            optional_fields: vec![],
        }
    }

    fn make_event(name: &str) -> EventDescriptor {
        EventDescriptor {
            name: name.to_string(),
            schema: simple_event_schema(),
            event_time_field: None,
            dedupe_key: None,
            dedupe_window_ms: None,
            keep_events_for_ms: None,
            tolerate_delay_ms: None,
            registered_at_version: 0,
        }
    }

    fn make_agg_desc(
        node_name: &str,
        source: &str,
        keys: &[&str],
        features: &[(&str, AggOpDescriptor)],
    ) -> AggregationDescriptor {
        AggregationDescriptor {
            node_name: node_name.to_string(),
            source_node_name: source.to_string(),
            group_keys: keys.iter().map(|k| k.to_string()).collect(),
            features: features
                .iter()
                .map(|(name, d)| NamedAggOp {
                    feature_name: name.to_string(),
                    descriptor: d.clone(),
                })
                .collect(),
        }
    }

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
        }
    }

    fn sum_desc(field: &str) -> AggOpDescriptor {
        AggOpDescriptor {
            kind: AggKind::Sum,
            field: Some(field.to_string()),
            window_ms: None,
            where_expr: None,
            n: None,
            half_life_ms: None,
            sub_window_ms: None,
            sigma: None,
            sketch_params: None,
            ext: Default::default(),
        }
    }

    fn make_registry_with_agg(event_name: &str, agg: AggregationDescriptor) -> Arc<Registry> {
        let registry = Arc::new(Registry::new());
        let deriv_name = agg.node_name.clone();

        let deriv = DerivationDescriptor {
            name: deriv_name.clone(),
            output_kind: OutputKind::Table,
            upstreams: vec![event_name.to_string()],
            ops: vec![],
            schema: DerivedSchema {
                fields: BTreeMap::new(),
                optional_fields: vec![],
            },
            table_primary_key: None,
            registered_at_version: 0,
        };

        registry.apply_registration(
            vec![
                PayloadNode::Event(make_event(event_name)),
                PayloadNode::Derivation(deriv),
            ],
            vec![],
            vec![],
            vec![(deriv_name, Arc::new(agg))],
        );

        registry
    }

    // ── apply_event_to_aggregations tests ─────────────────────────────────────

    /// A01: Event routes to matching source only — not to aggregations with a
    /// different source.
    #[test]
    fn apply_routes_event_to_matching_source_only() {
        // Register AggA (source=Transaction) and AggB (source=Login).
        let registry = Arc::new(Registry::new());

        let agg_a = make_agg_desc(
            "AggA",
            "Transaction",
            &["user_id"],
            &[("cnt", count_desc())],
        );
        let agg_b = make_agg_desc("AggB", "Login", &["user_id"], &[("cnt", count_desc())]);

        let deriv_a = DerivationDescriptor {
            name: "AggA".to_string(),
            output_kind: OutputKind::Table,
            upstreams: vec!["Transaction".to_string()],
            ops: vec![],
            schema: DerivedSchema {
                fields: BTreeMap::new(),
                optional_fields: vec![],
            },
            table_primary_key: None,
            registered_at_version: 0,
        };
        let deriv_b = DerivationDescriptor {
            name: "AggB".to_string(),
            output_kind: OutputKind::Table,
            upstreams: vec!["Login".to_string()],
            ops: vec![],
            schema: DerivedSchema {
                fields: BTreeMap::new(),
                optional_fields: vec![],
            },
            table_primary_key: None,
            registered_at_version: 0,
        };

        registry.apply_registration(
            vec![
                PayloadNode::Event(make_event("Transaction")),
                PayloadNode::Event(make_event("Login")),
                PayloadNode::Derivation(deriv_a),
                PayloadNode::Derivation(deriv_b),
            ],
            vec![],
            vec![],
            vec![
                ("AggA".to_string(), Arc::new(agg_a)),
                ("AggB".to_string(), Arc::new(agg_b)),
            ],
        );

        let mut state_tables: BTreeMap<String, AggStateTable> = BTreeMap::new();
        let row = Row::new().with_field("user_id", Value::Str("alice".into()));

        apply_event_to_aggregations("Transaction", &row, 1000, 0, &registry, &mut state_tables);

        // AggA's table should be populated; AggB's table should NOT.
        assert!(
            state_tables.contains_key("AggA"),
            "AggA must be populated for Transaction events"
        );
        assert!(
            !state_tables.contains_key("AggB"),
            "AggB must NOT be populated for Transaction events"
        );
    }

    /// A02: Count aggregation, 10 events → count == I64(10).
    #[test]
    fn apply_increments_count_feature() {
        let agg = make_agg_desc(
            "UserCount",
            "Transaction",
            &["user_id"],
            &[("cnt", count_desc())],
        );
        let registry = make_registry_with_agg("Transaction", agg);

        let mut state_tables: BTreeMap<String, AggStateTable> = BTreeMap::new();
        let row = Row::new().with_field("user_id", Value::Str("alice".into()));

        for i in 0..10 {
            apply_event_to_aggregations(
                "Transaction",
                &row,
                1000 + i,
                i as u64,
                &registry,
                &mut state_tables,
            );
        }

        let table = state_tables
            .get("UserCount")
            .expect("UserCount table must exist");
        let key = crate::agg_state_table::EntityKey({
            let mut sv: smallvec::SmallVec<
                [(compact_str::CompactString, Value); 2],
            > = smallvec::SmallVec::new();
            sv.push(("user_id".into(), Value::Str("alice".into())));
            sv
        });
        let val = table
            .query_feature(&key, 0, 10_000)
            .expect("must have value");
        assert_eq!(val, Value::I64(10), "count must be 10 after 10 events");
    }

    /// A03: Event with null group-key is dropped — no state_table entry created.
    #[test]
    fn apply_drops_events_with_null_group_key() {
        let agg = make_agg_desc(
            "UserCount",
            "Transaction",
            &["user_id"],
            &[("cnt", count_desc())],
        );
        let registry = make_registry_with_agg("Transaction", agg);

        let mut state_tables: BTreeMap<String, AggStateTable> = BTreeMap::new();
        // Row with user_id = Null → should be dropped.
        let row = Row::new().with_field("user_id", Value::Null);

        apply_event_to_aggregations("Transaction", &row, 1000, 0, &registry, &mut state_tables);

        // No state should exist at all.
        let is_empty = state_tables
            .get("UserCount")
            .map(|t| t.entity_count() == 0)
            .unwrap_or(true);
        assert!(
            is_empty,
            "null group-key event must not create any entity state"
        );
    }

    /// A04: where predicate = "(amount > 100)"; amount=50 event → entity row
    /// created but count feature stays at I64(0).
    ///
    /// Semantics (D-03): `AggOp::update_with_row` gates the update per feature.
    /// The entity row IS created (get_or_init is called before evaluating the
    /// predicate), but the per-feature update is skipped when where=false.
    ///
    /// NOTE: Revised semantics — entity row is NOT created if we guard before
    /// get_or_init. Either is acceptable; DOCUMENT which is chosen. This test
    /// accepts EITHER: entity row absent OR entity row present with count=0.
    #[test]
    fn apply_with_where_false_skips_update() {
        let where_expr =
            std::sync::Arc::new(crate::expr::parse("(amount > 100)").expect("parse where expr"));
        let agg = make_agg_desc(
            "UserCount",
            "Transaction",
            &["user_id"],
            &[(
                "cnt",
                AggOpDescriptor {
                    kind: AggKind::Count,
                    field: None,
                    window_ms: None,
                    where_expr: Some(where_expr),
                    n: None,
                    half_life_ms: None,
                    sub_window_ms: None,
                    sigma: None,
                    sketch_params: None,
                    ext: Default::default(),
                },
            )],
        );
        let registry = make_registry_with_agg("Transaction", agg);

        let mut state_tables: BTreeMap<String, AggStateTable> = BTreeMap::new();
        let row = Row::new()
            .with_field("user_id", Value::Str("alice".into()))
            .with_field("amount", Value::F64(50.0)); // below threshold

        apply_event_to_aggregations("Transaction", &row, 1000, 0, &registry, &mut state_tables);

        // Either: no entry for alice, OR alice's count == 0.
        let count = state_tables.get("UserCount").and_then(|t| {
            let key = crate::agg_state_table::EntityKey({
                let mut sv: smallvec::SmallVec<
                    [(compact_str::CompactString, Value); 2],
                > = smallvec::SmallVec::new();
                sv.push(("user_id".into(), Value::Str("alice".into())));
                sv
            });
            t.query_feature(&key, 0, 10_000)
        });

        match count {
            None => {}                // Acceptable: no entity row created
            Some(Value::I64(0)) => {} // Acceptable: entity row exists but count=0
            Some(other) => panic!("where=false must not increment count; got {:?}", other),
        }
    }

    /// A05: Replay determinism — apply same 5-event stream twice; Debug repr
    /// of state_table must be byte-identical.
    #[test]
    fn apply_replay_determinism() {
        let agg = make_agg_desc(
            "UserCount",
            "Transaction",
            &["user_id"],
            &[("cnt", count_desc())],
        );
        let registry = make_registry_with_agg("Transaction", agg);

        let events: Vec<(Row, i64)> = (0..5)
            .map(|i| {
                let row = Row::new()
                    .with_field("user_id", Value::Str(format!("user_{}", i % 2).into()));
                (row, 1000 + i)
            })
            .collect();

        let apply_all = |tables: &mut BTreeMap<String, AggStateTable>| {
            for (i, (row, t)) in events.iter().enumerate() {
                apply_event_to_aggregations("Transaction", row, *t, i as u64, &registry, tables);
            }
        };

        let mut tables1: BTreeMap<String, AggStateTable> = BTreeMap::new();
        let mut tables2: BTreeMap<String, AggStateTable> = BTreeMap::new();
        apply_all(&mut tables1);
        apply_all(&mut tables2);

        assert_eq!(
            format!("{:?}", tables1.get("UserCount").map(|t| &t.entities)),
            format!("{:?}", tables2.get("UserCount").map(|t| &t.entities)),
            "apply_event_to_aggregations must be deterministic (D-06)"
        );
    }

    /// A06: Multi-feature aggregation [count, sum(amount)] updated correctly.
    #[test]
    fn apply_multi_feature_aggregation_updates_all() {
        let agg = make_agg_desc(
            "UserStats",
            "Transaction",
            &["user_id"],
            &[("cnt", count_desc()), ("total", sum_desc("amount"))],
        );
        let registry = make_registry_with_agg("Transaction", agg);

        let mut state_tables: BTreeMap<String, AggStateTable> = BTreeMap::new();
        let amounts = [10.0_f64, 20.0, 30.0, 40.0, 50.0];
        for (i, &amt) in amounts.iter().enumerate() {
            let row = Row::new()
                .with_field("user_id", Value::Str("alice".into()))
                .with_field("amount", Value::F64(amt));
            apply_event_to_aggregations(
                "Transaction",
                &row,
                1000 + i as i64,
                i as u64,
                &registry,
                &mut state_tables,
            );
        }

        let table = state_tables.get("UserStats").expect("UserStats must exist");
        let key = crate::agg_state_table::EntityKey({
            let mut sv: smallvec::SmallVec<
                [(compact_str::CompactString, Value); 2],
            > = smallvec::SmallVec::new();
            sv.push(("user_id".into(), Value::Str("alice".into())));
            sv
        });

        let cnt = table
            .query_feature(&key, 0, 10_000)
            .expect("cnt must exist");
        assert_eq!(cnt, Value::I64(5), "count must be 5");

        let total = table
            .query_feature(&key, 1, 10_000)
            .expect("total must exist");
        match total {
            Value::F64(v) => assert!((v - 150.0).abs() < 1e-10, "total must be 150.0, got {v}"),
            other => panic!("expected F64 for total, got {:?}", other),
        }
    }

    /// A07: event_id has no observable effect in Phase 5.
    ///
    /// Apply the SAME (row, event_time_ms) twice — once with event_id=0 and
    /// once with event_id=99 — into two independent state_table instances.
    /// The resulting state must be identical.
    #[test]
    fn apply_accepts_event_id_and_ignores_it_in_phase_5() {
        let agg = make_agg_desc(
            "UserCount",
            "Transaction",
            &["user_id"],
            &[("cnt", count_desc())],
        );
        let registry = make_registry_with_agg("Transaction", agg);

        let row = Row::new().with_field("user_id", Value::Str("alice".into()));
        let t = 1000_i64;

        // Apply with event_id=0.
        let mut tables_0: BTreeMap<String, AggStateTable> = BTreeMap::new();
        apply_event_to_aggregations("Transaction", &row, t, 0, &registry, &mut tables_0);

        // Apply with event_id=99.
        let mut tables_99: BTreeMap<String, AggStateTable> = BTreeMap::new();
        apply_event_to_aggregations("Transaction", &row, t, 99, &registry, &mut tables_99);

        // State must be identical regardless of event_id.
        assert_eq!(
            format!("{:?}", tables_0.get("UserCount").map(|t| &t.entities)),
            format!("{:?}", tables_99.get("UserCount").map(|t| &t.entities)),
            "event_id must have no observable effect in Phase 5"
        );
    }

    /// A08: No wall-clock reads or rand in agg_apply.rs (D-06 grep guard).
    #[test]
    fn no_systemtime_now_in_agg_apply() {
        let src = include_str!("agg_apply.rs");
        let forbidden_clock = ["SystemTime", "::", "now"].concat();
        let forbidden_rand = ["rand", "::"].concat();
        assert!(
            !src.contains(forbidden_clock.as_str()),
            "agg_apply.rs must not use wall-clock reads (D-06)"
        );
        assert!(
            !src.contains(forbidden_rand.as_str()),
            "agg_apply.rs must not use rand crate (D-06)"
        );
    }
}

// ─── Registry extension tests ─────────────────────────────────────────────────

#[cfg(test)]
mod registry_source_tests {
    use crate::agg_descriptor::{AggregationDescriptor, NamedAggOp};
    use crate::agg_op::{AggKind, AggOpDescriptor};
    use crate::registry::{DerivationDescriptor, EventDescriptor, OutputKind, Registry};
    use crate::registry_diff::PayloadNode;
    use crate::schema::{DerivedSchema, EventSchema, FieldType};
    use std::collections::BTreeMap;
    use std::sync::Arc;

    fn make_event(name: &str) -> EventDescriptor {
        let mut fields = BTreeMap::new();
        fields.insert("user_id".to_string(), FieldType::Str);
        EventDescriptor {
            name: name.to_string(),
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
        }
    }

    fn make_agg(node_name: &str, source: &str) -> AggregationDescriptor {
        AggregationDescriptor {
            node_name: node_name.to_string(),
            source_node_name: source.to_string(),
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
                },
            }],
        }
    }

    /// R01: Two aggregations with source=Transaction; lookup returns both.
    #[test]
    fn compiled_aggregations_for_source_returns_matching() {
        let registry = Arc::new(Registry::new());

        let agg1 = make_agg("Agg1", "Transaction");
        let agg2 = make_agg("Agg2", "Transaction");
        let agg3 = make_agg("Agg3", "Login");

        for (name, event_name, agg) in [
            ("Agg1", "Transaction", agg1),
            ("Agg2", "Transaction", agg2),
            ("Agg3", "Login", agg3),
        ] {
            let deriv = DerivationDescriptor {
                name: name.to_string(),
                output_kind: OutputKind::Table,
                upstreams: vec![event_name.to_string()],
                ops: vec![],
                schema: DerivedSchema {
                    fields: BTreeMap::new(),
                    optional_fields: vec![],
                },
                table_primary_key: None,
                registered_at_version: 0,
            };
            registry.apply_registration(
                vec![
                    PayloadNode::Event(make_event(event_name)),
                    PayloadNode::Derivation(deriv),
                ],
                vec![],
                vec![],
                vec![(name.to_string(), Arc::new(agg))],
            );
        }

        let txn_aggs = registry.compiled_aggregations_for_source("Transaction");
        assert_eq!(
            txn_aggs.len(),
            2,
            "two aggregations should match source=Transaction"
        );
        let names: Vec<&str> = txn_aggs.iter().map(|a| a.node_name.as_str()).collect();
        assert!(names.contains(&"Agg1"), "Agg1 must be in results");
        assert!(names.contains(&"Agg2"), "Agg2 must be in results");
    }

    /// R02: Lookup for unknown source → empty Vec.
    #[test]
    fn compiled_aggregations_for_source_empty_for_unknown() {
        let registry = Arc::new(Registry::new());
        let result = registry.compiled_aggregations_for_source("Nonexistent");
        assert!(
            result.is_empty(),
            "unknown source must return empty Vec, got {} entries",
            result.len()
        );
    }
}
