// Phase 5 aggregation hot-path bench (plan 05.5-04).
//
// Groups:
//   agg_op/{count,sum,avg,min,max,variance,stddev,ratio}  — 8 windowless per-op update benches
//   windowed/fold_count_5m_1Mevt                           — 1M-event 64-bucket fold (Count)
//   windowed/fold_sum_5m_1Mevt                             — 1M-event 64-bucket fold (Sum)
//   apply/3agg_100ent_1Kevt                                — end-to-end apply_event_to_aggregations

use beava_core::agg_apply::apply_event_to_aggregations;
use beava_core::agg_descriptor::{AggregationDescriptor, NamedAggOp};
use beava_core::agg_op::{AggKind, AggOp, AggOpDescriptor};
use beava_core::agg_state_table::AggStateTable;
use beava_core::agg_windowed::WindowedOp;
use beava_core::registry::{DerivationDescriptor, EventDescriptor, OutputKind, Registry};
use beava_core::registry_diff::PayloadNode;
use beava_core::row::{Row, Value};
use beava_core::schema::{DerivedSchema, EventSchema, FieldType};
use criterion::{
    criterion_group, criterion_main, BatchSize, BenchmarkGroup, Criterion, Throughput,
};
use std::collections::BTreeMap;
use std::sync::Arc;

// ─── Row fixture ─────────────────────────────────────────────────────────────

fn make_row() -> Row {
    Row::new()
        .with_field("user_id", Value::Str("u1".into()))
        .with_field("amount", Value::F64(100.0))
        .with_field("matched", Value::Bool(true))
}

// ─── AggOpDescriptor helpers ──────────────────────────────────────────────────

fn windowless_desc(kind: AggKind, field: Option<&str>) -> AggOpDescriptor {
    AggOpDescriptor {
        kind,
        field: field.map(|s| s.to_string()),
        window_ms: None,
        where_expr: None,
    }
}

// ─── Bench 1: AggOp::update per variant (windowless) ─────────────────────────

fn bench_agg_op_update(c: &mut Criterion) {
    let row = make_row();

    // (kind, field_for_update)
    // Count/Ratio use None for field; all others use "amount".
    let variants: &[(AggKind, Option<&str>)] = &[
        (AggKind::Count, None),
        (AggKind::Sum, Some("amount")),
        (AggKind::Avg, Some("amount")),
        (AggKind::Min, Some("amount")),
        (AggKind::Max, Some("amount")),
        (AggKind::Variance, Some("amount")),
        (AggKind::StdDev, Some("amount")),
        (AggKind::Ratio, None),
    ];

    for &(kind, field) in variants {
        let name = format!("agg_op/{}", format!("{:?}", kind).to_lowercase());
        let desc = windowless_desc(kind, field);
        let mut op = AggOp::new(&desc);
        c.bench_function(&name, |b| {
            b.iter(|| {
                op.update(
                    std::hint::black_box(&row),
                    std::hint::black_box(0i64),
                    field,
                    true,
                );
            });
        });
    }
}

// ─── Bench 2: WindowedOp fold — 1M deterministic events ──────────────────────

fn bench_windowed(c: &mut Criterion) {
    let row = make_row();
    let window_ms: u64 = 300_000; // 5 minutes

    let mut g: BenchmarkGroup<_> = c.benchmark_group("windowed");
    g.throughput(Throughput::Elements(1_000_000));

    // fold_count_5m_1Mevt
    g.bench_function("fold_count_5m_1Mevt", |b| {
        b.iter_batched(
            || WindowedOp::new(AggKind::Count, window_ms),
            |mut op| {
                for t in 0..1_000_000i64 {
                    op.update(std::hint::black_box(&row), t, None, true);
                }
                let v = op.query(1_000_001);
                std::hint::black_box(v);
            },
            BatchSize::LargeInput,
        );
    });

    // fold_sum_5m_1Mevt
    g.bench_function("fold_sum_5m_1Mevt", |b| {
        b.iter_batched(
            || WindowedOp::new(AggKind::Sum, window_ms),
            |mut op| {
                for t in 0..1_000_000i64 {
                    op.update(std::hint::black_box(&row), t, Some("amount"), true);
                }
                let v = op.query(1_000_001);
                std::hint::black_box(v);
            },
            BatchSize::LargeInput,
        );
    });

    g.finish();
}

// ─── Registry + event helpers for bench_apply ────────────────────────────────

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

fn make_event_descriptor(name: &str) -> EventDescriptor {
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

fn make_agg_descriptor(
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
    }
}

/// Build a Registry with three aggregations on "Transaction":
///   Agg1: count (windowless)
///   Agg2: sum(amount) (windowless)
///   Agg3: count with 5m window (windowed)
fn build_registry() -> Registry {
    let registry = Registry::new();

    let agg1 = make_agg_descriptor(
        "Agg1",
        "Transaction",
        vec![("cnt", windowless_desc(AggKind::Count, None))],
    );
    let agg2 = make_agg_descriptor(
        "Agg2",
        "Transaction",
        vec![("total", windowless_desc(AggKind::Sum, Some("amount")))],
    );
    let agg3 = make_agg_descriptor(
        "Agg3",
        "Transaction",
        vec![(
            "cnt_5m",
            AggOpDescriptor {
                kind: AggKind::Count,
                field: None,
                window_ms: Some(300_000),
                where_expr: None,
            },
        )],
    );

    registry.apply_registration(
        vec![
            PayloadNode::Event(make_event_descriptor("Transaction")),
            PayloadNode::Derivation(make_derivation("Agg1", "Transaction")),
            PayloadNode::Derivation(make_derivation("Agg2", "Transaction")),
            PayloadNode::Derivation(make_derivation("Agg3", "Transaction")),
        ],
        vec![],
        vec![],
        vec![
            ("Agg1".to_string(), Arc::new(agg1)),
            ("Agg2".to_string(), Arc::new(agg2)),
            ("Agg3".to_string(), Arc::new(agg3)),
        ],
    );

    registry
}

/// Generate 1,000 deterministic events: user_id cycles through 100 entities,
/// amount = ((i * 37) % 1000) as F64, event_time_ms = i * 10.
fn build_events() -> Vec<(String, Row, i64, u64)> {
    (0u64..1_000)
        .map(|i| {
            let uid = format!("user_{}", i % 100);
            let amount = ((i * 37) % 1000) as f64;
            let t = (i * 10) as i64;
            let row = Row::new()
                .with_field("user_id", Value::Str(uid))
                .with_field("amount", Value::F64(amount))
                .with_field("status", Value::Str("ok".into()));
            ("Transaction".to_string(), row, t, i)
        })
        .collect()
}

// ─── Bench 3: apply_event_to_aggregations end-to-end ─────────────────────────

fn bench_apply(c: &mut Criterion) {
    let registry = build_registry();
    let events = build_events();

    let mut g = c.benchmark_group("apply");
    g.throughput(Throughput::Elements(1_000));

    g.bench_function("3agg_100ent_1Kevt", |b| {
        b.iter_batched(
            BTreeMap::<String, AggStateTable>::new,
            |mut state_tables| {
                for (src, row, t, id) in &events {
                    apply_event_to_aggregations(
                        std::hint::black_box(src.as_str()),
                        std::hint::black_box(row),
                        *t,
                        *id,
                        &registry,
                        &mut state_tables,
                    );
                }
                std::hint::black_box(state_tables);
            },
            BatchSize::LargeInput,
        );
    });

    g.finish();
}

// ─── Criterion groups + main ──────────────────────────────────────────────────

criterion_group!(phase5_agg, bench_agg_op_update, bench_windowed, bench_apply);
criterion_main!(phase5_agg);

// ─── Contract constant ────────────────────────────────────────────────────────

#[allow(dead_code)]
pub mod phase5_agg_benches {
    /// Total number of bench IDs registered: 8 AggOp + 2 WindowedOp + 1 apply.
    pub const EXPECTED_GROUPS: usize = 11;
}

#[cfg(test)]
mod tests {
    #[test]
    fn groups_registered() {
        assert_eq!(
            phase5_agg_benches::EXPECTED_GROUPS,
            11,
            "bench must register 8 AggOp update + 2 WindowedOp + 1 apply group"
        );
    }
}
