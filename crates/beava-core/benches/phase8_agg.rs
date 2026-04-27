// Phase 8 aggregation hot-path bench (Plan 08-04).
//
// Groups:
//   agg_op_phase8/{first,last,first_n,last_n,lag,
//                  first_seen,last_seen,age,has_seen,time_since,
//                  time_since_last_n,streak,max_streak,negative_streak,
//                  first_seen_in_window}
//
// Each bench wires a windowless AggOp of that kind and measures the cost of
// one `update` call. Mirrors the `agg_op/*` group from `phase5_agg.rs` so
// the two families are directly comparable across hw-classes.

use beava_core::agg_op::{AggKind, AggOp, AggOpDescriptor};
use beava_core::row::{Row, Value};
use criterion::{criterion_group, criterion_main, Criterion};

fn make_row() -> Row {
    Row::new()
        .with_field("user_id", Value::Str("u1".into()))
        .with_field("amount", Value::F64(100.0))
        .with_field("status", Value::Str("ok".into()))
}

fn desc(
    kind: AggKind,
    field: Option<&str>,
    n: Option<u32>,
    window_ms: Option<u64>,
) -> AggOpDescriptor {
    AggOpDescriptor {
        kind,
        field: field.map(|s| s.to_string()),
        window_ms,
        where_expr: None,
        n,
        half_life_ms: None,
        sub_window_ms: None,
        sigma: None,
        sketch_params: None,
        ext: Default::default(),
        field_idx: beava_core::agg_op::FIELD_IDX_NONE,
    }
}

/// One bench variant. Field-tuple in struct form (vs. anonymous tuple) to
/// keep clippy's `type_complexity` lint quiet.
struct Variant {
    name: &'static str,
    kind: AggKind,
    field: Option<&'static str>,
    n: Option<u32>,
    window_ms: Option<u64>,
}

const fn v(
    name: &'static str,
    kind: AggKind,
    field: Option<&'static str>,
    n: Option<u32>,
    window_ms: Option<u64>,
) -> Variant {
    Variant {
        name,
        kind,
        field,
        n,
        window_ms,
    }
}

fn bench_phase8_ops(c: &mut Criterion) {
    let row = make_row();

    let variants: &[Variant] = &[
        v("first", AggKind::First, Some("amount"), None, None),
        v("last", AggKind::Last, Some("amount"), None, None),
        v("first_n", AggKind::FirstN, Some("amount"), Some(10), None),
        v("last_n", AggKind::LastN, Some("amount"), Some(10), None),
        v("lag", AggKind::Lag, Some("amount"), Some(3), None),
        v("first_seen", AggKind::FirstSeen, None, None, None),
        v("last_seen", AggKind::LastSeen, None, None, None),
        v("age", AggKind::Age, None, None, None),
        v("has_seen", AggKind::HasSeen, None, None, None),
        v("time_since", AggKind::TimeSince, None, None, None),
        v(
            "time_since_last_n",
            AggKind::TimeSinceLastN,
            None,
            Some(5),
            None,
        ),
        v("streak", AggKind::Streak, None, None, None),
        v("max_streak", AggKind::MaxStreak, None, None, None),
        v("negative_streak", AggKind::NegativeStreak, None, None, None),
        v(
            "first_seen_in_window",
            AggKind::FirstSeenInWindow,
            None,
            None,
            Some(300_000),
        ),
    ];

    for var in variants {
        let d = desc(var.kind, var.field, var.n, var.window_ms);
        let mut op = AggOp::new(&d);
        let bench_name = format!("agg_op_phase8/{}", var.name);
        let mut t = 0i64;
        let field = var.field;
        c.bench_function(&bench_name, |b| {
            b.iter(|| {
                op.update(
                    std::hint::black_box(&row),
                    std::hint::black_box(t),
                    field,
                    true,
                );
                t = t.wrapping_add(1);
            });
        });
    }
}

criterion_group!(benches, bench_phase8_ops);
criterion_main!(benches);
