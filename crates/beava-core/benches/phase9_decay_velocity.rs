//! Phase 9 decay + velocity microbenches (plan 09-01 T7).
//!
//! Groups:
//!   agg_op_p9/{op_name}  — per-variant `AggOp::update` with one event (15 benches
//!     covering 16 operator variants — ewma also exercised implicitly when
//!     registering via `ema`). Each bench iterates update() with a fresh tick so
//!     stateful ops see a time-advancing stream.

use beava_core::agg_op::{AggKind, AggOp, AggOpDescriptor};
use beava_core::row::{Row, Value};
use criterion::{criterion_group, criterion_main, Criterion};

fn make_row() -> Row {
    Row::new()
        .with_field("user_id", Value::Str("u1".into()))
        .with_field("amount", Value::F64(100.0))
}

fn desc_with(
    kind: AggKind,
    field: Option<&str>,
    half_life: Option<u64>,
    sub_window: Option<u64>,
    sigma: Option<f64>,
) -> AggOpDescriptor {
    AggOpDescriptor {
        kind,
        field: field.map(String::from),
        window_ms: None,
        where_expr: None,
        half_life_ms: half_life,
        sub_window_ms: sub_window,
        sigma,
    }
}

fn bench_agg_op_update(c: &mut Criterion) {
    let row = make_row();

    type Variant = (
        AggKind,
        Option<&'static str>,
        Option<u64>,
        Option<u64>,
        Option<f64>,
    );
    let variants: &[Variant] = &[
        (AggKind::Ewma, Some("amount"), Some(60_000), None, None),
        (AggKind::EwVar, Some("amount"), Some(60_000), None, None),
        (AggKind::EwZScore, Some("amount"), Some(60_000), None, None),
        (
            AggKind::DecayedSum,
            Some("amount"),
            Some(60_000),
            None,
            None,
        ),
        (AggKind::DecayedCount, None, Some(60_000), None, None),
        (AggKind::Twa, Some("amount"), None, None, None),
        (AggKind::RateOfChange, Some("amount"), None, None, None),
        (AggKind::InterArrivalStats, None, None, None, None),
        (AggKind::BurstCount, None, None, Some(1_000), None),
        (AggKind::DeltaFromPrev, Some("amount"), None, None, None),
        (AggKind::Trend, Some("amount"), None, None, None),
        (AggKind::TrendResidual, Some("amount"), None, None, None),
        (AggKind::OutlierCount, Some("amount"), None, None, Some(3.0)),
        (AggKind::ValueChangeCount, Some("amount"), None, None, None),
        (AggKind::ZScore, Some("amount"), None, None, None),
    ];

    for &(kind, field, hl, sw, sig) in variants {
        let name = format!("agg_op_p9/{}", format!("{:?}", kind).to_lowercase());
        let desc = desc_with(kind, field, hl, sw, sig);
        let mut op = AggOp::new(&desc);
        let mut tick: i64 = 0;
        c.bench_function(&name, |b| {
            b.iter(|| {
                tick += 1;
                op.update(
                    std::hint::black_box(&row),
                    std::hint::black_box(tick),
                    field,
                    true,
                );
            });
        });
    }
}

criterion_group!(benches, bench_agg_op_update);
criterion_main!(benches);
