// Phase 19.1-04 — WindowedOp lazy-bucket cold-init microbench.
//
// Per Phase 19.1 CONTEXT D-16/D-19/D-20 + CLAUDE.md §Performance Discipline.
// Measures the cost of constructing a fresh WindowedOp + the first update().
// Pre-fix, `WindowedOp::new` zero-initialises `[Option<Box<AggOp>>; 64]` +
// `[i64; 64]` (~512B + 512B per instance) — observed at ~1500 ns of the
// 2576 ns cold-key entity init cost on fraud-team.
// Post-fix, the `SmallVec<[(i64, Box<AggOp>); 4]>` layout is allocation-free
// at construction (SmallVec::new is a no-op).
//
// Groups:
//   windowed_op_init/new_count                  — WindowedOp::new(Count, 60s)
//   windowed_op_init/new_percentile             — WindowedOp::new(Percentile, 60s)
//   windowed_op_init/new_plus_first_update      — full cold-key path

use beava_core::agg_op::AggKind;
use beava_core::agg_windowed::WindowedOp;
use beava_core::row::Row;
use criterion::{black_box, criterion_group, criterion_main, Criterion};

fn bench_windowed_op_new_count(c: &mut Criterion) {
    c.bench_function("windowed_op_init/new_count_60s", |b| {
        b.iter(|| {
            let op = WindowedOp::new(black_box(AggKind::Count), black_box(60_000));
            black_box(op);
        });
    });
}

fn bench_windowed_op_new_percentile(c: &mut Criterion) {
    c.bench_function("windowed_op_init/new_percentile_60s", |b| {
        b.iter(|| {
            let op = WindowedOp::new(black_box(AggKind::Percentile), black_box(60_000));
            black_box(op);
        });
    });
}

fn bench_windowed_op_first_update(c: &mut Criterion) {
    let row = Row::default();
    c.bench_function("windowed_op_init/new_plus_first_update", |b| {
        b.iter(|| {
            let mut op = WindowedOp::new(black_box(AggKind::Count), black_box(60_000));
            op.update(black_box(&row), black_box(1_000), None, true);
            black_box(op);
        });
    });
}

criterion_group!(
    benches,
    bench_windowed_op_new_count,
    bench_windowed_op_new_percentile,
    bench_windowed_op_first_update,
);
criterion_main!(benches);
