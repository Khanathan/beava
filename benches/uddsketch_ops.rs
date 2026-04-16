//! Criterion micro-benches for UDDSketch core ops (Plan 22-04 Step 3).
//!
//! Ported from the old std::time::Instant harness in `tests/bench_hybrid_ops.rs`
//! (22-03 Decision C=1). Runs via `cargo bench --bench uddsketch_ops`.

use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use beava::engine::uddsketch::UDDSketch;

fn bench_insert(c: &mut Criterion) {
    let mut group = c.benchmark_group("uddsketch");
    group.throughput(Throughput::Elements(1));

    group.bench_function("insert_uniform_0_1000", |b| {
        let mut sk = UDDSketch::new(0.01, 2048);
        let mut i: f64 = 0.0;
        b.iter(|| {
            sk.insert(black_box(i));
            i = (i + 1.0) % 1000.0;
        });
    });

    // Pre-fill 10k, then measure quantile reads.
    let mut sk = UDDSketch::new(0.01, 2048);
    for i in 0..10_000 {
        sk.insert(i as f64);
    }
    group.bench_function("quantile_p95_after_10k_inserts", |b| {
        b.iter(|| black_box(sk.quantile(black_box(0.95))));
    });

    group.bench_function("quantile_p50_after_10k_inserts", |b| {
        b.iter(|| black_box(sk.quantile(black_box(0.50))));
    });

    // Decrement bench: a stable-state sketch undergoing retraction.
    group.bench_function("decrement_after_10k_fill", |b| {
        let mut sk = UDDSketch::new(0.01, 2048);
        for i in 0..10_000 {
            sk.insert(i as f64);
        }
        // Re-insert before decrement so the sketch stays non-empty.
        let mut i: f64 = 0.0;
        b.iter(|| {
            sk.insert(i);
            sk.decrement(black_box(i));
            i = (i + 1.0) % 10_000.0;
        });
    });

    group.finish();
}

criterion_group!(benches, bench_insert);
criterion_main!(benches);
