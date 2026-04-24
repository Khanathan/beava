// Phase 10-04 TopKHeap microbench — confirms the Plan 22-04 O(log k)
// optimization: insert_or_bump should be ~300 ns at k=10 with ~80 candidates.

use beava_core::sketches::cms::{TopKHeap, TopKValue};
use criterion::{criterion_group, criterion_main, BatchSize, Criterion};

fn bench_topk_insert_steady_state(c: &mut Criterion) {
    let mut group = c.benchmark_group("phase10_04_topk");
    group.sample_size(50);

    // Pre-build a "warm" heap with the top-k slots already filled and a
    // realistic candidate distribution: 80 distinct values in rotation,
    // k = 10. This stresses the at-capacity path that Plan 22-04 targets.
    let k = 10usize;
    let n_distinct = 80usize;
    group.bench_function("insert_or_bump_at_capacity_k10_d80", |b| {
        b.iter_batched(
            || {
                let mut h = TopKHeap::new(k);
                for i in 0..n_distinct {
                    let count = ((i as u64) % 10) + 1;
                    h.insert_or_bump(TopKValue::Str(format!("v{}", i)), count);
                }
                h
            },
            |mut h| {
                // 100 inserts mixing existing-bumps and new-values.
                for i in 0..100u64 {
                    let v = TopKValue::Str(format!("v{}", i % n_distinct as u64));
                    h.insert_or_bump(v, i);
                }
                h
            },
            BatchSize::SmallInput,
        );
    });

    group.bench_function("insert_or_bump_below_capacity_k10", |b| {
        b.iter_batched(
            || TopKHeap::new(k),
            |mut h| {
                for i in 0..10u64 {
                    h.insert_or_bump(TopKValue::Int(i as i64), i + 1);
                }
                h
            },
            BatchSize::SmallInput,
        );
    });

    group.finish();
}

criterion_group!(phase10_04_topk, bench_topk_insert_steady_state);
criterion_main!(phase10_04_topk);
