//! Criterion micro-benches for CountMinSketch + TopKHeap (Plan 22-04 Step 3).
//!
//! Runs via `cargo bench --bench cms_ops`. The top_k.push bench is the
//! primary gate for Plan 22-04 Step 4 (O(log k) insert optimization).

use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use beava::engine::cms::{CountMinSketch, TopKHeap, TopKValue};

fn bench_cms(c: &mut Criterion) {
    let mut group = c.benchmark_group("cms");
    group.throughput(Throughput::Elements(1));

    // Raw update is the critical path for every CMS-backed top_k insert.
    group.bench_function("update_insert_rotating_2000", |b| {
        let mut cms = CountMinSketch::new(2048, 4);
        let mut i: i64 = 0;
        b.iter(|| {
            let h = TopKValue::Int(i % 2000).hash64();
            cms.insert(black_box(h));
            i += 1;
        });
    });

    // Warm to 11k then keep hammering a hot key.
    group.bench_function("estimate_after_11k_distinct", |b| {
        let mut cms = CountMinSketch::new(2048, 4);
        for i in 0..10_000 {
            cms.insert(TopKValue::Str(format!("v_{}", i)).hash64());
        }
        let hot = TopKValue::Str("hot".into()).hash64();
        for _ in 0..1_000 {
            cms.insert(hot);
        }
        b.iter(|| black_box(cms.estimate(black_box(hot))));
    });

    group.finish();
}

fn bench_topk(c: &mut Criterion) {
    let mut group = c.benchmark_group("topk");
    group.throughput(Throughput::Elements(1));

    // Primary Plan 22-04 gate: top_k.push in sketch mode with rotating uniques.
    // 22-03 baseline: ~1484 ns/op. 22-04 target: ≤ 300 ns/op (≥ 5× speedup).
    group.bench_function("observe_rotating_2000_cap_80", |b| {
        let mut cms = CountMinSketch::new(2048, 4);
        let mut heap = TopKHeap::new(10); // max_candidates = 80
        // Warm past exact threshold.
        for i in 0..1100i64 {
            let v = TopKValue::Int(i);
            cms.insert(v.hash64());
            heap.observe(&v, &cms);
        }
        let mut i: i64 = 1100;
        b.iter(|| {
            let v = TopKValue::Int(i % 2000);
            cms.insert(v.hash64());
            heap.observe(black_box(&v), black_box(&cms));
            i += 1;
        });
    });

    // Top-k read after steady-state: costs O(|candidates|) CMS estimate calls.
    group.bench_function("top_k_read_cap_80", |b| {
        let mut cms = CountMinSketch::new(2048, 4);
        let mut heap = TopKHeap::new(10);
        for i in 0..1100i64 {
            let v = TopKValue::Int(i);
            cms.insert(v.hash64());
            heap.observe(&v, &cms);
        }
        b.iter(|| black_box(heap.top_k(black_box(&cms))));
    });

    group.finish();
}

criterion_group!(benches, bench_cms, bench_topk);
criterion_main!(benches);
