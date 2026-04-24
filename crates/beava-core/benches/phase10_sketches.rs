//! Phase 10 sketch hot-path benches (CLAUDE.md §Performance Discipline gate).
//!
//! Groups:
//!   sketch_ops/{count_distinct,percentile,top_k,bloom,entropy}_*  — per-mode update + query
//!   windowed/{hll,uddsketch,cms,entropy}_1Mevt — 1M-event tight-loop
//!
//! Establishes the regression tripwire baseline for Phase 10 sketch ops.
//! No prior baselines exist for these benches; Phase 11+ compares against these.

use beava_core::sketches::{
    bloom::BloomFilter,
    cms::{CountMinSketch, TopKValue, DEFAULT_CMS_DEPTH, DEFAULT_CMS_WIDTH},
    count_distinct::CountDistinctState,
    entropy::EntropyHistogram,
    hll::Hll,
    percentile::PercentileState,
    top_k::TopKState,
    uddsketch::UDDSketch,
};
use criterion::{black_box, criterion_group, criterion_main, BatchSize, Criterion, Throughput};
use std::hash::{Hash, Hasher};

fn hash_str(s: &str) -> u64 {
    let mut h = ahash::AHasher::default();
    s.hash(&mut h);
    h.finish()
}

fn bench_count_distinct(c: &mut Criterion) {
    let mut g = c.benchmark_group("sketch_ops/count_distinct");
    g.throughput(Throughput::Elements(1));
    g.bench_function("exact_array_update", |b| {
        b.iter_batched(
            || CountDistinctState::new(1024),
            |mut s| {
                s.add_hash(black_box(hash_str("k0")));
                s
            },
            BatchSize::SmallInput,
        );
    });
    g.bench_function("hash_set_update", |b| {
        b.iter_batched(
            || {
                let mut s = CountDistinctState::new(1024);
                for i in 0..50 {
                    s.add_hash(hash_str(&format!("k{}", i)));
                }
                s
            },
            |mut s| {
                s.add_hash(black_box(hash_str("new_key")));
                s
            },
            BatchSize::SmallInput,
        );
    });
    g.bench_function("hll_update", |b| {
        b.iter_batched(
            || {
                let mut s = CountDistinctState::new(1024);
                for i in 0..2000 {
                    s.add_hash(hash_str(&format!("k{}", i)));
                }
                s
            },
            |mut s| {
                s.add_hash(black_box(hash_str("new_key")));
                s
            },
            BatchSize::SmallInput,
        );
    });
    g.bench_function("promote_array_to_set", |b| {
        b.iter_batched(
            || {
                let mut s = CountDistinctState::new(1024);
                for i in 0..16 {
                    s.add_hash(hash_str(&format!("k{}", i)));
                }
                s
            },
            |mut s| {
                s.add_hash(black_box(hash_str("trigger")));
                s
            },
            BatchSize::SmallInput,
        );
    });
    g.bench_function("promote_set_to_hll", |b| {
        b.iter_batched(
            || {
                let mut s = CountDistinctState::new(1024);
                for i in 0..1024 {
                    s.add_hash(hash_str(&format!("k{}", i)));
                }
                s
            },
            |mut s| {
                s.add_hash(black_box(hash_str("trigger")));
                s
            },
            BatchSize::SmallInput,
        );
    });
    g.finish();
}

fn bench_percentile(c: &mut Criterion) {
    let mut g = c.benchmark_group("sketch_ops/percentile");
    g.bench_function("exact_update", |b| {
        b.iter_batched(
            || PercentileState::new(256, 0.01),
            |mut s| {
                s.insert(black_box(42.0));
                s
            },
            BatchSize::SmallInput,
        );
    });
    g.bench_function("uddsketch_update", |b| {
        b.iter_batched(
            || {
                let mut s = PercentileState::new(256, 0.01);
                for i in 0..1000 {
                    s.insert(i as f64);
                }
                s
            },
            |mut s| {
                s.insert(black_box(42.0));
                s
            },
            BatchSize::SmallInput,
        );
    });
    g.bench_function("uddsketch_query_p99", |b| {
        let mut s = PercentileState::new(256, 0.01);
        for i in 0..10_000 {
            s.insert(i as f64);
        }
        b.iter(|| black_box(s.quantile(0.99)));
    });
    g.finish();
}

fn bench_top_k(c: &mut Criterion) {
    let mut g = c.benchmark_group("sketch_ops/top_k");
    g.bench_function("exact_update", |b| {
        b.iter_batched(
            || TopKState::new(10, 1024, DEFAULT_CMS_WIDTH, DEFAULT_CMS_DEPTH),
            |mut s| {
                s.insert(black_box(TopKValue::Str("k0".into())));
                s
            },
            BatchSize::SmallInput,
        );
    });
    g.bench_function("hybrid_update", |b| {
        b.iter_batched(
            || {
                let mut s = TopKState::new(10, 100, DEFAULT_CMS_WIDTH, DEFAULT_CMS_DEPTH);
                for i in 0..200 {
                    s.insert(TopKValue::Str(format!("k{}", i)));
                }
                s
            },
            |mut s| {
                s.insert(black_box(TopKValue::Str("k_new".into())));
                s
            },
            BatchSize::SmallInput,
        );
    });
    g.bench_function("hybrid_query_top10", |b| {
        let mut s = TopKState::new(10, 100, DEFAULT_CMS_WIDTH, DEFAULT_CMS_DEPTH);
        for i in 0..1000 {
            s.insert(TopKValue::Str(format!("k{}", i)));
        }
        b.iter(|| black_box(s.top()));
    });
    g.finish();
}

fn bench_bloom(c: &mut Criterion) {
    let mut g = c.benchmark_group("sketch_ops/bloom");
    g.bench_function("update_1k_capacity", |b| {
        b.iter_batched(
            || BloomFilter::with_capacity_and_fpr(1024, 0.01),
            |mut s| {
                s.insert(black_box("k0"));
                s
            },
            BatchSize::SmallInput,
        );
    });
    let bf = {
        let mut bf = BloomFilter::with_capacity_and_fpr(1024, 0.01);
        for i in 0..1024 {
            bf.insert(&format!("k{}", i));
        }
        bf
    };
    g.bench_function("query_member_1k", |b| {
        b.iter(|| black_box(bf.contains("k500")));
    });
    g.finish();
}

fn bench_entropy(c: &mut Criterion) {
    let mut g = c.benchmark_group("sketch_ops/entropy");
    g.bench_function("update_100cat", |b| {
        b.iter_batched(
            || {
                let mut h = EntropyHistogram::new(1024);
                for i in 0..100 {
                    h.insert(&format!("c{}", i));
                }
                h
            },
            |mut h| {
                h.insert(black_box("c_new"));
                h
            },
            BatchSize::SmallInput,
        );
    });
    g.bench_function("query_bits_100cat", |b| {
        let mut h = EntropyHistogram::new(1024);
        for i in 0..100 {
            for _ in 0..10 {
                h.insert(&format!("c{}", i));
            }
        }
        b.iter(|| black_box(h.entropy_bits()));
    });
    g.finish();
}

fn bench_windowed_sketches(c: &mut Criterion) {
    let mut g = c.benchmark_group("windowed");
    g.throughput(Throughput::Elements(1_000_000));
    g.bench_function("hll_1Mevt", |b| {
        b.iter(|| {
            let mut h = Hll::new();
            for i in 0..1_000_000_u64 {
                h.add_hash(black_box(i));
            }
            black_box(h.estimate())
        });
    });
    g.bench_function("uddsketch_1Mevt", |b| {
        b.iter(|| {
            let mut s = UDDSketch::default();
            for i in 0..1_000_000_u64 {
                s.insert(black_box(i as f64 + 1.0));
            }
            black_box(s.quantile(0.99))
        });
    });
    g.bench_function("cms_1Mevt", |b| {
        b.iter(|| {
            let mut c = CountMinSketch::new(DEFAULT_CMS_WIDTH, DEFAULT_CMS_DEPTH);
            for i in 0..1_000_000_u64 {
                c.insert(black_box(i));
            }
            black_box(c.total())
        });
    });
    g.bench_function("entropy_1Mevt", |b| {
        b.iter(|| {
            let mut h = EntropyHistogram::new(1024);
            for i in 0..1_000_000_u32 {
                h.insert(black_box(&format!("c{}", i % 100)));
            }
            black_box(h.entropy_bits())
        });
    });
    g.finish();
}

criterion_group!(
    benches,
    bench_count_distinct,
    bench_percentile,
    bench_top_k,
    bench_bloom,
    bench_entropy,
    bench_windowed_sketches,
);
criterion_main!(benches);
