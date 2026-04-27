//! Phase 19 criterion microbench for crates/beava-bench/src/blast_shape.rs.
//!
//! Bench targets:
//!   1. build_pool/fixed/n_10000          — Pool=N for the simplest shape (one
//!      pre-encoded frame, refcount-cloned N times).
//!   2. build_pool/uniform/n_10000_k_1000 — Pool=N with K=1000 uniform sampling.
//!   3. build_pool/zipfian/n_10000_k_1000_alpha_1.0 — Pool=N with the
//!      production-default Zipfian (alpha=1.0, K=1000).
//!   4. build_pool/mixed/n_10000_m_3      — Pool=N with M=3 round-robin event
//!      names (multi-stream realism).
//!   5. sample_zipfian/k_1000_alpha_1.0   — single-sample throughput of the
//!      hand-rolled ZipfianSampler.
//!   6. sample_uniform/k_1000             — single-sample throughput baseline
//!      using `rand::Rng::gen_range`. Useful as a known-cheap reference point
//!      for the zipfian number above.
//!
//! Why N=10_000 (not N=1_000_000): criterion's default 10s warm + 100 sample
//! budget would burn ~30 minutes per bench at N=1M. N=10k gives a per-iter
//! cost in the µs-ms range that fits criterion's sample budget while still
//! amortising per-frame encode cost. Future regression detection: if Pool=N
//! builder slows down by 10%, the relative numbers at N=10k slow down by the
//! same ratio.
//!
//! Baseline rows append to .planning/perf-baselines.md under hw-class
//! apple-m4. Phase 19 sets the start-of-line; future bench changes regress
//! against these numbers per CLAUDE.md §Performance Discipline (+10% WARN /
//! +25% BLOCK).

use beava_bench::blast_shape::{
    build_pool, BlastShape, BlastShapeConfig, PipelineConfig, WireFormat, ZipfianSampler,
};
// WARNING 7 fix (revision 1): BenchmarkId intentionally NOT imported — none of
// the six bench functions use parameterized benches. Re-add it (and adopt
// parameterized benches) only if a future Phase 19.x sweep over multiple K
// values needs it.
use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};

/// Build the canonical small-pipeline shape used for every build_pool bench:
/// one entity (`user_id: str`) + one extra field (`amount: f64`).
fn make_pipeline_config() -> PipelineConfig {
    let mut extra_fields = serde_json::Map::new();
    extra_fields.insert(
        "amount".to_string(),
        serde_json::Value::String("f64".to_string()),
    );
    PipelineConfig {
        name: "small".to_string(),
        description: "criterion bench fixture".to_string(),
        register: serde_json::json!({"nodes": []}),
        event_name: "Txn".to_string(),
        features: vec!["cnt".to_string()],
        key_field: "user_id".to_string(),
        extra_fields,
    }
}

fn bench_build_pool_fixed(c: &mut Criterion) {
    let pipeline = make_pipeline_config();
    let mut group = c.benchmark_group("build_pool");
    group.throughput(Throughput::Elements(10_000));
    group.bench_function("fixed/n_10000", |b| {
        b.iter(|| {
            let cfg = BlastShapeConfig {
                pipeline: &pipeline,
                event_names_for_mixed: &[],
                wire_format: WireFormat::Json,
                seed: 42,
            };
            let pool = build_pool(BlastShape::Fixed, &cfg, 10_000).expect("build_pool fixed");
            black_box(pool);
        });
    });
    group.finish();
}

fn bench_build_pool_uniform(c: &mut Criterion) {
    let pipeline = make_pipeline_config();
    let mut group = c.benchmark_group("build_pool");
    group.throughput(Throughput::Elements(10_000));
    group.bench_function("uniform/n_10000_k_1000", |b| {
        b.iter(|| {
            let cfg = BlastShapeConfig {
                pipeline: &pipeline,
                event_names_for_mixed: &[],
                wire_format: WireFormat::Json,
                seed: 42,
            };
            let pool = build_pool(BlastShape::Uniform { cardinality: 1_000 }, &cfg, 10_000)
                .expect("build_pool uniform");
            black_box(pool);
        });
    });
    group.finish();
}

fn bench_build_pool_zipfian(c: &mut Criterion) {
    let pipeline = make_pipeline_config();
    let mut group = c.benchmark_group("build_pool");
    group.throughput(Throughput::Elements(10_000));
    group.bench_function("zipfian/n_10000_k_1000_alpha_1.0", |b| {
        b.iter(|| {
            let cfg = BlastShapeConfig {
                pipeline: &pipeline,
                event_names_for_mixed: &[],
                wire_format: WireFormat::Json,
                seed: 42,
            };
            let pool = build_pool(
                BlastShape::Zipfian {
                    alpha: 1.0,
                    cardinality: 1_000,
                },
                &cfg,
                10_000,
            )
            .expect("build_pool zipfian");
            black_box(pool);
        });
    });
    group.finish();
}

fn bench_build_pool_mixed(c: &mut Criterion) {
    let pipeline = make_pipeline_config();
    let event_names: Vec<&str> = vec!["A", "B", "C"];
    let mut group = c.benchmark_group("build_pool");
    group.throughput(Throughput::Elements(10_000));
    group.bench_function("mixed/n_10000_m_3", |b| {
        b.iter(|| {
            let cfg = BlastShapeConfig {
                pipeline: &pipeline,
                event_names_for_mixed: &event_names,
                wire_format: WireFormat::Json,
                seed: 42,
            };
            let pool = build_pool(BlastShape::Mixed { event_count: 3 }, &cfg, 10_000)
                .expect("build_pool mixed");
            black_box(pool);
        });
    });
    group.finish();
}

fn bench_sample_zipfian(c: &mut Criterion) {
    let mut group = c.benchmark_group("sampler");
    group.throughput(Throughput::Elements(1));
    group.bench_function("sample_zipfian/k_1000_alpha_1.0", |b| {
        let mut s = ZipfianSampler::new(1.0, 1_000, 42);
        b.iter(|| {
            let r = s.sample();
            black_box(r);
        });
    });
    group.finish();
}

fn bench_sample_uniform(c: &mut Criterion) {
    use rand::{Rng, SeedableRng};
    let mut group = c.benchmark_group("sampler");
    group.throughput(Throughput::Elements(1));
    group.bench_function("sample_uniform/k_1000", |b| {
        let mut rng: rand::rngs::StdRng = rand::rngs::StdRng::seed_from_u64(42);
        b.iter(|| {
            let r = rng.gen_range(0_u64..1_000);
            black_box(r);
        });
    });
    group.finish();
}

criterion_group!(
    benches,
    bench_build_pool_fixed,
    bench_build_pool_uniform,
    bench_build_pool_zipfian,
    bench_build_pool_mixed,
    bench_sample_zipfian,
    bench_sample_uniform,
);
criterion_main!(benches);
