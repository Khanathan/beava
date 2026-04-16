//! Criterion micro-benches for Hll + DistinctCountOp (Plan 22-04 Step 3).
//!
//! Runs via `cargo bench --bench hll_ops`.

use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use serde_json::json;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use beava::engine::hll::{DistinctCountOp, Hll};
use beava::engine::operators::Operator;

fn ts(secs: u64) -> SystemTime {
    UNIX_EPOCH + Duration::from_secs(secs)
}

fn bench_hll_raw(c: &mut Criterion) {
    let mut group = c.benchmark_group("hll");
    group.throughput(Throughput::Elements(1));

    // Exact / HashSet / HLL transition: insert pattern with rotating strings.
    group.bench_function("insert_rotating_2000_strings", |b| {
        let mut hll = Hll::new();
        // Warm past the HLL promotion threshold (1024 uniques).
        for i in 0..1500 {
            hll.insert(&format!("w_{}", i));
        }
        let mut i: u64 = 1500;
        b.iter(|| {
            let s = format!("u_{}", i % 2000);
            hll.insert(black_box(&s));
            i += 1;
        });
    });

    group.bench_function("count_after_20k_distinct", |b| {
        let mut hll = Hll::new();
        for i in 0..20_000 {
            hll.insert(&format!("v_{}", i));
        }
        b.iter(|| black_box(hll.count()));
    });

    group.finish();
}

fn bench_distinct_count_op(c: &mut Criterion) {
    let mut group = c.benchmark_group("distinct_count_op");
    group.throughput(Throughput::Elements(1));

    group.bench_function("push_hll_mode_rotating_2000", |b| {
        let mut op = DistinctCountOp::new(
            "d",
            Duration::from_secs(3600),
            Duration::from_secs(60),
            false,
        );
        let t = ts(1_000_000);
        // Warm past the HLL threshold.
        for i in 0..2000 {
            op.push(&json!({ "d": format!("w{}", i) }), None, t).unwrap();
        }
        let mut i: u64 = 2000;
        b.iter(|| {
            let ev = json!({ "d": format!("u{}", i % 4000) });
            op.push(black_box(&ev), None, t).unwrap();
            i += 1;
        });
    });

    group.finish();
}

criterion_group!(benches, bench_hll_raw, bench_distinct_count_op);
criterion_main!(benches);
