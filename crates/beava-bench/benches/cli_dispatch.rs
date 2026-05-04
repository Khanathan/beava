//! Phase 13.5 Plan 12 — `beava bench` CLI cold-path microbench.
//!
//! Measures the "before-the-real-work" overhead of:
//! 1. Workload load (`workloads::load_by_name`)
//! 2. Memory estimator (`estimator::estimate_memory`)
//! 3. clap argv parse (small synthetic schema mirroring the real CLI)
//!
//! Each completes in micro/nanoseconds; if any balloons (e.g. dynamic
//! doc-loading), this bench catches the regression.

use criterion::{black_box, criterion_group, criterion_main, Criterion};

use beava_bench::cli::estimator;
use beava_bench::workloads;

fn bench_workload_load(c: &mut Criterion) {
    c.bench_function("workload_load_fraud", |b| {
        b.iter(|| {
            let w = workloads::load_by_name(black_box("fraud")).unwrap();
            black_box(w.derivations.len());
        });
    });
    c.bench_function("workload_load_adtech", |b| {
        b.iter(|| {
            let w = workloads::load_by_name(black_box("adtech")).unwrap();
            black_box(w.derivations.len());
        });
    });
    c.bench_function("workload_load_small", |b| {
        b.iter(|| {
            let w = workloads::load_by_name(black_box("small")).unwrap();
            black_box(w.derivations.len());
        });
    });
}

fn bench_memory_estimator(c: &mut Criterion) {
    c.bench_function("estimator_fraud_medium", |b| {
        b.iter(|| {
            let est = estimator::estimate_memory(black_box("fraud"), black_box("medium")).unwrap();
            black_box(est.expected_rss_bytes);
        });
    });
    c.bench_function("estimator_adtech_small", |b| {
        b.iter(|| {
            let est = estimator::estimate_memory(black_box("adtech"), black_box("small")).unwrap();
            black_box(est.expected_rss_bytes);
        });
    });
}

fn bench_clap_parse(c: &mut Criterion) {
    let argv = vec![
        "beava-bench",
        "throughput",
        "--workload=fraud",
        "--size=medium",
        "--duration=60s",
        "--yes",
    ];
    c.bench_function("clap_parse_throughput_args", |b| {
        b.iter(|| {
            let cmd = clap::Command::new("beava-bench").subcommand(
                clap::Command::new("throughput")
                    .arg(clap::Arg::new("workload").long("workload"))
                    .arg(clap::Arg::new("size").long("size"))
                    .arg(clap::Arg::new("duration").long("duration"))
                    .arg(
                        clap::Arg::new("yes")
                            .long("yes")
                            .action(clap::ArgAction::SetTrue),
                    ),
            );
            black_box(cmd.get_matches_from(black_box(&argv)));
        });
    });
}

criterion_group!(
    benches,
    bench_workload_load,
    bench_memory_estimator,
    bench_clap_parse
);
criterion_main!(benches);
