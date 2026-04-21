//! Phase 59.6 Wave 0 — per-event pipeline-phase latency bench (TPC-PERF-11).
//!
//! Wave 0 baseline assertion: pipeline phase latency is at the current
//! 59.5-W3.5 measurement (~8.5μs avg). Wave 7 revises the threshold to
//! ≤ 2.0μs (the TPC-PERF-11 gate).
//!
//! Harness (Wave 7): drives `PipelineEngine::push_with_cascade_on_shard` on
//! a single shard thread with a registered typed stream + 10K-event burst;
//! reads the `beava_shard_push_phase_seconds{phase="pipeline"}` histogram
//! avg over the burst. Runs for 60 seconds OR 10M events, whichever first.
//!
//! Wave 0 body: zero-work noop loop so `cargo bench --no-run` passes the
//! gate without needing `RegisteredSchema` infra (lands in Wave 1).
//!
//! Gate shape (Wave 7):
//! ```text
//! assert!(avg_pipeline_phase <= Duration::from_nanos(2_000),
//!         "TPC-PERF-11 gate: pipeline phase latency {:.2}μs > 2.0μs target",
//!         avg_pipeline_phase.as_secs_f64() * 1e6);
//! ```

use criterion::{criterion_group, criterion_main, Criterion};

fn bench_pipeline_phase_latency(c: &mut Criterion) {
    c.bench_function("typed_pipeline_phase_latency_burst_10k", |b| {
        b.iter(|| {
            // Wave 0 stub — zero-work iter so `cargo bench --no-run` gates
            // pass before `RegisteredSchema` + typed-row infra land. Wave 7
            // replaces with a real sustained-throughput harness that drives
            // `process_shard_event` through a typed stream and asserts the
            // `beava_shard_push_phase_seconds{phase="pipeline"}` histogram
            // avg ≤ 2.0μs (TPC-PERF-11 gate).
            let x = std::hint::black_box(42u64);
            std::hint::black_box(x.wrapping_add(1));
        });
    });
}

criterion_group!(benches, bench_pipeline_phase_latency);
criterion_main!(benches);
