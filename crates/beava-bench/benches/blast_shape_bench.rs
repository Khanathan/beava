//! Phase 19 criterion microbench for crates/beava-bench/src/blast_shape.rs.
//!
//! Stub commit (Plan 19-04 Task 1.a — red): proves the criterion harness wires
//! up against the new crate-level `[[bench]]` entry. Real measurements (six
//! benches: build_pool/{fixed,uniform,zipfian,mixed} + sample_zipfian +
//! sample_uniform) land in Task 1.b.
//!
//! Hot paths under bench: build_pool (all 4 shapes), sample_zipfian,
//! sample_uniform. Baseline rows append to .planning/perf-baselines.md under
//! hw-class apple-m4 (Phase 19 sets the start-of-line; future regressions
//! gate at +10% WARN / +25% BLOCK per CLAUDE.md §Performance Discipline).

use criterion::{black_box, criterion_group, criterion_main, Criterion};

fn bench_blast_shape_stub(c: &mut Criterion) {
    c.bench_function("blast_shape/stub", |b| {
        b.iter(|| black_box(0_u64));
    });
}

criterion_group!(benches, bench_blast_shape_stub);
criterion_main!(benches);
