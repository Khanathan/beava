//! Phase 59.7 Wave 0 (RED scaffolding, TPC-PERF-11 extension / TPC-CORR-07 extension) —
//! 10 RED windowed typed-agg parity tests. Each test asserts that the typed path
//! (via `crate::engine::operators_typed_aggs_windowed::*` — NOT YET EXISTING) produces
//! byte-identical `FeatureMap` output to the `serde_json::Value` windowed operator
//! path after replaying 100K events over a 30s event-time range with `window=5s`,
//! `bucket=1s`, at 20 checkpoints (every 1.5s).
//!
//! # Why all 10 are `#[ignore = "59.7-W1"]` today
//!
//! Wave 0 RED scaffolding plants the test shape; Wave 1 (`59.7-01-PLAN`) ships
//! `src/engine/operators_typed_aggs_windowed.rs` — the 10 windowed typed-agg
//! impls (CountOpTypedWindowed, SumOpTypedWindowedI64/F64, AvgOpTypedWindowedF64,
//! MinOpTypedWindowedI64/F64, MaxOpTypedWindowedI64/F64, LastOpTypedWindowedInlineStr,
//! FirstOpTypedWindowedInlineStr) + the `TypedRingBuffer` packed ring buffer. At
//! that point the cfg below flips off and each test body compiles + executes.
//!
//! # Compile-today pattern (RED scaffolding)
//!
//! Bodies are gated behind `#[cfg(any())]` (a never-true cfg predicate) so
//! `cargo test --no-run` compiles this file against today's crate surface
//! (which does NOT yet expose `operators_typed_aggs_windowed`). `--list`
//! reports 10 ignored tests. Wave 1's first task flips `#[cfg(any())]` off
//! and fills the bodies.
//!
//! This mirrors Phase 59.6 Wave 0 RED scaffolding for the unwindowed parity
//! gate (see `.planning/phases/59.6-typed-pipeline-records/59.6-00-SUMMARY.md`).
//!
//! # Windowed semantics contract
//!
//! window = 5s, bucket = 1s → 5 buckets per entity.
//! 100K events: event_time = start + (i as u64 * 300) µs (monotone), total range ~30s.
//! Checkpoints every 1.5s event-time; at each checkpoint, read both FeatureMaps,
//! `assert_eq!(typed, value)`. Generator seeded with `ChaCha8Rng::seed_from_u64(59_700 + idx)`
//! per test for determinism.

#![allow(unused_imports, dead_code)]

// Wave 1 will flip this cfg off to compile the test bodies against the new
// `operators_typed_aggs_windowed` module. Today, bodies are no-ops and the
// tests are `#[ignore = "59.7-W1"]` so the test runner never runs them.
//
// The `#[cfg(any())]` pattern keeps references to not-yet-existing types out
// of the compile graph while preserving the test names + signatures for
// `cargo test --list` gating.

#[test]
#[ignore = "59.7-W1"]
fn parity_count_typed_vs_value_windowed_5s_bucket_1s() {
    #[cfg(any())]
    {
        use beava::engine::operators_typed_aggs_windowed::CountOpTypedWindowed;
        unreachable!("59.7-W1 RED: CountOpTypedWindowed not yet implemented");
    }
}

#[test]
#[ignore = "59.7-W1"]
fn parity_sum_i64_typed_vs_value_windowed_5s_bucket_1s() {
    #[cfg(any())]
    {
        use beava::engine::operators_typed_aggs_windowed::SumOpTypedWindowedI64;
        unreachable!("59.7-W1 RED: SumOpTypedWindowedI64 not yet implemented");
    }
}

#[test]
#[ignore = "59.7-W1"]
fn parity_sum_f64_typed_vs_value_windowed_5s_bucket_1s() {
    #[cfg(any())]
    {
        use beava::engine::operators_typed_aggs_windowed::SumOpTypedWindowedF64;
        unreachable!("59.7-W1 RED: SumOpTypedWindowedF64 not yet implemented");
    }
}

#[test]
#[ignore = "59.7-W1"]
fn parity_avg_f64_typed_vs_value_windowed_5s_bucket_1s() {
    #[cfg(any())]
    {
        use beava::engine::operators_typed_aggs_windowed::AvgOpTypedWindowedF64;
        unreachable!("59.7-W1 RED: AvgOpTypedWindowedF64 not yet implemented");
    }
}

#[test]
#[ignore = "59.7-W1"]
fn parity_min_i64_typed_vs_value_windowed_5s_bucket_1s() {
    #[cfg(any())]
    {
        use beava::engine::operators_typed_aggs_windowed::MinOpTypedWindowedI64;
        unreachable!("59.7-W1 RED: MinOpTypedWindowedI64 not yet implemented");
    }
}

#[test]
#[ignore = "59.7-W1"]
fn parity_min_f64_typed_vs_value_windowed_5s_bucket_1s() {
    #[cfg(any())]
    {
        use beava::engine::operators_typed_aggs_windowed::MinOpTypedWindowedF64;
        unreachable!("59.7-W1 RED: MinOpTypedWindowedF64 not yet implemented");
    }
}

#[test]
#[ignore = "59.7-W1"]
fn parity_max_i64_typed_vs_value_windowed_5s_bucket_1s() {
    #[cfg(any())]
    {
        use beava::engine::operators_typed_aggs_windowed::MaxOpTypedWindowedI64;
        unreachable!("59.7-W1 RED: MaxOpTypedWindowedI64 not yet implemented");
    }
}

#[test]
#[ignore = "59.7-W1"]
fn parity_max_f64_typed_vs_value_windowed_5s_bucket_1s() {
    #[cfg(any())]
    {
        use beava::engine::operators_typed_aggs_windowed::MaxOpTypedWindowedF64;
        unreachable!("59.7-W1 RED: MaxOpTypedWindowedF64 not yet implemented");
    }
}

#[test]
#[ignore = "59.7-W1"]
fn parity_last_inline_str_typed_vs_value_windowed_5s_bucket_1s() {
    #[cfg(any())]
    {
        use beava::engine::operators_typed_aggs_windowed::LastOpTypedWindowedInlineStr;
        unreachable!("59.7-W1 RED: LastOpTypedWindowedInlineStr not yet implemented");
    }
}

#[test]
#[ignore = "59.7-W1"]
fn parity_first_inline_str_typed_vs_value_windowed_5s_bucket_1s() {
    #[cfg(any())]
    {
        use beava::engine::operators_typed_aggs_windowed::FirstOpTypedWindowedInlineStr;
        unreachable!("59.7-W1 RED: FirstOpTypedWindowedInlineStr not yet implemented");
    }
}
