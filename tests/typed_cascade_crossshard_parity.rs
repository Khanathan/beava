//! Phase 59.7 Wave 0 (RED scaffolding, TPC-PERF-11 extension / TPC-CORR-07 extension) —
//! 4 RED cross-shard typed-cascade parity tests. Each test asserts that the
//! typed-cascade-direct walker (`run_typed_direct_cascade`, NOT YET EXISTING)
//! produces byte-identical cross-shard state mutations to the Value-cascade
//! path (`push_with_cascade_on_shard`).
//!
//! # Why all 4 are `#[ignore = "59.7-W3"]` today
//!
//! Wave 0 plants the test shape. Wave 3 (`59.7-03-PLAN`) ships the
//! `ShardOp::RunTypedAggCascadeStep` variant + cascade walker; at that
//! point each test body compiles and executes.
//!
//! # Compile-today pattern
//!
//! Bodies gated behind `#[cfg(any())]` so `cargo test --no-run` compiles
//! against today's crate surface (no `run_typed_direct_cascade`, no
//! `ShardOp::RunTypedAggCascadeStep`, no `BEAVA_TYPED_CASCADE_VALUE_FALLBACK`
//! counter read site).
//!
//! # Correctness contract (per-test)
//!
//! - `parity_same_shard_cascade_typed_vs_value_direct` — N=8, primary `Txns`
//!   + downstream `UserMetrics` sharing the same shard owner (entity_key
//!   chosen to collide). Assert: `run_typed_direct_cascade` and
//!   `push_with_cascade_on_shard` produce identical `entity_state` bytes.
//!
//! - `parity_cross_shard_cascade_typed_vs_value_direct` — N=8, primary `Txns`
//!   on shard J, downstream `UserMetrics` keyed by `Txn.user_id` hashing to
//!   shard K (J≠K). Asserts cross-shard dispatch produces identical target-
//!   shard state.
//!
//! - `parity_value_fallback_for_nontyped_downstream` — cascade: `Txns` →
//!   `Foo` (typed-compatible) → `Bar` (has an SSJ feature, not typed-
//!   compatible). Asserts the WHOLE cascade falls back to Value when
//!   `BEAVA_TYPED_CASCADE_DIRECT=1` (not just the `Bar` hop), AND that
//!   `Foo` entity_state is identical, AND that
//!   `BEAVA_TYPED_CASCADE_VALUE_FALLBACK.load(Ordering::Relaxed) > 0` at end.
//!
//! - `parity_v11_snapshot_roundtrip_with_windowed_typed` — seed 100 entities
//!   with windowed typed aggs (via W1 stubs), snapshot-save, re-load, assert
//!   `entity_ringbuffers_typed` round-trips byte-identical.

#![allow(unused_imports, dead_code)]

#[test]
#[ignore = "59.7-W3"]
fn parity_same_shard_cascade_typed_vs_value_direct() {
    #[cfg(any())]
    {
        use beava::engine::pipeline::PipelineEngine;
        unreachable!("59.7-W3 RED: run_typed_direct_cascade not yet implemented");
    }
}

#[test]
#[ignore = "59.7-W3"]
fn parity_cross_shard_cascade_typed_vs_value_direct() {
    #[cfg(any())]
    {
        use beava::engine::pipeline::PipelineEngine;
        unreachable!("59.7-W3 RED: ShardOp::RunTypedAggCascadeStep dispatch not yet implemented");
    }
}

#[test]
#[ignore = "59.7-W3"]
fn parity_value_fallback_for_nontyped_downstream() {
    #[cfg(any())]
    {
        use beava::engine::pipeline::PipelineEngine;
        // Wave 3 asserts: BEAVA_TYPED_CASCADE_VALUE_FALLBACK.load(Relaxed) > 0
        unreachable!("59.7-W3 RED: value-fallback counter + cascade detection not yet implemented");
    }
}

#[test]
#[ignore = "59.7-W3"]
fn parity_v11_snapshot_roundtrip_with_windowed_typed() {
    #[cfg(any())]
    {
        use beava::engine::pipeline::PipelineEngine;
        unreachable!("59.7-W3 RED: entity_ringbuffers_typed snapshot v11 extension not yet implemented");
    }
}
