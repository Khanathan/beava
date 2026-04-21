/// Phase 55-01: `CascadeTarget` trait + `LiveCascadeTargets` impl for
/// cross-shard TT-cascade dispatch (see `src/engine/cascade_target.rs`).
pub mod cascade_target;
pub mod cms;
pub mod event_time;
pub mod expression;
pub mod hll;
pub mod join_validator;
pub mod operators;
/// Phase 59.6 Wave 3 (TPC-PERF-11): typed-row operator implementations.
/// See `src/engine/operators_typed.rs` for the `TypedOperator` trait +
/// `EnrichFromTableTyped` + `derive_enriched_schema`.
pub mod operators_typed;
/// Phase 59.6 Wave 4 (TPC-PERF-11): typed aggregation operator impls
/// (CountOp, SumOp, AvgOp, MinOp, MaxOp, LastOp, FirstOp) implementing
/// the `TypedAggOp` trait. See `src/engine/operators_typed_aggs.rs`.
pub mod operators_typed_aggs;
/// Phase 59.6 Wave 6 (TPC-PERF-11): typed advanced aggregation operator
/// impls — DistinctCountOpTyped (HLL), PercentileOpTyped (UDDSketch),
/// TopKOpTyped (CMS+heap), StddevOpTyped, VarianceOpTyped. Sketch ops
/// live in the per-entity SideBand via the D-C1 type-erasure tradeoff.
pub mod operators_typed_sketches;
/// Phase 59.6 Wave 6 (TPC-PERF-11): typed windowed / recurrence operator
/// impls — EmaOpTyped, LagOpTyped, FirstNOpTyped, LastNOpTyped.
pub mod operators_typed_windows;
pub mod pipeline;
pub mod recommend;
pub mod register;
pub mod retracting_ring;
/// Phase 59.6 Wave 1 (TPC-PERF-11): typed-row runtime schema.
/// See `src/engine/schema.rs` module doc for the full design.
pub mod schema;
pub mod uddsketch;
pub mod window;

// Phase 59.6 Wave 1 (TPC-PERF-11) — convenience re-exports for the typed
// row runtime. Consumers (`engine::register`, `PipelineEngine` accessors,
// and future Wave 2+ wire codec paths) import via `crate::engine::schema::*`
// but these aliases keep the public surface discoverable from `engine::`.
pub use schema::{FieldSpec, FieldTy, RegisteredSchema, Row, SchemaId, SchemaRegistry};

// Phase 59.6 Wave 3 (TPC-PERF-11) — typed operator re-exports.
pub use operators_typed::{
    derive_enriched_schema, EnrichFromTableTyped, ProjectedField, SideBand, TypedAggOp,
    TypedOperator,
};

// Phase 59.6 Wave 4 (TPC-PERF-11) — typed aggregation operator re-exports.
pub use operators_typed_aggs::{
    AvgOpTypedF64, CountOpTyped, FirstOpTypedInlineStr, LastOpTypedInlineStr,
    MaxOpTypedF64, MaxOpTypedI64, MinOpTypedF64, MinOpTypedI64, SumOpTypedF64,
    SumOpTypedI64,
};

// Phase 59.6 Wave 6 (TPC-PERF-11) — typed advanced agg re-exports.
pub use operators_typed_sketches::{
    DistinctCountOpTyped, NumCol, PercentileOpTyped, StddevOpTyped, TopKOpTyped,
    VarianceOpTyped,
};
pub use operators_typed_windows::{EmaOpTyped, FirstNOpTyped, LagOpTyped, LastNOpTyped};
