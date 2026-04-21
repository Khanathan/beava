//! Phase 59.6 Wave 7 — per-event pipeline-phase latency bench (TPC-PERF-11).
//!
//! Replaces the Wave 0 zero-work stub with a real harness that measures
//! the per-event cost of the typed-row hot path:
//!
//! 1. Build a typed input schema (`Txns { user_id: InlineStr, merchant_id:
//!    InlineStr, amount: f64 }`) — mirrors the fraud-pipeline workload
//!    `Transactions` stream (see `benchmark/fraud-pipeline/bench.py`).
//! 2. Build an agg-state schema holding Count + Sum + Avg state for a
//!    single `(stream, entity_key)` slot.
//! 3. Instantiate `CountOpTyped` + `SumOpTypedF64` + `AvgOpTypedF64` —
//!    three Wave-4 typed aggs whose hot path is `read_f64 + add + write_f64`
//!    (no allocations, no enum dispatch, no `serde_json::Value`).
//! 4. Criterion loops:
//!    - `single_event`: one event through the typed cascade — `update_typed`
//!      × N ops + `read_feature` × N ops. This is the per-event "pipeline
//!      phase" cost that `beava_shard_push_phase_seconds{phase="pipeline"}`
//!      measures in production.
//!    - `cascade_17ops`: same event through a 17-op cascade (Count × 1 +
//!      Sum × 8 + Avg × 8) to simulate the fraud-pipeline's 47-feature
//!      density. Target is < 3μs/event (vs 8.5μs Value-path baseline).
//!
//! # TPC-PERF-11 gate reference
//!
//! The Criterion measurements here, combined with the 60s fraud-pipeline
//! throughput measurement (see `benchmark/fraud-pipeline/run_bench.sh`),
//! are the two evidence sources for Wave 7's `59.6-PERF-GATE.md`.
//!
//! # What this bench does NOT measure
//!
//! - Shard-thread lock acquisition cost (~0.42μs per Phase 59.5-W3.5
//!   histogram). That path goes through `push_typed_on_shard` → SPSC
//!   inbox → shard worker — not reachable from a Criterion bench without
//!   spawning the whole runtime. The `beava_shard_push_phase_seconds`
//!   histogram under the live 60s fraud-pipeline bench is the
//!   authoritative signal.
//! - Wire decode cost (~0.15μs post-59.6 per CONTEXT.md). Covered by
//!   `benches/pareto_workload.rs` and the `decode_typed_row_push_batch`
//!   unit test.
//! - SideBand (HLL / UDDSketch / CMS) overhead — advanced aggs. Sketch
//!   ops are Wave 6 additions; this bench focuses on the simple-agg hot
//!   path which is the dominant 80 % of the fraud-pipeline workload.

use beava::engine::operators_typed::TypedAggOp;
use beava::engine::operators_typed_aggs::{AvgOpTypedF64, CountOpTyped, SumOpTypedF64};
use beava::engine::schema::{FieldSpec, FieldTy, RegisteredSchema, Row};
use criterion::{criterion_group, criterion_main, BatchSize, Criterion};
use std::sync::Arc;
use std::time::SystemTime;

/// Build the typed input schema mirroring `Transactions` from
/// `benchmark/fraud-pipeline/bench.py`: user_id, merchant_id, amount.
fn build_input_schema() -> Arc<RegisteredSchema> {
    Arc::new(RegisteredSchema {
        schema_id: 1,
        name: "Txns".into(),
        fields: vec![
            FieldSpec {
                name: "user_id".into(),
                ty: FieldTy::InlineStr,
                offset: 0,
                nullable: false,
            },
            FieldSpec {
                name: "merchant_id".into(),
                ty: FieldTy::InlineStr,
                offset: 16,
                nullable: false,
            },
            FieldSpec {
                name: "amount".into(),
                ty: FieldTy::F64,
                offset: 32,
                nullable: false,
            },
        ],
        inline_str_cap: 15,
        row_size: 40,
    })
}

/// State schema for Count(1) + Sum(amount) + Avg(amount) — three
/// features stored as i64 + f64 + (f64, i64) = 32 bytes contiguous.
fn build_state_schema_3ops() -> Arc<RegisteredSchema> {
    Arc::new(RegisteredSchema {
        schema_id: 100,
        name: "UserStats3".into(),
        fields: vec![
            FieldSpec {
                name: "count".into(),
                ty: FieldTy::I64,
                offset: 0,
                nullable: false,
            },
            FieldSpec {
                name: "sum_amount".into(),
                ty: FieldTy::F64,
                offset: 8,
                nullable: false,
            },
            FieldSpec {
                name: "avg_sum".into(),
                ty: FieldTy::F64,
                offset: 16,
                nullable: false,
            },
            FieldSpec {
                name: "avg_count".into(),
                ty: FieldTy::I64,
                offset: 24,
                nullable: false,
            },
        ],
        inline_str_cap: 15,
        row_size: 32,
    })
}

/// State schema for a 17-feature cascade: 1 × Count, 8 × Sum, 8 × Avg.
/// Row = 8 (count) + 8*8 (sums) + 8*(8+8) (avgs) = 200 bytes.
fn build_state_schema_17ops() -> Arc<RegisteredSchema> {
    let mut fields = vec![FieldSpec {
        name: "count".into(),
        ty: FieldTy::I64,
        offset: 0,
        nullable: false,
    }];
    let mut off: u16 = 8;
    for i in 0..8 {
        fields.push(FieldSpec {
            name: format!("sum_{}", i),
            ty: FieldTy::F64,
            offset: off,
            nullable: false,
        });
        off += 8;
    }
    for i in 0..8 {
        fields.push(FieldSpec {
            name: format!("avg_sum_{}", i),
            ty: FieldTy::F64,
            offset: off,
            nullable: false,
        });
        off += 8;
        fields.push(FieldSpec {
            name: format!("avg_count_{}", i),
            ty: FieldTy::I64,
            offset: off,
            nullable: false,
        });
        off += 8;
    }
    Arc::new(RegisteredSchema {
        schema_id: 101,
        name: "UserStats17".into(),
        fields,
        inline_str_cap: 15,
        row_size: off,
    })
}

fn bench_single_event_typed_agg(c: &mut Criterion) {
    let input_schema = build_input_schema();
    let state_schema = build_state_schema_3ops();

    // Build one event Row.
    let mut event = Row::zeroed(&input_schema);
    event.write_inline_str(0, 15, "user_42");
    event.write_inline_str(16, 15, "merch_7");
    event.write_f64(32, 19.95);

    let count_op = CountOpTyped {
        name: "tx_count".into(),
        output_offset: 0,
    };
    let sum_op = SumOpTypedF64 {
        name: "tx_sum".into(),
        input_offset: 32,
        output_offset: 8,
    };
    let avg_op = AvgOpTypedF64 {
        name: "tx_avg".into(),
        input_offset: 32,
        sum_offset: 16,
        count_offset: 24,
    };

    // Bench measures: init fresh state + update × 3 ops + read × 3 ops.
    // Criterion reports ns/iter which maps directly to per-event cost.
    c.bench_function("typed_pipeline_phase_single_event_3ops", |b| {
        let now = SystemTime::now();
        b.iter_batched(
            || Row::zeroed(&state_schema),
            |mut state| {
                // Init state (simple-agg init is zero-write × N).
                count_op.init_state(&state_schema, &mut state);
                sum_op.init_state(&state_schema, &mut state);
                avg_op.init_state(&state_schema, &mut state);
                // Update (hot path).
                count_op.update_typed(&mut state, &state_schema, &event, &input_schema, now);
                sum_op.update_typed(&mut state, &state_schema, &event, &input_schema, now);
                avg_op.update_typed(&mut state, &state_schema, &event, &input_schema, now);
                // Read feature (projection to FeatureValue).
                let _ = count_op.read_feature(&state, &state_schema);
                let _ = sum_op.read_feature(&state, &state_schema);
                let _ = avg_op.read_feature(&state, &state_schema);
                state
            },
            BatchSize::SmallInput,
        );
    });
}

fn bench_update_only_3ops(c: &mut Criterion) {
    let input_schema = build_input_schema();
    let state_schema = build_state_schema_3ops();

    let mut event = Row::zeroed(&input_schema);
    event.write_inline_str(0, 15, "user_42");
    event.write_inline_str(16, 15, "merch_7");
    event.write_f64(32, 19.95);

    let count_op = CountOpTyped {
        name: "tx_count".into(),
        output_offset: 0,
    };
    let sum_op = SumOpTypedF64 {
        name: "tx_sum".into(),
        input_offset: 32,
        output_offset: 8,
    };
    let avg_op = AvgOpTypedF64 {
        name: "tx_avg".into(),
        input_offset: 32,
        sum_offset: 16,
        count_offset: 24,
    };

    // Steady-state update-only cost: state already allocated, entity is
    // warm in the HashMap. This is the 10_001st event through the same
    // `(stream, entity_key)` — the dominant per-event cost in the live
    // workload after the first few seconds.
    let mut state = Row::zeroed(&state_schema);
    count_op.init_state(&state_schema, &mut state);
    sum_op.init_state(&state_schema, &mut state);
    avg_op.init_state(&state_schema, &mut state);
    let now = SystemTime::now();
    c.bench_function("typed_pipeline_phase_update_only_3ops", |b| {
        b.iter(|| {
            count_op.update_typed(&mut state, &state_schema, &event, &input_schema, now);
            sum_op.update_typed(&mut state, &state_schema, &event, &input_schema, now);
            avg_op.update_typed(&mut state, &state_schema, &event, &input_schema, now);
        });
    });
}

fn bench_cascade_17ops(c: &mut Criterion) {
    let input_schema = build_input_schema();
    let state_schema = build_state_schema_17ops();

    let mut event = Row::zeroed(&input_schema);
    event.write_inline_str(0, 15, "user_42");
    event.write_inline_str(16, 15, "merch_7");
    event.write_f64(32, 19.95);

    // Build 17 ops: 1 Count + 8 Sum + 8 Avg. Offsets match
    // `build_state_schema_17ops` layout.
    let mut ops: Vec<Box<dyn TypedAggOp>> = Vec::with_capacity(17);
    ops.push(Box::new(CountOpTyped {
        name: "count".into(),
        output_offset: 0,
    }));
    for i in 0..8 {
        ops.push(Box::new(SumOpTypedF64 {
            name: format!("sum_{}", i),
            input_offset: 32,
            output_offset: 8 + i * 8,
        }));
    }
    let avg_base: u16 = 8 + 8 * 8;
    for i in 0..8 {
        ops.push(Box::new(AvgOpTypedF64 {
            name: format!("avg_{}", i),
            input_offset: 32,
            sum_offset: avg_base + i * 16,
            count_offset: avg_base + i * 16 + 8,
        }));
    }

    // Pre-initialize state so we measure steady-state update cost for the
    // 17-op cascade (matches the fraud-pipeline's 47-feature steady state
    // after the first events).
    let mut state = Row::zeroed(&state_schema);
    for op in &ops {
        op.init_state(&state_schema, &mut state);
    }
    let now = SystemTime::now();

    c.bench_function("typed_pipeline_phase_cascade_17ops", |b| {
        b.iter(|| {
            for op in &ops {
                op.update_typed(&mut state, &state_schema, &event, &input_schema, now);
            }
        });
    });
}

criterion_group!(
    benches,
    bench_single_event_typed_agg,
    bench_update_only_3ops,
    bench_cascade_17ops
);
criterion_main!(benches);
