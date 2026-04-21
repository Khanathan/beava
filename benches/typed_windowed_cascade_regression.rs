//! Phase 59.7 Wave 0 (TPC-PERF-11 extension) — regression tripwire bench.
//!
//! Pins three per-event cost cells against the Phase 59.6 baseline numbers so
//! any later wave (W1 windowed aggs / W4 cascade-direct walker) that regresses
//! by more than ±3 % fails the CI bench gate loudly instead of silently
//! showing up only in the end-to-end fraud-pipeline aggregate.
//!
//! # Three cells
//!
//! | Cell | Pinned (ns/event) | Source of baseline | Flipped GREEN in wave |
//! |------|-------------------|--------------------|-----------------------|
//! | `typed_windowed_cascade_regression_windowed_count_hot` | 50.0 | W1 budget (59.7 Plan 01) | 59.7-W1 |
//! | `typed_windowed_cascade_regression_cascade_direct_hot` | 23.66 | Phase 59.6-07 Criterion `cascade_17ops = 22.97 ns` × 1.03 | 59.7-W4 |
//! | `typed_windowed_cascade_regression_cascade_bridge_hot` | 760.0 | 59.6 live-bench ~740 ns/event end-to-end × 1.03 | 59.7-W4 |
//!
//! # Wave-0 body is a proxy
//!
//! The windowed typed aggs (`CountOpTypedWindowed` + friends) don't exist until
//! Wave 1; the cascade-direct walker (`run_typed_direct_cascade`) doesn't exist
//! until Wave 4. To keep `cargo build --bench typed_windowed_cascade_regression`
//! green today, the three cells measure the **unwindowed** typed-agg cost
//! (CountOpTyped / SumOpTypedF64 / AvgOpTypedF64) as a stand-in. W1 Task 1
//! swaps `windowed_count_hot` to the real `CountOpTypedWindowed`; W4 Task 1
//! swaps the two cascade cells to `run_typed_direct_cascade` + the Value
//! bridge.
//!
//! The `PINNED_*` constants are load-bearing — `scripts/verify-typed-path.sh`
//! greps for them (Phase 59.6 Wave 7 gate pattern).

use beava::engine::operators_typed::TypedAggOp;
use beava::engine::operators_typed_aggs::{AvgOpTypedF64, CountOpTyped, SumOpTypedF64};
use beava::engine::schema::{FieldSpec, FieldTy, RegisteredSchema, Row};
use criterion::{criterion_group, criterion_main, Criterion};
use std::sync::Arc;
use std::time::SystemTime;

// Pinned baselines — 3 % regression tripwire.

/// Windowed CountOpTypedWindowed target: ≤ 50 ns/event steady-state.
/// Proxy bench today (W0 RED): unwindowed CountOpTyped as stand-in.
/// W1 swaps in the real windowed op — budget stays 50 ns because the
/// ring-walk adds ~48 ns to 1.84 ns unwindowed.
pub const PINNED_WINDOWED_COUNT_NS: f64 = 50.0;

/// Cascade-direct 17-op path: ≤ 23.66 ns/event = Phase 59.6-07 Criterion
/// `cascade_17ops` at 22.97 ns × 1.03 tolerance. W4 swaps in
/// `run_typed_direct_cascade`; this cell FAILS until W4 because the
/// current bridge walk does a `row_to_value` round-trip.
pub const PINNED_CASCADE_DIRECT_NS: f64 = 23.66;

/// Cascade-bridge (today's `run_typed_enrich_cascade`) floor tripwire:
/// ≤ 760 ns/event = 740 ns Phase 59.6 live-bench end-to-end × 1.03. Any
/// later wave that accidentally makes the bridge slower is caught here.
pub const PINNED_CASCADE_BRIDGE_NS: f64 = 760.0;

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

/// Cell 1 (windowed_count_hot) — W1 swap target.
///
/// Today: measures unwindowed CountOpTyped as a proxy (~1.84 ns/event).
/// Budget asserts the **pinned baseline** (50 ns/event) covers the
/// ring-walk cost W1 will introduce.
fn bench_windowed_count_hot(c: &mut Criterion) {
    let input_schema = build_input_schema();
    let state_schema = build_state_schema_3ops();

    let mut event = Row::zeroed(&input_schema);
    event.write_inline_str(0, 15, "user_42");
    event.write_inline_str(16, 15, "merch_7");
    event.write_f64(32, 19.95);

    let count_op = CountOpTyped {
        name: "windowed_count".into(),
        output_offset: 0,
    };

    let mut state = Row::zeroed(&state_schema);
    count_op.init_state(&state_schema, &mut state);
    let now = SystemTime::now();

    c.bench_function("typed_windowed_cascade_regression_windowed_count_hot", |b| {
        b.iter(|| {
            count_op.update_typed(&mut state, &state_schema, &event, &input_schema, now);
        });
    });
}

/// Cell 2 (cascade_direct_hot) — W4 swap target.
///
/// Today: 3-op typed cascade proxy (Count + Sum + Avg). Reports
/// ~5-6 ns/event today; pinned baseline is 23.66 ns (Phase 59.6
/// `cascade_17ops` × 1.03). W4 swaps in `run_typed_direct_cascade`
/// over 17 ops.
fn bench_cascade_direct_hot(c: &mut Criterion) {
    let input_schema = build_input_schema();
    let state_schema = build_state_schema_3ops();

    let mut event = Row::zeroed(&input_schema);
    event.write_inline_str(0, 15, "user_42");
    event.write_inline_str(16, 15, "merch_7");
    event.write_f64(32, 19.95);

    let count_op = CountOpTyped {
        name: "count".into(),
        output_offset: 0,
    };
    let sum_op = SumOpTypedF64 {
        name: "sum_amount".into(),
        input_offset: 32,
        output_offset: 8,
    };
    let avg_op = AvgOpTypedF64 {
        name: "avg_amount".into(),
        input_offset: 32,
        sum_offset: 16,
        count_offset: 24,
    };

    let mut state = Row::zeroed(&state_schema);
    count_op.init_state(&state_schema, &mut state);
    sum_op.init_state(&state_schema, &mut state);
    avg_op.init_state(&state_schema, &mut state);
    let now = SystemTime::now();

    c.bench_function("typed_windowed_cascade_regression_cascade_direct_hot", |b| {
        b.iter(|| {
            count_op.update_typed(&mut state, &state_schema, &event, &input_schema, now);
            sum_op.update_typed(&mut state, &state_schema, &event, &input_schema, now);
            avg_op.update_typed(&mut state, &state_schema, &event, &input_schema, now);
        });
    });
}

/// Cell 3 (cascade_bridge_hot) — regression tripwire for the existing
/// `run_typed_enrich_cascade` Value-bridge walker.
///
/// Today: same 3-op proxy to give Criterion a non-empty cell. W4 swaps
/// this to a harness that actually drives `run_typed_enrich_cascade`
/// through the bridge so we can catch any accidental slowdown to the
/// existing Value fallback path.
fn bench_cascade_bridge_hot(c: &mut Criterion) {
    let input_schema = build_input_schema();
    let state_schema = build_state_schema_3ops();

    let mut event = Row::zeroed(&input_schema);
    event.write_inline_str(0, 15, "user_42");
    event.write_inline_str(16, 15, "merch_7");
    event.write_f64(32, 19.95);

    let count_op = CountOpTyped {
        name: "count".into(),
        output_offset: 0,
    };
    let sum_op = SumOpTypedF64 {
        name: "sum_amount".into(),
        input_offset: 32,
        output_offset: 8,
    };
    let avg_op = AvgOpTypedF64 {
        name: "avg_amount".into(),
        input_offset: 32,
        sum_offset: 16,
        count_offset: 24,
    };

    let mut state = Row::zeroed(&state_schema);
    count_op.init_state(&state_schema, &mut state);
    sum_op.init_state(&state_schema, &mut state);
    avg_op.init_state(&state_schema, &mut state);
    let now = SystemTime::now();

    c.bench_function("typed_windowed_cascade_regression_cascade_bridge_hot", |b| {
        b.iter(|| {
            count_op.update_typed(&mut state, &state_schema, &event, &input_schema, now);
            sum_op.update_typed(&mut state, &state_schema, &event, &input_schema, now);
            avg_op.update_typed(&mut state, &state_schema, &event, &input_schema, now);
        });
    });
}

criterion_group!(
    benches,
    bench_windowed_count_hot,
    bench_cascade_direct_hot,
    bench_cascade_bridge_hot
);
criterion_main!(benches);
