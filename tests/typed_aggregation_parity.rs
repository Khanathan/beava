//! Phase 59.6 SC-4 — CountOp, LastOp, SumOp, AvgOp, MinOp, MaxOp, FirstOp
//! typed implementations produce identical output to the Value-path
//! operator siblings after 100K events.
//!
//! Wave 4 flips the simple-aggs subset (count + simple-aggs) from RED →
//! GREEN. The advanced-aggs test (distinct_count, percentile, ema, lag,
//! stddev, variance, topk, firstn, lastn) stays RED until Wave 6.
//!
//! Scope note (TPC-CORR-07 operator-boundary parity): these tests compare
//! the Wave-4 [`TypedAggOp`] output against its Value-path sibling
//! [`beava::engine::operators::Operator`] impl on the SAME 100K-event
//! stream. Both paths see every event in order; windowed Value-path ops
//! are configured with a large window (so no events expire) to match the
//! typed path's running-total semantics — the windowed + bucketed
//! semantics parity is covered by SC-5 (Wave 7's perf gate) when the full
//! typed pipeline can replay real event-time ordering.

#![allow(unused_imports, dead_code)]

use beava::engine::operators::{
    AvgOp, CountOp as CountOpValue, FirstOp as FirstOpValue, LastOp as LastOpValue,
    MaxOp as MaxOpValue, MinOp as MinOpValue, Operator, SumOp as SumOpValue,
};
use beava::engine::operators_typed::TypedAggOp;
use beava::engine::operators_typed_aggs::{
    AvgOpTypedF64, CountOpTyped, FirstOpTypedInlineStr, LastOpTypedInlineStr,
    MaxOpTypedF64, MinOpTypedF64, SumOpTypedF64,
};
use beava::engine::schema::{FieldSpec, FieldTy, RegisteredSchema, Row};
use beava::types::FeatureValue;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

const OPS_WAVE_4: &[&str] = &["count", "last", "first", "sum", "avg", "min", "max"];
const OPS_WAVE_6: &[&str] = &[
    "distinct_count", "percentile", "ema", "lag", "stddev", "variance",
    "topk", "firstn", "lastn",
];

const N_EVENTS: usize = 100_000;

/// Big-enough window so no events expire during the 100K run. Keeps the
/// windowed Value-path op semantically equivalent to the typed path's
/// flat running-total shape for this parity gate.
fn big_window() -> Duration {
    Duration::from_secs(86_400 * 365)
}
fn big_bucket() -> Duration {
    Duration::from_secs(3_600)
}

fn event_schema_num() -> Arc<RegisteredSchema> {
    let s = RegisteredSchema {
        schema_id: 0,
        name: "Txns".into(),
        fields: vec![
            FieldSpec {
                name: "user_id".into(),
                ty: FieldTy::InlineStr,
                offset: 0,
                nullable: false,
            },
            FieldSpec {
                name: "amount".into(),
                ty: FieldTy::F64,
                offset: 16,
                nullable: false,
            },
        ],
        inline_str_cap: 15,
        row_size: 24,
    };
    s.validate_layout().expect("valid");
    Arc::new(s)
}

fn state_schema_all() -> Arc<RegisteredSchema> {
    // Layout reserves one column per Wave-4 op:
    // [count@0 | sum@8 | avg_count@16 | avg_sum@24 | min@32 | min_seen@40
    //  | max@41 | max_seen@49]
    let s = RegisteredSchema {
        schema_id: 0,
        name: "AggState".into(),
        fields: vec![
            FieldSpec { name: "count".into(), ty: FieldTy::I64, offset: 0, nullable: false },
            FieldSpec { name: "sum".into(), ty: FieldTy::F64, offset: 8, nullable: false },
            FieldSpec { name: "avg_count".into(), ty: FieldTy::I64, offset: 16, nullable: false },
            FieldSpec { name: "avg_sum".into(), ty: FieldTy::F64, offset: 24, nullable: false },
            FieldSpec { name: "min".into(), ty: FieldTy::F64, offset: 32, nullable: false },
            FieldSpec { name: "min_seen".into(), ty: FieldTy::Bool, offset: 40, nullable: false },
            FieldSpec { name: "max".into(), ty: FieldTy::F64, offset: 41, nullable: false },
            FieldSpec { name: "max_seen".into(), ty: FieldTy::Bool, offset: 49, nullable: false },
        ],
        inline_str_cap: 15,
        row_size: 50,
    };
    s.validate_layout().expect("valid");
    Arc::new(s)
}

fn make_event_num(user: &str, amount: f64) -> (Row, serde_json::Value) {
    let sch = event_schema_num();
    let mut r = Row::zeroed(&sch);
    r.write_inline_str(0, sch.inline_str_cap, user);
    r.write_f64(16, amount);
    let v = serde_json::json!({ "user_id": user, "amount": amount });
    (r, v)
}

/// SC-4 count parity: typed CountOpTyped matches Value-path CountOp
/// after 100K events.
#[test]
fn typed_count_op_parity_100k_events() {
    let state_schema = state_schema_all();
    let event_schema = event_schema_num();

    // Typed
    let typed = CountOpTyped { name: "count".into(), output_offset: 0 };
    let mut state = Row::zeroed(&state_schema);
    typed.init_state(&state_schema, &mut state);

    // Value
    let mut value_op = CountOpValue::new(big_window(), big_bucket());
    let mut now = SystemTime::now();

    for i in 0..N_EVENTS {
        let (row, val) = make_event_num("u1", i as f64);
        typed.update_typed(&mut state, &state_schema, &row, &event_schema, now);
        value_op
            .push(&val, None, now)
            .expect("value push");
        now += Duration::from_millis(1);
    }

    let typed_out = typed.read_feature(&state, &state_schema);
    let value_out = value_op.read(now);
    assert_eq!(
        typed_out, value_out,
        "CountOp parity: typed={:?} value={:?}",
        typed_out, value_out
    );
    // Both paths must report 100K events.
    assert_eq!(typed_out, FeatureValue::Int(N_EVENTS as i64));
}

/// SC-4 simple-aggs parity: all 7 Wave-4 typed aggs match their Value
/// siblings after 100K events.
#[test]
fn typed_simple_aggs_parity_100k_events() {
    let _ops = OPS_WAVE_4;
    let state_schema = state_schema_all();
    let event_schema = event_schema_num();

    // Typed ops
    let count = CountOpTyped { name: "count".into(), output_offset: 0 };
    let sum = SumOpTypedF64 {
        name: "sum".into(),
        input_offset: 16,
        output_offset: 8,
    };
    let avg = AvgOpTypedF64 {
        name: "avg".into(),
        input_offset: 16,
        sum_offset: 24,
        count_offset: 16,
    };
    let min = MinOpTypedF64 {
        name: "min".into(),
        input_offset: 16,
        output_offset: 32,
        seen_offset: 40,
    };
    let max = MaxOpTypedF64 {
        name: "max".into(),
        input_offset: 16,
        output_offset: 41,
        seen_offset: 49,
    };

    // Last + First use inline string events. Build a separate state schema
    // to isolate them.
    // For this test we use `user_id` as the observed string (varies per event).
    let lf_event_schema = event_schema.clone();
    let lf_state_schema = {
        let s = RegisteredSchema {
            schema_id: 0,
            name: "AggStateLF".into(),
            fields: vec![
                FieldSpec { name: "last".into(), ty: FieldTy::InlineStr, offset: 0, nullable: false },
                FieldSpec { name: "last_time".into(), ty: FieldTy::I64, offset: 16, nullable: false },
                FieldSpec { name: "first".into(), ty: FieldTy::InlineStr, offset: 24, nullable: false },
                FieldSpec { name: "first_flag".into(), ty: FieldTy::Bool, offset: 40, nullable: false },
            ],
            inline_str_cap: 15,
            row_size: 41,
        };
        s.validate_layout().expect("valid");
        Arc::new(s)
    };
    let last = LastOpTypedInlineStr {
        name: "last".into(),
        input_offset: 0, // user_id
        output_offset: 0,
        time_offset: 16,
        input_inline_str_cap: 15,
        output_inline_str_cap: 15,
    };
    let first = FirstOpTypedInlineStr {
        name: "first".into(),
        input_offset: 0,
        output_offset: 24,
        flag_offset: 40,
        input_inline_str_cap: 15,
        output_inline_str_cap: 15,
    };

    let mut state = Row::zeroed(&state_schema);
    let mut lf_state = Row::zeroed(&lf_state_schema);
    for op_ref in [&count as &dyn TypedAggOp, &sum, &avg, &min, &max] {
        op_ref.init_state(&state_schema, &mut state);
    }
    last.init_state(&lf_state_schema, &mut lf_state);
    first.init_state(&lf_state_schema, &mut lf_state);

    // Value ops
    let mut v_count = CountOpValue::new(big_window(), big_bucket());
    let mut v_sum = SumOpValue::new("amount".to_string(), big_window(), big_bucket(), false);
    let mut v_avg = AvgOp::new("amount".to_string(), big_window(), big_bucket(), false);
    let mut v_min = MinOpValue::new("amount".to_string(), big_window(), big_bucket(), false);
    let mut v_max = MaxOpValue::new("amount".to_string(), big_window(), big_bucket(), false);
    let mut v_last = LastOpValue::new("user_id".to_string(), false);
    let mut v_first = FirstOpValue::new("user_id".to_string(), false);

    let mut now = SystemTime::now();
    // Deterministic event stream with varied amounts and user_ids so
    // min/max/last/first have non-trivial witnesses.
    for i in 0..N_EVENTS {
        let user = format!("u{}", i % 37);
        let amount = ((i * 31 + 7) % 1000) as f64 - 500.0; // spread neg + pos
        let (row, val) = make_event_num(&user, amount);

        // Typed
        count.update_typed(&mut state, &state_schema, &row, &event_schema, now);
        sum.update_typed(&mut state, &state_schema, &row, &event_schema, now);
        avg.update_typed(&mut state, &state_schema, &row, &event_schema, now);
        min.update_typed(&mut state, &state_schema, &row, &event_schema, now);
        max.update_typed(&mut state, &state_schema, &row, &event_schema, now);
        last.update_typed(&mut lf_state, &lf_state_schema, &row, &lf_event_schema, now);
        first.update_typed(&mut lf_state, &lf_state_schema, &row, &lf_event_schema, now);

        // Value
        v_count.push(&val, None, now).expect("count");
        v_sum.push(&val, None, now).expect("sum");
        v_avg.push(&val, None, now).expect("avg");
        v_min.push(&val, None, now).expect("min");
        v_max.push(&val, None, now).expect("max");
        v_last.push(&val, None, now).expect("last");
        v_first.push(&val, None, now).expect("first");

        now += Duration::from_millis(1);
    }

    // Count parity: typed = Int, value = Int.
    assert_eq!(
        count.read_feature(&state, &state_schema),
        v_count.read(now),
        "count op divergence"
    );

    // Sum parity: both are Float.
    match (
        sum.read_feature(&state, &state_schema),
        v_sum.read(now),
    ) {
        (FeatureValue::Float(t), FeatureValue::Float(v)) => {
            assert!((t - v).abs() < 1e-6, "sum divergence: typed={} value={}", t, v);
        }
        (t, v) => panic!("sum: typed={:?} value={:?}", t, v),
    }

    // Avg parity.
    match (
        avg.read_feature(&state, &state_schema),
        v_avg.read(now),
    ) {
        (FeatureValue::Float(t), FeatureValue::Float(v)) => {
            assert!((t - v).abs() < 1e-6, "avg divergence: typed={} value={}", t, v);
        }
        (t, v) => panic!("avg: typed={:?} value={:?}", t, v),
    }

    // Min / Max parity.
    match (
        min.read_feature(&state, &state_schema),
        v_min.read(now),
    ) {
        (FeatureValue::Float(t), FeatureValue::Float(v)) => {
            assert!((t - v).abs() < 1e-9, "min divergence: typed={} value={}", t, v);
        }
        (t, v) => panic!("min: typed={:?} value={:?}", t, v),
    }
    match (
        max.read_feature(&state, &state_schema),
        v_max.read(now),
    ) {
        (FeatureValue::Float(t), FeatureValue::Float(v)) => {
            assert!((t - v).abs() < 1e-9, "max divergence: typed={} value={}", t, v);
        }
        (t, v) => panic!("max: typed={:?} value={:?}", t, v),
    }

    // Last / First parity: both return String.
    assert_eq!(
        last.read_feature(&lf_state, &lf_state_schema),
        v_last.read(now),
        "last op divergence"
    );
    assert_eq!(
        first.read_feature(&lf_state, &lf_state_schema),
        v_first.read(now),
        "first op divergence"
    );
}

/// SC-4 advanced-aggs parity stub. Stays RED until Wave 6 lands typed
/// sketch/window ops.
#[test]
#[ignore = "59.6-W6"]
fn typed_advanced_aggs_parity_100k_events() {
    let _ops = OPS_WAVE_6;
    panic!("SC-4 RED: Wave 6 advanced aggs not yet implemented");
}
