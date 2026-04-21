//! Phase 59.6 Wave 4 — integration mirror of
//! `src/engine/operators_typed_aggs.rs::tests`.
//!
//! Lib-test execution is currently blocked by the pre-existing Phase 60
//! salt sweep (33 `StreamDefinition { .. }` sites missing `salt: None`)
//! documented as deferred on Waves 0 / 2 / 3. This integration binary
//! exercises the same 7 typed-agg operators end-to-end so Wave 4's
//! acceptance criteria ("7+ unit tests GREEN for the typed aggs") is
//! satisfiable without first resolving the salt sweep.

use beava::engine::operators_typed::TypedAggOp;
use beava::engine::operators_typed_aggs::{
    AvgOpTypedF64, CountOpTyped, FirstOpTypedInlineStr, LastOpTypedInlineStr, MaxOpTypedF64,
    MinOpTypedI64, SumOpTypedF64, SumOpTypedI64,
};
use beava::engine::schema::{FieldSpec, FieldTy, RegisteredSchema, Row};
use beava::types::FeatureValue;
use std::sync::Arc;
use std::time::SystemTime;

fn build_event_schema_num() -> Arc<RegisteredSchema> {
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
            FieldSpec {
                name: "qty".into(),
                ty: FieldTy::I64,
                offset: 24,
                nullable: false,
            },
        ],
        inline_str_cap: 15,
        row_size: 32,
    };
    s.validate_layout().expect("valid");
    Arc::new(s)
}

fn build_event_schema_str() -> Arc<RegisteredSchema> {
    let s = RegisteredSchema {
        schema_id: 0,
        name: "Events".into(),
        fields: vec![FieldSpec {
            name: "label".into(),
            ty: FieldTy::InlineStr,
            offset: 0,
            nullable: false,
        }],
        inline_str_cap: 15,
        row_size: 16,
    };
    s.validate_layout().expect("valid");
    Arc::new(s)
}

fn state_schema_scalars() -> Arc<RegisteredSchema> {
    let s = RegisteredSchema {
        schema_id: 0,
        name: "AggState".into(),
        fields: vec![
            FieldSpec { name: "count".into(), ty: FieldTy::I64, offset: 0, nullable: false },
            FieldSpec { name: "sum_f64".into(), ty: FieldTy::F64, offset: 8, nullable: false },
            FieldSpec { name: "avg_count".into(), ty: FieldTy::I64, offset: 16, nullable: false },
            FieldSpec { name: "avg_sum".into(), ty: FieldTy::F64, offset: 24, nullable: false },
            FieldSpec { name: "min_f64".into(), ty: FieldTy::F64, offset: 32, nullable: false },
            FieldSpec { name: "min_f64_seen".into(), ty: FieldTy::Bool, offset: 40, nullable: false },
            FieldSpec { name: "max_f64".into(), ty: FieldTy::F64, offset: 41, nullable: false },
            FieldSpec { name: "max_f64_seen".into(), ty: FieldTy::Bool, offset: 49, nullable: false },
            FieldSpec { name: "min_i64".into(), ty: FieldTy::I64, offset: 50, nullable: false },
            FieldSpec { name: "min_i64_seen".into(), ty: FieldTy::Bool, offset: 58, nullable: false },
            FieldSpec { name: "max_i64".into(), ty: FieldTy::I64, offset: 59, nullable: false },
            FieldSpec { name: "max_i64_seen".into(), ty: FieldTy::Bool, offset: 67, nullable: false },
            FieldSpec { name: "sum_i64".into(), ty: FieldTy::I64, offset: 68, nullable: false },
        ],
        inline_str_cap: 15,
        row_size: 76,
    };
    s.validate_layout().expect("valid");
    Arc::new(s)
}

fn state_schema_lastfirst() -> Arc<RegisteredSchema> {
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
}

fn make_event_num(user: &str, amount: f64, qty: i64) -> Row {
    let sch = build_event_schema_num();
    let mut r = Row::zeroed(&sch);
    r.write_inline_str(0, sch.inline_str_cap, user);
    r.write_f64(16, amount);
    r.write_i64(24, qty);
    r
}

fn make_event_str(label: &str) -> Row {
    let sch = build_event_schema_str();
    let mut r = Row::zeroed(&sch);
    r.write_inline_str(0, sch.inline_str_cap, label);
    r
}

#[test]
fn count_typed_increments_state_on_each_event() {
    let state_schema = state_schema_scalars();
    let event_schema = build_event_schema_num();
    let op = CountOpTyped { name: "count".into(), output_offset: 0 };
    let mut state = Row::zeroed(&state_schema);
    op.init_state(&state_schema, &mut state);
    for _ in 0..100 {
        let e = make_event_num("u1", 0.0, 1);
        op.update_typed(&mut state, &state_schema, &e, &event_schema, SystemTime::now());
    }
    match op.read_feature(&state, &state_schema) {
        FeatureValue::Int(n) => assert_eq!(n, 100),
        v => panic!("expected Int(100), got {:?}", v),
    }
}

#[test]
fn sum_f64_typed_accumulates() {
    let state_schema = state_schema_scalars();
    let event_schema = build_event_schema_num();
    let op = SumOpTypedF64 {
        name: "sum".into(),
        input_offset: 16,
        output_offset: 8,
    };
    let mut state = Row::zeroed(&state_schema);
    op.init_state(&state_schema, &mut state);
    for amount in [1.0, 2.5, 3.5] {
        let e = make_event_num("u1", amount, 0);
        op.update_typed(&mut state, &state_schema, &e, &event_schema, SystemTime::now());
    }
    match op.read_feature(&state, &state_schema) {
        FeatureValue::Float(f) => assert!((f - 7.0).abs() < 1e-9),
        v => panic!("expected Float(7.0), got {:?}", v),
    }
}

#[test]
fn sum_i64_typed_accumulates() {
    let state_schema = state_schema_scalars();
    let event_schema = build_event_schema_num();
    let op = SumOpTypedI64 {
        name: "sum_qty".into(),
        input_offset: 24,
        output_offset: 68,
    };
    let mut state = Row::zeroed(&state_schema);
    op.init_state(&state_schema, &mut state);
    for qty in [5, 7, 11] {
        let e = make_event_num("u1", 0.0, qty);
        op.update_typed(&mut state, &state_schema, &e, &event_schema, SystemTime::now());
    }
    match op.read_feature(&state, &state_schema) {
        FeatureValue::Int(n) => assert_eq!(n, 23),
        v => panic!("expected Int(23), got {:?}", v),
    }
}

#[test]
fn avg_typed_matches_sum_div_count() {
    let state_schema = state_schema_scalars();
    let event_schema = build_event_schema_num();
    let op = AvgOpTypedF64 {
        name: "avg".into(),
        input_offset: 16,
        sum_offset: 24,
        count_offset: 16,
    };
    let mut state = Row::zeroed(&state_schema);
    op.init_state(&state_schema, &mut state);
    for amount in [1.0, 2.5, 3.5] {
        let e = make_event_num("u1", amount, 0);
        op.update_typed(&mut state, &state_schema, &e, &event_schema, SystemTime::now());
    }
    match op.read_feature(&state, &state_schema) {
        FeatureValue::Float(f) => assert!((f - 7.0 / 3.0).abs() < 1e-9),
        v => panic!("expected Float(~2.333), got {:?}", v),
    }
}

#[test]
fn min_i64_typed_tracks_minimum() {
    let state_schema = state_schema_scalars();
    let event_schema = build_event_schema_num();
    let op = MinOpTypedI64 {
        name: "min_qty".into(),
        input_offset: 24,
        output_offset: 50,
        seen_offset: 58,
    };
    let mut state = Row::zeroed(&state_schema);
    op.init_state(&state_schema, &mut state);
    for q in [5, 3, 8, 1, 2] {
        let e = make_event_num("u1", 0.0, q);
        op.update_typed(&mut state, &state_schema, &e, &event_schema, SystemTime::now());
    }
    match op.read_feature(&state, &state_schema) {
        FeatureValue::Int(n) => assert_eq!(n, 1),
        v => panic!("expected Int(1), got {:?}", v),
    }
}

#[test]
fn max_f64_typed_tracks_maximum() {
    let state_schema = state_schema_scalars();
    let event_schema = build_event_schema_num();
    let op = MaxOpTypedF64 {
        name: "max_amt".into(),
        input_offset: 16,
        output_offset: 41,
        seen_offset: 49,
    };
    let mut state = Row::zeroed(&state_schema);
    op.init_state(&state_schema, &mut state);
    for a in [5.0_f64, 3.0, 8.0, 1.0] {
        let e = make_event_num("u1", a, 0);
        op.update_typed(&mut state, &state_schema, &e, &event_schema, SystemTime::now());
    }
    match op.read_feature(&state, &state_schema) {
        FeatureValue::Float(f) => assert!((f - 8.0).abs() < 1e-9),
        v => panic!("expected Float(8.0), got {:?}", v),
    }
}

#[test]
fn last_inline_str_captures_most_recent() {
    let state_schema = state_schema_lastfirst();
    let event_schema = build_event_schema_str();
    let op = LastOpTypedInlineStr {
        name: "last".into(),
        input_offset: 0,
        output_offset: 0,
        time_offset: 16,
        input_inline_str_cap: event_schema.inline_str_cap,
        output_inline_str_cap: state_schema.inline_str_cap,
    };
    let mut state = Row::zeroed(&state_schema);
    op.init_state(&state_schema, &mut state);
    for (i, label) in ["alice", "bob", "charlie"].iter().enumerate() {
        let e = make_event_str(label);
        let now = SystemTime::UNIX_EPOCH + std::time::Duration::from_millis(1_000 + i as u64);
        op.update_typed(&mut state, &state_schema, &e, &event_schema, now);
    }
    match op.read_feature(&state, &state_schema) {
        FeatureValue::String(s) => assert_eq!(s, "charlie"),
        v => panic!("expected String(charlie), got {:?}", v),
    }
}

#[test]
fn first_inline_str_captures_first() {
    let state_schema = state_schema_lastfirst();
    let event_schema = build_event_schema_str();
    let op = FirstOpTypedInlineStr {
        name: "first".into(),
        input_offset: 0,
        output_offset: 24,
        flag_offset: 40,
        input_inline_str_cap: event_schema.inline_str_cap,
        output_inline_str_cap: state_schema.inline_str_cap,
    };
    let mut state = Row::zeroed(&state_schema);
    op.init_state(&state_schema, &mut state);
    for label in ["alice", "bob", "charlie"].iter() {
        let e = make_event_str(label);
        op.update_typed(&mut state, &state_schema, &e, &event_schema, SystemTime::now());
    }
    match op.read_feature(&state, &state_schema) {
        FeatureValue::String(s) => assert_eq!(s, "alice"),
        v => panic!("expected String(alice), got {:?}", v),
    }
}

#[test]
fn avg_typed_empty_is_missing() {
    let state_schema = state_schema_scalars();
    let op = AvgOpTypedF64 {
        name: "avg".into(),
        input_offset: 16,
        sum_offset: 24,
        count_offset: 16,
    };
    let mut state = Row::zeroed(&state_schema);
    op.init_state(&state_schema, &mut state);
    assert!(matches!(op.read_feature(&state, &state_schema), FeatureValue::Missing));
}
