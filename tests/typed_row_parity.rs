//! Phase 59.6 Wave 0 — parity harness. Wave 4 replaces the Wave-0 stub
//! panics with real typed + Value path drivers and diffs their output
//! state.
//!
//! Scope: runs the same event stream through BOTH the typed [`TypedAggOp`]
//! implementations and the Value-path [`Operator`] siblings; asserts the
//! resulting per-entity feature values are equivalent.
//!
//! See `.planning/phases/59.6-typed-pipeline-records/59.6-CONTEXT.md`
//! (D-F2) for the parity-gate semantics.

#![allow(unused_imports)]

use beava::engine::operators::{CountOp as CountOpValue, Operator};
use beava::engine::operators_typed::TypedAggOp;
use beava::engine::operators_typed_aggs::CountOpTyped;
use beava::engine::schema::{FieldSpec, FieldTy, RegisteredSchema, Row};
use beava::types::FeatureValue;
use serde_json::json;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

fn event_schema() -> Arc<RegisteredSchema> {
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

fn state_schema() -> Arc<RegisteredSchema> {
    let s = RegisteredSchema {
        schema_id: 0,
        name: "CountState".into(),
        fields: vec![FieldSpec {
            name: "count".into(),
            ty: FieldTy::I64,
            offset: 0,
            nullable: false,
        }],
        inline_str_cap: 15,
        row_size: 8,
    };
    s.validate_layout().expect("valid");
    Arc::new(s)
}

fn make_event(user: &str, amount: f64) -> (Row, serde_json::Value) {
    let sch = event_schema();
    let mut r = Row::zeroed(&sch);
    r.write_inline_str(0, sch.inline_str_cap, user);
    r.write_f64(16, amount);
    (r, json!({ "user_id": user, "amount": amount }))
}

/// SC-4 smoke: Count aggregation via typed + Value paths on same stream.
/// Wave 4 flips this from RED → GREEN.
#[test]
fn typed_and_value_paths_produce_identical_count_state() {
    // Per-entity typed state map (mirror of Shard.entity_state_typed
    // inlined here to keep the test self-contained).
    let state_schema = state_schema();
    let event_schema = event_schema();

    let typed = CountOpTyped {
        name: "count".into(),
        output_offset: 0,
    };

    // Drive 100 events spanning 10 entities (so each entity accrues 10).
    let mut typed_states: ahash::AHashMap<String, Row> = ahash::AHashMap::new();
    let big_window = Duration::from_secs(86_400 * 365);
    let big_bucket = Duration::from_secs(3_600);
    let mut value_ops: ahash::AHashMap<String, CountOpValue> = ahash::AHashMap::new();

    let mut now = SystemTime::now();
    for i in 0..100 {
        let user = format!("u{}", i % 10);
        let (row, val) = make_event(&user, 1.0);

        // Typed
        let state = typed_states.entry(user.clone()).or_insert_with(|| {
            let mut r = Row::zeroed(&state_schema);
            typed.init_state(&state_schema, &mut r);
            r
        });
        typed.update_typed(state, &state_schema, &row, &event_schema, now);

        // Value
        let op = value_ops
            .entry(user.clone())
            .or_insert_with(|| CountOpValue::new(big_window, big_bucket));
        op.push(&val, None, now).expect("value push");

        now += Duration::from_millis(1);
    }

    // Diff each entity's feature value across paths.
    for (user, typed_state) in &typed_states {
        let t = typed.read_feature(typed_state, &state_schema);
        let v = value_ops
            .get_mut(user)
            .expect("matching entity")
            .read(now);
        assert_eq!(t, v, "count divergence for {}: typed={:?} value={:?}", user, t, v);
        assert_eq!(t, FeatureValue::Int(10));
    }
}

/// SC-4 harness assertion: diff over 100K events, one entity.
#[test]
fn typed_row_parity_harness_diffs_100k_events() {
    let state_schema = state_schema();
    let event_schema = event_schema();

    let typed = CountOpTyped {
        name: "count".into(),
        output_offset: 0,
    };
    let mut state = Row::zeroed(&state_schema);
    typed.init_state(&state_schema, &mut state);

    let mut value_op = CountOpValue::new(
        Duration::from_secs(86_400 * 365),
        Duration::from_secs(3_600),
    );

    let mut now = SystemTime::now();
    for i in 0..100_000_usize {
        let (row, val) = make_event("u1", i as f64);
        typed.update_typed(&mut state, &state_schema, &row, &event_schema, now);
        value_op.push(&val, None, now).expect("value push");
        now += Duration::from_millis(1);
    }

    let t = typed.read_feature(&state, &state_schema);
    let v = value_op.read(now);
    assert_eq!(t, v, "100K parity diverged: typed={:?} value={:?}", t, v);
    assert_eq!(t, FeatureValue::Int(100_000));
}
