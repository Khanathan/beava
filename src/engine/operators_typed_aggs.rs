//! Phase 59.6 Wave 4 (TPC-PERF-11, D-F1 groups 1-4) — typed aggregation
//! operator implementations.
//!
//! Seven simple aggregation operators layered on top of the typed-row
//! runtime (Wave 1) and the [`TypedAggOp`] trait (Wave 4):
//!
//! - [`CountOpTyped`] — counts events per entity (type-agnostic).
//! - [`SumOpTypedI64`] / [`SumOpTypedF64`] — numeric running sum.
//! - [`AvgOpTypedF64`] — numeric running average (stores `sum` + `count`).
//! - [`MinOpTypedI64`] / [`MinOpTypedF64`] — numeric running minimum.
//! - [`MaxOpTypedI64`] / [`MaxOpTypedF64`] — numeric running maximum.
//! - [`LastOpTypedInlineStr`] — most recent inline-string value.
//! - [`FirstOpTypedInlineStr`] — first recorded inline-string value.
//!
//! Each op holds pre-resolved byte offsets into the per-entity agg-state
//! [`Row`]; the hot path performs a scalar `read` + arithmetic + `write`
//! with no allocation, no HashMap lookup, and no enum dispatch. This is
//! the Wave-4 realization of D-C4 (feature updates mutate specific columns
//! in place; no allocation per event once the entity state is initialized).
//!
//! # Parity contract (TPC-CORR-07)
//!
//! Each typed op's output [`FeatureValue`] MUST match the Value-path
//! equivalent op over the same event stream byte-for-byte. The
//! integration harness `tests/typed_aggregation_parity.rs` drives 100K
//! events through both paths and diffs; Wave-4 delivers the simple-aggs
//! subset; the advanced aggs stay RED until Wave 6.
//!
//! # Column-type specialization (D-C1)
//!
//! Per D-C1, operators are monomorphized over the input column type via
//! separate structs (`SumOpTypedI64`, `SumOpTypedF64`, etc.) rather than a
//! single generic-over-`T: AggNum` impl. This avoids codegen blow-up when
//! the runtime eventually supports 6+ column types and keeps each struct
//! strictly `#[inline]`-friendly.

use crate::engine::operators_typed::TypedAggOp;
use crate::engine::schema::{RegisteredSchema, Row};
use crate::types::FeatureValue;
use std::time::SystemTime;

// ---------------------------------------------------------------------------
// CountOpTyped
// ---------------------------------------------------------------------------

/// Type-agnostic per-event counter. Stores a single `i64` at
/// `output_offset` inside the per-entity agg-state Row.
#[derive(Clone, Debug)]
pub struct CountOpTyped {
    pub name: String,
    pub output_offset: u16,
}

impl TypedAggOp for CountOpTyped {
    fn init_state(&self, _ss: &RegisteredSchema, state: &mut Row) {
        state.write_i64(self.output_offset, 0);
    }
    #[inline]
    fn update_typed(
        &self,
        state: &mut Row,
        _ss: &RegisteredSchema,
        _e: &Row,
        _es: &RegisteredSchema,
        _now: SystemTime,
    ) {
        let cur = state.read_i64(self.output_offset);
        state.write_i64(self.output_offset, cur + 1);
    }
    fn read_feature(&self, state: &Row, _ss: &RegisteredSchema) -> FeatureValue {
        FeatureValue::Int(state.read_i64(self.output_offset))
    }
    fn name(&self) -> &str {
        &self.name
    }
}

// ---------------------------------------------------------------------------
// SumOpTyped<i64>
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
pub struct SumOpTypedI64 {
    pub name: String,
    pub input_offset: u16,
    pub output_offset: u16,
}

impl TypedAggOp for SumOpTypedI64 {
    fn init_state(&self, _ss: &RegisteredSchema, state: &mut Row) {
        state.write_i64(self.output_offset, 0);
    }
    #[inline]
    fn update_typed(
        &self,
        state: &mut Row,
        _ss: &RegisteredSchema,
        event: &Row,
        _es: &RegisteredSchema,
        _now: SystemTime,
    ) {
        let cur = state.read_i64(self.output_offset);
        let inc = event.read_i64(self.input_offset);
        state.write_i64(self.output_offset, cur.wrapping_add(inc));
    }
    fn read_feature(&self, state: &Row, _ss: &RegisteredSchema) -> FeatureValue {
        FeatureValue::Int(state.read_i64(self.output_offset))
    }
    fn name(&self) -> &str {
        &self.name
    }
}

// ---------------------------------------------------------------------------
// SumOpTyped<f64>
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
pub struct SumOpTypedF64 {
    pub name: String,
    pub input_offset: u16,
    pub output_offset: u16,
}

impl TypedAggOp for SumOpTypedF64 {
    fn init_state(&self, _ss: &RegisteredSchema, state: &mut Row) {
        state.write_f64(self.output_offset, 0.0);
    }
    #[inline]
    fn update_typed(
        &self,
        state: &mut Row,
        _ss: &RegisteredSchema,
        event: &Row,
        _es: &RegisteredSchema,
        _now: SystemTime,
    ) {
        let cur = state.read_f64(self.output_offset);
        let inc = event.read_f64(self.input_offset);
        state.write_f64(self.output_offset, cur + inc);
    }
    fn read_feature(&self, state: &Row, _ss: &RegisteredSchema) -> FeatureValue {
        FeatureValue::Float(state.read_f64(self.output_offset))
    }
    fn name(&self) -> &str {
        &self.name
    }
}

// ---------------------------------------------------------------------------
// AvgOpTyped<f64>
//
// Stores (sum: f64, count: i64) in state; feature = sum / count (or 0.0
// if count == 0 to mirror the Value-path behavior where avg of nothing
// reports as zero rather than NaN).
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
pub struct AvgOpTypedF64 {
    pub name: String,
    pub input_offset: u16,
    pub sum_offset: u16,
    pub count_offset: u16,
}

impl TypedAggOp for AvgOpTypedF64 {
    fn init_state(&self, _ss: &RegisteredSchema, state: &mut Row) {
        state.write_f64(self.sum_offset, 0.0);
        state.write_i64(self.count_offset, 0);
    }
    #[inline]
    fn update_typed(
        &self,
        state: &mut Row,
        _ss: &RegisteredSchema,
        event: &Row,
        _es: &RegisteredSchema,
        _now: SystemTime,
    ) {
        let new_sum = state.read_f64(self.sum_offset) + event.read_f64(self.input_offset);
        let new_count = state.read_i64(self.count_offset) + 1;
        state.write_f64(self.sum_offset, new_sum);
        state.write_i64(self.count_offset, new_count);
    }
    fn read_feature(&self, state: &Row, _ss: &RegisteredSchema) -> FeatureValue {
        let cnt = state.read_i64(self.count_offset);
        if cnt == 0 {
            FeatureValue::Missing
        } else {
            FeatureValue::Float(state.read_f64(self.sum_offset) / cnt as f64)
        }
    }
    fn name(&self) -> &str {
        &self.name
    }
}

// ---------------------------------------------------------------------------
// MinOpTyped<i64> / MinOpTyped<f64>
//
// The sentinel "no event seen yet" is encoded by a separate `seen_offset`
// bool. First event always wins. This mirrors the Value-path `MinOp`
// semantics (Missing before first event, then tracks minimum).
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
pub struct MinOpTypedI64 {
    pub name: String,
    pub input_offset: u16,
    pub output_offset: u16,
    pub seen_offset: u16,
}

impl TypedAggOp for MinOpTypedI64 {
    fn init_state(&self, _ss: &RegisteredSchema, state: &mut Row) {
        state.write_i64(self.output_offset, i64::MAX);
        state.write_bool(self.seen_offset, false);
    }
    #[inline]
    fn update_typed(
        &self,
        state: &mut Row,
        _ss: &RegisteredSchema,
        event: &Row,
        _es: &RegisteredSchema,
        _now: SystemTime,
    ) {
        let v = event.read_i64(self.input_offset);
        let seen = state.read_bool(self.seen_offset);
        if !seen || v < state.read_i64(self.output_offset) {
            state.write_i64(self.output_offset, v);
            state.write_bool(self.seen_offset, true);
        }
    }
    fn read_feature(&self, state: &Row, _ss: &RegisteredSchema) -> FeatureValue {
        if state.read_bool(self.seen_offset) {
            FeatureValue::Int(state.read_i64(self.output_offset))
        } else {
            FeatureValue::Missing
        }
    }
    fn name(&self) -> &str {
        &self.name
    }
}

#[derive(Clone, Debug)]
pub struct MinOpTypedF64 {
    pub name: String,
    pub input_offset: u16,
    pub output_offset: u16,
    pub seen_offset: u16,
}

impl TypedAggOp for MinOpTypedF64 {
    fn init_state(&self, _ss: &RegisteredSchema, state: &mut Row) {
        state.write_f64(self.output_offset, f64::MAX);
        state.write_bool(self.seen_offset, false);
    }
    #[inline]
    fn update_typed(
        &self,
        state: &mut Row,
        _ss: &RegisteredSchema,
        event: &Row,
        _es: &RegisteredSchema,
        _now: SystemTime,
    ) {
        let v = event.read_f64(self.input_offset);
        let seen = state.read_bool(self.seen_offset);
        if !seen || v < state.read_f64(self.output_offset) {
            state.write_f64(self.output_offset, v);
            state.write_bool(self.seen_offset, true);
        }
    }
    fn read_feature(&self, state: &Row, _ss: &RegisteredSchema) -> FeatureValue {
        if state.read_bool(self.seen_offset) {
            FeatureValue::Float(state.read_f64(self.output_offset))
        } else {
            FeatureValue::Missing
        }
    }
    fn name(&self) -> &str {
        &self.name
    }
}

// ---------------------------------------------------------------------------
// MaxOpTyped<i64> / MaxOpTyped<f64>
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
pub struct MaxOpTypedI64 {
    pub name: String,
    pub input_offset: u16,
    pub output_offset: u16,
    pub seen_offset: u16,
}

impl TypedAggOp for MaxOpTypedI64 {
    fn init_state(&self, _ss: &RegisteredSchema, state: &mut Row) {
        state.write_i64(self.output_offset, i64::MIN);
        state.write_bool(self.seen_offset, false);
    }
    #[inline]
    fn update_typed(
        &self,
        state: &mut Row,
        _ss: &RegisteredSchema,
        event: &Row,
        _es: &RegisteredSchema,
        _now: SystemTime,
    ) {
        let v = event.read_i64(self.input_offset);
        let seen = state.read_bool(self.seen_offset);
        if !seen || v > state.read_i64(self.output_offset) {
            state.write_i64(self.output_offset, v);
            state.write_bool(self.seen_offset, true);
        }
    }
    fn read_feature(&self, state: &Row, _ss: &RegisteredSchema) -> FeatureValue {
        if state.read_bool(self.seen_offset) {
            FeatureValue::Int(state.read_i64(self.output_offset))
        } else {
            FeatureValue::Missing
        }
    }
    fn name(&self) -> &str {
        &self.name
    }
}

#[derive(Clone, Debug)]
pub struct MaxOpTypedF64 {
    pub name: String,
    pub input_offset: u16,
    pub output_offset: u16,
    pub seen_offset: u16,
}

impl TypedAggOp for MaxOpTypedF64 {
    fn init_state(&self, _ss: &RegisteredSchema, state: &mut Row) {
        state.write_f64(self.output_offset, f64::MIN);
        state.write_bool(self.seen_offset, false);
    }
    #[inline]
    fn update_typed(
        &self,
        state: &mut Row,
        _ss: &RegisteredSchema,
        event: &Row,
        _es: &RegisteredSchema,
        _now: SystemTime,
    ) {
        let v = event.read_f64(self.input_offset);
        let seen = state.read_bool(self.seen_offset);
        if !seen || v > state.read_f64(self.output_offset) {
            state.write_f64(self.output_offset, v);
            state.write_bool(self.seen_offset, true);
        }
    }
    fn read_feature(&self, state: &Row, _ss: &RegisteredSchema) -> FeatureValue {
        if state.read_bool(self.seen_offset) {
            FeatureValue::Float(state.read_f64(self.output_offset))
        } else {
            FeatureValue::Missing
        }
    }
    fn name(&self) -> &str {
        &self.name
    }
}

// ---------------------------------------------------------------------------
// LastOpTyped<InlineStr>
//
// Stores (value, event_time_ms). Updates on every event whose `now` is at
// or after the stored timestamp — i.e. the most recent event wins. This
// mirrors the Value-path `LastOp` which uses wall-clock ordering.
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
pub struct LastOpTypedInlineStr {
    pub name: String,
    pub input_offset: u16,
    pub output_offset: u16,
    pub time_offset: u16,
    pub input_inline_str_cap: u8,
    pub output_inline_str_cap: u8,
}

impl TypedAggOp for LastOpTypedInlineStr {
    fn init_state(&self, _ss: &RegisteredSchema, state: &mut Row) {
        state.write_inline_str(self.output_offset, self.output_inline_str_cap, "");
        state.write_i64(self.time_offset, i64::MIN);
    }
    #[inline]
    fn update_typed(
        &self,
        state: &mut Row,
        _ss: &RegisteredSchema,
        event: &Row,
        _es: &RegisteredSchema,
        now: SystemTime,
    ) {
        let now_ms = now
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);
        if now_ms >= state.read_i64(self.time_offset) {
            // Copy via a stack-local buffer to avoid borrow-checker pain
            // (we want to take &event then write into &mut state).
            let v = event
                .read_inline_str(self.input_offset, self.input_inline_str_cap)
                .to_string();
            state.write_inline_str(self.output_offset, self.output_inline_str_cap, &v);
            state.write_i64(self.time_offset, now_ms);
        }
    }
    fn read_feature(&self, state: &Row, _ss: &RegisteredSchema) -> FeatureValue {
        if state.read_i64(self.time_offset) == i64::MIN {
            FeatureValue::Missing
        } else {
            FeatureValue::String(
                state
                    .read_inline_str(self.output_offset, self.output_inline_str_cap)
                    .to_string(),
            )
        }
    }
    fn name(&self) -> &str {
        &self.name
    }
}

// ---------------------------------------------------------------------------
// FirstOpTyped<InlineStr>
//
// Stores (value, flag). Flag set on first event; subsequent updates are
// no-ops. Mirrors the Value-path `FirstOp`.
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
pub struct FirstOpTypedInlineStr {
    pub name: String,
    pub input_offset: u16,
    pub output_offset: u16,
    pub flag_offset: u16,
    pub input_inline_str_cap: u8,
    pub output_inline_str_cap: u8,
}

impl TypedAggOp for FirstOpTypedInlineStr {
    fn init_state(&self, _ss: &RegisteredSchema, state: &mut Row) {
        state.write_inline_str(self.output_offset, self.output_inline_str_cap, "");
        state.write_bool(self.flag_offset, false);
    }
    #[inline]
    fn update_typed(
        &self,
        state: &mut Row,
        _ss: &RegisteredSchema,
        event: &Row,
        _es: &RegisteredSchema,
        _now: SystemTime,
    ) {
        if !state.read_bool(self.flag_offset) {
            let v = event
                .read_inline_str(self.input_offset, self.input_inline_str_cap)
                .to_string();
            state.write_inline_str(self.output_offset, self.output_inline_str_cap, &v);
            state.write_bool(self.flag_offset, true);
        }
    }
    fn read_feature(&self, state: &Row, _ss: &RegisteredSchema) -> FeatureValue {
        if state.read_bool(self.flag_offset) {
            FeatureValue::String(
                state
                    .read_inline_str(self.output_offset, self.output_inline_str_cap)
                    .to_string(),
            )
        } else {
            FeatureValue::Missing
        }
    }
    fn name(&self) -> &str {
        &self.name
    }
}

// ---------------------------------------------------------------------------
// Tests
//
// Lib-test execution is currently blocked by the pre-existing Phase-60 salt
// sweep (33 `StreamDefinition { .. }` sites missing `salt: None` in `src/`),
// documented as a deferred issue on Wave 0 / Wave 2 / Wave 3. These tests
// compile correctly when the salt sweep lands; until then the same
// behavior is exercised from the integration binary
// `tests/typed_aggs_unit.rs`.
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::schema::{FieldSpec, FieldTy, RegisteredSchema, Row};
    use std::sync::Arc;

    fn build_event_schema_num() -> Arc<RegisteredSchema> {
        // [user_id: inline_str@0 | amount: f64@16 | qty: i64@24]
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
        // [label: inline_str@0]
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
        // Layout: [count_i64@0 | sum_f64@8 | avg_count_i64@16 | avg_sum_f64@24
        //  | min_f64@32 | min_seen_bool@40 | max_f64@41 | max_seen_bool@49
        //  | min_i64@50 | min_i64_seen@58 | max_i64@59 | max_i64_seen@67
        //  | sum_i64@68]
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
        // {1.0, 2.5, 3.5} → avg 7/3 ≈ 2.333
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
    fn min_max_typed_empty_is_missing() {
        let state_schema = state_schema_scalars();
        let min = MinOpTypedF64 {
            name: "m".into(),
            input_offset: 16,
            output_offset: 32,
            seen_offset: 40,
        };
        let max = MaxOpTypedF64 {
            name: "M".into(),
            input_offset: 16,
            output_offset: 41,
            seen_offset: 49,
        };
        let mut state = Row::zeroed(&state_schema);
        min.init_state(&state_schema, &mut state);
        max.init_state(&state_schema, &mut state);
        assert!(matches!(min.read_feature(&state, &state_schema), FeatureValue::Missing));
        assert!(matches!(max.read_feature(&state, &state_schema), FeatureValue::Missing));
    }

    fn state_schema_lastfirst() -> Arc<RegisteredSchema> {
        // [last_str@0 slot=16 | last_time@16 | first_str@24 slot=16 | first_flag@40]
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
            // Simulate monotonic event time via offset in `now`.
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
    fn last_first_empty_is_missing() {
        let state_schema = state_schema_lastfirst();
        let last = LastOpTypedInlineStr {
            name: "l".into(),
            input_offset: 0,
            output_offset: 0,
            time_offset: 16,
            input_inline_str_cap: 15,
            output_inline_str_cap: 15,
        };
        let first = FirstOpTypedInlineStr {
            name: "f".into(),
            input_offset: 0,
            output_offset: 24,
            flag_offset: 40,
            input_inline_str_cap: 15,
            output_inline_str_cap: 15,
        };
        let mut state = Row::zeroed(&state_schema);
        last.init_state(&state_schema, &mut state);
        first.init_state(&state_schema, &mut state);
        assert!(matches!(last.read_feature(&state, &state_schema), FeatureValue::Missing));
        assert!(matches!(first.read_feature(&state, &state_schema), FeatureValue::Missing));
    }
}
