//! Phase 59.6 Wave 6 (TPC-PERF-11, D-F1 group 6+8) — typed windowed /
//! recurrence operator implementations.
//!
//! Four operators:
//!
//! - [`EmaOpTyped`] — exponentially-weighted moving average with
//!   time-based decay. State Row stores `current: f64` + `initialized:
//!   bool`; the SideBand carries `last_ts` (not representable in a
//!   fixed-layout Row).
//! - [`LagOpTyped`] — N-sample lag: returns the value from N events ago.
//!   Uses a [`VecDeque`] ring buffer in the SideBand.
//! - [`FirstNOpTyped`] — first N values seen, in arrival order.
//! - [`LastNOpTyped`] — most recent N values, in arrival order.
//!
//! # D-C1 SideBand usage
//!
//! Lag / FirstN / LastN all hold a ring of past `FeatureValue`s whose
//! size is parameterized by the op's `n`. Storing the ring inline in
//! the per-entity state Row would require knowing the op's `n` at
//! layout time and burn (n × ~32 bytes) per entity regardless of how
//! many events that entity has seen. The SideBand path amortizes this
//! to zero for unobserved entities and bounds it to the op's cap for
//! hot entities.

use crate::engine::operators_typed::{SideBand, TypedAggOp};
use crate::engine::schema::{FieldTy, RegisteredSchema, Row};
use crate::types::FeatureValue;
use std::collections::VecDeque;
use std::time::SystemTime;

// ---------------------------------------------------------------------------
// Shared scalar extraction helpers
// ---------------------------------------------------------------------------

fn event_feature_value(
    ev: &Row,
    ev_schema: &RegisteredSchema,
    input_offset: u16,
    input_ty: FieldTy,
) -> FeatureValue {
    match input_ty {
        FieldTy::I64 => FeatureValue::Int(ev.read_i64(input_offset)),
        FieldTy::F64 => FeatureValue::Float(ev.read_f64(input_offset)),
        FieldTy::Bool => FeatureValue::Int(if ev.read_bool(input_offset) { 1 } else { 0 }),
        FieldTy::InlineStr => FeatureValue::String(
            ev.read_inline_str(input_offset, ev_schema.inline_str_cap).to_string(),
        ),
        FieldTy::String => FeatureValue::String(ev.read_string(input_offset).to_string()),
        FieldTy::Bytes => FeatureValue::Missing,
    }
}

/// Serialize a ring of `FeatureValue`s into a JSON array string — mirrors
/// the Value-path FirstNOp / LastNOp `read()` shape so parity tests see
/// byte-identical string output.
fn ring_to_feature_value(ring: &VecDeque<FeatureValue>) -> FeatureValue {
    if ring.is_empty() {
        return FeatureValue::Missing;
    }
    let arr: Vec<serde_json::Value> = ring.iter().map(|v| v.to_json_value()).collect();
    let s = serde_json::to_string(&arr).unwrap_or_else(|_| "[]".into());
    FeatureValue::String(s)
}

// ---------------------------------------------------------------------------
// EmaOpTyped
//
// State Row: (current: f64, initialized: bool).
// SideBand: ema_last_ts[name] = last-observed SystemTime, for decay.
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
pub struct EmaOpTyped {
    pub name: String,
    pub input_offset: u16,
    pub input_ty: FieldTy,
    pub half_life_secs: f64,
    pub current_offset: u16,
    pub init_flag_offset: u16,
}

impl TypedAggOp for EmaOpTyped {
    fn init_state(&self, _ss: &RegisteredSchema, state: &mut Row) {
        state.write_f64(self.current_offset, 0.0);
        state.write_bool(self.init_flag_offset, false);
    }

    fn update_typed(
        &self,
        _state: &mut Row,
        _ss: &RegisteredSchema,
        _event: &Row,
        _es: &RegisteredSchema,
        _now: SystemTime,
    ) {
        // EMA requires SideBand (last-ts state).
    }

    fn update_with_sideband(
        &self,
        state: &mut Row,
        _ss: &RegisteredSchema,
        event: &Row,
        _es: &RegisteredSchema,
        sideband: &mut SideBand,
        now: SystemTime,
    ) {
        let v = match self.input_ty {
            FieldTy::I64 => event.read_i64(self.input_offset) as f64,
            FieldTy::F64 => event.read_f64(self.input_offset),
            _ => return,
        };
        let initialized = state.read_bool(self.init_flag_offset);
        if !initialized {
            state.write_f64(self.current_offset, v);
            state.write_bool(self.init_flag_offset, true);
        } else {
            let prev_ts = sideband.ema_last_ts.get(&self.name).copied();
            if let Some(prev) = prev_ts {
                let elapsed = now
                    .duration_since(prev)
                    .unwrap_or(std::time::Duration::ZERO)
                    .as_secs_f64();
                let alpha =
                    (-std::f64::consts::LN_2 * elapsed / self.half_life_secs).exp();
                let cur = state.read_f64(self.current_offset);
                let next = alpha * cur + (1.0 - alpha) * v;
                state.write_f64(self.current_offset, next);
            } else {
                // Initialized flag set but no last-ts — graceful reset (matches
                // the Value-path guard).
                state.write_f64(self.current_offset, v);
            }
        }
        sideband.ema_last_ts.insert(self.name.clone(), now);
    }

    fn read_feature(&self, state: &Row, _ss: &RegisteredSchema) -> FeatureValue {
        if state.read_bool(self.init_flag_offset) {
            FeatureValue::Float(state.read_f64(self.current_offset))
        } else {
            FeatureValue::Missing
        }
    }

    fn name(&self) -> &str {
        &self.name
    }
}

// ---------------------------------------------------------------------------
// LagOpTyped
//
// Maintains a ring of the last (N+1) values in the SideBand; `read` returns
// the front, i.e. the value from N events ago. State Row stores the
// current ring length (i64) at `size_offset` as a debug health value.
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
pub struct LagOpTyped {
    pub name: String,
    pub input_offset: u16,
    pub input_ty: FieldTy,
    pub n: usize,
    pub size_offset: u16,
}

impl TypedAggOp for LagOpTyped {
    fn init_state(&self, _ss: &RegisteredSchema, state: &mut Row) {
        state.write_i64(self.size_offset, 0);
    }

    fn update_typed(
        &self,
        _state: &mut Row,
        _ss: &RegisteredSchema,
        _event: &Row,
        _es: &RegisteredSchema,
        _now: SystemTime,
    ) {
        // Lag requires SideBand.
    }

    fn update_with_sideband(
        &self,
        state: &mut Row,
        _ss: &RegisteredSchema,
        event: &Row,
        es: &RegisteredSchema,
        sideband: &mut SideBand,
        _now: SystemTime,
    ) {
        let fv = event_feature_value(event, es, self.input_offset, self.input_ty);
        let ring = sideband
            .ring_buffers
            .entry(self.name.clone())
            .or_insert_with(|| VecDeque::with_capacity(self.n + 1));
        ring.push_back(fv);
        while ring.len() > self.n + 1 {
            ring.pop_front();
        }
        state.write_i64(self.size_offset, ring.len() as i64);
    }

    fn read_feature(&self, _state: &Row, _ss: &RegisteredSchema) -> FeatureValue {
        FeatureValue::Missing
    }

    fn read_feature_with_sideband(
        &self,
        _state: &Row,
        _ss: &RegisteredSchema,
        sideband: &SideBand,
    ) -> FeatureValue {
        match sideband.ring_buffers.get(&self.name) {
            Some(ring) if ring.len() == self.n + 1 => {
                ring.front().cloned().unwrap_or(FeatureValue::Missing)
            }
            _ => FeatureValue::Missing,
        }
    }

    fn name(&self) -> &str {
        &self.name
    }
}

// ---------------------------------------------------------------------------
// FirstNOpTyped
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
pub struct FirstNOpTyped {
    pub name: String,
    pub input_offset: u16,
    pub input_ty: FieldTy,
    pub n: usize,
    pub size_offset: u16,
}

impl TypedAggOp for FirstNOpTyped {
    fn init_state(&self, _ss: &RegisteredSchema, state: &mut Row) {
        state.write_i64(self.size_offset, 0);
    }

    fn update_typed(
        &self,
        _state: &mut Row,
        _ss: &RegisteredSchema,
        _event: &Row,
        _es: &RegisteredSchema,
        _now: SystemTime,
    ) {
        // FirstN requires SideBand.
    }

    fn update_with_sideband(
        &self,
        state: &mut Row,
        _ss: &RegisteredSchema,
        event: &Row,
        es: &RegisteredSchema,
        sideband: &mut SideBand,
        _now: SystemTime,
    ) {
        let ring = sideband
            .ring_buffers
            .entry(self.name.clone())
            .or_insert_with(|| VecDeque::with_capacity(self.n));
        if ring.len() < self.n {
            let fv = event_feature_value(event, es, self.input_offset, self.input_ty);
            ring.push_back(fv);
        }
        state.write_i64(self.size_offset, ring.len() as i64);
    }

    fn read_feature(&self, _state: &Row, _ss: &RegisteredSchema) -> FeatureValue {
        FeatureValue::Missing
    }

    fn read_feature_with_sideband(
        &self,
        _state: &Row,
        _ss: &RegisteredSchema,
        sideband: &SideBand,
    ) -> FeatureValue {
        match sideband.ring_buffers.get(&self.name) {
            Some(ring) => ring_to_feature_value(ring),
            None => FeatureValue::Missing,
        }
    }

    fn name(&self) -> &str {
        &self.name
    }
}

// ---------------------------------------------------------------------------
// LastNOpTyped
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
pub struct LastNOpTyped {
    pub name: String,
    pub input_offset: u16,
    pub input_ty: FieldTy,
    pub n: usize,
    pub size_offset: u16,
}

impl TypedAggOp for LastNOpTyped {
    fn init_state(&self, _ss: &RegisteredSchema, state: &mut Row) {
        state.write_i64(self.size_offset, 0);
    }

    fn update_typed(
        &self,
        _state: &mut Row,
        _ss: &RegisteredSchema,
        _event: &Row,
        _es: &RegisteredSchema,
        _now: SystemTime,
    ) {
        // LastN requires SideBand.
    }

    fn update_with_sideband(
        &self,
        state: &mut Row,
        _ss: &RegisteredSchema,
        event: &Row,
        es: &RegisteredSchema,
        sideband: &mut SideBand,
        _now: SystemTime,
    ) {
        let fv = event_feature_value(event, es, self.input_offset, self.input_ty);
        let ring = sideband
            .ring_buffers
            .entry(self.name.clone())
            .or_insert_with(|| VecDeque::with_capacity(self.n));
        ring.push_back(fv);
        while ring.len() > self.n {
            ring.pop_front();
        }
        state.write_i64(self.size_offset, ring.len() as i64);
    }

    fn read_feature(&self, _state: &Row, _ss: &RegisteredSchema) -> FeatureValue {
        FeatureValue::Missing
    }

    fn read_feature_with_sideband(
        &self,
        _state: &Row,
        _ss: &RegisteredSchema,
        sideband: &SideBand,
    ) -> FeatureValue {
        match sideband.ring_buffers.get(&self.name) {
            Some(ring) => ring_to_feature_value(ring),
            None => FeatureValue::Missing,
        }
    }

    fn name(&self) -> &str {
        &self.name
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::schema::{FieldSpec, FieldTy};
    use std::sync::Arc;
    use std::time::Duration;

    fn num_schema() -> Arc<RegisteredSchema> {
        let s = RegisteredSchema {
            schema_id: 0,
            name: "Txns".into(),
            fields: vec![FieldSpec {
                name: "amount".into(),
                ty: FieldTy::F64,
                offset: 0,
                nullable: false,
            }],
            inline_str_cap: 15,
            row_size: 8,
        };
        s.validate_layout().unwrap();
        Arc::new(s)
    }

    fn make_num_event(amount: f64) -> Row {
        let sch = num_schema();
        let mut r = Row::zeroed(&sch);
        r.write_f64(0, amount);
        r
    }

    fn ema_state_schema() -> Arc<RegisteredSchema> {
        // (current: f64 @0, init_flag: bool @8)
        let s = RegisteredSchema {
            schema_id: 0,
            name: "EmaState".into(),
            fields: vec![
                FieldSpec { name: "current".into(), ty: FieldTy::F64, offset: 0, nullable: false },
                FieldSpec { name: "init".into(), ty: FieldTy::Bool, offset: 8, nullable: false },
            ],
            inline_str_cap: 15,
            row_size: 9,
        };
        s.validate_layout().unwrap();
        Arc::new(s)
    }

    fn size_only_schema() -> Arc<RegisteredSchema> {
        let s = RegisteredSchema {
            schema_id: 0,
            name: "Size".into(),
            fields: vec![FieldSpec { name: "size".into(), ty: FieldTy::I64, offset: 0, nullable: false }],
            inline_str_cap: 15,
            row_size: 8,
        };
        s.validate_layout().unwrap();
        Arc::new(s)
    }

    #[test]
    fn ema_typed_matches_value_formula() {
        let op = EmaOpTyped {
            name: "e".into(),
            input_offset: 0,
            input_ty: FieldTy::F64,
            half_life_secs: 10.0,
            current_offset: 0,
            init_flag_offset: 8,
        };
        let state_sch = ema_state_schema();
        let ev_sch = num_schema();
        let mut state = Row::zeroed(&state_sch);
        op.init_state(&state_sch, &mut state);
        let mut sb = SideBand::default();
        let t0 = SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000);
        // First event: current = v
        op.update_with_sideband(&mut state, &state_sch, &make_num_event(10.0), &ev_sch, &mut sb, t0);
        assert!((state.read_f64(0) - 10.0).abs() < 1e-9);
        // 10 s later → alpha = exp(-ln2 * 10 / 10) = 0.5; current = 0.5*10 + 0.5*20 = 15
        let t1 = t0 + Duration::from_secs(10);
        op.update_with_sideband(&mut state, &state_sch, &make_num_event(20.0), &ev_sch, &mut sb, t1);
        assert!((state.read_f64(0) - 15.0).abs() < 1e-9, "got {}", state.read_f64(0));
    }

    #[test]
    fn ema_typed_empty_is_missing() {
        let op = EmaOpTyped {
            name: "e".into(),
            input_offset: 0,
            input_ty: FieldTy::F64,
            half_life_secs: 10.0,
            current_offset: 0,
            init_flag_offset: 8,
        };
        let state_sch = ema_state_schema();
        let mut state = Row::zeroed(&state_sch);
        op.init_state(&state_sch, &mut state);
        assert!(matches!(op.read_feature(&state, &state_sch), FeatureValue::Missing));
    }

    #[test]
    fn lag_typed_returns_n_events_ago() {
        let op = LagOpTyped {
            name: "l".into(),
            input_offset: 0,
            input_ty: FieldTy::F64,
            n: 3,
            size_offset: 0,
        };
        let state_sch = size_only_schema();
        let ev_sch = num_schema();
        let mut state = Row::zeroed(&state_sch);
        op.init_state(&state_sch, &mut state);
        let mut sb = SideBand::default();
        let seq = [1.0, 2.0, 3.0, 4.0, 5.0];
        for v in seq {
            op.update_with_sideband(&mut state, &state_sch, &make_num_event(v), &ev_sch, &mut sb, SystemTime::now());
        }
        // After 5 events with n=3, front should be value from 3 events ago = 2.0.
        match op.read_feature_with_sideband(&state, &state_sch, &sb) {
            FeatureValue::Float(f) => assert!((f - 2.0).abs() < 1e-9, "got {}", f),
            v => panic!("expected Float, got {:?}", v),
        }
    }

    #[test]
    fn lag_typed_missing_before_n_events() {
        let op = LagOpTyped {
            name: "l".into(),
            input_offset: 0,
            input_ty: FieldTy::F64,
            n: 3,
            size_offset: 0,
        };
        let state_sch = size_only_schema();
        let ev_sch = num_schema();
        let mut state = Row::zeroed(&state_sch);
        op.init_state(&state_sch, &mut state);
        let mut sb = SideBand::default();
        for v in [1.0, 2.0] {
            op.update_with_sideband(&mut state, &state_sch, &make_num_event(v), &ev_sch, &mut sb, SystemTime::now());
        }
        assert!(matches!(
            op.read_feature_with_sideband(&state, &state_sch, &sb),
            FeatureValue::Missing
        ));
    }

    #[test]
    fn firstn_typed_captures_first_n() {
        let op = FirstNOpTyped {
            name: "fn".into(),
            input_offset: 0,
            input_ty: FieldTy::F64,
            n: 3,
            size_offset: 0,
        };
        let state_sch = size_only_schema();
        let ev_sch = num_schema();
        let mut state = Row::zeroed(&state_sch);
        op.init_state(&state_sch, &mut state);
        let mut sb = SideBand::default();
        for v in [1.0, 2.0, 3.0, 4.0, 5.0] {
            op.update_with_sideband(&mut state, &state_sch, &make_num_event(v), &ev_sch, &mut sb, SystemTime::now());
        }
        match op.read_feature_with_sideband(&state, &state_sch, &sb) {
            FeatureValue::String(s) => {
                let parsed: Vec<f64> = serde_json::from_str(&s).unwrap();
                assert_eq!(parsed, vec![1.0, 2.0, 3.0]);
            }
            v => panic!("expected String JSON, got {:?}", v),
        }
    }

    #[test]
    fn lastn_typed_captures_last_n() {
        let op = LastNOpTyped {
            name: "ln".into(),
            input_offset: 0,
            input_ty: FieldTy::F64,
            n: 3,
            size_offset: 0,
        };
        let state_sch = size_only_schema();
        let ev_sch = num_schema();
        let mut state = Row::zeroed(&state_sch);
        op.init_state(&state_sch, &mut state);
        let mut sb = SideBand::default();
        for v in [1.0, 2.0, 3.0, 4.0, 5.0] {
            op.update_with_sideband(&mut state, &state_sch, &make_num_event(v), &ev_sch, &mut sb, SystemTime::now());
        }
        match op.read_feature_with_sideband(&state, &state_sch, &sb) {
            FeatureValue::String(s) => {
                let parsed: Vec<f64> = serde_json::from_str(&s).unwrap();
                assert_eq!(parsed, vec![3.0, 4.0, 5.0]);
            }
            v => panic!("expected String JSON, got {:?}", v),
        }
    }

    #[test]
    fn firstn_lastn_empty_is_missing() {
        let state_sch = size_only_schema();
        let sb = SideBand::default();
        let state = Row::zeroed(&state_sch);
        let fo = FirstNOpTyped { name: "fn".into(), input_offset: 0, input_ty: FieldTy::F64, n: 3, size_offset: 0 };
        let lo = LastNOpTyped { name: "ln".into(), input_offset: 0, input_ty: FieldTy::F64, n: 3, size_offset: 0 };
        assert!(matches!(fo.read_feature_with_sideband(&state, &state_sch, &sb), FeatureValue::Missing));
        assert!(matches!(lo.read_feature_with_sideband(&state, &state_sch, &sb), FeatureValue::Missing));
    }
}
