//! Decay-family aggregation state structs (Phase 9).
//!
//! RED commit (Phase 9 plan 01 task 1.a): tests defined; implementation stubs
//! return defaults so test runs fail. Plan 01 task 1.b will fill the impls.

use crate::row::{Row, Value};
use serde::{Deserialize, Serialize};

// Stubs; tests will fail until plan 01 task 1.b implements these.

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct EwmaState;

impl EwmaState {
    pub fn update(
        &mut self,
        _row: &Row,
        _event_time_ms: i64,
        _field: Option<&str>,
        _where_matched: bool,
        _half_life_ms: u64,
    ) {
    }
    pub fn query(&self) -> Value {
        Value::Null
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct EwVarState;

impl EwVarState {
    pub fn update(
        &mut self,
        _row: &Row,
        _event_time_ms: i64,
        _field: Option<&str>,
        _where_matched: bool,
        _half_life_ms: u64,
    ) {
    }
    pub fn query_variance(&self) -> Value {
        Value::Null
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct EwZScoreState;

impl EwZScoreState {
    pub fn update(
        &mut self,
        _row: &Row,
        _event_time_ms: i64,
        _field: Option<&str>,
        _where_matched: bool,
        _half_life_ms: u64,
    ) {
    }
    pub fn query(&self) -> Value {
        Value::Null
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DecayedSumState;

impl DecayedSumState {
    pub fn update(
        &mut self,
        _row: &Row,
        _event_time_ms: i64,
        _field: Option<&str>,
        _where_matched: bool,
        _half_life_ms: u64,
    ) {
    }
    pub fn query(&self) -> Value {
        Value::Null
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DecayedCountState;

impl DecayedCountState {
    pub fn update(
        &mut self,
        _row: &Row,
        _event_time_ms: i64,
        _field: Option<&str>,
        _where_matched: bool,
        _half_life_ms: u64,
    ) {
    }
    pub fn query(&self) -> Value {
        Value::Null
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TwaState;

impl TwaState {
    pub fn update(
        &mut self,
        _row: &Row,
        _event_time_ms: i64,
        _field: Option<&str>,
        _where_matched: bool,
    ) {
    }
    pub fn query(&self) -> Value {
        Value::Null
    }
}

// ─── Tests (RED) ─────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn row_f64(field: &str, v: f64) -> Row {
        Row::new().with_field(field, Value::F64(v))
    }

    #[test]
    fn ewma_first_event_seeds_value() {
        let mut s = EwmaState::default();
        s.update(&row_f64("x", 10.0), 0, Some("x"), true, 1000);
        assert_eq!(s.query(), Value::F64(10.0));
    }

    #[test]
    fn ewma_dt_equals_half_life_yields_alpha_half() {
        let mut s = EwmaState::default();
        s.update(&row_f64("x", 0.0), 0, Some("x"), true, 1000);
        s.update(&row_f64("x", 100.0), 1000, Some("x"), true, 1000);
        match s.query() {
            Value::F64(v) => assert!((v - 50.0).abs() < 1e-9, "expected ~50.0, got {v}"),
            other => panic!("expected F64, got {:?}", other),
        }
    }

    #[test]
    fn ewma_unset_when_no_events() {
        let s = EwmaState::default();
        assert_eq!(s.query(), Value::Null);
    }

    #[test]
    fn ewma_skips_when_predicate_false() {
        let mut s = EwmaState::default();
        s.update(&row_f64("x", 10.0), 0, Some("x"), false, 1000);
        assert_eq!(s.query(), Value::Null);
    }

    #[test]
    fn ewvar_constant_stream_yields_zero_variance() {
        let mut s = EwVarState::default();
        for i in 0..10 {
            s.update(&row_f64("x", 5.0), i * 100, Some("x"), true, 500);
        }
        match s.query_variance() {
            Value::F64(v) => assert!(v.abs() < 1e-6, "constant stream variance = {v}"),
            other => panic!("expected F64, got {:?}", other),
        }
    }

    #[test]
    fn ewvar_responds_to_step_change() {
        let mut s = EwVarState::default();
        for i in 0..5 {
            s.update(&row_f64("x", 1.0), i * 100, Some("x"), true, 500);
        }
        let v0 = match s.query_variance() {
            Value::F64(v) => v,
            _ => panic!(),
        };
        s.update(&row_f64("x", 100.0), 600, Some("x"), true, 500);
        let v1 = match s.query_variance() {
            Value::F64(v) => v,
            _ => panic!(),
        };
        assert!(v1 > v0 + 100.0, "variance should grow: v0={v0}, v1={v1}");
    }

    #[test]
    fn ew_zscore_zero_when_value_equals_mean() {
        let mut s = EwZScoreState::default();
        for i in 0..3 {
            s.update(&row_f64("x", 10.0), i * 100, Some("x"), true, 500);
        }
        assert_eq!(s.query(), Value::Null);
    }

    #[test]
    fn ew_zscore_positive_for_value_above_mean() {
        let mut s = EwZScoreState::default();
        s.update(&row_f64("x", 0.0), 0, Some("x"), true, 500);
        s.update(&row_f64("x", 0.0), 100, Some("x"), true, 500);
        s.update(&row_f64("x", 100.0), 200, Some("x"), true, 500);
        match s.query() {
            Value::F64(v) => assert!(v > 0.0, "expected positive z, got {v}"),
            _ => panic!("expected F64"),
        }
    }

    #[test]
    fn decayed_sum_first_event_is_value() {
        let mut s = DecayedSumState::default();
        s.update(&row_f64("amt", 10.0), 0, Some("amt"), true, 1000);
        assert_eq!(s.query(), Value::F64(10.0));
    }

    #[test]
    fn decayed_sum_decays_old_then_adds_new() {
        let mut s = DecayedSumState::default();
        s.update(&row_f64("amt", 100.0), 0, Some("amt"), true, 1000);
        s.update(&row_f64("amt", 50.0), 1000, Some("amt"), true, 1000);
        match s.query() {
            Value::F64(v) => assert!((v - 100.0).abs() < 1e-9, "got {v}"),
            _ => panic!(),
        }
    }

    #[test]
    fn decayed_count_first_event_is_one() {
        let mut s = DecayedCountState::default();
        s.update(&Row::new(), 0, None, true, 1000);
        assert_eq!(s.query(), Value::F64(1.0));
    }

    #[test]
    fn decayed_count_decays_then_adds() {
        let mut s = DecayedCountState::default();
        s.update(&Row::new(), 0, None, true, 1000);
        s.update(&Row::new(), 1000, None, true, 1000);
        match s.query() {
            Value::F64(v) => assert!((v - 1.5).abs() < 1e-9, "got {v}"),
            _ => panic!(),
        }
    }

    #[test]
    fn twa_single_event_returns_value() {
        let mut s = TwaState::default();
        s.update(&row_f64("g", 5.0), 0, Some("g"), true);
        assert_eq!(s.query(), Value::F64(5.0));
    }

    #[test]
    fn twa_step_function() {
        let mut s = TwaState::default();
        s.update(&row_f64("g", 10.0), 0, Some("g"), true);
        s.update(&row_f64("g", 20.0), 100, Some("g"), true);
        s.update(&row_f64("g", 30.0), 300, Some("g"), true);
        match s.query() {
            Value::F64(v) => assert!((v - 5000.0 / 300.0).abs() < 1e-6, "got {v}"),
            _ => panic!(),
        }
    }
}
