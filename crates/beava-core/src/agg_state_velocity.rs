//! Velocity-family aggregation state structs (Phase 9).
//!
//! RED commit (Phase 9 plan 01 task 2.a): tests defined; impls are stubs.

use crate::row::{Row, Value};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RateOfChangeState;

impl RateOfChangeState {
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

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct InterArrivalStatsState;

impl InterArrivalStatsState {
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

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct BurstCountState;

impl BurstCountState {
    pub fn update(
        &mut self,
        _row: &Row,
        _event_time_ms: i64,
        _field: Option<&str>,
        _where_matched: bool,
        _sub_window_ms: u64,
    ) {
    }
    pub fn query(&self) -> Value {
        Value::Null
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DeltaFromPrevState;

impl DeltaFromPrevState {
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

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TrendState;

impl TrendState {
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

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TrendResidualState;

impl TrendResidualState {
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

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct OutlierCountState;

impl OutlierCountState {
    pub fn update(
        &mut self,
        _row: &Row,
        _event_time_ms: i64,
        _field: Option<&str>,
        _where_matched: bool,
        _sigma: f64,
    ) {
    }
    pub fn query(&self) -> Value {
        Value::Null
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ValueChangeCountState;

impl ValueChangeCountState {
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

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ZScoreState;

impl ZScoreState {
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

    // ── RateOfChange ──────────────────────────────────────────────────────────

    #[test]
    fn rate_of_change_two_events_yields_dvalue_per_dt() {
        let mut s = RateOfChangeState::default();
        s.update(&row_f64("v", 10.0), 0, Some("v"), true);
        s.update(&row_f64("v", 20.0), 1000, Some("v"), true);
        // (20-10) / 1000 ms = 0.01 per ms
        match s.query() {
            Value::F64(v) => assert!((v - 0.01).abs() < 1e-9, "got {v}"),
            other => panic!("expected F64, got {:?}", other),
        }
    }

    #[test]
    fn rate_of_change_single_event_is_null() {
        let mut s = RateOfChangeState::default();
        s.update(&row_f64("v", 10.0), 0, Some("v"), true);
        assert_eq!(s.query(), Value::Null);
    }

    // ── InterArrivalStats ─────────────────────────────────────────────────────

    #[test]
    fn inter_arrival_mean_with_uniform_gaps() {
        let mut s = InterArrivalStatsState::default();
        for i in 0..5 {
            s.update(&Row::new(), i * 100, None, true);
        }
        // Gaps: 100, 100, 100, 100 → mean=100
        match s.query() {
            Value::F64(v) => assert!((v - 100.0).abs() < 1e-9, "got {v}"),
            other => panic!("expected F64, got {:?}", other),
        }
    }

    #[test]
    fn inter_arrival_one_event_is_null() {
        let mut s = InterArrivalStatsState::default();
        s.update(&Row::new(), 0, None, true);
        assert_eq!(s.query(), Value::Null);
    }

    // ── BurstCount ────────────────────────────────────────────────────────────

    #[test]
    fn burst_count_max_in_subwindow() {
        let mut s = BurstCountState::default();
        // Sub-window = 100 ms. Insert 3 events in [0,100), 1 in [100,200), 5 in [200,300).
        for t in &[0, 30, 60] {
            s.update(&Row::new(), *t, None, true, 100);
        }
        for t in &[100] {
            s.update(&Row::new(), *t, None, true, 100);
        }
        for t in &[200, 220, 240, 260, 280] {
            s.update(&Row::new(), *t, None, true, 100);
        }
        // Max = 5
        assert_eq!(s.query(), Value::I64(5));
    }

    // ── DeltaFromPrev ─────────────────────────────────────────────────────────

    #[test]
    fn delta_from_prev_yields_diff() {
        let mut s = DeltaFromPrevState::default();
        s.update(&row_f64("v", 10.0), 0, Some("v"), true);
        s.update(&row_f64("v", 25.0), 100, Some("v"), true);
        match s.query() {
            Value::F64(v) => assert!((v - 15.0).abs() < 1e-9, "got {v}"),
            other => panic!("expected F64, got {:?}", other),
        }
    }

    #[test]
    fn delta_from_prev_single_event_is_null() {
        let mut s = DeltaFromPrevState::default();
        s.update(&row_f64("v", 10.0), 0, Some("v"), true);
        assert_eq!(s.query(), Value::Null);
    }

    // ── Trend ──────────────────────────────────────────────────────────────────

    #[test]
    fn trend_constant_stream_zero_slope() {
        let mut s = TrendState::default();
        for i in 0..5 {
            s.update(&row_f64("v", 5.0), i * 100, Some("v"), true);
        }
        match s.query() {
            Value::F64(v) => assert!(v.abs() < 1e-6, "expected ~0, got {v}"),
            other => panic!("expected F64, got {:?}", other),
        }
    }

    #[test]
    fn trend_linear_stream_positive_slope() {
        let mut s = TrendState::default();
        for i in 0..5 {
            s.update(&row_f64("v", i as f64), (i as i64) * 100, Some("v"), true);
        }
        // y = i, x = i*100 → slope = 1/100 = 0.01
        match s.query() {
            Value::F64(v) => assert!(v > 0.0, "expected positive slope, got {v}"),
            _ => panic!("expected F64"),
        }
    }

    // ── TrendResidual ─────────────────────────────────────────────────────────

    #[test]
    fn trend_residual_zero_when_value_on_trend_line() {
        let mut s = TrendResidualState::default();
        for i in 0..5 {
            s.update(&row_f64("v", i as f64), (i as i64) * 100, Some("v"), true);
        }
        // The current value (4) sits exactly on the trend line; residual ≈ 0
        match s.query() {
            Value::F64(v) => assert!(v.abs() < 1e-6, "expected ~0 residual, got {v}"),
            other => panic!("expected F64, got {:?}", other),
        }
    }

    // ── OutlierCount ──────────────────────────────────────────────────────────

    #[test]
    fn outlier_count_zero_for_steady_stream() {
        let mut s = OutlierCountState::default();
        for i in 0..20 {
            s.update(&row_f64("v", 5.0 + (i as f64 % 2.0)), i * 100, Some("v"), true, 3.0);
        }
        assert_eq!(s.query(), Value::I64(0));
    }

    #[test]
    fn outlier_count_increments_on_extreme_value() {
        let mut s = OutlierCountState::default();
        // Build up baseline (no outliers expected during build-up)
        for i in 0..20 {
            s.update(&row_f64("v", 5.0 + (i as f64 % 2.0)), i * 100, Some("v"), true, 3.0);
        }
        // Big spike — should be > mean + 3*stddev
        s.update(&row_f64("v", 100.0), 2000, Some("v"), true, 3.0);
        match s.query() {
            Value::I64(n) => assert!(n >= 1, "expected ≥1 outlier, got {n}"),
            other => panic!("expected I64, got {:?}", other),
        }
    }

    // ── ValueChangeCount ──────────────────────────────────────────────────────

    #[test]
    fn value_change_count_increments_on_flips() {
        let mut s = ValueChangeCountState::default();
        s.update(&row_f64("v", 1.0), 0, Some("v"), true);
        s.update(&row_f64("v", 1.0), 100, Some("v"), true); // no change
        s.update(&row_f64("v", 2.0), 200, Some("v"), true); // change
        s.update(&row_f64("v", 2.0), 300, Some("v"), true); // no change
        s.update(&row_f64("v", 3.0), 400, Some("v"), true); // change
        assert_eq!(s.query(), Value::I64(2));
    }

    // ── ZScore ────────────────────────────────────────────────────────────────

    #[test]
    fn zscore_zero_for_mean_value() {
        let mut s = ZScoreState::default();
        for v in &[1.0, 2.0, 3.0, 4.0, 5.0] {
            s.update(&row_f64("v", *v), 0, Some("v"), true);
        }
        // Last value (5) above mean (3); z should be > 0
        match s.query() {
            Value::F64(z) => assert!(z > 0.0, "expected z > 0, got {z}"),
            other => panic!("expected F64, got {:?}", other),
        }
    }

    #[test]
    fn zscore_null_for_constant_stream() {
        let mut s = ZScoreState::default();
        for _ in 0..5 {
            s.update(&row_f64("v", 5.0), 0, Some("v"), true);
        }
        // stddev=0 → null
        assert_eq!(s.query(), Value::Null);
    }
}
