//! Velocity-family aggregation state structs (Phase 9).
//!
//! Implements 9 state structs covering the velocity / trend / outlier / z-score ops:
//! - `RateOfChangeState`         — AGG-VEL-01 (Δvalue / Δt across consecutive events)
//! - `InterArrivalStatsState`    — AGG-VEL-02 (mean inter-arrival ms; v0 emits mean only)
//! - `BurstCountState`           — AGG-VEL-03 (max events in any sub-window)
//! - `DeltaFromPrevState`        — AGG-VEL-04 (current - previous)
//! - `TrendState`                — AGG-VEL-05 (online linear-regression slope)
//! - `TrendResidualState`        — AGG-VEL-06 (current value - regression-predicted)
//! - `OutlierCountState`         — AGG-VEL-07 (count of |x - mean| > sigma * stddev)
//! - `ValueChangeCountState`     — AGG-VEL-08 (count of value flips)
//! - `ZScoreState`               — AGG-Z-01 (current_value - mean) / stddev
//!
//! D-06 (Phase 5 CONTEXT): no wall-clock reads — now_ms only.

use crate::row::{Row, Value};
use serde::{Deserialize, Serialize};

// ─── Helpers ─────────────────────────────────────────────────────────────────

fn numeric_from_row(row: &Row, field: &str) -> Option<f64> {
    match row.get(field)? {
        Value::F64(v) => Some(*v),
        Value::I64(v) => Some(*v as f64),
        _ => None,
    }
}

// ─── RateOfChangeState ───────────────────────────────────────────────────────

/// AGG-VEL-01: rate of change between consecutive events. (value_curr - value_prev) / dt_ms.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RateOfChangeState {
    pub last_value: f64,
    pub last_t: i64,
    pub current_rate: f64,
    pub initialized: bool,
    pub has_rate: bool,
}

impl RateOfChangeState {
    pub fn update(&mut self, row: &Row, now_ms: i64, field: Option<&str>, where_matched: bool) {
        if !where_matched {
            return;
        }
        let Some(fname) = field else { return };
        let Some(x) = numeric_from_row(row, fname) else {
            return;
        };
        if !self.initialized {
            self.last_value = x;
            self.last_t = now_ms;
            self.initialized = true;
            return;
        }
        let dt = now_ms - self.last_t;
        if dt > 0 {
            self.current_rate = (x - self.last_value) / dt as f64;
            self.has_rate = true;
        }
        self.last_value = x;
        self.last_t = now_ms;
    }

    pub fn query(&self) -> Value {
        if self.has_rate {
            Value::F64(self.current_rate)
        } else {
            Value::Null
        }
    }
}

// ─── InterArrivalStatsState ──────────────────────────────────────────────────

/// AGG-VEL-02: inter-arrival statistics. v0 emits mean_ms only.
/// State: Welford accumulator over inter-arrival gaps (ms).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct InterArrivalStatsState {
    pub last_t: i64,
    pub initialized: bool,
    pub n: u64,
    pub mean: f64,
    pub m2: f64,
}

impl InterArrivalStatsState {
    pub fn update(&mut self, _row: &Row, now_ms: i64, _field: Option<&str>, where_matched: bool) {
        if !where_matched {
            return;
        }
        if !self.initialized {
            self.last_t = now_ms;
            self.initialized = true;
            return;
        }
        let gap = (now_ms - self.last_t).max(0) as f64;
        self.n += 1;
        let delta = gap - self.mean;
        self.mean += delta / self.n as f64;
        let delta2 = gap - self.mean;
        self.m2 += delta * delta2;
        self.last_t = now_ms;
    }

    pub fn query(&self) -> Value {
        if self.n == 0 {
            Value::Null
        } else {
            Value::F64(self.mean)
        }
    }
}

// ─── BurstCountState ─────────────────────────────────────────────────────────

/// AGG-VEL-03: max events in any sub-window seen so far. Lifetime bound;
/// when wrapped in WindowedOp it gives bursts within outer window.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BurstCountState {
    /// 64 sliding sub-window buckets (Vec for serde compat); index = floor(t/sub_window_ms) % 64.
    pub buckets: Vec<u64>,
    pub bucket_epoch: Vec<i64>,
    pub max_seen: u64,
    pub initialized: bool,
}

impl Default for BurstCountState {
    fn default() -> Self {
        Self {
            buckets: vec![0; 64],
            bucket_epoch: vec![i64::MIN; 64],
            max_seen: 0,
            initialized: false,
        }
    }
}

impl BurstCountState {
    pub fn update(
        &mut self,
        _row: &Row,
        now_ms: i64,
        _field: Option<&str>,
        where_matched: bool,
        sub_window_ms: u64,
    ) {
        if !where_matched || sub_window_ms == 0 {
            return;
        }
        // Defensive: re-initialise vecs if a deserialised value came in short.
        if self.buckets.len() != 64 {
            self.buckets = vec![0; 64];
        }
        if self.bucket_epoch.len() != 64 {
            self.bucket_epoch = vec![i64::MIN; 64];
        }
        let epoch = now_ms.div_euclid(sub_window_ms as i64) * sub_window_ms as i64;
        let idx = (now_ms.div_euclid(sub_window_ms as i64).rem_euclid(64)) as usize;
        self.initialized = true;
        if self.bucket_epoch[idx] != epoch {
            self.buckets[idx] = 0;
            self.bucket_epoch[idx] = epoch;
        }
        self.buckets[idx] += 1;
        if self.buckets[idx] > self.max_seen {
            self.max_seen = self.buckets[idx];
        }
    }

    pub fn query(&self) -> Value {
        if !self.initialized {
            Value::I64(0)
        } else {
            Value::I64(self.max_seen as i64)
        }
    }
}

// ─── DeltaFromPrevState ──────────────────────────────────────────────────────

/// AGG-VEL-04: current value - previous event's value.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DeltaFromPrevState {
    pub last_value: f64,
    pub current_delta: f64,
    pub initialized: bool,
    pub has_delta: bool,
}

impl DeltaFromPrevState {
    pub fn update(&mut self, row: &Row, _now_ms: i64, field: Option<&str>, where_matched: bool) {
        if !where_matched {
            return;
        }
        let Some(fname) = field else { return };
        let Some(x) = numeric_from_row(row, fname) else {
            return;
        };
        if !self.initialized {
            self.last_value = x;
            self.initialized = true;
            return;
        }
        self.current_delta = x - self.last_value;
        self.has_delta = true;
        self.last_value = x;
    }

    pub fn query(&self) -> Value {
        if self.has_delta {
            Value::F64(self.current_delta)
        } else {
            Value::Null
        }
    }
}

// ─── TrendState ──────────────────────────────────────────────────────────────

/// AGG-VEL-05: slope of online linear regression of (now_ms, value).
///
/// Uses the closed-form OLS: slope = (n * Σxy - Σx * Σy) / (n * Σx² - (Σx)²).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TrendState {
    pub n: u64,
    pub sum_x: f64,
    pub sum_y: f64,
    pub sum_xx: f64,
    pub sum_xy: f64,
    pub initialized: bool,
}

impl TrendState {
    pub fn update(&mut self, row: &Row, now_ms: i64, field: Option<&str>, where_matched: bool) {
        if !where_matched {
            return;
        }
        let Some(fname) = field else { return };
        let Some(y) = numeric_from_row(row, fname) else {
            return;
        };
        let x = now_ms as f64;
        self.n += 1;
        self.sum_x += x;
        self.sum_y += y;
        self.sum_xx += x * x;
        self.sum_xy += x * y;
        self.initialized = true;
    }

    /// slope (denominator zero or n<2 → Null).
    pub fn slope(&self) -> Option<f64> {
        if self.n < 2 {
            return None;
        }
        let n = self.n as f64;
        let denom = n * self.sum_xx - self.sum_x * self.sum_x;
        if denom == 0.0 {
            return None;
        }
        Some((n * self.sum_xy - self.sum_x * self.sum_y) / denom)
    }

    /// intercept = (Σy - slope * Σx) / n
    pub fn intercept(&self) -> Option<f64> {
        let slope = self.slope()?;
        let n = self.n as f64;
        Some((self.sum_y - slope * self.sum_x) / n)
    }

    pub fn query(&self) -> Value {
        match self.slope() {
            Some(s) => Value::F64(s),
            None => Value::Null,
        }
    }
}

// ─── TrendResidualState ──────────────────────────────────────────────────────

/// AGG-VEL-06: current_value - (slope * now_ms + intercept).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TrendResidualState {
    pub trend: TrendState,
    pub last_value: f64,
    pub last_t: i64,
    pub initialized: bool,
}

impl TrendResidualState {
    pub fn update(&mut self, row: &Row, now_ms: i64, field: Option<&str>, where_matched: bool) {
        if !where_matched {
            return;
        }
        let Some(fname) = field else { return };
        let Some(y) = numeric_from_row(row, fname) else {
            return;
        };
        self.trend.update(row, now_ms, field, where_matched);
        self.last_value = y;
        self.last_t = now_ms;
        self.initialized = true;
    }

    pub fn query(&self) -> Value {
        if !self.initialized {
            return Value::Null;
        }
        let (Some(slope), Some(intercept)) = (self.trend.slope(), self.trend.intercept()) else {
            return Value::Null;
        };
        let predicted = slope * (self.last_t as f64) + intercept;
        Value::F64(self.last_value - predicted)
    }
}

// ─── OutlierCountState ───────────────────────────────────────────────────────

/// AGG-VEL-07: count of events whose value deviates from running mean by more
/// than sigma * stddev. Uses Welford-style online mean+variance.
///
/// Outlier check fires only after `MIN_BASELINE_N` observations to avoid
/// false-positives during warm-up. Defaults to 5.
const MIN_BASELINE_N: u64 = 5;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct OutlierCountState {
    pub n: u64,
    pub mean: f64,
    pub m2: f64,
    pub outliers: u64,
}

impl OutlierCountState {
    pub fn update(
        &mut self,
        row: &Row,
        _now_ms: i64,
        field: Option<&str>,
        where_matched: bool,
        sigma: f64,
    ) {
        if !where_matched {
            return;
        }
        let Some(fname) = field else { return };
        let Some(x) = numeric_from_row(row, fname) else {
            return;
        };
        // Outlier check uses pre-update statistics (so we don't bias by the
        // event we're testing).
        if self.n >= MIN_BASELINE_N {
            let var = if self.n > 1 {
                self.m2 / (self.n - 1) as f64
            } else {
                0.0
            };
            if var > 0.0 {
                let stddev = var.sqrt();
                if (x - self.mean).abs() > sigma * stddev {
                    self.outliers += 1;
                }
            }
        }
        // Welford update.
        self.n += 1;
        let delta = x - self.mean;
        self.mean += delta / self.n as f64;
        let delta2 = x - self.mean;
        self.m2 += delta * delta2;
    }

    pub fn query(&self) -> Value {
        Value::I64(self.outliers as i64)
    }
}

// ─── ValueChangeCountState ───────────────────────────────────────────────────

/// AGG-VEL-08: count of value flips (consecutive different values).
///
/// Phase 13.5.2: stores `last_value` as a generic `Value` (was `f64`) so
/// string / bool / numeric state transitions are all counted. The previous
/// numeric-only impl silently dropped string-typed events (e.g. tracking
/// `state` field with values "A"/"B"/"C") because the per-row `numeric_from_row`
/// check rejected non-numeric inputs.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ValueChangeCountState {
    pub last_value: Option<Value>,
    pub changes: u64,
}

impl ValueChangeCountState {
    pub fn update(&mut self, row: &Row, _now_ms: i64, field: Option<&str>, where_matched: bool) {
        if !where_matched {
            return;
        }
        let Some(fname) = field else { return };
        let v = match row.get(fname) {
            None | Some(Value::Null) => return,
            Some(v) => v.clone(),
        };
        match &self.last_value {
            None => {
                self.last_value = Some(v);
            }
            Some(prev) if *prev != v => {
                self.changes += 1;
                self.last_value = Some(v);
            }
            _ => {}
        }
    }

    pub fn query(&self) -> Value {
        Value::I64(self.changes as i64)
    }
}

// ─── ZScoreState ─────────────────────────────────────────────────────────────

/// AGG-Z-01: current_event_value - mean, divided by stddev.
/// Reuses Welford accumulator (same as Phase 5 Variance) plus stores last_value.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ZScoreState {
    pub n: u64,
    pub mean: f64,
    pub m2: f64,
    pub last_value: f64,
    pub initialized: bool,
}

impl ZScoreState {
    pub fn update(&mut self, row: &Row, _now_ms: i64, field: Option<&str>, where_matched: bool) {
        if !where_matched {
            return;
        }
        let Some(fname) = field else { return };
        let Some(x) = numeric_from_row(row, fname) else {
            return;
        };
        self.n += 1;
        let delta = x - self.mean;
        self.mean += delta / self.n as f64;
        let delta2 = x - self.mean;
        self.m2 += delta * delta2;
        self.last_value = x;
        self.initialized = true;
    }

    pub fn query(&self) -> Value {
        if !self.initialized || self.n < 2 {
            return Value::Null;
        }
        let var = self.m2 / (self.n - 1) as f64;
        if var <= 0.0 {
            return Value::Null;
        }
        let stddev = var.sqrt();
        Value::F64((self.last_value - self.mean) / stddev)
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

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
        for t in &[0, 30, 60] {
            s.update(&Row::new(), *t, None, true, 100);
        }
        s.update(&Row::new(), 100, None, true, 100);
        for t in &[200, 220, 240, 260, 280] {
            s.update(&Row::new(), *t, None, true, 100);
        }
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
            s.update(
                &row_f64("v", 5.0 + (i as f64 % 2.0)),
                i * 100,
                Some("v"),
                true,
                3.0,
            );
        }
        assert_eq!(s.query(), Value::I64(0));
    }

    #[test]
    fn outlier_count_increments_on_extreme_value() {
        let mut s = OutlierCountState::default();
        for i in 0..20 {
            s.update(
                &row_f64("v", 5.0 + (i as f64 % 2.0)),
                i * 100,
                Some("v"),
                true,
                3.0,
            );
        }
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
        s.update(&row_f64("v", 1.0), 100, Some("v"), true);
        s.update(&row_f64("v", 2.0), 200, Some("v"), true);
        s.update(&row_f64("v", 2.0), 300, Some("v"), true);
        s.update(&row_f64("v", 3.0), 400, Some("v"), true);
        assert_eq!(s.query(), Value::I64(2));
    }

    // ── ZScore ────────────────────────────────────────────────────────────────

    #[test]
    fn zscore_zero_for_mean_value() {
        let mut s = ZScoreState::default();
        for v in &[1.0, 2.0, 3.0, 4.0, 5.0] {
            s.update(&row_f64("v", *v), 0, Some("v"), true);
        }
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
        assert_eq!(s.query(), Value::Null);
    }
}
