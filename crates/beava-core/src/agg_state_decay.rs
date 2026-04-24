//! Decay-family aggregation state structs (Phase 9).
//!
//! Implements 6 state structs:
//! - `EwmaState`            — AGG-DECAY-01: exponentially-weighted moving average
//! - `EwVarState`           — AGG-DECAY-02: exponentially-weighted variance
//! - `EwZScoreState`        — AGG-DECAY-03: current event z-score against EW baseline
//! - `DecayedSumState`      — AGG-DECAY-04: forward-decay sum (Cormode)
//! - `DecayedCountState`    — AGG-DECAY-05: forward-decay count
//! - `TwaState`             — AGG-DECAY-06: time-weighted average
//!
//! Each state persists (running_value, last_event_time_ms). On each event, the
//! decay coefficient α = 1 - exp(-Δt / half_life_ms) is applied to integrate the
//! new event into the running statistic. Late events (Δt < 0) keep prior state.
//!
//! D-06 (from Phase 5 CONTEXT): no wall-clock reads — event_time_ms only.

use crate::row::{Row, Value};
use serde::{Deserialize, Serialize};

// ─── Helpers ─────────────────────────────────────────────────────────────────

/// Extract numeric (F64 / I64) from a row field.
/// Returns None for absent / Null / non-numeric values.
fn numeric_from_row(row: &Row, field: &str) -> Option<f64> {
    match row.get(field)? {
        Value::F64(v) => Some(*v),
        Value::I64(v) => Some(*v as f64),
        _ => None,
    }
}

/// Compute α = 1 - exp(-Δt / half_life_ms) given event/last/half_life in ms.
///
/// Returns None for Δt <= 0 (late or duplicate event — caller should keep prior
/// state) or half_life_ms == 0 (caller's bug — should be rejected at register
/// time but defensively returns None).
#[inline]
fn decay_alpha(event_time_ms: i64, last_event_time_ms: i64, half_life_ms: u64) -> Option<f64> {
    if half_life_ms == 0 {
        return None;
    }
    let dt = event_time_ms.saturating_sub(last_event_time_ms);
    if dt <= 0 {
        return None;
    }
    // α = 1 - exp(-Δt * ln(2) / half_life)  (proper half-life convention)
    let lambda = std::f64::consts::LN_2 / half_life_ms as f64;
    Some(1.0 - (-(dt as f64) * lambda).exp())
}

/// Compute the multiplicative-decay factor exp(-Δt * ln(2) / half_life) used by
/// forward-decay (Cormode-style) sum/count: each prior contribution decays by
/// this factor on each event observation.
#[inline]
fn decay_factor(event_time_ms: i64, last_event_time_ms: i64, half_life_ms: u64) -> f64 {
    if half_life_ms == 0 {
        return 1.0;
    }
    let dt = event_time_ms.saturating_sub(last_event_time_ms).max(0) as f64;
    let lambda = std::f64::consts::LN_2 / half_life_ms as f64;
    (-dt * lambda).exp()
}

// ─── EwmaState ───────────────────────────────────────────────────────────────

/// AGG-DECAY-01: Exponentially-weighted moving average.
///
/// state.value = α * x + (1 - α) * state.value
/// where α = 1 - exp(-Δt * ln(2) / half_life).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct EwmaState {
    pub value: f64,
    pub last_event_time_ms: i64,
    /// True after the first observation.
    pub initialized: bool,
}

impl EwmaState {
    pub fn update(
        &mut self,
        row: &Row,
        event_time_ms: i64,
        field: Option<&str>,
        where_matched: bool,
        half_life_ms: u64,
    ) {
        if !where_matched {
            return;
        }
        let Some(fname) = field else { return };
        let Some(x) = numeric_from_row(row, fname) else {
            return;
        };
        if !self.initialized {
            self.value = x;
            self.last_event_time_ms = event_time_ms;
            self.initialized = true;
            return;
        }
        match decay_alpha(event_time_ms, self.last_event_time_ms, half_life_ms) {
            Some(alpha) => {
                self.value = alpha * x + (1.0 - alpha) * self.value;
                self.last_event_time_ms = event_time_ms;
            }
            None => {
                // Late event: don't move time backwards. Apply unweighted blend
                // (treat as same-instant observation).
                self.value = 0.5 * x + 0.5 * self.value;
            }
        }
    }

    pub fn query(&self) -> Value {
        if self.initialized {
            Value::F64(self.value)
        } else {
            Value::Null
        }
    }
}

// ─── EwVarState ──────────────────────────────────────────────────────────────

/// AGG-DECAY-02: Exponentially-weighted variance.
///
/// Maintains EWMA of value (mean) and EWMA of squared deviation (variance)
/// using a decay-adapted Welford. variance = m2.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct EwVarState {
    pub mean: f64,
    pub m2: f64,
    pub last_event_time_ms: i64,
    pub initialized: bool,
    pub last_value: f64,
}

impl EwVarState {
    pub fn update(
        &mut self,
        row: &Row,
        event_time_ms: i64,
        field: Option<&str>,
        where_matched: bool,
        half_life_ms: u64,
    ) {
        if !where_matched {
            return;
        }
        let Some(fname) = field else { return };
        let Some(x) = numeric_from_row(row, fname) else {
            return;
        };
        if !self.initialized {
            self.mean = x;
            self.m2 = 0.0;
            self.last_event_time_ms = event_time_ms;
            self.last_value = x;
            self.initialized = true;
            return;
        }
        let alpha =
            decay_alpha(event_time_ms, self.last_event_time_ms, half_life_ms).unwrap_or(0.0);
        // EW Welford-style: update mean, then m2 against new mean.
        let delta = x - self.mean;
        let new_mean = self.mean + alpha * delta;
        let delta2 = x - new_mean;
        self.m2 = (1.0 - alpha) * self.m2 + alpha * delta * delta2;
        self.mean = new_mean;
        if alpha > 0.0 {
            self.last_event_time_ms = event_time_ms;
        }
        self.last_value = x;
    }

    pub fn query_variance(&self) -> Value {
        if self.initialized {
            Value::F64(self.m2.max(0.0))
        } else {
            Value::Null
        }
    }
}

// ─── EwZScoreState ───────────────────────────────────────────────────────────

/// AGG-DECAY-03: current event z-score against EW baseline.
///
/// Wraps an EwVarState and emits (last_value - mean) / sqrt(variance).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct EwZScoreState {
    pub inner: EwVarState,
}

impl EwZScoreState {
    pub fn update(
        &mut self,
        row: &Row,
        event_time_ms: i64,
        field: Option<&str>,
        where_matched: bool,
        half_life_ms: u64,
    ) {
        self.inner
            .update(row, event_time_ms, field, where_matched, half_life_ms);
    }

    pub fn query(&self) -> Value {
        if !self.inner.initialized {
            return Value::Null;
        }
        let var = self.inner.m2.max(0.0);
        if var == 0.0 {
            return Value::Null;
        }
        let stddev = var.sqrt();
        Value::F64((self.inner.last_value - self.inner.mean) / stddev)
    }
}

// ─── DecayedSumState ─────────────────────────────────────────────────────────

/// AGG-DECAY-04: forward-decay sum (Cormode).
///
/// On each event: total = total * exp(-Δt * ln(2) / half_life) + x
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DecayedSumState {
    pub total: f64,
    pub last_event_time_ms: i64,
    pub initialized: bool,
}

impl DecayedSumState {
    pub fn update(
        &mut self,
        row: &Row,
        event_time_ms: i64,
        field: Option<&str>,
        where_matched: bool,
        half_life_ms: u64,
    ) {
        if !where_matched {
            return;
        }
        let Some(fname) = field else { return };
        let Some(x) = numeric_from_row(row, fname) else {
            return;
        };
        if !self.initialized {
            self.total = x;
            self.last_event_time_ms = event_time_ms;
            self.initialized = true;
            return;
        }
        let factor = decay_factor(event_time_ms, self.last_event_time_ms, half_life_ms);
        self.total = self.total * factor + x;
        if event_time_ms > self.last_event_time_ms {
            self.last_event_time_ms = event_time_ms;
        }
    }

    pub fn query(&self) -> Value {
        if self.initialized {
            Value::F64(self.total)
        } else {
            Value::Null
        }
    }
}

// ─── DecayedCountState ───────────────────────────────────────────────────────

/// AGG-DECAY-05: forward-decay count.
///
/// On each event: total = total * exp(-Δt * ln(2) / half_life) + 1
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DecayedCountState {
    pub total: f64,
    pub last_event_time_ms: i64,
    pub initialized: bool,
}

impl DecayedCountState {
    pub fn update(
        &mut self,
        _row: &Row,
        event_time_ms: i64,
        _field: Option<&str>,
        where_matched: bool,
        half_life_ms: u64,
    ) {
        if !where_matched {
            return;
        }
        if !self.initialized {
            self.total = 1.0;
            self.last_event_time_ms = event_time_ms;
            self.initialized = true;
            return;
        }
        let factor = decay_factor(event_time_ms, self.last_event_time_ms, half_life_ms);
        self.total = self.total * factor + 1.0;
        if event_time_ms > self.last_event_time_ms {
            self.last_event_time_ms = event_time_ms;
        }
    }

    pub fn query(&self) -> Value {
        if self.initialized {
            Value::F64(self.total)
        } else {
            Value::Null
        }
    }
}

// ─── TwaState ────────────────────────────────────────────────────────────────

/// AGG-DECAY-06: Time-weighted average for irregularly-sampled gauge fields.
///
/// State: (sum_v_dt, sum_dt, last_v, last_t). On event:
///   if initialized: sum_v_dt += last_v * (now - last_t); sum_dt += (now - last_t)
///   set last_v = x, last_t = now.
///
/// query(): sum_v_dt / sum_dt, or last_v if sum_dt == 0 (single point).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TwaState {
    pub sum_v_dt: f64,
    pub sum_dt: f64,
    pub last_v: f64,
    pub last_t: i64,
    pub initialized: bool,
}

impl TwaState {
    pub fn update(
        &mut self,
        row: &Row,
        event_time_ms: i64,
        field: Option<&str>,
        where_matched: bool,
    ) {
        if !where_matched {
            return;
        }
        let Some(fname) = field else { return };
        let Some(x) = numeric_from_row(row, fname) else {
            return;
        };
        if !self.initialized {
            self.last_v = x;
            self.last_t = event_time_ms;
            self.initialized = true;
            return;
        }
        let dt = (event_time_ms - self.last_t).max(0) as f64;
        if dt > 0.0 {
            self.sum_v_dt += self.last_v * dt;
            self.sum_dt += dt;
        }
        self.last_v = x;
        self.last_t = event_time_ms;
    }

    pub fn query(&self) -> Value {
        if !self.initialized {
            return Value::Null;
        }
        if self.sum_dt == 0.0 {
            return Value::F64(self.last_v);
        }
        Value::F64(self.sum_v_dt / self.sum_dt)
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn row_f64(field: &str, v: f64) -> Row {
        Row::new().with_field(field, Value::F64(v))
    }

    // ── EWMA ──────────────────────────────────────────────────────────────────

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
        // α = 1 - exp(-1000 * ln2 / 1000) = 1 - 0.5 = 0.5
        s.update(&row_f64("x", 100.0), 1000, Some("x"), true, 1000);
        match s.query() {
            Value::F64(v) => {
                assert!((v - 50.0).abs() < 1e-9, "expected ~50.0, got {v}");
            }
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

    // ── EW Variance ───────────────────────────────────────────────────────────

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
        // Big jump
        s.update(&row_f64("x", 100.0), 600, Some("x"), true, 500);
        let v1 = match s.query_variance() {
            Value::F64(v) => v,
            _ => panic!(),
        };
        assert!(v1 > v0 + 100.0, "variance should grow: v0={v0}, v1={v1}");
    }

    // ── EW Z-Score ────────────────────────────────────────────────────────────

    #[test]
    fn ew_zscore_zero_when_value_equals_mean() {
        let mut s = EwZScoreState::default();
        for i in 0..3 {
            s.update(&row_f64("x", 10.0), i * 100, Some("x"), true, 500);
        }
        // Variance is 0 → null
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

    // ── Decayed Sum / Count ───────────────────────────────────────────────────

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
        // After 1 half_life, factor = 0.5; total = 50 + 50 = 100
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
        // 1 * 0.5 + 1 = 1.5
        match s.query() {
            Value::F64(v) => assert!((v - 1.5).abs() < 1e-9, "got {v}"),
            _ => panic!(),
        }
    }

    // ── TWA ───────────────────────────────────────────────────────────────────

    #[test]
    fn twa_single_event_returns_value() {
        let mut s = TwaState::default();
        s.update(&row_f64("g", 5.0), 0, Some("g"), true);
        assert_eq!(s.query(), Value::F64(5.0));
    }

    #[test]
    fn twa_step_function() {
        // 10 for 100ms, then 20 for 200ms → time-weighted = (10*100 + 20*200) / 300 = 5000/300 ≈ 16.667
        // After 3 events at t=0,100,300:
        //   between 0 and 100: value=10; sum_v_dt += 10*100 = 1000; sum_dt += 100
        //   between 100 and 300: value=20; sum_v_dt += 20*200 = 4000; sum_dt += 200
        // After third event sum_v_dt=5000, sum_dt=300 → 16.667
        let mut s = TwaState::default();
        s.update(&row_f64("g", 10.0), 0, Some("g"), true);
        s.update(&row_f64("g", 20.0), 100, Some("g"), true);
        s.update(&row_f64("g", 30.0), 300, Some("g"), true);
        match s.query() {
            Value::F64(v) => {
                assert!((v - 5000.0 / 300.0).abs() < 1e-6, "got {v}");
            }
            _ => panic!(),
        }
    }
}
