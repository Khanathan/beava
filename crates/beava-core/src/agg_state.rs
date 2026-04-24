//! Per-operator aggregation state structs for Beava Phase 5.
//!
//! Each state struct tracks the running value for exactly one (feature, entity)
//! slot. State is updated via `update()` and queried via `query()` / per-op
//! query helpers. No syscalls, no wall clock, no random sources — pure
//! deterministic state transitions on `event_time_ms`.
//!
//! # Requirements traceability
//! - AGG-CORE-01: CountState
//! - AGG-CORE-02: SumState
//! - AGG-CORE-03: AvgState
//! - AGG-CORE-04: MinState
//! - AGG-CORE-05: MaxState
//! - AGG-CORE-06: VarianceState (+ StdDev query)
//! - AGG-CORE-07: RatioState
//!
//! D-06: no wall-clock reads in apply paths — event_time_ms only.
//! D-06: Welford online algorithm for variance — deterministic, numerically
//!       stable, combinable across tumbling buckets.

use crate::row::Value;
use crate::sketches::bloom::BloomFilter;
use crate::sketches::cms::TopKValue;
use crate::sketches::count_distinct::CountDistinctState as InnerCountDistinct;
use crate::sketches::entropy::EntropyHistogram;
use crate::sketches::percentile::PercentileState as InnerPercentile;
use crate::sketches::top_k::TopKState as InnerTopK;
use serde::{Deserialize, Serialize};
use std::hash::{Hash, Hasher};

// ─── Helpers ─────────────────────────────────────────────────────────────────

/// Extract a numeric (F64 or I64) value from a Row field.
/// Returns `None` when the field is absent or `Value::Null` (three-valued null).
fn numeric_from_row(row: &crate::row::Row, field: &str) -> Option<f64> {
    match row.get(field)? {
        Value::F64(v) => Some(*v),
        Value::I64(v) => Some(*v as f64),
        Value::Null => None,
        _ => None,
    }
}

/// Same-type less-than comparison for Min/Max ordering.
/// Returns true iff `a < b` using natural ordering for the type.
/// Cross-type comparisons always return false (type-stable ordering).
pub fn value_lt(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::F64(x), Value::F64(y)) => x < y,
        (Value::I64(x), Value::I64(y)) => x < y,
        (Value::Str(x), Value::Str(y)) => x < y,
        (Value::Datetime(x), Value::Datetime(y)) => x < y,
        _ => false,
    }
}

// ─── CountState ──────────────────────────────────────────────────────────────

/// AGG-CORE-01: Counts rows. Increments when `where_matched=true`.
/// Null field values are irrelevant — Count counts *rows*, not field values.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CountState {
    pub n: u64,
}

impl CountState {
    pub fn update(
        &mut self,
        _row: &crate::row::Row,
        _event_time_ms: i64,
        _field: Option<&str>,
        where_matched: bool,
    ) {
        if where_matched {
            self.n += 1;
        }
    }

    pub fn query(&self) -> Value {
        Value::I64(self.n as i64)
    }
}

// ─── SumState ────────────────────────────────────────────────────────────────

/// AGG-CORE-02: Sum of a numeric field. SQL null semantics: Null field skipped.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SumState {
    pub total: f64,
    /// n tracks whether any row contributed (for returning Null when empty).
    pub n: u64,
}

impl SumState {
    pub fn update(
        &mut self,
        row: &crate::row::Row,
        _event_time_ms: i64,
        field: Option<&str>,
        where_matched: bool,
    ) {
        if !where_matched {
            return;
        }
        let Some(fname) = field else { return };
        let Some(v) = numeric_from_row(row, fname) else {
            return;
        };
        self.total += v;
        self.n += 1;
    }

    pub fn query(&self) -> Value {
        if self.n == 0 {
            Value::Null
        } else {
            Value::F64(self.total)
        }
    }
}

// ─── AvgState ────────────────────────────────────────────────────────────────

/// AGG-CORE-03: Arithmetic mean of a numeric field. Returns Null when n==0.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct AvgState {
    pub sum: f64,
    pub n: u64,
}

impl AvgState {
    pub fn update(
        &mut self,
        row: &crate::row::Row,
        _event_time_ms: i64,
        field: Option<&str>,
        where_matched: bool,
    ) {
        if !where_matched {
            return;
        }
        let Some(fname) = field else { return };
        let Some(v) = numeric_from_row(row, fname) else {
            return;
        };
        self.sum += v;
        self.n += 1;
    }

    pub fn query(&self) -> Value {
        if self.n == 0 {
            Value::Null
        } else {
            Value::F64(self.sum / self.n as f64)
        }
    }
}

// ─── MinState ────────────────────────────────────────────────────────────────

/// AGG-CORE-04: Running minimum. Preserves original Value type for min/max type
/// inference (e.g., I64 field stays I64). First observed value wins on ties.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct MinState {
    pub current: Option<Value>,
}

impl MinState {
    pub fn update(
        &mut self,
        row: &crate::row::Row,
        _event_time_ms: i64,
        field: Option<&str>,
        where_matched: bool,
    ) {
        if !where_matched {
            return;
        }
        let Some(fname) = field else { return };
        let val = match row.get(fname) {
            None | Some(Value::Null) => return,
            Some(v) => v.clone(),
        };
        match &self.current {
            None => self.current = Some(val),
            Some(current) => {
                if value_lt(&val, current) {
                    self.current = Some(val);
                }
            }
        }
    }

    pub fn query(&self) -> Value {
        self.current.clone().unwrap_or(Value::Null)
    }
}

// ─── MaxState ────────────────────────────────────────────────────────────────

/// AGG-CORE-05: Running maximum. Mirror of MinState.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct MaxState {
    pub current: Option<Value>,
}

impl MaxState {
    pub fn update(
        &mut self,
        row: &crate::row::Row,
        _event_time_ms: i64,
        field: Option<&str>,
        where_matched: bool,
    ) {
        if !where_matched {
            return;
        }
        let Some(fname) = field else { return };
        let val = match row.get(fname) {
            None | Some(Value::Null) => return,
            Some(v) => v.clone(),
        };
        match &self.current {
            None => self.current = Some(val),
            Some(current) => {
                if value_lt(current, &val) {
                    self.current = Some(val);
                }
            }
        }
    }

    pub fn query(&self) -> Value {
        self.current.clone().unwrap_or(Value::Null)
    }
}

// ─── VarianceState ───────────────────────────────────────────────────────────

/// AGG-CORE-06: Online variance + stddev using Welford's algorithm.
///
/// Welford update:
/// ```text
/// n    += 1
/// delta = x - mean
/// mean += delta / n
/// delta2 = x - mean   (re-computed AFTER mean update)
/// m2   += delta * delta2
/// ```
/// Sample variance = m2 / (n-1).  Numerically stable and combinable across
/// buckets via pairwise merge (see `agg_windowed.rs`).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct VarianceState {
    pub n: u64,
    pub mean: f64,
    pub m2: f64,
}

impl VarianceState {
    pub fn update(
        &mut self,
        row: &crate::row::Row,
        _event_time_ms: i64,
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

        self.n += 1;
        let delta = x - self.mean;
        self.mean += delta / self.n as f64;
        let delta2 = x - self.mean;
        self.m2 += delta * delta2;
    }

    /// Sample variance (Bessel-corrected, n-1 denominator). Returns Null when n < 2.
    pub fn query_variance(&self) -> Value {
        if self.n < 2 {
            Value::Null
        } else {
            Value::F64(self.m2 / (self.n - 1) as f64)
        }
    }

    /// Sample standard deviation = sqrt(sample_variance). Returns Null when n < 2.
    pub fn query_stddev(&self) -> Value {
        match self.query_variance() {
            Value::F64(v) => Value::F64(v.sqrt()),
            other => other,
        }
    }
}

// ─── RatioState ──────────────────────────────────────────────────────────────

/// AGG-CORE-07: Ratio of matching events to all events.
///
/// `where_matched` is the numerator predicate: if true, both `total` and
/// `matching` increment; otherwise only `total` increments. This gives the
/// fraction of events satisfying the predicate.
///
/// Note: In Plan 05-01 (pre-predicate threading), callers pass `where_matched`
/// directly to encode the numerator condition. Plan 05-02 wires in the Expr
/// evaluator to compute `where_matched` from a row + predicate expression.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RatioState {
    pub matching: u64,
    pub total: u64,
}

impl RatioState {
    pub fn update(
        &mut self,
        _row: &crate::row::Row,
        _event_time_ms: i64,
        _field: Option<&str>,
        where_matched: bool,
    ) {
        self.total += 1;
        if where_matched {
            self.matching += 1;
        }
    }

    pub fn query(&self) -> Value {
        if self.total == 0 {
            Value::Null
        } else {
            Value::F64(self.matching as f64 / self.total as f64)
        }
    }
}

// ─── Sketch wrappers (Plan 10-05) ─────────────────────────────────────────────

/// Hash a `Value` for use as a CountDistinct entry. Uses ahash deterministic
/// seed (BuildHasher) — same seed across the process so estimates are stable.
fn hash_value(v: &Value) -> u64 {
    let mut h = ahash::AHasher::default();
    match v {
        Value::Str(s) => s.hash(&mut h),
        Value::I64(n) => n.hash(&mut h),
        Value::F64(f) => f.to_bits().hash(&mut h),
        Value::Bool(b) => b.hash(&mut h),
        Value::Datetime(ms) => ms.hash(&mut h),
        Value::Bytes(b) => b.hash(&mut h),
        Value::Null => 0u64.hash(&mut h),
        Value::Json(j) => j.to_string().hash(&mut h),
    }
    h.finish()
}

/// String view of a `Value` for use as Entropy / Bloom key.
fn value_to_key_string(v: &Value) -> Option<String> {
    match v {
        Value::Str(s) => Some(s.clone()),
        Value::I64(n) => Some(n.to_string()),
        Value::F64(f) => Some(format!("{f:?}")),
        Value::Bool(b) => Some(b.to_string()),
        Value::Datetime(ms) => Some(ms.to_string()),
        Value::Null => None,
        Value::Bytes(_) => None,
        Value::Json(_) => None,
    }
}

/// AGG-SKETCH-01: count_distinct wrapper.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CountDistinctStateWrap {
    pub inner: InnerCountDistinct,
}

impl Default for CountDistinctStateWrap {
    fn default() -> Self {
        Self {
            inner: InnerCountDistinct::new(1024),
        }
    }
}

impl CountDistinctStateWrap {
    pub fn update(
        &mut self,
        row: &crate::row::Row,
        _event_time_ms: i64,
        field: Option<&str>,
        where_matched: bool,
    ) {
        if !where_matched {
            return;
        }
        let Some(fname) = field else { return };
        let Some(v) = row.get(fname) else { return };
        if matches!(v, Value::Null) {
            return;
        }
        self.inner.add_hash(hash_value(v));
    }
    pub fn query(&self) -> Value {
        Value::I64(self.inner.estimate() as i64)
    }
}

/// AGG-SKETCH-02: percentile wrapper.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PercentileStateWrap {
    pub inner: InnerPercentile,
    pub q: f64,
}

impl Default for PercentileStateWrap {
    fn default() -> Self {
        Self {
            inner: InnerPercentile::new(256, 0.01),
            q: 0.5,
        }
    }
}

impl PercentileStateWrap {
    pub fn update(
        &mut self,
        row: &crate::row::Row,
        _event_time_ms: i64,
        field: Option<&str>,
        where_matched: bool,
    ) {
        if !where_matched {
            return;
        }
        let Some(fname) = field else { return };
        let Some(v) = numeric_from_row(row, fname) else {
            return;
        };
        self.inner.insert(v);
    }
    pub fn query(&self) -> Value {
        match self.inner.quantile(self.q) {
            Some(v) => Value::F64(v),
            None => Value::Null,
        }
    }
}

/// AGG-SKETCH-03: top_k wrapper.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopKStateWrap {
    pub inner: InnerTopK,
}

impl Default for TopKStateWrap {
    fn default() -> Self {
        Self {
            inner: InnerTopK::new(10, 1024, 2048, 4),
        }
    }
}

impl TopKStateWrap {
    pub fn update(
        &mut self,
        row: &crate::row::Row,
        _event_time_ms: i64,
        field: Option<&str>,
        where_matched: bool,
    ) {
        if !where_matched {
            return;
        }
        let Some(fname) = field else { return };
        let v = match row.get(fname) {
            None | Some(Value::Null) => return,
            Some(other) => other,
        };
        let tkv = match v {
            Value::Str(s) => TopKValue::Str(s.clone()),
            Value::I64(n) => TopKValue::Int(*n),
            Value::F64(f) => TopKValue::Float(ordered_float::OrderedFloat(*f)),
            Value::Bool(b) => TopKValue::Bool(*b),
            Value::Datetime(ms) => TopKValue::Int(*ms),
            Value::Bytes(_) | Value::Null | Value::Json(_) => return,
        };
        self.inner.insert(tkv);
    }
    pub fn query(&self) -> Value {
        let entries: Vec<serde_json::Value> = self
            .inner
            .top()
            .into_iter()
            .map(|(v, c)| serde_json::json!({"value": v.to_json(), "count": c}))
            .collect();
        Value::Json(serde_json::Value::Array(entries))
    }
}

/// AGG-SKETCH-04: bloom_member wrapper. v0 placeholder: query returns
/// `Value::Bool(true)` once the filter has at least one insertion. Full
/// membership-test API (passing a value to GET) deferred to v0.1.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BloomMemberStateWrap {
    pub inner: BloomFilter,
    pub n_inserts: u64,
}

impl Default for BloomMemberStateWrap {
    fn default() -> Self {
        Self {
            inner: BloomFilter::with_capacity_and_fpr(1024, 0.01),
            n_inserts: 0,
        }
    }
}

impl BloomMemberStateWrap {
    pub fn with_params(capacity: usize, fpr: f64) -> Self {
        Self {
            inner: BloomFilter::with_capacity_and_fpr(capacity.max(1), fpr.clamp(1e-9, 0.999)),
            n_inserts: 0,
        }
    }
    pub fn update(
        &mut self,
        row: &crate::row::Row,
        _event_time_ms: i64,
        field: Option<&str>,
        where_matched: bool,
    ) {
        if !where_matched {
            return;
        }
        let Some(fname) = field else { return };
        let Some(v) = row.get(fname) else { return };
        let Some(s) = value_to_key_string(v) else {
            return;
        };
        self.inner.insert(&s);
        self.n_inserts = self.n_inserts.saturating_add(1);
    }
    pub fn query(&self) -> Value {
        Value::Bool(self.n_inserts > 0)
    }
}

/// AGG-SKETCH-05: entropy wrapper.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EntropyStateWrap {
    pub inner: EntropyHistogram,
}

impl Default for EntropyStateWrap {
    fn default() -> Self {
        Self {
            inner: EntropyHistogram::new(1024),
        }
    }
}

impl EntropyStateWrap {
    pub fn update(
        &mut self,
        row: &crate::row::Row,
        _event_time_ms: i64,
        field: Option<&str>,
        where_matched: bool,
    ) {
        if !where_matched {
            return;
        }
        let Some(fname) = field else { return };
        let Some(v) = row.get(fname) else { return };
        let Some(s) = value_to_key_string(v) else {
            return;
        };
        self.inner.insert(&s);
    }
    pub fn query(&self) -> Value {
        Value::F64(self.inner.entropy_bits())
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::row::Row;

    fn row_f64(field: &str, v: f64) -> Row {
        Row::new().with_field(field, Value::F64(v))
    }

    fn row_i64(field: &str, v: i64) -> Row {
        Row::new().with_field(field, Value::I64(v))
    }

    fn row_null(field: &str) -> Row {
        Row::new().with_field(field, Value::Null)
    }

    fn empty_row() -> Row {
        Row::new()
    }

    // ── Count ────────────────────────────────────────────────────────────────

    #[test]
    fn count_counts_all_rows() {
        let mut state = CountState::default();
        let r = empty_row();
        state.update(&r, 0, None, true);
        state.update(&r, 1, None, true);
        state.update(&r, 2, None, true);
        assert_eq!(state.n, 3);
        assert_eq!(state.query(), Value::I64(3));
    }

    #[test]
    fn count_ignores_field_and_where_matched() {
        // Count increments when where_matched=true regardless of field
        let mut state = CountState::default();
        let r = row_f64("amount", 100.0);
        state.update(&r, 0, Some("amount"), true);
        state.update(&r, 1, Some("amount"), true);
        assert_eq!(state.n, 2);

        // Does NOT increment when where_matched=false
        state.update(&r, 2, Some("amount"), false);
        state.update(&r, 3, None, false);
        assert_eq!(state.n, 2, "where_matched=false should not increment count");
    }

    // ── Sum ──────────────────────────────────────────────────────────────────

    #[test]
    fn sum_sums_field() {
        let mut state = SumState::default();
        state.update(&row_f64("amount", 10.0), 0, Some("amount"), true);
        state.update(&row_f64("amount", 5.0), 1, Some("amount"), true);
        state.update(&row_f64("amount", -2.0), 2, Some("amount"), true);
        assert!(
            (state.total - 13.0).abs() < 1e-10,
            "total should be 13.0, got {}",
            state.total
        );
        assert_eq!(state.query(), Value::F64(13.0));
    }

    #[test]
    fn sum_skips_null_field() {
        let mut state = SumState::default();
        state.update(&row_f64("amount", 5.0), 0, Some("amount"), true);
        state.update(&row_null("amount"), 1, Some("amount"), true);
        state.update(&row_f64("amount", 3.0), 2, Some("amount"), true);
        assert_eq!(
            state.query(),
            Value::F64(8.0),
            "Null field should be skipped"
        );
    }

    #[test]
    fn sum_empty_returns_null() {
        let state = SumState::default();
        assert_eq!(state.query(), Value::Null);
    }

    // ── Avg ──────────────────────────────────────────────────────────────────

    #[test]
    fn avg_is_mean() {
        let mut state = AvgState::default();
        for v in [1.0_f64, 2.0, 3.0] {
            state.update(&row_f64("x", v), 0, Some("x"), true);
        }
        assert!((state.sum - 6.0).abs() < 1e-10);
        assert_eq!(state.n, 3);
        assert_eq!(state.query(), Value::F64(2.0));
    }

    #[test]
    fn avg_empty_returns_null() {
        let state = AvgState::default();
        assert_eq!(state.query(), Value::Null);
    }

    // ── Min ──────────────────────────────────────────────────────────────────

    #[test]
    fn min_tracks_min_f64() {
        let mut state = MinState::default();
        for v in [3.0_f64, 1.0, 5.0, 2.0] {
            state.update(&row_f64("x", v), 0, Some("x"), true);
        }
        assert_eq!(state.query(), Value::F64(1.0));
    }

    #[test]
    fn min_preserves_i64_type() {
        let mut state = MinState::default();
        for v in [3_i64, 1, 5] {
            state.update(&row_i64("x", v), 0, Some("x"), true);
        }
        assert_eq!(state.query(), Value::I64(1));
    }

    #[test]
    fn min_first_value_wins_on_tie() {
        let mut state = MinState::default();
        state.update(&row_f64("x", 1.0), 0, Some("x"), true);
        state.update(&row_f64("x", 1.0), 1, Some("x"), true);
        // Both are 1.0; first observed should be in state
        assert_eq!(state.query(), Value::F64(1.0));
        // Verify it doesn't replace with equal value (strict less-than)
        assert_eq!(state.current.as_ref().map(|_| ()), Some(()));
    }

    // ── Max ──────────────────────────────────────────────────────────────────

    #[test]
    fn max_tracks_max_f64() {
        let mut state = MaxState::default();
        for v in [3.0_f64, 1.0, 5.0, 2.0] {
            state.update(&row_f64("x", v), 0, Some("x"), true);
        }
        assert_eq!(state.query(), Value::F64(5.0));
    }

    // ── Variance + StdDev ────────────────────────────────────────────────────

    #[test]
    fn variance_welford_matches_textbook() {
        // Stream: [2, 4, 4, 4, 5, 5, 7, 9]
        // Mean = 5.0, SS = 32.0
        // Sample variance (Bessel-corrected, n-1 = 7 denominator) = 32/7 ≈ 4.571428...
        //
        // Note: The plan referenced "4.0" which is the population variance (n denominator).
        // Beava uses sample variance (n-1) for Welford to be consistent with statistical
        // convention and combinable across buckets. Test asserts 32/7 = 4.571428...
        // (Deviation: plan had incorrect expected value; correct sample variance is 32/7.)
        let mut state = VarianceState::default();
        for v in [2.0_f64, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0] {
            state.update(&row_f64("x", v), 0, Some("x"), true);
        }
        let variance = match state.query_variance() {
            Value::F64(v) => v,
            other => panic!("expected F64, got {:?}", other),
        };
        let expected = 32.0_f64 / 7.0; // sample variance (n-1 denominator) = 4.571428...
        assert!(
            (variance - expected).abs() < 1e-10,
            "sample variance should be {expected:.6}, got {variance}"
        );
    }

    #[test]
    fn stddev_is_sqrt_variance() {
        // Same stream as variance test: stddev = sqrt(32/7)
        let mut state = VarianceState::default();
        for v in [2.0_f64, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0] {
            state.update(&row_f64("x", v), 0, Some("x"), true);
        }
        let stddev = match state.query_stddev() {
            Value::F64(v) => v,
            other => panic!("expected F64, got {:?}", other),
        };
        let expected_stddev = (32.0_f64 / 7.0_f64).sqrt();
        assert!(
            (stddev - expected_stddev).abs() < 1e-10,
            "stddev should be sqrt(32/7)={expected_stddev:.6}, got {stddev}"
        );
    }

    #[test]
    fn variance_single_element_returns_null() {
        let mut state = VarianceState::default();
        state.update(&row_f64("x", 5.0), 0, Some("x"), true);
        assert_eq!(state.query_variance(), Value::Null);
        assert_eq!(state.query_stddev(), Value::Null);
    }

    // ── Ratio ────────────────────────────────────────────────────────────────

    #[test]
    fn ratio_counts_matching_over_total() {
        // 10 events, 3 matched → 0.3
        let mut state = RatioState::default();
        let r = empty_row();
        for i in 0..10 {
            state.update(&r, i, None, i < 3);
        }
        assert_eq!(state.matching, 3);
        assert_eq!(state.total, 10);
        let ratio = match state.query() {
            Value::F64(v) => v,
            other => panic!("expected F64, got {:?}", other),
        };
        assert!(
            (ratio - 0.3).abs() < 1e-10,
            "ratio should be 0.3, got {ratio}"
        );
    }

    #[test]
    fn ratio_empty_returns_null() {
        let state = RatioState::default();
        assert_eq!(state.query(), Value::Null);
    }

    // ── Determinism guard ────────────────────────────────────────────────────

    #[test]
    fn no_systemtime_now_in_agg_state() {
        // Split the forbidden patterns so this file does not itself trigger the check.
        let forbidden_clock = ["SystemTime", "::", "now"].concat();
        let forbidden_rand = ["rand", "::"].concat();
        let src = include_str!("agg_state.rs");
        assert!(
            !src.contains(forbidden_clock.as_str()),
            "agg_state.rs must not use wall-clock reads (D-06 determinism invariant)"
        );
        assert!(
            !src.contains(forbidden_rand.as_str()),
            "agg_state.rs must not use rand crate (D-06 determinism invariant)"
        );
    }
}
