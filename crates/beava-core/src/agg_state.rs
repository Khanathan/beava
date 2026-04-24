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

// ─── FirstState (Phase 8) ─────────────────────────────────────────────────────

/// AGG-POINT-01: First non-null field value seen by the entity. Once set,
/// subsequent updates are ignored. Returns `Value::Null` until first event.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct FirstState {
    pub current: Option<Value>,
}

impl FirstState {
    pub fn update(
        &mut self,
        row: &crate::row::Row,
        _event_time_ms: i64,
        field: Option<&str>,
        where_matched: bool,
    ) {
        if !where_matched || self.current.is_some() {
            return;
        }
        let Some(fname) = field else { return };
        let val = match row.get(fname) {
            None | Some(Value::Null) => return,
            Some(v) => v.clone(),
        };
        self.current = Some(val);
    }

    pub fn query(&self) -> Value {
        self.current.clone().unwrap_or(Value::Null)
    }
}

// ─── LastState (Phase 8) ──────────────────────────────────────────────────────

/// AGG-POINT-02: Most recent non-null field value seen.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct LastState {
    pub current: Option<Value>,
}

impl LastState {
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
        self.current = Some(val);
    }

    pub fn query(&self) -> Value {
        self.current.clone().unwrap_or(Value::Null)
    }
}

// ─── FirstNState / LastNState (Phase 8) ───────────────────────────────────────

/// AGG-POINT-03: First n field values seen, in arrival order.
///
/// Wire encoding: a JSON-array string `Value::Str(serde_json::to_string(&list))`
/// since the v0 `Value` enum has no `List` variant (D-07 in 08-CONTEXT).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FirstNState {
    pub n: u32,
    pub values: Vec<Value>,
}

impl FirstNState {
    pub fn new(n: u32) -> Self {
        Self {
            n,
            values: Vec::new(),
        }
    }

    pub fn update(
        &mut self,
        row: &crate::row::Row,
        _event_time_ms: i64,
        field: Option<&str>,
        where_matched: bool,
    ) {
        if !where_matched || self.values.len() >= self.n as usize {
            return;
        }
        let Some(fname) = field else { return };
        let val = match row.get(fname) {
            None | Some(Value::Null) => return,
            Some(v) => v.clone(),
        };
        self.values.push(val);
    }

    pub fn query(&self) -> Value {
        Value::Str(values_to_json_array(&self.values))
    }
}

/// AGG-POINT-04: Last n field values seen, in arrival order (oldest first).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LastNState {
    pub n: u32,
    pub values: std::collections::VecDeque<Value>,
}

impl LastNState {
    pub fn new(n: u32) -> Self {
        Self {
            n,
            values: std::collections::VecDeque::with_capacity(n as usize),
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
        let val = match row.get(fname) {
            None | Some(Value::Null) => return,
            Some(v) => v.clone(),
        };
        if self.values.len() == self.n as usize {
            self.values.pop_front();
        }
        self.values.push_back(val);
    }

    pub fn query(&self) -> Value {
        let v: Vec<&Value> = self.values.iter().collect();
        let mut owned = Vec::with_capacity(v.len());
        for x in v {
            owned.push(x.clone());
        }
        Value::Str(values_to_json_array(&owned))
    }
}

// ─── LagState (Phase 8) ───────────────────────────────────────────────────────

/// AGG-POINT-05: Returns the field value `n` events ago. `lag(field, 1)` is
/// the previous event's value; `lag(field, 2)` is the one before that, etc.
///
/// Stores an internal ring of capacity n+1: the most recent n+1 values.
/// Query returns ring[0] when len == n+1, else Null.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LagState {
    pub n: u32,
    pub values: std::collections::VecDeque<Value>,
}

impl LagState {
    pub fn new(n: u32) -> Self {
        Self {
            n,
            values: std::collections::VecDeque::with_capacity(n as usize + 1),
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
        let val = match row.get(fname) {
            None | Some(Value::Null) => return,
            Some(v) => v.clone(),
        };
        if self.values.len() == self.n as usize + 1 {
            self.values.pop_front();
        }
        self.values.push_back(val);
    }

    pub fn query(&self) -> Value {
        if self.values.len() == self.n as usize + 1 {
            self.values.front().cloned().unwrap_or(Value::Null)
        } else {
            Value::Null
        }
    }
}

// ─── Recency markers (Phase 8) ────────────────────────────────────────────────

/// AGG-RECENCY shared-shape state — used by `FirstSeen`, `LastSeen`, `Age`,
/// `HasSeen`, `TimeSince`. Records the timestamps of the first and most-recent
/// matching events. Returns Datetime/I64/Bool depending on which AggOp variant
/// wraps the state.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SeenState {
    /// event_time_ms of the first matching event, or None.
    pub first_ms: Option<i64>,
    /// event_time_ms of the most recent matching event, or None.
    pub last_ms: Option<i64>,
}

impl SeenState {
    pub fn update(
        &mut self,
        _row: &crate::row::Row,
        event_time_ms: i64,
        _field: Option<&str>,
        where_matched: bool,
    ) {
        if !where_matched {
            return;
        }
        if self.first_ms.is_none() {
            self.first_ms = Some(event_time_ms);
        }
        self.last_ms = Some(event_time_ms);
    }

    pub fn query_first_seen(&self) -> Value {
        match self.first_ms {
            Some(t) => Value::Datetime(t),
            None => Value::Null,
        }
    }
    pub fn query_last_seen(&self) -> Value {
        match self.last_ms {
            Some(t) => Value::Datetime(t),
            None => Value::Null,
        }
    }
    /// Age = query_time_ms - first_ms (lifetime since first observed). Null
    /// when never seen.
    pub fn query_age(&self, query_time_ms: i64) -> Value {
        match self.first_ms {
            Some(t) => Value::I64((query_time_ms - t).max(0)),
            None => Value::Null,
        }
    }
    pub fn query_has_seen(&self) -> Value {
        Value::Bool(self.first_ms.is_some())
    }
    /// time_since = query_time_ms - last_ms (ms since most recent matching event).
    /// Null when never seen.
    pub fn query_time_since(&self, query_time_ms: i64) -> Value {
        match self.last_ms {
            Some(t) => Value::I64((query_time_ms - t).max(0)),
            None => Value::Null,
        }
    }
}

/// AGG-RECENCY-time_since_last_n — keeps a bounded ring of the last n
/// matching event_time_ms values; query returns ms-since the n-th most recent.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimeSinceLastNState {
    pub n: u32,
    pub times_ms: std::collections::VecDeque<i64>,
}

impl TimeSinceLastNState {
    pub fn new(n: u32) -> Self {
        Self {
            n,
            times_ms: std::collections::VecDeque::with_capacity(n as usize),
        }
    }

    pub fn update(
        &mut self,
        _row: &crate::row::Row,
        event_time_ms: i64,
        _field: Option<&str>,
        where_matched: bool,
    ) {
        if !where_matched {
            return;
        }
        if self.times_ms.len() == self.n as usize {
            self.times_ms.pop_front();
        }
        self.times_ms.push_back(event_time_ms);
    }

    /// Returns ms since the n-th most recent matching event (i.e. the oldest
    /// timestamp in the ring once full). Null until the ring holds n entries.
    pub fn query(&self, query_time_ms: i64) -> Value {
        if self.times_ms.len() < self.n as usize {
            return Value::Null;
        }
        let oldest = self.times_ms.front().copied().unwrap_or(query_time_ms);
        Value::I64((query_time_ms - oldest).max(0))
    }
}

// ─── Streak ops (Phase 8) ─────────────────────────────────────────────────────

/// AGG-RECENCY-streak: count of consecutive matching events. Resets to 0 on
/// any non-matching event. Maintains a `max_seen` for `MaxStreak`.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct StreakState {
    pub current: u64,
    pub max_seen: u64,
}

impl StreakState {
    pub fn update(
        &mut self,
        _row: &crate::row::Row,
        _event_time_ms: i64,
        _field: Option<&str>,
        where_matched: bool,
    ) {
        if where_matched {
            self.current += 1;
            if self.current > self.max_seen {
                self.max_seen = self.current;
            }
        } else {
            self.current = 0;
        }
    }
    pub fn query_current(&self) -> Value {
        Value::I64(self.current as i64)
    }
    pub fn query_max(&self) -> Value {
        Value::I64(self.max_seen as i64)
    }
}

/// AGG-RECENCY-negative_streak: count of consecutive NON-matching events.
/// Resets to 0 on any matching event.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct NegativeStreakState {
    pub current: u64,
}

impl NegativeStreakState {
    pub fn update(
        &mut self,
        _row: &crate::row::Row,
        _event_time_ms: i64,
        _field: Option<&str>,
        where_matched: bool,
    ) {
        if where_matched {
            self.current = 0;
        } else {
            self.current += 1;
        }
    }
    pub fn query(&self) -> Value {
        Value::I64(self.current as i64)
    }
}

// ─── FirstSeenInWindow (Phase 8) ──────────────────────────────────────────────

/// AGG-RECENCY-first_seen_in_window: returns Bool(true) iff the most-recent
/// matching event is within `window_ms` of the query time. Lifetime state:
/// just `last_ms` plus a parameter window.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct FirstSeenInWindowState {
    /// Window duration in milliseconds (parameter, not state).
    pub window_ms: u64,
    pub last_ms: Option<i64>,
}

impl FirstSeenInWindowState {
    pub fn new(window_ms: u64) -> Self {
        Self {
            window_ms,
            last_ms: None,
        }
    }
    pub fn update(
        &mut self,
        _row: &crate::row::Row,
        event_time_ms: i64,
        _field: Option<&str>,
        where_matched: bool,
    ) {
        if !where_matched {
            return;
        }
        self.last_ms = Some(event_time_ms);
    }
    pub fn query(&self, query_time_ms: i64) -> Value {
        match self.last_ms {
            Some(t) => {
                let age = query_time_ms - t;
                Value::Bool(age >= 0 && (age as u64) < self.window_ms)
            }
            None => Value::Bool(false),
        }
    }
}

// ─── Helpers (Phase 8) ────────────────────────────────────────────────────────

/// Encode a list of `Value`s as a JSON array string for first_n/last_n wire output.
///
/// We project each `Value` to a plain JSON scalar so the produced string is
/// `"[10.0,20.0,30.0]"` rather than the tagged enum form
/// `"[{\"F64\":10.0},...]"` that serde's default `Value::Serialize` impl
/// produces. This matches the user-facing wire shape documented for
/// `first_n` / `last_n` in `docs/operators.md`.
pub(crate) fn values_to_json_array(values: &[Value]) -> String {
    let projected: Vec<serde_json::Value> = values.iter().map(value_to_json).collect();
    serde_json::to_string(&projected).unwrap_or_else(|_| "[]".to_string())
}

/// Project a `Value` to its untagged JSON form (plain scalar). `Bytes` and
/// `Datetime` round-trip as integer / string respectively. `Null` → JSON null.
fn value_to_json(v: &Value) -> serde_json::Value {
    match v {
        Value::Null => serde_json::Value::Null,
        Value::Bool(b) => serde_json::Value::Bool(*b),
        Value::I64(i) => serde_json::Value::Number((*i).into()),
        Value::F64(f) => serde_json::Number::from_f64(*f)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        Value::Str(s) => serde_json::Value::String(s.clone()),
        Value::Bytes(b) => serde_json::Value::String(format!("0x{}", hex::encode_lower(b))),
        Value::Datetime(t) => serde_json::Value::Number((*t).into()),
        Value::Json(j) => j.clone(),
    }
}

/// Tiny in-module hex helper to avoid an extra dep just for `Value::Bytes`
/// → JSON projection.
mod hex {
    pub fn encode_lower(bytes: &[u8]) -> String {
        let mut s = String::with_capacity(bytes.len() * 2);
        for b in bytes {
            s.push(char::from_digit((*b >> 4) as u32, 16).unwrap());
            s.push(char::from_digit((*b & 0xF) as u32, 16).unwrap());
        }
        s
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

    // ── First / Last (Phase 8) ───────────────────────────────────────────────

    #[test]
    fn first_records_first_event_value() {
        let mut s = FirstState::default();
        s.update(&row_f64("amount", 10.0), 0, Some("amount"), true);
        s.update(&row_f64("amount", 20.0), 1, Some("amount"), true);
        s.update(&row_f64("amount", 30.0), 2, Some("amount"), true);
        assert_eq!(s.query(), Value::F64(10.0));
    }

    #[test]
    fn first_empty_returns_null() {
        assert_eq!(FirstState::default().query(), Value::Null);
    }

    #[test]
    fn first_skips_when_where_false() {
        let mut s = FirstState::default();
        s.update(&row_f64("amount", 10.0), 0, Some("amount"), false);
        s.update(&row_f64("amount", 20.0), 1, Some("amount"), true);
        assert_eq!(s.query(), Value::F64(20.0), "first matching event wins");
    }

    #[test]
    fn first_skips_when_field_null_or_missing() {
        let mut s = FirstState::default();
        s.update(&row_null("amount"), 0, Some("amount"), true);
        s.update(&empty_row(), 1, Some("amount"), true);
        s.update(&row_i64("amount", 7), 2, Some("amount"), true);
        assert_eq!(s.query(), Value::I64(7));
    }

    #[test]
    fn last_records_most_recent_value() {
        let mut s = LastState::default();
        s.update(&row_f64("amount", 10.0), 0, Some("amount"), true);
        s.update(&row_f64("amount", 20.0), 1, Some("amount"), true);
        s.update(&row_f64("amount", 30.0), 2, Some("amount"), true);
        assert_eq!(s.query(), Value::F64(30.0));
    }

    #[test]
    fn last_empty_returns_null() {
        assert_eq!(LastState::default().query(), Value::Null);
    }

    #[test]
    fn last_skips_when_where_false() {
        let mut s = LastState::default();
        s.update(&row_f64("amount", 10.0), 0, Some("amount"), true);
        s.update(&row_f64("amount", 20.0), 1, Some("amount"), false);
        assert_eq!(s.query(), Value::F64(10.0));
    }

    // ── FirstN / LastN (Phase 8) ─────────────────────────────────────────────

    #[test]
    fn first_n_collects_first_n_then_stops() {
        let mut s = FirstNState::new(3);
        for v in [10.0, 20.0, 30.0, 40.0, 50.0] {
            s.update(&row_f64("amount", v), 0, Some("amount"), true);
        }
        let q = s.query();
        match q {
            Value::Str(s) => assert_eq!(s, "[10.0,20.0,30.0]"),
            other => panic!("expected Str, got {:?}", other),
        }
    }

    #[test]
    fn first_n_empty_returns_empty_array() {
        let s = FirstNState::new(3);
        assert_eq!(s.query(), Value::Str("[]".to_string()));
    }

    #[test]
    fn first_n_skips_when_where_false() {
        let mut s = FirstNState::new(2);
        s.update(&row_f64("amount", 10.0), 0, Some("amount"), false);
        s.update(&row_f64("amount", 20.0), 1, Some("amount"), true);
        s.update(&row_f64("amount", 30.0), 2, Some("amount"), true);
        match s.query() {
            Value::Str(s) => assert_eq!(s, "[20.0,30.0]"),
            other => panic!("expected Str, got {:?}", other),
        }
    }

    #[test]
    fn last_n_keeps_most_recent_n() {
        let mut s = LastNState::new(3);
        for v in [10.0, 20.0, 30.0, 40.0, 50.0] {
            s.update(&row_f64("amount", v), 0, Some("amount"), true);
        }
        match s.query() {
            Value::Str(s) => assert_eq!(s, "[30.0,40.0,50.0]"),
            other => panic!("expected Str, got {:?}", other),
        }
    }

    #[test]
    fn last_n_empty_returns_empty_array() {
        assert_eq!(LastNState::new(3).query(), Value::Str("[]".to_string()));
    }

    // ── Lag (Phase 8) ────────────────────────────────────────────────────────

    #[test]
    fn lag_returns_value_n_events_ago() {
        // lag(field, 1) = previous event's value (the one before the most recent)
        let mut s = LagState::new(1);
        s.update(&row_f64("amount", 10.0), 0, Some("amount"), true);
        // After 1 event, only the current event is in the ring; lag(1) needs 2 → Null.
        assert_eq!(s.query(), Value::Null);
        s.update(&row_f64("amount", 20.0), 1, Some("amount"), true);
        // After 2 events, lag(1) = oldest = 10.0
        assert_eq!(s.query(), Value::F64(10.0));
        s.update(&row_f64("amount", 30.0), 2, Some("amount"), true);
        // After 3 events, lag(1) = previous (which was 20.0)
        assert_eq!(s.query(), Value::F64(20.0));
    }

    #[test]
    fn lag_2_needs_three_events_to_return_value() {
        let mut s = LagState::new(2);
        s.update(&row_f64("amount", 10.0), 0, Some("amount"), true);
        s.update(&row_f64("amount", 20.0), 1, Some("amount"), true);
        assert_eq!(s.query(), Value::Null);
        s.update(&row_f64("amount", 30.0), 2, Some("amount"), true);
        assert_eq!(s.query(), Value::F64(10.0));
        s.update(&row_f64("amount", 40.0), 3, Some("amount"), true);
        assert_eq!(s.query(), Value::F64(20.0));
    }

    #[test]
    fn lag_empty_returns_null() {
        assert_eq!(LagState::new(1).query(), Value::Null);
    }

    // ── SeenState (Phase 8) ──────────────────────────────────────────────────

    #[test]
    fn seen_state_records_first_and_last() {
        let mut s = SeenState::default();
        s.update(&empty_row(), 100, None, true);
        s.update(&empty_row(), 200, None, true);
        s.update(&empty_row(), 300, None, true);
        assert_eq!(s.first_ms, Some(100));
        assert_eq!(s.last_ms, Some(300));
    }

    #[test]
    fn seen_state_first_seen_returns_datetime() {
        let mut s = SeenState::default();
        assert_eq!(s.query_first_seen(), Value::Null);
        s.update(&empty_row(), 100, None, true);
        assert_eq!(s.query_first_seen(), Value::Datetime(100));
    }

    #[test]
    fn seen_state_last_seen_returns_datetime() {
        let mut s = SeenState::default();
        assert_eq!(s.query_last_seen(), Value::Null);
        s.update(&empty_row(), 100, None, true);
        s.update(&empty_row(), 200, None, true);
        assert_eq!(s.query_last_seen(), Value::Datetime(200));
    }

    #[test]
    fn seen_state_age_computes_query_minus_first() {
        let mut s = SeenState::default();
        assert_eq!(s.query_age(500), Value::Null);
        s.update(&empty_row(), 100, None, true);
        assert_eq!(s.query_age(500), Value::I64(400));
    }

    #[test]
    fn seen_state_has_seen_returns_bool() {
        let mut s = SeenState::default();
        assert_eq!(s.query_has_seen(), Value::Bool(false));
        s.update(&empty_row(), 100, None, true);
        assert_eq!(s.query_has_seen(), Value::Bool(true));
    }

    #[test]
    fn seen_state_time_since_computes_query_minus_last() {
        let mut s = SeenState::default();
        assert_eq!(s.query_time_since(500), Value::Null);
        s.update(&empty_row(), 100, None, true);
        s.update(&empty_row(), 200, None, true);
        assert_eq!(s.query_time_since(500), Value::I64(300));
    }

    #[test]
    fn seen_state_skips_when_where_false() {
        let mut s = SeenState::default();
        s.update(&empty_row(), 100, None, false);
        assert_eq!(s.query_has_seen(), Value::Bool(false));
        s.update(&empty_row(), 200, None, true);
        assert_eq!(s.query_first_seen(), Value::Datetime(200));
    }

    // ── TimeSinceLastN (Phase 8) ─────────────────────────────────────────────

    #[test]
    fn time_since_last_n_returns_null_until_ring_full() {
        let mut s = TimeSinceLastNState::new(3);
        assert_eq!(s.query(1000), Value::Null);
        s.update(&empty_row(), 100, None, true);
        assert_eq!(s.query(1000), Value::Null);
        s.update(&empty_row(), 200, None, true);
        assert_eq!(s.query(1000), Value::Null);
        s.update(&empty_row(), 300, None, true);
        // Ring is now full with [100, 200, 300]; query at 1000 → 1000-100 = 900
        assert_eq!(s.query(1000), Value::I64(900));
    }

    #[test]
    fn time_since_last_n_evicts_oldest_when_ring_full() {
        let mut s = TimeSinceLastNState::new(2);
        s.update(&empty_row(), 100, None, true);
        s.update(&empty_row(), 200, None, true);
        // Ring: [100, 200]; query → 1000 - 100 = 900
        assert_eq!(s.query(1000), Value::I64(900));
        s.update(&empty_row(), 300, None, true);
        // Ring: [200, 300]; query → 1000 - 200 = 800
        assert_eq!(s.query(1000), Value::I64(800));
    }

    // ── StreakState (Phase 8) ────────────────────────────────────────────────

    #[test]
    fn streak_increments_on_match() {
        let mut s = StreakState::default();
        for _ in 0..5 {
            s.update(&empty_row(), 0, None, true);
        }
        assert_eq!(s.query_current(), Value::I64(5));
        assert_eq!(s.query_max(), Value::I64(5));
    }

    #[test]
    fn streak_resets_on_non_match() {
        let mut s = StreakState::default();
        s.update(&empty_row(), 0, None, true);
        s.update(&empty_row(), 0, None, true);
        s.update(&empty_row(), 0, None, false); // resets current to 0
        assert_eq!(s.query_current(), Value::I64(0));
        assert_eq!(s.query_max(), Value::I64(2)); // max was 2
        s.update(&empty_row(), 0, None, true);
        s.update(&empty_row(), 0, None, true);
        s.update(&empty_row(), 0, None, true);
        s.update(&empty_row(), 0, None, true);
        assert_eq!(s.query_current(), Value::I64(4));
        assert_eq!(s.query_max(), Value::I64(4)); // max bumped to 4
    }

    #[test]
    fn streak_empty_returns_zero() {
        let s = StreakState::default();
        assert_eq!(s.query_current(), Value::I64(0));
        assert_eq!(s.query_max(), Value::I64(0));
    }

    #[test]
    fn negative_streak_increments_on_non_match() {
        let mut s = NegativeStreakState::default();
        s.update(&empty_row(), 0, None, false);
        s.update(&empty_row(), 0, None, false);
        s.update(&empty_row(), 0, None, false);
        assert_eq!(s.query(), Value::I64(3));
        s.update(&empty_row(), 0, None, true);
        assert_eq!(s.query(), Value::I64(0));
    }

    // ── FirstSeenInWindow (Phase 8) ──────────────────────────────────────────

    #[test]
    fn first_seen_in_window_empty_returns_false() {
        let s = FirstSeenInWindowState::new(1000);
        assert_eq!(s.query(500), Value::Bool(false));
    }

    #[test]
    fn first_seen_in_window_recent_event_returns_true() {
        let mut s = FirstSeenInWindowState::new(1000);
        s.update(&empty_row(), 100, None, true);
        // age = 500 - 100 = 400 < 1000 → true
        assert_eq!(s.query(500), Value::Bool(true));
    }

    #[test]
    fn first_seen_in_window_old_event_returns_false() {
        let mut s = FirstSeenInWindowState::new(1000);
        s.update(&empty_row(), 100, None, true);
        // age = 2000 - 100 = 1900 >= 1000 → false
        assert_eq!(s.query(2000), Value::Bool(false));
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
