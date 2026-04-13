//! Streaming operators for windowed aggregation.
//!
//! Each operator wraps one or more `RingBuffer`s and implements the `Operator`
//! trait: `push()` to ingest an event, `read()` to get the current aggregate.

use super::window::RingBuffer;
use crate::error::TallyError;
use crate::types::FeatureValue;
use ordered_float::OrderedFloat;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, VecDeque};
use std::time::SystemTime;

/// Resolve a field value from enrichment overlay first, then raw event.
/// Used by all field-reading operators (sum, avg, min, max, last, etc.).
pub fn resolve_field<'a>(
    field: &str,
    event: &'a serde_json::Value,
    enrichment: Option<&'a ahash::AHashMap<String, serde_json::Value>>,
) -> Option<&'a serde_json::Value> {
    if let Some(enr) = enrichment {
        if let Some(val) = enr.get(field) {
            return Some(val);
        }
    }
    event.get(field)
}

/// Trait implemented by all streaming operators.
/// - `push` processes an incoming event. Called once per event per operator.
/// - `read` returns the current aggregate value. Called to collect features.
///
/// `read` takes `&mut self` so implementations can call `advance_to(now)` to
/// expire stale buckets before aggregating. This is safe in Tally's
/// single-threaded Redis-like design (no concurrent reads).
pub trait Operator: std::fmt::Debug + Send {
    fn push(
        &mut self,
        event: &serde_json::Value,
        enrichment: Option<&ahash::AHashMap<String, serde_json::Value>>,
        now: SystemTime,
    ) -> Result<(), TallyError>;
    fn read(&mut self, now: SystemTime) -> FeatureValue;
}

/// Counts events within a time window. Needs no field -- always succeeds
/// regardless of event shape.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CountOp {
    buffer: RingBuffer<u64>,
}

impl CountOp {
    pub fn new(window_duration: std::time::Duration, bucket_duration: std::time::Duration) -> Self {
        Self {
            buffer: RingBuffer::new(window_duration, bucket_duration),
        }
    }
}

impl Operator for CountOp {
    fn push(
        &mut self,
        _event: &serde_json::Value,
        _enrichment: Option<&ahash::AHashMap<String, serde_json::Value>>,
        now: SystemTime,
    ) -> Result<(), TallyError> {
        // count needs no field -- always succeeds regardless of event shape (CONTEXT.md)
        self.buffer.add_to_current(1u64, now);
        Ok(())
    }

    fn read(&mut self, now: SystemTime) -> FeatureValue {
        // Advance time to expire stale buckets before aggregating.
        // Safe in single-threaded design (see trait docs).
        self.buffer.advance_to(now);
        let total = self.buffer.sum_all();
        if total == 0 {
            FeatureValue::Missing // Zero events in window -> Missing (CONTEXT.md)
        } else {
            FeatureValue::Int(i64::try_from(total).unwrap_or(i64::MAX))
        }
    }
}

/// Sums a numeric field's values within a time window.
/// Extracts the named field from each event and type-checks it.
/// Redis-strict: non-numeric field -> TallyError::Type.
/// optional=true: absent field -> silent skip (Ok(())).
/// optional=false: absent field -> TallyError::Type.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SumOp {
    field: String,
    buffer: RingBuffer<f64>,
    /// Parallel count buffer to track whether any events were pushed.
    /// Needed because count_nonzero on the sum buffer returns 0 for all-zero
    /// sums, which would incorrectly return Missing instead of Float(0.0).
    event_count: RingBuffer<u64>,
    optional: bool,
}

impl SumOp {
    pub fn new(
        field: impl Into<String>,
        window_duration: std::time::Duration,
        bucket_duration: std::time::Duration,
        optional: bool,
    ) -> Self {
        Self {
            field: field.into(),
            buffer: RingBuffer::new(window_duration, bucket_duration),
            event_count: RingBuffer::new(window_duration, bucket_duration),
            optional,
        }
    }
}

impl Operator for SumOp {
    fn push(
        &mut self,
        event: &serde_json::Value,
        enrichment: Option<&ahash::AHashMap<String, serde_json::Value>>,
        now: SystemTime,
    ) -> Result<(), TallyError> {
        match resolve_field(&self.field, event, enrichment) {
            None => {
                if self.optional {
                    Ok(()) // optional=true: absent field -> skip silently
                } else {
                    Err(TallyError::Type {
                        field: self.field.clone(),
                        expected: "numeric".into(),
                        got: "absent".into(),
                    })
                }
            }
            Some(val) => {
                // Extract numeric value. Int or Float accepted, anything else -> type error.
                if let Some(f) = val.as_f64() {
                    self.buffer.add_to_current(f, now);
                    self.event_count.add_to_current(1u64, now);
                    Ok(())
                } else {
                    Err(TallyError::Type {
                        field: self.field.clone(),
                        expected: "numeric".into(),
                        got: format!("{}", val),
                    })
                }
            }
        }
    }

    fn read(&mut self, now: SystemTime) -> FeatureValue {
        // Advance both buffers to expire stale buckets before reading.
        self.buffer.advance_to(now);
        self.event_count.advance_to(now);
        // Use event_count (not count_nonzero on sum buffer) to detect empty state.
        // count_nonzero would incorrectly return 0 for all-zero sums (WR-01).
        let count = self.event_count.sum_all();
        if count == 0 {
            FeatureValue::Missing // Zero events -> Missing
        } else {
            FeatureValue::Float(self.buffer.sum_all())
        }
    }
}

// ======================== MinBucket / MaxBucket ========================

/// Bucket wrapper for MinOp. Default is +INFINITY so any real value replaces it.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
pub struct MinBucket(pub f64);
impl Default for MinBucket {
    fn default() -> Self {
        MinBucket(f64::INFINITY)
    }
}

/// Bucket wrapper for MaxOp. Default is -INFINITY so any real value replaces it.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
pub struct MaxBucket(pub f64);
impl Default for MaxBucket {
    fn default() -> Self {
        MaxBucket(f64::NEG_INFINITY)
    }
}

// ======================== MinOp ========================

/// Tracks the minimum value of a numeric field within a time window.
/// Uses a RingBuffer<MinBucket> with per-bucket min tracking and a
/// parallel event_count buffer to distinguish "no events" from "min is INFINITY".
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MinOp {
    field: String,
    buffer: RingBuffer<MinBucket>,
    event_count: RingBuffer<u64>,
    optional: bool,
}

impl MinOp {
    pub fn new(
        field: impl Into<String>,
        window_duration: std::time::Duration,
        bucket_duration: std::time::Duration,
        optional: bool,
    ) -> Self {
        Self {
            field: field.into(),
            buffer: RingBuffer::new(window_duration, bucket_duration),
            event_count: RingBuffer::new(window_duration, bucket_duration),
            optional,
        }
    }
}

impl Operator for MinOp {
    fn push(
        &mut self,
        event: &serde_json::Value,
        enrichment: Option<&ahash::AHashMap<String, serde_json::Value>>,
        now: SystemTime,
    ) -> Result<(), TallyError> {
        match resolve_field(&self.field, event, enrichment) {
            None => {
                if self.optional {
                    Ok(())
                } else {
                    Err(TallyError::Type {
                        field: self.field.clone(),
                        expected: "numeric".into(),
                        got: "absent".into(),
                    })
                }
            }
            Some(val) => {
                if let Some(f) = val.as_f64() {
                    self.buffer.update_current(
                        |bucket| {
                            if f < bucket.0 {
                                bucket.0 = f;
                            }
                        },
                        now,
                    );
                    self.event_count.add_to_current(1u64, now);
                    Ok(())
                } else {
                    Err(TallyError::Type {
                        field: self.field.clone(),
                        expected: "numeric".into(),
                        got: format!("{}", val),
                    })
                }
            }
        }
    }

    fn read(&mut self, now: SystemTime) -> FeatureValue {
        self.buffer.advance_to(now);
        self.event_count.advance_to(now);
        if self.event_count.sum_all() == 0 {
            return FeatureValue::Missing;
        }
        let min_val = self
            .buffer
            .buckets_iter()
            .filter(|b| b.0 != f64::INFINITY)
            .map(|b| b.0)
            .fold(f64::INFINITY, f64::min);
        if min_val == f64::INFINITY {
            FeatureValue::Missing
        } else {
            FeatureValue::Float(min_val)
        }
    }
}

// ======================== MaxOp ========================

/// Tracks the maximum value of a numeric field within a time window.
/// Mirrors MinOp with MaxBucket(f64::NEG_INFINITY) and f64::max logic.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MaxOp {
    field: String,
    buffer: RingBuffer<MaxBucket>,
    event_count: RingBuffer<u64>,
    optional: bool,
}

impl MaxOp {
    pub fn new(
        field: impl Into<String>,
        window_duration: std::time::Duration,
        bucket_duration: std::time::Duration,
        optional: bool,
    ) -> Self {
        Self {
            field: field.into(),
            buffer: RingBuffer::new(window_duration, bucket_duration),
            event_count: RingBuffer::new(window_duration, bucket_duration),
            optional,
        }
    }
}

impl Operator for MaxOp {
    fn push(
        &mut self,
        event: &serde_json::Value,
        enrichment: Option<&ahash::AHashMap<String, serde_json::Value>>,
        now: SystemTime,
    ) -> Result<(), TallyError> {
        match resolve_field(&self.field, event, enrichment) {
            None => {
                if self.optional {
                    Ok(())
                } else {
                    Err(TallyError::Type {
                        field: self.field.clone(),
                        expected: "numeric".into(),
                        got: "absent".into(),
                    })
                }
            }
            Some(val) => {
                if let Some(f) = val.as_f64() {
                    self.buffer.update_current(
                        |bucket| {
                            if f > bucket.0 {
                                bucket.0 = f;
                            }
                        },
                        now,
                    );
                    self.event_count.add_to_current(1u64, now);
                    Ok(())
                } else {
                    Err(TallyError::Type {
                        field: self.field.clone(),
                        expected: "numeric".into(),
                        got: format!("{}", val),
                    })
                }
            }
        }
    }

    fn read(&mut self, now: SystemTime) -> FeatureValue {
        self.buffer.advance_to(now);
        self.event_count.advance_to(now);
        if self.event_count.sum_all() == 0 {
            return FeatureValue::Missing;
        }
        let max_val = self
            .buffer
            .buckets_iter()
            .filter(|b| b.0 != f64::NEG_INFINITY)
            .map(|b| b.0)
            .fold(f64::NEG_INFINITY, f64::max);
        if max_val == f64::NEG_INFINITY {
            FeatureValue::Missing
        } else {
            FeatureValue::Float(max_val)
        }
    }
}

// ======================== LastOp ========================

/// Stores the most recent value of a field. No window -- always returns
/// the last-seen value regardless of how long ago it was pushed.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LastOp {
    field: String,
    value: FeatureValue,
    timestamp: Option<SystemTime>,
    optional: bool,
}

impl LastOp {
    pub fn new(field: impl Into<String>, optional: bool) -> Self {
        Self {
            field: field.into(),
            value: FeatureValue::Missing,
            timestamp: None,
            optional,
        }
    }
}

impl Operator for LastOp {
    fn push(
        &mut self,
        event: &serde_json::Value,
        enrichment: Option<&ahash::AHashMap<String, serde_json::Value>>,
        now: SystemTime,
    ) -> Result<(), TallyError> {
        match resolve_field(&self.field, event, enrichment) {
            None => {
                if self.optional {
                    Ok(())
                } else {
                    Err(TallyError::Type {
                        field: self.field.clone(),
                        expected: "any".into(),
                        got: "absent".into(),
                    })
                }
            }
            Some(val) => {
                let fv = match val {
                    serde_json::Value::Number(n) => {
                        if let Some(i) = n.as_i64() {
                            FeatureValue::Int(i)
                        } else if let Some(f) = n.as_f64() {
                            FeatureValue::Float(f)
                        } else {
                            FeatureValue::Missing
                        }
                    }
                    serde_json::Value::String(s) => FeatureValue::String(s.clone()),
                    serde_json::Value::Bool(b) => FeatureValue::Int(if *b { 1 } else { 0 }),
                    _ => FeatureValue::Missing,
                };
                // Only update if this event is newer or same time
                let should_update = match self.timestamp {
                    None => true,
                    Some(prev) => now >= prev,
                };
                if should_update {
                    self.value = fv;
                    self.timestamp = Some(now);
                }
                Ok(())
            }
        }
    }

    fn read(&mut self, _now: SystemTime) -> FeatureValue {
        // LastOp has no window -- just return the stored value
        self.value.clone()
    }
}

/// Computes the running average (sum/count) of a numeric field within a window.
/// Uses paired ring buffers: one for count, one for sum. Divides on read.
/// Same Redis-strict type checking as SumOp.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AvgOp {
    field: String,
    count_buffer: RingBuffer<u64>,
    sum_buffer: RingBuffer<f64>,
    optional: bool,
}

impl AvgOp {
    pub fn new(
        field: impl Into<String>,
        window_duration: std::time::Duration,
        bucket_duration: std::time::Duration,
        optional: bool,
    ) -> Self {
        Self {
            field: field.into(),
            count_buffer: RingBuffer::new(window_duration, bucket_duration),
            sum_buffer: RingBuffer::new(window_duration, bucket_duration),
            optional,
        }
    }
}

impl Operator for AvgOp {
    fn push(
        &mut self,
        event: &serde_json::Value,
        enrichment: Option<&ahash::AHashMap<String, serde_json::Value>>,
        now: SystemTime,
    ) -> Result<(), TallyError> {
        match resolve_field(&self.field, event, enrichment) {
            None => {
                if self.optional {
                    Ok(())
                } else {
                    Err(TallyError::Type {
                        field: self.field.clone(),
                        expected: "numeric".into(),
                        got: "absent".into(),
                    })
                }
            }
            Some(val) => {
                if let Some(f) = val.as_f64() {
                    self.count_buffer.add_to_current(1u64, now);
                    self.sum_buffer.add_to_current(f, now);
                    Ok(())
                } else {
                    Err(TallyError::Type {
                        field: self.field.clone(),
                        expected: "numeric".into(),
                        got: format!("{}", val),
                    })
                }
            }
        }
    }

    fn read(&mut self, now: SystemTime) -> FeatureValue {
        // Advance both buffers to expire stale buckets before reading.
        self.count_buffer.advance_to(now);
        self.sum_buffer.advance_to(now);
        let count = self.count_buffer.sum_all();
        if count == 0 {
            FeatureValue::Missing // Zero events -> Missing, not NaN (CONTEXT.md)
        } else {
            let sum = self.sum_buffer.sum_all();
            FeatureValue::Float(sum / count as f64)
        }
    }
}

// ======================== StddevBucket ========================

/// Bucket wrapper for StddevOp. Tracks count, sum, and sum-of-squares per bucket.
/// Standard deviation is computed on read by aggregating across all buckets.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StddevBucket {
    pub count: u64,
    pub sum: f64,
    pub sum_sq: f64,
}

impl Default for StddevBucket {
    fn default() -> Self {
        StddevBucket {
            count: 0,
            sum: 0.0,
            sum_sq: 0.0,
        }
    }
}

// ======================== StddevOp ========================

/// Computes the population standard deviation of a numeric field within a window.
/// Uses bucketed ring buffer with (count, sum, sum_sq) per bucket.
/// On read: variance = (sum_sq / count) - (mean * mean), stddev = sqrt(variance).
/// Returns 0.0 if count < 2.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StddevOp {
    field: String,
    buffer: RingBuffer<StddevBucket>,
    optional: bool,
}

impl StddevOp {
    pub fn new(
        field: impl Into<String>,
        window_duration: std::time::Duration,
        bucket_duration: std::time::Duration,
        optional: bool,
    ) -> Self {
        Self {
            field: field.into(),
            buffer: RingBuffer::new(window_duration, bucket_duration),
            optional,
        }
    }
}

impl Operator for StddevOp {
    fn push(
        &mut self,
        event: &serde_json::Value,
        enrichment: Option<&ahash::AHashMap<String, serde_json::Value>>,
        now: SystemTime,
    ) -> Result<(), TallyError> {
        match resolve_field(&self.field, event, enrichment) {
            None => {
                if self.optional {
                    Ok(())
                } else {
                    Err(TallyError::Type {
                        field: self.field.clone(),
                        expected: "numeric".into(),
                        got: "absent".into(),
                    })
                }
            }
            Some(val) => {
                if let Some(f) = val.as_f64() {
                    self.buffer.update_current(
                        |bucket| {
                            bucket.count += 1;
                            bucket.sum += f;
                            bucket.sum_sq += f * f;
                        },
                        now,
                    );
                    Ok(())
                } else {
                    Err(TallyError::Type {
                        field: self.field.clone(),
                        expected: "numeric".into(),
                        got: format!("{}", val),
                    })
                }
            }
        }
    }

    fn read(&mut self, now: SystemTime) -> FeatureValue {
        self.buffer.advance_to(now);
        let mut total_count: u64 = 0;
        let mut total_sum: f64 = 0.0;
        let mut total_sum_sq: f64 = 0.0;
        for bucket in self.buffer.buckets_iter() {
            total_count += bucket.count;
            total_sum += bucket.sum;
            total_sum_sq += bucket.sum_sq;
        }
        if total_count < 2 {
            if total_count == 0 {
                return FeatureValue::Missing;
            }
            return FeatureValue::Float(0.0);
        }
        let mean = total_sum / total_count as f64;
        let variance = (total_sum_sq / total_count as f64) - (mean * mean);
        // Floating-point rounding can produce tiny negative variance
        let stddev = if variance < 0.0 { 0.0 } else { variance.sqrt() };
        FeatureValue::Float(stddev)
    }
}

// ======================== PercentileBucket ========================

/// Bucket wrapper for PercentileOp. Stores a sorted Vec<f64> of all values
/// pushed into this time bucket. On read, values from all non-expired buckets
/// are merged and the quantile is computed exactly.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[derive(Default)]
pub struct PercentileBucket {
    pub values: Vec<f64>,
}


// ======================== PercentileOp ========================

/// Computes an approximate percentile of a numeric field within a window.
/// Uses sorted Vec<f64> per ring bucket (exact within bucket granularity).
/// On read: merges all non-expired bucket values and computes the quantile
/// using linear interpolation (same as numpy's default method).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PercentileOp {
    field: String,
    quantile: f64,
    buffer: RingBuffer<PercentileBucket>,
    optional: bool,
}

impl PercentileOp {
    pub fn new(
        field: impl Into<String>,
        quantile: f64,
        window_duration: std::time::Duration,
        bucket_duration: std::time::Duration,
        optional: bool,
    ) -> Self {
        Self {
            field: field.into(),
            quantile: quantile.clamp(0.0, 1.0),
            buffer: RingBuffer::new(window_duration, bucket_duration),
            optional,
        }
    }

    /// Compute the quantile from a sorted slice using linear interpolation.
    fn compute_quantile(sorted: &[f64], q: f64) -> f64 {
        if sorted.is_empty() {
            return 0.0;
        }
        if sorted.len() == 1 {
            return sorted[0];
        }
        if q <= 0.0 {
            return sorted[0];
        }
        if q >= 1.0 {
            return sorted[sorted.len() - 1];
        }
        let index = q * (sorted.len() - 1) as f64;
        let lower = index.floor() as usize;
        let upper = index.ceil() as usize;
        if lower == upper {
            sorted[lower]
        } else {
            let frac = index - lower as f64;
            sorted[lower] * (1.0 - frac) + sorted[upper] * frac
        }
    }
}

impl Operator for PercentileOp {
    fn push(
        &mut self,
        event: &serde_json::Value,
        enrichment: Option<&ahash::AHashMap<String, serde_json::Value>>,
        now: SystemTime,
    ) -> Result<(), TallyError> {
        match resolve_field(&self.field, event, enrichment) {
            None => {
                if self.optional {
                    Ok(())
                } else {
                    Err(TallyError::Type {
                        field: self.field.clone(),
                        expected: "numeric".into(),
                        got: "absent".into(),
                    })
                }
            }
            Some(val) => {
                if let Some(f) = val.as_f64() {
                    self.buffer.update_current(
                        |bucket| {
                            bucket.values.push(f);
                        },
                        now,
                    );
                    Ok(())
                } else {
                    Err(TallyError::Type {
                        field: self.field.clone(),
                        expected: "numeric".into(),
                        got: format!("{}", val),
                    })
                }
            }
        }
    }

    fn read(&mut self, now: SystemTime) -> FeatureValue {
        self.buffer.advance_to(now);
        // Collect all values from all buckets, then sort
        let mut all_values: Vec<f64> = Vec::new();
        for bucket in self.buffer.buckets_iter() {
            all_values.extend_from_slice(&bucket.values);
        }
        if all_values.is_empty() {
            return FeatureValue::Missing;
        }
        all_values.sort_unstable_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        FeatureValue::Float(Self::compute_quantile(&all_values, self.quantile))
    }
}

// ======================== LagOp ========================

/// Returns the Nth-oldest value for an entity key. Event-count-based, no window.
/// Stores the last N values in a VecDeque ring buffer.
/// `read()` returns the front (oldest) value, which is the value from N events ago.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LagOp {
    field: String,
    n: usize,
    values: VecDeque<FeatureValue>,
    optional: bool,
}

impl LagOp {
    pub fn new(field: impl Into<String>, n: usize, optional: bool) -> Self {
        Self {
            field: field.into(),
            n,
            values: VecDeque::with_capacity(n),
            optional,
        }
    }
}

impl Operator for LagOp {
    fn push(
        &mut self,
        event: &serde_json::Value,
        enrichment: Option<&ahash::AHashMap<String, serde_json::Value>>,
        _now: SystemTime,
    ) -> Result<(), TallyError> {
        match resolve_field(&self.field, event, enrichment) {
            None => {
                if self.optional {
                    Ok(())
                } else {
                    Err(TallyError::Type {
                        field: self.field.clone(),
                        expected: "any".into(),
                        got: "absent".into(),
                    })
                }
            }
            Some(val) => {
                let fv = match val {
                    serde_json::Value::Number(n) => {
                        if let Some(i) = n.as_i64() {
                            FeatureValue::Int(i)
                        } else if let Some(f) = n.as_f64() {
                            FeatureValue::Float(f)
                        } else {
                            FeatureValue::Missing
                        }
                    }
                    serde_json::Value::String(s) => FeatureValue::String(s.clone()),
                    serde_json::Value::Bool(b) => FeatureValue::Int(if *b { 1 } else { 0 }),
                    _ => FeatureValue::Missing,
                };
                self.values.push_back(fv);
                if self.values.len() > self.n {
                    self.values.pop_front();
                }
                Ok(())
            }
        }
    }

    fn read(&mut self, _now: SystemTime) -> FeatureValue {
        // Return the oldest value (N events ago). If buffer not full yet, Missing.
        if self.values.len() == self.n {
            self.values
                .front()
                .cloned()
                .unwrap_or(FeatureValue::Missing)
        } else {
            FeatureValue::Missing
        }
    }
}

// ======================== EmaOp ========================

/// Exponential moving average with time-based decay. O(1) state.
/// alpha = exp(-ln(2) * elapsed_secs / half_life)
/// current = alpha * current + (1 - alpha) * value
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EmaOp {
    field: String,
    half_life_secs: f64,
    current: f64,
    last_time: Option<SystemTime>,
    initialized: bool,
    optional: bool,
}

impl EmaOp {
    pub fn new(field: impl Into<String>, half_life_secs: f64, optional: bool) -> Self {
        Self {
            field: field.into(),
            half_life_secs,
            current: 0.0,
            last_time: None,
            initialized: false,
            optional,
        }
    }
}

impl Operator for EmaOp {
    fn push(
        &mut self,
        event: &serde_json::Value,
        enrichment: Option<&ahash::AHashMap<String, serde_json::Value>>,
        now: SystemTime,
    ) -> Result<(), TallyError> {
        match resolve_field(&self.field, event, enrichment) {
            None => {
                if self.optional {
                    Ok(())
                } else {
                    Err(TallyError::Type {
                        field: self.field.clone(),
                        expected: "numeric".into(),
                        got: "absent".into(),
                    })
                }
            }
            Some(val) => {
                if let Some(value) = val.as_f64() {
                    if !self.initialized {
                        self.current = value;
                        self.initialized = true;
                    } else if let Some(prev_time) = self.last_time {
                        let elapsed = now
                            .duration_since(prev_time)
                            .unwrap_or(std::time::Duration::ZERO)
                            .as_secs_f64();
                        let alpha = (-std::f64::consts::LN_2 * elapsed / self.half_life_secs).exp();
                        self.current = alpha * self.current + (1.0 - alpha) * value;
                    } else {
                        // initialized but no last_time (shouldn't happen, but handle gracefully)
                        self.current = value;
                    }
                    self.last_time = Some(now);
                    Ok(())
                } else {
                    Err(TallyError::Type {
                        field: self.field.clone(),
                        expected: "numeric".into(),
                        got: format!("{}", val),
                    })
                }
            }
        }
    }

    fn read(&mut self, _now: SystemTime) -> FeatureValue {
        if self.initialized {
            FeatureValue::Float(self.current)
        } else {
            FeatureValue::Missing
        }
    }
}

// ======================== LastNOp ========================

/// Stores the last N values of a field. Returns them as a JSON array string.
/// Unlike LagOp (returns ONE value from N ago), LastNOp returns ALL N recent values.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LastNOp {
    field: String,
    n: usize,
    values: VecDeque<FeatureValue>,
    optional: bool,
}

impl LastNOp {
    pub fn new(field: impl Into<String>, n: usize, optional: bool) -> Self {
        Self {
            field: field.into(),
            n,
            values: VecDeque::with_capacity(n),
            optional,
        }
    }
}

impl Operator for LastNOp {
    fn push(
        &mut self,
        event: &serde_json::Value,
        enrichment: Option<&ahash::AHashMap<String, serde_json::Value>>,
        _now: SystemTime,
    ) -> Result<(), TallyError> {
        match resolve_field(&self.field, event, enrichment) {
            None => {
                if self.optional {
                    Ok(())
                } else {
                    Err(TallyError::Type {
                        field: self.field.clone(),
                        expected: "any".into(),
                        got: "absent".into(),
                    })
                }
            }
            Some(val) => {
                let fv = match val {
                    serde_json::Value::Number(n) => {
                        if let Some(i) = n.as_i64() {
                            FeatureValue::Int(i)
                        } else if let Some(f) = n.as_f64() {
                            FeatureValue::Float(f)
                        } else {
                            FeatureValue::Missing
                        }
                    }
                    serde_json::Value::String(s) => FeatureValue::String(s.clone()),
                    serde_json::Value::Bool(b) => FeatureValue::Int(if *b { 1 } else { 0 }),
                    _ => FeatureValue::Missing,
                };
                self.values.push_back(fv);
                if self.values.len() > self.n {
                    self.values.pop_front();
                }
                Ok(())
            }
        }
    }

    fn read(&mut self, _now: SystemTime) -> FeatureValue {
        if self.values.is_empty() {
            return FeatureValue::Missing;
        }
        // Return as JSON array string since FeatureValue has no List variant
        let arr: Vec<serde_json::Value> = self.values.iter().map(|v| v.to_json_value()).collect();
        let json_str = serde_json::to_string(&arr).unwrap_or_else(|_| "[]".to_string());
        FeatureValue::String(json_str)
    }
}

// ======================== FirstOp ========================

/// Stores the first value ever seen for an entity key. Never overwrites.
/// Like LastOp but only sets on the first event.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FirstOp {
    field: String,
    value: FeatureValue,
    timestamp: Option<SystemTime>,
    optional: bool,
}

impl FirstOp {
    pub fn new(field: impl Into<String>, optional: bool) -> Self {
        Self {
            field: field.into(),
            value: FeatureValue::Missing,
            timestamp: None,
            optional,
        }
    }
}

impl Operator for FirstOp {
    fn push(
        &mut self,
        event: &serde_json::Value,
        enrichment: Option<&ahash::AHashMap<String, serde_json::Value>>,
        now: SystemTime,
    ) -> Result<(), TallyError> {
        // Only store the first value; ignore all subsequent events
        if self.timestamp.is_some() {
            return Ok(());
        }
        match resolve_field(&self.field, event, enrichment) {
            None => {
                if self.optional {
                    Ok(())
                } else {
                    Err(TallyError::Type {
                        field: self.field.clone(),
                        expected: "any".into(),
                        got: "absent".into(),
                    })
                }
            }
            Some(val) => {
                let fv = match val {
                    serde_json::Value::Number(n) => {
                        if let Some(i) = n.as_i64() {
                            FeatureValue::Int(i)
                        } else if let Some(f) = n.as_f64() {
                            FeatureValue::Float(f)
                        } else {
                            FeatureValue::Missing
                        }
                    }
                    serde_json::Value::String(s) => FeatureValue::String(s.clone()),
                    serde_json::Value::Bool(b) => FeatureValue::Int(if *b { 1 } else { 0 }),
                    _ => FeatureValue::Missing,
                };
                self.value = fv;
                self.timestamp = Some(now);
                Ok(())
            }
        }
    }

    fn read(&mut self, _now: SystemTime) -> FeatureValue {
        self.value.clone()
    }
}

// ======================== ValBucket ========================

/// Wrapper for Vec<f64> to use in RingBuffer (needs Default + Clone).
/// Stores per-bucket value lists for retraction in ExactMin/ExactMax operators.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[derive(Default)]
pub struct ValBucket(pub Vec<f64>);

// ======================== ExactMinOp ========================

/// Retractable min using BTreeMap<OrderedFloat<f64>, u32> for exact windowed minimum.
/// Tracks all values in a sorted map with counts, plus per-bucket value lists
/// in the ring buffer for retraction on bucket expiry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExactMinOp {
    field: String,
    sorted_values: BTreeMap<OrderedFloat<f64>, u32>,
    bucket_values: RingBuffer<ValBucket>,
    event_count: RingBuffer<u64>,
    optional: bool,
}

impl ExactMinOp {
    pub fn new(
        field: impl Into<String>,
        window_duration: std::time::Duration,
        bucket_duration: std::time::Duration,
        optional: bool,
    ) -> Self {
        Self {
            field: field.into(),
            sorted_values: BTreeMap::new(),
            bucket_values: RingBuffer::new(window_duration, bucket_duration),
            event_count: RingBuffer::new(window_duration, bucket_duration),
            optional,
        }
    }

    /// Retract expired bucket values from the BTreeMap.
    fn retract_bucket_values(&mut self) {
        // Collect all values from all buckets, rebuild sorted_values from scratch.
        // This is simpler and correct: on each read we rebuild from current bucket state.
        self.sorted_values.clear();
        for bucket in self.bucket_values.buckets_iter() {
            for &val in &bucket.0 {
                *self.sorted_values.entry(OrderedFloat(val)).or_insert(0) += 1;
            }
        }
    }
}

impl Operator for ExactMinOp {
    fn push(
        &mut self,
        event: &serde_json::Value,
        enrichment: Option<&ahash::AHashMap<String, serde_json::Value>>,
        now: SystemTime,
    ) -> Result<(), TallyError> {
        match resolve_field(&self.field, event, enrichment) {
            None => {
                if self.optional {
                    Ok(())
                } else {
                    Err(TallyError::Type {
                        field: self.field.clone(),
                        expected: "numeric".into(),
                        got: "absent".into(),
                    })
                }
            }
            Some(val) => {
                if let Some(f) = val.as_f64() {
                    // Add to sorted map
                    *self.sorted_values.entry(OrderedFloat(f)).or_insert(0) += 1;
                    // Add to current bucket's value list
                    self.bucket_values.update_current(
                        |bucket| {
                            bucket.0.push(f);
                        },
                        now,
                    );
                    self.event_count.add_to_current(1u64, now);
                    Ok(())
                } else {
                    Err(TallyError::Type {
                        field: self.field.clone(),
                        expected: "numeric".into(),
                        got: format!("{}", val),
                    })
                }
            }
        }
    }

    fn read(&mut self, now: SystemTime) -> FeatureValue {
        // Advance ring buffers to expire old buckets
        self.bucket_values.advance_to(now);
        self.event_count.advance_to(now);
        if self.event_count.sum_all() == 0 {
            return FeatureValue::Missing;
        }
        // Rebuild sorted_values from non-expired buckets
        self.retract_bucket_values();
        match self.sorted_values.keys().next() {
            Some(key) => FeatureValue::Float(key.into_inner()),
            None => FeatureValue::Missing,
        }
    }
}

// ======================== ExactMaxOp ========================

/// Retractable max using BTreeMap<OrderedFloat<f64>, u32> for exact windowed maximum.
/// Same approach as ExactMinOp but returns the last (largest) key.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExactMaxOp {
    field: String,
    sorted_values: BTreeMap<OrderedFloat<f64>, u32>,
    bucket_values: RingBuffer<ValBucket>,
    event_count: RingBuffer<u64>,
    optional: bool,
}

impl ExactMaxOp {
    pub fn new(
        field: impl Into<String>,
        window_duration: std::time::Duration,
        bucket_duration: std::time::Duration,
        optional: bool,
    ) -> Self {
        Self {
            field: field.into(),
            sorted_values: BTreeMap::new(),
            bucket_values: RingBuffer::new(window_duration, bucket_duration),
            event_count: RingBuffer::new(window_duration, bucket_duration),
            optional,
        }
    }

    fn retract_bucket_values(&mut self) {
        self.sorted_values.clear();
        for bucket in self.bucket_values.buckets_iter() {
            for &val in &bucket.0 {
                *self.sorted_values.entry(OrderedFloat(val)).or_insert(0) += 1;
            }
        }
    }
}

impl Operator for ExactMaxOp {
    fn push(
        &mut self,
        event: &serde_json::Value,
        enrichment: Option<&ahash::AHashMap<String, serde_json::Value>>,
        now: SystemTime,
    ) -> Result<(), TallyError> {
        match resolve_field(&self.field, event, enrichment) {
            None => {
                if self.optional {
                    Ok(())
                } else {
                    Err(TallyError::Type {
                        field: self.field.clone(),
                        expected: "numeric".into(),
                        got: "absent".into(),
                    })
                }
            }
            Some(val) => {
                if let Some(f) = val.as_f64() {
                    *self.sorted_values.entry(OrderedFloat(f)).or_insert(0) += 1;
                    self.bucket_values.update_current(
                        |bucket| {
                            bucket.0.push(f);
                        },
                        now,
                    );
                    self.event_count.add_to_current(1u64, now);
                    Ok(())
                } else {
                    Err(TallyError::Type {
                        field: self.field.clone(),
                        expected: "numeric".into(),
                        got: format!("{}", val),
                    })
                }
            }
        }
    }

    fn read(&mut self, now: SystemTime) -> FeatureValue {
        self.bucket_values.advance_to(now);
        self.event_count.advance_to(now);
        if self.event_count.sum_all() == 0 {
            return FeatureValue::Missing;
        }
        self.retract_bucket_values();
        match self.sorted_values.keys().next_back() {
            Some(key) => FeatureValue::Float(key.into_inner()),
            None => FeatureValue::Missing,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    fn ts(secs: u64) -> SystemTime {
        UNIX_EPOCH + Duration::from_secs(secs)
    }

    // ======================== CountOp Tests ========================

    #[test]
    fn test_count_new_creates_successfully() {
        let op = CountOp::new(Duration::from_secs(30 * 60), Duration::from_secs(60));
        // Should have 30 buckets (30m / 1m)
        assert_eq!(op.buffer.num_buckets(), 30);
    }

    #[test]
    fn test_count_push_one_event_read_returns_int_1() {
        let mut op = CountOp::new(Duration::from_secs(30 * 60), Duration::from_secs(60));
        let t = ts(1000 * 60);
        op.push(&json!({}), None, t).unwrap();
        assert_eq!(op.read(t), FeatureValue::Int(1));
    }

    #[test]
    fn test_count_push_5_events_same_timestamp() {
        let mut op = CountOp::new(Duration::from_secs(30 * 60), Duration::from_secs(60));
        let t = ts(1000 * 60);
        for _ in 0..5 {
            op.push(&json!({}), None, t).unwrap();
        }
        assert_eq!(op.read(t), FeatureValue::Int(5));
    }

    #[test]
    fn test_count_push_events_across_multiple_buckets() {
        let mut op = CountOp::new(Duration::from_secs(30 * 60), Duration::from_secs(60));
        let t0 = ts(1000 * 60);
        op.push(&json!({}), None, t0).unwrap();
        op.push(&json!({}), None, t0 + Duration::from_secs(60))
            .unwrap();
        op.push(&json!({}), None, t0 + Duration::from_secs(120))
            .unwrap();
        // All 3 events within window, should sum to 3
        assert_eq!(op.read(t0 + Duration::from_secs(120)), FeatureValue::Int(3));
    }

    #[test]
    fn test_count_read_returns_missing_after_window_expires() {
        let mut op = CountOp::new(Duration::from_secs(5 * 60), Duration::from_secs(60));
        let t0 = ts(1000 * 60);
        op.push(&json!({}), None, t0).unwrap();
        assert_eq!(op.read(t0), FeatureValue::Int(1));

        // Advance past the full window (10 minutes > 5 minute window)
        let t_future = t0 + Duration::from_secs(10 * 60);
        assert_eq!(op.read(t_future), FeatureValue::Missing);
    }

    #[test]
    fn test_count_ignores_event_content() {
        let mut op = CountOp::new(Duration::from_secs(30 * 60), Duration::from_secs(60));
        let t = ts(1000 * 60);
        // Count should succeed regardless of event shape
        op.push(&json!({"amount": 50.0, "status": "ok"}), None, t)
            .unwrap();
        assert_eq!(op.read(t), FeatureValue::Int(1));
    }

    #[test]
    fn test_count_push_with_various_json_shapes_succeeds() {
        let mut op = CountOp::new(Duration::from_secs(30 * 60), Duration::from_secs(60));
        let t = ts(1000 * 60);
        // Empty object
        assert!(op.push(&json!({}), None, t).is_ok());
        // Nested object
        assert!(op.push(&json!({"nested": {"deep": true}}), None, t).is_ok());
        // Array value
        assert!(op.push(&json!({"list": [1, 2, 3]}), None, t).is_ok());
        // String value
        assert!(op.push(&json!({"name": "test"}), None, t).is_ok());
        // Null value
        assert!(op.push(&json!(null), None, t).is_ok());
        assert_eq!(op.read(t), FeatureValue::Int(5));
    }

    #[test]
    fn test_count_read_with_no_events_returns_missing() {
        let mut op = CountOp::new(Duration::from_secs(30 * 60), Duration::from_secs(60));
        let t = ts(1000 * 60);
        // No push -- read should return Missing
        assert_eq!(op.read(t), FeatureValue::Missing);
    }

    // ======================== SumOp Tests ========================

    #[test]
    fn test_sum_push_two_events_read_returns_sum() {
        let mut op = SumOp::new(
            "amount",
            Duration::from_secs(60 * 60),
            Duration::from_secs(60),
            false,
        );
        let t = ts(1000 * 60);
        op.push(&json!({"amount": 50.0}), None, t).unwrap();
        op.push(&json!({"amount": 30.0}), None, t).unwrap();
        assert_eq!(op.read(t), FeatureValue::Float(80.0));
    }

    #[test]
    fn test_sum_type_error_on_string_field() {
        let mut op = SumOp::new(
            "amount",
            Duration::from_secs(60 * 60),
            Duration::from_secs(60),
            false,
        );
        let t = ts(1000 * 60);
        let result = op.push(&json!({"amount": "not_a_number"}), None, t);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, TallyError::Type { ref field, .. } if field == "amount"));
    }

    #[test]
    fn test_sum_non_optional_missing_field_returns_type_error() {
        let mut op = SumOp::new(
            "amount",
            Duration::from_secs(60 * 60),
            Duration::from_secs(60),
            false,
        );
        let t = ts(1000 * 60);
        let result = op.push(&json!({}), None, t);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, TallyError::Type { ref got, .. } if got == "absent"));
    }

    #[test]
    fn test_sum_optional_missing_field_returns_ok() {
        let mut op = SumOp::new(
            "amount",
            Duration::from_secs(60 * 60),
            Duration::from_secs(60),
            true,
        );
        let t = ts(1000 * 60);
        // Push event without the field -- should succeed silently
        assert!(op.push(&json!({}), None, t).is_ok());
        // No numeric data was added, so read returns Missing
        assert_eq!(op.read(t), FeatureValue::Missing);
    }

    #[test]
    fn test_sum_optional_missing_field_does_not_affect_existing_sum() {
        let mut op = SumOp::new(
            "amount",
            Duration::from_secs(60 * 60),
            Duration::from_secs(60),
            true,
        );
        let t = ts(1000 * 60);
        op.push(&json!({"amount": 50.0}), None, t).unwrap();
        // Push event without field -- should not affect the sum
        op.push(&json!({}), None, t).unwrap();
        assert_eq!(op.read(t), FeatureValue::Float(50.0));
    }

    #[test]
    fn test_sum_with_int_values_coerces_to_f64() {
        let mut op = SumOp::new(
            "amount",
            Duration::from_secs(60 * 60),
            Duration::from_secs(60),
            false,
        );
        let t = ts(1000 * 60);
        op.push(&json!({"amount": 50}), None, t).unwrap(); // Int, not Float
        assert_eq!(op.read(t), FeatureValue::Float(50.0));
    }

    #[test]
    fn test_sum_read_with_zero_events_returns_missing() {
        let mut op = SumOp::new(
            "amount",
            Duration::from_secs(60 * 60),
            Duration::from_secs(60),
            false,
        );
        let t = ts(1000 * 60);
        assert_eq!(op.read(t), FeatureValue::Missing);
    }

    #[test]
    fn test_sum_expires_correctly_when_time_advances_past_window() {
        let mut op = SumOp::new(
            "amount",
            Duration::from_secs(5 * 60),
            Duration::from_secs(60),
            false,
        );
        let t0 = ts(1000 * 60);
        op.push(&json!({"amount": 100.0}), None, t0).unwrap();
        assert_eq!(op.read(t0), FeatureValue::Float(100.0));

        // Advance past the full window
        let t_future = t0 + Duration::from_secs(10 * 60);
        assert_eq!(op.read(t_future), FeatureValue::Missing);
    }

    // ======================== AvgOp Tests ========================

    #[test]
    fn test_avg_push_three_events_returns_average() {
        let mut op = AvgOp::new(
            "amount",
            Duration::from_secs(60 * 60),
            Duration::from_secs(60),
            false,
        );
        let t = ts(1000 * 60);
        op.push(&json!({"amount": 10.0}), None, t).unwrap();
        op.push(&json!({"amount": 20.0}), None, t).unwrap();
        op.push(&json!({"amount": 30.0}), None, t).unwrap();
        assert_eq!(op.read(t), FeatureValue::Float(20.0));
    }

    #[test]
    fn test_avg_read_with_zero_events_returns_missing() {
        let mut op = AvgOp::new(
            "amount",
            Duration::from_secs(60 * 60),
            Duration::from_secs(60),
            false,
        );
        let t = ts(1000 * 60);
        // No push -- should return Missing, not NaN or 0
        assert_eq!(op.read(t), FeatureValue::Missing);
    }

    #[test]
    fn test_avg_type_error_on_non_numeric_field() {
        let mut op = AvgOp::new(
            "amount",
            Duration::from_secs(60 * 60),
            Duration::from_secs(60),
            false,
        );
        let t = ts(1000 * 60);
        let result = op.push(&json!({"amount": "hello"}), None, t);
        assert!(result.is_err());
        assert!(
            matches!(result.unwrap_err(), TallyError::Type { ref field, .. } if field == "amount")
        );
    }

    #[test]
    fn test_avg_non_optional_missing_field_returns_type_error() {
        let mut op = AvgOp::new(
            "amount",
            Duration::from_secs(60 * 60),
            Duration::from_secs(60),
            false,
        );
        let t = ts(1000 * 60);
        let result = op.push(&json!({}), None, t);
        assert!(result.is_err());
    }

    #[test]
    fn test_avg_optional_missing_field_does_not_affect_average() {
        let mut op = AvgOp::new(
            "amount",
            Duration::from_secs(60 * 60),
            Duration::from_secs(60),
            true,
        );
        let t = ts(1000 * 60);
        op.push(&json!({"amount": 10.0}), None, t).unwrap();
        op.push(&json!({"amount": 20.0}), None, t).unwrap();
        // Push event without field -- should not affect average
        op.push(&json!({}), None, t).unwrap();
        // Average should be (10+20)/2 = 15.0, not (10+20)/3
        assert_eq!(op.read(t), FeatureValue::Float(15.0));
    }

    #[test]
    fn test_avg_optional_only_missing_fields_returns_missing() {
        let mut op = AvgOp::new(
            "amount",
            Duration::from_secs(60 * 60),
            Duration::from_secs(60),
            true,
        );
        let t = ts(1000 * 60);
        op.push(&json!({}), None, t).unwrap();
        op.push(&json!({}), None, t).unwrap();
        // All events had missing field, count is 0 -> Missing
        assert_eq!(op.read(t), FeatureValue::Missing);
    }

    #[test]
    fn test_avg_expires_correctly_when_time_advances_past_window() {
        let mut op = AvgOp::new(
            "amount",
            Duration::from_secs(5 * 60),
            Duration::from_secs(60),
            false,
        );
        let t0 = ts(1000 * 60);
        op.push(&json!({"amount": 100.0}), None, t0).unwrap();
        assert_eq!(op.read(t0), FeatureValue::Float(100.0));

        // Advance past the full window
        let t_future = t0 + Duration::from_secs(10 * 60);
        assert_eq!(op.read(t_future), FeatureValue::Missing);
    }

    #[test]
    fn test_avg_with_int_values() {
        let mut op = AvgOp::new(
            "amount",
            Duration::from_secs(60 * 60),
            Duration::from_secs(60),
            false,
        );
        let t = ts(1000 * 60);
        op.push(&json!({"amount": 10}), None, t).unwrap();
        op.push(&json!({"amount": 20}), None, t).unwrap();
        assert_eq!(op.read(t), FeatureValue::Float(15.0));
    }

    // ======================== Negative Number Tests ========================

    #[test]
    fn test_sum_with_negative_values() {
        let mut op = SumOp::new(
            "amount",
            Duration::from_secs(3600),
            Duration::from_secs(60),
            false,
        );
        let t = ts(1000 * 60);
        op.push(&json!({"amount": -10.0}), None, t).unwrap();
        op.push(&json!({"amount": -20.0}), None, t).unwrap();
        assert_eq!(op.read(t), FeatureValue::Float(-30.0));
    }

    #[test]
    fn test_sum_with_mixed_positive_and_negative() {
        let mut op = SumOp::new(
            "amount",
            Duration::from_secs(3600),
            Duration::from_secs(60),
            false,
        );
        let t = ts(1000 * 60);
        op.push(&json!({"amount": 100.0}), None, t).unwrap();
        op.push(&json!({"amount": -30.0}), None, t).unwrap();
        op.push(&json!({"amount": -20.0}), None, t).unwrap();
        assert_eq!(op.read(t), FeatureValue::Float(50.0));
    }

    #[test]
    fn test_avg_with_negative_values() {
        let mut op = AvgOp::new(
            "amount",
            Duration::from_secs(3600),
            Duration::from_secs(60),
            false,
        );
        let t = ts(1000 * 60);
        op.push(&json!({"amount": -10.0}), None, t).unwrap();
        op.push(&json!({"amount": -30.0}), None, t).unwrap();
        assert_eq!(op.read(t), FeatureValue::Float(-20.0));
    }

    #[test]
    fn test_avg_with_mixed_positive_and_negative() {
        let mut op = AvgOp::new(
            "amount",
            Duration::from_secs(3600),
            Duration::from_secs(60),
            false,
        );
        let t = ts(1000 * 60);
        op.push(&json!({"amount": 10.0}), None, t).unwrap();
        op.push(&json!({"amount": -30.0}), None, t).unwrap();
        assert_eq!(op.read(t), FeatureValue::Float(-10.0));
    }

    // ======================== MinOp Tests ========================

    #[test]
    fn test_min_three_events_returns_minimum() {
        let mut op = MinOp::new(
            "amount",
            Duration::from_secs(3600),
            Duration::from_secs(60),
            false,
        );
        let t = ts(1000 * 60);
        op.push(&json!({"amount": 10.0}), None, t).unwrap();
        op.push(&json!({"amount": 5.0}), None, t).unwrap();
        op.push(&json!({"amount": 20.0}), None, t).unwrap();
        assert_eq!(op.read(t), FeatureValue::Float(5.0));
    }

    #[test]
    fn test_min_zero_events_returns_missing() {
        let mut op = MinOp::new(
            "amount",
            Duration::from_secs(3600),
            Duration::from_secs(60),
            false,
        );
        let t = ts(1000 * 60);
        assert_eq!(op.read(t), FeatureValue::Missing);
    }

    #[test]
    fn test_min_expires_old_buckets() {
        let mut op = MinOp::new(
            "amount",
            Duration::from_secs(5 * 60),
            Duration::from_secs(60),
            false,
        );
        let t0 = ts(1000 * 60);
        op.push(&json!({"amount": 42.0}), None, t0).unwrap();
        assert_eq!(op.read(t0), FeatureValue::Float(42.0));
        // Advance past the full window (2x window)
        let t_future = t0 + Duration::from_secs(10 * 60);
        assert_eq!(op.read(t_future), FeatureValue::Missing);
    }

    #[test]
    fn test_min_optional_skips_missing_field() {
        let mut op = MinOp::new(
            "amount",
            Duration::from_secs(3600),
            Duration::from_secs(60),
            true,
        );
        let t = ts(1000 * 60);
        op.push(&json!({"amount": 10.0}), None, t).unwrap();
        // Push event without field -- should succeed silently
        assert!(op.push(&json!({}), None, t).is_ok());
        assert_eq!(op.read(t), FeatureValue::Float(10.0));
    }

    #[test]
    fn test_min_non_optional_missing_field_errors() {
        let mut op = MinOp::new(
            "amount",
            Duration::from_secs(3600),
            Duration::from_secs(60),
            false,
        );
        let t = ts(1000 * 60);
        let result = op.push(&json!({}), None, t);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), TallyError::Type { ref got, .. } if got == "absent"));
    }

    #[test]
    fn test_min_type_error_on_string_field() {
        let mut op = MinOp::new(
            "amount",
            Duration::from_secs(3600),
            Duration::from_secs(60),
            false,
        );
        let t = ts(1000 * 60);
        let result = op.push(&json!({"amount": "not_a_number"}), None, t);
        assert!(result.is_err());
        assert!(
            matches!(result.unwrap_err(), TallyError::Type { ref field, .. } if field == "amount")
        );
    }

    // ======================== MaxOp Tests ========================

    #[test]
    fn test_max_three_events_returns_maximum() {
        let mut op = MaxOp::new(
            "amount",
            Duration::from_secs(3600),
            Duration::from_secs(60),
            false,
        );
        let t = ts(1000 * 60);
        op.push(&json!({"amount": 10.0}), None, t).unwrap();
        op.push(&json!({"amount": 5.0}), None, t).unwrap();
        op.push(&json!({"amount": 20.0}), None, t).unwrap();
        assert_eq!(op.read(t), FeatureValue::Float(20.0));
    }

    #[test]
    fn test_max_zero_events_returns_missing() {
        let mut op = MaxOp::new(
            "amount",
            Duration::from_secs(3600),
            Duration::from_secs(60),
            false,
        );
        let t = ts(1000 * 60);
        assert_eq!(op.read(t), FeatureValue::Missing);
    }

    #[test]
    fn test_max_expires_old_buckets() {
        let mut op = MaxOp::new(
            "amount",
            Duration::from_secs(5 * 60),
            Duration::from_secs(60),
            false,
        );
        let t0 = ts(1000 * 60);
        op.push(&json!({"amount": 42.0}), None, t0).unwrap();
        assert_eq!(op.read(t0), FeatureValue::Float(42.0));
        let t_future = t0 + Duration::from_secs(10 * 60);
        assert_eq!(op.read(t_future), FeatureValue::Missing);
    }

    #[test]
    fn test_max_optional_skips_missing_field() {
        let mut op = MaxOp::new(
            "amount",
            Duration::from_secs(3600),
            Duration::from_secs(60),
            true,
        );
        let t = ts(1000 * 60);
        op.push(&json!({"amount": 10.0}), None, t).unwrap();
        assert!(op.push(&json!({}), None, t).is_ok());
        assert_eq!(op.read(t), FeatureValue::Float(10.0));
    }

    #[test]
    fn test_max_non_optional_missing_field_errors() {
        let mut op = MaxOp::new(
            "amount",
            Duration::from_secs(3600),
            Duration::from_secs(60),
            false,
        );
        let t = ts(1000 * 60);
        let result = op.push(&json!({}), None, t);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), TallyError::Type { ref got, .. } if got == "absent"));
    }

    #[test]
    fn test_max_type_error_on_string_field() {
        let mut op = MaxOp::new(
            "amount",
            Duration::from_secs(3600),
            Duration::from_secs(60),
            false,
        );
        let t = ts(1000 * 60);
        let result = op.push(&json!({"amount": "not_a_number"}), None, t);
        assert!(result.is_err());
        assert!(
            matches!(result.unwrap_err(), TallyError::Type { ref field, .. } if field == "amount")
        );
    }

    // ======================== LastOp Tests ========================

    #[test]
    fn test_last_stores_most_recent_value() {
        let mut op = LastOp::new("country", false);
        let t = ts(1000 * 60);
        op.push(&json!({"country": "US"}), None, t).unwrap();
        assert_eq!(op.read(t), FeatureValue::String("US".into()));
    }

    #[test]
    fn test_last_no_events_returns_missing() {
        let mut op = LastOp::new("country", false);
        let t = ts(1000 * 60);
        assert_eq!(op.read(t), FeatureValue::Missing);
    }

    #[test]
    fn test_last_stores_string_values() {
        let mut op = LastOp::new("status", false);
        let t = ts(1000 * 60);
        op.push(&json!({"status": "active"}), None, t).unwrap();
        assert_eq!(op.read(t), FeatureValue::String("active".into()));
    }

    #[test]
    fn test_last_updates_to_newer_value() {
        let mut op = LastOp::new("country", false);
        let t1 = ts(1000 * 60);
        let t2 = ts(1001 * 60);
        op.push(&json!({"country": "US"}), None, t1).unwrap();
        op.push(&json!({"country": "UK"}), None, t2).unwrap();
        assert_eq!(op.read(t2), FeatureValue::String("UK".into()));
    }

    #[test]
    fn test_last_stores_numeric_values() {
        let mut op = LastOp::new("amount", false);
        let t = ts(1000 * 60);
        op.push(&json!({"amount": 42.5}), None, t).unwrap();
        assert_eq!(op.read(t), FeatureValue::Float(42.5));
    }

    #[test]
    fn test_last_stores_int_values() {
        let mut op = LastOp::new("count", false);
        let t = ts(1000 * 60);
        op.push(&json!({"count": 7}), None, t).unwrap();
        assert_eq!(op.read(t), FeatureValue::Int(7));
    }

    #[test]
    fn test_last_optional_skips_missing_field() {
        let mut op = LastOp::new("country", true);
        let t = ts(1000 * 60);
        op.push(&json!({"country": "US"}), None, t).unwrap();
        assert!(op.push(&json!({}), None, t).is_ok());
        // Should still be US (missing field was skipped)
        assert_eq!(op.read(t), FeatureValue::String("US".into()));
    }

    #[test]
    fn test_last_non_optional_missing_field_errors() {
        let mut op = LastOp::new("country", false);
        let t = ts(1000 * 60);
        let result = op.push(&json!({}), None, t);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), TallyError::Type { ref got, .. } if got == "absent"));
    }

    // ======================== StddevOp Tests ========================

    #[test]
    fn test_stddev_basic_push_and_read() {
        let mut op = StddevOp::new(
            "amount",
            Duration::from_secs(3600),
            Duration::from_secs(60),
            false,
        );
        let t = ts(1000 * 60);
        op.push(&json!({"amount": 10.0}), None, t).unwrap();
        op.push(&json!({"amount": 20.0}), None, t).unwrap();
        op.push(&json!({"amount": 30.0}), None, t).unwrap();
        let val = op.read(t);
        // stddev of [10, 20, 30]: mean=20, variance=((100+400+900)/3 - 400) = 200/3, stddev=sqrt(200/3) ~= 8.165
        match val {
            FeatureValue::Float(f) => assert!((f - 8.16496580927726).abs() < 0.001, "got {}", f),
            other => panic!("expected Float, got {:?}", other),
        }
    }

    #[test]
    fn test_stddev_empty_returns_missing() {
        let mut op = StddevOp::new(
            "amount",
            Duration::from_secs(3600),
            Duration::from_secs(60),
            false,
        );
        let t = ts(1000 * 60);
        assert_eq!(op.read(t), FeatureValue::Missing);
    }

    #[test]
    fn test_stddev_single_value_returns_zero() {
        let mut op = StddevOp::new(
            "amount",
            Duration::from_secs(3600),
            Duration::from_secs(60),
            false,
        );
        let t = ts(1000 * 60);
        op.push(&json!({"amount": 42.0}), None, t).unwrap();
        assert_eq!(op.read(t), FeatureValue::Float(0.0));
    }

    #[test]
    fn test_stddev_all_same_value_returns_zero() {
        let mut op = StddevOp::new(
            "amount",
            Duration::from_secs(3600),
            Duration::from_secs(60),
            false,
        );
        let t = ts(1000 * 60);
        for _ in 0..10 {
            op.push(&json!({"amount": 5.0}), None, t).unwrap();
        }
        match op.read(t) {
            FeatureValue::Float(f) => assert!(f.abs() < 1e-10, "expected ~0, got {}", f),
            other => panic!("expected Float, got {:?}", other),
        }
    }

    #[test]
    fn test_stddev_window_expiry() {
        // 5-minute window, 1-minute buckets
        let mut op = StddevOp::new(
            "amount",
            Duration::from_secs(5 * 60),
            Duration::from_secs(60),
            false,
        );
        let t0 = ts(1000 * 60);
        op.push(&json!({"amount": 100.0}), None, t0).unwrap();
        op.push(&json!({"amount": 200.0}), None, t0).unwrap();

        // After full window expires, data should be gone
        let t1 = t0 + Duration::from_secs(10 * 60);
        assert_eq!(op.read(t1), FeatureValue::Missing);
    }

    #[test]
    fn test_stddev_multiple_buckets() {
        // 3-minute window, 1-minute buckets
        let mut op = StddevOp::new(
            "amount",
            Duration::from_secs(3 * 60),
            Duration::from_secs(60),
            false,
        );
        let t0 = ts(1000 * 60);
        op.push(&json!({"amount": 2.0}), None, t0).unwrap();
        let t1 = t0 + Duration::from_secs(60);
        op.push(&json!({"amount": 4.0}), None, t1).unwrap();
        let t2 = t0 + Duration::from_secs(120);
        op.push(&json!({"amount": 6.0}), None, t2).unwrap();
        // stddev of [2, 4, 6]: mean=4, variance=((4+16+36)/3 - 16) = 8/3, stddev=sqrt(8/3) ~= 1.633
        match op.read(t2) {
            FeatureValue::Float(f) => assert!((f - 1.632993161855452).abs() < 0.001, "got {}", f),
            other => panic!("expected Float, got {:?}", other),
        }
    }

    #[test]
    fn test_stddev_where_clause_filtering_non_optional_errors() {
        // Non-optional: missing field should error
        let mut op = StddevOp::new(
            "amount",
            Duration::from_secs(3600),
            Duration::from_secs(60),
            false,
        );
        let t = ts(1000 * 60);
        let result = op.push(&json!({}), None, t);
        assert!(result.is_err());
    }

    #[test]
    fn test_stddev_optional_missing_field_skips() {
        let mut op = StddevOp::new(
            "amount",
            Duration::from_secs(3600),
            Duration::from_secs(60),
            true,
        );
        let t = ts(1000 * 60);
        op.push(&json!({}), None, t).unwrap(); // should not error
        assert_eq!(op.read(t), FeatureValue::Missing); // no data pushed
    }

    // ======================== PercentileOp Tests ========================

    #[test]
    fn test_percentile_basic_push_and_read_p50() {
        let mut op = PercentileOp::new(
            "amount",
            0.5,
            Duration::from_secs(3600),
            Duration::from_secs(60),
            false,
        );
        let t = ts(1000 * 60);
        for i in 1..=100 {
            op.push(&json!({"amount": i as f64}), None, t).unwrap();
        }
        // p50 of [1..100] with linear interpolation: index = 0.5 * 99 = 49.5
        // = values[49] * 0.5 + values[50] * 0.5 = 50 * 0.5 + 51 * 0.5 = 50.5
        match op.read(t) {
            FeatureValue::Float(f) => assert!((f - 50.5).abs() < 0.01, "got {}", f),
            other => panic!("expected Float, got {:?}", other),
        }
    }

    #[test]
    fn test_percentile_p95() {
        let mut op = PercentileOp::new(
            "latency",
            0.95,
            Duration::from_secs(3600),
            Duration::from_secs(60),
            false,
        );
        let t = ts(1000 * 60);
        for i in 1..=100 {
            op.push(&json!({"latency": i as f64}), None, t).unwrap();
        }
        // p95 of [1..100]: index = 0.95 * 99 = 94.05
        // = values[94] * 0.95 + values[95] * 0.05 = 95 * 0.95 + 96 * 0.05 = 90.25 + 4.8 = 95.05
        match op.read(t) {
            FeatureValue::Float(f) => assert!((f - 95.05).abs() < 0.01, "got {}", f),
            other => panic!("expected Float, got {:?}", other),
        }
    }

    #[test]
    fn test_percentile_empty_returns_missing() {
        let mut op = PercentileOp::new(
            "amount",
            0.5,
            Duration::from_secs(3600),
            Duration::from_secs(60),
            false,
        );
        let t = ts(1000 * 60);
        assert_eq!(op.read(t), FeatureValue::Missing);
    }

    #[test]
    fn test_percentile_single_value() {
        let mut op = PercentileOp::new(
            "amount",
            0.99,
            Duration::from_secs(3600),
            Duration::from_secs(60),
            false,
        );
        let t = ts(1000 * 60);
        op.push(&json!({"amount": 42.0}), None, t).unwrap();
        assert_eq!(op.read(t), FeatureValue::Float(42.0));
    }

    #[test]
    fn test_percentile_all_same_value() {
        let mut op = PercentileOp::new(
            "amount",
            0.5,
            Duration::from_secs(3600),
            Duration::from_secs(60),
            false,
        );
        let t = ts(1000 * 60);
        for _ in 0..10 {
            op.push(&json!({"amount": 7.0}), None, t).unwrap();
        }
        assert_eq!(op.read(t), FeatureValue::Float(7.0));
    }

    #[test]
    fn test_percentile_window_expiry() {
        let mut op = PercentileOp::new(
            "amount",
            0.5,
            Duration::from_secs(5 * 60),
            Duration::from_secs(60),
            false,
        );
        let t0 = ts(1000 * 60);
        op.push(&json!({"amount": 100.0}), None, t0).unwrap();

        // After full window expires
        let t1 = t0 + Duration::from_secs(10 * 60);
        assert_eq!(op.read(t1), FeatureValue::Missing);
    }

    #[test]
    fn test_percentile_p0_and_p100() {
        let mut op_p0 = PercentileOp::new(
            "v",
            0.0,
            Duration::from_secs(3600),
            Duration::from_secs(60),
            false,
        );
        let mut op_p100 = PercentileOp::new(
            "v",
            1.0,
            Duration::from_secs(3600),
            Duration::from_secs(60),
            false,
        );
        let t = ts(1000 * 60);
        for i in &[5.0, 1.0, 9.0, 3.0, 7.0] {
            op_p0.push(&json!({"v": i}), None, t).unwrap();
            op_p100.push(&json!({"v": i}), None, t).unwrap();
        }
        assert_eq!(op_p0.read(t), FeatureValue::Float(1.0));
        assert_eq!(op_p100.read(t), FeatureValue::Float(9.0));
    }

    #[test]
    fn test_percentile_optional_missing_field_skips() {
        let mut op = PercentileOp::new(
            "amount",
            0.5,
            Duration::from_secs(3600),
            Duration::from_secs(60),
            true,
        );
        let t = ts(1000 * 60);
        op.push(&json!({}), None, t).unwrap(); // should not error
        assert_eq!(op.read(t), FeatureValue::Missing);
    }

    #[test]
    fn test_percentile_multiple_buckets() {
        // 3-minute window, 1-minute buckets
        let mut op = PercentileOp::new(
            "v",
            0.5,
            Duration::from_secs(3 * 60),
            Duration::from_secs(60),
            false,
        );
        let t0 = ts(1000 * 60);
        op.push(&json!({"v": 1.0}), None, t0).unwrap();
        let t1 = t0 + Duration::from_secs(60);
        op.push(&json!({"v": 2.0}), None, t1).unwrap();
        let t2 = t0 + Duration::from_secs(120);
        op.push(&json!({"v": 3.0}), None, t2).unwrap();
        // p50 of [1, 2, 3]: index = 0.5 * 2 = 1.0 -> values[1] = 2.0
        assert_eq!(op.read(t2), FeatureValue::Float(2.0));
    }
    // ======================== LagOp Tests ========================

    #[test]
    fn test_lag_returns_missing_until_n_events() {
        let mut op = LagOp::new("amount", 3, false);
        let t = ts(1000 * 60);
        op.push(&json!({"amount": 10.0}), None, t).unwrap();
        assert_eq!(op.read(t), FeatureValue::Missing); // only 1 event, need 3
        op.push(&json!({"amount": 20.0}), None, t).unwrap();
        assert_eq!(op.read(t), FeatureValue::Missing); // only 2 events
        op.push(&json!({"amount": 30.0}), None, t).unwrap();
        // Now buffer is full [10, 20, 30], lag(3) returns front = 10
        assert_eq!(op.read(t), FeatureValue::Float(10.0));
    }

    #[test]
    fn test_lag_returns_nth_oldest_value() {
        let mut op = LagOp::new("amount", 2, false);
        let t = ts(1000 * 60);
        op.push(&json!({"amount": 100.0}), None, t).unwrap();
        op.push(&json!({"amount": 200.0}), None, t).unwrap();
        // Buffer [100, 200], lag(2) = 100
        assert_eq!(op.read(t), FeatureValue::Float(100.0));
        op.push(&json!({"amount": 300.0}), None, t).unwrap();
        // Buffer [200, 300], lag(2) = 200
        assert_eq!(op.read(t), FeatureValue::Float(200.0));
    }

    #[test]
    fn test_lag_with_string_values() {
        let mut op = LagOp::new("country", 1, false);
        let t = ts(1000 * 60);
        op.push(&json!({"country": "US"}), None, t).unwrap();
        assert_eq!(op.read(t), FeatureValue::String("US".into()));
        op.push(&json!({"country": "UK"}), None, t).unwrap();
        assert_eq!(op.read(t), FeatureValue::String("UK".into()));
    }

    #[test]
    fn test_lag_non_optional_missing_field_errors() {
        let mut op = LagOp::new("amount", 1, false);
        let t = ts(1000 * 60);
        let result = op.push(&json!({}), None, t);
        assert!(result.is_err());
    }

    #[test]
    fn test_lag_optional_skips_missing_field() {
        let mut op = LagOp::new("amount", 1, true);
        let t = ts(1000 * 60);
        assert!(op.push(&json!({}), None, t).is_ok());
        assert_eq!(op.read(t), FeatureValue::Missing); // nothing pushed
    }

    // ======================== EmaOp Tests ========================

    #[test]
    fn test_ema_first_value_is_exact() {
        let mut op = EmaOp::new("amount", 60.0, false); // 60s half-life
        let t = ts(1000 * 60);
        op.push(&json!({"amount": 100.0}), None, t).unwrap();
        assert_eq!(op.read(t), FeatureValue::Float(100.0));
    }

    #[test]
    fn test_ema_decays_over_time() {
        let mut op = EmaOp::new("amount", 60.0, false); // 60s half-life
        let t0 = ts(1000 * 60);
        op.push(&json!({"amount": 100.0}), None, t0).unwrap();
        // After one half-life, push 0 -- EMA should be ~50
        let t1 = t0 + Duration::from_secs(60);
        op.push(&json!({"amount": 0.0}), None, t1).unwrap();
        if let FeatureValue::Float(v) = op.read(t1) {
            assert!((v - 50.0).abs() < 1.0, "expected ~50, got {}", v);
        } else {
            panic!("expected Float");
        }
    }

    #[test]
    fn test_ema_same_timestamp_no_decay() {
        let mut op = EmaOp::new("amount", 60.0, false);
        let t = ts(1000 * 60);
        op.push(&json!({"amount": 100.0}), None, t).unwrap();
        op.push(&json!({"amount": 0.0}), None, t).unwrap();
        // alpha = exp(0) = 1, so: 1*100 + 0*0 = 100... wait no.
        // elapsed=0, alpha=exp(0)=1, current = 1*100 + 0*0 = 100
        assert_eq!(op.read(t), FeatureValue::Float(100.0));
    }

    #[test]
    fn test_ema_returns_missing_before_any_push() {
        let op = EmaOp::new("amount", 60.0, false);
        let t = ts(1000 * 60);
        let mut op = op;
        assert_eq!(op.read(t), FeatureValue::Missing);
    }

    #[test]
    fn test_ema_non_numeric_field_errors() {
        let mut op = EmaOp::new("amount", 60.0, false);
        let t = ts(1000 * 60);
        let result = op.push(&json!({"amount": "not_a_number"}), None, t);
        assert!(result.is_err());
    }

    // ======================== LastNOp Tests ========================

    #[test]
    fn test_last_n_returns_missing_when_empty() {
        let mut op = LastNOp::new("merchant", 3, false);
        let t = ts(1000 * 60);
        assert_eq!(op.read(t), FeatureValue::Missing);
    }

    #[test]
    fn test_last_n_returns_partial_list() {
        let mut op = LastNOp::new("merchant", 3, false);
        let t = ts(1000 * 60);
        op.push(&json!({"merchant": "m1"}), None, t).unwrap();
        op.push(&json!({"merchant": "m2"}), None, t).unwrap();
        if let FeatureValue::String(s) = op.read(t) {
            let arr: Vec<String> = serde_json::from_str(&s).unwrap();
            assert_eq!(arr, vec!["m1", "m2"]);
        } else {
            panic!("expected String (JSON array)");
        }
    }

    #[test]
    fn test_last_n_evicts_oldest_when_full() {
        let mut op = LastNOp::new("merchant", 2, false);
        let t = ts(1000 * 60);
        op.push(&json!({"merchant": "m1"}), None, t).unwrap();
        op.push(&json!({"merchant": "m2"}), None, t).unwrap();
        op.push(&json!({"merchant": "m3"}), None, t).unwrap();
        if let FeatureValue::String(s) = op.read(t) {
            let arr: Vec<String> = serde_json::from_str(&s).unwrap();
            assert_eq!(arr, vec!["m2", "m3"]);
        } else {
            panic!("expected String (JSON array)");
        }
    }

    #[test]
    fn test_last_n_with_numeric_values() {
        let mut op = LastNOp::new("amount", 3, false);
        let t = ts(1000 * 60);
        op.push(&json!({"amount": 10}), None, t).unwrap();
        op.push(&json!({"amount": 20}), None, t).unwrap();
        op.push(&json!({"amount": 30}), None, t).unwrap();
        if let FeatureValue::String(s) = op.read(t) {
            let arr: Vec<i64> = serde_json::from_str(&s).unwrap();
            assert_eq!(arr, vec![10, 20, 30]);
        } else {
            panic!("expected String (JSON array)");
        }
    }

    // ======================== FirstOp Tests ========================

    #[test]
    fn test_first_stores_first_value() {
        let mut op = FirstOp::new("country", false);
        let t = ts(1000 * 60);
        op.push(&json!({"country": "US"}), None, t).unwrap();
        assert_eq!(op.read(t), FeatureValue::String("US".into()));
    }

    #[test]
    fn test_first_never_overwrites() {
        let mut op = FirstOp::new("country", false);
        let t = ts(1000 * 60);
        op.push(&json!({"country": "US"}), None, t).unwrap();
        op.push(&json!({"country": "UK"}), None, t + Duration::from_secs(60))
            .unwrap();
        op.push(
            &json!({"country": "DE"}),
            None,
            t + Duration::from_secs(120),
        )
        .unwrap();
        assert_eq!(op.read(t), FeatureValue::String("US".into()));
    }

    #[test]
    fn test_first_returns_missing_before_any_push() {
        let mut op = FirstOp::new("country", false);
        let t = ts(1000 * 60);
        assert_eq!(op.read(t), FeatureValue::Missing);
    }

    #[test]
    fn test_first_non_optional_missing_field_errors() {
        let mut op = FirstOp::new("country", false);
        let t = ts(1000 * 60);
        let result = op.push(&json!({}), None, t);
        assert!(result.is_err());
    }

    #[test]
    fn test_first_optional_skips_missing_field_waits_for_real_value() {
        let mut op = FirstOp::new("country", true);
        let t = ts(1000 * 60);
        assert!(op.push(&json!({}), None, t).is_ok()); // skip
        assert_eq!(op.read(t), FeatureValue::Missing); // still no value
        op.push(&json!({"country": "US"}), None, t).unwrap();
        assert_eq!(op.read(t), FeatureValue::String("US".into()));
        // Subsequent events do not overwrite
        op.push(&json!({"country": "UK"}), None, t).unwrap();
        assert_eq!(op.read(t), FeatureValue::String("US".into()));
    }

    // ======================== ExactMinOp Tests ========================

    #[test]
    fn test_exact_min_basic() {
        let mut op = ExactMinOp::new(
            "amount",
            Duration::from_secs(5 * 60),
            Duration::from_secs(60),
            false,
        );
        let t = ts(1000 * 60);
        op.push(&json!({"amount": 30.0}), None, t).unwrap();
        op.push(&json!({"amount": 10.0}), None, t).unwrap();
        op.push(&json!({"amount": 20.0}), None, t).unwrap();
        assert_eq!(op.read(t), FeatureValue::Float(10.0));
    }

    #[test]
    fn test_exact_min_retracts_expired_values() {
        let mut op = ExactMinOp::new(
            "amount",
            Duration::from_secs(3 * 60),
            Duration::from_secs(60),
            false,
        );
        let t0 = ts(1000 * 60);
        op.push(&json!({"amount": 5.0}), None, t0).unwrap();
        op.push(&json!({"amount": 20.0}), None, t0 + Duration::from_secs(60))
            .unwrap();
        op.push(
            &json!({"amount": 15.0}),
            None,
            t0 + Duration::from_secs(120),
        )
        .unwrap();
        assert_eq!(
            op.read(t0 + Duration::from_secs(120)),
            FeatureValue::Float(5.0)
        );
        // Advance past the window so the 5.0 bucket expires
        let t_future = t0 + Duration::from_secs(4 * 60);
        op.push(&json!({"amount": 25.0}), None, t_future).unwrap();
        // 5.0 should be expired; min should now be 15.0 or 25.0
        let val = op.read(t_future);
        if let FeatureValue::Float(v) = val {
            assert!(
                v >= 15.0,
                "expected min >= 15.0 after retraction, got {}",
                v
            );
        } else {
            panic!("expected Float");
        }
    }

    #[test]
    fn test_exact_min_returns_missing_when_empty() {
        let mut op = ExactMinOp::new(
            "amount",
            Duration::from_secs(5 * 60),
            Duration::from_secs(60),
            false,
        );
        let t = ts(1000 * 60);
        assert_eq!(op.read(t), FeatureValue::Missing);
    }

    #[test]
    fn test_exact_min_returns_missing_after_all_expire() {
        let mut op = ExactMinOp::new(
            "amount",
            Duration::from_secs(3 * 60),
            Duration::from_secs(60),
            false,
        );
        let t0 = ts(1000 * 60);
        op.push(&json!({"amount": 10.0}), None, t0).unwrap();
        assert_eq!(op.read(t0), FeatureValue::Float(10.0));
        // Advance well past window
        let t_future = t0 + Duration::from_secs(10 * 60);
        assert_eq!(op.read(t_future), FeatureValue::Missing);
    }

    // ======================== ExactMaxOp Tests ========================

    #[test]
    fn test_exact_max_basic() {
        let mut op = ExactMaxOp::new(
            "amount",
            Duration::from_secs(5 * 60),
            Duration::from_secs(60),
            false,
        );
        let t = ts(1000 * 60);
        op.push(&json!({"amount": 10.0}), None, t).unwrap();
        op.push(&json!({"amount": 30.0}), None, t).unwrap();
        op.push(&json!({"amount": 20.0}), None, t).unwrap();
        assert_eq!(op.read(t), FeatureValue::Float(30.0));
    }

    #[test]
    fn test_exact_max_retracts_expired_values() {
        let mut op = ExactMaxOp::new(
            "amount",
            Duration::from_secs(3 * 60),
            Duration::from_secs(60),
            false,
        );
        let t0 = ts(1000 * 60);
        op.push(&json!({"amount": 100.0}), None, t0).unwrap();
        op.push(&json!({"amount": 20.0}), None, t0 + Duration::from_secs(60))
            .unwrap();
        op.push(
            &json!({"amount": 30.0}),
            None,
            t0 + Duration::from_secs(120),
        )
        .unwrap();
        assert_eq!(
            op.read(t0 + Duration::from_secs(120)),
            FeatureValue::Float(100.0)
        );
        // Advance past the window so the 100.0 bucket expires
        let t_future = t0 + Duration::from_secs(4 * 60);
        op.push(&json!({"amount": 25.0}), None, t_future).unwrap();
        let val = op.read(t_future);
        if let FeatureValue::Float(v) = val {
            assert!(
                v <= 30.0,
                "expected max <= 30.0 after retraction, got {}",
                v
            );
        } else {
            panic!("expected Float");
        }
    }

    #[test]
    fn test_exact_max_returns_missing_when_empty() {
        let mut op = ExactMaxOp::new(
            "amount",
            Duration::from_secs(5 * 60),
            Duration::from_secs(60),
            false,
        );
        let t = ts(1000 * 60);
        assert_eq!(op.read(t), FeatureValue::Missing);
    }

    #[test]
    fn test_exact_max_duplicate_values() {
        let mut op = ExactMaxOp::new(
            "amount",
            Duration::from_secs(5 * 60),
            Duration::from_secs(60),
            false,
        );
        let t = ts(1000 * 60);
        op.push(&json!({"amount": 50.0}), None, t).unwrap();
        op.push(&json!({"amount": 50.0}), None, t).unwrap();
        op.push(&json!({"amount": 50.0}), None, t).unwrap();
        assert_eq!(op.read(t), FeatureValue::Float(50.0));
    }
}
