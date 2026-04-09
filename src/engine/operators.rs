//! Streaming operators for windowed aggregation.
//!
//! Each operator wraps one or more `RingBuffer`s and implements the `Operator`
//! trait: `push()` to ingest an event, `read()` to get the current aggregate.

use std::time::SystemTime;
use serde::{Serialize, Deserialize};
use crate::types::FeatureValue;
use crate::error::TallyError;
use super::window::RingBuffer;

/// Trait implemented by all streaming operators.
/// - `push` processes an incoming event. Called once per event per operator.
/// - `read` returns the current aggregate value. Called to collect features.
///
/// `read` takes `&mut self` so implementations can call `advance_to(now)` to
/// expire stale buckets before aggregating. This is safe in Tally's
/// single-threaded Redis-like design (no concurrent reads).
pub trait Operator: std::fmt::Debug + Send {
    fn push(&mut self, event: &serde_json::Value, now: SystemTime) -> Result<(), TallyError>;
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
    fn push(&mut self, _event: &serde_json::Value, now: SystemTime) -> Result<(), TallyError> {
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
    fn push(&mut self, event: &serde_json::Value, now: SystemTime) -> Result<(), TallyError> {
        match event.get(&self.field) {
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
    fn default() -> Self { MinBucket(f64::INFINITY) }
}

/// Bucket wrapper for MaxOp. Default is -INFINITY so any real value replaces it.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
pub struct MaxBucket(pub f64);
impl Default for MaxBucket {
    fn default() -> Self { MaxBucket(f64::NEG_INFINITY) }
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
    fn push(&mut self, event: &serde_json::Value, now: SystemTime) -> Result<(), TallyError> {
        match event.get(&self.field) {
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
                    self.buffer.update_current(|bucket| {
                        if f < bucket.0 {
                            bucket.0 = f;
                        }
                    }, now);
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
        let min_val = self.buffer.buckets_iter()
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
    fn push(&mut self, event: &serde_json::Value, now: SystemTime) -> Result<(), TallyError> {
        match event.get(&self.field) {
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
                    self.buffer.update_current(|bucket| {
                        if f > bucket.0 {
                            bucket.0 = f;
                        }
                    }, now);
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
        let max_val = self.buffer.buckets_iter()
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
    fn push(&mut self, event: &serde_json::Value, now: SystemTime) -> Result<(), TallyError> {
        match event.get(&self.field) {
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
    fn push(&mut self, event: &serde_json::Value, now: SystemTime) -> Result<(), TallyError> {
        match event.get(&self.field) {
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};
    use serde_json::json;

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
        op.push(&json!({}), t).unwrap();
        assert_eq!(op.read(t), FeatureValue::Int(1));
    }

    #[test]
    fn test_count_push_5_events_same_timestamp() {
        let mut op = CountOp::new(Duration::from_secs(30 * 60), Duration::from_secs(60));
        let t = ts(1000 * 60);
        for _ in 0..5 {
            op.push(&json!({}), t).unwrap();
        }
        assert_eq!(op.read(t), FeatureValue::Int(5));
    }

    #[test]
    fn test_count_push_events_across_multiple_buckets() {
        let mut op = CountOp::new(Duration::from_secs(30 * 60), Duration::from_secs(60));
        let t0 = ts(1000 * 60);
        op.push(&json!({}), t0).unwrap();
        op.push(&json!({}), t0 + Duration::from_secs(60)).unwrap();
        op.push(&json!({}), t0 + Duration::from_secs(120)).unwrap();
        // All 3 events within window, should sum to 3
        assert_eq!(op.read(t0 + Duration::from_secs(120)), FeatureValue::Int(3));
    }

    #[test]
    fn test_count_read_returns_missing_after_window_expires() {
        let mut op = CountOp::new(Duration::from_secs(5 * 60), Duration::from_secs(60));
        let t0 = ts(1000 * 60);
        op.push(&json!({}), t0).unwrap();
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
        op.push(&json!({"amount": 50.0, "status": "ok"}), t).unwrap();
        assert_eq!(op.read(t), FeatureValue::Int(1));
    }

    #[test]
    fn test_count_push_with_various_json_shapes_succeeds() {
        let mut op = CountOp::new(Duration::from_secs(30 * 60), Duration::from_secs(60));
        let t = ts(1000 * 60);
        // Empty object
        assert!(op.push(&json!({}), t).is_ok());
        // Nested object
        assert!(op.push(&json!({"nested": {"deep": true}}), t).is_ok());
        // Array value
        assert!(op.push(&json!({"list": [1, 2, 3]}), t).is_ok());
        // String value
        assert!(op.push(&json!({"name": "test"}), t).is_ok());
        // Null value
        assert!(op.push(&json!(null), t).is_ok());
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
        let mut op = SumOp::new("amount", Duration::from_secs(60 * 60), Duration::from_secs(60), false);
        let t = ts(1000 * 60);
        op.push(&json!({"amount": 50.0}), t).unwrap();
        op.push(&json!({"amount": 30.0}), t).unwrap();
        assert_eq!(op.read(t), FeatureValue::Float(80.0));
    }

    #[test]
    fn test_sum_type_error_on_string_field() {
        let mut op = SumOp::new("amount", Duration::from_secs(60 * 60), Duration::from_secs(60), false);
        let t = ts(1000 * 60);
        let result = op.push(&json!({"amount": "not_a_number"}), t);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, TallyError::Type { ref field, .. } if field == "amount"));
    }

    #[test]
    fn test_sum_non_optional_missing_field_returns_type_error() {
        let mut op = SumOp::new("amount", Duration::from_secs(60 * 60), Duration::from_secs(60), false);
        let t = ts(1000 * 60);
        let result = op.push(&json!({}), t);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, TallyError::Type { ref got, .. } if got == "absent"));
    }

    #[test]
    fn test_sum_optional_missing_field_returns_ok() {
        let mut op = SumOp::new("amount", Duration::from_secs(60 * 60), Duration::from_secs(60), true);
        let t = ts(1000 * 60);
        // Push event without the field -- should succeed silently
        assert!(op.push(&json!({}), t).is_ok());
        // No numeric data was added, so read returns Missing
        assert_eq!(op.read(t), FeatureValue::Missing);
    }

    #[test]
    fn test_sum_optional_missing_field_does_not_affect_existing_sum() {
        let mut op = SumOp::new("amount", Duration::from_secs(60 * 60), Duration::from_secs(60), true);
        let t = ts(1000 * 60);
        op.push(&json!({"amount": 50.0}), t).unwrap();
        // Push event without field -- should not affect the sum
        op.push(&json!({}), t).unwrap();
        assert_eq!(op.read(t), FeatureValue::Float(50.0));
    }

    #[test]
    fn test_sum_with_int_values_coerces_to_f64() {
        let mut op = SumOp::new("amount", Duration::from_secs(60 * 60), Duration::from_secs(60), false);
        let t = ts(1000 * 60);
        op.push(&json!({"amount": 50}), t).unwrap(); // Int, not Float
        assert_eq!(op.read(t), FeatureValue::Float(50.0));
    }

    #[test]
    fn test_sum_read_with_zero_events_returns_missing() {
        let mut op = SumOp::new("amount", Duration::from_secs(60 * 60), Duration::from_secs(60), false);
        let t = ts(1000 * 60);
        assert_eq!(op.read(t), FeatureValue::Missing);
    }

    #[test]
    fn test_sum_expires_correctly_when_time_advances_past_window() {
        let mut op = SumOp::new("amount", Duration::from_secs(5 * 60), Duration::from_secs(60), false);
        let t0 = ts(1000 * 60);
        op.push(&json!({"amount": 100.0}), t0).unwrap();
        assert_eq!(op.read(t0), FeatureValue::Float(100.0));

        // Advance past the full window
        let t_future = t0 + Duration::from_secs(10 * 60);
        assert_eq!(op.read(t_future), FeatureValue::Missing);
    }

    // ======================== AvgOp Tests ========================

    #[test]
    fn test_avg_push_three_events_returns_average() {
        let mut op = AvgOp::new("amount", Duration::from_secs(60 * 60), Duration::from_secs(60), false);
        let t = ts(1000 * 60);
        op.push(&json!({"amount": 10.0}), t).unwrap();
        op.push(&json!({"amount": 20.0}), t).unwrap();
        op.push(&json!({"amount": 30.0}), t).unwrap();
        assert_eq!(op.read(t), FeatureValue::Float(20.0));
    }

    #[test]
    fn test_avg_read_with_zero_events_returns_missing() {
        let mut op = AvgOp::new("amount", Duration::from_secs(60 * 60), Duration::from_secs(60), false);
        let t = ts(1000 * 60);
        // No push -- should return Missing, not NaN or 0
        assert_eq!(op.read(t), FeatureValue::Missing);
    }

    #[test]
    fn test_avg_type_error_on_non_numeric_field() {
        let mut op = AvgOp::new("amount", Duration::from_secs(60 * 60), Duration::from_secs(60), false);
        let t = ts(1000 * 60);
        let result = op.push(&json!({"amount": "hello"}), t);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), TallyError::Type { ref field, .. } if field == "amount"));
    }

    #[test]
    fn test_avg_non_optional_missing_field_returns_type_error() {
        let mut op = AvgOp::new("amount", Duration::from_secs(60 * 60), Duration::from_secs(60), false);
        let t = ts(1000 * 60);
        let result = op.push(&json!({}), t);
        assert!(result.is_err());
    }

    #[test]
    fn test_avg_optional_missing_field_does_not_affect_average() {
        let mut op = AvgOp::new("amount", Duration::from_secs(60 * 60), Duration::from_secs(60), true);
        let t = ts(1000 * 60);
        op.push(&json!({"amount": 10.0}), t).unwrap();
        op.push(&json!({"amount": 20.0}), t).unwrap();
        // Push event without field -- should not affect average
        op.push(&json!({}), t).unwrap();
        // Average should be (10+20)/2 = 15.0, not (10+20)/3
        assert_eq!(op.read(t), FeatureValue::Float(15.0));
    }

    #[test]
    fn test_avg_optional_only_missing_fields_returns_missing() {
        let mut op = AvgOp::new("amount", Duration::from_secs(60 * 60), Duration::from_secs(60), true);
        let t = ts(1000 * 60);
        op.push(&json!({}), t).unwrap();
        op.push(&json!({}), t).unwrap();
        // All events had missing field, count is 0 -> Missing
        assert_eq!(op.read(t), FeatureValue::Missing);
    }

    #[test]
    fn test_avg_expires_correctly_when_time_advances_past_window() {
        let mut op = AvgOp::new("amount", Duration::from_secs(5 * 60), Duration::from_secs(60), false);
        let t0 = ts(1000 * 60);
        op.push(&json!({"amount": 100.0}), t0).unwrap();
        assert_eq!(op.read(t0), FeatureValue::Float(100.0));

        // Advance past the full window
        let t_future = t0 + Duration::from_secs(10 * 60);
        assert_eq!(op.read(t_future), FeatureValue::Missing);
    }

    #[test]
    fn test_avg_with_int_values() {
        let mut op = AvgOp::new("amount", Duration::from_secs(60 * 60), Duration::from_secs(60), false);
        let t = ts(1000 * 60);
        op.push(&json!({"amount": 10}), t).unwrap();
        op.push(&json!({"amount": 20}), t).unwrap();
        assert_eq!(op.read(t), FeatureValue::Float(15.0));
    }

    // ======================== Negative Number Tests ========================

    #[test]
    fn test_sum_with_negative_values() {
        let mut op = SumOp::new("amount", Duration::from_secs(3600), Duration::from_secs(60), false);
        let t = ts(1000 * 60);
        op.push(&json!({"amount": -10.0}), t).unwrap();
        op.push(&json!({"amount": -20.0}), t).unwrap();
        assert_eq!(op.read(t), FeatureValue::Float(-30.0));
    }

    #[test]
    fn test_sum_with_mixed_positive_and_negative() {
        let mut op = SumOp::new("amount", Duration::from_secs(3600), Duration::from_secs(60), false);
        let t = ts(1000 * 60);
        op.push(&json!({"amount": 100.0}), t).unwrap();
        op.push(&json!({"amount": -30.0}), t).unwrap();
        op.push(&json!({"amount": -20.0}), t).unwrap();
        assert_eq!(op.read(t), FeatureValue::Float(50.0));
    }

    #[test]
    fn test_avg_with_negative_values() {
        let mut op = AvgOp::new("amount", Duration::from_secs(3600), Duration::from_secs(60), false);
        let t = ts(1000 * 60);
        op.push(&json!({"amount": -10.0}), t).unwrap();
        op.push(&json!({"amount": -30.0}), t).unwrap();
        assert_eq!(op.read(t), FeatureValue::Float(-20.0));
    }

    #[test]
    fn test_avg_with_mixed_positive_and_negative() {
        let mut op = AvgOp::new("amount", Duration::from_secs(3600), Duration::from_secs(60), false);
        let t = ts(1000 * 60);
        op.push(&json!({"amount": 10.0}), t).unwrap();
        op.push(&json!({"amount": -30.0}), t).unwrap();
        assert_eq!(op.read(t), FeatureValue::Float(-10.0));
    }

    // ======================== MinOp Tests ========================

    #[test]
    fn test_min_three_events_returns_minimum() {
        let mut op = MinOp::new("amount", Duration::from_secs(3600), Duration::from_secs(60), false);
        let t = ts(1000 * 60);
        op.push(&json!({"amount": 10.0}), t).unwrap();
        op.push(&json!({"amount": 5.0}), t).unwrap();
        op.push(&json!({"amount": 20.0}), t).unwrap();
        assert_eq!(op.read(t), FeatureValue::Float(5.0));
    }

    #[test]
    fn test_min_zero_events_returns_missing() {
        let mut op = MinOp::new("amount", Duration::from_secs(3600), Duration::from_secs(60), false);
        let t = ts(1000 * 60);
        assert_eq!(op.read(t), FeatureValue::Missing);
    }

    #[test]
    fn test_min_expires_old_buckets() {
        let mut op = MinOp::new("amount", Duration::from_secs(5 * 60), Duration::from_secs(60), false);
        let t0 = ts(1000 * 60);
        op.push(&json!({"amount": 42.0}), t0).unwrap();
        assert_eq!(op.read(t0), FeatureValue::Float(42.0));
        // Advance past the full window (2x window)
        let t_future = t0 + Duration::from_secs(10 * 60);
        assert_eq!(op.read(t_future), FeatureValue::Missing);
    }

    #[test]
    fn test_min_optional_skips_missing_field() {
        let mut op = MinOp::new("amount", Duration::from_secs(3600), Duration::from_secs(60), true);
        let t = ts(1000 * 60);
        op.push(&json!({"amount": 10.0}), t).unwrap();
        // Push event without field -- should succeed silently
        assert!(op.push(&json!({}), t).is_ok());
        assert_eq!(op.read(t), FeatureValue::Float(10.0));
    }

    #[test]
    fn test_min_non_optional_missing_field_errors() {
        let mut op = MinOp::new("amount", Duration::from_secs(3600), Duration::from_secs(60), false);
        let t = ts(1000 * 60);
        let result = op.push(&json!({}), t);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), TallyError::Type { ref got, .. } if got == "absent"));
    }

    #[test]
    fn test_min_type_error_on_string_field() {
        let mut op = MinOp::new("amount", Duration::from_secs(3600), Duration::from_secs(60), false);
        let t = ts(1000 * 60);
        let result = op.push(&json!({"amount": "not_a_number"}), t);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), TallyError::Type { ref field, .. } if field == "amount"));
    }

    // ======================== MaxOp Tests ========================

    #[test]
    fn test_max_three_events_returns_maximum() {
        let mut op = MaxOp::new("amount", Duration::from_secs(3600), Duration::from_secs(60), false);
        let t = ts(1000 * 60);
        op.push(&json!({"amount": 10.0}), t).unwrap();
        op.push(&json!({"amount": 5.0}), t).unwrap();
        op.push(&json!({"amount": 20.0}), t).unwrap();
        assert_eq!(op.read(t), FeatureValue::Float(20.0));
    }

    #[test]
    fn test_max_zero_events_returns_missing() {
        let mut op = MaxOp::new("amount", Duration::from_secs(3600), Duration::from_secs(60), false);
        let t = ts(1000 * 60);
        assert_eq!(op.read(t), FeatureValue::Missing);
    }

    #[test]
    fn test_max_expires_old_buckets() {
        let mut op = MaxOp::new("amount", Duration::from_secs(5 * 60), Duration::from_secs(60), false);
        let t0 = ts(1000 * 60);
        op.push(&json!({"amount": 42.0}), t0).unwrap();
        assert_eq!(op.read(t0), FeatureValue::Float(42.0));
        let t_future = t0 + Duration::from_secs(10 * 60);
        assert_eq!(op.read(t_future), FeatureValue::Missing);
    }

    #[test]
    fn test_max_optional_skips_missing_field() {
        let mut op = MaxOp::new("amount", Duration::from_secs(3600), Duration::from_secs(60), true);
        let t = ts(1000 * 60);
        op.push(&json!({"amount": 10.0}), t).unwrap();
        assert!(op.push(&json!({}), t).is_ok());
        assert_eq!(op.read(t), FeatureValue::Float(10.0));
    }

    #[test]
    fn test_max_non_optional_missing_field_errors() {
        let mut op = MaxOp::new("amount", Duration::from_secs(3600), Duration::from_secs(60), false);
        let t = ts(1000 * 60);
        let result = op.push(&json!({}), t);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), TallyError::Type { ref got, .. } if got == "absent"));
    }

    #[test]
    fn test_max_type_error_on_string_field() {
        let mut op = MaxOp::new("amount", Duration::from_secs(3600), Duration::from_secs(60), false);
        let t = ts(1000 * 60);
        let result = op.push(&json!({"amount": "not_a_number"}), t);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), TallyError::Type { ref field, .. } if field == "amount"));
    }

    // ======================== LastOp Tests ========================

    #[test]
    fn test_last_stores_most_recent_value() {
        let mut op = LastOp::new("country", false);
        let t = ts(1000 * 60);
        op.push(&json!({"country": "US"}), t).unwrap();
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
        op.push(&json!({"status": "active"}), t).unwrap();
        assert_eq!(op.read(t), FeatureValue::String("active".into()));
    }

    #[test]
    fn test_last_updates_to_newer_value() {
        let mut op = LastOp::new("country", false);
        let t1 = ts(1000 * 60);
        let t2 = ts(1001 * 60);
        op.push(&json!({"country": "US"}), t1).unwrap();
        op.push(&json!({"country": "UK"}), t2).unwrap();
        assert_eq!(op.read(t2), FeatureValue::String("UK".into()));
    }

    #[test]
    fn test_last_stores_numeric_values() {
        let mut op = LastOp::new("amount", false);
        let t = ts(1000 * 60);
        op.push(&json!({"amount": 42.5}), t).unwrap();
        assert_eq!(op.read(t), FeatureValue::Float(42.5));
    }

    #[test]
    fn test_last_stores_int_values() {
        let mut op = LastOp::new("count", false);
        let t = ts(1000 * 60);
        op.push(&json!({"count": 7}), t).unwrap();
        assert_eq!(op.read(t), FeatureValue::Int(7));
    }

    #[test]
    fn test_last_optional_skips_missing_field() {
        let mut op = LastOp::new("country", true);
        let t = ts(1000 * 60);
        op.push(&json!({"country": "US"}), t).unwrap();
        assert!(op.push(&json!({}), t).is_ok());
        // Should still be US (missing field was skipped)
        assert_eq!(op.read(t), FeatureValue::String("US".into()));
    }

    #[test]
    fn test_last_non_optional_missing_field_errors() {
        let mut op = LastOp::new("country", false);
        let t = ts(1000 * 60);
        let result = op.push(&json!({}), t);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), TallyError::Type { ref got, .. } if got == "absent"));
    }
}
