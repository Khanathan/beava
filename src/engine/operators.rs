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
pub trait Operator: std::fmt::Debug {
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
            FeatureValue::Int(total as i64)
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
        // Advance time to expire stale buckets before reading.
        self.buffer.advance_to(now);
        if self.buffer.count_nonzero() == 0 {
            FeatureValue::Missing // Zero events -> Missing
        } else {
            FeatureValue::Float(self.buffer.sum_all())
        }
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
}
