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
}
