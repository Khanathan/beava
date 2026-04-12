//! HyperLogLog implementation for approximate distinct counting.
//!
//! Implements the HLL algorithm with 14-bit precision (16384 registers)
//! yielding ~1.6% standard error. Built from scratch per locked decision
//! (no external crates). Uses ahash for hash function.
//!
//! Also contains `DistinctCountOp` which wraps `RingBuffer<Hll>` for
//! windowed approximate distinct counting.

use std::time::{Duration, SystemTime};
use serde::{Serialize, Deserialize};
use crate::engine::window::RingBuffer;
use crate::engine::operators::Operator;
use crate::types::FeatureValue;
use crate::error::TallyError;

/// Precision: 14 bits (locked decision from CONTEXT.md)
const HLL_P: usize = 14;
/// Number of registers: 2^14 = 16384
const HLL_M: usize = 1 << HLL_P;
/// Alpha correction constant for m=16384
const HLL_ALPHA: f64 = 0.7213 / (1.0 + 1.079 / HLL_M as f64);

/// Hash a string value using ahash (already a project dependency).
/// Returns a 64-bit hash for HLL register selection and rank computation.
fn hash_value(value: &str) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut hasher = ahash::AHasher::default();
    value.hash(&mut hasher);
    hasher.finish()
}

/// HyperLogLog sketch for approximate cardinality estimation.
///
/// Uses 14-bit precision (16384 registers of 1 byte each = 16KB).
/// Standard error ~1.6% for cardinalities above ~1000.
/// Implements Clone + Default for RingBuffer<Hll> compatibility.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Hll {
    registers: Vec<u8>,
}

impl Default for Hll {
    fn default() -> Self {
        Self {
            registers: vec![0u8; HLL_M],
        }
    }
}

impl Hll {
    /// Create a new empty HLL sketch.
    pub fn new() -> Self {
        Self::default()
    }

    /// Insert a string value into the sketch.
    /// Uses the top 14 bits of the hash for register index,
    /// and counts leading zeros of the remaining bits for the rank.
    pub fn insert(&mut self, value: &str) {
        let hash = hash_value(value);
        let index = (hash >> (64 - HLL_P)) as usize;
        // Use remaining bits (lower 50 bits). Set a guard bit so
        // leading_zeros is bounded even if remaining bits are all zero.
        let remaining = (hash << HLL_P) | (1 << (HLL_P - 1));
        let leading_zeros = remaining.leading_zeros() as u8 + 1;
        self.registers[index] = self.registers[index].max(leading_zeros);
    }

    /// Estimate the cardinality (number of distinct items inserted).
    /// Applies linear counting correction for small cardinalities.
    pub fn count(&self) -> f64 {
        let sum: f64 = self.registers.iter()
            .map(|&r| 2.0_f64.powi(-(r as i32)))
            .sum();
        let raw = HLL_ALPHA * (HLL_M as f64) * (HLL_M as f64) / sum;

        // Small range correction (linear counting)
        if raw <= 2.5 * HLL_M as f64 {
            let zeros = self.registers.iter().filter(|&&r| r == 0).count();
            if zeros > 0 {
                return (HLL_M as f64) * (HLL_M as f64 / zeros as f64).ln();
            }
        }
        raw
    }

    /// Merge another HLL sketch into this one (union semantics).
    /// Takes element-wise maximum of registers.
    pub fn merge(&mut self, other: &Hll) {
        for (a, &b) in self.registers.iter_mut().zip(other.registers.iter()) {
            *a = (*a).max(b);
        }
    }

    /// Check if the sketch has had no insertions.
    pub fn is_empty(&self) -> bool {
        self.registers.iter().all(|&r| r == 0)
    }
}

// ======================== DistinctCountOp ========================

/// Windowed approximate distinct count operator using RingBuffer<Hll>.
///
/// Each bucket holds an independent HLL sketch. On push, the value is
/// inserted into the current bucket's sketch. On read, all non-empty
/// bucket sketches are merged and the combined cardinality is returned.
///
/// Per locked CONTEXT.md decision: "HLL uses RingBuffer<Hll> pattern".
/// Hll implements Default (empty sketch) so advance_to clearing works
/// via T::default().
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DistinctCountOp {
    field: String,
    buffer: RingBuffer<Hll>,
    /// Parallel count buffer to track whether any events were pushed.
    /// Needed because an empty HLL still has count() > 0 issues at edge.
    event_count: RingBuffer<u64>,
    optional: bool,
}

impl DistinctCountOp {
    pub fn new(
        field: impl Into<String>,
        window_duration: Duration,
        bucket_duration: Duration,
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

impl Operator for DistinctCountOp {
    fn push(&mut self, event: &serde_json::Value, enrichment: Option<&ahash::AHashMap<String, serde_json::Value>>, now: SystemTime) -> Result<(), TallyError> {
        match crate::engine::operators::resolve_field(&self.field, event, enrichment) {
            None => {
                if self.optional {
                    Ok(())
                } else {
                    Err(TallyError::Type {
                        field: self.field.clone(),
                        expected: "string or numeric".into(),
                        got: "absent".into(),
                    })
                }
            }
            Some(val) => {
                let str_val = match val {
                    serde_json::Value::String(s) => s.clone(),
                    serde_json::Value::Number(n) => n.to_string(),
                    serde_json::Value::Bool(b) => b.to_string(),
                    _ => {
                        return Err(TallyError::Type {
                            field: self.field.clone(),
                            expected: "string or numeric".into(),
                            got: format!("{}", val),
                        })
                    }
                };
                self.buffer.update_current(
                    |hll| {
                        hll.insert(&str_val);
                    },
                    now,
                );
                self.event_count.add_to_current(1u64, now);
                Ok(())
            }
        }
    }

    fn read(&mut self, now: SystemTime) -> FeatureValue {
        self.buffer.advance_to(now);
        self.event_count.advance_to(now);

        // Check if any events exist in window
        if self.event_count.sum_all() == 0 {
            return FeatureValue::Missing;
        }

        // Merge all non-empty buckets into a single HLL, then count
        let mut merged = Hll::new();
        for bucket in self.buffer.buckets_iter() {
            if !bucket.is_empty() {
                merged.merge(bucket);
            }
        }
        let count = merged.count();
        FeatureValue::Float(count)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ======================== Hll Tests ========================

    #[test]
    fn test_hll_new_is_empty() {
        let hll = Hll::new();
        assert!(hll.is_empty());
    }

    #[test]
    fn test_hll_insert_single_item_count_approx_1() {
        let mut hll = Hll::new();
        hll.insert("a");
        let count = hll.count();
        // Single item: count should be close to 1.0
        assert!(count >= 0.5 && count <= 2.0,
            "Expected count ~1.0, got {}", count);
    }

    #[test]
    fn test_hll_insert_not_empty_after_insert() {
        let mut hll = Hll::new();
        hll.insert("a");
        assert!(!hll.is_empty());
    }

    #[test]
    fn test_hll_100_unique_within_10_percent() {
        let mut hll = Hll::new();
        for i in 0..100 {
            hll.insert(&format!("item_{}", i));
        }
        let count = hll.count();
        assert!(count >= 90.0 && count <= 110.0,
            "Expected count ~100, got {}", count);
    }

    #[test]
    fn test_hll_1000_unique_within_5_percent() {
        let mut hll = Hll::new();
        for i in 0..1000 {
            hll.insert(&format!("item_{}", i));
        }
        let count = hll.count();
        assert!(count >= 950.0 && count <= 1050.0,
            "Expected count ~1000, got {}", count);
    }

    #[test]
    fn test_hll_duplicate_inserts_count_approx_1() {
        let mut hll = Hll::new();
        for _ in 0..100 {
            hll.insert("same_value");
        }
        let count = hll.count();
        assert!(count >= 0.5 && count <= 2.0,
            "Expected count ~1.0 for duplicates, got {}", count);
    }

    #[test]
    fn test_hll_merge_union_semantics() {
        let mut hll1 = Hll::new();
        let mut hll2 = Hll::new();

        for i in 0..50 {
            hll1.insert(&format!("a_{}", i));
        }
        for i in 0..50 {
            hll2.insert(&format!("b_{}", i));
        }

        // Disjoint sets of 50 each, merged should be ~100
        hll1.merge(&hll2);
        let count = hll1.count();
        assert!(count >= 85.0 && count <= 115.0,
            "Expected merged count ~100, got {}", count);
    }

    #[test]
    fn test_hll_merge_overlapping_sets() {
        let mut hll1 = Hll::new();
        let mut hll2 = Hll::new();

        // Both insert the same 50 items
        for i in 0..50 {
            hll1.insert(&format!("item_{}", i));
            hll2.insert(&format!("item_{}", i));
        }

        hll1.merge(&hll2);
        let count = hll1.count();
        // Should still be ~50 (union of identical sets)
        assert!(count >= 40.0 && count <= 60.0,
            "Expected merged count ~50 for overlapping, got {}", count);
    }

    #[test]
    fn test_hll_serialized_size() {
        let hll = Hll::new();
        let bytes = postcard::to_allocvec(&hll).unwrap();
        // Registers: 16384 bytes + small overhead for Vec length encoding
        // postcard encodes Vec length as varint (3 bytes for 16384) + data
        assert!(bytes.len() >= 16384, "Expected at least 16384 bytes, got {}", bytes.len());
        assert!(bytes.len() <= 16400, "Expected at most ~16400 bytes, got {}", bytes.len());
    }

    #[test]
    fn test_hll_postcard_round_trip() {
        let mut hll = Hll::new();
        for i in 0..100 {
            hll.insert(&format!("item_{}", i));
        }
        let count_before = hll.count();

        let bytes = postcard::to_allocvec(&hll).unwrap();
        let restored: Hll = postcard::from_bytes(&bytes).unwrap();
        let count_after = restored.count();

        assert!((count_before - count_after).abs() < f64::EPSILON,
            "Round-trip changed count: {} -> {}", count_before, count_after);
    }

    #[test]
    fn test_hash_value_different_inputs_different_hashes() {
        let h1 = hash_value("hello");
        let h2 = hash_value("world");
        assert_ne!(h1, h2, "Different inputs should produce different hashes");
    }

    #[test]
    fn test_hash_value_same_input_same_hash() {
        let h1 = hash_value("hello");
        let h2 = hash_value("hello");
        assert_eq!(h1, h2, "Same input should produce same hash");
    }

    // ======================== DistinctCountOp Tests ========================

    use std::time::{Duration, UNIX_EPOCH};

    fn ts(secs: u64) -> SystemTime {
        UNIX_EPOCH + Duration::from_secs(secs)
    }

    fn make_op(optional: bool) -> DistinctCountOp {
        // 5-minute window, 1-minute buckets
        DistinctCountOp::new(
            "merchant_id",
            Duration::from_secs(5 * 60),
            Duration::from_secs(60),
            optional,
        )
    }

    fn event(field: &str, value: serde_json::Value) -> serde_json::Value {
        serde_json::json!({ field: value })
    }

    #[test]
    fn test_distinct_count_5_unique_values_approx_5() {
        let mut op = make_op(false);
        let t0 = ts(1000 * 60);
        for i in 0..5 {
            let ev = event("merchant_id", serde_json::Value::String(format!("m{}", i)));
            op.push(&ev, None, t0).unwrap();
        }
        match op.read(t0) {
            FeatureValue::Float(v) => {
                assert!(v >= 4.0 && v <= 6.0,
                    "Expected ~5 distinct, got {}", v);
            }
            other => panic!("Expected Float, got {:?}", other),
        }
    }

    #[test]
    fn test_distinct_count_5_identical_values_approx_1() {
        let mut op = make_op(false);
        let t0 = ts(1000 * 60);
        for _ in 0..5 {
            let ev = event("merchant_id", serde_json::Value::String("m_same".into()));
            op.push(&ev, None, t0).unwrap();
        }
        match op.read(t0) {
            FeatureValue::Float(v) => {
                assert!(v >= 0.5 && v <= 2.0,
                    "Expected ~1 distinct for duplicates, got {}", v);
            }
            other => panic!("Expected Float, got {:?}", other),
        }
    }

    #[test]
    fn test_distinct_count_zero_events_returns_missing() {
        let mut op = make_op(false);
        let t0 = ts(1000 * 60);
        assert_eq!(op.read(t0), FeatureValue::Missing);
    }

    #[test]
    fn test_distinct_count_expires_old_buckets() {
        let mut op = make_op(false);
        let t0 = ts(1000 * 60);

        // Push event at t0
        let ev = event("merchant_id", serde_json::Value::String("m1".into()));
        op.push(&ev, None, t0).unwrap();

        // Read at t0 -- should have data
        assert_ne!(op.read(t0), FeatureValue::Missing);

        // Read at t0 + 2 * window (10 minutes) -- all buckets expired
        let t_far = t0 + Duration::from_secs(10 * 60);
        assert_eq!(op.read(t_far), FeatureValue::Missing);
    }

    #[test]
    fn test_distinct_count_events_in_different_buckets_merge() {
        let mut op = make_op(false);
        let t0 = ts(1000 * 60);

        // Push unique values into different buckets
        for i in 0..3 {
            let t = t0 + Duration::from_secs(i * 60);
            let ev = event("merchant_id", serde_json::Value::String(format!("m{}", i)));
            op.push(&ev, None, t).unwrap();
        }

        // Read should merge all buckets, returning ~3 distinct
        let t_read = t0 + Duration::from_secs(2 * 60);
        match op.read(t_read) {
            FeatureValue::Float(v) => {
                assert!(v >= 2.0 && v <= 4.0,
                    "Expected ~3 distinct from merged buckets, got {}", v);
            }
            other => panic!("Expected Float, got {:?}", other),
        }
    }

    #[test]
    fn test_distinct_count_optional_true_skips_absent_field() {
        let mut op = make_op(true);
        let t0 = ts(1000 * 60);

        // Event without the field
        let ev = serde_json::json!({"other_field": "value"});
        assert!(op.push(&ev, None, t0).is_ok());

        // No events with the field -- should be Missing
        assert_eq!(op.read(t0), FeatureValue::Missing);
    }

    #[test]
    fn test_distinct_count_optional_false_errors_on_absent_field() {
        let mut op = make_op(false);
        let t0 = ts(1000 * 60);

        let ev = serde_json::json!({"other_field": "value"});
        let result = op.push(&ev, None, t0);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("absent"), "Error should mention absent: {}", err);
    }

    #[test]
    fn test_distinct_count_type_error_on_non_string_non_numeric() {
        let mut op = make_op(false);
        let t0 = ts(1000 * 60);

        // Array value -- not string or numeric
        let ev = serde_json::json!({"merchant_id": [1, 2, 3]});
        let result = op.push(&ev, None, t0);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("string or numeric"),
            "Error should mention expected type: {}", err);
    }

    #[test]
    fn test_distinct_count_accepts_numeric_values() {
        let mut op = make_op(false);
        let t0 = ts(1000 * 60);

        // Numeric values should be accepted (converted to string for hashing)
        for i in 0..5 {
            let ev = serde_json::json!({"merchant_id": i});
            op.push(&ev, None, t0).unwrap();
        }
        match op.read(t0) {
            FeatureValue::Float(v) => {
                assert!(v >= 4.0 && v <= 6.0,
                    "Expected ~5 distinct numeric values, got {}", v);
            }
            other => panic!("Expected Float, got {:?}", other),
        }
    }

    #[test]
    fn test_distinct_count_accepts_bool_values() {
        let mut op = make_op(false);
        let t0 = ts(1000 * 60);

        let ev1 = serde_json::json!({"merchant_id": true});
        let ev2 = serde_json::json!({"merchant_id": false});
        op.push(&ev1, None, t0).unwrap();
        op.push(&ev2, None, t0).unwrap();

        match op.read(t0) {
            FeatureValue::Float(v) => {
                assert!(v >= 1.5 && v <= 3.0,
                    "Expected ~2 distinct bools, got {}", v);
            }
            other => panic!("Expected Float, got {:?}", other),
        }
    }

    #[test]
    fn test_distinct_count_postcard_round_trip() {
        let mut op = make_op(false);
        let t0 = ts(1000 * 60);

        for i in 0..10 {
            let ev = event("merchant_id", serde_json::Value::String(format!("m{}", i)));
            op.push(&ev, None, t0).unwrap();
        }
        let val_before = op.read(t0);

        let bytes = postcard::to_allocvec(&op).unwrap();
        let mut restored: DistinctCountOp = postcard::from_bytes(&bytes).unwrap();
        let val_after = restored.read(t0);

        assert_eq!(val_before, val_after,
            "Round-trip changed value: {:?} -> {:?}", val_before, val_after);
    }
}
