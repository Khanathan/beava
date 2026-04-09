//! HyperLogLog implementation for approximate distinct counting.
//!
//! Implements the HLL algorithm with 14-bit precision (16384 registers)
//! yielding ~1.6% standard error. Built from scratch per locked decision
//! (no external crates). Uses ahash for hash function.
//!
//! Also contains `DistinctCountOp` which wraps `RingBuffer<Hll>` for
//! windowed approximate distinct counting.

use serde::{Serialize, Deserialize};

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
}
