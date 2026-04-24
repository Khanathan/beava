//! CountDistinctState — 3-mode hybrid: ExactArray (≤16) → HashSet (≤1024) → HLL p=12.
//! Mode tag uses serde rename for snapshot stability across v0.x.y.
//!
//! Per CONTEXT D-01 (port from main hybrid-distinct), D-04 (serde rename tags),
//! D-05 (memory bounds: ~128B/exact, ~8KB/hashset cap, ~4KB/hll dense).

use crate::sketches::hll::Hll;
use serde::{Deserialize, Serialize};

const EXACT_THRESHOLD: usize = 16;
const HASH_THRESHOLD: usize = 1024;

/// Three-mode adaptive distinct-count state. Promotes from `ExactArray` →
/// `HashSet` → `Hll` automatically on insert. Serde tags are stable v0
/// snapshot identifiers (`v0_count_distinct_*`).
// External tagging (default) is required: bincode does not support internally-
// tagged enums (those use `deserialize_any` which bincode lacks). External
// tags still satisfy v0 snapshot stability — the variant rename strings are
// the tag strings emitted in JSON / consumed by bincode's variant index.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CountDistinctState {
    #[serde(rename = "v0_count_distinct_exact_array")]
    ExactArray { values: Vec<u64> },
    #[serde(rename = "v0_count_distinct_hash_set")]
    HashSet { hashes: std::collections::HashSet<u64> },
    #[serde(rename = "v0_count_distinct_hll")]
    Hll { sketch: Hll },
}

impl CountDistinctState {
    /// Construct an empty state in `ExactArray` mode.
    /// `_hash_threshold` parameter is reserved for v0.1+ configurability;
    /// v0 uses the fixed `HASH_THRESHOLD = 1024` per locked spec (D-01).
    pub fn new(_hash_threshold: usize) -> Self {
        CountDistinctState::ExactArray {
            values: Vec::with_capacity(EXACT_THRESHOLD),
        }
    }

    pub fn mode_name(&self) -> &'static str {
        match self {
            CountDistinctState::ExactArray { .. } => "v0_count_distinct_exact_array",
            CountDistinctState::HashSet { .. } => "v0_count_distinct_hash_set",
            CountDistinctState::Hll { .. } => "v0_count_distinct_hll",
        }
    }

    /// Insert a precomputed u64 hash. Promotes mode if threshold exceeded.
    pub fn add_hash(&mut self, hash: u64) {
        match self {
            CountDistinctState::ExactArray { values } => {
                if let Err(pos) = values.binary_search(&hash) {
                    values.insert(pos, hash);
                    if values.len() > EXACT_THRESHOLD {
                        // Promote to HashSet, preserving every value seen.
                        let mut set: std::collections::HashSet<u64> =
                            std::collections::HashSet::with_capacity(HASH_THRESHOLD);
                        for &h in values.iter() {
                            set.insert(h);
                        }
                        *self = CountDistinctState::HashSet { hashes: set };
                    }
                }
            }
            CountDistinctState::HashSet { hashes } => {
                hashes.insert(hash);
                if hashes.len() > HASH_THRESHOLD {
                    // Promote to Hll. Re-feed every retained hash so cardinality
                    // estimate remains continuous across the boundary.
                    let mut hll = Hll::new();
                    for &h in hashes.iter() {
                        hll.add_hash(h);
                    }
                    *self = CountDistinctState::Hll { sketch: hll };
                }
            }
            CountDistinctState::Hll { sketch } => {
                sketch.add_hash(hash);
            }
        }
    }

    /// Estimated cardinality.
    pub fn estimate(&self) -> u64 {
        match self {
            CountDistinctState::ExactArray { values } => values.len() as u64,
            CountDistinctState::HashSet { hashes } => hashes.len() as u64,
            CountDistinctState::Hll { sketch } => sketch.estimate(),
        }
    }

    /// Approximate memory footprint in bytes.
    pub fn estimated_bytes(&self) -> usize {
        match self {
            CountDistinctState::ExactArray { values } => {
                std::mem::size_of::<Self>() + values.capacity() * std::mem::size_of::<u64>()
            }
            CountDistinctState::HashSet { hashes } => {
                std::mem::size_of::<Self>() + hashes.capacity() * 16
            }
            CountDistinctState::Hll { sketch } => sketch.estimated_bytes(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn hash_str(s: &str) -> u64 {
        use std::hash::{BuildHasher, Hash, Hasher};
        // Deterministic seed — see hll.rs tests for rationale.
        let rs = ahash::RandomState::with_seeds(
            0x243f_6a88_85a3_08d3,
            0x1319_8a2e_0370_7344,
            0xa409_3822_299f_31d0,
            0x082e_fa98_ec4e_6c89,
        );
        let mut h = rs.build_hasher();
        s.hash(&mut h);
        h.finish()
    }
    #[test]
    fn starts_in_exact_array_mode() {
        let s = CountDistinctState::new(1024);
        assert_eq!(s.mode_name(), "v0_count_distinct_exact_array");
        assert_eq!(s.estimate(), 0);
    }
    #[test]
    fn promotes_array_to_hash_set_at_16() {
        let mut s = CountDistinctState::new(1024);
        for i in 0..20 {
            s.add_hash(hash_str(&format!("k{}", i)));
        }
        assert_eq!(s.mode_name(), "v0_count_distinct_hash_set");
        assert_eq!(s.estimate(), 20);
    }
    #[test]
    fn promotes_hash_set_to_hll_at_threshold() {
        let mut s = CountDistinctState::new(1024);
        for i in 0..1100 {
            s.add_hash(hash_str(&format!("k{}", i)));
        }
        assert_eq!(s.mode_name(), "v0_count_distinct_hll");
        let est = s.estimate();
        let err = (est as i64 - 1100).abs() as f64 / 1100.0;
        assert!(err < 0.05, "promote err {}", err);
    }
    #[test]
    fn promotion_preserves_distinct_count() {
        let mut s = CountDistinctState::new(1024);
        for i in 0..15 {
            s.add_hash(hash_str(&format!("k{}", i)));
        }
        let before = s.estimate();
        s.add_hash(hash_str("k15"));
        s.add_hash(hash_str("k16"));
        assert_eq!(s.mode_name(), "v0_count_distinct_hash_set");
        assert!(s.estimate() >= before + 2);
    }
    #[test]
    fn duplicate_inserts_dont_inflate_count() {
        let mut s = CountDistinctState::new(1024);
        let h = hash_str("dup");
        for _ in 0..10 {
            s.add_hash(h);
        }
        assert_eq!(s.estimate(), 1);
    }
    #[test]
    fn bincode_round_trip_each_mode() {
        // exact_array
        let mut s1 = CountDistinctState::new(1024);
        for i in 0..5 {
            s1.add_hash(hash_str(&format!("k{}", i)));
        }
        let bytes = bincode::serialize(&s1).unwrap();
        let s1r: CountDistinctState = bincode::deserialize(&bytes).unwrap();
        assert_eq!(s1r.estimate(), s1.estimate());
        assert_eq!(s1r.mode_name(), "v0_count_distinct_exact_array");
        // hash_set
        let mut s2 = CountDistinctState::new(1024);
        for i in 0..50 {
            s2.add_hash(hash_str(&format!("k{}", i)));
        }
        let bytes = bincode::serialize(&s2).unwrap();
        let s2r: CountDistinctState = bincode::deserialize(&bytes).unwrap();
        assert_eq!(s2r.estimate(), s2.estimate());
        assert_eq!(s2r.mode_name(), "v0_count_distinct_hash_set");
        // hll
        let mut s3 = CountDistinctState::new(1024);
        for i in 0..2000 {
            s3.add_hash(hash_str(&format!("k{}", i)));
        }
        let bytes = bincode::serialize(&s3).unwrap();
        let s3r: CountDistinctState = bincode::deserialize(&bytes).unwrap();
        assert_eq!(s3r.estimate(), s3.estimate());
        assert_eq!(s3r.mode_name(), "v0_count_distinct_hll");
    }
    #[test]
    fn serde_tag_in_json() {
        let mut s = CountDistinctState::new(1024);
        s.add_hash(hash_str("a"));
        let j = serde_json::to_string(&s).unwrap();
        assert!(j.contains("v0_count_distinct_exact_array"), "json={}", j);
    }
}
