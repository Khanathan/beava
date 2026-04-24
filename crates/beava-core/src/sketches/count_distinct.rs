//! CountDistinctState — 3-mode hybrid: ExactArray (≤16) → HashSet (≤1024) → HLL p=12.
//! Mode tag uses serde rename for snapshot stability across v0.x.y.

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
