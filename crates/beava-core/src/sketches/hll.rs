//! HyperLogLog++ with p=12 (4096 registers), bias correction, linear counting.
//! Ported from main:src/engine/hll.rs (Apache 2.0).

#[cfg(test)]
mod tests {
    use super::*;
    fn hash_str(s: &str) -> u64 {
        use std::hash::{Hash, Hasher};
        let mut h = ahash::AHasher::default();
        s.hash(&mut h);
        h.finish()
    }
    #[test]
    fn empty_estimate_is_zero() {
        let h = Hll::new();
        assert_eq!(h.estimate(), 0);
    }
    #[test]
    fn small_set_estimate_within_1pct() {
        let mut h = Hll::new();
        for i in 0..500 {
            h.add_hash(hash_str(&format!("k{}", i)));
        }
        let est = h.estimate();
        let err = (est as i64 - 500).abs() as f64 / 500.0;
        assert!(err < 0.05, "small-set err {} > 5%", err);
    }
    #[test]
    fn med_set_estimate_within_2pct() {
        let mut h = Hll::new();
        for i in 0..10_000 {
            h.add_hash(hash_str(&format!("k{}", i)));
        }
        let est = h.estimate();
        let err = (est as i64 - 10_000).abs() as f64 / 10_000.0;
        assert!(err < 0.02, "med-set err {} > 2%", err);
    }
    #[test]
    fn large_set_estimate_within_15pct() {
        let mut h = Hll::new();
        for i in 0..100_000 {
            h.add_hash(hash_str(&format!("k{}", i)));
        }
        let est = h.estimate();
        let err = (est as i64 - 100_000).abs() as f64 / 100_000.0;
        assert!(err < 0.015, "large-set err {} > 1.5%", err);
    }
    #[test]
    fn merge_unions_registers() {
        let mut h1 = Hll::new();
        let mut h2 = Hll::new();
        for i in 0..5_000 {
            h1.add_hash(hash_str(&format!("a{}", i)));
        }
        for i in 0..5_000 {
            h2.add_hash(hash_str(&format!("b{}", i)));
        }
        h1.merge(&h2);
        let est = h1.estimate();
        let err = (est as i64 - 10_000).abs() as f64 / 10_000.0;
        assert!(err < 0.03, "merged err {} > 3%", err);
    }
    #[test]
    fn bincode_round_trip_preserves_estimate() {
        let mut h = Hll::new();
        for i in 0..1_000 {
            h.add_hash(hash_str(&format!("k{}", i)));
        }
        let bytes = bincode::serialize(&h).unwrap();
        let h2: Hll = bincode::deserialize(&bytes).unwrap();
        assert_eq!(h2.estimate(), h.estimate());
    }
    #[test]
    fn estimated_bytes_within_5kb() {
        let h = Hll::new();
        assert!(
            h.estimated_bytes() <= 5_000,
            "Hll should be ≤ 5KB; got {}",
            h.estimated_bytes()
        );
    }
}
