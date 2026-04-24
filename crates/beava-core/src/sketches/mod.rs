//! Phase 10 sketches submodule. Plans 10-01..10-04 land child modules.

pub mod bloom;
pub mod cms;
pub mod count_distinct;
pub mod entropy;
pub mod hll;
pub mod percentile;
pub mod retracting_ring;
pub mod top_k;
pub mod uddsketch;

#[cfg(test)]
mod tests {
    #[test]
    fn module_compiles() {
        assert_eq!(1 + 1, 2);
    }
}

/// Plan 10-06: cross-sketch bincode round-trip proptests covering all 5 state
/// types (BloomFilter, EntropyHistogram, CountDistinctState, PercentileState,
/// TopKState) with arbitrary insertion sequences. Locks the SC2 contract:
/// snapshot serialization round-trips deterministically.
#[cfg(test)]
mod proptest_round_trip {
    use crate::sketches::{
        bloom::BloomFilter, cms::TopKValue, count_distinct::CountDistinctState,
        entropy::EntropyHistogram, percentile::PercentileState, top_k::TopKState,
    };
    use proptest::prelude::*;

    fn hash_str(s: &str) -> u64 {
        use std::hash::{Hash, Hasher};
        let mut h = ahash::AHasher::default();
        s.hash(&mut h);
        h.finish()
    }

    proptest! {
        #[test]
        fn bloom_round_trip(values in prop::collection::vec("[a-z]{1,8}", 0..200)) {
            let mut b = BloomFilter::with_capacity_and_fpr(256, 0.01);
            for v in &values {
                b.insert(v);
            }
            let bytes = bincode::serialize(&b).unwrap();
            let b2: BloomFilter = bincode::deserialize(&bytes).unwrap();
            for v in &values {
                prop_assert!(b2.contains(v));
            }
        }

        #[test]
        fn entropy_round_trip(values in prop::collection::vec("[a-z]{1,4}", 1..500)) {
            let mut h = EntropyHistogram::new(1024);
            for v in &values {
                h.insert(v);
            }
            let bytes = bincode::serialize(&h).unwrap();
            let h2: EntropyHistogram = bincode::deserialize(&bytes).unwrap();
            prop_assert!((h.entropy_bits() - h2.entropy_bits()).abs() < 1e-9);
            prop_assert_eq!(h.total(), h2.total());
        }

        #[test]
        fn count_distinct_round_trip(values in prop::collection::vec("[a-z]{1,8}", 0..2000)) {
            let mut s = CountDistinctState::new(1024);
            for v in &values {
                s.add_hash(hash_str(v));
            }
            let bytes = bincode::serialize(&s).unwrap();
            let s2: CountDistinctState = bincode::deserialize(&bytes).unwrap();
            prop_assert_eq!(s.estimate(), s2.estimate());
            prop_assert_eq!(s.mode_name(), s2.mode_name());
        }

        #[test]
        fn percentile_round_trip(values in prop::collection::vec(0.0_f64..1e6, 1..500)) {
            let mut s = PercentileState::new(256, 0.01);
            for &v in &values {
                s.insert(v);
            }
            let bytes = bincode::serialize(&s).unwrap();
            let s2: PercentileState = bincode::deserialize(&bytes).unwrap();
            let q1 = s.quantile(0.5).unwrap_or(0.0);
            let q2 = s2.quantile(0.5).unwrap_or(0.0);
            prop_assert!((q1 - q2).abs() < 1e-6);
            prop_assert_eq!(s.mode_name(), s2.mode_name());
        }

        #[test]
        fn top_k_round_trip(values in prop::collection::vec("[a-c]{1,3}", 0..500)) {
            let mut s = TopKState::new(3, 100, 1024, 4);
            for v in &values {
                s.insert(TopKValue::Str(v.clone()));
            }
            let bytes = bincode::serialize(&s).unwrap();
            let s2: TopKState = bincode::deserialize(&bytes).unwrap();
            // Top sets should match modulo CMS order. Compare set + counts ordering.
            prop_assert_eq!(s.top(), s2.top());
            prop_assert_eq!(s.mode_name(), s2.mode_name());
        }
    }
}
