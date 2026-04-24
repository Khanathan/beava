//! Shannon entropy (bits, log2) over a categorical histogram.
//! Greenfield (no main prior art). Cap-and-spill: distinct categories beyond
//! 1024 collapse into a single "__beava_other__" bucket so memory stays bounded.

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_returns_zero() {
        let h = EntropyHistogram::new(1024);
        assert_eq!(h.entropy_bits(), 0.0);
    }

    #[test]
    fn single_category_returns_zero() {
        let mut h = EntropyHistogram::new(1024);
        for _ in 0..10 {
            h.insert("a");
        }
        assert!((h.entropy_bits() - 0.0).abs() < 1e-9);
    }

    #[test]
    fn uniform_two_categories_returns_one_bit() {
        let mut h = EntropyHistogram::new(1024);
        for _ in 0..100 {
            h.insert("a");
            h.insert("b");
        }
        assert!((h.entropy_bits() - 1.0).abs() < 0.01);
    }

    #[test]
    fn uniform_n_categories_returns_log2_n() {
        let mut h = EntropyHistogram::new(1024);
        for k in 0..8 {
            for _ in 0..100 {
                h.insert(&format!("c{}", k));
            }
        }
        // log2(8) = 3.0
        assert!((h.entropy_bits() - 3.0).abs() < 0.01);
    }

    #[test]
    fn cap_and_spill_at_threshold() {
        let mut h = EntropyHistogram::new(4); // tiny cap
        h.insert("a");
        h.insert("b");
        h.insert("c");
        h.insert("d");
        h.insert("e");
        h.insert("f");
        // After cap, e/f spill to __beava_other__
        assert_eq!(h.distinct_count(), 5); // a,b,c,d + __beava_other__
        assert_eq!(h.spill_count(), 2);
    }

    #[test]
    fn merge_combines_histograms() {
        let mut h1 = EntropyHistogram::new(1024);
        h1.insert("a");
        h1.insert("a");
        h1.insert("b");
        let mut h2 = EntropyHistogram::new(1024);
        h2.insert("a");
        h2.insert("c");
        h1.merge(&h2);
        assert_eq!(h1.total(), 5);
        // "a"=3, "b"=1, "c"=1
        let p_a = 3.0 / 5.0;
        let p_b = 1.0 / 5.0;
        let p_c = 1.0 / 5.0;
        let expected = -(p_a * p_a.log2() + p_b * p_b.log2() + p_c * p_c.log2());
        assert!((h1.entropy_bits() - expected).abs() < 1e-9);
    }

    #[test]
    fn bincode_round_trip() {
        let mut h = EntropyHistogram::new(1024);
        h.insert("x");
        h.insert("y");
        h.insert("y");
        let bytes = bincode::serialize(&h).unwrap();
        let h2: EntropyHistogram = bincode::deserialize(&bytes).unwrap();
        assert_eq!(h2.total(), h.total());
        assert!((h2.entropy_bits() - h.entropy_bits()).abs() < 1e-12);
    }
}
