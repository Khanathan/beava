//! Bloom filter (greenfield — no main prior art). Standard bit-array + k MurmurHash3
//! double-hashing per Kirsch-Mitzenmacher.

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_sizes_bits_optimally() {
        let b = BloomFilter::with_capacity_and_fpr(1024, 0.01);
        // m = -1024 * ln(0.01) / ln(2)^2 ≈ 9814 bits → rounded up to next u64 word
        assert!(b.bit_count() >= 9792);
        assert!(b.bit_count() <= 9856);
        // k = (m/n) * ln(2) ≈ 6.65 → 7
        assert_eq!(b.num_hashes(), 7);
    }

    #[test]
    fn insert_then_contains_returns_true() {
        let mut b = BloomFilter::with_capacity_and_fpr(1024, 0.01);
        b.insert("hello");
        b.insert("world");
        assert!(b.contains("hello"));
        assert!(b.contains("world"));
    }

    #[test]
    fn definitely_not_in_returns_false() {
        let b = BloomFilter::with_capacity_and_fpr(1024, 0.01);
        assert!(!b.contains("anything"));
    }

    #[test]
    fn fpr_at_capacity_within_tolerance() {
        let mut b = BloomFilter::with_capacity_and_fpr(1024, 0.01);
        for i in 0..1024 {
            b.insert(&format!("k{}", i));
        }
        let mut fp = 0;
        for i in 1024..11024 {
            if b.contains(&format!("k{}", i)) {
                fp += 1;
            }
        }
        let observed = fp as f64 / 10_000.0;
        assert!(
            observed < 0.013,
            "expected fpr ≤ 0.013 (1.3x target), got {}",
            observed
        );
    }

    #[test]
    fn bincode_round_trip() {
        let mut b = BloomFilter::with_capacity_and_fpr(256, 0.01);
        b.insert("abc");
        let bytes = bincode::serialize(&b).unwrap();
        let b2: BloomFilter = bincode::deserialize(&bytes).unwrap();
        assert!(b2.contains("abc"));
        assert!(!b2.contains("xyz"));
    }
}
