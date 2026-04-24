//! Bloom filter (greenfield — no main prior art). Standard bit-array + k MurmurHash3
//! double-hashing per Kirsch-Mitzenmacher.

use serde::{Deserialize, Serialize};

/// MurmurHash3 finalizer, identical to the function in cms.rs (Plan 10-04 ports CMS).
#[inline]
fn murmur3_finalize(mut h: u64, seed: u64) -> u64 {
    h ^= seed;
    h ^= h >> 33;
    h = h.wrapping_mul(0xff51afd7ed558ccd);
    h ^= h >> 33;
    h = h.wrapping_mul(0xc4ceb9fe1a85ec53);
    h ^= h >> 33;
    h
}

const SEED_A: u64 = 0x9E37_79B9_7F4A_7C15;
const SEED_B: u64 = 0xBF58_476D_1CE4_E5B9;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BloomFilter {
    /// Bits stored as u64 words. Bit i lives at words[i / 64], bit (i % 64).
    words: Vec<u64>,
    /// Total bit count (m) — words.len() * 64.
    bit_count: usize,
    /// Number of hash functions (k).
    num_hashes: u32,
}

impl BloomFilter {
    pub fn with_capacity_and_fpr(capacity: usize, fpr: f64) -> Self {
        assert!(capacity > 0 && fpr > 0.0 && fpr < 1.0);
        let m_f = -(capacity as f64) * fpr.ln() / (std::f64::consts::LN_2 * std::f64::consts::LN_2);
        let m = m_f.ceil() as usize;
        // round up to whole u64 words
        let words = m.div_ceil(64);
        let bit_count = words * 64;
        let k = ((m_f / capacity as f64) * std::f64::consts::LN_2).ceil() as u32;
        Self {
            words: vec![0u64; words],
            bit_count,
            num_hashes: k.max(1),
        }
    }

    pub fn bit_count(&self) -> usize {
        self.bit_count
    }
    pub fn num_hashes(&self) -> u32 {
        self.num_hashes
    }

    fn base_hashes(&self, value: &str) -> (u64, u64) {
        // Hash the str via ahash for the input → 64-bit base; then derive h1/h2 with seeded finalizer.
        use std::hash::{Hash, Hasher};
        let mut h = ahash::AHasher::default();
        value.hash(&mut h);
        let raw = h.finish();
        (
            murmur3_finalize(raw, SEED_A),
            murmur3_finalize(raw, SEED_B),
        )
    }

    /// Compute k bit positions via Kirsch-Mitzenmacher double-hashing: h_i = h1 + i*h2 mod m.
    fn position_at(&self, h1: u64, h2: u64, i: u32) -> usize {
        let m = self.bit_count as u64;
        ((h1.wrapping_add((i as u64).wrapping_mul(h2))) % m) as usize
    }

    pub fn insert(&mut self, value: &str) {
        let (h1, h2) = self.base_hashes(value);
        for i in 0..self.num_hashes {
            let pos = self.position_at(h1, h2, i);
            self.words[pos / 64] |= 1u64 << (pos % 64);
        }
    }

    pub fn contains(&self, value: &str) -> bool {
        let (h1, h2) = self.base_hashes(value);
        for i in 0..self.num_hashes {
            let pos = self.position_at(h1, h2, i);
            if (self.words[pos / 64] >> (pos % 64)) & 1 == 0 {
                return false;
            }
        }
        true
    }

    pub fn estimated_bytes(&self) -> usize {
        std::mem::size_of::<Self>() + self.words.capacity() * std::mem::size_of::<u64>()
    }
}

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
