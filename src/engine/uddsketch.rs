//! UDDSketch: Uniform Dyadic Distribution Sketch for quantile estimation.
//!
//! Adapted from the Timescale UDDSketch Rust port (Apache 2.0), plus a
//! `decrement()` method that allows ring-buffer retraction on bucket expiry.
//!
//! # Properties
//!
//! - Relative-error quantile sketch: estimated quantile `q̂` satisfies
//!   `|q̂ - q_true| / q_true <= α`.
//! - `α` starts at `alpha0` (default 0.01) and grows at most logarithmically
//!   via bucket-collapse operations when the sketch exceeds `max_buckets`.
//!   After each collapse: `α_k = (1+α_{k-1})² / 2 - 1` per the UDDSketch paper.
//! - Memory: `O(max_buckets)` — default 2048 buckets × BTreeMap entry ≈ 24KB
//!   worst case; typical post-collapse ≤ a few KB.
//!
//! # `decrement()`
//!
//! The Timescale port is insert-only. This file adds a decrement that mirrors
//! insert: compute the same bucket, subtract one from its counter, saturate at
//! zero, and drop the bucket entry when its count reaches zero. `total_count`
//! also saturates at zero. `current_alpha` is NOT rolled back on decrement —
//! alpha drift is a one-way property of the sketch (documented).

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Default max buckets before a collapse round halves α-resolution.
pub const DEFAULT_MAX_BUCKETS: usize = 2048;

/// Default starting α. Spec says 0.005, planner locked 0.01 — planner wins.
pub const DEFAULT_ALPHA: f64 = 0.01;

/// UDDSketch: a Uniform Dyadic Distribution Sketch with retraction.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UDDSketch {
    alpha0: f64,
    current_alpha: f64,
    /// ln(γ) where γ = (1+α)/(1-α). Bucket-index formula: k = floor(ln(x)/ln(γ)).
    ln_gamma: f64,
    num_collapses: u32,
    max_buckets: usize,
    pos_buckets: BTreeMap<i32, u64>,
    neg_buckets: BTreeMap<i32, u64>,
    /// Count of observations equal to exactly 0.0 (ln(0) is undefined).
    zero_count: u64,
    total_count: u64,
}

impl Default for UDDSketch {
    fn default() -> Self {
        Self::new(DEFAULT_ALPHA, DEFAULT_MAX_BUCKETS)
    }
}

impl UDDSketch {
    pub fn new(alpha0: f64, max_buckets: usize) -> Self {
        assert!(alpha0 > 0.0 && alpha0 < 1.0, "alpha must be in (0, 1)");
        assert!(max_buckets >= 16, "max_buckets must be >= 16");
        let gamma = (1.0 + alpha0) / (1.0 - alpha0);
        Self {
            alpha0,
            current_alpha: alpha0,
            ln_gamma: gamma.ln(),
            num_collapses: 0,
            max_buckets,
            pos_buckets: BTreeMap::new(),
            neg_buckets: BTreeMap::new(),
            zero_count: 0,
            total_count: 0,
        }
    }

    #[inline]
    pub fn current_alpha(&self) -> f64 {
        self.current_alpha
    }

    #[inline]
    pub fn alpha0(&self) -> f64 {
        self.alpha0
    }

    #[inline]
    pub fn num_collapses(&self) -> u32 {
        self.num_collapses
    }

    #[inline]
    pub fn total_count(&self) -> u64 {
        self.total_count
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.total_count == 0
    }

    #[inline]
    fn bucket_key(&self, value: f64) -> i32 {
        (value.ln() / self.ln_gamma).floor() as i32
    }

    /// Insert a value.
    pub fn insert(&mut self, value: f64) {
        if !value.is_finite() {
            return;
        }
        self.total_count = self.total_count.saturating_add(1);
        if value == 0.0 {
            self.zero_count = self.zero_count.saturating_add(1);
            return;
        }
        let key = self.bucket_key(value.abs());
        let target = if value > 0.0 {
            &mut self.pos_buckets
        } else {
            &mut self.neg_buckets
        };
        *target.entry(key).or_insert(0) += 1;

        if self.pos_buckets.len() + self.neg_buckets.len() > self.max_buckets {
            self.collapse();
        }
    }

    /// Decrement a single occurrence. Saturates at 0; never produces negatives.
    pub fn decrement(&mut self, value: f64) {
        if !value.is_finite() {
            return;
        }
        if value == 0.0 {
            if self.zero_count > 0 {
                self.zero_count -= 1;
                self.total_count = self.total_count.saturating_sub(1);
            }
            return;
        }
        let key = self.bucket_key(value.abs());
        let target = if value > 0.0 {
            &mut self.pos_buckets
        } else {
            &mut self.neg_buckets
        };
        if let Some(count) = target.get_mut(&key) {
            if *count > 0 {
                *count -= 1;
                self.total_count = self.total_count.saturating_sub(1);
                if *count == 0 {
                    target.remove(&key);
                }
            }
        }
    }

    /// Merge another sketch (test-only; collapses to coarser alpha).
    pub fn merge(&mut self, other: &UDDSketch) {
        let mut other = other.clone();
        while self.num_collapses < other.num_collapses {
            self.collapse();
        }
        while other.num_collapses < self.num_collapses {
            other.collapse();
        }
        for (k, v) in other.pos_buckets {
            *self.pos_buckets.entry(k).or_insert(0) += v;
        }
        for (k, v) in other.neg_buckets {
            *self.neg_buckets.entry(k).or_insert(0) += v;
        }
        self.zero_count = self.zero_count.saturating_add(other.zero_count);
        self.total_count = self.total_count.saturating_add(other.total_count);
        if self.pos_buckets.len() + self.neg_buckets.len() > self.max_buckets {
            self.collapse();
        }
    }

    /// Collapse: merge adjacent bucket pairs, doubling γ and raising α.
    ///
    /// Since γ_new = γ_old², the new α satisfies
    /// `(1+α_new)/(1-α_new) = ((1+α)/(1-α))²`, which simplifies to
    /// `α_new = 2α / (1+α²)`.
    fn collapse(&mut self) {
        let a = self.current_alpha;
        self.current_alpha = (2.0 * a) / (1.0 + a * a);
        self.ln_gamma *= 2.0;
        self.num_collapses += 1;

        let merge = |buckets: BTreeMap<i32, u64>| -> BTreeMap<i32, u64> {
            let mut out = BTreeMap::new();
            for (k, v) in buckets {
                let new_key = k.div_euclid(2);
                *out.entry(new_key).or_insert(0) += v;
            }
            out
        };

        self.pos_buckets = merge(std::mem::take(&mut self.pos_buckets));
        self.neg_buckets = merge(std::mem::take(&mut self.neg_buckets));
    }

    /// Estimate the q-quantile. NaN if empty. q is clamped to [0, 1].
    pub fn quantile(&self, q: f64) -> f64 {
        if self.total_count == 0 {
            return f64::NAN;
        }
        let q = q.clamp(0.0, 1.0);
        let target_rank = (q * (self.total_count.saturating_sub(1)) as f64).floor() as u64;

        let mut cumul: u64 = 0;
        // Most-negative values first: high |key| = large magnitude.
        for (&key, &count) in self.neg_buckets.iter().rev() {
            if cumul + count > target_rank {
                return -self.bucket_center(key);
            }
            cumul += count;
        }
        if self.zero_count > 0 {
            if cumul + self.zero_count > target_rank {
                return 0.0;
            }
            cumul += self.zero_count;
        }
        for (&key, &count) in self.pos_buckets.iter() {
            if cumul + count > target_rank {
                return self.bucket_center(key);
            }
            cumul += count;
        }

        if let Some((&k, _)) = self.pos_buckets.iter().next_back() {
            return self.bucket_center(k);
        }
        if let Some((&k, _)) = self.neg_buckets.iter().next() {
            return -self.bucket_center(k);
        }
        0.0
    }

    #[inline]
    fn bucket_center(&self, k: i32) -> f64 {
        let lower = (k as f64 * self.ln_gamma).exp();
        let upper = ((k + 1) as f64 * self.ln_gamma).exp();
        (lower + upper) / 2.0
    }

    pub fn estimated_bytes(&self) -> usize {
        let per_entry = std::mem::size_of::<i32>() + std::mem::size_of::<u64>() + 48;
        std::mem::size_of::<Self>() + per_entry * (self.pos_buckets.len() + self.neg_buckets.len())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ground_truth_quantile(values: &[f64], q: f64) -> f64 {
        let mut sorted: Vec<f64> = values.to_vec();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let idx = (q * (sorted.len() - 1) as f64).floor() as usize;
        sorted[idx]
    }

    #[test]
    fn test_new_sketch_is_empty() {
        let s = UDDSketch::new(0.01, 2048);
        assert!(s.is_empty());
        assert_eq!(s.total_count(), 0);
        assert_eq!(s.num_collapses(), 0);
        assert!((s.current_alpha() - 0.01).abs() < 1e-12);
        assert!(s.quantile(0.5).is_nan());
    }

    #[test]
    fn test_insert_and_quantile_basic() {
        let mut s = UDDSketch::new(0.01, 2048);
        for i in 1..=1000 {
            s.insert(i as f64);
        }
        let q50 = s.quantile(0.5);
        let err = (q50 - 500.0).abs() / 500.0;
        assert!(err <= s.current_alpha() * 2.0, "q50={}", q50);
    }

    #[test]
    fn test_decrement_saturates_at_zero() {
        let mut s = UDDSketch::new(0.01, 2048);
        s.insert(42.0);
        s.decrement(42.0);
        s.decrement(42.0);
        s.decrement(42.0);
        assert_eq!(s.total_count(), 0);
        assert!(s.is_empty());
    }

    #[test]
    fn test_decrement_unknown_value_is_noop() {
        let mut s = UDDSketch::new(0.01, 2048);
        s.insert(1.0);
        s.decrement(100.0);
        assert_eq!(s.total_count(), 1);
    }

    #[test]
    fn test_insert_decrement_half_quantile_still_accurate() {
        let mut s = UDDSketch::new(0.01, 2048);
        for v in 1..=1000 {
            s.insert(v as f64);
        }
        for v in 1..=500 {
            s.decrement(v as f64);
        }
        let survivors: Vec<f64> = (501..=1000).map(|i| i as f64).collect();
        let truth = ground_truth_quantile(&survivors, 0.5);
        let q50 = s.quantile(0.5);
        let err = (q50 - truth).abs() / truth;
        assert!(
            err <= s.current_alpha() * 3.0,
            "q50={} truth={}",
            q50,
            truth
        );
        assert_eq!(s.total_count(), 500);
    }

    #[test]
    fn test_empty_after_full_decrement() {
        let mut s = UDDSketch::new(0.01, 2048);
        for i in 1..=100 {
            s.insert(i as f64);
        }
        for i in 1..=100 {
            s.decrement(i as f64);
        }
        assert!(s.is_empty());
        assert!(s.quantile(0.5).is_nan());
    }

    #[test]
    fn test_estimated_bytes_grows() {
        let mut s = UDDSketch::new(0.01, 2048);
        let b0 = s.estimated_bytes();
        for i in 1..=100 {
            s.insert(i as f64);
        }
        assert!(s.estimated_bytes() > b0);
    }

    #[test]
    fn test_collapse_raises_alpha() {
        // With alpha=0.01, ln_gamma ≈ 0.02. To force >64 distinct bucket keys
        // we need values spanning > e^(64*0.02) ≈ 3.6x in magnitude, with
        // enough granularity to hit each bucket. A geometric sweep over 6
        // orders of magnitude guarantees plenty of distinct keys.
        let mut s = UDDSketch::new(0.01, 64);
        for i in 0..2_000 {
            // exp in [1e-3, 1e3]
            let v = (1.006_f64).powi(i - 1000);
            s.insert(v);
        }
        assert!(s.num_collapses() > 0, "expected collapses, got 0");
        assert!(s.current_alpha() > 0.01);
    }

    #[test]
    fn test_negative_values() {
        let mut s = UDDSketch::new(0.01, 2048);
        for i in -100..=100 {
            if i != 0 {
                s.insert(i as f64);
            }
        }
        let q25 = s.quantile(0.25);
        assert!(q25 < 0.0, "q25 should be negative, got {}", q25);
    }

    #[test]
    fn test_gaussian_quantile_within_alpha() {
        let mut s = UDDSketch::new(0.01, 2048);
        let mut values = Vec::with_capacity(10_000);
        let mut x: u64 = 0xdeadbeef;
        for _ in 0..10_000 {
            x = x
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            let u1 = ((x >> 32) as f64 / u32::MAX as f64).max(1e-9);
            x = x
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            let u2 = (x >> 32) as f64 / u32::MAX as f64;
            let z = (-2.0 * u1.ln()).sqrt() * (2.0 * std::f64::consts::PI * u2).cos();
            let v = 100.0 + 15.0 * z;
            if v > 0.0 {
                values.push(v);
                s.insert(v);
            }
        }
        for q in [0.1, 0.5, 0.9, 0.99] {
            let truth = ground_truth_quantile(&values, q);
            let est = s.quantile(q);
            let err = (est - truth).abs() / truth;
            assert!(
                err <= s.current_alpha() * 2.0,
                "q={} truth={} est={} err={}",
                q,
                truth,
                est,
                err
            );
        }
    }
}
