//! Ported from main:src/engine/retracting_ring.rs (Apache 2.0).
//! Adapted: event_time_ms (i64) per Phase 5 D-06 (replay-determinism invariant — no
//! wall-clock reads in apply paths).
//!
//! Ring buffer with eviction callback, dedicated to hybrid sketch operators.
//! Buckets are lazily expired when `advance_to(now_ms)` is called. Before a bucket
//! is cleared, the provided `on_evict` closure is invoked with a mutable reference
//! to the bucket so the operator can process (drain / reverse-apply) its contents.

use serde::{Deserialize, Serialize};

/// Fixed-capacity time-bucketed ring buffer that notifies on bucket eviction.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetractingRingBuffer<T: Default + Clone> {
    buckets: Vec<T>,
    head: usize,
    bucket_ms: i64,
    window_ms: i64,
    current_bucket_start_ms: Option<i64>,
}

impl<T: Default + Clone> RetractingRingBuffer<T> {
    /// Construct a new ring buffer covering `window_ms` divided into `bucket_ms` slots.
    pub fn new(window_ms: i64, bucket_ms: i64) -> Self {
        let bucket = bucket_ms.max(1);
        let num_buckets = ((window_ms as f64) / (bucket as f64)).ceil() as usize;
        Self {
            buckets: vec![T::default(); num_buckets.max(1)],
            head: 0,
            bucket_ms: bucket,
            window_ms,
            current_bucket_start_ms: None,
        }
    }

    pub fn num_buckets(&self) -> usize {
        self.buckets.len()
    }

    pub fn window_ms(&self) -> i64 {
        self.window_ms
    }

    pub fn bucket_ms(&self) -> i64 {
        self.bucket_ms
    }

    pub fn head(&self) -> usize {
        self.head
    }

    /// Mutable accessor for a bucket at `offset` from index 0 (NOT relative to head).
    pub fn bucket_at(&mut self, offset: usize) -> Option<&mut T> {
        self.buckets.get_mut(offset)
    }

    /// Iterate all buckets (for read-time aggregation).
    pub fn buckets_iter(&self) -> impl Iterator<Item = &T> {
        self.buckets.iter()
    }

    /// Mutably iterate all buckets (used by transition routines).
    pub fn buckets_iter_mut(&mut self) -> impl Iterator<Item = &mut T> {
        self.buckets.iter_mut()
    }

    /// Align timestamp down to bucket start.
    fn bucket_start_for(&self, time_ms: i64) -> i64 {
        let bs = self.bucket_ms.max(1);
        (time_ms.div_euclid(bs)) * bs
    }

    /// Advance to `now_ms`, calling `on_evict(&mut bucket)` for every bucket
    /// that gets cleared. Returns the new head index.
    pub fn advance_to<F>(&mut self, now_ms: i64, mut on_evict: F) -> usize
    where
        F: FnMut(&mut T),
    {
        let start = match self.current_bucket_start_ms {
            Some(start) => start,
            None => {
                let aligned = self.bucket_start_for(now_ms);
                self.current_bucket_start_ms = Some(aligned);
                self.head = 0;
                return 0;
            }
        };

        let elapsed = now_ms.saturating_sub(start).max(0);
        let bucket = self.bucket_ms.max(1);
        let buckets_to_advance = (elapsed / bucket) as usize;
        if buckets_to_advance == 0 {
            return self.head;
        }

        let n = self.buckets.len();
        if buckets_to_advance >= n {
            // Window gap: evict every bucket.
            for b in self.buckets.iter_mut() {
                on_evict(b);
                *b = T::default();
            }
            self.head = 0;
        } else {
            for i in 1..=buckets_to_advance {
                let idx = (self.head + i) % n;
                on_evict(&mut self.buckets[idx]);
                self.buckets[idx] = T::default();
            }
            self.head = (self.head + buckets_to_advance) % n;
        }
        self.current_bucket_start_ms = Some(self.bucket_start_for(now_ms));
        self.head
    }

    /// Mutate current bucket after advancing time. Callback runs for each
    /// evicted bucket first; then `f` runs on the (now-current) head bucket.
    pub fn update_current<F, G>(&mut self, mut f: F, now_ms: i64, on_evict: G)
    where
        F: FnMut(&mut T),
        G: FnMut(&mut T),
    {
        self.advance_to(now_ms, on_evict);
        f(&mut self.buckets[self.head]);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_creates_buckets_for_window_and_bucket_size() {
        let r: RetractingRingBuffer<u64> = RetractingRingBuffer::new(60_000, 1_000);
        assert_eq!(r.num_buckets(), 60);
    }

    #[test]
    fn advance_to_evicts_stale_buckets_via_callback() {
        let mut r: RetractingRingBuffer<u64> = RetractingRingBuffer::new(10_000, 1_000);
        // First call initializes the clock at t=0.
        r.advance_to(0, |_| {});
        if let Some(b) = r.bucket_at(0) {
            *b = 5;
        }
        let mut evicted: Vec<u64> = Vec::new();
        // Jumping past the entire window evicts every bucket including the one we set.
        r.advance_to(15_000, |bucket| evicted.push(*bucket));
        assert!(evicted.contains(&5u64));
    }

    #[test]
    fn event_time_clock_no_systemtime() {
        // Verified by inspection: this file must NOT contain the wall-clock type
        // (constructed at runtime to avoid the literal in source).
        let banned = format!("System{}", "Time");
        let src = include_str!("retracting_ring.rs");
        assert!(
            !src.contains(&banned),
            "RetractingRingBuffer must use event_time_ms only"
        );
    }
}
