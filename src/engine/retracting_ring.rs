//! Ring buffer with eviction callback, dedicated to hybrid sketch operators.
//!
//! Duplicated from `window.rs::RingBuffer<T>` intentionally (per 22-03
//! decision A=2): we need to invoke a callback with the *contents* of each
//! evicted bucket so sketch operators can decrement their backing state
//! (UDDSketch, CMS, etc.) when a bucket expires out of the window.
//!
//! `window.rs`'s RingBuffer stays untouched so 22-02 operators (count, sum,
//! avg, variance, ...) are unaffected.
//!
//! The callback receives `&mut T` on the bucket right *before* it is
//! replaced with `T::default()`, so the operator can drain whatever
//! retention structure the bucket holds (typically a `Vec<f64>` or
//! `Vec<TopKValue>`).

use serde::{Deserialize, Serialize};
use std::time::{Duration, SystemTime};

/// Fixed-capacity time-bucketed ring buffer that notifies on bucket eviction.
///
/// Semantics match `window::RingBuffer`: buckets are lazily expired when
/// `advance_to(now)` is called. Before a bucket is cleared, the provided
/// `on_evict` closure is invoked with a mutable reference to the bucket so
/// the operator can process (drain / reverse-apply) its contents.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetractingRingBuffer<T: Default + Clone> {
    buckets: Vec<T>,
    head: usize,
    bucket_duration: Duration,
    window_duration: Duration,
    current_bucket_start: Option<SystemTime>,
}

impl<T: Default + Clone> RetractingRingBuffer<T> {
    pub fn new(window_duration: Duration, bucket_duration: Duration) -> Self {
        let window_secs = window_duration.as_secs_f64();
        let bucket_secs = bucket_duration.as_secs_f64();
        let num_buckets = (window_secs / bucket_secs).ceil() as usize;
        Self {
            buckets: vec![T::default(); num_buckets.max(1)],
            head: 0,
            bucket_duration,
            window_duration,
            current_bucket_start: None,
        }
    }

    pub fn num_buckets(&self) -> usize {
        self.buckets.len()
    }

    pub fn window_duration(&self) -> Duration {
        self.window_duration
    }

    pub fn bucket_duration(&self) -> Duration {
        self.bucket_duration
    }

    pub fn head(&self) -> usize {
        self.head
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
    fn bucket_start_for(&self, time: SystemTime) -> SystemTime {
        let since_epoch = time
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or(Duration::ZERO);
        let bs = self.bucket_duration.as_secs().max(1);
        let aligned = (since_epoch.as_secs() / bs) * bs;
        SystemTime::UNIX_EPOCH + Duration::from_secs(aligned)
    }

    /// Advance to `now`, calling `on_evict(&mut bucket)` for every bucket
    /// that gets cleared. Returns the new head index.
    pub fn advance_to<F>(&mut self, now: SystemTime, mut on_evict: F) -> usize
    where
        F: FnMut(&mut T),
    {
        let start = match self.current_bucket_start {
            Some(start) => start,
            None => {
                let aligned = self.bucket_start_for(now);
                self.current_bucket_start = Some(aligned);
                self.head = 0;
                return 0;
            }
        };

        let elapsed = now.duration_since(start).unwrap_or(Duration::ZERO);
        let bucket_secs = self.bucket_duration.as_secs_f64();
        let buckets_to_advance = (elapsed.as_secs_f64() / bucket_secs) as usize;
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
        self.current_bucket_start = Some(self.bucket_start_for(now));
        self.head
    }

    /// Mutate current bucket after advancing time. Callback runs for each
    /// evicted bucket first; then `f` runs on the (now-current) head bucket.
    pub fn update_current<F, G>(&mut self, mut f: F, now: SystemTime, on_evict: G)
    where
        F: FnMut(&mut T),
        G: FnMut(&mut T),
    {
        self.advance_to(now, on_evict);
        f(&mut self.buckets[self.head]);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::UNIX_EPOCH;

    fn ts(secs: u64) -> SystemTime {
        UNIX_EPOCH + Duration::from_secs(secs)
    }

    #[test]
    fn test_eviction_callback_fires_on_advance() {
        let mut rb: RetractingRingBuffer<Vec<i32>> =
            RetractingRingBuffer::new(Duration::from_secs(180), Duration::from_secs(60));
        let t0 = ts(60_000);
        rb.update_current(|b| b.push(1), t0, |_| {});
        rb.update_current(|b| b.push(2), t0, |_| {});
        // Advance well past the window (>180s).
        let mut evicted: Vec<i32> = Vec::new();
        rb.update_current(
            |_| {},
            t0 + Duration::from_secs(1_000),
            |b| evicted.append(b),
        );
        assert!(evicted.contains(&1));
        assert!(evicted.contains(&2));
    }

    #[test]
    fn test_no_eviction_within_same_bucket() {
        let mut rb: RetractingRingBuffer<Vec<i32>> =
            RetractingRingBuffer::new(Duration::from_secs(180), Duration::from_secs(60));
        let t0 = ts(60_000);
        rb.update_current(|b| b.push(1), t0, |_| {});
        let mut evicted: Vec<i32> = Vec::new();
        rb.update_current(
            |b| b.push(2),
            t0 + Duration::from_secs(10),
            |b| evicted.append(b),
        );
        assert!(evicted.is_empty());
    }

    #[test]
    fn test_partial_advance_evicts_only_stepped_buckets() {
        let mut rb: RetractingRingBuffer<Vec<i32>> =
            RetractingRingBuffer::new(Duration::from_secs(300), Duration::from_secs(60));
        let t0 = ts(60_000);
        rb.update_current(|b| b.push(10), t0, |_| {});
        // Step 1 bucket forward; it should not evict anything yet (the old
        // bucket is still within the window).
        let mut evicted: Vec<i32> = Vec::new();
        rb.update_current(
            |b| b.push(20),
            t0 + Duration::from_secs(60),
            |b| evicted.append(b),
        );
        assert!(evicted.is_empty());
        // Now jump past the window: original (10) and (20) both evicted.
        let mut evicted2: Vec<i32> = Vec::new();
        rb.update_current(
            |_| {},
            t0 + Duration::from_secs(1_000),
            |b| evicted2.append(b),
        );
        assert!(evicted2.contains(&10));
        assert!(evicted2.contains(&20));
    }
}
