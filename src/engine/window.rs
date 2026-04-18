//! Time-bucketed ring buffer for sliding window aggregation.
//!
//! The `RingBuffer<T>` is the core data structure underlying all windowed
//! operators (count, sum, avg, min, max). It divides a time window into
//! fixed-duration buckets arranged in a ring, lazily expiring old buckets
//! on access rather than using background timers.

use crate::engine::event_time::DropReason;
use serde::{Deserialize, Serialize};
use std::iter::Sum;
use std::ops::AddAssign;
use std::time::{Duration, SystemTime};

/// A fixed-capacity ring buffer with time-based bucket selection.
/// Used by all windowed operators (count, sum, avg, min, max).
/// Buckets are lazily expired on advance_to() -- no background timers.
/// Clone bound (not Copy) allows use with wrapper types like MinBucket/MaxBucket.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RingBuffer<T: Default + Clone> {
    buckets: Vec<T>,
    head: usize,
    bucket_duration: Duration,
    window_duration: Duration,
    current_bucket_start: Option<SystemTime>,
    /// Last drop reason from `bucket_index_for`. Set on each drop; cleared on
    /// success. Not serialized — resets to None after snapshot reload (we lose
    /// at most the first N drops after crash-recovery; counters restart from
    /// zero, which is acceptable since they are observability-only).
    ///
    /// OBS-02 note: this field is set AFTER the late-drop gate at tcp.rs fires
    /// a `continue`, so the same event can never increment both
    /// `beava_late_events_dropped_total` and `beava_ring_buffer_drops_total`.
    /// At most one fires per dropped event.
    #[serde(skip)]
    pub last_drop: Option<DropReason>,
}

impl<T: Default + Clone> RingBuffer<T> {
    /// Create a new ring buffer for the given window and bucket durations.
    /// Bucket count = ceil(window_duration / bucket_duration).
    pub fn new(window_duration: Duration, bucket_duration: Duration) -> Self {
        let window_secs = window_duration.as_secs_f64();
        let bucket_secs = bucket_duration.as_secs_f64();
        let num_buckets = (window_secs / bucket_secs).ceil() as usize;

        Self {
            buckets: vec![T::default(); num_buckets],
            head: 0,
            bucket_duration,
            window_duration,
            current_bucket_start: None,
            last_drop: None,
        }
    }

    /// Consume and return the last drop reason recorded by `bucket_index_for`.
    /// Clears `last_drop` so the next call starts fresh.
    /// Returns `None` if the most recent bucket resolution succeeded (no drop).
    pub fn take_last_drop(&mut self) -> Option<DropReason> {
        self.last_drop.take()
    }

    /// Return the number of buckets in this ring buffer.
    pub fn num_buckets(&self) -> usize {
        self.buckets.len()
    }

    /// Advance the ring buffer to the given timestamp, zeroing skipped buckets.
    /// Returns the head index after advancement.
    ///
    /// Per RESEARCH.md Pitfall 1: Uses unwrap_or(Duration::ZERO) for SystemTime arithmetic.
    /// Per RESEARCH.md Pitfall 3: If gap > full window, zeros ALL buckets.
    pub fn advance_to(&mut self, now: SystemTime) -> usize {
        let start = match self.current_bucket_start {
            Some(start) => start,
            None => {
                // First event: initialize from the bucket-aligned time
                let aligned = self.bucket_start_for(now);
                self.current_bucket_start = Some(aligned);
                self.head = 0;
                return 0;
            }
        };

        // Pitfall 1: unwrap_or(Duration::ZERO) for out-of-order timestamps
        let elapsed = now.duration_since(start).unwrap_or(Duration::ZERO);
        let bucket_secs = self.bucket_duration.as_secs_f64();
        let buckets_to_advance = (elapsed.as_secs_f64() / bucket_secs) as usize;

        if buckets_to_advance == 0 {
            return self.head;
        }

        let num_buckets = self.buckets.len();

        if buckets_to_advance >= num_buckets {
            // Pitfall 3: gap exceeds full window -- zero ALL buckets
            for bucket in self.buckets.iter_mut() {
                *bucket = T::default();
            }
            self.head = 0;
        } else {
            // Zero only the skipped buckets (head+1 through head+advance, mod num_buckets)
            for i in 1..=buckets_to_advance {
                let idx = (self.head + i) % num_buckets;
                self.buckets[idx] = T::default();
            }
            self.head = (self.head + buckets_to_advance) % num_buckets;
        }

        // Update current_bucket_start to the new bucket's start time
        self.current_bucket_start = Some(self.bucket_start_for(now));

        self.head
    }

    /// Align a timestamp down to the start of its containing bucket.
    fn bucket_start_for(&self, time: SystemTime) -> SystemTime {
        let since_epoch = time
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or(Duration::ZERO);
        let bucket_secs = self.bucket_duration.as_secs();
        // Integer division truncates, aligning down to bucket boundary
        let aligned_secs = (since_epoch.as_secs() / bucket_secs) * bucket_secs;
        SystemTime::UNIX_EPOCH + Duration::from_secs(aligned_secs)
    }
}

impl<T: Default + Clone + AddAssign> RingBuffer<T> {
    /// Add a value to the current bucket, advancing time if needed.
    ///
    /// Retained as a thin compatibility shim: from Phase 24-04 onwards
    /// every production caller in `src/engine/operators.rs` passes the
    /// parsed event-time as `now`, and we route through
    /// `add_at_event_time` so out-of-order events within the window
    /// land in their historical bucket. A direct alias is cheaper than
    /// churning every operator call-site.
    pub fn add_to_current(&mut self, value: T, now: SystemTime) {
        self.add_at_event_time(value, now);
    }

    /// Phase 24-04: event-time aware add. Routes `value` into the
    /// bucket that CONTAINS `event_time` rather than the head bucket.
    ///
    /// Behavior:
    /// - If `event_time >= current_bucket_start + bucket_duration`:
    ///   advance the head forward (existing expiry semantics) and add
    ///   to the new head. Same as `add_to_current(value, event_time)`.
    /// - If `event_time` is within the current bucket: add to head
    ///   (same as before).
    /// - If `event_time` falls in a historical bucket still inside the
    ///   window: the corresponding ring slot is incremented in place —
    ///   out-of-order events within the window land in the correct
    ///   bucket.
    /// - If `event_time` is older than the full window (older than
    ///   `current_bucket_start - (num_buckets - 1) * bucket_duration`):
    ///   silently dropped. This is redundant with the
    ///   `WatermarkTracker` late-drop gate at the TCP layer but safe.
    pub fn add_at_event_time(&mut self, value: T, event_time: SystemTime) {
        if let Some(idx) = self.bucket_index_for(event_time) {
            self.buckets[idx] += value;
        }
    }
}

impl<T: Default + Clone> RingBuffer<T> {
    /// Mutate the current (head) bucket via a closure, advancing time if needed.
    ///
    /// Phase 24-04: now a shim onto `update_at_event_time` — `now` is
    /// treated as the event-time by every production caller (the
    /// rename at the operator layer is explicit in Task 2; this keeps
    /// it local to the ring).
    pub fn update_current<F: FnOnce(&mut T)>(&mut self, f: F, now: SystemTime) {
        self.update_at_event_time(f, now);
    }

    /// Phase 24-04: event-time aware update. Picks the bucket
    /// containing `event_time` (see `add_at_event_time` for routing
    /// semantics). If the event_time is out-of-window, the closure
    /// is NOT invoked — the event is dropped at the ring level.
    pub fn update_at_event_time<F: FnOnce(&mut T)>(&mut self, f: F, event_time: SystemTime) {
        if let Some(idx) = self.bucket_index_for(event_time) {
            f(&mut self.buckets[idx]);
        }
    }

    /// Resolve the ring slot corresponding to `event_time`. Returns
    /// `None` if the event must be dropped. Side effect: sets
    /// `self.last_drop` to the drop reason on a drop, or `None` on
    /// success. Callers use `take_last_drop()` to retrieve the reason.
    ///
    /// Drop reasons (D-05 / OBS-01):
    /// - `PreEpoch`: `event_time < UNIX_EPOCH`.
    /// - `TooNew`: `et_bucket_start > current_bucket_start` (overflow).
    /// - `TooOld`: event is older than the full retained window.
    ///
    /// OBS-02: at most one of `beava_late_events_dropped_total` or
    /// `beava_ring_buffer_drops_total` fires per event. The late-drop
    /// gate at `tcp.rs:~1753` fires a `continue` before reaching this
    /// code, so both counters cannot fire for the same event.
    fn bucket_index_for(&mut self, event_time: SystemTime) -> Option<usize> {
        use std::time::UNIX_EPOCH;

        // PreEpoch check: event_time before UNIX_EPOCH produces a zero
        // `since_epoch` in `bucket_start_for` (unwrap_or(ZERO)), which
        // maps to UNIX_EPOCH. That would silently fold into a large
        // delta_buckets drop. Detect it explicitly so the reason is
        // accurate.
        if event_time < UNIX_EPOCH {
            // OBS-02: exclusive with beava_late_events_dropped_total by path.
            self.last_drop = Some(DropReason::PreEpoch);
            return None;
        }

        // Fresh ring — use existing path to initialize.
        let start = match self.current_bucket_start {
            Some(s) => s,
            None => {
                self.last_drop = None;
                self.advance_to(event_time);
                return Some(self.head);
            }
        };

        // Forward case: event_time is in the head bucket or beyond →
        // existing advance_to handles bucket expiry.
        if event_time >= start {
            self.last_drop = None;
            self.advance_to(event_time);
            return Some(self.head);
        }

        // Historical case: compute how many buckets back from the head
        // this event_time falls. Align event_time to its bucket start
        // and compare.
        let et_bucket_start = self.bucket_start_for(event_time);
        let delta = match start.duration_since(et_bucket_start) {
            Ok(d) => d,
            Err(_) => {
                // et_bucket_start > start: event time rounds up past the
                // current head — treat as TooNew.
                // OBS-02: exclusive with beava_late_events_dropped_total by path.
                self.last_drop = Some(DropReason::TooNew);
                return None;
            }
        };
        let bucket_secs = self.bucket_duration.as_secs();
        if bucket_secs == 0 {
            self.last_drop = None;
            return Some(self.head);
        }
        let delta_buckets = (delta.as_secs() / bucket_secs) as usize;
        let num_buckets = self.buckets.len();
        if delta_buckets >= num_buckets {
            // Past the full window → drop.
            // OBS-02: exclusive with beava_late_events_dropped_total by path.
            self.last_drop = Some(DropReason::TooOld);
            return None;
        }
        self.last_drop = None;
        // Walk back from the head. Add num_buckets to avoid negative
        // wrap under modular subtraction.
        let idx = (self.head + num_buckets - delta_buckets) % num_buckets;
        Some(idx)
    }

    /// Iterate over all bucket values in the ring buffer.
    pub fn buckets_iter(&self) -> impl Iterator<Item = &T> {
        self.buckets.iter()
    }
}

impl<T: Default + Clone + Sum> RingBuffer<T> {
    /// Sum all bucket values currently in the buffer.
    pub fn sum_all(&self) -> T {
        self.buckets.iter().cloned().sum()
    }
}

impl<T: Default + Clone + PartialEq> RingBuffer<T> {
    /// Count the number of buckets with non-default (non-zero) values.
    pub fn count_nonzero(&self) -> usize {
        let default = T::default();
        self.buckets.iter().filter(|b| **b != default).count()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    fn ts(secs: u64) -> SystemTime {
        UNIX_EPOCH + Duration::from_secs(secs)
    }

    #[test]
    fn test_new_30m_window_1m_bucket_creates_30_buckets() {
        let rb = RingBuffer::<u64>::new(Duration::from_secs(30 * 60), Duration::from_secs(60));
        assert_eq!(rb.num_buckets(), 30);
    }

    #[test]
    fn test_new_24h_window_15m_bucket_creates_96_buckets() {
        let rb = RingBuffer::<u64>::new(
            Duration::from_secs(24 * 60 * 60),
            Duration::from_secs(15 * 60),
        );
        assert_eq!(rb.num_buckets(), 96);
    }

    #[test]
    fn test_non_divisible_window_rounds_up() {
        // 31 minutes / 10 minute buckets = ceil(3.1) = 4 buckets
        let rb = RingBuffer::<u64>::new(Duration::from_secs(31 * 60), Duration::from_secs(10 * 60));
        assert_eq!(rb.num_buckets(), 4);
    }

    #[test]
    fn test_add_to_current_increments_head_bucket() {
        let mut rb = RingBuffer::<u64>::new(Duration::from_secs(30 * 60), Duration::from_secs(60));
        let now = ts(1000 * 60); // Use an even minute boundary
        rb.add_to_current(1, now);
        rb.add_to_current(1, now);
        rb.add_to_current(1, now);
        assert_eq!(rb.sum_all(), 3);
    }

    #[test]
    fn test_advance_within_same_bucket_returns_same_head() {
        let mut rb = RingBuffer::<u64>::new(Duration::from_secs(30 * 60), Duration::from_secs(60));
        let now = ts(1000 * 60);
        let head1 = rb.advance_to(now);
        // 30 seconds later, still in same bucket
        let head2 = rb.advance_to(now + Duration::from_secs(30));
        assert_eq!(head1, head2);
    }

    #[test]
    fn test_advance_by_one_bucket_zeros_next_and_moves_head() {
        let mut rb = RingBuffer::<u64>::new(Duration::from_secs(5 * 60), Duration::from_secs(60));
        let t0 = ts(1000 * 60);
        rb.add_to_current(10, t0);
        assert_eq!(rb.sum_all(), 10);

        // Advance by exactly 1 bucket (60 seconds)
        let t1 = t0 + Duration::from_secs(60);
        let old_head = rb.advance_to(t0);
        let new_head = rb.advance_to(t1);
        assert_ne!(old_head, new_head);

        // The new bucket should be zeroed, so sum is still 10
        // (the old value is in a different bucket)
        assert_eq!(rb.sum_all(), 10);
    }

    #[test]
    fn test_advance_by_three_buckets_zeros_three() {
        let mut rb = RingBuffer::<u64>::new(Duration::from_secs(5 * 60), Duration::from_secs(60));
        let t0 = ts(1000 * 60);
        rb.add_to_current(5, t0);

        // Advance by 3 buckets (180 seconds)
        let t1 = t0 + Duration::from_secs(3 * 60);
        rb.advance_to(t1);

        // Old data in bucket 0 should still be there (only 3 buckets skipped, 5 total)
        assert_eq!(rb.sum_all(), 5);
    }

    #[test]
    fn test_advance_beyond_full_window_zeros_all_buckets() {
        let mut rb = RingBuffer::<u64>::new(Duration::from_secs(5 * 60), Duration::from_secs(60));
        let t0 = ts(1000 * 60);
        rb.add_to_current(100, t0);
        assert_eq!(rb.sum_all(), 100);

        // Advance by more than the full window (10 minutes > 5 minute window)
        let t1 = t0 + Duration::from_secs(10 * 60);
        rb.advance_to(t1);

        // ALL buckets should be zeroed (pitfall 3)
        assert_eq!(rb.sum_all(), 0);
    }

    #[test]
    fn test_sum_all_returns_sum_of_all_buckets() {
        let mut rb = RingBuffer::<u64>::new(Duration::from_secs(5 * 60), Duration::from_secs(60));
        let t0 = ts(1000 * 60);
        rb.add_to_current(10, t0);
        rb.add_to_current(20, t0 + Duration::from_secs(60));
        rb.add_to_current(30, t0 + Duration::from_secs(2 * 60));
        assert_eq!(rb.sum_all(), 60);
    }

    #[test]
    fn test_first_event_initializes_current_bucket_start() {
        let mut rb = RingBuffer::<u64>::new(Duration::from_secs(5 * 60), Duration::from_secs(60));
        assert!(rb.current_bucket_start.is_none());

        let now = ts(1000 * 60 + 30); // Mid-bucket
        rb.advance_to(now);
        assert!(rb.current_bucket_start.is_some());
    }

    #[test]
    fn test_out_of_order_timestamp_uses_duration_zero() {
        let mut rb = RingBuffer::<u64>::new(Duration::from_secs(5 * 60), Duration::from_secs(60));
        let t0 = ts(1000 * 60);
        rb.add_to_current(10, t0);

        // Send an event with an earlier timestamp -- should not panic (pitfall 1)
        let t_earlier = ts(999 * 60);
        rb.add_to_current(5, t_earlier);

        // Both values should be in the buffer (earlier event goes to current bucket)
        assert_eq!(rb.sum_all(), 15);
    }

    #[test]
    fn test_count_nonzero_returns_number_of_nondefault_buckets() {
        let mut rb = RingBuffer::<u64>::new(Duration::from_secs(5 * 60), Duration::from_secs(60));
        let t0 = ts(1000 * 60);
        rb.add_to_current(1, t0);
        rb.add_to_current(1, t0 + Duration::from_secs(60));
        rb.add_to_current(1, t0 + Duration::from_secs(2 * 60));

        assert_eq!(rb.count_nonzero(), 3);
    }

    #[test]
    fn test_f64_ring_buffer_sum() {
        let mut rb = RingBuffer::<f64>::new(Duration::from_secs(5 * 60), Duration::from_secs(60));
        let t0 = ts(1000 * 60);
        rb.add_to_current(1.5, t0);
        rb.add_to_current(2.5, t0 + Duration::from_secs(60));
        assert!((rb.sum_all() - 4.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_bucket_wraps_around_ring() {
        // 3-bucket ring buffer, push through more than 3 buckets
        let mut rb = RingBuffer::<u64>::new(Duration::from_secs(3 * 60), Duration::from_secs(60));
        let t0 = ts(1000 * 60);
        rb.add_to_current(1, t0);
        rb.add_to_current(2, t0 + Duration::from_secs(60));
        rb.add_to_current(3, t0 + Duration::from_secs(2 * 60));

        // All 3 buckets full: sum = 6
        assert_eq!(rb.sum_all(), 6);

        // Advance one more bucket -- oldest (1) should be zeroed
        rb.add_to_current(4, t0 + Duration::from_secs(3 * 60));
        assert_eq!(rb.sum_all(), 9); // 2 + 3 + 4
    }

    // ======================== update_current Tests ========================

    #[test]
    fn test_update_current_replaces_value_via_closure() {
        let mut rb = RingBuffer::<f64>::new(Duration::from_secs(5 * 60), Duration::from_secs(60));
        let t0 = ts(1000 * 60);
        // Set initial value via update_current
        rb.update_current(|b| *b = 10.0, t0);
        assert_eq!(rb.buckets_iter().next().copied(), Some(10.0));

        // Update: only replace if smaller
        rb.update_current(
            |b| {
                if 5.0 < *b {
                    *b = 5.0
                }
            },
            t0,
        );
        // Bucket should now be 5.0 (replaced because 5 < 10)
        let vals: Vec<f64> = rb.buckets_iter().cloned().collect();
        assert_eq!(vals[rb.head], 5.0);
    }

    #[test]
    fn test_update_current_advances_time() {
        let mut rb = RingBuffer::<f64>::new(Duration::from_secs(5 * 60), Duration::from_secs(60));
        let t0 = ts(1000 * 60);
        rb.update_current(|b| *b = 10.0, t0);
        let head_before = rb.head;

        // Advance by one bucket
        let t1 = t0 + Duration::from_secs(60);
        rb.update_current(|b| *b = 20.0, t1);
        let head_after = rb.head;
        assert_ne!(head_before, head_after);
    }

    // ======================== buckets_iter Tests ========================

    #[test]
    fn test_buckets_iter_returns_all_buckets() {
        let rb = RingBuffer::<u64>::new(Duration::from_secs(5 * 60), Duration::from_secs(60));
        let count = rb.buckets_iter().count();
        assert_eq!(count, 5);
    }

    #[test]
    fn test_buckets_iter_reflects_added_values() {
        let mut rb = RingBuffer::<u64>::new(Duration::from_secs(3 * 60), Duration::from_secs(60));
        let t0 = ts(1000 * 60);
        rb.add_to_current(42, t0);
        let sum: u64 = rb.buckets_iter().sum();
        assert_eq!(sum, 42);
    }
}
