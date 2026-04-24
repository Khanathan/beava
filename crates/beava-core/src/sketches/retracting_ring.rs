//! RetractingRingBuffer<T> ported from main, adapted to event-time clock per Phase 5 D-06.

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
        // Verified by inspection: this file must NOT contain `SystemTime`.
        let src = include_str!("retracting_ring.rs");
        assert!(
            !src.contains("SystemTime"),
            "RetractingRingBuffer must use event_time_ms only"
        );
    }
}
