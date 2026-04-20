//! Phase 55-01 D-A1 + D-A2: per-batch source-side coalesce buffer for
//! cross-shard TT cascade.
//!
//! Lifecycle: created fresh at entry to `push_with_cascade_on_shard`'s batch
//! loop, populated per-event (ONLY when `target_shard != source_shard` —
//! same-shard writes take the inline `StoreView::Sharded::upsert_table_row`
//! path, zero SPSC hop), flushed at end-of-batch, dropped when the batch
//! returns. Stack-allocated; no Arc.
//!
//! ## Single emission site
//!
//! `CascadeBuffer::flush` is the **only** call site that increments
//! `beava_cascade_cross_shard_total`. `LiveCascadeTargets::dispatch_batch`
//! does NOT emit the counter. This avoids double-counting and keeps the
//! counter semantics identical under mock dispatch.

use crate::engine::cascade_target::CascadeTarget;
use crate::error::BeavaError;
use crate::types::FeatureValue;
use ahash::AHashMap;
use std::time::{Instant, SystemTime};

/// Per-batch coalesce buffer for cross-shard TT cascade writes.
///
/// Key: `(target_shard_idx, output_table_name, output_key)`.
/// Value: merged `fields` map (full-replace on duplicate keys within the
/// same batch — last-write-wins; matches D-B5's source-table semantics
/// extended to TT cascade output per the plan's Task-1 Test 2 spec).
pub struct CascadeBuffer {
    entries: AHashMap<(usize, String, String), AHashMap<String, FeatureValue>>,
    source_shard_idx: usize,
    n_shards: usize,
    /// Total individual cross-shard events accumulated (NOT the coalesced
    /// count — this is what gets emitted as the cross_shard_total counter
    /// so metrics reflect logical writes, not wire sends).
    total_events: usize,
    /// Batch start time — used to record `beava_cascade_lag_seconds`.
    batch_start: Instant,
}

impl CascadeBuffer {
    /// Construct an empty buffer. `n_shards` is the total sibling-shards
    /// count including the source (used for debug bounds checking).
    pub fn new(source_shard_idx: usize, n_shards: usize) -> Self {
        Self {
            entries: AHashMap::with_capacity(64),
            source_shard_idx,
            n_shards,
            total_events: 0,
            batch_start: Instant::now(),
        }
    }

    /// Accumulate a cross-shard write. Same-shard writes are NOT expected
    /// here — caller uses inline `StoreView` path for `source == target`.
    pub fn accumulate(
        &mut self,
        target_shard_idx: usize,
        table_name: String,
        key: String,
        fields: AHashMap<String, FeatureValue>,
    ) {
        debug_assert_ne!(
            target_shard_idx, self.source_shard_idx,
            "same-shard must use inline path, not CascadeBuffer"
        );
        debug_assert!(
            target_shard_idx < self.n_shards,
            "target_shard_idx {} >= n_shards {}",
            target_shard_idx,
            self.n_shards
        );
        self.entries
            .insert((target_shard_idx, table_name, key), fields); // full-replace
        self.total_events += 1;
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn total_events(&self) -> usize {
        self.total_events
    }

    /// Group entries by `target_shard_idx`, dispatch ONE batch per target
    /// via the `CascadeTarget`, and emit Phase 55 metrics. On any dispatch
    /// failure, returns early — the caller must NOT advance the cascade
    /// delivery cursor on error.
    ///
    /// Metrics emitted (single source of truth — do NOT also emit from
    /// `CascadeTarget::dispatch_batch`):
    /// - `beava_cascade_cross_shard_total{source, target}` += entries-per-target
    /// - `beava_cascade_queue_depth{source, target}` set to entries-per-target
    /// - `beava_cascade_lag_seconds{source, target}` histogram record
    pub fn flush<T: CascadeTarget>(
        self,
        target: &T,
        now: SystemTime,
    ) -> Result<(), BeavaError> {
        if self.entries.is_empty() {
            return Ok(());
        }

        // Group by target_shard_idx.
        let mut by_target: AHashMap<
            usize,
            Vec<(String, String, AHashMap<String, FeatureValue>)>,
        > = AHashMap::with_capacity(target.target_count().max(1));
        for ((tgt, tbl, k), f) in self.entries {
            by_target.entry(tgt).or_default().push((tbl, k, f));
        }

        let lag = self.batch_start.elapsed().as_secs_f64();

        for (tgt, writes) in by_target {
            let depth = writes.len();
            let src_lbl = self.source_shard_idx.to_string();
            let tgt_lbl = tgt.to_string();
            metrics::gauge!(
                crate::shard::metrics::CASCADE_QUEUE_DEPTH,
                "source" => src_lbl.clone(),
                "target" => tgt_lbl.clone(),
            )
            .set(depth as f64);

            // Dispatch. If this fails, we return Err BEFORE incrementing
            // cross_shard_total — preserves the contract that the counter
            // reflects SUCCESSFUL deliveries only.
            target.dispatch_batch(tgt, writes, now)?;

            metrics::counter!(
                crate::shard::metrics::CASCADE_CROSS_SHARD_TOTAL,
                "source" => src_lbl.clone(),
                "target" => tgt_lbl.clone(),
            )
            .increment(depth as u64);
            metrics::histogram!(
                crate::shard::metrics::CASCADE_LAG_SECONDS,
                "source" => src_lbl,
                "target" => tgt_lbl,
            )
            .record(lag);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::cascade_target::CascadeTarget;
    use std::sync::Mutex;

    struct CountingTarget {
        calls: Mutex<
            Vec<(
                usize,
                Vec<(String, String, AHashMap<String, FeatureValue>)>,
            )>,
        >,
        n: usize,
    }

    impl CountingTarget {
        fn new(n: usize) -> Self {
            Self {
                calls: Mutex::new(Vec::new()),
                n,
            }
        }
    }

    impl CascadeTarget for CountingTarget {
        fn dispatch_batch(
            &self,
            target_shard_idx: usize,
            writes: Vec<(String, String, AHashMap<String, FeatureValue>)>,
            _now: SystemTime,
        ) -> Result<(), BeavaError> {
            self.calls.lock().unwrap().push((target_shard_idx, writes));
            Ok(())
        }
        fn target_count(&self) -> usize {
            self.n
        }
    }

    fn fields_one(k: &str, v: i64) -> AHashMap<String, FeatureValue> {
        let mut m = AHashMap::new();
        m.insert(k.to_string(), FeatureValue::Int(v));
        m
    }

    #[test]
    fn new_buffer_is_empty() {
        let b = CascadeBuffer::new(0, 8);
        assert!(b.is_empty());
        assert_eq!(b.total_events(), 0);
    }

    #[test]
    fn accumulate_merges_same_key_full_replace() {
        let mut b = CascadeBuffer::new(0, 8);
        b.accumulate(5, "M".into(), "mX".into(), fields_one("count", 1));
        b.accumulate(5, "M".into(), "mX".into(), fields_one("count", 7));
        // Internal entries map still has one row (keyed by target/table/key),
        // but total_events counts both accumulates — it's the logical
        // cross-shard event count for metrics.
        assert_eq!(b.total_events(), 2);

        let mock = CountingTarget::new(8);
        b.flush(&mock, SystemTime::now()).unwrap();
        let calls = mock.calls.lock().unwrap();
        assert_eq!(calls.len(), 1);
        let (tgt, writes) = &calls[0];
        assert_eq!(*tgt, 5);
        assert_eq!(writes.len(), 1);
        // Last-write-wins: count == 7, not 1.
        assert_eq!(
            writes[0].2.get("count"),
            Some(&FeatureValue::Int(7)),
            "full-replace — last accumulate wins"
        );
    }

    #[test]
    fn empty_flush_is_noop() {
        let b = CascadeBuffer::new(0, 8);
        let mock = CountingTarget::new(8);
        b.flush(&mock, SystemTime::now()).unwrap();
        assert!(mock.calls.lock().unwrap().is_empty());
    }

    #[test]
    fn flush_groups_by_target_one_dispatch_per_target() {
        let mut b = CascadeBuffer::new(0, 8);
        b.accumulate(3, "T".into(), "k1".into(), fields_one("a", 1));
        b.accumulate(3, "T".into(), "k2".into(), fields_one("a", 2));
        b.accumulate(5, "T".into(), "k3".into(), fields_one("a", 3));

        let mock = CountingTarget::new(8);
        b.flush(&mock, SystemTime::now()).unwrap();
        let calls = mock.calls.lock().unwrap();
        // Exactly 2 dispatches — one per unique target.
        assert_eq!(calls.len(), 2);
        let mut counts: std::collections::HashMap<usize, usize> =
            std::collections::HashMap::new();
        for (tgt, writes) in calls.iter() {
            counts.insert(*tgt, writes.len());
        }
        assert_eq!(counts[&3], 2);
        assert_eq!(counts[&5], 1);
    }

    #[cfg(debug_assertions)]
    #[test]
    #[should_panic(expected = "same-shard must use inline path")]
    fn accumulate_same_shard_debug_asserts() {
        // Only fires in debug builds (release + `cargo test --release` have
        // `debug_assertions` off). Protects the invariant that the caller
        // splits same-shard vs cross-shard BEFORE hitting the buffer.
        let mut b = CascadeBuffer::new(2, 8);
        b.accumulate(2, "T".into(), "k".into(), fields_one("a", 1));
    }

    #[test]
    fn flush_propagates_dispatch_error() {
        struct ErrTarget;
        impl CascadeTarget for ErrTarget {
            fn dispatch_batch(
                &self,
                _: usize,
                _: Vec<(String, String, AHashMap<String, FeatureValue>)>,
                _: SystemTime,
            ) -> Result<(), BeavaError> {
                Err(BeavaError::Protocol("boom".into()))
            }
            fn target_count(&self) -> usize {
                8
            }
        }
        let mut b = CascadeBuffer::new(0, 8);
        b.accumulate(3, "T".into(), "k".into(), fields_one("a", 1));
        let e = b.flush(&ErrTarget, SystemTime::now()).unwrap_err();
        assert!(format!("{}", e).contains("boom"));
    }
}
