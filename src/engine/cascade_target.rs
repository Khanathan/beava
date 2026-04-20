//! Phase 55-01 D-A2: abstraction over cross-shard cascade dispatch targets.
//!
//! Live path uses crossbeam `try_send` + tokio oneshot gather
//! (`LiveCascadeTargets`). Boot rematerialization (Wave 3) will use a
//! sync-apply impl on the main thread because fjall is single-writer and
//! shard threads are NOT yet spawned at boot-replay time (see
//! 55-RESEARCH.md Pattern 5).
//!
//! ## Design — single emission site for `beava_cascade_cross_shard_total`
//!
//! `LiveCascadeTargets::dispatch_batch` does **NOT** increment
//! `beava_cascade_cross_shard_total`. Emission is the responsibility of
//! `CascadeBuffer::flush` (single source of truth). The trait here is
//! purely a dispatch primitive.

use crate::error::BeavaError;
use crate::shard::thread::{ShardEvent, ShardHandle, ShardOp, ShardResult};
use crate::types::FeatureValue;
use ahash::AHashMap;
use std::time::SystemTime;

/// Abstraction over cross-shard TT-cascade dispatch targets.
///
/// `dispatch_batch` delivers a Vec of `(table_name, key, fields)` writes
/// to the shard at `target_shard_idx` and blocks until the target has
/// applied them all. Returns `Ok(())` on full success; on any failure,
/// returns an `Err` describing the condition (inbox full → `ShardOverload`,
/// target quarantined → `Protocol`).
pub trait CascadeTarget: Send + Sync {
    fn dispatch_batch(
        &self,
        target_shard_idx: usize,
        writes: Vec<(String, String, AHashMap<String, FeatureValue>)>,
        now: SystemTime,
    ) -> Result<(), BeavaError>;

    fn target_count(&self) -> usize;
}

/// Live implementation: ShardHandle slice. Uses crossbeam `try_send`
/// + tokio oneshot + `futures::executor::block_on` gather — matches the
/// deadlock-free pattern of `cascade_table_upsert_on_shard` in
/// `src/engine/pipeline.rs` (Phase 54 Wave 2).
pub struct LiveCascadeTargets<'a> {
    pub shards: &'a [ShardHandle],
    pub source_shard_idx: usize,
}

impl<'a> CascadeTarget for LiveCascadeTargets<'a> {
    fn dispatch_batch(
        &self,
        target_shard_idx: usize,
        writes: Vec<(String, String, AHashMap<String, FeatureValue>)>,
        now: SystemTime,
    ) -> Result<(), BeavaError> {
        debug_assert_ne!(
            target_shard_idx, self.source_shard_idx,
            "LiveCascadeTargets::dispatch_batch must not dispatch to source shard"
        );
        if target_shard_idx >= self.shards.len() {
            return Err(BeavaError::Protocol(format!(
                "cascade target_shard_idx {} out of range (n_shards={})",
                target_shard_idx,
                self.shards.len()
            )));
        }
        let target = &self.shards[target_shard_idx];
        if target.is_down.load(std::sync::atomic::Ordering::Relaxed) {
            return Err(BeavaError::Protocol(format!(
                "cascade target shard {} is down (quarantined)",
                target_shard_idx
            )));
        }

        // Phase 55-01 SC-5: high-watermark signal at 75% of inbox capacity.
        let depth = target.inbox_tx.len();
        let cap = target.inbox_tx.capacity().unwrap_or(usize::MAX);
        crate::shard::metrics::record_inbox_depth(target_shard_idx, depth, cap);

        let (tx, rx) = tokio::sync::oneshot::channel();
        let ev = ShardEvent {
            payload: bytes::Bytes::new(),
            stream_name: std::sync::Arc::from(""),
            shard_hint: 0,
            response_tx: Some(tx),
            op: ShardOp::UpsertTableBatch { writes, now },
        };

        match target.inbox_tx.try_send(ev) {
            Ok(()) => {}
            Err(crossbeam_channel::TrySendError::Full(_)) => {
                crate::shard::metrics::record_inbox_full(target_shard_idx);
                return Err(BeavaError::Protocol(format!(
                    "shard inbox full — cascade backpressure (target={})",
                    target_shard_idx
                )));
            }
            Err(crossbeam_channel::TrySendError::Disconnected(_)) => {
                return Err(BeavaError::Protocol(format!(
                    "shard inbox disconnected (target={})",
                    target_shard_idx
                )));
            }
        }

        // GATHER phase — futures::executor::block_on (NOT tokio block_on)
        // because caller runs inside tokio current_thread; see the pattern
        // comment in `cascade_table_upsert_on_shard`.
        match futures::executor::block_on(rx) {
            Ok(ShardResult::SetOk) => Ok(()),
            Ok(ShardResult::Err(e)) => Err(BeavaError::Protocol(format!(
                "cascade dispatch to shard {} failed: {:?}",
                target_shard_idx, e
            ))),
            Ok(other) => Err(BeavaError::Protocol(format!(
                "cascade dispatch to shard {} returned unexpected ShardResult: {:?}",
                target_shard_idx, other
            ))),
            Err(_) => Err(BeavaError::Protocol(format!(
                "cascade dispatch to shard {} oneshot closed",
                target_shard_idx
            ))),
        }
    }

    fn target_count(&self) -> usize {
        self.shards.len()
    }
}

/// Phase 55-03 D-C3: boot-time cascade dispatch. Applies writes directly to
/// the target shard's state (fjall partition or in-mem) on the calling (main)
/// thread. Preserves fjall single-writer invariant because shard threads are
/// NOT yet spawned during rematerialization (see
/// `src/state/recovery.rs::rematerialize_tables_from_event_logs`).
///
/// Unlike `LiveCascadeTargets`, no SPSC, no oneshot, no shard thread — every
/// `dispatch_batch` call synchronously locks the target shard and applies all
/// writes inline via `Shard::upsert_table_row`. The `CascadeTarget` trait
/// abstraction is the seam that lets boot replay reuse the exact same
/// cascade-buffer + per-batch-flush machinery as live ingest.
pub struct SyncCascadeTargets<'a> {
    /// Per-shard mutexes; `shards[target_shard_idx]` is locked in
    /// `dispatch_batch`. Using `Mutex` (rather than bare `&mut Shard`) lets
    /// multiple `SyncCascadeTargets` instances coexist when the replay driver
    /// processes sibling shards sequentially.
    pub shards: &'a [std::sync::Arc<std::sync::Mutex<crate::shard::Shard>>],
    pub source_shard_idx: usize,
}

impl<'a> CascadeTarget for SyncCascadeTargets<'a> {
    fn dispatch_batch(
        &self,
        target_shard_idx: usize,
        writes: Vec<(String, String, AHashMap<String, FeatureValue>)>,
        now: SystemTime,
    ) -> Result<(), BeavaError> {
        if target_shard_idx >= self.shards.len() {
            return Err(BeavaError::Protocol(format!(
                "sync cascade target_shard_idx {} out of range (n_shards={})",
                target_shard_idx,
                self.shards.len()
            )));
        }
        let shard_arc = &self.shards[target_shard_idx];
        let mut shard = shard_arc.lock().map_err(|e| {
            BeavaError::Protocol(format!(
                "sync cascade: failed to lock target shard {}: {e}",
                target_shard_idx
            ))
        })?;
        for (table_name, key, fields) in writes {
            // `Shard::upsert_table_row` is the same single-writer primitive
            // the live shard thread uses when applying ShardOp::UpsertTableBatch.
            // Writes land directly in the fjall partition (or AHashMap in
            // state-inmem) without going through any SPSC.
            shard.upsert_table_row(&key, &table_name, fields, now);
        }
        Ok(())
    }

    fn target_count(&self) -> usize {
        self.shards.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Counting mock — record every dispatch_batch call without touching
    /// any shard thread. Used by CascadeBuffer unit tests.
    pub struct MockCascadeTarget {
        pub calls: std::sync::Mutex<
            Vec<(
                usize,
                Vec<(String, String, AHashMap<String, FeatureValue>)>,
            )>,
        >,
        pub n_targets: usize,
    }

    impl MockCascadeTarget {
        pub fn new(n: usize) -> Self {
            Self {
                calls: std::sync::Mutex::new(Vec::new()),
                n_targets: n,
            }
        }
    }

    impl CascadeTarget for MockCascadeTarget {
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
            self.n_targets
        }
    }

    #[test]
    fn mock_target_records_dispatch() {
        let m = MockCascadeTarget::new(4);
        let writes = vec![(
            "T".to_string(),
            "k1".to_string(),
            AHashMap::new(),
        )];
        m.dispatch_batch(2, writes, SystemTime::now()).unwrap();
        let calls = m.calls.lock().unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].0, 2);
        assert_eq!(calls[0].1.len(), 1);
    }
}
