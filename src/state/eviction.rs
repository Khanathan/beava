//! TTL-based key eviction (shard-dispatch only).
//!
//! Keys with no events for 2x the largest window are evicted from memory.
//! Evicted keys re-initialize fresh on next event (CLAUDE.md spec).
//!
//! Phase 54-04 Pass B: the legacy `evict_expired_keys` and
//! `evict_expired_table_rows` `&StateStore` entry points were deleted.
//! Production eviction now flows through `evict_expired_{keys,table_rows}_on_shards`
//! which scatter-gather `ShardOp::EvictExpired{TableRows}` across live shards
//! (see `src/shard/thread.rs::evict_expired_{stream_entries,table_rows}_on_shard`).
//!
//! Phase 54-04 Pass A6b: the remaining `evict_expired_stream_entries(&StateStore, ...)`
//! helper was deleted alongside the `StateStore` struct. The body is preserved
//! inside the shard thread's `EvictExpired` dispatch arm for per-shard execution.
//! The in-file `#[cfg(test)]` tests were deleted here — the shard-dispatch path
//! is covered by integration tests under `tests/` that exercise the real shard
//! actor loop rather than the legacy DashMap store.

use crate::engine::pipeline::PipelineEngine;
use std::time::SystemTime;

/// Phase 54-04 Pass A4: shard-aware counterpart to the (now-deleted)
/// `evict_expired_stream_entries`. Scatter-gathers a per-shard
/// `ShardOp::EvictExpired` over every live `ShardHandle` and sums the
/// per-shard eviction counts.
///
/// The `engine` + `now` + `ttl_multiplier` arguments are not consumed
/// on the caller side — each shard thread re-reads `state.engine.read()`
/// inside the dispatch arm to compute per-stream TTLs against its own
/// entities. `engine` is kept in the signature for symmetry with the
/// legacy `evict_expired_keys(&StateStore, &PipelineEngine, ...)` call
/// site and to avoid a main.rs touch (locked by Pass A3).
///
/// Dispatch is fire-and-gather: `try_send` each `EvictExpired` into the
/// target shard's inbox (non-blocking, fails fast on `Full`), then
/// `futures::executor::block_on` each oneshot receiver. The eviction
/// timer lives on the main multi-thread tokio runtime (NOT a shard's
/// pinned current_thread runtime), so block_on is safe — one tokio
/// worker parks for the duration of the scatter, and eviction fires
/// once per 60s so the blocking window is bounded.
///
/// Down / Full / Disconnected shards are skipped with a metrics bump so
/// eviction progress on healthy shards is not stalled by a single bad
/// actor. Non-SetOk / non-EvictedCount responses are counted as 0.
#[allow(unused_variables)]
pub fn evict_expired_keys_on_shards(
    shard_handles: &[crate::shard::thread::ShardHandle],
    engine: &PipelineEngine,
    now: SystemTime,
    ttl_multiplier: u32,
) -> usize {
    use crate::shard::thread::{ShardEvent, ShardOp, ShardResult};
    use std::sync::atomic::Ordering;

    let mut pending: Vec<tokio::sync::oneshot::Receiver<ShardResult>> =
        Vec::with_capacity(shard_handles.len());

    // Scatter: try_send EvictExpired into each healthy shard's inbox.
    for handle in shard_handles {
        if handle.is_down.load(Ordering::Relaxed) {
            crate::shard::metrics::record_shard_down(handle.shard_index);
            continue;
        }
        let (tx, rx) = tokio::sync::oneshot::channel();
        let evt = ShardEvent {
            payload: bytes::Bytes::new(),
            stream_name: std::sync::Arc::from(""),
            shard_hint: 0,
            response_tx: Some(tx),
            op: ShardOp::EvictExpired { now, ttl_multiplier },
            payload_fmt: crate::wire::PayloadFmt::Binary,
            schema_id: 0,
        };
        match handle.inbox_tx.try_send(evt) {
            Ok(()) => pending.push(rx),
            Err(crossbeam_channel::TrySendError::Full(_)) => {
                // Eviction is best-effort; dropping the scatter on a
                // full inbox is preferable to blocking the listener.
                crate::shard::metrics::record_inbox_full(handle.shard_index);
            }
            Err(crossbeam_channel::TrySendError::Disconnected(_)) => {
                // Shard went away — nothing to evict against.
            }
        }
    }

    // Gather: block_on each oneshot Receiver. Executor is `futures`
    // (not tokio::Handle) per the Pass A2 pattern — the timer runs on
    // a multi-thread worker and one worker may briefly park here. No
    // reactor progress is required on this thread while waiting;
    // wakeups originate on the per-shard thread's sender side.
    let mut total: usize = 0;
    for rx in pending {
        match futures::executor::block_on(rx) {
            Ok(ShardResult::EvictedCount(n)) => total += n,
            // Any other variant (including Err) is counted as 0 and
            // silently skipped. A future enhancement could bump a
            // dedicated `beava_eviction_dispatch_errors_total` counter.
            _ => {}
        }
    }

    total
}

/// Phase 54-04 Pass A4: shard-aware counterpart to
/// `evict_expired_table_rows`. Scatter-gathers `ShardOp::EvictExpiredTableRows`
/// across every live `ShardHandle`; each shard thread records its own
/// evictions into the shared `EvictionTracker` (Arc-backed, RwLock<AHashMap>
/// internals are safe under multi-reader / multi-writer usage, per Wave 3).
///
/// Accepts `&EvictionTracker` for signature parity with
/// `evict_expired_table_rows(&StateStore, ..., &EvictionTracker, ...)`;
/// the shard dispatch actually uses `state.eviction_tracker` on the
/// shard side, so this caller-side reference is unused. Kept to avoid
/// a main.rs touch.
#[allow(unused_variables)]
pub fn evict_expired_table_rows_on_shards(
    shard_handles: &[crate::shard::thread::ShardHandle],
    engine: &PipelineEngine,
    tracker: &crate::state::eviction_tracker::EvictionTracker,
    now: SystemTime,
) -> usize {
    use crate::shard::thread::{ShardEvent, ShardOp, ShardResult};
    use std::sync::atomic::Ordering;

    let mut pending: Vec<tokio::sync::oneshot::Receiver<ShardResult>> =
        Vec::with_capacity(shard_handles.len());

    for handle in shard_handles {
        if handle.is_down.load(Ordering::Relaxed) {
            crate::shard::metrics::record_shard_down(handle.shard_index);
            continue;
        }
        let (tx, rx) = tokio::sync::oneshot::channel();
        let evt = ShardEvent {
            payload: bytes::Bytes::new(),
            stream_name: std::sync::Arc::from(""),
            shard_hint: 0,
            response_tx: Some(tx),
            op: ShardOp::EvictExpiredTableRows { now },
            payload_fmt: crate::wire::PayloadFmt::Binary,
            schema_id: 0,
        };
        match handle.inbox_tx.try_send(evt) {
            Ok(()) => pending.push(rx),
            Err(crossbeam_channel::TrySendError::Full(_)) => {
                crate::shard::metrics::record_inbox_full(handle.shard_index);
            }
            Err(crossbeam_channel::TrySendError::Disconnected(_)) => {}
        }
    }

    let mut total: usize = 0;
    for rx in pending {
        match futures::executor::block_on(rx) {
            Ok(ShardResult::EvictedCount(n)) => total += n,
            _ => {}
        }
    }

    total
}
