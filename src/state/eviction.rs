//! TTL-based key eviction.
//!
//! Keys with no events for 2x the largest window are evicted from memory.
//! Evicted keys re-initialize fresh on next event (CLAUDE.md spec).
//!
//! Phase 54-04 Pass B: the legacy `evict_expired_keys` and
//! `evict_expired_table_rows` `&StateStore` entry points were deleted here.
//! Production eviction now flows through `evict_expired_{keys,table_rows}_on_shards`
//! which scatter-gather `ShardOp::EvictExpired{TableRows}` across live shards
//! (see `src/shard/thread.rs::evict_expired_{stream_entries,table_rows}_on_shard`).
//! `evict_expired_stream_entries` remains only as a reference body for the
//! in-file tests and the `state-inmem` feature; production callers are zero.

use crate::engine::pipeline::PipelineEngine;
use crate::state::store::StateStore;
use std::time::SystemTime;

/// Evict individual stream entries from entities based on per-stream entity_ttl.
/// Two-phase process:
/// 1. For each entity, iterate streams and remove those whose last_event_at
///    exceeds their stream's entity_ttl.
/// 2. Remove entities that have zero remaining streams AND zero static_features.
///
/// Streams with entity_ttl=None fall back to the global TTL behavior
/// (ttl_multiplier * max_window). Streams with last_event_at=None are not evicted.
///
/// Returns the number of stream entries evicted.
///
/// Phase 54-04 Pass B: still takes `&StateStore` (tests-only callers remain).
/// Production eviction flows through
/// `evict_expired_{keys,table_rows}_on_shards`; Pass C deletes both `StateStore`
/// and this helper when the `state-inmem` feature is retired.
pub fn evict_expired_stream_entries(
    store: &StateStore,
    engine: &PipelineEngine,
    now: SystemTime,
    ttl_multiplier: u32,
) -> usize {
    let max_window = engine.max_window_duration();
    let global_ttl = if max_window.is_zero() {
        None // No global fallback when max_window is zero
    } else {
        Some(max_window * ttl_multiplier)
    };

    let mut total_evicted = 0;

    // Phase 1: Collect eviction decisions using only immutable borrows, so we
    // can separately call mark_deleted before mutating
    // the entity streams. Each plan entry is (key, streams_to_remove, will_be_empty).
    let entity_keys: Vec<String> = store.entity_keys();
    let mut eviction_plan: Vec<(String, Vec<String>, bool)> = Vec::new();

    for key in &entity_keys {
        if let Some(entity) = store.get_entity(key) {
            // Collect stream names to evict
            let mut streams_to_remove: Vec<String> = Vec::new();

            for (stream_name, stream_state) in entity.streams.iter() {
                // Skip streams with no last_event_at (never received an event)
                let last_event = match stream_state.last_event_at {
                    Some(t) => t,
                    None => continue,
                };

                // Determine the TTL for this stream
                let ttl = match engine.get_stream_entity_ttl(stream_name) {
                    Some(stream_ttl) => stream_ttl,
                    None => match global_ttl {
                        Some(gt) => gt,
                        None => continue, // No TTL applicable -- skip
                    },
                };

                // D-17 / CORR-07: source the eviction clock from the per-stream
                // watermark's observed_max so historical backfills (30-day-old events)
                // don't immediately evict entities whose event-time is old-by-wall-clock
                // but alive-by-event-time. Fallback to the wall-clock `now` arg preserves
                // existing semantics for streams that have never been observed.
                let scan_clock = engine.wm_observed_max(stream_name).unwrap_or(now);

                // Check if stream entry is expired
                let age = scan_clock
                    .duration_since(last_event)
                    .unwrap_or(std::time::Duration::ZERO);
                if age > ttl {
                    streams_to_remove.push(stream_name.clone());
                }
            }

            if !streams_to_remove.is_empty() {
                // Will this entity become completely empty (and thus fully removed
                // by Phase 3 remove_empty_entities)? If so, mark it deleted so the
                // next delta snapshot records the removal (OPS-03).
                let remaining = entity.streams.len().saturating_sub(streams_to_remove.len());
                let will_be_empty = remaining == 0 && entity.static_features.is_empty();
                eviction_plan.push((key.clone(), streams_to_remove, will_be_empty));
            }
        }
    }

    // Phase 2: Apply evictions. Mark fully-removed entities as deleted BEFORE
    // mutating streams, so the snapshot delta can include them in deleted_keys
    // even if a concurrent snapshot cycle observes an intermediate state.
    for (key, streams_to_remove, will_be_empty) in &eviction_plan {
        if *will_be_empty {
            store.mark_deleted(key);
        }
        if let Some(mut entity) = store.get_entity_mut(key) {
            for stream_name in streams_to_remove {
                entity.streams.remove(stream_name);
            }
        }
        total_evicted += streams_to_remove.len();
    }

    // Phase 3: Remove entities that are now empty (no streams AND no static features)
    store.remove_empty_entities();

    total_evicted
}

/// Phase 54-04 Pass A4: shard-aware counterpart to
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::pipeline::{FeatureDef, StreamDefinition};
    use std::time::{Duration, UNIX_EPOCH};

    fn ts(secs: u64) -> SystemTime {
        UNIX_EPOCH + Duration::from_secs(secs)
    }

    fn make_stream_with_window(name: &str, window_secs: u64) -> StreamDefinition {
        StreamDefinition {
            name: name.to_string(),
            key_field: Some("user_id".to_string()),
            group_by_keys: None,
            features: vec![(
                "count".to_string(),
                FeatureDef::Count {
                    window: Duration::from_secs(window_secs),
                    bucket: Duration::from_secs(60),
                    where_expr: None,
                    backfill: false,
                },
            )],
            depends_on: None,
            filter: None,
            entity_ttl: None,
            history_ttl: None,
            projection: None,
            ephemeral: None,
            pipeline_ttl: None,
            max_keys: None,
            watermark_lateness: None,
            shard_key: None,
        }
    }

    #[test]
    fn test_evict_expired_keys_removes_old() {
        let store = StateStore::new();
        let mut engine = PipelineEngine::new();
        engine
            .register(make_stream_with_window("stream1", 3600))
            .unwrap(); // 1h window

        // Add entity with old last_event_at
        {
            let mut entity = store.get_or_create_entity("old_user");
            let stream = entity.get_or_create_stream("stream1");
            stream.last_event_at = Some(ts(1000)); // Very old
        }

        let now = ts(100_000);
        // TTL = 2 * 3600 = 7200 seconds. Entity is 99000 seconds old -> evicted.
        let evicted = evict_expired_stream_entries(&store, &engine, now, 2);
        assert_eq!(evicted, 1);
        assert_eq!(store.entity_count(), 0);
    }

    #[test]
    fn test_evict_expired_keys_keeps_recent() {
        let store = StateStore::new();
        let mut engine = PipelineEngine::new();
        engine
            .register(make_stream_with_window("stream1", 3600))
            .unwrap();

        // Add entity with recent last_event_at
        {
            let mut entity = store.get_or_create_entity("recent_user");
            let stream = entity.get_or_create_stream("stream1");
            stream.last_event_at = Some(ts(99_000)); // Recent
        }

        let now = ts(100_000);
        // TTL = 2 * 3600 = 7200 seconds. Entity is 1000 seconds old -> kept.
        let evicted = evict_expired_stream_entries(&store, &engine, now, 2);
        assert_eq!(evicted, 0);
        assert_eq!(store.entity_count(), 1);
    }

    #[test]
    fn test_evict_expired_keys_keeps_no_event() {
        let store = StateStore::new();
        let mut engine = PipelineEngine::new();
        engine
            .register(make_stream_with_window("stream1", 3600))
            .unwrap();

        // Add entity with a stream but no last_event_at (never pushed)
        {
            let mut entity = store.get_or_create_entity("no_event_user");
            entity.get_or_create_stream("stream1"); // has a stream entry, so not empty
        }

        let now = ts(100_000);
        let evicted = evict_expired_stream_entries(&store, &engine, now, 2);
        assert_eq!(evicted, 0);
        assert_eq!(store.entity_count(), 1);
    }

    #[test]
    fn test_evict_expired_keys_no_streams_returns_zero() {
        let store = StateStore::new();
        let engine = PipelineEngine::new(); // No streams registered

        {
            let mut entity = store.get_or_create_entity("user");
            let stream = entity.get_or_create_stream("SomeStream");
            stream.last_event_at = Some(ts(1000));
        }

        let now = ts(100_000);
        let evicted = evict_expired_stream_entries(&store, &engine, now, 2);
        assert_eq!(evicted, 0);
        assert_eq!(store.entity_count(), 1); // Not evicted because no streams -> no max window
    }

    fn make_stream_with_ttl(
        name: &str,
        window_secs: u64,
        ttl_secs: Option<u64>,
    ) -> StreamDefinition {
        StreamDefinition {
            name: name.to_string(),
            key_field: Some("user_id".to_string()),
            group_by_keys: None,
            features: vec![(
                "count".to_string(),
                FeatureDef::Count {
                    window: Duration::from_secs(window_secs),
                    bucket: Duration::from_secs(60),
                    where_expr: None,
                    backfill: false,
                },
            )],
            depends_on: None,
            filter: None,
            entity_ttl: ttl_secs.map(Duration::from_secs),
            history_ttl: None,
            projection: None,
            ephemeral: None,
            pipeline_ttl: None,
            max_keys: None,
            watermark_lateness: None,
            shard_key: None,
        }
    }

    // ======================== Per-stream eviction tests ========================

    #[test]
    fn test_evict_expired_stream_entries_removes_expired_stream_only() {
        let store = StateStore::new();
        let mut engine = PipelineEngine::new();
        // stream_short has 300s entity_ttl, stream_long has 7200s entity_ttl
        engine
            .register(make_stream_with_ttl("stream_short", 3600, Some(300)))
            .unwrap();
        engine
            .register(make_stream_with_ttl("stream_long", 3600, Some(7200)))
            .unwrap();

        // Entity with two streams; stream_short is old, stream_long is recent
        {
            let mut entity = store.get_or_create_entity("user1");
            let short = entity.get_or_create_stream("stream_short");
            short.last_event_at = Some(ts(1000)); // Very old
            let long = entity.get_or_create_stream("stream_long");
            long.last_event_at = Some(ts(99_000)); // Recent
        }

        let now = ts(100_000);
        let evicted = evict_expired_stream_entries(&store, &engine, now, 2);
        assert_eq!(evicted, 1, "only stream_short should be evicted");

        let entity = store.get_entity("user1").unwrap();
        assert!(
            entity.streams.get("stream_short").is_none(),
            "stream_short should be removed"
        );
        assert!(
            entity.streams.get("stream_long").is_some(),
            "stream_long should remain"
        );
    }

    #[test]
    fn test_evict_all_streams_removes_entity_when_no_static_features() {
        let store = StateStore::new();
        let mut engine = PipelineEngine::new();
        engine
            .register(make_stream_with_ttl("stream1", 3600, Some(300)))
            .unwrap();

        {
            let mut entity = store.get_or_create_entity("user1");
            let stream = entity.get_or_create_stream("stream1");
            stream.last_event_at = Some(ts(1000)); // Old
        }

        let now = ts(100_000);
        let evicted = evict_expired_stream_entries(&store, &engine, now, 2);
        assert_eq!(evicted, 1);
        assert_eq!(
            store.entity_count(),
            0,
            "entity should be removed when all streams evicted and no static features"
        );
    }

    #[test]
    fn test_evict_all_streams_keeps_entity_with_static_features() {
        let store = StateStore::new();
        let mut engine = PipelineEngine::new();
        engine
            .register(make_stream_with_ttl("stream1", 3600, Some(300)))
            .unwrap();

        {
            let mut entity = store.get_or_create_entity("user1");
            let stream = entity.get_or_create_stream("stream1");
            stream.last_event_at = Some(ts(1000)); // Old
        }
        // Add a static feature
        store.set_static(
            "user1",
            "lifetime_value",
            crate::types::FeatureValue::Float(100.0),
            ts(1000),
        );

        let now = ts(100_000);
        let evicted = evict_expired_stream_entries(&store, &engine, now, 2);
        assert_eq!(evicted, 1, "stream1 should be evicted");
        assert_eq!(
            store.entity_count(),
            1,
            "entity should be kept because of static features"
        );
        let entity = store.get_entity("user1").unwrap();
        assert!(entity.streams.is_empty(), "all streams should be gone");
        assert!(
            !entity.static_features.is_empty(),
            "static features should remain"
        );
    }

    #[test]
    fn test_evict_stream_with_none_entity_ttl_falls_back_to_global() {
        let store = StateStore::new();
        let mut engine = PipelineEngine::new();
        // entity_ttl=None -> falls back to global (max_window * ttl_multiplier = 3600 * 2 = 7200)
        engine
            .register(make_stream_with_window("stream_global", 3600))
            .unwrap();

        {
            let mut entity = store.get_or_create_entity("user1");
            let stream = entity.get_or_create_stream("stream_global");
            stream.last_event_at = Some(ts(1000)); // 99000 seconds old -> exceeds 7200 TTL
        }

        let now = ts(100_000);
        let evicted = evict_expired_stream_entries(&store, &engine, now, 2);
        assert_eq!(
            evicted, 1,
            "stream with None entity_ttl should fall back to global TTL"
        );
    }

    #[test]
    fn test_evict_stream_with_none_entity_ttl_and_zero_max_window_skips() {
        let store = StateStore::new();
        let mut engine = PipelineEngine::new();
        // A derive-only stream has zero max window
        engine
            .register(StreamDefinition {
                name: "derived_only".to_string(),
                key_field: Some("user_id".to_string()),
                group_by_keys: None,
                features: vec![(
                    "ratio".to_string(),
                    FeatureDef::Derive {
                        expr: crate::engine::expression::parse_expr("1 + 1").unwrap(),
                    },
                )],
                depends_on: None,
                filter: None,
                entity_ttl: None,
                history_ttl: None,
                projection: None,
                ephemeral: None,
                pipeline_ttl: None,
                max_keys: None,
                watermark_lateness: None,
                shard_key: None,
            })
            .unwrap();

        {
            let mut entity = store.get_or_create_entity("user1");
            let stream = entity.get_or_create_stream("derived_only");
            stream.last_event_at = Some(ts(1000)); // Very old
        }

        let now = ts(100_000);
        let evicted = evict_expired_stream_entries(&store, &engine, now, 2);
        assert_eq!(
            evicted, 0,
            "should not evict when entity_ttl=None and max_window=0"
        );
    }

    #[test]
    fn test_evict_stream_with_no_last_event_at_not_evicted() {
        let store = StateStore::new();
        let mut engine = PipelineEngine::new();
        engine
            .register(make_stream_with_ttl("stream1", 3600, Some(300)))
            .unwrap();

        {
            let mut entity = store.get_or_create_entity("user1");
            entity.get_or_create_stream("stream1");
            // last_event_at is None (default)
        }

        let now = ts(100_000);
        let evicted = evict_expired_stream_entries(&store, &engine, now, 2);
        assert_eq!(
            evicted, 0,
            "stream with no last_event_at should not be evicted"
        );
    }

    #[test]
    fn test_evict_mixed_entity_ttl_and_global() {
        let store = StateStore::new();
        let mut engine = PipelineEngine::new();
        // stream_custom has 300s TTL, stream_global has None (falls back to 3600*2=7200)
        engine
            .register(make_stream_with_ttl("stream_custom", 3600, Some(300)))
            .unwrap();
        engine
            .register(make_stream_with_window("stream_global", 3600))
            .unwrap();

        {
            let mut entity = store.get_or_create_entity("user1");
            // stream_custom: old (should be evicted with 300s TTL)
            let custom = entity.get_or_create_stream("stream_custom");
            custom.last_event_at = Some(ts(99_000)); // 1000s old > 300s TTL
                                                     // stream_global: old but within global TTL
            let global = entity.get_or_create_stream("stream_global");
            global.last_event_at = Some(ts(99_000)); // 1000s old < 7200s global TTL
        }

        let now = ts(100_000);
        let evicted = evict_expired_stream_entries(&store, &engine, now, 2);
        assert_eq!(evicted, 1, "only stream_custom should be evicted");
        let entity = store.get_entity("user1").unwrap();
        assert!(entity.streams.get("stream_custom").is_none());
        assert!(entity.streams.get("stream_global").is_some());
    }

    #[test]
    fn test_evict_mixed_entities() {
        let store = StateStore::new();
        let mut engine = PipelineEngine::new();
        engine
            .register(make_stream_with_window("stream1", 3600))
            .unwrap();

        // Old entity (should be evicted -- stream entry removed, then entity removed because empty)
        {
            let mut entity = store.get_or_create_entity("old_user");
            let stream = entity.get_or_create_stream("stream1");
            stream.last_event_at = Some(ts(1000));
        }
        // Recent entity (should be kept)
        {
            let mut entity = store.get_or_create_entity("recent_user");
            let stream = entity.get_or_create_stream("stream1");
            stream.last_event_at = Some(ts(99_000));
        }
        // No event entity with a stream (should be kept -- no last_event_at means not evicted)
        {
            let mut entity = store.get_or_create_entity("no_event_user");
            entity.get_or_create_stream("stream1"); // has a stream entry, not empty
        }

        let now = ts(100_000);
        let evicted = evict_expired_stream_entries(&store, &engine, now, 2);
        assert_eq!(evicted, 1);
        assert_eq!(store.entity_count(), 2);
        assert!(store.get_entity("old_user").is_none());
        assert!(store.get_entity("recent_user").is_some());
        assert!(store.get_entity("no_event_user").is_some());
    }

    // ======================== Phase 9: mark_deleted wiring tests ========================

    #[test]
    fn test_eviction_marks_fully_removed_entity_deleted() {
        let store = StateStore::new();
        let mut engine = PipelineEngine::new();
        engine
            .register(make_stream_with_ttl("stream1", 3600, Some(300)))
            .unwrap();

        // Entity whose only stream will be evicted and has no static features
        {
            let mut entity = store.get_or_create_entity("doomed");
            let stream = entity.get_or_create_stream("stream1");
            stream.last_event_at = Some(ts(1000)); // Very old
        }

        let now = ts(100_000);
        let evicted = evict_expired_stream_entries(&store, &engine, now, 2);
        assert_eq!(evicted, 1);
        assert_eq!(store.entity_count(), 0, "entity should be fully removed");

        // take_deleted should contain "doomed"
        let deleted = store.take_deleted();
        assert_eq!(deleted, vec!["doomed".to_string()]);
    }

    #[test]
    fn test_eviction_does_not_mark_deleted_when_static_features_remain() {
        let store = StateStore::new();
        let mut engine = PipelineEngine::new();
        engine
            .register(make_stream_with_ttl("stream1", 3600, Some(300)))
            .unwrap();

        {
            let mut entity = store.get_or_create_entity("user1");
            let stream = entity.get_or_create_stream("stream1");
            stream.last_event_at = Some(ts(1000)); // Old
        }
        store.set_static(
            "user1",
            "lifetime_value",
            crate::types::FeatureValue::Float(100.0),
            ts(1000),
        );

        let now = ts(100_000);
        let evicted = evict_expired_stream_entries(&store, &engine, now, 2);
        assert_eq!(evicted, 1, "stream1 should be evicted");
        assert_eq!(
            store.entity_count(),
            1,
            "entity kept due to static features"
        );

        // take_deleted should be empty: entity still exists, not "deleted"
        let deleted = store.take_deleted();
        assert!(
            deleted.is_empty(),
            "static-only entity must NOT be marked deleted"
        );
    }

    #[test]
    fn test_eviction_does_not_mark_deleted_when_other_stream_remains() {
        let store = StateStore::new();
        let mut engine = PipelineEngine::new();
        // short TTL stream gets evicted, long TTL stream stays
        engine
            .register(make_stream_with_ttl("stream_short", 3600, Some(300)))
            .unwrap();
        engine
            .register(make_stream_with_ttl("stream_long", 3600, Some(7200)))
            .unwrap();

        {
            let mut entity = store.get_or_create_entity("user1");
            let short = entity.get_or_create_stream("stream_short");
            short.last_event_at = Some(ts(1000)); // Old
            let long = entity.get_or_create_stream("stream_long");
            long.last_event_at = Some(ts(99_000)); // Recent
        }

        let now = ts(100_000);
        let evicted = evict_expired_stream_entries(&store, &engine, now, 2);
        assert_eq!(evicted, 1);
        assert_eq!(
            store.entity_count(),
            1,
            "entity kept because stream_long remains"
        );

        let deleted = store.take_deleted();
        assert!(
            deleted.is_empty(),
            "entity with remaining stream must NOT be marked deleted"
        );
    }
}
