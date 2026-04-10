//! TTL-based key eviction.
//!
//! Keys with no events for 2x the largest window are evicted from memory.
//! Evicted keys re-initialize fresh on next event (CLAUDE.md spec).

use std::time::SystemTime;
use crate::state::store::StateStore;
use crate::engine::pipeline::PipelineEngine;

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
pub fn evict_expired_stream_entries(
    store: &mut StateStore,
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
    // can separately call mark_deleted (which needs &mut store) before mutating
    // the entity streams. Each plan entry is (key, streams_to_remove, will_be_empty).
    let entity_keys: Vec<String> = store.entity_keys().collect();
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

                // Check if stream entry is expired
                let age = now.duration_since(last_event).unwrap_or(std::time::Duration::ZERO);
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
        if let Some(entity) = store.get_entity_mut(key) {
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

/// Legacy wrapper: Evict entity keys whose last_event_at is older than ttl_multiplier * max_window.
/// Delegates to evict_expired_stream_entries for per-stream eviction behavior.
/// Returns the number of stream entries evicted.
pub fn evict_expired_keys(
    store: &mut StateStore,
    engine: &PipelineEngine,
    now: SystemTime,
    ttl_multiplier: u32,
) -> usize {
    evict_expired_stream_entries(store, engine, now, ttl_multiplier)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, UNIX_EPOCH};
    use crate::engine::pipeline::{StreamDefinition, FeatureDef};

    fn ts(secs: u64) -> SystemTime {
        UNIX_EPOCH + Duration::from_secs(secs)
    }

    fn make_stream_with_window(name: &str, window_secs: u64) -> StreamDefinition {
        StreamDefinition {
            name: name.to_string(),
            key_field: Some("user_id".to_string()),
            features: vec![
                ("count".to_string(), FeatureDef::Count {
                    window: Duration::from_secs(window_secs),
                    bucket: Duration::from_secs(60),
                    where_expr: None,
                    backfill: false,
                }),
            ],
            depends_on: None,
            filter: None,
            entity_ttl: None,
            history_ttl: None,
        }
    }

    #[test]
    fn test_evict_expired_keys_removes_old() {
        let mut store = StateStore::new();
        let mut engine = PipelineEngine::new();
        engine.register(make_stream_with_window("stream1", 3600)).unwrap(); // 1h window

        // Add entity with old last_event_at
        {
            let entity = store.get_or_create_entity("old_user");
            let stream = entity.get_or_create_stream("stream1");
            stream.last_event_at = Some(ts(1000)); // Very old
        }

        let now = ts(100_000);
        // TTL = 2 * 3600 = 7200 seconds. Entity is 99000 seconds old -> evicted.
        let evicted = evict_expired_keys(&mut store, &engine, now, 2);
        assert_eq!(evicted, 1);
        assert_eq!(store.entity_count(), 0);
    }

    #[test]
    fn test_evict_expired_keys_keeps_recent() {
        let mut store = StateStore::new();
        let mut engine = PipelineEngine::new();
        engine.register(make_stream_with_window("stream1", 3600)).unwrap();

        // Add entity with recent last_event_at
        {
            let entity = store.get_or_create_entity("recent_user");
            let stream = entity.get_or_create_stream("stream1");
            stream.last_event_at = Some(ts(99_000)); // Recent
        }

        let now = ts(100_000);
        // TTL = 2 * 3600 = 7200 seconds. Entity is 1000 seconds old -> kept.
        let evicted = evict_expired_keys(&mut store, &engine, now, 2);
        assert_eq!(evicted, 0);
        assert_eq!(store.entity_count(), 1);
    }

    #[test]
    fn test_evict_expired_keys_keeps_no_event() {
        let mut store = StateStore::new();
        let mut engine = PipelineEngine::new();
        engine.register(make_stream_with_window("stream1", 3600)).unwrap();

        // Add entity with a stream but no last_event_at (never pushed)
        {
            let entity = store.get_or_create_entity("no_event_user");
            entity.get_or_create_stream("stream1"); // has a stream entry, so not empty
        }

        let now = ts(100_000);
        let evicted = evict_expired_keys(&mut store, &engine, now, 2);
        assert_eq!(evicted, 0);
        assert_eq!(store.entity_count(), 1);
    }

    #[test]
    fn test_evict_expired_keys_no_streams_returns_zero() {
        let mut store = StateStore::new();
        let engine = PipelineEngine::new(); // No streams registered

        {
            let entity = store.get_or_create_entity("user");
            let stream = entity.get_or_create_stream("SomeStream");
            stream.last_event_at = Some(ts(1000));
        }

        let now = ts(100_000);
        let evicted = evict_expired_keys(&mut store, &engine, now, 2);
        assert_eq!(evicted, 0);
        assert_eq!(store.entity_count(), 1); // Not evicted because no streams -> no max window
    }

    fn make_stream_with_ttl(name: &str, window_secs: u64, ttl_secs: Option<u64>) -> StreamDefinition {
        StreamDefinition {
            name: name.to_string(),
            key_field: Some("user_id".to_string()),
            features: vec![
                ("count".to_string(), FeatureDef::Count {
                    window: Duration::from_secs(window_secs),
                    bucket: Duration::from_secs(60),
                    where_expr: None,
                    backfill: false,
                }),
            ],
            depends_on: None,
            filter: None,
            entity_ttl: ttl_secs.map(|s| Duration::from_secs(s)),
            history_ttl: None,
        }
    }

    // ======================== Per-stream eviction tests ========================

    #[test]
    fn test_evict_expired_stream_entries_removes_expired_stream_only() {
        let mut store = StateStore::new();
        let mut engine = PipelineEngine::new();
        // stream_short has 300s entity_ttl, stream_long has 7200s entity_ttl
        engine.register(make_stream_with_ttl("stream_short", 3600, Some(300))).unwrap();
        engine.register(make_stream_with_ttl("stream_long", 3600, Some(7200))).unwrap();

        // Entity with two streams; stream_short is old, stream_long is recent
        {
            let entity = store.get_or_create_entity("user1");
            let short = entity.get_or_create_stream("stream_short");
            short.last_event_at = Some(ts(1000)); // Very old
            let long = entity.get_or_create_stream("stream_long");
            long.last_event_at = Some(ts(99_000)); // Recent
        }

        let now = ts(100_000);
        let evicted = evict_expired_stream_entries(&mut store, &engine, now, 2);
        assert_eq!(evicted, 1, "only stream_short should be evicted");

        let entity = store.get_entity("user1").unwrap();
        assert!(entity.streams.get("stream_short").is_none(), "stream_short should be removed");
        assert!(entity.streams.get("stream_long").is_some(), "stream_long should remain");
    }

    #[test]
    fn test_evict_all_streams_removes_entity_when_no_static_features() {
        let mut store = StateStore::new();
        let mut engine = PipelineEngine::new();
        engine.register(make_stream_with_ttl("stream1", 3600, Some(300))).unwrap();

        {
            let entity = store.get_or_create_entity("user1");
            let stream = entity.get_or_create_stream("stream1");
            stream.last_event_at = Some(ts(1000)); // Old
        }

        let now = ts(100_000);
        let evicted = evict_expired_stream_entries(&mut store, &engine, now, 2);
        assert_eq!(evicted, 1);
        assert_eq!(store.entity_count(), 0, "entity should be removed when all streams evicted and no static features");
    }

    #[test]
    fn test_evict_all_streams_keeps_entity_with_static_features() {
        let mut store = StateStore::new();
        let mut engine = PipelineEngine::new();
        engine.register(make_stream_with_ttl("stream1", 3600, Some(300))).unwrap();

        {
            let entity = store.get_or_create_entity("user1");
            let stream = entity.get_or_create_stream("stream1");
            stream.last_event_at = Some(ts(1000)); // Old
        }
        // Add a static feature
        store.set_static("user1", "lifetime_value", crate::types::FeatureValue::Float(100.0), ts(1000));

        let now = ts(100_000);
        let evicted = evict_expired_stream_entries(&mut store, &engine, now, 2);
        assert_eq!(evicted, 1, "stream1 should be evicted");
        assert_eq!(store.entity_count(), 1, "entity should be kept because of static features");
        let entity = store.get_entity("user1").unwrap();
        assert!(entity.streams.is_empty(), "all streams should be gone");
        assert!(!entity.static_features.is_empty(), "static features should remain");
    }

    #[test]
    fn test_evict_stream_with_none_entity_ttl_falls_back_to_global() {
        let mut store = StateStore::new();
        let mut engine = PipelineEngine::new();
        // entity_ttl=None -> falls back to global (max_window * ttl_multiplier = 3600 * 2 = 7200)
        engine.register(make_stream_with_window("stream_global", 3600)).unwrap();

        {
            let entity = store.get_or_create_entity("user1");
            let stream = entity.get_or_create_stream("stream_global");
            stream.last_event_at = Some(ts(1000)); // 99000 seconds old -> exceeds 7200 TTL
        }

        let now = ts(100_000);
        let evicted = evict_expired_stream_entries(&mut store, &engine, now, 2);
        assert_eq!(evicted, 1, "stream with None entity_ttl should fall back to global TTL");
    }

    #[test]
    fn test_evict_stream_with_none_entity_ttl_and_zero_max_window_skips() {
        let mut store = StateStore::new();
        let mut engine = PipelineEngine::new();
        // A derive-only stream has zero max window
        engine.register(StreamDefinition {
            name: "derived_only".to_string(),
            key_field: Some("user_id".to_string()),
            features: vec![
                ("ratio".to_string(), FeatureDef::Derive {
                    expr: crate::engine::expression::parse_expr("1 + 1").unwrap(),
                }),
            ],
            depends_on: None,
            filter: None,
            entity_ttl: None,
            history_ttl: None,
        }).unwrap();

        {
            let entity = store.get_or_create_entity("user1");
            let stream = entity.get_or_create_stream("derived_only");
            stream.last_event_at = Some(ts(1000)); // Very old
        }

        let now = ts(100_000);
        let evicted = evict_expired_stream_entries(&mut store, &engine, now, 2);
        assert_eq!(evicted, 0, "should not evict when entity_ttl=None and max_window=0");
    }

    #[test]
    fn test_evict_stream_with_no_last_event_at_not_evicted() {
        let mut store = StateStore::new();
        let mut engine = PipelineEngine::new();
        engine.register(make_stream_with_ttl("stream1", 3600, Some(300))).unwrap();

        {
            let entity = store.get_or_create_entity("user1");
            entity.get_or_create_stream("stream1");
            // last_event_at is None (default)
        }

        let now = ts(100_000);
        let evicted = evict_expired_stream_entries(&mut store, &engine, now, 2);
        assert_eq!(evicted, 0, "stream with no last_event_at should not be evicted");
    }

    #[test]
    fn test_evict_mixed_entity_ttl_and_global() {
        let mut store = StateStore::new();
        let mut engine = PipelineEngine::new();
        // stream_custom has 300s TTL, stream_global has None (falls back to 3600*2=7200)
        engine.register(make_stream_with_ttl("stream_custom", 3600, Some(300))).unwrap();
        engine.register(make_stream_with_window("stream_global", 3600)).unwrap();

        {
            let entity = store.get_or_create_entity("user1");
            // stream_custom: old (should be evicted with 300s TTL)
            let custom = entity.get_or_create_stream("stream_custom");
            custom.last_event_at = Some(ts(99_000)); // 1000s old > 300s TTL
            // stream_global: old but within global TTL
            let global = entity.get_or_create_stream("stream_global");
            global.last_event_at = Some(ts(99_000)); // 1000s old < 7200s global TTL
        }

        let now = ts(100_000);
        let evicted = evict_expired_stream_entries(&mut store, &engine, now, 2);
        assert_eq!(evicted, 1, "only stream_custom should be evicted");
        let entity = store.get_entity("user1").unwrap();
        assert!(entity.streams.get("stream_custom").is_none());
        assert!(entity.streams.get("stream_global").is_some());
    }

    #[test]
    fn test_evict_mixed_entities() {
        let mut store = StateStore::new();
        let mut engine = PipelineEngine::new();
        engine.register(make_stream_with_window("stream1", 3600)).unwrap();

        // Old entity (should be evicted -- stream entry removed, then entity removed because empty)
        {
            let entity = store.get_or_create_entity("old_user");
            let stream = entity.get_or_create_stream("stream1");
            stream.last_event_at = Some(ts(1000));
        }
        // Recent entity (should be kept)
        {
            let entity = store.get_or_create_entity("recent_user");
            let stream = entity.get_or_create_stream("stream1");
            stream.last_event_at = Some(ts(99_000));
        }
        // No event entity with a stream (should be kept -- no last_event_at means not evicted)
        {
            let entity = store.get_or_create_entity("no_event_user");
            entity.get_or_create_stream("stream1"); // has a stream entry, not empty
        }

        let now = ts(100_000);
        let evicted = evict_expired_keys(&mut store, &engine, now, 2);
        assert_eq!(evicted, 1);
        assert_eq!(store.entity_count(), 2);
        assert!(store.get_entity("old_user").is_none());
        assert!(store.get_entity("recent_user").is_some());
        assert!(store.get_entity("no_event_user").is_some());
    }

    // ======================== Phase 9: mark_deleted wiring tests ========================

    #[test]
    fn test_eviction_marks_fully_removed_entity_deleted() {
        let mut store = StateStore::new();
        let mut engine = PipelineEngine::new();
        engine.register(make_stream_with_ttl("stream1", 3600, Some(300))).unwrap();

        // Entity whose only stream will be evicted and has no static features
        {
            let entity = store.get_or_create_entity("doomed");
            let stream = entity.get_or_create_stream("stream1");
            stream.last_event_at = Some(ts(1000)); // Very old
        }

        let now = ts(100_000);
        let evicted = evict_expired_stream_entries(&mut store, &engine, now, 2);
        assert_eq!(evicted, 1);
        assert_eq!(store.entity_count(), 0, "entity should be fully removed");

        // take_deleted should contain "doomed"
        let deleted = store.take_deleted();
        assert_eq!(deleted, vec!["doomed".to_string()]);
    }

    #[test]
    fn test_eviction_does_not_mark_deleted_when_static_features_remain() {
        let mut store = StateStore::new();
        let mut engine = PipelineEngine::new();
        engine.register(make_stream_with_ttl("stream1", 3600, Some(300))).unwrap();

        {
            let entity = store.get_or_create_entity("user1");
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
        let evicted = evict_expired_stream_entries(&mut store, &engine, now, 2);
        assert_eq!(evicted, 1, "stream1 should be evicted");
        assert_eq!(store.entity_count(), 1, "entity kept due to static features");

        // take_deleted should be empty: entity still exists, not "deleted"
        let deleted = store.take_deleted();
        assert!(deleted.is_empty(), "static-only entity must NOT be marked deleted");
    }

    #[test]
    fn test_eviction_does_not_mark_deleted_when_other_stream_remains() {
        let mut store = StateStore::new();
        let mut engine = PipelineEngine::new();
        // short TTL stream gets evicted, long TTL stream stays
        engine.register(make_stream_with_ttl("stream_short", 3600, Some(300))).unwrap();
        engine.register(make_stream_with_ttl("stream_long", 3600, Some(7200))).unwrap();

        {
            let entity = store.get_or_create_entity("user1");
            let short = entity.get_or_create_stream("stream_short");
            short.last_event_at = Some(ts(1000)); // Old
            let long = entity.get_or_create_stream("stream_long");
            long.last_event_at = Some(ts(99_000)); // Recent
        }

        let now = ts(100_000);
        let evicted = evict_expired_stream_entries(&mut store, &engine, now, 2);
        assert_eq!(evicted, 1);
        assert_eq!(store.entity_count(), 1, "entity kept because stream_long remains");

        let deleted = store.take_deleted();
        assert!(deleted.is_empty(), "entity with remaining stream must NOT be marked deleted");
    }
}
