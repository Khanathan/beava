//! TTL-based key eviction.
//!
//! Keys with no events for 2x the largest window are evicted from memory.
//! Evicted keys re-initialize fresh on next event (CLAUDE.md spec).

use std::time::SystemTime;
use crate::state::store::StateStore;
use crate::engine::pipeline::PipelineEngine;

/// Evict entity keys whose last_event_at is older than ttl_multiplier * max_window.
/// Returns the number of evicted keys.
///
/// If no streams are registered (max_window == 0), nothing is evicted.
/// Uses Duration arithmetic with unwrap_or(Duration::ZERO) for clock skew safety (T-04-04).
pub fn evict_expired_keys(
    store: &mut StateStore,
    engine: &PipelineEngine,
    now: SystemTime,
    ttl_multiplier: u32,
) -> usize {
    let max_window = engine.max_window_duration();
    if max_window.is_zero() {
        return 0; // No streams registered -- nothing to evict
    }
    let ttl = max_window * ttl_multiplier;
    store.remove_expired_entities(now, ttl)
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
            key_field: "user_id".to_string(),
            features: vec![
                ("count".to_string(), FeatureDef::Count {
                    window: Duration::from_secs(window_secs),
                    bucket: Duration::from_secs(60),
                    where_expr: None,
                }),
            ],
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
            entity.update_last_event(ts(1000)); // Very old
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
            entity.update_last_event(ts(99_000)); // Recent
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

        // Add entity with no last_event_at (never pushed)
        store.get_or_create_entity("no_event_user");

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
            entity.update_last_event(ts(1000));
        }

        let now = ts(100_000);
        let evicted = evict_expired_keys(&mut store, &engine, now, 2);
        assert_eq!(evicted, 0);
        assert_eq!(store.entity_count(), 1); // Not evicted because no streams -> no max window
    }

    #[test]
    fn test_evict_mixed_entities() {
        let mut store = StateStore::new();
        let mut engine = PipelineEngine::new();
        engine.register(make_stream_with_window("stream1", 3600)).unwrap();

        // Old entity (should be evicted)
        {
            let entity = store.get_or_create_entity("old_user");
            entity.update_last_event(ts(1000));
        }
        // Recent entity (should be kept)
        {
            let entity = store.get_or_create_entity("recent_user");
            entity.update_last_event(ts(99_000));
        }
        // No event entity (should be kept)
        store.get_or_create_entity("no_event_user");

        let now = ts(100_000);
        let evicted = evict_expired_keys(&mut store, &engine, now, 2);
        assert_eq!(evicted, 1);
        assert_eq!(store.entity_count(), 2);
        assert!(store.get_entity("old_user").is_none());
        assert!(store.get_entity("recent_user").is_some());
        assert!(store.get_entity("no_event_user").is_some());
    }
}
