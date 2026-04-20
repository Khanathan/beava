// CORR-07: eviction clock sources from WatermarkTracker::observed_max(), not
// SystemTime::now().
//
// Verifies that historical (30-day-old) events with a 7-day entity_ttl are NOT
// evicted immediately: the eviction scan clock tracks the per-stream watermark
// observed_max, so age = watermark - last_event_at = 0 when the only events are
// 30 days old and the watermark is also 30 days old.  After the watermark
// advances to "now", the entity's age exceeds the 7-day TTL and is evicted.
//
// Note: push_with_cascade_no_features applies operator state but does NOT
// internally advance the watermark (that's the TCP layer's job at tcp.rs:1750).
// We call engine.wm_observe() explicitly here to mirror what the TCP
// live-ingest path does.

use beava::engine::pipeline::{FeatureDef, PipelineEngine, StreamDefinition};
use beava::state::eviction::evict_expired_stream_entries;
use beava::state::store::StateStore;
use serde_json::json;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

fn millis_since_epoch(t: SystemTime) -> u64 {
    t.duration_since(UNIX_EPOCH).unwrap().as_millis() as u64
}

fn make_ttl_stream(name: &str, entity_ttl_secs: u64) -> StreamDefinition {
    StreamDefinition {
        name: name.to_string(),
        key_field: Some("user_id".to_string()),
        group_by_keys: None,
        features: vec![(
            "count_1h".to_string(),
            FeatureDef::Count {
                window: Duration::from_secs(3600),
                bucket: Duration::from_secs(60),
                where_expr: None,
                backfill: false,
            },
        )],
        depends_on: None,
        filter: None,
        entity_ttl: Some(Duration::from_secs(entity_ttl_secs)),
        history_ttl: None,
        projection: None,
        ephemeral: None,
        pipeline_ttl: None,
        max_keys: None,
        watermark_lateness: None,
        shard_key: None,    }
}

#[ignore = "54-03 Task 4: legacy StateStore API / engine.push(&store, ...); Wave 4 re-enables after legacy-engine removal"]
#[test]
fn ttl_honors_event_time_not_wall_clock() {
    let store = StateStore::new();
    let mut engine = PipelineEngine::new();
    // Register stream with 7-day entity TTL.
    engine
        .register(make_ttl_stream("Txns", 7 * 24 * 3600))
        .unwrap();

    let now = SystemTime::now();
    let thirty_days_ago = now - Duration::from_secs(30 * 24 * 3600);

    // Push a single event with a 30-day-old event_time (mirrors live-ingest).
    let event = json!({
        "user_id": "u1",
        "_event_time": millis_since_epoch(thirty_days_ago)
    });
    engine
        .push_with_cascade_no_features("Txns", &event, &store, thirty_days_ago)
        .unwrap();
    // Mirror the TCP live-ingest path: advance watermark for this stream.
    // (push_with_cascade_no_features applies operator state but does not
    // call watermarks.observe — the TCP dispatcher does that at tcp.rs:1750.)
    engine.wm_observe("Txns", thirty_days_ago);

    // Watermark observed_max = thirty_days_ago; last_event_at = thirty_days_ago.
    // scan_clock = thirty_days_ago; age = scan_clock - last_event_at = 0 < 7d TTL.
    // Entity must NOT be evicted.
    let evicted_before = evict_expired_stream_entries(&store, &engine, now, 2);
    assert_eq!(
        evicted_before, 0,
        "CORR-07: 30d-old event must NOT evict under 7d TTL when using event-time clock \
         (watermark = last event = 0 age)"
    );
    assert_eq!(
        store.entity_count(),
        1,
        "entity u1 must still exist after first eviction pass"
    );

    // Advance watermark to now by observing a fresh event_time.
    // After this: scan_clock = now, age = now - thirty_days_ago = 30d > 7d TTL.
    engine.wm_observe("Txns", now);

    // Second eviction pass: watermark observed_max = now; age = 30d > 7d TTL.
    // Entity MUST be evicted.
    let evicted_after = evict_expired_stream_entries(&store, &engine, now, 2);
    assert_eq!(
        evicted_after, 1,
        "CORR-07: after watermark advances to now, 30d-old entity must be evicted \
         (age 30d > TTL 7d)"
    );
    assert_eq!(
        store.entity_count(),
        0,
        "entity u1 must be removed after second eviction pass"
    );
}
