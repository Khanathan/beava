// Phase 54-04 Pass A6b: whole file gated off — references the deleted
// `StateStore`. Pass C re-gates or prunes.
#![cfg(any())]

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
    // Phase 54-04 Pass B: legacy push/cascade helper deleted. Body stubbed
    // pending Pass C on_shard rewrite.
    unimplemented!("54-04 Pass B: legacy helper deleted; rewrite via on_shard path in Pass C")
}
