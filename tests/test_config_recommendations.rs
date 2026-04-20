//! Phase 25-02 Task 2: configuration recommendation engine.
//!
//! Covers:
//! - Empty engine → no recommendations.
//! - TTL-too-short signal → doubled-TTL recommendation.
//! - history_ttl < max downstream Table ttl → history_ttl recommendation.
//! - Clean signals → empty recommendations.
//! - Schema correctness (copy_paste / knob / confidence).

use beava::engine::pipeline::PipelineEngine;
use beava::engine::recommend::{humanize_duration_secs, recommend_config};
use beava::engine::register::{v0_source_to_stream_def, SourceDescriptor};
use beava::state::eviction_tracker::EvictionTracker;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

/// Phase 54-03 Task 2: `EvictionTracker.{evictions, reinits}` migrated from
/// `DashMap<_, AtomicU64>` to `RwLock<AHashMap<_, Arc<AtomicU64>>>`. These
/// helpers replicate the old `entry().or_default().store()` flow.
fn set_evictions(tracker: &EvictionTracker, table: &str, n: u64) {
    let mut w = tracker.evictions.write();
    w.entry(table.to_string())
        .or_insert_with(|| Arc::new(AtomicU64::new(0)))
        .store(n, Ordering::Relaxed);
}

fn set_reinits(tracker: &EvictionTracker, table: &str, n: u64) {
    let mut w = tracker.reinits.write();
    w.entry(table.to_string())
        .or_insert_with(|| Arc::new(AtomicU64::new(0)))
        .store(n, Ordering::Relaxed);
}

fn table(name: &str, ttl: Option<&str>) -> SourceDescriptor {
    SourceDescriptor {
        name: name.to_string(),
        kind: "table".to_string(),
        key_field: Some("user_id".to_string()),
        key_fields: None,
        mode: Some("append".to_string()),
        fields: serde_json::json!({"user_id": {"type": "str", "optional": false}}),
        history_ttl: None,
        entity_ttl: ttl.map(|s| s.to_string()),
        watermark_lateness: None,
        shard_key: None,
    }
}

fn stream(name: &str, history_ttl: Option<&str>) -> SourceDescriptor {
    SourceDescriptor {
        name: name.to_string(),
        kind: "stream".to_string(),
        key_field: None,
        key_fields: None,
        mode: None,
        fields: serde_json::json!({"user_id": {"type": "str", "optional": false}}),
        history_ttl: history_ttl.map(|s| s.to_string()),
        entity_ttl: None,
        watermark_lateness: None,
        shard_key: None,
    }
}

fn register_raw_kind(engine: &mut PipelineEngine, desc: &SourceDescriptor) {
    let def = v0_source_to_stream_def(desc).unwrap();
    let name = def.name.clone();
    engine.register(def).unwrap();
    // Store raw JSON carrying kind so has_registered_table returns true.
    let j = serde_json::to_value(desc).unwrap();
    engine.store_raw_register_json(&name, j);
}

#[test]
fn empty_engine_yields_no_recommendations() {
    let engine = PipelineEngine::new();
    let tracker = EvictionTracker::new();
    assert!(recommend_config(&engine, &tracker).is_empty());
}

#[test]
fn ttl_too_short_triggers_recommendation() {
    let mut engine = PipelineEngine::new();
    register_raw_kind(&mut engine, &table("UserStats", Some("30d")));
    let tracker = EvictionTracker::new();
    // Simulate 1000 evictions with 100 reinits → 10% reinit rate.
    for i in 0..1000 {
        tracker.record_eviction("UserStats", &format!("u{}", i));
    }
    // Bump the counters directly to sidestep bloom FP noise.
    set_evictions(&tracker, "UserStats", 1000);
    set_reinits(&tracker, "UserStats", 100);

    let recs = recommend_config(&engine, &tracker);
    assert_eq!(recs.len(), 1, "expected exactly one recommendation");
    let r = &recs[0];
    assert_eq!(r.knob, "UserStats.ttl");
    assert_eq!(r.current, "30d");
    assert_eq!(r.suggested, "60d");
    assert!(r.confidence > 0.0 && r.confidence <= 1.0);
    assert!(r.copy_paste.contains("ttl=\"60d\""));
    // Evidence carries the raw counters.
    assert_eq!(
        r.evidence.get("evictions").and_then(|v| v.as_u64()),
        Some(1000)
    );
    assert_eq!(
        r.evidence.get("reinits").and_then(|v| v.as_u64()),
        Some(100)
    );
}

#[test]
fn history_ttl_lt_downstream_table_ttl_triggers() {
    let mut engine = PipelineEngine::new();
    // Stream with history_ttl = 30d
    register_raw_kind(&mut engine, &stream("Clicks", Some("30d")));
    // Table with ttl = 60d, depends_on Clicks
    let table_desc = table("UserSummary", Some("60d"));
    // Reuse the same Source shape but wire depends_on by manually calling the
    // converter. We need depends_on on the StreamDefinition, which the Source
    // converter leaves as None. Instead: register the table, then patch its
    // depends_on via registering a fresh StreamDefinition.
    let mut def = v0_source_to_stream_def(&table_desc).unwrap();
    def.depends_on = Some(vec!["Clicks".to_string()]);
    let name = def.name.clone();
    engine.register(def).unwrap();
    engine.store_raw_register_json(&name, serde_json::json!({"kind": "table", "name": name}));
    let _ = table_desc; // silence unused

    let tracker = EvictionTracker::new();
    let recs = recommend_config(&engine, &tracker);
    assert!(
        recs.iter().any(|r| r.knob == "Clicks.history_ttl"),
        "expected a Clicks.history_ttl recommendation; got {:?}",
        recs.iter().map(|r| &r.knob).collect::<Vec<_>>()
    );
    let r = recs
        .iter()
        .find(|r| r.knob == "Clicks.history_ttl")
        .unwrap();
    assert_eq!(r.current, "30d");
    assert_eq!(r.suggested, "60d");
    assert!(r.copy_paste.contains("history_ttl=\"60d\""));
}

#[test]
fn clean_signals_yield_empty_recommendations() {
    let mut engine = PipelineEngine::new();
    register_raw_kind(&mut engine, &table("UserStats", Some("30d")));
    let tracker = EvictionTracker::new();
    // 1000 evictions, only 5 reinits → 0.5% reinit rate (below 5% threshold)
    set_evictions(&tracker, "UserStats", 1000);
    set_reinits(&tracker, "UserStats", 5);
    let recs = recommend_config(&engine, &tracker);
    assert!(recs.is_empty(), "expected empty recs, got {:?}", recs);
}

#[test]
fn insufficient_sample_yields_no_recommendation() {
    let mut engine = PipelineEngine::new();
    register_raw_kind(&mut engine, &table("UserStats", Some("30d")));
    let tracker = EvictionTracker::new();
    // 10 evictions with 5 reinits → 50% rate, but sample is too small.
    set_evictions(&tracker, "UserStats", 10);
    set_reinits(&tracker, "UserStats", 5);
    let recs = recommend_config(&engine, &tracker);
    assert!(
        recs.is_empty(),
        "sample < MIN_EVICTIONS_FOR_SIGNAL should suppress the recommendation"
    );
}

#[test]
fn humanize_roundtrip() {
    assert_eq!(humanize_duration_secs(30 * 86400), "30d");
    assert_eq!(humanize_duration_secs(60 * 86400), "60d");
    assert_eq!(humanize_duration_secs(90 * 86400), "90d");
    assert_eq!(humanize_duration_secs(3600), "1h");
}

#[test]
fn recommendation_schema_shape() {
    let mut engine = PipelineEngine::new();
    register_raw_kind(&mut engine, &table("UserStats", Some("30d")));
    let tracker = EvictionTracker::new();
    set_evictions(&tracker, "UserStats", 1000);
    set_reinits(&tracker, "UserStats", 100);
    let recs = recommend_config(&engine, &tracker);
    assert_eq!(recs.len(), 1);
    let v = serde_json::to_value(&recs[0]).unwrap();
    for k in [
        "knob",
        "current",
        "suggested",
        "confidence",
        "reason",
        "evidence",
        "copy_paste",
    ] {
        assert!(v.get(k).is_some(), "missing field: {}", k);
    }
}

#[test]
fn suppress_silence_table_stream_both_healthy() {
    // Duration import used? keep for future additions
    let _ = Duration::from_secs(0);
    let mut engine = PipelineEngine::new();
    register_raw_kind(&mut engine, &stream("Events", Some("90d")));
    register_raw_kind(&mut engine, &table("Users", Some("30d")));
    let tracker = EvictionTracker::new();
    let recs = recommend_config(&engine, &tracker);
    assert!(recs.is_empty());
}
