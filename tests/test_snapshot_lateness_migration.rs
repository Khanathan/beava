// CORR-04: backward-compatible snapshot migration for watermark_lateness.
// Phase 46 Wave 3 (D-12): verifies that an older SourceDescriptor JSON
// without the watermark_lateness field deserializes cleanly and resolves
// to the 5s default via lateness_for().

use beava::engine::event_time::WATERMARK_LATENESS;
use beava::engine::pipeline::{PipelineEngine, StreamDefinition};
use beava::engine::register::{v0_source_to_stream_def, SourceDescriptor};

#[test]
fn old_snapshot_loads_with_default_lateness() {
    // Craft a legacy register payload that does NOT contain watermark_lateness.
    // This simulates an older snapshot taken before this field was introduced.
    let legacy_json = r#"{
        "name": "Txns",
        "kind": "stream",
        "key_field": "user_id",
        "fields": {},
        "history_ttl": "90d"
    }"#;

    // Deserialize — must succeed (no error on missing field).
    let desc: SourceDescriptor = serde_json::from_str(legacy_json)
        .expect("legacy JSON without watermark_lateness must deserialize without error");

    // watermark_lateness field must be None (serde default).
    assert!(
        desc.watermark_lateness.is_none(),
        "SourceDescriptor.watermark_lateness must default to None on absent field"
    );

    // v0_source_to_stream_def must succeed and produce a StreamDefinition with None.
    let stream_def: StreamDefinition = v0_source_to_stream_def(&desc)
        .expect("v0_source_to_stream_def must succeed on legacy payload");

    assert!(
        stream_def.watermark_lateness.is_none(),
        "StreamDefinition.watermark_lateness must be None when absent from payload"
    );

    // Register into an engine — lateness_for must return the 5s default.
    let mut engine = PipelineEngine::new();
    let stream_name = stream_def.name.clone();
    engine.register(stream_def).unwrap();

    assert_eq!(
        engine.watermarks.lateness_for(&stream_name),
        WATERMARK_LATENESS,
        "watermark lateness must fall back to 5s constant for legacy streams"
    );
}

#[test]
fn new_payload_with_watermark_lateness_parses_correctly() {
    // Verify the forward path too: a new payload WITH watermark_lateness parses
    // into a non-None StreamDefinition field.
    let new_json = r#"{
        "name": "Txns",
        "kind": "stream",
        "key_field": "user_id",
        "fields": {},
        "watermark_lateness": "10m"
    }"#;

    let desc: SourceDescriptor =
        serde_json::from_str(new_json).expect("new JSON with watermark_lateness must deserialize");

    assert_eq!(
        desc.watermark_lateness.as_deref(),
        Some("10m"),
        "SourceDescriptor.watermark_lateness must carry the string from JSON"
    );

    let stream_def: StreamDefinition = v0_source_to_stream_def(&desc)
        .expect("v0_source_to_stream_def must succeed with watermark_lateness=10m");

    use std::time::Duration;
    assert_eq!(
        stream_def.watermark_lateness,
        Some(Duration::from_secs(600)),
        "StreamDefinition.watermark_lateness must be 600s when payload has '10m'"
    );
}
