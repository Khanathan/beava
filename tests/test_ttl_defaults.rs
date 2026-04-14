//! Phase 25-02 Task 1: v0 TTL defaults end-to-end.
//!
//! Verifies that REGISTER JSON without an explicit `ttl` / `history_ttl`
//! receives the 30d / 90d defaults at engine-level, that explicit overrides
//! flow through unchanged, and that the `"forever"` / `"0"` sentinels
//! behave as locked by v0-restructure-spec §7.2.

use std::time::Duration;
use tally::engine::pipeline::PipelineEngine;
use tally::engine::register::{v0_source_to_stream_def, SourceDescriptor};
use tally::server::protocol::{is_forever_ttl, parse_duration_str, FOREVER_TTL};

fn table_source(name: &str, entity_ttl: Option<&str>) -> SourceDescriptor {
    SourceDescriptor {
        name: name.to_string(),
        kind: "table".to_string(),
        key_field: Some("user_id".to_string()),
        key_fields: None,
        mode: Some("append".to_string()),
        fields: serde_json::json!({"user_id": {"type": "str", "optional": false}}),
        history_ttl: None,
        entity_ttl: entity_ttl.map(|s| s.to_string()),
    }
}

fn stream_source(name: &str, history_ttl: Option<&str>) -> SourceDescriptor {
    SourceDescriptor {
        name: name.to_string(),
        kind: "stream".to_string(),
        key_field: None,
        key_fields: None,
        mode: None,
        fields: serde_json::json!({"user_id": {"type": "str", "optional": false}}),
        history_ttl: history_ttl.map(|s| s.to_string()),
        entity_ttl: None,
    }
}

#[test]
fn register_table_without_ttl_defaults_to_30d() {
    let desc = table_source("Users", None);
    let def = v0_source_to_stream_def(&desc).unwrap();
    assert_eq!(def.entity_ttl, Some(Duration::from_secs(30 * 86400)));
}

#[test]
fn register_stream_without_history_ttl_defaults_to_90d() {
    let desc = stream_source("Clicks", None);
    let def = v0_source_to_stream_def(&desc).unwrap();
    assert_eq!(def.history_ttl, Some(Duration::from_secs(90 * 86400)));
}

#[test]
fn register_table_with_explicit_ttl_respected() {
    let desc = table_source("Users", Some("180d"));
    let def = v0_source_to_stream_def(&desc).unwrap();
    assert_eq!(def.entity_ttl, Some(Duration::from_secs(180 * 86400)));
}

#[test]
fn register_stream_with_explicit_history_ttl_respected() {
    let desc = stream_source("Clicks", Some("30d"));
    let def = v0_source_to_stream_def(&desc).unwrap();
    assert_eq!(def.history_ttl, Some(Duration::from_secs(30 * 86400)));
}

#[test]
fn register_table_with_forever_ttl_is_sentinel() {
    let desc = table_source("Users", Some("forever"));
    let def = v0_source_to_stream_def(&desc).unwrap();
    let ttl = def.entity_ttl.unwrap();
    assert!(is_forever_ttl(ttl));
    assert_eq!(ttl, FOREVER_TTL);
}

#[test]
fn register_table_with_zero_ttl_is_zero_duration() {
    let desc = table_source("Users", Some("0"));
    let def = v0_source_to_stream_def(&desc).unwrap();
    assert_eq!(def.entity_ttl, Some(Duration::ZERO));
}

#[test]
fn parse_duration_forever() {
    assert_eq!(parse_duration_str("forever").unwrap(), FOREVER_TTL);
    assert_eq!(parse_duration_str("FOREVER").unwrap(), FOREVER_TTL);
}

#[test]
fn parse_duration_zero() {
    assert_eq!(parse_duration_str("0").unwrap(), Duration::ZERO);
}

#[test]
fn engine_register_roundtrips_ttls() {
    // Simulate: REGISTER → engine → query back the stream def.
    let mut engine = PipelineEngine::new();
    let def = v0_source_to_stream_def(&table_source("Users", Some("60d"))).unwrap();
    engine.register(def).unwrap();
    let stored = engine.get_stream("Users").unwrap();
    assert_eq!(stored.entity_ttl, Some(Duration::from_secs(60 * 86400)));
}
