//! Phase 12.8 Plan 02 — RED tests for `EventDescriptor.cold_after_ms` field.
//!
//! Per CONTEXT D-01 (locked 2026-05-01): the Python `@bv.event(cold_after=...)`
//! decorator parses to milliseconds and the parsed value is persisted on the
//! Rust `EventDescriptor` struct as `cold_after_ms: Option<u64>`. Plan 02 adds
//! the field; Plan 03 wires it into the apply hot path for lazy eviction.
//!
//! These three tests pin the wire-level contract:
//!
//! 1. Round-trip — building an EventDescriptor with `cold_after_ms: Some(N)`
//!    and serializing to JSON then deserializing back preserves the value.
//!
//! 2. Default — JSON without a `cold_after_ms` key deserializes to `None`
//!    (forward-compat with older Python clients per `#[serde(default)]`).
//!
//! 3. Equivalence — `equiv_ignoring_version` includes the new field, so two
//!    descriptors that differ ONLY in `cold_after_ms` are NOT equivalent.
//!    This locks the field into the diff-engine's conflict-detection set
//!    (alongside `dedupe_key`, `dedupe_window_ms`, `keep_events_for_ms`).
//!
//! All three FAIL at HEAD because the field does not exist. Plan 02 Task 2.b
//! lands GREEN.

use beava_core::registry::EventDescriptor;
use beava_core::schema::{EventSchema, FieldType};
use std::collections::BTreeMap;
use std::sync::Arc;

fn make_event_schema() -> EventSchema {
    let mut fields = BTreeMap::new();
    fields.insert("amount".to_string(), FieldType::F64);
    EventSchema {
        fields,
        optional_fields: vec![],
    }
}

#[test]
fn event_descriptor_cold_after_ms_round_trips_through_json() {
    let original = EventDescriptor {
        name: "Tx".to_string(),
        schema: make_event_schema(),
        dedupe_key: None,
        dedupe_window_ms: None,
        keep_events_for_ms: None,
        cold_after_ms: Some(604_800_000),
        registered_at_version: 0,
        name_arc: Arc::from(""),
        apply_field_names: vec![],
    };

    let json = serde_json::to_string(&original).expect("serialize EventDescriptor");
    let back: EventDescriptor = serde_json::from_str(&json).expect("deserialize EventDescriptor");

    assert_eq!(
        back.cold_after_ms,
        Some(604_800_000),
        "cold_after_ms must round-trip through JSON; serialized: {json}"
    );
}

#[test]
fn event_descriptor_cold_after_ms_default_is_none_when_key_absent() {
    // Older Python clients may emit JSON without a cold_after_ms key.
    // The #[serde(default)] annotation must handle the missing key as None.
    let json = r#"{
        "name": "Tx",
        "schema": {
            "fields": {"amount": "f64"},
            "optional_fields": []
        },
        "dedupe_key": null,
        "dedupe_window_ms": null,
        "keep_events_for_ms": null
    }"#;

    let descriptor: EventDescriptor =
        serde_json::from_str(json).expect("deserialize EventDescriptor without cold_after_ms key");

    assert_eq!(
        descriptor.cold_after_ms, None,
        "cold_after_ms must default to None when the key is absent (forward-compat)"
    );
}

#[test]
fn event_descriptor_equiv_ignoring_version_includes_cold_after_ms() {
    let schema = make_event_schema();

    let a = EventDescriptor {
        name: "Tx".to_string(),
        schema: schema.clone(),
        dedupe_key: None,
        dedupe_window_ms: None,
        keep_events_for_ms: None,
        cold_after_ms: Some(604_800_000),
        registered_at_version: 0,
        name_arc: Arc::from(""),
        apply_field_names: vec![],
    };
    let b = EventDescriptor {
        name: "Tx".to_string(),
        schema,
        dedupe_key: None,
        dedupe_window_ms: None,
        keep_events_for_ms: None,
        cold_after_ms: Some(2_592_000_000),
        registered_at_version: 0,
        name_arc: Arc::from(""),
        apply_field_names: vec![],
    };

    assert!(
        !a.equiv_ignoring_version(&b),
        "equiv_ignoring_version must consider cold_after_ms; otherwise diff engine \
         silently accepts conflicting cold-TTL configs across re-registers"
    );
}
