// CORR-03: per-stream watermark_lateness override on StreamDefinition.
// Phase 46 Wave 3 (D-09/D-10): verifies StreamDefinition.watermark_lateness
// and WatermarkTracker.lateness_for().

use beava::engine::event_time::{WatermarkTracker, WATERMARK_LATENESS};
use beava::engine::pipeline::{PipelineEngine, StreamDefinition};
use std::time::{Duration, UNIX_EPOCH};

#[test]
fn per_stream_override_honored() {
    // Arrange: build a WatermarkTracker, register stream "Txns" with lateness = 10m.
    let tracker = WatermarkTracker::new();
    tracker.set_lateness("Txns", Duration::from_secs(600));

    // Assert lateness_for returns the override, not the 5s default.
    assert_eq!(
        tracker.lateness_for("Txns"),
        Duration::from_secs(600),
        "lateness_for should return the 600s override"
    );

    // Observe an event at a known time and verify watermark subtracts 600s.
    let event_time = UNIX_EPOCH + Duration::from_secs(1_000_000);
    tracker.observe("Txns", event_time);
    let wm = tracker
        .watermark("Txns")
        .expect("watermark should be Some after observe");
    let expected = event_time - Duration::from_secs(600);
    assert_eq!(
        wm, expected,
        "watermark should equal observed_max - 600s override"
    );
}

#[test]
fn absent_field_defaults_to_5s() {
    // Arrange: fresh WatermarkTracker, no set_lateness call.
    let tracker = WatermarkTracker::new();

    // Assert lateness_for falls back to the WATERMARK_LATENESS constant (5s).
    assert_eq!(
        tracker.lateness_for("AnyStream"),
        Duration::from_secs(5),
        "absent override should fall back to 5s constant"
    );
    assert_eq!(
        tracker.lateness_for("AnyStream"),
        WATERMARK_LATENESS,
        "absent override should equal WATERMARK_LATENESS"
    );

    // Observe an event and verify watermark subtracts 5s.
    let event_time = UNIX_EPOCH + Duration::from_secs(1_000_000);
    tracker.observe("AnyStream", event_time);
    let wm = tracker
        .watermark("AnyStream")
        .expect("watermark should be Some after observe");
    let expected = event_time - Duration::from_secs(5);
    assert_eq!(
        wm, expected,
        "watermark should equal observed_max - 5s default"
    );
}

#[test]
fn register_propagates_watermark_lateness_to_tracker() {
    // Full engine integration: register a StreamDefinition with watermark_lateness set
    // and verify WatermarkTracker picks it up.
    let mut engine = PipelineEngine::new();
    let def = StreamDefinition {
        name: "Txns".to_string(),
        key_field: Some("user_id".to_string()),
        watermark_lateness: Some(Duration::from_secs(600)),
        ..StreamDefinition::default()
    };
    engine.register(def).unwrap();
    // Engine exposes watermarks field; verify it was set.
    assert_eq!(
        engine.wm_lateness_for("Txns"),
        Duration::from_secs(600),
        "PipelineEngine::register must call set_lateness when Some"
    );
}

#[test]
fn register_no_watermark_lateness_keeps_5s_default() {
    let mut engine = PipelineEngine::new();
    let def = StreamDefinition {
        name: "Events".to_string(),
        key_field: None,
        watermark_lateness: None,
        ..StreamDefinition::default()
    };
    engine.register(def).unwrap();
    assert_eq!(
        engine.wm_lateness_for("Events"),
        Duration::from_secs(5),
        "absent watermark_lateness should leave tracker at 5s default"
    );
}
