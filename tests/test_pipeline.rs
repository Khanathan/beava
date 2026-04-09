//! Integration tests for PipelineEngine push-through flow.
//!
//! Exercises the full path: create PipelineEngine, register stream,
//! create StateStore, push events, verify returned features.

use std::time::{Duration, SystemTime, UNIX_EPOCH};
use serde_json::json;
use tally::engine::pipeline::{PipelineEngine, StreamDefinition, FeatureDef};
use tally::engine::expression::parse_expr;
use tally::state::store::StateStore;
use tally::types::FeatureValue;

fn ts(secs: u64) -> SystemTime {
    UNIX_EPOCH + Duration::from_secs(secs)
}

fn make_tx_stream_with_derive() -> StreamDefinition {
    StreamDefinition {
        name: "Transactions".into(),
        key_field: "user_id".into(),
        features: vec![
            ("tx_count_1h".into(), FeatureDef::Count {
                window: Duration::from_secs(3600),
                bucket: Duration::from_secs(60),
            }),
            ("tx_sum_1h".into(), FeatureDef::Sum {
                field: "amount".into(),
                window: Duration::from_secs(3600),
                bucket: Duration::from_secs(60),
                optional: false,
            }),
            ("avg_amount_1h".into(), FeatureDef::Avg {
                field: "amount".into(),
                window: Duration::from_secs(3600),
                bucket: Duration::from_secs(60),
                optional: false,
            }),
            ("avg_via_derive".into(), FeatureDef::Derive {
                expr: parse_expr("tx_sum_1h / tx_count_1h").unwrap(),
            }),
        ],
    }
}

#[test]
fn test_push_single_event_returns_all_features() {
    let mut engine = PipelineEngine::new();
    let mut store = StateStore::new();
    engine.register(make_tx_stream_with_derive()).unwrap();

    let now = ts(60_000);
    let event = json!({"user_id": "u123", "amount": 50.0});
    let features = engine.push("Transactions", &event, &mut store, now).unwrap();

    assert_eq!(features.get("tx_count_1h"), Some(&FeatureValue::Int(1)));
    assert_eq!(features.get("tx_sum_1h"), Some(&FeatureValue::Float(50.0)));
    assert_eq!(features.get("avg_amount_1h"), Some(&FeatureValue::Float(50.0)));
    assert_eq!(features.get("avg_via_derive"), Some(&FeatureValue::Float(50.0)));
}

#[test]
fn test_push_multiple_events_aggregates_correctly() {
    let mut engine = PipelineEngine::new();
    let mut store = StateStore::new();
    engine.register(make_tx_stream_with_derive()).unwrap();

    let now = ts(60_000);
    engine.push("Transactions", &json!({"user_id": "u123", "amount": 10.0}), &mut store, now).unwrap();
    engine.push("Transactions", &json!({"user_id": "u123", "amount": 20.0}), &mut store, now).unwrap();
    let features = engine.push("Transactions", &json!({"user_id": "u123", "amount": 30.0}), &mut store, now).unwrap();

    assert_eq!(features.get("tx_count_1h"), Some(&FeatureValue::Int(3)));
    assert_eq!(features.get("tx_sum_1h"), Some(&FeatureValue::Float(60.0)));
    assert_eq!(features.get("avg_amount_1h"), Some(&FeatureValue::Float(20.0)));
    assert_eq!(features.get("avg_via_derive"), Some(&FeatureValue::Float(20.0)));
}

#[test]
fn test_different_keys_have_separate_state() {
    let mut engine = PipelineEngine::new();
    let mut store = StateStore::new();
    engine.register(make_tx_stream_with_derive()).unwrap();

    let now = ts(60_000);
    engine.push("Transactions", &json!({"user_id": "u123", "amount": 100.0}), &mut store, now).unwrap();
    engine.push("Transactions", &json!({"user_id": "u456", "amount": 200.0}), &mut store, now).unwrap();

    let f1 = store.get_all_features("u123", now);
    let f2 = store.get_all_features("u456", now);

    assert_eq!(f1.get("tx_sum_1h"), Some(&FeatureValue::Float(100.0)));
    assert_eq!(f2.get("tx_sum_1h"), Some(&FeatureValue::Float(200.0)));
}

#[test]
fn test_derive_division_by_zero_returns_missing() {
    let stream = StreamDefinition {
        name: "Test".into(),
        key_field: "id".into(),
        features: vec![
            ("count_1h".into(), FeatureDef::Count {
                window: Duration::from_secs(3600),
                bucket: Duration::from_secs(60),
            }),
            // Derive references a feature that doesn't exist -> Missing
            ("ratio".into(), FeatureDef::Derive {
                expr: parse_expr("count_1h / nonexistent_feature").unwrap(),
            }),
        ],
    };

    let mut engine = PipelineEngine::new();
    let mut store = StateStore::new();
    engine.register(stream).unwrap();

    let now = ts(60_000);
    let features = engine.push("Test", &json!({"id": "k1"}), &mut store, now).unwrap();

    // nonexistent_feature -> Missing, division with Missing -> Missing
    assert_eq!(features.get("ratio"), Some(&FeatureValue::Missing));
}

#[test]
fn test_get_features_unknown_key_returns_empty() {
    let engine = PipelineEngine::new();
    let mut store = StateStore::new();
    let features = engine.get_features("nonexistent", &mut store, ts(1000));
    assert!(features.is_empty());
}

#[test]
fn test_static_feature_alongside_live_features() {
    let mut engine = PipelineEngine::new();
    let mut store = StateStore::new();
    engine.register(make_tx_stream_with_derive()).unwrap();

    let now = ts(60_000);
    engine.push("Transactions", &json!({"user_id": "u123", "amount": 50.0}), &mut store, now).unwrap();

    // Write a static feature
    store.set_static("u123", "lifetime_value", FeatureValue::Float(4500.0), now);

    let features = engine.get_features("u123", &mut store, now);
    assert_eq!(features.get("tx_count_1h"), Some(&FeatureValue::Int(1)));
    assert_eq!(features.get("lifetime_value"), Some(&FeatureValue::Float(4500.0)));
}

#[test]
fn test_window_expiration_end_to_end() {
    let stream = StreamDefinition {
        name: "Short".into(),
        key_field: "id".into(),
        features: vec![
            ("count_5m".into(), FeatureDef::Count {
                window: Duration::from_secs(300),  // 5 minute window
                bucket: Duration::from_secs(60),
            }),
        ],
    };

    let mut engine = PipelineEngine::new();
    let mut store = StateStore::new();
    engine.register(stream).unwrap();

    let t0 = ts(60_000);
    engine.push("Short", &json!({"id": "k1"}), &mut store, t0).unwrap();

    // Verify count is 1 at t0
    let f = store.get_all_features("k1", t0);
    assert_eq!(f.get("count_5m"), Some(&FeatureValue::Int(1)));

    // Advance past window (10 minutes > 5 minute window)
    let t_future = t0 + Duration::from_secs(600);
    let f = store.get_all_features("k1", t_future);
    assert_eq!(f.get("count_5m"), Some(&FeatureValue::Missing));
}

#[test]
fn test_push_type_error_on_non_numeric_sum_field() {
    let mut engine = PipelineEngine::new();
    let mut store = StateStore::new();
    engine.register(make_tx_stream_with_derive()).unwrap();

    let now = ts(60_000);
    let event = json!({"user_id": "u123", "amount": "not_a_number"});
    let result = engine.push("Transactions", &event, &mut store, now);
    assert!(result.is_err());
}

#[test]
fn test_derive_with_event_field_access() {
    let stream = StreamDefinition {
        name: "Test".into(),
        key_field: "id".into(),
        features: vec![
            ("avg_1h".into(), FeatureDef::Avg {
                field: "amount".into(),
                window: Duration::from_secs(3600),
                bucket: Duration::from_secs(60),
                optional: false,
            }),
            ("amount_vs_avg".into(), FeatureDef::Derive {
                expr: parse_expr("_event.amount / avg_1h").unwrap(),
            }),
        ],
    };

    let mut engine = PipelineEngine::new();
    let mut store = StateStore::new();
    engine.register(stream).unwrap();

    let now = ts(60_000);
    // Push first event: avg=10
    engine.push("Test", &json!({"id": "k1", "amount": 10.0}), &mut store, now).unwrap();
    // Push second event: avg=15, event.amount=20, ratio=20/15=1.333...
    let features = engine.push("Test", &json!({"id": "k1", "amount": 20.0}), &mut store, now).unwrap();

    let ratio = features.get("amount_vs_avg").unwrap();
    if let FeatureValue::Float(v) = ratio {
        assert!((v - 20.0 / 15.0).abs() < 1e-9);
    } else {
        panic!("expected Float, got {:?}", ratio);
    }
}
