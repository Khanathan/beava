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
        key_field: Some("user_id".into()),
        features: vec![
            ("tx_count_1h".into(), FeatureDef::Count {
                window: Duration::from_secs(3600),
                bucket: Duration::from_secs(60),
                where_expr: None,
            }),
            ("tx_sum_1h".into(), FeatureDef::Sum {
                field: "amount".into(),
                window: Duration::from_secs(3600),
                bucket: Duration::from_secs(60),
                optional: false,
                where_expr: None,
            }),
            ("avg_amount_1h".into(), FeatureDef::Avg {
                field: "amount".into(),
                window: Duration::from_secs(3600),
                bucket: Duration::from_secs(60),
                optional: false,
                where_expr: None,
            }),
            ("avg_via_derive".into(), FeatureDef::Derive {
                expr: parse_expr("tx_sum_1h / tx_count_1h").unwrap(),
            }),
        ],
        depends_on: None,
        filter: None,
        entity_ttl: None,
        history_ttl: None,
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
        key_field: Some("id".into()),
        features: vec![
            ("count_1h".into(), FeatureDef::Count {
                window: Duration::from_secs(3600),
                bucket: Duration::from_secs(60),
                where_expr: None,
            }),
            // Derive references a feature that doesn't exist -> Missing
            ("ratio".into(), FeatureDef::Derive {
                expr: parse_expr("count_1h / nonexistent_feature").unwrap(),
            }),
        ],
        depends_on: None,
        filter: None,
        entity_ttl: None,
        history_ttl: None,
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
        key_field: Some("id".into()),
        features: vec![
            ("count_5m".into(), FeatureDef::Count {
                window: Duration::from_secs(300),  // 5 minute window
                bucket: Duration::from_secs(60),
                where_expr: None,
            }),
        ],
        depends_on: None,
        filter: None,
        entity_ttl: None,
        history_ttl: None,
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
        key_field: Some("id".into()),
        features: vec![
            ("avg_1h".into(), FeatureDef::Avg {
                field: "amount".into(),
                window: Duration::from_secs(3600),
                bucket: Duration::from_secs(60),
                optional: false,
                where_expr: None,
            }),
            ("amount_vs_avg".into(), FeatureDef::Derive {
                expr: parse_expr("_event.amount / avg_1h").unwrap(),
            }),
        ],
        depends_on: None,
        filter: None,
        entity_ttl: None,
        history_ttl: None,
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

#[test]
fn test_get_features_returns_live_and_derived() {
    let mut engine = PipelineEngine::new();
    let mut store = StateStore::new();
    engine.register(make_tx_stream_with_derive()).unwrap();

    let now = ts(60_000);
    // Push two events so derive (avg_via_derive = sum/count) is meaningful
    engine.push("Transactions", &json!({"user_id": "u1", "amount": 30.0}), &mut store, now).unwrap();
    engine.push("Transactions", &json!({"user_id": "u1", "amount": 70.0}), &mut store, now).unwrap();

    let features = engine.get_features("u1", &mut store, now);

    // Live features
    assert_eq!(features.get("tx_count_1h"), Some(&FeatureValue::Int(2)));
    assert_eq!(features.get("tx_sum_1h"), Some(&FeatureValue::Float(100.0)));
    assert_eq!(features.get("avg_amount_1h"), Some(&FeatureValue::Float(50.0)));

    // Derived feature: tx_sum_1h / tx_count_1h = 100 / 2 = 50
    assert_eq!(features.get("avg_via_derive"), Some(&FeatureValue::Float(50.0)));
}

// ======================== Phase 7 Plan 03: DAG Cascade Tests ========================

fn make_keyless_stream(name: &str) -> StreamDefinition {
    StreamDefinition {
        name: name.into(),
        key_field: None,
        features: vec![],
        entity_ttl: None,
        history_ttl: None,
        depends_on: None,
        filter: None,
    }
}

fn make_keyed_dependent_stream(name: &str, key: &str, deps: Vec<&str>) -> StreamDefinition {
    StreamDefinition {
        name: name.into(),
        key_field: Some(key.into()),
        features: vec![
            ("count_1h".into(), FeatureDef::Count {
                window: Duration::from_secs(3600),
                bucket: Duration::from_secs(60),
                where_expr: None,
            }),
        ],
        entity_ttl: None,
        history_ttl: None,
        depends_on: Some(deps.iter().map(|s| s.to_string()).collect()),
        filter: None,
    }
}

#[test]
fn test_cascade_push_keyless_to_keyed() {
    let mut engine = PipelineEngine::new();
    let mut store = StateStore::new();
    let now = ts(1000);

    engine.register(make_keyless_stream("RawEvents")).unwrap();
    engine.register(make_keyed_dependent_stream("UserTx", "user_id", vec!["RawEvents"])).unwrap();

    // Push to keyless stream -- should cascade to UserTx
    let features = engine.push_with_cascade("RawEvents", &json!({
        "user_id": "u1", "amount": 50.0
    }), &mut store, now).unwrap();

    // Primary push to keyless returns empty
    assert!(features.is_empty());

    // But downstream keyed stream should have entity state
    let all = engine.get_features("u1", &mut store, now);
    assert_eq!(all.get("count_1h"), Some(&FeatureValue::Int(1)));
}

#[test]
fn test_multi_level_cascade() {
    let mut engine = PipelineEngine::new();
    let mut store = StateStore::new();
    let now = ts(1000);

    engine.register(make_keyless_stream("Raw")).unwrap();
    engine.register(make_keyed_dependent_stream("Level1", "user_id", vec!["Raw"])).unwrap();

    // Level2 depends on Level1 (keyed-to-keyed)
    let level2 = make_keyed_dependent_stream("Level2", "user_id", vec!["Level1"]);
    engine.register(level2).unwrap();

    let features = engine.push_with_cascade("Raw", &json!({
        "user_id": "u1", "amount": 10.0
    }), &mut store, now).unwrap();

    assert!(features.is_empty()); // keyless returns empty

    // Both Level1 and Level2 should have state
    let all = engine.get_features("u1", &mut store, now);
    assert!(all.contains_key("count_1h"));
}

#[test]
fn test_cascade_skips_missing_key_field() {
    let mut engine = PipelineEngine::new();
    let mut store = StateStore::new();
    let now = ts(1000);

    engine.register(make_keyless_stream("Raw")).unwrap();
    engine.register(make_keyed_dependent_stream("UserTx", "user_id", vec!["Raw"])).unwrap();
    engine.register(make_keyed_dependent_stream("MerchantTx", "merchant_id", vec!["Raw"])).unwrap();

    // Push event WITHOUT merchant_id -- MerchantTx should be skipped
    let _ = engine.push_with_cascade("Raw", &json!({
        "user_id": "u1", "amount": 50.0
    }), &mut store, now).unwrap();

    // UserTx has state, MerchantTx does not
    let user_features = engine.get_features("u1", &mut store, now);
    assert!(user_features.contains_key("count_1h"));

    // No merchant entity should exist
    assert_eq!(store.entity_count(), 1); // Only "u1"
}

#[test]
fn test_cycle_detection_rejects_registration() {
    let mut engine = PipelineEngine::new();

    let a = make_keyed_dependent_stream("A", "uid", vec!["B"]);
    let b = make_keyed_dependent_stream("B", "uid", vec!["A"]);

    engine.register(a).unwrap(); // A depends_on B (B not registered yet, OK)
    let result = engine.register(b); // B depends_on A -- cycle!
    assert!(result.is_err());
    let err_msg = result.unwrap_err().to_string();
    assert!(err_msg.contains("circular dependency"), "error should mention circular dependency: {}", err_msg);
}

#[test]
fn test_self_dependency_rejected() {
    let mut engine = PipelineEngine::new();
    let s = make_keyed_dependent_stream("Self", "uid", vec!["Self"]);
    let result = engine.register(s);
    assert!(result.is_err());
    let err_msg = result.unwrap_err().to_string();
    assert!(err_msg.contains("circular dependency"), "error should mention circular dependency: {}", err_msg);
}

#[test]
fn test_cascade_with_filter_on_downstream() {
    let mut engine = PipelineEngine::new();
    let mut store = StateStore::new();
    let now = ts(1000);

    engine.register(make_keyless_stream("Raw")).unwrap();

    // Downstream with filter: only failed events
    let mut filtered = make_keyed_dependent_stream("Failed", "user_id", vec!["Raw"]);
    filtered.filter = Some(parse_expr("_event.status == 'failed'").unwrap());
    engine.register(filtered).unwrap();

    // Push success event -- should NOT cascade to Failed
    let _ = engine.push_with_cascade("Raw", &json!({
        "user_id": "u1", "status": "success"
    }), &mut store, now).unwrap();
    assert_eq!(store.entity_count(), 0); // no entity created

    // Push failed event -- SHOULD cascade to Failed
    let _ = engine.push_with_cascade("Raw", &json!({
        "user_id": "u1", "status": "failed"
    }), &mut store, now).unwrap();
    let all = engine.get_features("u1", &mut store, now);
    assert_eq!(all.get("count_1h"), Some(&FeatureValue::Int(1)));
}

#[test]
fn test_keyed_to_keyed_cascade() {
    // Keyed stream A (key=user_id) -> Keyed stream B (key=user_id)
    let mut engine = PipelineEngine::new();
    let mut store = StateStore::new();
    let now = ts(1000);

    let a = StreamDefinition {
        name: "A".into(),
        key_field: Some("user_id".into()),
        features: vec![("a_count".into(), FeatureDef::Count {
            window: Duration::from_secs(3600),
            bucket: Duration::from_secs(60),
            where_expr: None,
        })],
        entity_ttl: None, history_ttl: None,
        depends_on: None, filter: None,
    };
    let b = StreamDefinition {
        name: "B".into(),
        key_field: Some("user_id".into()),
        features: vec![("b_count".into(), FeatureDef::Count {
            window: Duration::from_secs(3600),
            bucket: Duration::from_secs(60),
            where_expr: None,
        })],
        entity_ttl: None, history_ttl: None,
        depends_on: Some(vec!["A".into()]), filter: None,
    };
    engine.register(a).unwrap();
    engine.register(b).unwrap();

    // Push to A -- should cascade to B
    let features = engine.push_with_cascade("A", &json!({
        "user_id": "u1"
    }), &mut store, now).unwrap();

    // Features from primary push (stream A)
    assert_eq!(features.get("a_count"), Some(&FeatureValue::Int(1)));

    // B should also have been updated
    let all = engine.get_features("u1", &mut store, now);
    assert_eq!(all.get("b_count"), Some(&FeatureValue::Int(1)));
}

#[test]
fn test_multiple_depends_on_sources() {
    // Stream C depends on both A and B
    let mut engine = PipelineEngine::new();
    let mut store = StateStore::new();
    let now = ts(1000);

    engine.register(make_keyless_stream("A")).unwrap();
    engine.register(make_keyless_stream("B")).unwrap();
    engine.register(make_keyed_dependent_stream("C", "user_id", vec!["A", "B"])).unwrap();

    // Push to A -- should cascade to C
    let _ = engine.push_with_cascade("A", &json!({
        "user_id": "u1"
    }), &mut store, now).unwrap();
    let all = engine.get_features("u1", &mut store, now);
    assert_eq!(all.get("count_1h"), Some(&FeatureValue::Int(1)));

    // Push to B -- should also cascade to C
    let _ = engine.push_with_cascade("B", &json!({
        "user_id": "u1"
    }), &mut store, now).unwrap();
    let all = engine.get_features("u1", &mut store, now);
    assert_eq!(all.get("count_1h"), Some(&FeatureValue::Int(2)));
}

// ======================== FeatureValue Serialization Round-Trip ========================

#[test]
fn test_feature_value_json_round_trip() {
    let values = vec![
        FeatureValue::Float(3.14),
        FeatureValue::Int(42),
        FeatureValue::String("hello".into()),
        FeatureValue::Missing,
        FeatureValue::Float(-0.0),
        FeatureValue::Int(i64::MIN),
        FeatureValue::Int(i64::MAX),
    ];

    for val in &values {
        let json = serde_json::to_string(val).expect("serialize");
        let back: FeatureValue = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(&back, val, "round-trip failed for {:?} -> {}", val, json);
    }
}
