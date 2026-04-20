//! Integration tests for PipelineEngine push-through flow.
//!
//! Exercises the full path: create PipelineEngine, register stream,
//! create StateStore, push events, verify returned features.
//!
//! Phase 54-04 Pass A5: gated under `state-inmem`. `engine.get_features` +
//! the legacy `store: StateStore` constructor arg / `engine.push(&store)`
//! path only exist on the in-memory build after this pass. Category-B
//! cases were `#[ignore]`'d in Wave 3 pending Wave-4 migration; this
//! module-level gate keeps them from breaking the default `cargo check
//! --tests` gate until Pass B migrates / deletes the remaining legacy
//! calls.
#![cfg(feature = "state-inmem")]

use ahash::AHashSet;
use beava::engine::expression::parse_expr;
use beava::engine::pipeline::{FeatureDef, PipelineEngine, Projection, StreamDefinition};
use beava::state::store::StateStore;
use beava::types::FeatureValue;
use serde_json::json;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

fn ts(secs: u64) -> SystemTime {
    UNIX_EPOCH + Duration::from_secs(secs)
}

fn make_tx_stream_with_derive() -> StreamDefinition {
    StreamDefinition {
        name: "Transactions".into(),
        key_field: Some("user_id".into()),
        group_by_keys: None,
        features: vec![
            (
                "tx_count_1h".into(),
                FeatureDef::Count {
                    window: Duration::from_secs(3600),
                    bucket: Duration::from_secs(60),
                    where_expr: None,
                    backfill: false,
                },
            ),
            (
                "tx_sum_1h".into(),
                FeatureDef::Sum {
                    field: "amount".into(),
                    window: Duration::from_secs(3600),
                    bucket: Duration::from_secs(60),
                    optional: false,
                    where_expr: None,
                    backfill: false,
                },
            ),
            (
                "avg_amount_1h".into(),
                FeatureDef::Avg {
                    field: "amount".into(),
                    window: Duration::from_secs(3600),
                    bucket: Duration::from_secs(60),
                    optional: false,
                    where_expr: None,
                    backfill: false,
                },
            ),
            (
                "avg_via_derive".into(),
                FeatureDef::Derive {
                    expr: parse_expr("tx_sum_1h / tx_count_1h").unwrap(),
                },
            ),
        ],
        depends_on: None,
        filter: None,
        entity_ttl: None,
        history_ttl: None,
        projection: None,
        ephemeral: None,
        pipeline_ttl: None,
        max_keys: None,
        watermark_lateness: None,
        shard_key: None,
    }
}

#[ignore = "54-03 Task 4: legacy StateStore API / engine.push(&store, ...); Wave 4 re-enables after legacy-engine removal"]
#[test]
fn test_push_single_event_returns_all_features() {
    let mut engine = PipelineEngine::new();
    let store = StateStore::new();
    engine.register(make_tx_stream_with_derive()).unwrap();

    let now = ts(60_000);
    let event = json!({"user_id": "u123", "amount": 50.0});
    let features = engine.push("Transactions", &event, &store, now).unwrap();

    assert_eq!(features.get("tx_count_1h"), Some(&FeatureValue::Int(1)));
    assert_eq!(features.get("tx_sum_1h"), Some(&FeatureValue::Float(50.0)));
    assert_eq!(
        features.get("avg_amount_1h"),
        Some(&FeatureValue::Float(50.0))
    );
    assert_eq!(
        features.get("avg_via_derive"),
        Some(&FeatureValue::Float(50.0))
    );
}

#[ignore = "54-03 Task 4: legacy StateStore API / engine.push(&store, ...); Wave 4 re-enables after legacy-engine removal"]
#[test]
fn test_push_multiple_events_aggregates_correctly() {
    let mut engine = PipelineEngine::new();
    let store = StateStore::new();
    engine.register(make_tx_stream_with_derive()).unwrap();

    let now = ts(60_000);
    engine
        .push(
            "Transactions",
            &json!({"user_id": "u123", "amount": 10.0}),
            &store,
            now,
        )
        .unwrap();
    engine
        .push(
            "Transactions",
            &json!({"user_id": "u123", "amount": 20.0}),
            &store,
            now,
        )
        .unwrap();
    let features = engine
        .push(
            "Transactions",
            &json!({"user_id": "u123", "amount": 30.0}),
            &store,
            now,
        )
        .unwrap();

    assert_eq!(features.get("tx_count_1h"), Some(&FeatureValue::Int(3)));
    assert_eq!(features.get("tx_sum_1h"), Some(&FeatureValue::Float(60.0)));
    assert_eq!(
        features.get("avg_amount_1h"),
        Some(&FeatureValue::Float(20.0))
    );
    assert_eq!(
        features.get("avg_via_derive"),
        Some(&FeatureValue::Float(20.0))
    );
}

#[ignore = "54-03 Task 4: legacy StateStore API / engine.push(&store, ...); Wave 4 re-enables after legacy-engine removal"]
#[test]
fn test_different_keys_have_separate_state() {
    let mut engine = PipelineEngine::new();
    let store = StateStore::new();
    engine.register(make_tx_stream_with_derive()).unwrap();

    let now = ts(60_000);
    engine
        .push(
            "Transactions",
            &json!({"user_id": "u123", "amount": 100.0}),
            &store,
            now,
        )
        .unwrap();
    engine
        .push(
            "Transactions",
            &json!({"user_id": "u456", "amount": 200.0}),
            &store,
            now,
        )
        .unwrap();

    let f1 = store.get_all_features("u123", now);
    let f2 = store.get_all_features("u456", now);

    assert_eq!(f1.get("tx_sum_1h"), Some(&FeatureValue::Float(100.0)));
    assert_eq!(f2.get("tx_sum_1h"), Some(&FeatureValue::Float(200.0)));
}

#[ignore = "54-03 Task 4: legacy StateStore API / engine.push(&store, ...); Wave 4 re-enables after legacy-engine removal"]
#[test]
fn test_derive_division_by_zero_returns_missing() {
    let stream = StreamDefinition {
        name: "Test".into(),
        key_field: Some("id".into()),
        group_by_keys: None,
        features: vec![
            (
                "count_1h".into(),
                FeatureDef::Count {
                    window: Duration::from_secs(3600),
                    bucket: Duration::from_secs(60),
                    where_expr: None,
                    backfill: false,
                },
            ),
            // Derive references a feature that doesn't exist -> Missing
            (
                "ratio".into(),
                FeatureDef::Derive {
                    expr: parse_expr("count_1h / nonexistent_feature").unwrap(),
                },
            ),
        ],
        depends_on: None,
        filter: None,
        entity_ttl: None,
        history_ttl: None,
        projection: None,
        ephemeral: None,
        pipeline_ttl: None,
        max_keys: None,
        watermark_lateness: None,
        shard_key: None,
    };

    let mut engine = PipelineEngine::new();
    let store = StateStore::new();
    engine.register(stream).unwrap();

    let now = ts(60_000);
    let features = engine
        .push("Test", &json!({"id": "k1"}), &store, now)
        .unwrap();

    // nonexistent_feature -> Missing, division with Missing -> Missing
    assert_eq!(features.get("ratio"), Some(&FeatureValue::Missing));
}

#[ignore = "54-03 Task 4: legacy StateStore API / engine.push(&store, ...); Wave 4 re-enables after legacy-engine removal"]
#[test]
fn test_get_features_unknown_key_returns_empty() {
    let engine = PipelineEngine::new();
    let store = StateStore::new();
    let features = engine.get_features("nonexistent", &store, ts(1000));
    assert!(features.is_empty());
}

#[ignore = "54-03 Task 4: legacy StateStore API / engine.push(&store, ...); Wave 4 re-enables after legacy-engine removal"]
#[test]
fn test_static_feature_alongside_live_features() {
    let mut engine = PipelineEngine::new();
    let store = StateStore::new();
    engine.register(make_tx_stream_with_derive()).unwrap();

    let now = ts(60_000);
    engine
        .push(
            "Transactions",
            &json!({"user_id": "u123", "amount": 50.0}),
            &store,
            now,
        )
        .unwrap();

    // Write a static feature
    store.set_static("u123", "lifetime_value", FeatureValue::Float(4500.0), now);

    let features = engine.get_features("u123", &store, now);
    assert_eq!(features.get("tx_count_1h"), Some(&FeatureValue::Int(1)));
    assert_eq!(
        features.get("lifetime_value"),
        Some(&FeatureValue::Float(4500.0))
    );
}

#[ignore = "54-03 Task 4: legacy StateStore API / engine.push(&store, ...); Wave 4 re-enables after legacy-engine removal"]
#[test]
fn test_window_expiration_end_to_end() {
    let stream = StreamDefinition {
        name: "Short".into(),
        key_field: Some("id".into()),
        group_by_keys: None,
        features: vec![(
            "count_5m".into(),
            FeatureDef::Count {
                window: Duration::from_secs(300), // 5 minute window
                bucket: Duration::from_secs(60),
                where_expr: None,
                backfill: false,
            },
        )],
        depends_on: None,
        filter: None,
        entity_ttl: None,
        history_ttl: None,
        projection: None,
        ephemeral: None,
        pipeline_ttl: None,
        max_keys: None,
        watermark_lateness: None,
        shard_key: None,
    };

    let mut engine = PipelineEngine::new();
    let store = StateStore::new();
    engine.register(stream).unwrap();

    let t0 = ts(60_000);
    engine
        .push("Short", &json!({"id": "k1"}), &store, t0)
        .unwrap();

    // Verify count is 1 at t0
    let f = store.get_all_features("k1", t0);
    assert_eq!(f.get("count_5m"), Some(&FeatureValue::Int(1)));

    // Advance past window (10 minutes > 5 minute window)
    let t_future = t0 + Duration::from_secs(600);
    let f = store.get_all_features("k1", t_future);
    assert_eq!(f.get("count_5m"), Some(&FeatureValue::Missing));
}

#[ignore = "54-03 Task 4: legacy StateStore API / engine.push(&store, ...); Wave 4 re-enables after legacy-engine removal"]
#[test]
fn test_push_type_error_on_non_numeric_sum_field() {
    let mut engine = PipelineEngine::new();
    let store = StateStore::new();
    engine.register(make_tx_stream_with_derive()).unwrap();

    let now = ts(60_000);
    let event = json!({"user_id": "u123", "amount": "not_a_number"});
    let result = engine.push("Transactions", &event, &store, now);
    assert!(result.is_err());
}

#[ignore = "54-03 Task 4: legacy StateStore API / engine.push(&store, ...); Wave 4 re-enables after legacy-engine removal"]
#[test]
fn test_derive_with_event_field_access() {
    let stream = StreamDefinition {
        name: "Test".into(),
        key_field: Some("id".into()),
        group_by_keys: None,
        features: vec![
            (
                "avg_1h".into(),
                FeatureDef::Avg {
                    field: "amount".into(),
                    window: Duration::from_secs(3600),
                    bucket: Duration::from_secs(60),
                    optional: false,
                    where_expr: None,
                    backfill: false,
                },
            ),
            (
                "amount_vs_avg".into(),
                FeatureDef::Derive {
                    expr: parse_expr("_event.amount / avg_1h").unwrap(),
                },
            ),
        ],
        depends_on: None,
        filter: None,
        entity_ttl: None,
        history_ttl: None,
        projection: None,
        ephemeral: None,
        pipeline_ttl: None,
        max_keys: None,
        watermark_lateness: None,
        shard_key: None,
    };

    let mut engine = PipelineEngine::new();
    let store = StateStore::new();
    engine.register(stream).unwrap();

    let now = ts(60_000);
    // Push first event: avg=10
    engine
        .push("Test", &json!({"id": "k1", "amount": 10.0}), &store, now)
        .unwrap();
    // Push second event: avg=15, event.amount=20, ratio=20/15=1.333...
    let features = engine
        .push("Test", &json!({"id": "k1", "amount": 20.0}), &store, now)
        .unwrap();

    let ratio = features.get("amount_vs_avg").unwrap();
    if let FeatureValue::Float(v) = ratio {
        assert!((v - 20.0 / 15.0).abs() < 1e-9);
    } else {
        panic!("expected Float, got {:?}", ratio);
    }
}

#[ignore = "54-03 Task 4: legacy StateStore API / engine.push(&store, ...); Wave 4 re-enables after legacy-engine removal"]
#[test]
fn test_get_features_returns_live_and_derived() {
    let mut engine = PipelineEngine::new();
    let store = StateStore::new();
    engine.register(make_tx_stream_with_derive()).unwrap();

    let now = ts(60_000);
    // Push two events so derive (avg_via_derive = sum/count) is meaningful
    engine
        .push(
            "Transactions",
            &json!({"user_id": "u1", "amount": 30.0}),
            &store,
            now,
        )
        .unwrap();
    engine
        .push(
            "Transactions",
            &json!({"user_id": "u1", "amount": 70.0}),
            &store,
            now,
        )
        .unwrap();

    let features = engine.get_features("u1", &store, now);

    // Live features
    assert_eq!(features.get("tx_count_1h"), Some(&FeatureValue::Int(2)));
    assert_eq!(features.get("tx_sum_1h"), Some(&FeatureValue::Float(100.0)));
    assert_eq!(
        features.get("avg_amount_1h"),
        Some(&FeatureValue::Float(50.0))
    );

    // Derived feature: tx_sum_1h / tx_count_1h = 100 / 2 = 50
    assert_eq!(
        features.get("avg_via_derive"),
        Some(&FeatureValue::Float(50.0))
    );
}

// ======================== Phase 7 Plan 03: DAG Cascade Tests ========================

fn make_keyless_stream(name: &str) -> StreamDefinition {
    StreamDefinition {
        name: name.into(),
        key_field: None,
        group_by_keys: None,
        features: vec![],
        entity_ttl: None,
        history_ttl: None,
        projection: None,
        ephemeral: None,
        pipeline_ttl: None,
        max_keys: None,
        depends_on: None,
        filter: None,
        watermark_lateness: None,
        shard_key: None,
    }
}

fn make_keyed_dependent_stream(name: &str, key: &str, deps: Vec<&str>) -> StreamDefinition {
    StreamDefinition {
        name: name.into(),
        key_field: Some(key.into()),
        group_by_keys: None,
        features: vec![(
            "count_1h".into(),
            FeatureDef::Count {
                window: Duration::from_secs(3600),
                bucket: Duration::from_secs(60),
                where_expr: None,
                backfill: false,
            },
        )],
        entity_ttl: None,
        history_ttl: None,
        projection: None,
        ephemeral: None,
        pipeline_ttl: None,
        max_keys: None,
        depends_on: Some(deps.iter().map(|s| s.to_string()).collect()),
        filter: None,
        watermark_lateness: None,
        shard_key: None,
    }
}

#[ignore = "54-03 Task 4: legacy StateStore API / engine.push(&store, ...); Wave 4 re-enables after legacy-engine removal"]
#[test]
fn test_cascade_push_keyless_to_keyed() {
    let mut engine = PipelineEngine::new();
    let store = StateStore::new();
    let now = ts(1000);

    engine.register(make_keyless_stream("RawEvents")).unwrap();
    engine
        .register(make_keyed_dependent_stream(
            "UserTx",
            "user_id",
            vec!["RawEvents"],
        ))
        .unwrap();

    // Push to keyless stream -- should cascade to UserTx
    let features = engine
        .push_with_cascade(
            "RawEvents",
            &json!({
                "user_id": "u1", "amount": 50.0
            }),
            &store,
            now,
        )
        .unwrap();

    // Primary push to keyless returns empty
    assert!(features.is_empty());

    // But downstream keyed stream should have entity state
    let all = engine.get_features("u1", &store, now);
    assert_eq!(all.get("count_1h"), Some(&FeatureValue::Int(1)));
}

#[ignore = "54-03 Task 4: legacy StateStore API / engine.push(&store, ...); Wave 4 re-enables after legacy-engine removal"]
#[test]
fn test_multi_level_cascade() {
    let mut engine = PipelineEngine::new();
    let store = StateStore::new();
    let now = ts(1000);

    engine.register(make_keyless_stream("Raw")).unwrap();
    engine
        .register(make_keyed_dependent_stream(
            "Level1",
            "user_id",
            vec!["Raw"],
        ))
        .unwrap();

    // Level2 depends on Level1 (keyed-to-keyed)
    let level2 = make_keyed_dependent_stream("Level2", "user_id", vec!["Level1"]);
    engine.register(level2).unwrap();

    let features = engine
        .push_with_cascade(
            "Raw",
            &json!({
                "user_id": "u1", "amount": 10.0
            }),
            &store,
            now,
        )
        .unwrap();

    assert!(features.is_empty()); // keyless returns empty

    // Both Level1 and Level2 should have state
    let all = engine.get_features("u1", &store, now);
    assert!(all.contains_key("count_1h"));
}

#[ignore = "54-03 Task 4: legacy StateStore API / engine.push(&store, ...); Wave 4 re-enables after legacy-engine removal"]
#[test]
fn test_cascade_skips_missing_key_field() {
    let mut engine = PipelineEngine::new();
    let store = StateStore::new();
    let now = ts(1000);

    engine.register(make_keyless_stream("Raw")).unwrap();
    engine
        .register(make_keyed_dependent_stream(
            "UserTx",
            "user_id",
            vec!["Raw"],
        ))
        .unwrap();
    engine
        .register(make_keyed_dependent_stream(
            "MerchantTx",
            "merchant_id",
            vec!["Raw"],
        ))
        .unwrap();

    // Push event WITHOUT merchant_id -- MerchantTx should be skipped
    let _ = engine
        .push_with_cascade(
            "Raw",
            &json!({
                "user_id": "u1", "amount": 50.0
            }),
            &store,
            now,
        )
        .unwrap();

    // UserTx has state, MerchantTx does not
    let user_features = engine.get_features("u1", &store, now);
    assert!(user_features.contains_key("count_1h"));

    // No merchant entity should exist
    assert_eq!(store.entity_count(), 1); // Only "u1"
}

#[ignore = "54-03 Task 4: legacy StateStore API / engine.push(&store, ...); Wave 4 re-enables after legacy-engine removal"]
#[test]
fn test_cycle_detection_rejects_registration() {
    let mut engine = PipelineEngine::new();

    let a = make_keyed_dependent_stream("A", "uid", vec!["B"]);
    let b = make_keyed_dependent_stream("B", "uid", vec!["A"]);

    engine.register(a).unwrap(); // A depends_on B (B not registered yet, OK)
    let result = engine.register(b); // B depends_on A -- cycle!
    assert!(result.is_err());
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("circular dependency"),
        "error should mention circular dependency: {}",
        err_msg
    );
}

#[ignore = "54-03 Task 4: legacy StateStore API / engine.push(&store, ...); Wave 4 re-enables after legacy-engine removal"]
#[test]
fn test_self_dependency_rejected() {
    let mut engine = PipelineEngine::new();
    let s = make_keyed_dependent_stream("Self", "uid", vec!["Self"]);
    let result = engine.register(s);
    assert!(result.is_err());
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("circular dependency"),
        "error should mention circular dependency: {}",
        err_msg
    );
}

#[ignore = "54-03 Task 4: legacy StateStore API / engine.push(&store, ...); Wave 4 re-enables after legacy-engine removal"]
#[test]
fn test_cascade_with_filter_on_downstream() {
    let mut engine = PipelineEngine::new();
    let store = StateStore::new();
    let now = ts(1000);

    engine.register(make_keyless_stream("Raw")).unwrap();

    // Downstream with filter: only failed events
    let mut filtered = make_keyed_dependent_stream("Failed", "user_id", vec!["Raw"]);
    filtered.filter = Some(parse_expr("_event.status == 'failed'").unwrap());
    engine.register(filtered).unwrap();

    // Push success event -- should NOT cascade to Failed
    let _ = engine
        .push_with_cascade(
            "Raw",
            &json!({
                "user_id": "u1", "status": "success"
            }),
            &store,
            now,
        )
        .unwrap();
    assert_eq!(store.entity_count(), 0); // no entity created

    // Push failed event -- SHOULD cascade to Failed
    let _ = engine
        .push_with_cascade(
            "Raw",
            &json!({
                "user_id": "u1", "status": "failed"
            }),
            &store,
            now,
        )
        .unwrap();
    let all = engine.get_features("u1", &store, now);
    assert_eq!(all.get("count_1h"), Some(&FeatureValue::Int(1)));
}

#[ignore = "54-03 Task 4: legacy StateStore API / engine.push(&store, ...); Wave 4 re-enables after legacy-engine removal"]
#[test]
fn test_keyed_to_keyed_cascade() {
    // Keyed stream A (key=user_id) -> Keyed stream B (key=user_id)
    let mut engine = PipelineEngine::new();
    let store = StateStore::new();
    let now = ts(1000);

    let a = StreamDefinition {
        name: "A".into(),
        key_field: Some("user_id".into()),
        group_by_keys: None,
        features: vec![(
            "a_count".into(),
            FeatureDef::Count {
                window: Duration::from_secs(3600),
                bucket: Duration::from_secs(60),
                where_expr: None,
                backfill: false,
            },
        )],
        entity_ttl: None,
        history_ttl: None,
        projection: None,
        ephemeral: None,
        pipeline_ttl: None,
        max_keys: None,
        depends_on: None,
        filter: None,
        watermark_lateness: None,
        shard_key: None,
    };
    let b = StreamDefinition {
        name: "B".into(),
        key_field: Some("user_id".into()),
        group_by_keys: None,
        features: vec![(
            "b_count".into(),
            FeatureDef::Count {
                window: Duration::from_secs(3600),
                bucket: Duration::from_secs(60),
                where_expr: None,
                backfill: false,
            },
        )],
        entity_ttl: None,
        history_ttl: None,
        projection: None,
        ephemeral: None,
        pipeline_ttl: None,
        max_keys: None,
        depends_on: Some(vec!["A".into()]),
        filter: None,
        watermark_lateness: None,
        shard_key: None,
    };
    engine.register(a).unwrap();
    engine.register(b).unwrap();

    // Push to A -- should cascade to B
    let features = engine
        .push_with_cascade(
            "A",
            &json!({
                "user_id": "u1"
            }),
            &store,
            now,
        )
        .unwrap();

    // Features from primary push (stream A)
    assert_eq!(features.get("a_count"), Some(&FeatureValue::Int(1)));

    // B should also have been updated
    let all = engine.get_features("u1", &store, now);
    assert_eq!(all.get("b_count"), Some(&FeatureValue::Int(1)));
}

#[ignore = "54-03 Task 4: legacy StateStore API / engine.push(&store, ...); Wave 4 re-enables after legacy-engine removal"]
#[test]
fn test_multiple_depends_on_sources() {
    // Stream C depends on both A and B
    let mut engine = PipelineEngine::new();
    let store = StateStore::new();
    let now = ts(1000);

    engine.register(make_keyless_stream("A")).unwrap();
    engine.register(make_keyless_stream("B")).unwrap();
    engine
        .register(make_keyed_dependent_stream("C", "user_id", vec!["A", "B"]))
        .unwrap();

    // Push to A -- should cascade to C
    let _ = engine
        .push_with_cascade(
            "A",
            &json!({
                "user_id": "u1"
            }),
            &store,
            now,
        )
        .unwrap();
    let all = engine.get_features("u1", &store, now);
    assert_eq!(all.get("count_1h"), Some(&FeatureValue::Int(1)));

    // Push to B -- should also cascade to C
    let _ = engine
        .push_with_cascade(
            "B",
            &json!({
                "user_id": "u1"
            }),
            &store,
            now,
        )
        .unwrap();
    let all = engine.get_features("u1", &store, now);
    assert_eq!(all.get("count_1h"), Some(&FeatureValue::Int(2)));
}

// ======================== FeatureValue Serialization Round-Trip ========================

#[ignore = "54-03 Task 4: legacy StateStore API / engine.push(&store, ...); Wave 4 re-enables after legacy-engine removal"]
#[test]
#[allow(clippy::approx_constant)]
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

// ======================== Phase 8 Plan 02: Backfill Integration Tests ========================

use beava::server::protocol::{convert_register_request, RegisterRequest};
use beava::server::tcp::{
    make_concurrent_state, run_backfill, BackfillStatus, BackfillTracker, SharedState,
};
use beava::state::event_log::EventLog;
use beava::state::snapshot::{load_snapshot, save_snapshot, SerializablePipeline, SnapshotState};
use std::collections::HashSet;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

/// Helper: create a SharedState with event log enabled in a temp dir.
fn make_state_with_event_log(log_dir: &std::path::Path) -> SharedState {
    let event_log = EventLog::new(log_dir.to_path_buf()).ok();
    make_concurrent_state(
        PipelineEngine::new(),
        event_log,
        log_dir.join("test.snapshot"),
        Arc::new(BackfillTracker::default()),
        true,
        true,
    )
}

/// Helper: push events to a stream via the engine, also writing to event log.
fn push_events(
    state: &SharedState,
    stream_name: &str,
    events: &[serde_json::Value],
    times: &[SystemTime],
) {
    for (event, &t) in events.iter().zip(times.iter()) {
        let engine = state.engine.read();
        // Phase 54-04 Pass A6a: `state.store` deleted — local scratch store
        // until Pass C migrates to shard-dispatch helper.
        let local_store = beava::state::store::StateStore::new();
        let _ = engine.push(stream_name, event, &local_store, t);
        drop(engine);
        if let Some(ref log) = state.event_log {
            let event_bytes = serde_json::to_vec(event).unwrap();
            let _ = log.append(stream_name, &event_bytes, t);
        }
    }
    // Flush event log
    if let Some(ref log) = state.event_log {
        let _ = log.fsync_all();
    }
}

/// Helper: wait for a backfill to complete (yield loop, max 200 iterations).
async fn wait_for_backfill_complete(state: &SharedState, stream_name: &str) {
    for _ in 0..200 {
        tokio::task::yield_now().await;
        let tasks = state.backfill_tracker.tasks.lock().unwrap();
        let all_done = tasks
            .iter()
            .filter(|t| t.stream == stream_name)
            .all(|t| t.completed_at.lock().unwrap().is_some());
        if all_done && !tasks.is_empty() {
            return;
        }
    }
    panic!(
        "Backfill for {} did not complete within 200 yield cycles",
        stream_name
    );
}

#[ignore = "54-03 Task 4: legacy StateStore API / engine.push(&store, ...); Wave 4 re-enables after legacy-engine removal"]
#[tokio::test(flavor = "current_thread")]
async fn test_backfill_replay_deterministic() {
    let tmp = tempfile::TempDir::new().unwrap();
    let state = make_state_with_event_log(tmp.path());

    // Register stream with count_1h
    let stream1 = StreamDefinition {
        name: "Transactions".into(),
        key_field: Some("user_id".into()),
        group_by_keys: None,
        features: vec![(
            "count_1h".into(),
            FeatureDef::Count {
                window: Duration::from_secs(3600),
                bucket: Duration::from_secs(60),
                where_expr: None,
                backfill: false,
            },
        )],
        depends_on: None,
        filter: None,
        entity_ttl: None,
        history_ttl: None,
        projection: None,
        ephemeral: None,
        pipeline_ttl: None,
        max_keys: None,
        watermark_lateness: None,
        shard_key: None,
    };
    {
        state.engine.write().register(stream1).unwrap();
        if let Some(ref log) = state.event_log {
            let _ = log.register_stream("Transactions", None);
        }
    }

    // Push 10 events for user "u1"
    let base_time = ts(60_000);
    let events: Vec<serde_json::Value> = (0..10)
        .map(|i| json!({"user_id": "u1", "amount": (i + 1) as f64 * 10.0}))
        .collect();
    let times: Vec<SystemTime> = (0..10)
        .map(|i| base_time + Duration::from_secs(i))
        .collect();
    push_events(&state, "Transactions", &events, &times);

    // Verify count_1h reads 10
    // Phase 54-04 Pass A6a: `state.store` deleted; test is `#[ignore]`'d for
    // Pass C test-harness migration. Local scratch store keeps compile green.
    {
        let engine = state.engine.read();
        let local_store = beava::state::store::StateStore::new();
        let features = engine.get_features("u1", &local_store, base_time + Duration::from_secs(9));
        assert_eq!(features.get("count_1h"), Some(&FeatureValue::Int(10)));
    }

    // Re-register with added sum_1h(backfill=true)
    let stream2 = StreamDefinition {
        name: "Transactions".into(),
        key_field: Some("user_id".into()),
        group_by_keys: None,
        features: vec![
            (
                "count_1h".into(),
                FeatureDef::Count {
                    window: Duration::from_secs(3600),
                    bucket: Duration::from_secs(60),
                    where_expr: None,
                    backfill: false,
                },
            ),
            (
                "sum_1h".into(),
                FeatureDef::Sum {
                    field: "amount".into(),
                    window: Duration::from_secs(3600),
                    bucket: Duration::from_secs(60),
                    optional: false,
                    where_expr: None,
                    backfill: true,
                },
            ),
        ],
        depends_on: None,
        filter: None,
        entity_ttl: None,
        history_ttl: None,
        projection: None,
        ephemeral: None,
        pipeline_ttl: None,
        max_keys: None,
        watermark_lateness: None,
        shard_key: None,
    };
    {
        let mut engine = state.engine.write();
        let diff = engine.register(stream2).unwrap();
        assert!(diff.backfilling.contains(&"sum_1h".to_string()));
        drop(engine);

        // Spawn backfill
        if let Some(ref log) = state.event_log {
            let _ = log.fsync_all();
        }
        let entries = state
            .event_log
            .as_ref()
            .map(|log| log.read_entries("Transactions").unwrap_or_default())
            .unwrap_or_default();
        assert_eq!(entries.len(), 10);

        let status = Arc::new(BackfillStatus {
            stream: "Transactions".into(),
            features: vec!["sum_1h".into()],
            total_events: entries.len(),
            processed_events: Arc::new(AtomicUsize::new(0)),
            started_at: SystemTime::now(),
            completed_at: std::sync::Mutex::new(None),
        });
        state
            .backfill_tracker
            .tasks
            .lock()
            .unwrap()
            .push(Arc::clone(&status));
        let state_clone = state.clone();
        tokio::spawn(run_backfill(
            state_clone,
            "Transactions".into(),
            vec!["sum_1h".into()],
            entries,
            status,
        ));
    }

    // Wait for backfill to complete
    wait_for_backfill_complete(&state, "Transactions").await;

    // Verify sum_1h equals sum of all 10 event amounts: 10+20+30+...+100 = 550
    // Phase 54-04 Pass A6a: state.store deleted; ignored test, compile-only.
    {
        let engine = state.engine.read();
        let local_store = beava::state::store::StateStore::new();
        let features = engine.get_features("u1", &local_store, base_time + Duration::from_secs(9));
        assert_eq!(features.get("sum_1h"), Some(&FeatureValue::Float(550.0)));
        // count_1h should still be 10
        assert_eq!(features.get("count_1h"), Some(&FeatureValue::Int(10)));
    }
}

#[ignore = "54-03 Task 4: legacy StateStore API / engine.push(&store, ...); Wave 4 re-enables after legacy-engine removal"]
#[tokio::test(flavor = "current_thread")]
async fn test_backfill_event_timestamps_not_wall_clock() {
    let tmp = tempfile::TempDir::new().unwrap();
    let state = make_state_with_event_log(tmp.path());

    // Register stream with count_1h
    let stream1 = StreamDefinition {
        name: "Txns".into(),
        key_field: Some("user_id".into()),
        group_by_keys: None,
        features: vec![(
            "count_1h".into(),
            FeatureDef::Count {
                window: Duration::from_secs(3600),
                bucket: Duration::from_secs(60),
                where_expr: None,
                backfill: false,
            },
        )],
        depends_on: None,
        filter: None,
        entity_ttl: None,
        history_ttl: None,
        projection: None,
        ephemeral: None,
        pipeline_ttl: None,
        max_keys: None,
        watermark_lateness: None,
        shard_key: None,
    };
    {
        state.engine.write().register(stream1).unwrap();
        if let Some(ref log) = state.event_log {
            let _ = log.register_stream("Txns", None);
        }
    }

    // Push 5 events at time T (UNIX_EPOCH + 3600)
    let t1 = ts(3600);
    let events1: Vec<serde_json::Value> = (0..5).map(|_| json!({"user_id": "u1"})).collect();
    let times1: Vec<SystemTime> = (0..5).map(|_| t1).collect();
    push_events(&state, "Txns", &events1, &times1);

    // Push 5 events at time T + 7200 (2 hours later)
    let t2 = ts(3600 + 7200);
    let events2: Vec<serde_json::Value> = (0..5).map(|_| json!({"user_id": "u1"})).collect();
    let times2: Vec<SystemTime> = (0..5).map(|_| t2).collect();
    push_events(&state, "Txns", &events2, &times2);

    // Re-register with count_30m(backfill=true)
    let stream2 = StreamDefinition {
        name: "Txns".into(),
        key_field: Some("user_id".into()),
        group_by_keys: None,
        features: vec![
            (
                "count_1h".into(),
                FeatureDef::Count {
                    window: Duration::from_secs(3600),
                    bucket: Duration::from_secs(60),
                    where_expr: None,
                    backfill: false,
                },
            ),
            (
                "count_30m".into(),
                FeatureDef::Count {
                    window: Duration::from_secs(1800),
                    bucket: Duration::from_secs(60),
                    where_expr: None,
                    backfill: true,
                },
            ),
        ],
        depends_on: None,
        filter: None,
        entity_ttl: None,
        history_ttl: None,
        projection: None,
        ephemeral: None,
        pipeline_ttl: None,
        max_keys: None,
        watermark_lateness: None,
        shard_key: None,
    };
    {
        let mut engine = state.engine.write();
        let diff = engine.register(stream2).unwrap();
        assert!(diff.backfilling.contains(&"count_30m".to_string()));
        drop(engine);

        if let Some(ref log) = state.event_log {
            let _ = log.fsync_all();
        }
        let entries = state
            .event_log
            .as_ref()
            .map(|log| log.read_entries("Txns").unwrap_or_default())
            .unwrap_or_default();
        assert_eq!(entries.len(), 10);

        let status = Arc::new(BackfillStatus {
            stream: "Txns".into(),
            features: vec!["count_30m".into()],
            total_events: entries.len(),
            processed_events: Arc::new(AtomicUsize::new(0)),
            started_at: SystemTime::now(),
            completed_at: std::sync::Mutex::new(None),
        });
        state
            .backfill_tracker
            .tasks
            .lock()
            .unwrap()
            .push(Arc::clone(&status));
        tokio::spawn(run_backfill(
            state.clone(),
            "Txns".into(),
            vec!["count_30m".into()],
            entries,
            status,
        ));
    }

    wait_for_backfill_complete(&state, "Txns").await;

    // Read count_30m at time T+7200 -- should be 5 (only the second batch within 30m window)
    // Phase 54-04 Pass A6a: state.store deleted; ignored test, compile-only.
    {
        let engine = state.engine.read();
        let local_store = beava::state::store::StateStore::new();
        let features = engine.get_features("u1", &local_store, t2);
        let count_30m = features.get("count_30m");
        assert_eq!(
            count_30m,
            Some(&FeatureValue::Int(5)),
            "count_30m should be 5 (only events within 30m window at T+7200), got {:?}",
            count_30m
        );
    }
}

#[ignore = "54-03 Task 4: legacy StateStore API / engine.push(&store, ...); Wave 4 re-enables after legacy-engine removal"]
#[test]
fn test_schema_evolution_add_remove() {
    let mut engine = PipelineEngine::new();
    let store = beava::state::store::StateStore::new();
    let now = ts(60_000);

    // Register stream with [count_1h, sum_1h]
    let stream1 = StreamDefinition {
        name: "Txns".into(),
        key_field: Some("user_id".into()),
        group_by_keys: None,
        features: vec![
            (
                "count_1h".into(),
                FeatureDef::Count {
                    window: Duration::from_secs(3600),
                    bucket: Duration::from_secs(60),
                    where_expr: None,
                    backfill: false,
                },
            ),
            (
                "sum_1h".into(),
                FeatureDef::Sum {
                    field: "amount".into(),
                    window: Duration::from_secs(3600),
                    bucket: Duration::from_secs(60),
                    optional: false,
                    where_expr: None,
                    backfill: false,
                },
            ),
        ],
        depends_on: None,
        filter: None,
        entity_ttl: None,
        history_ttl: None,
        projection: None,
        ephemeral: None,
        pipeline_ttl: None,
        max_keys: None,
        watermark_lateness: None,
        shard_key: None,
    };
    engine.register(stream1).unwrap();

    // Push 5 events
    for i in 0..5 {
        engine
            .push(
                "Txns",
                &json!({"user_id": "u1", "amount": (i + 1) as f64 * 10.0}),
                &store,
                now,
            )
            .unwrap();
    }

    // Verify
    let features = store.get_all_features("u1", now);
    assert_eq!(features.get("count_1h"), Some(&FeatureValue::Int(5)));
    assert_eq!(features.get("sum_1h"), Some(&FeatureValue::Float(150.0)));

    // Re-register with [count_1h, avg_1h] (removing sum_1h, adding avg_1h)
    let stream2 = StreamDefinition {
        name: "Txns".into(),
        key_field: Some("user_id".into()),
        group_by_keys: None,
        features: vec![
            (
                "count_1h".into(),
                FeatureDef::Count {
                    window: Duration::from_secs(3600),
                    bucket: Duration::from_secs(60),
                    where_expr: None,
                    backfill: false,
                },
            ),
            (
                "avg_1h".into(),
                FeatureDef::Avg {
                    field: "amount".into(),
                    window: Duration::from_secs(3600),
                    bucket: Duration::from_secs(60),
                    optional: false,
                    where_expr: None,
                    backfill: false,
                },
            ),
        ],
        depends_on: None,
        filter: None,
        entity_ttl: None,
        history_ttl: None,
        projection: None,
        ephemeral: None,
        pipeline_ttl: None,
        max_keys: None,
        watermark_lateness: None,
        shard_key: None,
    };
    let diff = engine.register(stream2).unwrap();
    assert!(diff.removed.contains(&"sum_1h".to_string()));
    assert!(diff.added.contains(&"avg_1h".to_string()));
    assert!(diff.unchanged.contains(&"count_1h".to_string()));

    // Push 3 more events
    for i in 0..3 {
        engine
            .push(
                "Txns",
                &json!({"user_id": "u1", "amount": (i + 1) as f64 * 5.0}),
                &store,
                now,
            )
            .unwrap();
    }

    // Verify count_1h=8 (preserved, continued counting)
    let features = engine.get_features("u1", &store, now);
    assert_eq!(features.get("count_1h"), Some(&FeatureValue::Int(8)));
    // avg_1h should have correct value (only 3 events since it was added)
    assert_eq!(features.get("avg_1h"), Some(&FeatureValue::Float(10.0))); // (5+10+15)/3 = 10
}

#[ignore = "54-03 Task 4: legacy StateStore API / engine.push(&store, ...); Wave 4 re-enables after legacy-engine removal"]
#[tokio::test(flavor = "current_thread")]
async fn test_backfill_idempotent_restart() {
    let tmp = tempfile::TempDir::new().unwrap();
    let state = make_state_with_event_log(tmp.path());

    // Register stream with count_1h
    let stream1 = StreamDefinition {
        name: "Txns".into(),
        key_field: Some("user_id".into()),
        group_by_keys: None,
        features: vec![(
            "count_1h".into(),
            FeatureDef::Count {
                window: Duration::from_secs(3600),
                bucket: Duration::from_secs(60),
                where_expr: None,
                backfill: false,
            },
        )],
        depends_on: None,
        filter: None,
        entity_ttl: None,
        history_ttl: None,
        projection: None,
        ephemeral: None,
        pipeline_ttl: None,
        max_keys: None,
        watermark_lateness: None,
        shard_key: None,
    };
    {
        state.engine.write().register(stream1).unwrap();
        if let Some(ref log) = state.event_log {
            let _ = log.register_stream("Txns", None);
        }
    }

    // Push 10 events
    let base_time = ts(60_000);
    let events: Vec<serde_json::Value> = (0..10)
        .map(|i| json!({"user_id": "u1", "amount": (i + 1) as f64 * 10.0}))
        .collect();
    let times: Vec<SystemTime> = (0..10)
        .map(|i| base_time + Duration::from_secs(i))
        .collect();
    push_events(&state, "Txns", &events, &times);

    // Re-register with sum_1h(backfill=true)
    let raw_register_json = serde_json::json!({
        "name": "Txns",
        "key_field": "user_id",
        "features": [
            {"name": "count_1h", "type": "count", "window": "1h"},
            {"name": "sum_1h", "type": "sum", "field": "amount", "window": "1h", "backfill": true}
        ]
    });

    let stream2 = StreamDefinition {
        name: "Txns".into(),
        key_field: Some("user_id".into()),
        group_by_keys: None,
        features: vec![
            (
                "count_1h".into(),
                FeatureDef::Count {
                    window: Duration::from_secs(3600),
                    bucket: Duration::from_secs(60),
                    where_expr: None,
                    backfill: false,
                },
            ),
            (
                "sum_1h".into(),
                FeatureDef::Sum {
                    field: "amount".into(),
                    window: Duration::from_secs(3600),
                    bucket: Duration::from_secs(60),
                    optional: false,
                    where_expr: None,
                    backfill: true,
                },
            ),
        ],
        depends_on: None,
        filter: None,
        entity_ttl: None,
        history_ttl: None,
        projection: None,
        ephemeral: None,
        pipeline_ttl: None,
        max_keys: None,
        watermark_lateness: None,
        shard_key: None,
    };

    {
        let mut engine = state.engine.write();
        let diff = engine.register(stream2).unwrap();
        engine.store_raw_register_json("Txns", raw_register_json.clone());
        assert!(diff.backfilling.contains(&"sum_1h".to_string()));
        drop(engine);

        if let Some(ref log) = state.event_log {
            let _ = log.fsync_all();
        }
        let entries = state
            .event_log
            .as_ref()
            .map(|log| log.read_entries("Txns").unwrap_or_default())
            .unwrap_or_default();

        let status = Arc::new(BackfillStatus {
            stream: "Txns".into(),
            features: vec!["sum_1h".into()],
            total_events: entries.len(),
            processed_events: Arc::new(AtomicUsize::new(0)),
            started_at: SystemTime::now(),
            completed_at: std::sync::Mutex::new(None),
        });
        state
            .backfill_tracker
            .tasks
            .lock()
            .unwrap()
            .push(Arc::clone(&status));
        tokio::spawn(run_backfill(
            state.clone(),
            "Txns".into(),
            vec!["sum_1h".into()],
            entries,
            status,
        ));
    }

    wait_for_backfill_complete(&state, "Txns").await;

    // Step 6: Verify backfill_complete contains ("Txns", "sum_1h")
    {
        let bc = state.backfill_complete.lock();
        assert!(
            bc.contains(&("Txns".to_string(), "sum_1h".to_string())),
            "backfill_complete should contain (Txns, sum_1h)"
        );
    }

    // Step 7: Save snapshot with backfill_complete included
    // Phase 54-04 Pass A6a: state.store deleted; ignored test, compile-only.
    let snapshot_bytes = {
        let engine = state.engine.read();
        let store = beava::state::store::StateStore::new();
        let valid_features = engine.valid_features_map();
        let entities = store.clone_for_snapshot_with_gc(&valid_features);
        let pipelines = vec![SerializablePipeline {
            name: "Txns".to_string(),
            key_field: "user_id".to_string(),
            raw_register_json: serde_json::to_string(&raw_register_json).unwrap(),
        }];
        let backfill_complete: Vec<(String, String)> =
            state.backfill_complete.lock().iter().cloned().collect();
        let snap = SnapshotState {
            entities,
            pipelines,
            backfill_complete,
        };
        save_snapshot(&snap).unwrap()
    };

    // Step 8-12: Simulate restart with backfill_complete intact
    // The key assertion: sum_1h IS in backfill_complete, so no backfill needed
    {
        let restored = load_snapshot(&snapshot_bytes).unwrap();
        let mut restored_complete: HashSet<(String, String)> = HashSet::new();
        for (s, f) in &restored.backfill_complete {
            restored_complete.insert((s.clone(), f.clone()));
        }

        // Re-register pipeline from snapshot
        let mut engine2 = PipelineEngine::new();
        for pipeline in &restored.pipelines {
            let parsed: serde_json::Value =
                serde_json::from_str(&pipeline.raw_register_json).unwrap();
            let req: RegisterRequest = serde_json::from_value(parsed).unwrap();
            let stream_def = convert_register_request(req).unwrap();
            engine2.register(stream_def).unwrap();
        }

        // Check incomplete backfills
        let mut incomplete: Vec<(String, Vec<String>)> = Vec::new();
        for stream in engine2.list_streams() {
            let missing: Vec<String> = stream
                .features
                .iter()
                .filter(|(_, def)| beava::engine::pipeline::get_backfill_flag(def))
                .filter(|(name, _)| {
                    !restored_complete.contains(&(stream.name.clone(), name.clone()))
                })
                .map(|(name, _)| name.clone())
                .collect();
            if !missing.is_empty() {
                incomplete.push((stream.name.clone(), missing));
            }
        }
        // sum_1h should NOT be in incomplete (it's completed)
        assert!(
            incomplete.is_empty(),
            "No incomplete backfills expected after successful run, got {:?}",
            incomplete
        );
    }

    // Step 13-17: Simulate crash (clear backfill_complete)
    {
        let restored = load_snapshot(&snapshot_bytes).unwrap();
        // Simulate crash: empty backfill_complete (as if marker wasn't written)
        let restored_complete: HashSet<(String, String)> = HashSet::new();

        let mut engine3 = PipelineEngine::new();
        for pipeline in &restored.pipelines {
            let parsed: serde_json::Value =
                serde_json::from_str(&pipeline.raw_register_json).unwrap();
            let req: RegisterRequest = serde_json::from_value(parsed).unwrap();
            let stream_def = convert_register_request(req).unwrap();
            engine3.register(stream_def).unwrap();
        }

        // Detect incomplete backfills (should find sum_1h since backfill_complete is empty)
        let mut incomplete: Vec<(String, Vec<String>)> = Vec::new();
        for stream in engine3.list_streams() {
            let missing: Vec<String> = stream
                .features
                .iter()
                .filter(|(_, def)| beava::engine::pipeline::get_backfill_flag(def))
                .filter(|(name, _)| {
                    !restored_complete.contains(&(stream.name.clone(), name.clone()))
                })
                .map(|(name, _)| name.clone())
                .collect();
            if !missing.is_empty() {
                incomplete.push((stream.name.clone(), missing));
            }
        }
        assert!(
            !incomplete.is_empty(),
            "Should detect incomplete backfill for sum_1h after simulated crash"
        );
        let (stream_name, features) = &incomplete[0];
        assert_eq!(stream_name, "Txns");
        assert!(features.contains(&"sum_1h".to_string()));

        // Re-run backfill and verify deterministic result
        // Phase 54-04 Pass A6a: state.store deleted; ignored test, compile-only.
        let state2 = make_state_with_event_log(tmp.path());
        {
            let legacy_store = beava::state::store::StateStore::new();
            legacy_store.restore_from_snapshot(restored.entities);
            let mut engine2w = state2.engine.write();
            for pipeline in &restored.pipelines {
                let parsed: serde_json::Value =
                    serde_json::from_str(&pipeline.raw_register_json).unwrap();
                let req: RegisterRequest = serde_json::from_value(parsed).unwrap();
                let stream_def = convert_register_request(req).unwrap();
                engine2w.register(stream_def).unwrap();
            }
            drop(engine2w);
            if let Some(ref log) = state2.event_log {
                let _ = log.register_stream("Txns", None);
            }
        }

        // Read entries and spawn backfill
        let entries = state2
            .event_log
            .as_ref()
            .map(|log| log.read_entries("Txns").unwrap_or_default())
            .unwrap_or_default();
        assert!(!entries.is_empty());

        let status = Arc::new(BackfillStatus {
            stream: "Txns".into(),
            features: vec!["sum_1h".into()],
            total_events: entries.len(),
            processed_events: Arc::new(AtomicUsize::new(0)),
            started_at: SystemTime::now(),
            completed_at: std::sync::Mutex::new(None),
        });
        state2
            .backfill_tracker
            .tasks
            .lock()
            .unwrap()
            .push(Arc::clone(&status));
        tokio::spawn(run_backfill(
            state2.clone(),
            "Txns".into(),
            vec!["sum_1h".into()],
            entries,
            status,
        ));

        wait_for_backfill_complete(&state2, "Txns").await;

        // Verify same deterministic result: sum should be 550
        // Phase 54-04 Pass A6a: state.store deleted; ignored test, compile-only.
        let engine2r = state2.engine.read();
        let store2 = beava::state::store::StateStore::new();
        let features = engine2r.get_features("u1", &store2, base_time + Duration::from_secs(9));
        assert_eq!(
            features.get("sum_1h"),
            Some(&FeatureValue::Float(550.0)),
            "Re-run backfill should produce same deterministic result"
        );
    }
}

// ======================== Phase 17 Plan 03: Enriched Event Propagation Tests ========================

/// Test 1: Upstream derive result is visible to downstream operator.
/// Pipeline: RawTxns (keyless) -> CurrencyNorm (keyed, derive amount_usd) -> UserStats (keyed, sum of amount_usd)
#[ignore = "54-03 Task 4: legacy StateStore API / engine.push(&store, ...); Wave 4 re-enables after legacy-engine removal"]
#[test]
fn test_enriched_derive_to_downstream_sum() {
    let mut engine = PipelineEngine::new();
    let store = StateStore::new();
    let now = ts(60_000);

    // Stage 1: RawTxns (keyless source)
    engine
        .register(StreamDefinition {
            name: "RawTxns".into(),
            key_field: None,
            group_by_keys: None,
            features: vec![],
            depends_on: None,
            filter: None,
            entity_ttl: None,
            history_ttl: None,
            projection: None,
            ephemeral: None,
            pipeline_ttl: None,
            max_keys: None,
            watermark_lateness: None,
            shard_key: None,
        })
        .unwrap();

    // Stage 2: CurrencyNorm (keyed, depends on RawTxns, derives amount_usd)
    engine
        .register(StreamDefinition {
            name: "CurrencyNorm".into(),
            key_field: Some("user_id".into()),
            group_by_keys: None,
            features: vec![(
                "amount_usd".into(),
                FeatureDef::Derive {
                    expr: parse_expr("_event.amount * _event.exchange_rate").unwrap(),
                },
            )],
            depends_on: Some(vec!["RawTxns".into()]),
            filter: None,
            entity_ttl: None,
            history_ttl: None,
            projection: None,
            ephemeral: None,
            pipeline_ttl: None,
            max_keys: None,
            watermark_lateness: None,
            shard_key: None,
        })
        .unwrap();

    // Stage 3: UserStats (keyed, depends on CurrencyNorm, sums CurrencyNorm.amount_usd)
    engine
        .register(StreamDefinition {
            name: "UserStats".into(),
            key_field: Some("user_id".into()),
            group_by_keys: None,
            features: vec![(
                "total_usd_1h".into(),
                FeatureDef::Sum {
                    field: "CurrencyNorm.amount_usd".into(),
                    window: Duration::from_secs(3600),
                    bucket: Duration::from_secs(60),
                    optional: false,
                    where_expr: None,
                    backfill: false,
                },
            )],
            depends_on: Some(vec!["CurrencyNorm".into()]),
            filter: None,
            entity_ttl: None,
            history_ttl: None,
            projection: None,
            ephemeral: None,
            pipeline_ttl: None,
            max_keys: None,
            watermark_lateness: None,
            shard_key: None,
        })
        .unwrap();

    // Push event: amount=100, exchange_rate=1.2 -> amount_usd = 120.0
    let _ = engine
        .push_with_cascade(
            "RawTxns",
            &json!({
                "user_id": "u123",
                "amount": 100.0,
                "exchange_rate": 1.2
            }),
            &store,
            now,
        )
        .unwrap();

    // Verify downstream UserStats sees enriched amount_usd
    let all = engine.get_features("u123", &store, now);
    assert_eq!(
        all.get("total_usd_1h"),
        Some(&FeatureValue::Float(120.0)),
        "UserStats.total_usd_1h should be 120.0 (100 * 1.2), got {:?}",
        all.get("total_usd_1h")
    );
}

/// Test 2: Multi-level cascade enrichment (4 hops: A -> B -> C -> D)
#[ignore = "54-03 Task 4: legacy StateStore API / engine.push(&store, ...); Wave 4 re-enables after legacy-engine removal"]
#[test]
fn test_enriched_multi_hop_cascade() {
    let mut engine = PipelineEngine::new();
    let store = StateStore::new();
    let now = ts(60_000);

    // A: keyless source
    engine
        .register(StreamDefinition {
            name: "HopA".into(),
            key_field: None,
            group_by_keys: None,
            features: vec![],
            depends_on: None,
            filter: None,
            entity_ttl: None,
            history_ttl: None,
            projection: None,
            ephemeral: None,
            pipeline_ttl: None,
            max_keys: None,
            watermark_lateness: None,
            shard_key: None,
        })
        .unwrap();

    // B: keyed, depends on A, derives computed_b = _event.raw_value * 2
    engine
        .register(StreamDefinition {
            name: "HopB".into(),
            key_field: Some("user_id".into()),
            group_by_keys: None,
            features: vec![(
                "computed_b".into(),
                FeatureDef::Derive {
                    expr: parse_expr("_event.raw_value * 2").unwrap(),
                },
            )],
            depends_on: Some(vec!["HopA".into()]),
            filter: None,
            entity_ttl: None,
            history_ttl: None,
            projection: None,
            ephemeral: None,
            pipeline_ttl: None,
            max_keys: None,
            watermark_lateness: None,
            shard_key: None,
        })
        .unwrap();

    // C: keyed, depends on B, sums B.computed_b and derives computed_c = B.computed_b + 10
    engine
        .register(StreamDefinition {
            name: "HopC".into(),
            key_field: Some("user_id".into()),
            group_by_keys: None,
            features: vec![
                (
                    "sum_b".into(),
                    FeatureDef::Sum {
                        field: "HopB.computed_b".into(),
                        window: Duration::from_secs(3600),
                        bucket: Duration::from_secs(60),
                        optional: false,
                        where_expr: None,
                        backfill: false,
                    },
                ),
                (
                    "computed_c".into(),
                    FeatureDef::Derive {
                        expr: parse_expr("HopB.computed_b + 10").unwrap(),
                    },
                ),
            ],
            depends_on: Some(vec!["HopB".into()]),
            filter: None,
            entity_ttl: None,
            history_ttl: None,
            projection: None,
            ephemeral: None,
            pipeline_ttl: None,
            max_keys: None,
            watermark_lateness: None,
            shard_key: None,
        })
        .unwrap();

    // D: keyed, depends on C, sums C.computed_c
    engine
        .register(StreamDefinition {
            name: "HopD".into(),
            key_field: Some("user_id".into()),
            group_by_keys: None,
            features: vec![(
                "sum_c".into(),
                FeatureDef::Sum {
                    field: "HopC.computed_c".into(),
                    window: Duration::from_secs(3600),
                    bucket: Duration::from_secs(60),
                    optional: false,
                    where_expr: None,
                    backfill: false,
                },
            )],
            depends_on: Some(vec!["HopC".into()]),
            filter: None,
            entity_ttl: None,
            history_ttl: None,
            projection: None,
            ephemeral: None,
            pipeline_ttl: None,
            max_keys: None,
            watermark_lateness: None,
            shard_key: None,
        })
        .unwrap();

    // Push event: raw_value=5
    // B.computed_b = 5*2 = 10 (derive, only available during push via enrichment)
    // C.sum_b = 10 (aggregated from enrichment B.computed_b)
    // C.computed_c = B.computed_b + 10 = 20 (derive using enrichment)
    // D.sum_c = 20 (aggregated from enrichment C.computed_c)
    let _ = engine
        .push_with_cascade(
            "HopA",
            &json!({
                "user_id": "u1",
                "raw_value": 5.0
            }),
            &store,
            now,
        )
        .unwrap();

    let all = engine.get_features("u1", &store, now);
    // Derives are computed on read and need _event context which isn't stored,
    // so we verify the aggregated values that prove enrichment propagated correctly.
    assert_eq!(
        all.get("sum_b"),
        Some(&FeatureValue::Float(10.0)),
        "HopC.sum_b should be 10.0 (B.computed_b=10 propagated via enrichment)"
    );
    assert_eq!(
        all.get("sum_c"),
        Some(&FeatureValue::Float(20.0)),
        "HopD.sum_c should be 20.0 (C.computed_c=20 propagated via enrichment)"
    );
}

/// Test 3: Enrichment works in async mode (push_with_cascade_no_features)
#[ignore = "54-03 Task 4: legacy StateStore API / engine.push(&store, ...); Wave 4 re-enables after legacy-engine removal"]
#[test]
fn test_enriched_cascade_async_mode() {
    let mut engine = PipelineEngine::new();
    let store = StateStore::new();
    let now = ts(60_000);

    // Same 3-stream pipeline as test 1
    engine
        .register(StreamDefinition {
            name: "AsyncRaw".into(),
            key_field: None,
            group_by_keys: None,
            features: vec![],
            depends_on: None,
            filter: None,
            entity_ttl: None,
            history_ttl: None,
            projection: None,
            ephemeral: None,
            pipeline_ttl: None,
            max_keys: None,
            watermark_lateness: None,
            shard_key: None,
        })
        .unwrap();

    engine
        .register(StreamDefinition {
            name: "AsyncNorm".into(),
            key_field: Some("user_id".into()),
            group_by_keys: None,
            features: vec![(
                "amount_usd".into(),
                FeatureDef::Derive {
                    expr: parse_expr("_event.amount * _event.rate").unwrap(),
                },
            )],
            depends_on: Some(vec!["AsyncRaw".into()]),
            filter: None,
            entity_ttl: None,
            history_ttl: None,
            projection: None,
            ephemeral: None,
            pipeline_ttl: None,
            max_keys: None,
            watermark_lateness: None,
            shard_key: None,
        })
        .unwrap();

    engine
        .register(StreamDefinition {
            name: "AsyncStats".into(),
            key_field: Some("user_id".into()),
            group_by_keys: None,
            features: vec![(
                "total_usd_1h".into(),
                FeatureDef::Sum {
                    field: "AsyncNorm.amount_usd".into(),
                    window: Duration::from_secs(3600),
                    bucket: Duration::from_secs(60),
                    optional: false,
                    where_expr: None,
                    backfill: false,
                },
            )],
            depends_on: Some(vec!["AsyncNorm".into()]),
            filter: None,
            entity_ttl: None,
            history_ttl: None,
            projection: None,
            ephemeral: None,
            pipeline_ttl: None,
            max_keys: None,
            watermark_lateness: None,
            shard_key: None,
        })
        .unwrap();

    // Async push (no features returned)
    let result = engine
        .push_with_cascade_no_features(
            "AsyncRaw",
            &json!({
                "user_id": "u1", "amount": 100.0, "rate": 1.5
            }),
            &store,
            now,
        )
        .unwrap();
    assert!(
        result.is_empty(),
        "async push should return empty FeatureMap"
    );

    // Operators were still updated -- verify via sync push
    let _ = engine
        .push_with_cascade(
            "AsyncRaw",
            &json!({
                "user_id": "u1", "amount": 200.0, "rate": 2.0
            }),
            &store,
            now,
        )
        .unwrap();

    let all = engine.get_features("u1", &store, now);
    // First push: 100*1.5=150, second push: 200*2.0=400, total=550
    assert_eq!(
        all.get("total_usd_1h"),
        Some(&FeatureValue::Float(550.0)),
        "Async push should have updated operators; total should be 550.0, got {:?}",
        all.get("total_usd_1h")
    );
}

/// Test 4: Where-clause can reference enriched upstream fields
#[ignore = "54-03 Task 4: legacy StateStore API / engine.push(&store, ...); Wave 4 re-enables after legacy-engine removal"]
#[test]
fn test_enriched_where_clause() {
    let mut engine = PipelineEngine::new();
    let store = StateStore::new();
    let now = ts(60_000);

    engine
        .register(StreamDefinition {
            name: "WhereRaw".into(),
            key_field: None,
            group_by_keys: None,
            features: vec![],
            depends_on: None,
            filter: None,
            entity_ttl: None,
            history_ttl: None,
            projection: None,
            ephemeral: None,
            pipeline_ttl: None,
            max_keys: None,
            watermark_lateness: None,
            shard_key: None,
        })
        .unwrap();

    engine
        .register(StreamDefinition {
            name: "WhereNorm".into(),
            key_field: Some("user_id".into()),
            group_by_keys: None,
            features: vec![(
                "amount_usd".into(),
                FeatureDef::Derive {
                    expr: parse_expr("_event.amount * _event.exchange_rate").unwrap(),
                },
            )],
            depends_on: Some(vec!["WhereRaw".into()]),
            filter: None,
            entity_ttl: None,
            history_ttl: None,
            projection: None,
            ephemeral: None,
            pipeline_ttl: None,
            max_keys: None,
            watermark_lateness: None,
            shard_key: None,
        })
        .unwrap();

    // Downstream with where clause referencing enriched field
    engine
        .register(StreamDefinition {
            name: "WhereFiltered".into(),
            key_field: Some("user_id".into()),
            group_by_keys: None,
            features: vec![(
                "high_value_count".into(),
                FeatureDef::Count {
                    window: Duration::from_secs(3600),
                    bucket: Duration::from_secs(60),
                    where_expr: Some(parse_expr("WhereNorm.amount_usd > 50").unwrap()),
                    backfill: false,
                },
            )],
            depends_on: Some(vec!["WhereNorm".into()]),
            filter: None,
            entity_ttl: None,
            history_ttl: None,
            projection: None,
            ephemeral: None,
            pipeline_ttl: None,
            max_keys: None,
            watermark_lateness: None,
            shard_key: None,
        })
        .unwrap();

    // Push event where amount_usd = 100*1.2 = 120 > 50 -> should increment
    let _ = engine
        .push_with_cascade(
            "WhereRaw",
            &json!({
                "user_id": "u1", "amount": 100.0, "exchange_rate": 1.2
            }),
            &store,
            now,
        )
        .unwrap();

    let all = engine.get_features("u1", &store, now);
    assert_eq!(
        all.get("high_value_count"),
        Some(&FeatureValue::Int(1)),
        "high_value_count should be 1 (amount_usd 120 > 50)"
    );

    // Push event where amount_usd = 10*1.2 = 12 < 50 -> should NOT increment
    let _ = engine
        .push_with_cascade(
            "WhereRaw",
            &json!({
                "user_id": "u1", "amount": 10.0, "exchange_rate": 1.2
            }),
            &store,
            now,
        )
        .unwrap();

    let all2 = engine.get_features("u1", &store, now);
    assert_eq!(
        all2.get("high_value_count"),
        Some(&FeatureValue::Int(1)),
        "high_value_count should still be 1 (amount_usd 12 < 50)"
    );
}

/// Test 5: Qualified field resolution (sum("CurrencyNorm.amount_usd"))
#[ignore = "54-03 Task 4: legacy StateStore API / engine.push(&store, ...); Wave 4 re-enables after legacy-engine removal"]
#[test]
fn test_enriched_field_resolution_qualified() {
    let mut engine = PipelineEngine::new();
    let store = StateStore::new();
    let now = ts(60_000);

    engine
        .register(StreamDefinition {
            name: "QualRaw".into(),
            key_field: None,
            group_by_keys: None,
            features: vec![],
            depends_on: None,
            filter: None,
            entity_ttl: None,
            history_ttl: None,
            projection: None,
            ephemeral: None,
            pipeline_ttl: None,
            max_keys: None,
            watermark_lateness: None,
            shard_key: None,
        })
        .unwrap();

    engine
        .register(StreamDefinition {
            name: "QualNorm".into(),
            key_field: Some("user_id".into()),
            group_by_keys: None,
            features: vec![(
                "val".into(),
                FeatureDef::Derive {
                    expr: parse_expr("_event.x * 3").unwrap(),
                },
            )],
            depends_on: Some(vec!["QualRaw".into()]),
            filter: None,
            entity_ttl: None,
            history_ttl: None,
            projection: None,
            ephemeral: None,
            pipeline_ttl: None,
            max_keys: None,
            watermark_lateness: None,
            shard_key: None,
        })
        .unwrap();

    engine
        .register(StreamDefinition {
            name: "QualAgg".into(),
            key_field: Some("user_id".into()),
            group_by_keys: None,
            features: vec![(
                "total".into(),
                FeatureDef::Sum {
                    field: "QualNorm.val".into(),
                    window: Duration::from_secs(3600),
                    bucket: Duration::from_secs(60),
                    optional: false,
                    where_expr: None,
                    backfill: false,
                },
            )],
            depends_on: Some(vec!["QualNorm".into()]),
            filter: None,
            entity_ttl: None,
            history_ttl: None,
            projection: None,
            ephemeral: None,
            pipeline_ttl: None,
            max_keys: None,
            watermark_lateness: None,
            shard_key: None,
        })
        .unwrap();

    let _ = engine
        .push_with_cascade(
            "QualRaw",
            &json!({
                "user_id": "u1", "x": 10.0
            }),
            &store,
            now,
        )
        .unwrap();

    let all = engine.get_features("u1", &store, now);
    assert_eq!(
        all.get("total"),
        Some(&FeatureValue::Float(30.0)),
        "Qualified field QualNorm.val should resolve to 30.0 (10*3)"
    );
}

/// Test 6: Unqualified field resolution (sum("val") resolves from enrichment)
#[ignore = "54-03 Task 4: legacy StateStore API / engine.push(&store, ...); Wave 4 re-enables after legacy-engine removal"]
#[test]
fn test_enriched_field_resolution_unqualified() {
    let mut engine = PipelineEngine::new();
    let store = StateStore::new();
    let now = ts(60_000);

    engine
        .register(StreamDefinition {
            name: "UnqualRaw".into(),
            key_field: None,
            group_by_keys: None,
            features: vec![],
            depends_on: None,
            filter: None,
            entity_ttl: None,
            history_ttl: None,
            projection: None,
            ephemeral: None,
            pipeline_ttl: None,
            max_keys: None,
            watermark_lateness: None,
            shard_key: None,
        })
        .unwrap();

    engine
        .register(StreamDefinition {
            name: "UnqualNorm".into(),
            key_field: Some("user_id".into()),
            group_by_keys: None,
            features: vec![(
                "uval".into(),
                FeatureDef::Derive {
                    expr: parse_expr("_event.x * 5").unwrap(),
                },
            )],
            depends_on: Some(vec!["UnqualRaw".into()]),
            filter: None,
            entity_ttl: None,
            history_ttl: None,
            projection: None,
            ephemeral: None,
            pipeline_ttl: None,
            max_keys: None,
            watermark_lateness: None,
            shard_key: None,
        })
        .unwrap();

    // Use unqualified field name "uval" (not "UnqualNorm.uval")
    engine
        .register(StreamDefinition {
            name: "UnqualAgg".into(),
            key_field: Some("user_id".into()),
            group_by_keys: None,
            features: vec![(
                "total".into(),
                FeatureDef::Sum {
                    field: "uval".into(),
                    window: Duration::from_secs(3600),
                    bucket: Duration::from_secs(60),
                    optional: false,
                    where_expr: None,
                    backfill: false,
                },
            )],
            depends_on: Some(vec!["UnqualNorm".into()]),
            filter: None,
            entity_ttl: None,
            history_ttl: None,
            projection: None,
            ephemeral: None,
            pipeline_ttl: None,
            max_keys: None,
            watermark_lateness: None,
            shard_key: None,
        })
        .unwrap();

    let _ = engine
        .push_with_cascade(
            "UnqualRaw",
            &json!({
                "user_id": "u1", "x": 4.0
            }),
            &store,
            now,
        )
        .unwrap();

    let all = engine.get_features("u1", &store, now);
    assert_eq!(
        all.get("total"),
        Some(&FeatureValue::Float(20.0)),
        "Unqualified field 'uval' should resolve from enrichment to 20.0 (4*5)"
    );
}

/// Test 7: Single stream (no downstream) behaves identically to pre-enrichment
#[ignore = "54-03 Task 4: legacy StateStore API / engine.push(&store, ...); Wave 4 re-enables after legacy-engine removal"]
#[test]
fn test_enriched_no_cascade_unchanged() {
    let mut engine = PipelineEngine::new();
    let store = StateStore::new();
    let now = ts(60_000);

    engine
        .register(StreamDefinition {
            name: "Solo".into(),
            key_field: Some("user_id".into()),
            group_by_keys: None,
            features: vec![
                (
                    "count_1h".into(),
                    FeatureDef::Count {
                        window: Duration::from_secs(3600),
                        bucket: Duration::from_secs(60),
                        where_expr: None,
                        backfill: false,
                    },
                ),
                (
                    "sum_1h".into(),
                    FeatureDef::Sum {
                        field: "amount".into(),
                        window: Duration::from_secs(3600),
                        bucket: Duration::from_secs(60),
                        optional: false,
                        where_expr: None,
                        backfill: false,
                    },
                ),
                (
                    "ratio".into(),
                    FeatureDef::Derive {
                        expr: parse_expr("sum_1h / count_1h").unwrap(),
                    },
                ),
            ],
            depends_on: None,
            filter: None,
            entity_ttl: None,
            history_ttl: None,
            projection: None,
            ephemeral: None,
            pipeline_ttl: None,
            max_keys: None,
            watermark_lateness: None,
            shard_key: None,
        })
        .unwrap();

    // Use push_with_cascade on a single-stream (no downstream)
    let features = engine
        .push_with_cascade(
            "Solo",
            &json!({
                "user_id": "u1", "amount": 30.0
            }),
            &store,
            now,
        )
        .unwrap();

    assert_eq!(features.get("count_1h"), Some(&FeatureValue::Int(1)));
    assert_eq!(features.get("sum_1h"), Some(&FeatureValue::Float(30.0)));
    assert_eq!(features.get("ratio"), Some(&FeatureValue::Float(30.0)));

    // Push second event
    let features2 = engine
        .push_with_cascade(
            "Solo",
            &json!({
                "user_id": "u1", "amount": 70.0
            }),
            &store,
            now,
        )
        .unwrap();

    assert_eq!(features2.get("count_1h"), Some(&FeatureValue::Int(2)));
    assert_eq!(features2.get("sum_1h"), Some(&FeatureValue::Float(100.0)));
    assert_eq!(features2.get("ratio"), Some(&FeatureValue::Float(50.0)));
}

// ======================== Phase 18 Plan 01: Projection Integration Tests ========================

#[ignore = "54-03 Task 4: legacy StateStore API / engine.push(&store, ...); Wave 4 re-enables after legacy-engine removal"]
#[test]
fn test_projection_select_push() {
    let mut engine = PipelineEngine::new();
    let store = StateStore::new();
    let now = ts(60_000);

    engine
        .register(StreamDefinition {
            name: "Txns".into(),
            key_field: Some("user_id".into()),
            group_by_keys: None,
            features: vec![
                (
                    "count_1h".into(),
                    FeatureDef::Count {
                        window: Duration::from_secs(3600),
                        bucket: Duration::from_secs(60),
                        where_expr: None,
                        backfill: false,
                    },
                ),
                (
                    "sum_1h".into(),
                    FeatureDef::Sum {
                        field: "amount".into(),
                        window: Duration::from_secs(3600),
                        bucket: Duration::from_secs(60),
                        optional: false,
                        where_expr: None,
                        backfill: false,
                    },
                ),
                (
                    "internal_count_24h".into(),
                    FeatureDef::Count {
                        window: Duration::from_secs(86400),
                        bucket: Duration::from_secs(600),
                        where_expr: None,
                        backfill: false,
                    },
                ),
            ],
            depends_on: None,
            filter: None,
            entity_ttl: None,
            history_ttl: None,
            projection: Some(Projection::Select(AHashSet::from_iter([
                "count_1h".into(),
                "sum_1h".into(),
            ]))),
            ephemeral: None,
            pipeline_ttl: None,
            max_keys: None,
            watermark_lateness: None,
            shard_key: None,
        })
        .unwrap();

    let features = engine
        .push(
            "Txns",
            &json!({
                "user_id": "u1", "amount": 42.0
            }),
            &store,
            now,
        )
        .unwrap();

    assert_eq!(features.get("count_1h"), Some(&FeatureValue::Int(1)));
    assert_eq!(features.get("sum_1h"), Some(&FeatureValue::Float(42.0)));
    assert!(
        features.get("internal_count_24h").is_none(),
        "internal feature should be filtered by Select projection"
    );
}

#[ignore = "54-03 Task 4: legacy StateStore API / engine.push(&store, ...); Wave 4 re-enables after legacy-engine removal"]
#[test]
fn test_projection_drop_push() {
    let mut engine = PipelineEngine::new();
    let store = StateStore::new();
    let now = ts(60_000);

    engine
        .register(StreamDefinition {
            name: "Txns".into(),
            key_field: Some("user_id".into()),
            group_by_keys: None,
            features: vec![
                (
                    "count_1h".into(),
                    FeatureDef::Count {
                        window: Duration::from_secs(3600),
                        bucket: Duration::from_secs(60),
                        where_expr: None,
                        backfill: false,
                    },
                ),
                (
                    "sum_1h".into(),
                    FeatureDef::Sum {
                        field: "amount".into(),
                        window: Duration::from_secs(3600),
                        bucket: Duration::from_secs(60),
                        optional: false,
                        where_expr: None,
                        backfill: false,
                    },
                ),
                (
                    "internal_count_24h".into(),
                    FeatureDef::Count {
                        window: Duration::from_secs(86400),
                        bucket: Duration::from_secs(600),
                        where_expr: None,
                        backfill: false,
                    },
                ),
            ],
            depends_on: None,
            filter: None,
            entity_ttl: None,
            history_ttl: None,
            projection: Some(Projection::Drop(AHashSet::from_iter([
                "internal_count_24h".into(),
            ]))),
            ephemeral: None,
            pipeline_ttl: None,
            max_keys: None,
            watermark_lateness: None,
            shard_key: None,
        })
        .unwrap();

    let features = engine
        .push(
            "Txns",
            &json!({
                "user_id": "u1", "amount": 42.0
            }),
            &store,
            now,
        )
        .unwrap();

    assert_eq!(features.get("count_1h"), Some(&FeatureValue::Int(1)));
    assert_eq!(features.get("sum_1h"), Some(&FeatureValue::Float(42.0)));
    assert!(
        features.get("internal_count_24h").is_none(),
        "internal feature should be filtered by Drop projection"
    );
}

#[ignore = "54-03 Task 4: legacy StateStore API / engine.push(&store, ...); Wave 4 re-enables after legacy-engine removal"]
#[test]
fn test_projection_select_get() {
    let mut engine = PipelineEngine::new();
    let store = StateStore::new();
    let now = ts(60_000);

    engine
        .register(StreamDefinition {
            name: "Txns".into(),
            key_field: Some("user_id".into()),
            group_by_keys: None,
            features: vec![
                (
                    "count_1h".into(),
                    FeatureDef::Count {
                        window: Duration::from_secs(3600),
                        bucket: Duration::from_secs(60),
                        where_expr: None,
                        backfill: false,
                    },
                ),
                (
                    "internal_count_24h".into(),
                    FeatureDef::Count {
                        window: Duration::from_secs(86400),
                        bucket: Duration::from_secs(600),
                        where_expr: None,
                        backfill: false,
                    },
                ),
            ],
            depends_on: None,
            filter: None,
            entity_ttl: None,
            history_ttl: None,
            projection: Some(Projection::Select(AHashSet::from_iter(["count_1h".into()]))),
            ephemeral: None,
            pipeline_ttl: None,
            max_keys: None,
            watermark_lateness: None,
            shard_key: None,
        })
        .unwrap();

    engine
        .push("Txns", &json!({"user_id": "u1"}), &store, now)
        .unwrap();

    let features = engine.get_features("u1", &store, now);
    assert_eq!(features.get("count_1h"), Some(&FeatureValue::Int(1)));
    assert!(
        features.get("internal_count_24h").is_none(),
        "GET should also apply Select projection"
    );
}

#[ignore = "54-03 Task 4: legacy StateStore API / engine.push(&store, ...); Wave 4 re-enables after legacy-engine removal"]
#[test]
fn test_projection_drop_get() {
    let mut engine = PipelineEngine::new();
    let store = StateStore::new();
    let now = ts(60_000);

    engine
        .register(StreamDefinition {
            name: "Txns".into(),
            key_field: Some("user_id".into()),
            group_by_keys: None,
            features: vec![
                (
                    "count_1h".into(),
                    FeatureDef::Count {
                        window: Duration::from_secs(3600),
                        bucket: Duration::from_secs(60),
                        where_expr: None,
                        backfill: false,
                    },
                ),
                (
                    "internal_count_24h".into(),
                    FeatureDef::Count {
                        window: Duration::from_secs(86400),
                        bucket: Duration::from_secs(600),
                        where_expr: None,
                        backfill: false,
                    },
                ),
            ],
            depends_on: None,
            filter: None,
            entity_ttl: None,
            history_ttl: None,
            projection: Some(Projection::Drop(AHashSet::from_iter([
                "internal_count_24h".into(),
            ]))),
            ephemeral: None,
            pipeline_ttl: None,
            max_keys: None,
            watermark_lateness: None,
            shard_key: None,
        })
        .unwrap();

    engine
        .push("Txns", &json!({"user_id": "u1"}), &store, now)
        .unwrap();

    let features = engine.get_features("u1", &store, now);
    assert_eq!(features.get("count_1h"), Some(&FeatureValue::Int(1)));
    assert!(
        features.get("internal_count_24h").is_none(),
        "GET should also apply Drop projection"
    );
}

#[ignore = "54-03 Task 4: legacy StateStore API / engine.push(&store, ...); Wave 4 re-enables after legacy-engine removal"]
#[test]
fn test_projection_derive_still_evaluates() {
    // Derive references a dropped feature but derive itself is in the select list.
    // Derives evaluate BEFORE projection, so the derive should compute correctly.
    let mut engine = PipelineEngine::new();
    let store = StateStore::new();
    let now = ts(60_000);

    engine
        .register(StreamDefinition {
            name: "Txns".into(),
            key_field: Some("user_id".into()),
            group_by_keys: None,
            features: vec![
                (
                    "count_1h".into(),
                    FeatureDef::Count {
                        window: Duration::from_secs(3600),
                        bucket: Duration::from_secs(60),
                        where_expr: None,
                        backfill: false,
                    },
                ),
                (
                    "internal_count_24h".into(),
                    FeatureDef::Count {
                        window: Duration::from_secs(86400),
                        bucket: Duration::from_secs(600),
                        where_expr: None,
                        backfill: false,
                    },
                ),
                (
                    "ratio".into(),
                    FeatureDef::Derive {
                        expr: parse_expr("count_1h / internal_count_24h").unwrap(),
                    },
                ),
            ],
            depends_on: None,
            filter: None,
            entity_ttl: None,
            history_ttl: None,
            projection: Some(Projection::Select(AHashSet::from_iter([
                "count_1h".into(),
                "ratio".into(),
            ]))),
            ephemeral: None,
            pipeline_ttl: None,
            max_keys: None,
            watermark_lateness: None,
            shard_key: None,
        })
        .unwrap();

    let features = engine
        .push(
            "Txns",
            &json!({
                "user_id": "u1"
            }),
            &store,
            now,
        )
        .unwrap();

    // count_1h should be present (selected)
    assert_eq!(features.get("count_1h"), Some(&FeatureValue::Int(1)));
    // ratio should be present and correctly computed (1/1 = 1.0)
    assert_eq!(features.get("ratio"), Some(&FeatureValue::Float(1.0)));
    // internal_count_24h should be ABSENT (not in select list)
    assert!(
        features.get("internal_count_24h").is_none(),
        "internal_count_24h should be filtered out even though derive references it"
    );
}

#[ignore = "54-03 Task 4: legacy StateStore API / engine.push(&store, ...); Wave 4 re-enables after legacy-engine removal"]
#[test]
fn test_v1_3_register_backward_compat() {
    // v1.3-format JSON: no projection, ephemeral, ttl, max_keys fields
    let json_val: serde_json::Value = json!({
        "name": "Transactions",
        "key_field": "user_id",
        "features": [
            {"name": "tx_count_1h", "type": "count", "window": "1h"}
        ]
    });
    let req: RegisterRequest = serde_json::from_value(json_val).unwrap();
    let stream = convert_register_request(req).unwrap();
    assert_eq!(stream.name, "Transactions");
    assert!(stream.projection.is_none());
    assert!(stream.ephemeral.is_none());
    assert!(stream.pipeline_ttl.is_none());
    assert!(stream.max_keys.is_none());

    // Verify it actually works end-to-end
    let mut engine = PipelineEngine::new();
    let store = StateStore::new();
    engine.register(stream).unwrap();
    let features = engine
        .push(
            "Transactions",
            &json!({
                "user_id": "u1"
            }),
            &store,
            ts(60_000),
        )
        .unwrap();
    assert_eq!(features.get("tx_count_1h"), Some(&FeatureValue::Int(1)));
}

#[ignore = "54-03 Task 4: legacy StateStore API / engine.push(&store, ...); Wave 4 re-enables after legacy-engine removal"]
#[test]
fn test_ephemeral_fields_roundtrip() {
    let json_val: serde_json::Value = json!({
        "name": "EphemeralStream",
        "key_field": "user_id",
        "features": [
            {"name": "count_1h", "type": "count", "window": "1h"}
        ],
        "ephemeral": true,
        "ttl": "1h",
        "max_keys": 1000
    });
    let req: RegisterRequest = serde_json::from_value(json_val).unwrap();
    let stream = convert_register_request(req).unwrap();

    assert_eq!(stream.ephemeral, Some(true));
    assert_eq!(stream.pipeline_ttl, Some(Duration::from_secs(3600)));
    assert_eq!(stream.max_keys, Some(1000));

    // Verify no runtime side effects -- push works normally
    let mut engine = PipelineEngine::new();
    let store = StateStore::new();
    engine.register(stream).unwrap();
    let features = engine
        .push(
            "EphemeralStream",
            &json!({
                "user_id": "u1"
            }),
            &store,
            ts(60_000),
        )
        .unwrap();
    assert_eq!(features.get("count_1h"), Some(&FeatureValue::Int(1)));
}

#[ignore = "54-03 Task 4: legacy StateStore API / engine.push(&store, ...); Wave 4 re-enables after legacy-engine removal"]
#[test]
fn test_snapshot_roundtrip_new_fields() {
    // Register a stream with projection + ephemeral fields via raw JSON
    let mut engine = PipelineEngine::new();
    let store = StateStore::new();
    let now = ts(60_000);

    let raw_json = json!({
        "name": "Txns",
        "key_field": "user_id",
        "features": [
            {"name": "count_1h", "type": "count", "window": "1h"},
            {"name": "internal_count_24h", "type": "count", "window": "24h"}
        ],
        "projection": {"select": ["count_1h"]},
        "ephemeral": true,
        "ttl": "2h",
        "max_keys": 500
    });

    let req: RegisterRequest = serde_json::from_value(raw_json.clone()).unwrap();
    let stream = convert_register_request(req).unwrap();
    engine.register(stream).unwrap();
    engine.store_raw_register_json("Txns", raw_json.clone());

    // Push an event
    let features = engine
        .push(
            "Txns",
            &json!({
                "user_id": "u1"
            }),
            &store,
            now,
        )
        .unwrap();

    // Verify projection works before snapshot
    assert_eq!(features.get("count_1h"), Some(&FeatureValue::Int(1)));
    assert!(
        features.get("internal_count_24h").is_none(),
        "should be filtered before snapshot"
    );

    // Create snapshot
    let entities = store.clone_for_snapshot();
    let snapshot = SnapshotState {
        entities,
        pipelines: vec![SerializablePipeline {
            name: "Txns".into(),
            key_field: "user_id".into(),
            raw_register_json: serde_json::to_string(&raw_json).unwrap(),
        }],
        backfill_complete: vec![],
    };

    let bytes = save_snapshot(&snapshot).expect("save should succeed");
    let restored = load_snapshot(&bytes).expect("load should succeed");

    // Restore into new engine + store
    let mut new_engine = PipelineEngine::new();
    let new_store = StateStore::new();
    new_store.restore_from_snapshot(restored.entities);

    // Re-register from restored pipeline JSON
    for pipeline in &restored.pipelines {
        let json_val: serde_json::Value =
            serde_json::from_str(&pipeline.raw_register_json).unwrap();
        let req: RegisterRequest = serde_json::from_value(json_val.clone()).unwrap();
        let stream = convert_register_request(req).unwrap();
        // Verify new fields survived round-trip
        assert!(
            stream.projection.is_some(),
            "projection should survive snapshot round-trip"
        );
        assert_eq!(
            stream.ephemeral,
            Some(true),
            "ephemeral should survive snapshot round-trip"
        );
        assert_eq!(stream.pipeline_ttl, Some(Duration::from_secs(7200)));
        assert_eq!(stream.max_keys, Some(500));
        new_engine.register(stream).unwrap();
        new_engine.store_raw_register_json(&pipeline.name, json_val);
    }

    // Verify projection still works after restore
    let restored_features = new_engine.get_features("u1", &new_store, now);
    assert_eq!(
        restored_features.get("count_1h"),
        Some(&FeatureValue::Int(1))
    );
    assert!(
        restored_features.get("internal_count_24h").is_none(),
        "projection should still filter after snapshot round-trip"
    );
}
