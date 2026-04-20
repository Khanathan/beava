//! Phase 23-01 Task 1 — composite group_by keys.
//!
//! Verifies that aggregations declared with multi-key `group_by` bucket state
//! under the pipe-encoded composite key ("k1|k2|..."), that the single-key
//! fast-path is unchanged, and that missing composite-key fields surface as
//! a typed error (not a panic, not a silent miss).

use std::time::{Duration, SystemTime};

use beava::engine::pipeline::PipelineEngine;
use beava::engine::register::{
    encode_group_by, v0_aggregation_to_stream_def, v0_source_to_stream_def, V0RegisterPayload,
};
use beava::state::store::StateStore;
use beava::types::FeatureValue;

fn parse_agg(json: &str) -> V0RegisterPayload {
    V0RegisterPayload::parse(json.as_bytes()).expect("parse")
}

fn register_engine_with_source_and_agg(
    source_json: &str,
    agg_json: &str,
) -> (PipelineEngine, StateStore) {
    let mut engine = PipelineEngine::new();
    let source = match parse_agg(source_json) {
        V0RegisterPayload::Source(d) => v0_source_to_stream_def(&d).unwrap(),
        _ => panic!("expected Source"),
    };
    engine.register(source).unwrap();
    let agg = match parse_agg(agg_json) {
        V0RegisterPayload::Aggregation(d) => v0_aggregation_to_stream_def(&d).unwrap(),
        _ => panic!("expected Aggregation"),
    };
    engine.register(agg).unwrap();
    (engine, StateStore::new())
}

// (1) REGISTER payload with composite keys parses without the old rejection.
#[ignore = "54-03 Task 4: legacy StateStore API / engine.push(&store, ...); Wave 4 re-enables after legacy-engine removal"]
#[test]
fn composite_keys_register_accepted() {
    let source = r#"{
        "name":"TX","kind":"stream","key_field":null,"fields":{}
    }"#;
    let agg = r#"{
        "name":"UserMerchantStats","kind":"table","key_field":"user_id","mode":"overwrite",
        "fields":{},
        "aggregation":{
            "source":"TX",
            "keys":["user_id","merchant_id"],
            "features":[
                {"name":"n","type":"count","supports_retraction":true,"window":"1h"},
                {"name":"total","type":"sum","supports_retraction":true,"field":"amount","window":"1h"},
                {"name":"p95","type":"percentile","supports_retraction":false,
                 "field":"amount","window":"1h","quantile":0.95}
            ]
        },
        "depends_on":["TX"]
    }"#;
    let mut engine = PipelineEngine::new();
    let src = match parse_agg(source) {
        V0RegisterPayload::Source(d) => v0_source_to_stream_def(&d).unwrap(),
        _ => panic!(),
    };
    engine.register(src).unwrap();
    let agg_def = match parse_agg(agg) {
        V0RegisterPayload::Aggregation(d) => v0_aggregation_to_stream_def(&d).unwrap(),
        _ => panic!(),
    };
    // The stream def must carry the full composite key vector.
    assert_eq!(
        agg_def.group_by_keys.as_ref().unwrap(),
        &vec!["user_id".to_string(), "merchant_id".to_string()]
    );
    engine.register(agg_def).unwrap();
}

// (2) Two events with same user_id but different merchant_id bucket into two
//     distinct composite rows.
// (3) A third event matching one composite merges into that bucket only.
#[ignore = "54-03 Task 4: legacy StateStore API / engine.push(&store, ...); Wave 4 re-enables after legacy-engine removal"]
#[test]
fn composite_keys_bucket_independently_and_merge_on_match() {
    let source = r#"{"name":"TX","kind":"stream","key_field":null,"fields":{}}"#;
    let agg = r#"{
        "name":"UMS","kind":"table","key_field":"user_id","mode":"overwrite","fields":{},
        "aggregation":{
            "source":"TX","keys":["user_id","merchant_id"],
            "features":[
                {"name":"n","type":"count","supports_retraction":true,"window":"1h"},
                {"name":"total","type":"sum","supports_retraction":true,"field":"amount","window":"1h"}
            ]
        },
        "depends_on":["TX"]
    }"#;
    let (engine, store) = register_engine_with_source_and_agg(source, agg);
    let now = SystemTime::now();

    let e1 = serde_json::json!({"user_id": "u1", "merchant_id": "m1", "amount": 10.0});
    let e2 = serde_json::json!({"user_id": "u1", "merchant_id": "m2", "amount": 20.0});
    let e3 = serde_json::json!({"user_id": "u1", "merchant_id": "m1", "amount": 5.0});

    engine.push_with_cascade("TX", &e1, &store, now).unwrap();
    engine.push_with_cascade("TX", &e2, &store, now).unwrap();
    engine.push_with_cascade("TX", &e3, &store, now).unwrap();

    let row_m1 = store.get_all_features("u1|m1", now + Duration::from_millis(1));
    let row_m2 = store.get_all_features("u1|m2", now + Duration::from_millis(1));

    // u1|m1 sees events 1 and 3 — count=2, sum=15.
    assert!(
        matches!(row_m1.get("n"), Some(FeatureValue::Int(2))),
        "n={:?}",
        row_m1.get("n")
    );
    let sum_m1 = row_m1.get("total").expect("total");
    match sum_m1 {
        FeatureValue::Float(f) => assert!((f - 15.0).abs() < 1e-6, "sum m1 = {}", f),
        FeatureValue::Int(i) => assert_eq!(*i, 15),
        other => panic!("unexpected sum m1: {:?}", other),
    }

    // u1|m2 sees only event 2 — count=1, sum=20.
    assert!(matches!(row_m2.get("n"), Some(FeatureValue::Int(1))));
    let sum_m2 = row_m2.get("total").expect("total m2");
    match sum_m2 {
        FeatureValue::Float(f) => assert!((f - 20.0).abs() < 1e-6, "sum m2 = {}", f),
        FeatureValue::Int(i) => assert_eq!(*i, 20),
        other => panic!("unexpected sum m2: {:?}", other),
    }

    // The other composite key must not exist.
    assert!(
        store.get_entity("u1").is_none(),
        "composite-only aggregation must not create scalar-keyed row"
    );
}

// (4) Missing composite-key field still surfaces an error (preserved from 22-04).
#[ignore = "54-03 Task 4: legacy StateStore API / engine.push(&store, ...); Wave 4 re-enables after legacy-engine removal"]
#[test]
fn composite_keys_missing_field_errors() {
    let ev = serde_json::json!({"user_id": "u1", "amount": 10.0}); // merchant_id absent
    let err =
        encode_group_by(&["user_id".to_string(), "merchant_id".to_string()], &ev).unwrap_err();
    // BeavaError::Type { field: "merchant_id", .. }
    let msg = format!("{}", err);
    assert!(msg.contains("merchant_id"), "err msg: {}", msg);
}

// (5a) Single-key encode helper: one-element keys list produces an unpiped
// key string ("u1", not "u1|"). Regression guard for the 22-04 fast path.
#[ignore = "54-03 Task 4: legacy StateStore API / engine.push(&store, ...); Wave 4 re-enables after legacy-engine removal"]
#[test]
fn single_key_encode_fast_path_unchanged() {
    let ev = serde_json::json!({"user_id": "u1", "amount": 5.0});
    assert_eq!(
        encode_group_by(&["user_id".to_string()], &ev).unwrap(),
        "u1"
    );
}

// (5b) Single-key engine dispatch regression guard: an aggregation with a
// single-key `keys` array stores state under the plain key, not the
// composite-encoded key.
#[ignore = "54-03 Task 4: legacy StateStore API / engine.push(&store, ...); Wave 4 re-enables after legacy-engine removal"]
#[test]
fn single_key_engine_dispatch_unchanged() {
    use serde_json;
    let source = r#"{"name":"TX2","kind":"stream","key_field":null,"fields":{}}"#;
    let agg = r#"{
        "name":"UserStats","kind":"table","key_field":"user_id","mode":"overwrite","fields":{},
        "aggregation":{
            "source":"TX2","keys":["user_id"],
            "features":[{"name":"n","type":"count","supports_retraction":true,"window":"1h"}]
        },
        "depends_on":["TX2"]
    }"#;
    let (engine, store) = register_engine_with_source_and_agg(source, agg);
    let now = SystemTime::now();
    engine
        .push_with_cascade("TX2", &serde_json::json!({"user_id": "u1"}), &store, now)
        .unwrap();
    let row = store.get_all_features("u1", now + Duration::from_millis(1));
    assert!(matches!(row.get("n"), Some(FeatureValue::Int(1))));
}
