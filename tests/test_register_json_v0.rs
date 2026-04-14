//! Phase 22-01 integration test: parse realistic REGISTER JSON payloads as
//! emitted by `python/tally/_serialize.py` and verify `build_operator`
//! dispatches to the correct `OperatorState` variant for every AggOp.
//!
//! These fixtures are literal copies of the JSON `compile_to_register_json`
//! produces for each op type (see python/tests/test_v0_serialize.py for the
//! reference shapes). If the Python serializer changes, these strings must
//! change in lockstep — `serde(deny_unknown_fields)` would catch drift
//! earlier, but we deliberately stay permissive here for additive evolution.

use tally::engine::register::{build_operator, V0RegisterPayload};
use tally::state::snapshot::OperatorState;

fn aggregation_payload(features_json: &str) -> String {
    format!(
        r#"{{
            "name":"UserMetrics",
            "kind":"table",
            "key_field":"user_id",
            "mode":"overwrite",
            "fields":{{}},
            "aggregation":{{
                "source":"Events",
                "keys":["user_id"],
                "features":[{}]
            }},
            "depends_on":["Events"]
        }}"#,
        features_json
    )
}

fn parse_and_get_features(payload: &str) -> Vec<tally::engine::register::AggregationFeature> {
    let parsed = V0RegisterPayload::parse(payload.as_bytes()).unwrap();
    match parsed {
        V0RegisterPayload::Aggregation(d) => d.aggregation.features,
        _ => panic!("expected Aggregation variant"),
    }
}

#[test]
fn count_dispatches() {
    let p = aggregation_payload(r#"{"name":"n","type":"count","supports_retraction":true,"window":"1h"}"#);
    let f = parse_and_get_features(&p);
    assert!(matches!(build_operator(&f[0]).unwrap(), OperatorState::Count(_)));
}

#[test]
fn sum_dispatches() {
    let p = aggregation_payload(
        r#"{"name":"total","type":"sum","supports_retraction":true,"field":"amount","window":"1h"}"#,
    );
    let f = parse_and_get_features(&p);
    assert!(matches!(build_operator(&f[0]).unwrap(), OperatorState::Sum(_)));
}

#[test]
fn avg_dispatches() {
    let p = aggregation_payload(
        r#"{"name":"mean","type":"avg","supports_retraction":true,"field":"amount","window":"24h"}"#,
    );
    let f = parse_and_get_features(&p);
    assert!(matches!(build_operator(&f[0]).unwrap(), OperatorState::Avg(_)));
}

#[test]
fn min_max_dispatch() {
    let p = aggregation_payload(
        r#"{"name":"lo","type":"min","supports_retraction":false,"field":"amount","window":"1h"},
           {"name":"hi","type":"max","supports_retraction":false,"field":"amount","window":"1h"}"#,
    );
    let f = parse_and_get_features(&p);
    assert!(matches!(build_operator(&f[0]).unwrap(), OperatorState::Min(_)));
    assert!(matches!(build_operator(&f[1]).unwrap(), OperatorState::Max(_)));
}

#[test]
fn variance_stddev_dispatch() {
    let p = aggregation_payload(
        r#"{"name":"v","type":"variance","supports_retraction":true,"field":"amount","window":"1h"},
           {"name":"s","type":"stddev","supports_retraction":true,"field":"amount","window":"1h"}"#,
    );
    let f = parse_and_get_features(&p);
    assert!(matches!(build_operator(&f[0]).unwrap(), OperatorState::Variance(_)));
    assert!(matches!(build_operator(&f[1]).unwrap(), OperatorState::Stddev(_)));
}

#[test]
fn percentile_dispatches_with_hybrid_params() {
    let p = aggregation_payload(
        r#"{"name":"p95","type":"percentile","supports_retraction":false,
            "field":"latency","window":"1h","quantile":0.95,
            "exact_threshold":256,"hybrid_alpha":0.01}"#,
    );
    let f = parse_and_get_features(&p);
    assert!(matches!(build_operator(&f[0]).unwrap(), OperatorState::Percentile(_)));
}

#[test]
fn count_distinct_dispatches_with_hybrid_params() {
    let p = aggregation_payload(
        r#"{"name":"uniq","type":"count_distinct","supports_retraction":false,
            "field":"session_id","window":"24h",
            "exact_threshold":1024,"hybrid_precision":14}"#,
    );
    let f = parse_and_get_features(&p);
    assert!(matches!(build_operator(&f[0]).unwrap(), OperatorState::DistinctCount(_)));
}

#[test]
fn top_k_dispatches_with_hybrid_params() {
    let p = aggregation_payload(
        r#"{"name":"top","type":"top_k","supports_retraction":false,
            "field":"merchant_id","k":10,"window":"1h",
            "exact_threshold":1024,"hybrid_width":2048,"hybrid_depth":4}"#,
    );
    let f = parse_and_get_features(&p);
    assert!(matches!(build_operator(&f[0]).unwrap(), OperatorState::TopK(_)));
}

#[test]
fn first_last_dispatch() {
    let p = aggregation_payload(
        r#"{"name":"first_cty","type":"first","supports_retraction":false,"field":"country"},
           {"name":"last_cty","type":"last","supports_retraction":false,"field":"country"}"#,
    );
    let f = parse_and_get_features(&p);
    assert!(matches!(build_operator(&f[0]).unwrap(), OperatorState::First(_)));
    assert!(matches!(build_operator(&f[1]).unwrap(), OperatorState::Last(_)));
}

#[test]
fn first_n_last_n_dispatch() {
    let p = aggregation_payload(
        r#"{"name":"first5","type":"first_n","supports_retraction":false,"field":"country","n":5},
           {"name":"last5","type":"last_n","supports_retraction":false,"field":"country","n":5}"#,
    );
    let f = parse_and_get_features(&p);
    assert!(matches!(build_operator(&f[0]).unwrap(), OperatorState::FirstN(_)));
    assert!(matches!(build_operator(&f[1]).unwrap(), OperatorState::LastN(_)));
}

#[test]
fn ema_lag_dispatch() {
    let p = aggregation_payload(
        r#"{"name":"smooth","type":"ema","supports_retraction":false,"field":"amount","half_life":"30m"},
           {"name":"prev","type":"lag","supports_retraction":false,"field":"amount","n":3}"#,
    );
    let f = parse_and_get_features(&p);
    assert!(matches!(build_operator(&f[0]).unwrap(), OperatorState::Ema(_)));
    assert!(matches!(build_operator(&f[1]).unwrap(), OperatorState::Lag(_)));
}

// ---- Full 16-op shot: every AggOp in one payload ----

#[test]
fn all_sixteen_ops_in_one_payload() {
    let features = r#"
        {"name":"a","type":"count","supports_retraction":true,"window":"1h"},
        {"name":"b","type":"sum","supports_retraction":true,"field":"x","window":"1h"},
        {"name":"c","type":"avg","supports_retraction":true,"field":"x","window":"1h"},
        {"name":"d","type":"min","supports_retraction":false,"field":"x","window":"1h"},
        {"name":"e","type":"max","supports_retraction":false,"field":"x","window":"1h"},
        {"name":"f","type":"variance","supports_retraction":true,"field":"x","window":"1h"},
        {"name":"g","type":"stddev","supports_retraction":true,"field":"x","window":"1h"},
        {"name":"h","type":"percentile","supports_retraction":false,"field":"x","window":"1h","quantile":0.5,"exact_threshold":256,"hybrid_alpha":0.01},
        {"name":"i","type":"count_distinct","supports_retraction":false,"field":"x","window":"1h","exact_threshold":1024,"hybrid_precision":14},
        {"name":"j","type":"top_k","supports_retraction":false,"field":"x","k":5,"window":"1h","exact_threshold":1024,"hybrid_width":2048,"hybrid_depth":4},
        {"name":"k","type":"first","supports_retraction":false,"field":"x"},
        {"name":"l","type":"last","supports_retraction":false,"field":"x"},
        {"name":"m","type":"first_n","supports_retraction":false,"field":"x","n":3},
        {"name":"n","type":"last_n","supports_retraction":false,"field":"x","n":3},
        {"name":"o","type":"ema","supports_retraction":false,"field":"x","half_life":"10m"},
        {"name":"p","type":"lag","supports_retraction":false,"field":"x","n":2}
    "#;
    let payload = aggregation_payload(features);
    let feats = parse_and_get_features(&payload);
    assert_eq!(feats.len(), 16);
    for f in &feats {
        build_operator(f).unwrap_or_else(|e| panic!("dispatch failed for {}: {:?}", f.op_type, e));
    }
}

// ---- Rejection tests ----

#[test]
fn rejects_legacy_v2_payload_shape() {
    let legacy = br#"{
        "name":"Transactions",
        "key_field":"user_id",
        "features":[
            {"name":"count_30m","type":"count","window":"30m"}
        ]
    }"#;
    let err = V0RegisterPayload::parse(legacy).unwrap_err();
    match err {
        tally::error::TallyError::Protocol(msg) => {
            assert!(msg.contains("legacy top-level 'features'"), "msg was: {}", msg);
        }
        other => panic!("expected Protocol error, got {:?}", other),
    }
}

#[test]
fn rejects_missing_kind() {
    let bad = br#"{"name":"X","fields":{}}"#;
    assert!(V0RegisterPayload::parse(bad).is_err());
}

#[test]
fn rejects_unknown_op_type() {
    let p = aggregation_payload(
        r#"{"name":"mystery","type":"quantum_fourier_op","supports_retraction":false,"field":"x","window":"1h"}"#,
    );
    let f = parse_and_get_features(&p);
    let err = build_operator(&f[0]).unwrap_err();
    match err {
        tally::error::TallyError::Protocol(msg) => assert!(msg.contains("unknown aggregation op")),
        other => panic!("expected Protocol error, got {:?}", other),
    }
}

#[test]
fn rejects_malformed_window() {
    // window="30abc" has unknown suffix 'c'
    let p = aggregation_payload(
        r#"{"name":"bad","type":"count","supports_retraction":true,"window":"30xyz"}"#,
    );
    let f = parse_and_get_features(&p);
    assert!(build_operator(&f[0]).is_err());
}

// ---- Descriptor-kind smoke ----

#[test]
fn parse_stream_source_descriptor() {
    let json = br#"{"name":"Clicks","kind":"stream","key_field":null,"fields":{"user_id":{"type":"str","optional":false}}}"#;
    let p = V0RegisterPayload::parse(json).unwrap();
    assert_eq!(p.descriptor_kind(), "source");
    assert_eq!(p.descriptor_name(), "Clicks");
}

#[test]
fn parse_table_source_descriptor_composite() {
    let json = br#"{
        "name":"Ctx","kind":"table","mode":"overwrite",
        "key_field":null,"key_fields":["user_id","region"],
        "fields":{}
    }"#;
    let p = V0RegisterPayload::parse(json).unwrap();
    assert_eq!(p.descriptor_kind(), "source");
}

#[test]
fn parse_op_chain_descriptor() {
    let json = br#"{
        "name":"Filtered","kind":"stream","key_field":null,"fields":{},
        "ops":[{"kind":"filter","expr":"amount > 100"}],
        "depends_on":["Checkouts"]
    }"#;
    let p = V0RegisterPayload::parse(json).unwrap();
    assert_eq!(p.descriptor_kind(), "op_chain");
}

#[test]
fn parse_union_descriptor() {
    let json = br#"{
        "name":"AllEvents","kind":"stream","key_field":null,"fields":{},
        "union":{"sources":["A","B"]},
        "depends_on":["A","B"]
    }"#;
    let p = V0RegisterPayload::parse(json).unwrap();
    assert_eq!(p.descriptor_kind(), "union");
}

#[test]
fn parse_join_descriptor() {
    let json = br#"{
        "name":"Enriched","kind":"stream","key_field":null,"fields":{},
        "join":{"left":"Clicks","right":"Users","on":["user_id"],"type":"left","shape":"stream_table"},
        "depends_on":["Clicks","Users"]
    }"#;
    let p = V0RegisterPayload::parse(json).unwrap();
    assert_eq!(p.descriptor_kind(), "join");
}
