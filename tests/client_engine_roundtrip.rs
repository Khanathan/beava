//! Phase 28-03: prove the engine runs in a client context.
//!
//! Registers a minimal pipeline, pushes two events, asserts state
//! mutated across both pushes. No server imports; compiles and passes
//! under both default (server) and `--no-default-features --features
//! client` feature sets. This is the anti-regression guard for
//! decision D1 — if anyone re-introduces a server-only side effect
//! into the engine's `push` hot path (outside a `#[cfg(feature =
//! "server")]` gate), this test stops compiling under `--features
//! client`.
//!
//! Phase 29 should factor the minimal-pipeline setup below out into a
//! shared `minimal_client_harness()` helper; for now it's inlined to
//! keep the test self-contained and to avoid coupling Phase 28 to any
//! not-yet-existent helper module.

use beava::engine::pipeline::{FeatureDef, PipelineEngine, StreamDefinition};
use beava::state::store::StateStore;
use beava::types::FeatureValue;
use serde_json::json;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

fn ts(secs: u64) -> SystemTime {
    UNIX_EPOCH + Duration::from_secs(secs)
}

fn minimal_tx_stream() -> StreamDefinition {
    StreamDefinition {
        name: "Events".into(),
        key_field: Some("entity_key".into()),
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
                    field: "amt".into(),
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
    }
}

#[test]
fn engine_push_round_trip_under_client_features() {
    let mut engine = PipelineEngine::new();
    let store = StateStore::new();
    engine.register(minimal_tx_stream()).unwrap();

    let now = ts(60_000);

    // Event 1: state should mutate from empty to count=1, sum=10.0.
    let feats1 = engine
        .push(
            "Events",
            &json!({"entity_key": "u1", "amt": 10.0}),
            &store,
            now,
        )
        .unwrap();
    assert_eq!(feats1.get("count_1h"), Some(&FeatureValue::Int(1)));
    assert_eq!(feats1.get("sum_1h"), Some(&FeatureValue::Float(10.0)));

    // Event 2: aggregated feature value must advance — proves operator
    // state mutated, not just that `push` returned Ok.
    let feats2 = engine
        .push(
            "Events",
            &json!({"entity_key": "u1", "amt": 2.5}),
            &store,
            now,
        )
        .unwrap();
    assert_eq!(feats2.get("count_1h"), Some(&FeatureValue::Int(2)));
    assert_eq!(feats2.get("sum_1h"), Some(&FeatureValue::Float(12.5)));
}

#[test]
fn client_types_usable_alongside_engine() {
    // Smoke-check: the Phase 28 client surface is reachable from the
    // same test binary that drives the engine. No server import in
    // sight — this compiles identically under `--features client`.
    use beava::client::{OutOfScopeError, Session, SessionMode};

    let s = Session::new("replica.example:6400", vec!["Events".into()]);
    assert!(matches!(s.mode, SessionMode::Historical));
    let e = OutOfScopeError::new("Events/u999");
    assert_eq!(format!("{}", e), "query out of scope: Events/u999");
}
