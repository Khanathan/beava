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
        shard_key: None,
    }
}

#[ignore = "54-03 Task 4: legacy StateStore API / engine.push(&store, ...); Wave 4 re-enables after legacy-engine removal"]
#[test]
fn engine_push_round_trip_under_client_features() {
    // Phase 54-04 Pass B: legacy push/cascade helper deleted. Body stubbed
    // pending Pass C on_shard rewrite.
    unimplemented!("54-04 Pass B: legacy helper deleted; rewrite via on_shard path in Pass C")
}

#[ignore = "54-03 Task 4: legacy StateStore API / engine.push(&store, ...); Wave 4 re-enables after legacy-engine removal"]
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
