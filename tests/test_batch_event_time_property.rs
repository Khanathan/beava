// CORR-01: property test for batch vs single-event event-time bucketing equivalence.
// Phase 46 Wave 2 (D-01/D-02): push_batch_with_cascade_no_features now takes
// &[(&Value, SystemTime)] with group-by-bucket internals. This test asserts that
// the batch path produces bit-identical per-key features as the single-event path
// for adversarial event_time distributions (D-04).
use beava::engine::pipeline::{FeatureDef, PipelineEngine, StreamDefinition};
use beava::state::store::StateStore;
use proptest::prelude::*;
use serde_json::json;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Build a minimal engine+store with a single keyed `count(window=1h)` stream
/// named "Txns" with key_field "user". Mirrors the fixture pattern in
/// tests/test_batch_primitives.rs.
fn build_test_engine_with_count_op() -> (PipelineEngine, StateStore) {
    let mut engine = PipelineEngine::new();
    engine
        .register(StreamDefinition {
            name: "Txns".into(),
            key_field: Some("user".into()),
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
        })
        .unwrap();
    let store = StateStore::new();
    (engine, store)
}

proptest! {
    #[test]
    fn batch_path_equals_single_event_path(
        // Adversarial: offsets in [-3600, 0] seconds from a fixed base.
        // D-04: at least one large-negative offset (1h past) and at least one
        // near-zero offset (recent) appear in most samples of size >= 4;
        // proptest shrinking explores minimal failing cases.
        offsets_secs in proptest::collection::vec(-3600i64..0i64, 2..=16)
    ) {
        // Use a fixed base time well past UNIX_EPOCH so subtracting offsets
        // never underflows. 2024-01-01 00:00:00 UTC = 1_704_067_200 s.
        let base = UNIX_EPOCH + Duration::from_secs(1_704_067_200);

        // Build events: each event targets one of 3 user keys (u0, u1, u2)
        // to exercise per-key bucket accumulation.
        let events: Vec<serde_json::Value> = offsets_secs.iter().enumerate()
            .map(|(i, _)| json!({ "user": format!("u{}", i % 3) }))
            .collect();

        // Build per-event event_times from offsets (all <= base, >= base-1h).
        let ets: Vec<SystemTime> = offsets_secs.iter()
            .map(|&off| base - Duration::from_secs((-off) as u64))
            .collect();

        // Oracle: single-event path on engine_single.
        let (engine_single, store_single) = build_test_engine_with_count_op();
        for (ev, &et) in events.iter().zip(ets.iter()) {
            let _ = engine_single.push_with_cascade_no_features("Txns", ev, &store_single, et);
        }

        // SUT: batch path on engine_batch.
        let (engine_batch, store_batch) = build_test_engine_with_count_op();
        let pairs: Vec<(&serde_json::Value, SystemTime)> =
            events.iter().zip(ets.iter()).map(|(e, &et)| (e, et)).collect();
        let _ = engine_batch.push_batch_with_cascade_no_features("Txns", &pairs, &store_batch);

        // Compare per-key feature maps. Use `base` as the read-time so both
        // paths see the same window snapshot.
        for key in ["u0", "u1", "u2"] {
            let f_single = engine_single.get_features(key, &store_single, base);
            let f_batch  = engine_batch.get_features(key, &store_batch, base);
            prop_assert_eq!(
                f_single, f_batch,
                "{}",
                format!("CORR-01 violated for key={key} with offsets_secs={offsets_secs:?}")
            );
        }
    }
}
