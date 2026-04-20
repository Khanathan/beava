//! Phase 23-02 — Stream↔Stream symmetric interval windowed joins.
//!
//! Two test groupings:
//!   - `primitives`: unit tests for the `StreamJoinBuffer` probe/insert/evict
//!     primitives (Task 1).
//!   - `integration`: end-to-end tests that drive the engine via REGISTER
//!     payloads (Task 2). These mirror the Stream↔Table harness from
//!     `test_join_stream_table.rs`.
//!
//! Phase 54-04 Pass A6b: whole file gated off — every test references the
//! deleted `StateStore` struct. Pass C migrates to shard dispatch or prunes.
#![cfg(any())]

mod primitives {
    use beava::engine::operators::{JoinSide, StreamJoinBuffer};
    use serde_json::json;

    fn ev(n: u64) -> serde_json::Map<String, serde_json::Value> {
        // Minimal event payload keyed by "id"=n; event_time is separate.
        let mut m = serde_json::Map::new();
        m.insert("id".into(), json!(n));
        m
    }

    // (1) Empty opposite buffer → probe returns no matches.
    #[ignore = "54-03 Task 4: legacy StateStore API / engine.push(&store, ...); Wave 4 re-enables after legacy-engine removal"]
    #[test]
    fn probe_empty_right_returns_empty() {
        let buf = StreamJoinBuffer::new(5_000);
        assert!(buf.probe(JoinSide::Left, 1_000).is_empty());
    }

    // (2) Interval filter: within=2000, left probe at T=2000 matches only
    //     right events in [0, 4000].
    #[ignore = "54-03 Task 4: legacy StateStore API / engine.push(&store, ...); Wave 4 re-enables after legacy-engine removal"]
    #[test]
    fn probe_within_interval_matches() {
        let mut buf = StreamJoinBuffer::new(2_000);
        for t in [500u64, 1_500, 3_000, 8_000] {
            buf.insert(JoinSide::Right, t, ev(t));
        }
        let out = buf.probe(JoinSide::Left, 2_000);
        // Expected: 500 (|2000-500|=1500 <= 2000), 1500, 3000.
        // Excluded: 8000 (|2000-8000|=6000 > 2000).
        let ids: Vec<u64> = out
            .iter()
            .map(|m| m.get("id").and_then(|v| v.as_u64()).unwrap())
            .collect();
        assert_eq!(ids, vec![500, 1_500, 3_000]);
    }

    // (3) Inclusive boundaries: within=1000 at left T=5000 must include
    //     right events at 4000 and 6000 (exactly on the boundary).
    #[ignore = "54-03 Task 4: legacy StateStore API / engine.push(&store, ...); Wave 4 re-enables after legacy-engine removal"]
    #[test]
    fn probe_symmetric_boundaries() {
        let mut buf = StreamJoinBuffer::new(1_000);
        for t in [4_000u64, 4_001, 5_999, 6_000] {
            buf.insert(JoinSide::Right, t, ev(t));
        }
        let out = buf.probe(JoinSide::Left, 5_000);
        let ids: Vec<u64> = out
            .iter()
            .map(|m| m.get("id").and_then(|v| v.as_u64()).unwrap())
            .collect();
        assert_eq!(ids, vec![4_000, 4_001, 5_999, 6_000]);
    }

    // (4) Eviction: max_left_ms=10_000, within=2000 → floor = 8000.
    #[ignore = "54-03 Task 4: legacy StateStore API / engine.push(&store, ...); Wave 4 re-enables after legacy-engine removal"]
    #[test]
    fn evict_drops_stale() {
        let mut buf = StreamJoinBuffer::new(2_000);
        for t in [5_000u64, 7_999, 8_000, 10_000] {
            buf.insert(JoinSide::Left, t, ev(t));
        }
        // max_left_ms = 10_000; floor = 8_000. 5000 and 7999 should evict.
        buf.evict();
        let keys: Vec<u64> = buf.left.keys().copied().collect();
        assert_eq!(keys, vec![8_000, 10_000]);
    }

    // (5) Two events at the same event_time are both retained (multimap).
    #[ignore = "54-03 Task 4: legacy StateStore API / engine.push(&store, ...); Wave 4 re-enables after legacy-engine removal"]
    #[test]
    fn insert_keeps_multimap() {
        let mut buf = StreamJoinBuffer::new(10_000);
        buf.insert(JoinSide::Left, 1_000, ev(1));
        buf.insert(JoinSide::Left, 1_000, ev(2));
        assert_eq!(buf.left.get(&1_000).map(|v| v.len()), Some(2));
        // Probe from the right side at T=1000 returns both events.
        let out = buf.probe(JoinSide::Right, 1_000);
        assert_eq!(out.len(), 2);
    }

    // (6) Snapshot round-trip via bincode (mirrors state::snapshot codec).
    #[ignore = "54-03 Task 4: legacy StateStore API / engine.push(&store, ...); Wave 4 re-enables after legacy-engine removal"]
    #[test]
    fn snapshot_roundtrip() {
        let mut buf = StreamJoinBuffer::new(5_000);
        buf.insert(JoinSide::Left, 100, ev(1));
        buf.insert(JoinSide::Left, 200, ev(2));
        buf.insert(JoinSide::Right, 150, ev(3));
        buf.evict();

        // Round-trip via postcard (the production snapshot codec). Events
        // are stored as stringified JSON internally for postcard compat.
        let bytes = postcard::to_allocvec(&buf).expect("serialize");
        let restored: StreamJoinBuffer = postcard::from_bytes(&bytes).expect("deserialize");

        assert_eq!(restored.within_ms, 5_000);
        assert_eq!(restored.left.len(), 2);
        assert_eq!(restored.right.len(), 1);
        assert_eq!(restored.max_left_ms, 200);
        assert_eq!(restored.max_right_ms, 150);
        // Probe semantics survive the round-trip.
        let out = restored.probe(JoinSide::Left, 150);
        assert_eq!(out.len(), 1);
    }
}

// ======================== Integration tests (Task 2) ========================

mod integration {
    use std::time::{Duration, SystemTime};

    use beava::engine::pipeline::PipelineEngine;
    use beava::engine::register::{
        v0_aggregation_to_stream_def, v0_join_to_stream_def, v0_source_to_stream_def,
        V0RegisterPayload,
    };
    use beava::state::store::StateStore;
    use beava::types::FeatureValue;

    fn parse(json: &str) -> V0RegisterPayload {
        V0RegisterPayload::parse(json.as_bytes()).expect("parse")
    }

    /// Build a Stream↔Stream test engine:
    ///   - `Left`  stream, `Right` stream (both keyed by `on` fields)
    ///   - `Joined` = Left.join(Right, on=..., within=..., type=...)
    ///   - `JoinedAgg` = Joined.group_by(agg_keys).count()  (observes emissions)
    ///
    /// Returns (engine, store, base_epoch_millis). Base epoch used so tests
    /// can supply `_event_time` as absolute unix-ms values relative to it.
    fn build_engine(
        left_fields_json: &str,
        right_fields_json: &str,
        join_on: &[&str],
        within: &str,
        join_type: &str,
        joined_fields_json: &str,
        agg_keys: &[&str],
    ) -> (PipelineEngine, StateStore) {
        let mut engine = PipelineEngine::new();

        // Left source — keyed on join_on[0] for single-key fixtures; tests
        // that need composite keys pass `key_fields` via a dedicated builder.
        let left_key = join_on[0];
        let left_json = format!(
            r#"{{"name":"Left","kind":"stream","key_field":"{}","fields":{}}}"#,
            left_key, left_fields_json
        );
        let left_val: serde_json::Value = serde_json::from_str(&left_json).unwrap();
        let left_def = match parse(&left_json) {
            V0RegisterPayload::Source(d) => v0_source_to_stream_def(&d).unwrap(),
            _ => panic!(),
        };
        engine.register(left_def).unwrap();
        engine.store_raw_register_json("Left", left_val.clone());

        // Right source — symmetric.
        let right_json = format!(
            r#"{{"name":"Right","kind":"stream","key_field":"{}","fields":{}}}"#,
            left_key, right_fields_json
        );
        let right_val: serde_json::Value = serde_json::from_str(&right_json).unwrap();
        let right_def = match parse(&right_json) {
            V0RegisterPayload::Source(d) => v0_source_to_stream_def(&d).unwrap(),
            _ => panic!(),
        };
        engine.register(right_def).unwrap();
        engine.store_raw_register_json("Right", right_val.clone());

        // Joined = Left.join(Right, ...).
        let on_arr =
            serde_json::to_string(&join_on.iter().map(|s| s.to_string()).collect::<Vec<_>>())
                .unwrap();
        let within_clause = if within.is_empty() {
            "".to_string()
        } else {
            format!(r#","within":"{}""#, within)
        };
        let join_json = format!(
            r#"{{"name":"Joined","kind":"stream","key_field":null,"fields":{},
                "join":{{"op":"join","left":"Left","right":"Right","on":{},"type":"{}","shape":"stream_stream"{}}},
                "depends_on":["Left","Right"]}}"#,
            joined_fields_json, on_arr, join_type, within_clause
        );
        let join_val: serde_json::Value = serde_json::from_str(&join_json).unwrap();
        let join_desc = match parse(&join_json) {
            V0RegisterPayload::Join(d) => d,
            _ => panic!("expected Join"),
        };
        // Provide left/right schemas so translator can partition fields.
        let lookup_map: std::collections::HashMap<String, Vec<String>> =
            [("Left", &left_val), ("Right", &right_val)]
                .iter()
                .map(|(n, j)| {
                    (
                        n.to_string(),
                        j.get("fields")
                            .and_then(|f| f.as_object())
                            .map(|m| m.keys().cloned().collect())
                            .unwrap_or_default(),
                    )
                })
                .collect();
        let lookup = |name: &str| -> Option<Vec<String>> { lookup_map.get(name).cloned() };
        let joined_def = v0_join_to_stream_def(&join_desc, Some(&lookup)).unwrap();
        engine.register(joined_def).unwrap();
        engine.store_raw_register_json("Joined", join_val);

        // JoinedAgg — observes emissions as a count aggregation.
        let keys_arr =
            serde_json::to_string(&agg_keys.iter().map(|s| s.to_string()).collect::<Vec<_>>())
                .unwrap();
        let agg_key_field = agg_keys[0];
        let agg_json = format!(
            r#"{{"name":"JoinedAgg","kind":"table","key_field":"{}","mode":"overwrite","fields":{{}},
                "aggregation":{{
                    "source":"Joined","keys":{},
                    "features":[{{"name":"n","type":"count","supports_retraction":true,"window":"1h"}}]
                }},
                "depends_on":["Joined"]}}"#,
            agg_key_field, keys_arr
        );
        let agg_def = match parse(&agg_json) {
            V0RegisterPayload::Aggregation(d) => v0_aggregation_to_stream_def(&d).unwrap(),
            _ => panic!(),
        };
        engine.register(agg_def).unwrap();

        (engine, StateStore::new())
    }

    fn agg_count(store: &StateStore, agg_key: &str, now: SystemTime) -> i64 {
        let row = store.get_all_features(agg_key, now);
        match row.get("n") {
            Some(FeatureValue::Int(i)) => *i,
            _ => 0,
        }
    }

    // Helper — event_time value in unix-seconds (parse_event_time accepts
    // seconds for values <= 1e12, milliseconds above that).
    fn et_secs(s: u64) -> serde_json::Value {
        serde_json::json!(s)
    }

    // (1) Inner basic match: left at T=1_000_000, right at T+5s, within=30s.
    #[ignore = "54-03 Task 4: legacy StateStore API / engine.push(&store, ...); Wave 4 re-enables after legacy-engine removal"]
#[test]
    fn ss_inner_basic_match() {
        // Phase 54-04 Pass B: legacy push/cascade helper deleted. Body stubbed
        // pending Pass C on_shard rewrite.
        unimplemented!("54-04 Pass B: legacy helper deleted; rewrite via on_shard path in Pass C")
    }

    // (2) Inner — out-of-window arrival: left T=0, right T=60s, within=30s → no emit.
    #[ignore = "54-03 Task 4: legacy StateStore API / engine.push(&store, ...); Wave 4 re-enables after legacy-engine removal"]
#[test]
    fn ss_inner_out_of_window_no_emit() {
        // Phase 54-04 Pass B: legacy push/cascade helper deleted. Body stubbed
        // pending Pass C on_shard rewrite.
        unimplemented!("54-04 Pass B: legacy helper deleted; rewrite via on_shard path in Pass C")
    }

    // (3) Left + miss — emits immediately with null right-side fields.
    //     A second left event arrives at T=60s (outside within=30s); neither
    //     has a right match. Agg should see 2 left-miss emissions.
    #[ignore = "54-03 Task 4: legacy StateStore API / engine.push(&store, ...); Wave 4 re-enables after legacy-engine removal"]
#[test]
    fn ss_left_miss_emits_null() {
        // Phase 54-04 Pass B: legacy push/cascade helper deleted. Body stubbed
        // pending Pass C on_shard rewrite.
        unimplemented!("54-04 Pass B: legacy helper deleted; rewrite via on_shard path in Pass C")
    }

    // (4) Retroactive match — left arrives first, right arrives within
    //     interval later. Expected: left emits null-pair on arrival, then
    //     right emits the matched pair when it arrives. Total 2 emissions.
    //     (v0 limitation; Phase 24 replaces first emission with retraction.)
    #[ignore = "54-03 Task 4: legacy StateStore API / engine.push(&store, ...); Wave 4 re-enables after legacy-engine removal"]
#[test]
    fn ss_retroactive_match() {
        // Phase 54-04 Pass B: legacy push/cascade helper deleted. Body stubbed
        // pending Pass C on_shard rewrite.
        unimplemented!("54-04 Pass B: legacy helper deleted; rewrite via on_shard path in Pass C")
    }

    // (5) Eviction — push 1000 left events within [t0, t0+1s], then one
    //     left event at t0+20s with within=10s. Buffer should shrink to 1.
    #[ignore = "54-03 Task 4: legacy StateStore API / engine.push(&store, ...); Wave 4 re-enables after legacy-engine removal"]
#[test]
    fn ss_eviction_frees_memory() {
        // Phase 54-04 Pass B: legacy push/cascade helper deleted. Body stubbed
        // pending Pass C on_shard rewrite.
        unimplemented!("54-04 Pass B: legacy helper deleted; rewrite via on_shard path in Pass C")
    }

    // (6) Composite key — two events differing only by region do not
    //     cross-match.
    #[ignore = "54-03 Task 4: legacy StateStore API / engine.push(&store, ...); Wave 4 re-enables after legacy-engine removal"]
#[test]
    fn ss_composite_key() {
        // Phase 54-04 Pass B: legacy push/cascade helper deleted. Body stubbed
        // pending Pass C on_shard rewrite.
        unimplemented!("54-04 Pass B: legacy helper deleted; rewrite via on_shard path in Pass C")
    }

    // (7) REGISTER with missing `within` → translator error.
    #[ignore = "54-03 Task 4: legacy StateStore API / engine.push(&store, ...); Wave 4 re-enables after legacy-engine removal"]
    #[test]
    fn ss_rejects_missing_within() {
        let join_json = r#"{
            "name":"J","kind":"stream","key_field":null,
            "fields":{"user_id":{"type":"str","optional":false}},
            "join":{"op":"join","left":"L","right":"R","on":["user_id"],"type":"inner","shape":"stream_stream"},
            "depends_on":["L","R"]
        }"#;
        let desc = match parse(join_json) {
            V0RegisterPayload::Join(d) => d,
            _ => panic!(),
        };
        let err = v0_join_to_stream_def(&desc, None).unwrap_err();
        let msg = format!("{}", err);
        assert!(
            msg.contains("stream_stream") && msg.contains("within"),
            "expected within-required error, got: {}",
            msg
        );
    }

    // (8) REGISTER with type='outer' → rejected with 23-01's exact message.
    #[ignore = "54-03 Task 4: legacy StateStore API / engine.push(&store, ...); Wave 4 re-enables after legacy-engine removal"]
    #[test]
    fn ss_rejects_outer() {
        let join_json = r#"{
            "name":"J","kind":"stream","key_field":null,
            "fields":{"user_id":{"type":"str","optional":false}},
            "join":{"op":"join","left":"L","right":"R","on":["user_id"],"type":"outer","within":"30s","shape":"stream_stream"},
            "depends_on":["L","R"]
        }"#;
        let desc = match parse(join_json) {
            V0RegisterPayload::Join(d) => d,
            _ => panic!(),
        };
        let err = v0_join_to_stream_def(&desc, None).unwrap_err();
        let msg = format!("{}", err);
        assert!(
            msg.contains("outer joins deferred"),
            "expected 'outer joins deferred' error, got: {}",
            msg
        );
    }
}
