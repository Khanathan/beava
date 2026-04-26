//! Phase 12 Plan 01 — Batch primitives unit tests.
//!
//! Covers the four batch-shaped building blocks that Wave 2's
//! `handle_push_batch` will compose:
//!
//!   1. `EventLog::append_many`
//!   2. `StateStore::mark_dirty_many`
//!   3. `PipelineEngine::push_batch_no_features`            (primary-only)
//!   4. `PipelineEngine::push_batch_with_cascade_no_features` (cascade + fan-out aware)
//!
//! Every test here is a correctness gate — the performance win from these
//! primitives comes from their *caller* (handle_push_batch) holding the
//! AppState mutex once per batch. These tests ensure the primitives preserve
//! single-event semantics exactly.

#![allow(dead_code, unused_imports)]

use serde_json::json;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tempfile::TempDir;

use tally::engine::pipeline::{FeatureDef, PipelineEngine, StreamDefinition};
use tally::state::event_log::EventLog;
use tally::state::store::StateStore;
use tally::types::FeatureValue;

fn ts(secs: u64) -> SystemTime {
    UNIX_EPOCH + Duration::from_secs(secs)
}

fn make_count_stream(name: &str, key: &str) -> StreamDefinition {
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
        depends_on: None,
        filter: None,
        entity_ttl: None,
        history_ttl: None,
        projection: None,
        ephemeral: None,
        pipeline_ttl: None,
        max_keys: None,
    }
}

fn make_cascade_child(name: &str, key: &str, parent: &str) -> StreamDefinition {
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
        depends_on: Some(vec![parent.to_string()]),
        filter: None,
        entity_ttl: None,
        history_ttl: None,
        projection: None,
        ephemeral: None,
        pipeline_ttl: None,
        max_keys: None,
    }
}

// ============================================================================
// append_many
// ============================================================================

mod append_many {
    use super::*;

    #[test]
    fn empty_batch_returns_zero() {
        let tmp = TempDir::new().unwrap();
        let log = EventLog::new(tmp.path().to_path_buf()).unwrap();
        log.register_stream("s1", None).unwrap();

        let n = log.append_many("s1", &[], ts(1000)).unwrap();
        assert_eq!(n, 0);
        log.fsync_all().unwrap();
        let entries = log.read_entries("s1").unwrap();
        assert_eq!(entries.len(), 0);
    }

    #[test]
    fn three_events_written_and_readable() {
        let tmp = TempDir::new().unwrap();
        let log = EventLog::new(tmp.path().to_path_buf()).unwrap();
        log.register_stream("s1", None).unwrap();

        let a: &[u8] = b"payload-A";
        let b: &[u8] = b"payload-B";
        let c: &[u8] = b"payload-C";
        let events: [&[u8]; 3] = [a, b, c];
        let n = log.append_many("s1", &events, ts(2000)).unwrap();
        assert_eq!(n, 3);
        log.fsync_all().unwrap();

        let entries = log.read_entries("s1").unwrap();
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].payload, a);
        assert_eq!(entries[1].payload, b);
        assert_eq!(entries[2].payload, c);
        assert_eq!(entries[0].timestamp, ts(2000));
        assert_eq!(entries[2].timestamp, ts(2000));
    }

    #[test]
    fn unregistered_stream_returns_zero_not_error() {
        let tmp = TempDir::new().unwrap();
        let log = EventLog::new(tmp.path().to_path_buf()).unwrap();
        let payload: &[u8] = b"x";
        let events: [&[u8]; 2] = [payload, payload];
        let n = log.append_many("ghost", &events, ts(3000)).unwrap();
        assert_eq!(n, 0);
    }

    #[test]
    fn append_many_after_append_preserves_order() {
        let tmp = TempDir::new().unwrap();
        let log = EventLog::new(tmp.path().to_path_buf()).unwrap();
        log.register_stream("s1", None).unwrap();

        log.append("s1", b"first", ts(4000)).unwrap();
        let b: &[u8] = b"second";
        let c: &[u8] = b"third";
        let events: [&[u8]; 2] = [b, c];
        log.append_many("s1", &events, ts(4001)).unwrap();
        log.fsync_all().unwrap();

        let entries = log.read_entries("s1").unwrap();
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].payload, b"first");
        assert_eq!(entries[1].payload, b"second");
        assert_eq!(entries[2].payload, b"third");
    }
}

// ============================================================================
// mark_dirty_many
// ============================================================================

mod mark_dirty_many {
    use super::*;

    #[test]
    fn empty_iterator_leaves_dirty_count_unchanged() {
        let store = StateStore::new();
        assert_eq!(store.dirty_count(), 0);
        let empty: Vec<&str> = vec![];
        store.mark_dirty_many(empty);
        assert_eq!(store.dirty_count(), 0);
    }

    #[test]
    fn five_keys_with_duplicate_dedups_to_four() {
        let store = StateStore::new();
        store.mark_dirty_many(vec!["k1", "k2", "k3", "k4", "k1"]);
        assert_eq!(store.dirty_count(), 4);
    }

    #[test]
    fn mirrors_mark_dirty_does_not_touch_deleted_keys() {
        let store = StateStore::new();
        store.mark_deleted("ghost");
        store.mark_dirty_many(vec!["ghost", "alive"]);
        // "alive" is now dirty; "ghost" is also added to dirty_keys (mirroring
        // single-key `mark_dirty`, which unconditionally inserts). The delete
        // set is not scrubbed.
        assert_eq!(store.dirty_count(), 2);
        let deleted = store.take_deleted();
        assert_eq!(deleted, vec!["ghost".to_string()]);
    }
}

// ============================================================================
// push_batch_no_features (primary-only)
// ============================================================================

mod push_batch_no_features {
    use super::*;

    #[test]
    fn empty_batch_returns_empty_vec() {
        let mut engine = PipelineEngine::new();
        engine
            .register(make_count_stream("Txns", "user_id"))
            .unwrap();
        let store = StateStore::new();

        let events: Vec<&serde_json::Value> = vec![];
        let out = engine.push_batch_no_features("Txns", &events, &store, ts(1000));
        assert!(out.is_empty());
        assert_eq!(store.entity_count(), 0);
    }

    #[test]
    fn three_events_return_three_results_in_order() {
        let mut engine = PipelineEngine::new();
        engine
            .register(make_count_stream("Txns", "user_id"))
            .unwrap();
        let store = StateStore::new();

        let e0 = json!({"user_id": "u1"});
        let e1 = json!({"user_id": "u2"});
        let e2 = json!({"user_id": "u3"});
        let events = vec![&e0, &e1, &e2];

        let out = engine.push_batch_no_features("Txns", &events, &store, ts(1000));
        assert_eq!(out.len(), 3);
        assert!(out.iter().all(|r| r.is_ok()));
        assert_eq!(store.entity_count(), 3);
    }

    #[test]
    fn partial_failure_does_not_halt_and_preserves_side_effects() {
        let mut engine = PipelineEngine::new();
        engine
            .register(make_count_stream("Txns", "user_id"))
            .unwrap();
        let store = StateStore::new();

        let e0 = json!({"user_id": "u1"});
        let e1 = json!({"user_id": ""}); // empty key -> Protocol error
        let e2 = json!({"user_id": "u2"});
        let events = vec![&e0, &e1, &e2];

        let out = engine.push_batch_no_features("Txns", &events, &store, ts(1000));
        assert_eq!(out.len(), 3);
        assert!(out[0].is_ok(), "event 0 should apply");
        assert!(out[1].is_err(), "event 1 (empty key) should error");
        assert!(
            out[2].is_ok(),
            "event 2 should apply despite event 1's error"
        );

        let f1 = store.get_all_features("u1", ts(1000));
        assert_eq!(f1.get("count_1h"), Some(&FeatureValue::Int(1)));
        let f2 = store.get_all_features("u2", ts(1000));
        assert_eq!(f2.get("count_1h"), Some(&FeatureValue::Int(1)));
    }

    #[test]
    fn unknown_stream_errors_all_events() {
        let engine = PipelineEngine::new();
        let store = StateStore::new();
        let e = json!({"user_id": "u1"});
        let events = vec![&e, &e, &e];

        let out = engine.push_batch_no_features("nonexistent", &events, &store, ts(1000));
        assert_eq!(out.len(), 3);
        assert!(out.iter().all(|r| r.is_err()));
        assert_eq!(store.entity_count(), 0);
    }
}

// ============================================================================
// push_batch_with_cascade_no_features (cascade + fan-out)
// ============================================================================

mod push_batch_with_cascade_no_features {
    use super::*;

    fn build_cascade_engine() -> PipelineEngine {
        let mut engine = PipelineEngine::new();
        engine
            .register(make_count_stream("Txns", "user_id"))
            .unwrap();
        engine
            .register(make_cascade_child("UserRisk", "user_id", "Txns"))
            .unwrap();
        engine
    }

    #[test]
    fn empty_batch_returns_empty_vec() {
        let engine = build_cascade_engine();
        let store = StateStore::new();
        let events: Vec<&serde_json::Value> = vec![];
        let out = engine.push_batch_with_cascade_no_features("Txns", &events, &store, ts(1000));
        assert!(out.is_empty());
        assert_eq!(store.entity_count(), 0);
    }

    #[test]
    fn unknown_stream_errors_all() {
        let engine = build_cascade_engine();
        let store = StateStore::new();
        let e = json!({"user_id": "u1"});
        let events = vec![&e, &e];
        let out = engine.push_batch_with_cascade_no_features("ghost", &events, &store, ts(1000));
        assert_eq!(out.len(), 2);
        assert!(out.iter().all(|r| r.is_err()));
        assert_eq!(store.entity_count(), 0);
    }

    #[test]
    fn cascade_equivalence_3_events() {
        // Engine A receives a batch; Engine B receives sequential single-event
        // cascade pushes. After both, feature state on every stream/key pair
        // must match exactly — that's the D-06 / D-07 guarantee.
        let engine_a = build_cascade_engine();
        let engine_b = build_cascade_engine();
        let store_a = StateStore::new();
        let store_b = StateStore::new();

        let e0 = json!({"user_id": "u1"});
        let e1 = json!({"user_id": "u2"});
        let e2 = json!({"user_id": "u1"});
        let events = vec![&e0, &e1, &e2];
        let now = ts(1000);

        // Batch path on A
        let out_a = engine_a.push_batch_with_cascade_no_features("Txns", &events, &store_a, now);
        assert_eq!(out_a.len(), 3);
        assert!(out_a.iter().all(|r| r.is_ok()));

        // Single-event path on B
        for ev in &events {
            engine_b
                .push_with_cascade_no_features("Txns", ev, &store_b, now)
                .unwrap();
        }

        // u1 should have count == 2 on both engines (after cascade to UserRisk)
        let a_u1 = engine_a.get_features("u1", &store_a, now);
        let b_u1 = engine_b.get_features("u1", &store_b, now);
        assert_eq!(a_u1.get("count_1h"), b_u1.get("count_1h"));
        assert_eq!(a_u1.get("count_1h"), Some(&FeatureValue::Int(2)));

        let a_u2 = engine_a.get_features("u2", &store_a, now);
        let b_u2 = engine_b.get_features("u2", &store_b, now);
        assert_eq!(a_u2.get("count_1h"), b_u2.get("count_1h"));
        assert_eq!(a_u2.get("count_1h"), Some(&FeatureValue::Int(1)));

        assert_eq!(store_a.entity_count(), store_b.entity_count());
    }

    #[test]
    fn fan_out_single_update_per_event_on_target_key() {
        // Two keyed streams sharing a fan-out target. A Txns push that
        // contains both user_id AND merchant_id should apply to
        // MerchantActivity exactly once per event (not zero, not 16).
        let mut engine = PipelineEngine::new();
        engine
            .register(make_count_stream("Txns", "user_id"))
            .unwrap();
        engine
            .register(make_count_stream("MerchantActivity", "merchant_id"))
            .unwrap();
        let store = StateStore::new();

        let e0 = json!({"user_id": "u1", "merchant_id": "m1"});
        let e1 = json!({"user_id": "u2", "merchant_id": "m1"});
        let e2 = json!({"user_id": "u3", "merchant_id": "m1"});
        let e3 = json!({"user_id": "u4", "merchant_id": "m1"});
        let events = vec![&e0, &e1, &e2, &e3];
        let now = ts(1000);

        let out = engine.push_batch_with_cascade_no_features("Txns", &events, &store, now);
        assert_eq!(out.len(), 4);
        assert!(out.iter().all(|r| r.is_ok()));

        // MerchantActivity on m1 should have exactly 4 counts (one per event).
        let m1 = engine.get_features("m1", &store, now);
        assert_eq!(m1.get("count_1h"), Some(&FeatureValue::Int(4)));

        // And each user should have exactly 1 count on Txns.
        for user in &["u1", "u2", "u3", "u4"] {
            let f = engine.get_features(user, &store, now);
            assert_eq!(
                f.get("count_1h"),
                Some(&FeatureValue::Int(1)),
                "user {} count",
                user
            );
        }
    }

    #[test]
    fn error_order_preserved_on_partial_failure() {
        let engine = build_cascade_engine();
        let store = StateStore::new();
        let e0 = json!({"user_id": "u1"});
        let e1 = json!({"user_id": ""}); // bad
        let e2 = json!({"user_id": "u2"});
        let events = vec![&e0, &e1, &e2];
        let now = ts(1000);

        let out = engine.push_batch_with_cascade_no_features("Txns", &events, &store, now);
        assert_eq!(out.len(), 3);
        assert!(out[0].is_ok());
        assert!(out[1].is_err());
        assert!(out[2].is_ok());

        // Good events applied, bad event did not.
        let u1 = engine.get_features("u1", &store, now);
        assert_eq!(u1.get("count_1h"), Some(&FeatureValue::Int(1)));
        let u2 = engine.get_features("u2", &store, now);
        assert_eq!(u2.get("count_1h"), Some(&FeatureValue::Int(1)));
    }

    #[test]
    fn unknown_stream_returns_errors_in_order_without_side_effects() {
        let engine = build_cascade_engine();
        let store = StateStore::new();
        let e = json!({"user_id": "u1"});
        let events = vec![&e, &e, &e];

        let out = engine.push_batch_with_cascade_no_features("nope", &events, &store, ts(1000));
        assert_eq!(out.len(), 3);
        assert!(out.iter().all(|r| r.is_err()));
        assert_eq!(store.entity_count(), 0);
        assert_eq!(store.dirty_count(), 0);
    }
}
