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

use std::time::{Duration, SystemTime, UNIX_EPOCH};
use serde_json::json;
use tempfile::TempDir;

use tally::engine::pipeline::{PipelineEngine, StreamDefinition, FeatureDef};
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
        features: vec![
            ("count_1h".into(), FeatureDef::Count {
                window: Duration::from_secs(3600),
                bucket: Duration::from_secs(60),
                where_expr: None,
                backfill: false,
            }),
        ],
        depends_on: None,
        filter: None,
        entity_ttl: None,
        history_ttl: None,
    }
}

fn make_cascade_child(name: &str, key: &str, parent: &str) -> StreamDefinition {
    StreamDefinition {
        name: name.into(),
        key_field: Some(key.into()),
        features: vec![
            ("count_1h".into(), FeatureDef::Count {
                window: Duration::from_secs(3600),
                bucket: Duration::from_secs(60),
                where_expr: None,
                backfill: false,
            }),
        ],
        depends_on: Some(vec![parent.to_string()]),
        filter: None,
        entity_ttl: None,
        history_ttl: None,
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
        let mut log = EventLog::new(tmp.path().to_path_buf()).unwrap();
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
        let mut log = EventLog::new(tmp.path().to_path_buf()).unwrap();
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
        let mut log = EventLog::new(tmp.path().to_path_buf()).unwrap();
        let payload: &[u8] = b"x";
        let events: [&[u8]; 2] = [payload, payload];
        let n = log.append_many("ghost", &events, ts(3000)).unwrap();
        assert_eq!(n, 0);
    }

    #[test]
    fn append_many_after_append_preserves_order() {
        let tmp = TempDir::new().unwrap();
        let mut log = EventLog::new(tmp.path().to_path_buf()).unwrap();
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
        let mut store = StateStore::new();
        assert_eq!(store.dirty_count(), 0);
        let empty: Vec<&str> = vec![];
        store.mark_dirty_many(empty);
        assert_eq!(store.dirty_count(), 0);
    }

    #[test]
    fn five_keys_with_duplicate_dedups_to_four() {
        let mut store = StateStore::new();
        store.mark_dirty_many(vec!["k1", "k2", "k3", "k4", "k1"]);
        assert_eq!(store.dirty_count(), 4);
    }

    #[test]
    fn mirrors_mark_dirty_does_not_touch_deleted_keys() {
        let mut store = StateStore::new();
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

// Task 2 (push_batch_no_features) and Task 3
// (push_batch_with_cascade_no_features) modules are appended by those tasks.
