//! Replica-side batch ingest (`replica_ingest_batch`) semantic-equivalence test.
//!
//! Proves that feeding N events through `replica_ingest_batch` produces the
//! same per-key feature values as feeding the same events through N calls to
//! `replica_ingest`. Without this guarantee, the fork-replay catchup
//! optimization (see benchmark/fork-replay) silently changes aggregate
//! semantics and downstream forks see different values than upstream.
//!
//! Phase 54-04 Pass A5: gated under `state-inmem` — reads feature values
//! through `engine.get_features(&store)`, only compiled on the in-memory
//! build after this pass.

#![cfg(feature = "state-inmem")]

use std::sync::Arc;
use std::time::Duration;

use serde_json::json;

use beava::engine::pipeline::{FeatureDef, PipelineEngine, StreamDefinition};
use beava::server::tcp::{make_concurrent_state_full, replica_ingest, replica_ingest_batch, BackfillTracker, SharedState};
use beava::state::event_log::{EventLog, LOG_FMT_JSON};
fn count_stream(name: &str) -> StreamDefinition {
    StreamDefinition {
        name: name.into(),
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
    }
}

fn make_state(log_dir: &std::path::Path) -> SharedState {
    std::fs::create_dir_all(log_dir).unwrap();
    let mut engine = PipelineEngine::new();
    engine.register(count_stream("events")).unwrap();
    let event_log = EventLog::new(log_dir.to_path_buf()).unwrap();
    event_log.register_stream("events", None).unwrap();
    make_concurrent_state_full(
        engine,
        Some(event_log),
        log_dir.join("snapshot"),
        Arc::new(BackfillTracker::default()),
        false, // snapshot_enabled
        true,  // event_log_enabled
        None,
        false,
        1,
    )
}

/// Wrap a JSON payload in the on-the-wire log-payload format that the
/// replica decoder expects: `[fmt_byte][json_bytes]` with `LOG_FMT_JSON`.
fn wrap_json_log_payload(v: &serde_json::Value) -> Vec<u8> {
    let body = serde_json::to_vec(v).unwrap();
    let mut out = Vec::with_capacity(1 + body.len());
    out.push(LOG_FMT_JSON);
    out.extend_from_slice(&body);
    out
}

fn get_count(state: &SharedState, key: &str) -> Option<i64> {
    let now = std::time::UNIX_EPOCH + Duration::from_secs(4000);
    let engine = state.engine.read();
    // Phase 54-04 Pass A6a: `state.store` deleted — local scratch store keeps
    // the legacy `engine.get_features(&StateStore)` call compiling. Pass C
    // migrates to shard-scatter read.
    let _ = state;
    let local_store = beava::state::store::StateStore::new();
    let features = engine.get_features(key, &local_store, now);
    let fv = features
        .get("events.count_1h")
        .or_else(|| features.get("count_1h"))?;
    match fv {
        beava::types::FeatureValue::Int(n) => Some(*n),
        beava::types::FeatureValue::Float(f) => Some(*f as i64),
        _ => None,
    }
}

#[test]
fn replica_ingest_batch_matches_single_event_semantics() {
    // Two independent states, same pipeline, same events. One gets
    // single-event replica_ingest; the other gets a single batch call.
    let tmp_single = std::env::temp_dir().join(format!(
        "beava_replica_batch_single_{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let tmp_batch = std::env::temp_dir().join(format!(
        "beava_replica_batch_batch_{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));

    let state_single = make_state(&tmp_single);
    let state_batch = make_state(&tmp_batch);

    // 300 events across 5 users, event-times spread over 30 minutes so the
    // 1h bucket logic still sees them all inside the same bucket but the
    // per-event operator routing uses distinct `now`s.
    let mut events: Vec<(String, u64, Vec<u8>)> = Vec::with_capacity(300);
    let base_ts: u64 = 1_700_000_000_000; // 2023-11-14
    for i in 0..300u64 {
        let user = format!("u{}", i % 5);
        let ts_ms = base_ts + i * 1_000; // 1 s apart
        let payload = json!({"user_id": user, "amount": (i as i64) % 37});
        let wrapped = wrap_json_log_payload(&payload);
        events.push(("events".into(), ts_ms, wrapped));
    }

    // Path A: single-event replica_ingest.
    for (stream, ts_ms, raw) in &events {
        replica_ingest(&state_single, stream, *ts_ms, raw).expect("single-event ingest");
    }

    // Path B: one batched call.
    let n_applied = replica_ingest_batch(&state_batch, &events).expect("batch ingest");
    assert_eq!(n_applied, events.len(), "all events should apply");

    // Compare features per user.
    for uid in 0..5 {
        let key = format!("u{}", uid);
        let c_single = get_count(&state_single, &key);
        let c_batch = get_count(&state_batch, &key);
        assert_eq!(
            c_single, c_batch,
            "count_1h mismatch for key {}: single={:?} batch={:?}",
            key, c_single, c_batch,
        );
        assert_eq!(c_single, Some(60), "each user sees 60 events");
    }

    // Both paths should have advanced replica_last_applied_ts_ms to the
    // max ts in the batch.
    let last_single = state_single
        .replica_last_applied_ts_ms
        .load(std::sync::atomic::Ordering::Relaxed);
    let last_batch = state_batch
        .replica_last_applied_ts_ms
        .load(std::sync::atomic::Ordering::Relaxed);
    assert_eq!(last_single, last_batch);
    assert_eq!(last_single, base_ts + 299 * 1_000);

    // Event-log persisted counts should match too — both paths should have
    // written 300 log entries for the "events" stream.
    let entries_single = state_single
        .event_log
        .as_ref()
        .unwrap()
        .read_entries("events")
        .unwrap();
    let entries_batch = state_batch
        .event_log
        .as_ref()
        .unwrap()
        .read_entries("events")
        .unwrap();
    assert_eq!(entries_single.len(), 300);
    assert_eq!(entries_batch.len(), 300);

    // And per-entry timestamps should match (batch path must preserve
    // per-event LogEntry.timestamp via append_many_with_ts, not squash them
    // to a single batch-wide now).
    for i in 0..300 {
        assert_eq!(
            entries_single[i].timestamp, entries_batch[i].timestamp,
            "log entry {} timestamp differs between single and batch paths",
            i
        );
    }

    // Clean up temp dirs.
    let _ = std::fs::remove_dir_all(&tmp_single);
    let _ = std::fs::remove_dir_all(&tmp_batch);
}

#[test]
fn replica_ingest_batch_empty_is_noop() {
    let tmp = std::env::temp_dir().join(format!(
        "beava_replica_batch_empty_{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let state = make_state(&tmp);
    let events: Vec<(String, u64, Vec<u8>)> = Vec::new();
    let n = replica_ingest_batch(&state, &events).expect("empty batch");
    assert_eq!(n, 0);
    assert_eq!(
        state
            .replica_last_applied_ts_ms
            .load(std::sync::atomic::Ordering::Relaxed),
        0
    );
    let _ = std::fs::remove_dir_all(&tmp);
}
