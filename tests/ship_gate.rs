// SHIP-01 (+ CORR-01 + CORR-05 + CORR-06): single integration test exercising
// live-ingest → crash (drop) → recover (replay from event log) → read features.
//
// Exercises:
//   CORR-01: push_batch_with_cascade_no_features uses per-event _event_time
//            bucketing (batch events with distinct event times land in distinct
//            buckets, not the same shared-wall-clock bucket).
//   CORR-05: backfill uses the single-event path (push_for_backfill), not the
//            batch path. Verified structurally by test_backfill_uses_single_event_path.rs;
//            this test observes the end result.
//   CORR-06: run_backfill uses parse_event_time(&payload, entry.timestamp) so
//            crash-replay produces bit-identical features to live-ingest.
//            Before D-15, recovered features differed from live features for
//            events carrying explicit _event_time payloads.
//
// Design: the test drives the engine directly (handle_push_batch / run_backfill)
// rather than going through a TCP socket or HTTP layer. This gives full control
// over event timing and runs sub-second in practice.
//
// Target runtime: under 30 seconds on any modern laptop.

use std::sync::atomic::AtomicUsize;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde_json::json;

use beava::engine::event_time::parse_event_time;
use beava::engine::pipeline::{FeatureDef, PipelineEngine, StreamDefinition};
use beava::server::tcp::{
    handle_push_batch, make_concurrent_state, run_backfill, BackfillStatus, BackfillTracker,
    PendingAsync, SharedState,
};
use beava::state::event_log::EventLog;
use beava::state::store::StateStore;
use beava::types::FeatureMap;

// ---------------------------------------------------------------------------
// Stream definition
// ---------------------------------------------------------------------------

/// Stream with a 2-hour count window using 1-minute buckets.
/// We use 2h so that events spread across the last 90 minutes are always within
/// the window at read time.
fn txns_stream_def() -> StreamDefinition {
    StreamDefinition {
        name: "Txns".into(),
        key_field: Some("user".into()),
        group_by_keys: None,
        features: vec![(
            "count_2h".into(),
            FeatureDef::Count {
                window: Duration::from_secs(2 * 3600),
                bucket: Duration::from_secs(60),
                where_expr: None,
                backfill: true, // backfill=true so re-register triggers run_backfill
            },
        )],
        depends_on: None,
        filter: None,
        entity_ttl: None,
        history_ttl: Some(Duration::from_secs(7 * 24 * 3600)), // 7-day log retention
        projection: None,
        ephemeral: None,
        pipeline_ttl: None,
        max_keys: None,
        watermark_lateness: None,
        shard_key: None,    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Build a SharedState backed by a real EventLog in `log_dir`.
fn make_state_with_log(log_dir: &std::path::Path) -> SharedState {
    let event_log = EventLog::new(log_dir.to_path_buf()).expect("EventLog::new");
    make_concurrent_state(
        PipelineEngine::new(),
        StateStore::new(),
        Some(event_log),
        log_dir.join("ship_gate.snapshot"),
        Arc::new(BackfillTracker::default()),
        false, // snapshot_enabled — not needed for this test
        true,  // event_log_enabled
    )
}

/// Generate N synthetic events for keys u0..u9 with explicit `_event_time` values
/// spread across the last 90 minutes. All events fall within the 2h window at
/// read time, so count_2h will be non-zero for every key.
///
/// The spread exercises per-event bucketing (CORR-01): events at different minutes
/// land in different 1-minute ring-buffer buckets.
fn synth_events_with_event_times(n: usize) -> (Vec<serde_json::Value>, SystemTime) {
    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;

    // Spread events over the last 90 minutes (5400 seconds = 5_400_000 ms).
    // Events earlier in the list are further in the past (more varied buckets).
    let spread_ms: u64 = 90 * 60 * 1000; // 90 minutes in ms

    let events: Vec<serde_json::Value> = (0..n)
        .map(|i| {
            // Linear spread: event 0 is now-90min, event n-1 is now.
            let offset_ms = (i as u64 * spread_ms) / n.max(1) as u64;
            let et_ms = now_ms.saturating_sub(spread_ms) + offset_ms;
            json!({
                "user": format!("u{}", i % 10),
                "_event_time": et_ms,
                "amount": (i * 7) % 1000,
            })
        })
        .collect();

    // The read-time: use the latest event_time + 1s to ensure all events
    // are within the 2h window at this read point.
    let max_et_ms = now_ms;
    let read_time = UNIX_EPOCH + Duration::from_millis(max_et_ms + 1000);

    (events, read_time)
}

/// Push `events` to stream `stream_name` via `handle_push_batch`.
/// `handle_push_batch` also writes to the EventLog (if enabled), so events
/// survive a simulated crash.
fn push_and_log(state: &SharedState, stream_name: &str, events: &[serde_json::Value]) {
    let wall = SystemTime::now();

    // Build PendingAsync batch with per-event event-time (CORR-01 path).
    //
    // raw_payload is intentionally empty so make_log_payload uses the LOG_FMT_JSON
    // path. If raw_payload is non-empty, make_log_payload emits LOG_FMT_BINARY,
    // which run_backfill decodes via decode_event_binary (TCP binary wire format).
    // Plain JSON bytes in a LOG_FMT_BINARY frame would be skipped by run_backfill.
    let batch: Vec<PendingAsync> = events
        .iter()
        .enumerate()
        .map(|(seq, payload)| {
            let et = parse_event_time(payload, wall);
            PendingAsync {
                seq: seq as u64,
                stream_name: stream_name.to_string(),
                payload: payload.clone(),
                raw_payload: vec![], // empty → LOG_FMT_JSON in the event log
                now: et,
            }
        })
        .collect();

    // handle_push_batch writes to engine AND to the event log (if enabled).
    let _results = handle_push_batch(state, &batch);
}

/// Read features for all keys u0..u9 from the state store at `read_time`.
/// Returns a sorted Vec of (key, feature_map) pairs.
/// Uses `store.get_all_features` directly to avoid engine derive overhead.
fn read_features_all_keys(state: &SharedState, read_time: SystemTime) -> Vec<(String, FeatureMap)> {
    let mut out: Vec<(String, FeatureMap)> = (0..10)
        .map(|i| {
            let key = format!("u{}", i);
            let fm = state.store.get_all_features(&key, read_time);
            (key, fm)
        })
        .collect();

    out.sort_by(|a, b| a.0.cmp(&b.0));
    out
}

/// Trigger backfill for `stream_name` on `state` by reading the event log and
/// spawning `run_backfill`. Waits (via yield loop) until the backfill completes.
async fn trigger_and_wait_backfill(state: &SharedState, stream_name: &str) {
    // Flush the event log before reading entries.
    if let Some(ref log) = state.event_log {
        let _ = log.fsync_all();
    }

    let entries = state
        .event_log
        .as_ref()
        .map(|log| log.read_entries(stream_name).unwrap_or_default())
        .unwrap_or_default();

    assert!(
        !entries.is_empty(),
        "trigger_and_wait_backfill: no entries in event log for stream '{}'; \
         the event log was not populated during live ingest or was not flushed to disk",
        stream_name
    );

    let features: Vec<String> = {
        let engine = state.engine.read();
        engine
            .get_stream(stream_name)
            .map(|s| s.features.iter().map(|(n, _)| n.clone()).collect())
            .unwrap_or_default()
    };

    assert!(
        !features.is_empty(),
        "trigger_and_wait_backfill: stream '{}' not registered or has no features",
        stream_name
    );

    let total = entries.len();
    let status = Arc::new(BackfillStatus {
        stream: stream_name.to_string(),
        features: features.clone(),
        total_events: total,
        processed_events: Arc::new(AtomicUsize::new(0)),
        started_at: SystemTime::now(),
        completed_at: std::sync::Mutex::new(None),
    });

    {
        state
            .backfill_tracker
            .tasks
            .lock()
            .unwrap()
            .push(Arc::clone(&status));
    }

    let state_clone = state.clone();
    tokio::spawn(run_backfill(
        state_clone,
        stream_name.to_string(),
        features,
        entries,
        Arc::clone(&status),
    ));

    // Yield repeatedly until backfill marks completed_at, with a 5-second timeout.
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        tokio::task::yield_now().await;
        let done = status
            .completed_at
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .is_some();
        if done {
            return;
        }
        if std::time::Instant::now() > deadline {
            panic!(
                "Backfill for stream '{}' did not complete within 5 seconds \
                 (processed {} / {} events)",
                stream_name,
                status
                    .processed_events
                    .load(std::sync::atomic::Ordering::Relaxed),
                total,
            );
        }
    }
}

// ---------------------------------------------------------------------------
// SHIP-01: main integration test
// ---------------------------------------------------------------------------

/// SHIP-01: live-ingest → crash (drop) → recover from event log → assert parity.
///
/// Exercises CORR-01 (per-event _event_time bucketing in batch path),
/// CORR-05 (backfill uses single-event path), and CORR-06 (run_backfill uses
/// payload _event_time via parse_event_time, not entry.timestamp wall-clock).
#[tokio::test]
async fn test_ship_gate_backfill_crash_recover() {
    // TempDir held OUTSIDE the state_live scope so dropping state_live does not
    // delete the directory (simulates a crash that preserves disk state).
    let tmp = tempfile::TempDir::new().expect("tempdir");
    let data_dir = tmp.path().to_owned();

    // Generate events ONCE so both Phase A and Phase B use the same event sequence.
    // Events span the last 90 minutes with distinct per-event _event_time values.
    let (events, read_time) = synth_events_with_event_times(200);

    // -----------------------------------------------------------------------
    // Phase A: live ingest
    // -----------------------------------------------------------------------

    let live_features = {
        let state_live = make_state_with_log(&data_dir);

        // Register "Txns" stream.
        {
            let mut engine = state_live.engine.write();
            engine.register(txns_stream_def()).unwrap();
        }
        // Register stream with the event log (creates/opens the log file).
        if let Some(ref log) = state_live.event_log {
            log.register_stream("Txns", txns_stream_def().history_ttl)
                .expect("register_stream in EventLog");
        }

        // Push 200 events. handle_push_batch writes to engine + event log.
        push_and_log(&state_live, "Txns", &events);

        // Flush the event log to disk before crash (ensures entries are readable
        // on recovery; corresponds to the background fsync timer).
        if let Some(ref log) = state_live.event_log {
            log.fsync_all().expect("fsync before crash");
        }

        // Snapshot live features for all keys at read_time.
        let features = read_features_all_keys(&state_live, read_time);

        // Sanity: at least some keys must have a non-zero count.
        let has_nonzero = features.iter().any(|(_, fm)| {
            fm.get("count_2h")
                .and_then(|v| match v {
                    beava::types::FeatureValue::Int(n) => Some(*n),
                    _ => None,
                })
                .unwrap_or(0)
                > 0
        });
        assert!(
            has_nonzero,
            "Phase A: no key has a non-zero count_2h after live ingest. \
             Events may all be outside the 2h window. \
             live_features = {features:?}"
        );

        // DROP state_live — simulates kill -9 (no clean shutdown).
        drop(state_live);
        features
    };

    // -----------------------------------------------------------------------
    // Phase B: recover from event log
    // -----------------------------------------------------------------------

    let rec_features = {
        let state_rec = make_state_with_log(&data_dir);

        // Register the same stream definition with backfill=true.
        {
            let mut engine = state_rec.engine.write();
            engine.register(txns_stream_def()).unwrap();
        }
        // Register stream with event log (opens the existing log file).
        if let Some(ref log) = state_rec.event_log {
            log.register_stream("Txns", txns_stream_def().history_ttl)
                .expect("register_stream in EventLog (recover)");
        }

        // Explicitly trigger backfill and wait for completion.
        trigger_and_wait_backfill(&state_rec, "Txns").await;

        // Read recovered features at the same read_time as Phase A.
        read_features_all_keys(&state_rec, read_time)
    };

    // -----------------------------------------------------------------------
    // Phase C: assert bit-identical parity
    // -----------------------------------------------------------------------

    assert_eq!(
        live_features.len(),
        rec_features.len(),
        "SHIP-01: different number of keys between live ({}) and recovered ({}) runs",
        live_features.len(),
        rec_features.len()
    );

    let mut mismatches = 0usize;
    for ((live_key, live_fm), (rec_key, rec_fm)) in live_features.iter().zip(rec_features.iter()) {
        assert_eq!(
            live_key, rec_key,
            "SHIP-01: key ordering mismatch between live and recovered runs"
        );
        if live_fm != rec_fm {
            mismatches += 1;
            eprintln!(
                "SHIP-01 mismatch for key '{live_key}':\n  live:      {live_fm:?}\n  recovered: {rec_fm:?}"
            );
        }
    }

    assert_eq!(
        mismatches, 0,
        "SHIP-01 / CORR-06: {mismatches} key(s) have different features between live and \
         recovered runs. run_backfill must use parse_event_time(&payload, entry.timestamp) \
         (D-15) to bucket by payload _event_time, not entry.timestamp wall-clock."
    );

    // Reaching here means CORR-01, CORR-05, CORR-06 are all GREEN.
}
