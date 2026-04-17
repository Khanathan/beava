// SHIP-01 (+ CORR-01 + CORR-05 + CORR-06): single integration test exercising
// HTTP push -> crash (drop) -> recover (replay from event log) -> read features.
//
// RED until Phase 46 Wave 3 (D-15) patches run_backfill to use
// parse_event_time(&payload, entry.timestamp) instead of entry.timestamp.
// Without that fix, recovered features diverge from live-ingest features for
// events that carry an explicit _event_time payload field.
//
// ALSO exercises CORR-01 (batch path group-by-bucket) and CORR-05 (backfill
// uses single-event path — verified separately in
// tests/test_backfill_uses_single_event_path.rs).
//
// Plan 08 un-ignores this test and fills in the helper bodies.
// The Arrange/Act/Assert skeleton below is intentionally detailed so Plan 08
// has no design decisions to make — only implementation work.

use std::time::SystemTime;

/// Boot a server pointed at `data_dir`, register stream "Txns", push N events
/// with explicit _event_time payloads, read the features for entity "u1", then
/// drop (crash) the server.  Returns the feature snapshot captured before crash.
///
/// TODO (Wave 3 / Plan 08): implement using make_concurrent_state + TCP client.
async fn _boot_push_crash(
    _data_dir: &std::path::Path,
) -> Vec<(String, serde_json::Value)> {
    unimplemented!("Wave 3 (D-15) / Plan 08: fill helper body")
}

/// Boot a server pointed at an existing `data_dir` (triggers run_backfill on
/// startup), then read the features for entity "u1".
///
/// TODO (Wave 3 / Plan 08): implement using make_concurrent_state + TCP client.
async fn _boot_recover(
    _data_dir: &std::path::Path,
) -> Vec<(String, serde_json::Value)> {
    unimplemented!("Wave 3 (D-15) / Plan 08: fill helper body")
}

/// Generate N synthetic events for entity "u1" with explicit _event_time values
/// spread over the past 2 hours.  The first event is exactly 1 hour in the past
/// (bucket-boundary stress per D-04 / CORR-01).
///
/// TODO (Wave 3 / Plan 08): implement.
fn _synth_events(n: usize) -> Vec<serde_json::Value> {
    let _ = n;
    unimplemented!("Wave 3 (D-15) / Plan 08: fill helper body")
}

#[tokio::test]
#[ignore = "Phase 46 Wave 3 (D-15): run_backfill uses entry.timestamp wall-clock; crash-replay feature-parity will fail"]
async fn test_ship_gate_backfill_crash_recover() {
    let tmp = tempfile::TempDir::new().expect("tempdir");
    let data_dir = tmp.path().to_owned();

    // Phase A — live ingest via TCP/HTTP push of N events with known _event_time
    // values.  Features are snapshotted before the server is dropped.
    let live_features = _boot_push_crash(&data_dir).await;
    assert!(!live_features.is_empty(), "live ingest produced no features");

    // Phase B — recover from disk: server boots, run_backfill replays the event
    // log, features are read back.
    let rec_features = _boot_recover(&data_dir).await;
    assert!(!rec_features.is_empty(), "recovery produced no features");

    // Phase C — assert bit-identical parity.
    // If run_backfill uses entry.timestamp (wall-clock), features bucket
    // differently from the live-ingest path that used _event_time, so this
    // assertion will fail until D-15 is applied.
    assert_eq!(
        live_features, rec_features,
        "CORR-06: crash-replay must produce identical features to live ingest \
         for events with explicit _event_time; mismatch means run_backfill is \
         using wall-clock (entry.timestamp) instead of payload event-time"
    );

    panic!("MISSING: Wave 3 (D-15) must patch run_backfill to use parse_event_time; Plan 08 un-ignores and fills helpers");
}
