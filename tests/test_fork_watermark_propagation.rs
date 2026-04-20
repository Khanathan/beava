// CORR-08: replica_ingest_batch must call engine.wm_observe() per event
// so fork-replica watermarks advance and downstream table cascades are not stalled.
//
// Verifies that after calling replica_ingest_batch with N events, the engine's
// watermarks.observed_max(stream) >= the largest event_time in the batch.
// Before D-19 this would return None (no observe() call); after D-19 it returns
// Some(t) >= max_ts_ms.

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde_json::json;

use beava::engine::pipeline::{FeatureDef, PipelineEngine, StreamDefinition};
use beava::server::tcp::{make_concurrent_state_full, replica_ingest_batch, BackfillTracker};
use beava::state::event_log::{EventLog, LOG_FMT_JSON};
use beava::state::store::StateStore;

fn txns_stream() -> StreamDefinition {
    StreamDefinition {
        name: "Txns".into(),
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

fn wrap_json(v: &serde_json::Value) -> Vec<u8> {
    let body = serde_json::to_vec(v).unwrap();
    let mut out = Vec::with_capacity(1 + body.len());
    out.push(LOG_FMT_JSON);
    out.extend_from_slice(&body);
    out
}

#[test]
#[ignore = "54-01 Pass C: replica_ingest_batch now routes through SPSC (handle_push_core_ex), which requires spawn_shard_threads; this test builds state without spawning shard threads. Migrated by 54-03 Wave 3 (same class as Pass B's 12 ignored tests)."]
fn replica_batch_advances_watermark() {
    let tmp = std::env::temp_dir().join(format!(
        "beava_fork_wm_{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&tmp).unwrap();

    let mut engine = PipelineEngine::new();
    engine.register(txns_stream()).unwrap();
    let event_log = EventLog::new(tmp.clone()).unwrap();
    event_log.register_stream("Txns", None).unwrap();

    let state = make_concurrent_state_full(
        engine,
        StateStore::new(),
        Some(event_log),
        tmp.join("snapshot"),
        Arc::new(BackfillTracker::default()),
        false,
        true,
        None,
        false,
        1,
    );

    // Build 10 events spanning 1 hour, each 6 minutes apart.
    // base_ts is 2023-11-14T00:00:00Z in milliseconds.
    let base_ts_ms: u64 = 1_700_000_000_000;
    let events: Vec<(String, u64, Vec<u8>)> = (0u64..10)
        .map(|i| {
            let ts_ms = base_ts_ms + i * 360_000; // 6 min steps
            let payload = json!({ "user_id": format!("u{}", i % 3), "amount": i });
            ("Txns".to_string(), ts_ms, wrap_json(&payload))
        })
        .collect();

    let largest_ts_ms = events.iter().map(|(_, t, _)| *t).max().unwrap();

    // Ingest via replica batch path.
    let n_applied =
        replica_ingest_batch(&state, &events).expect("replica_ingest_batch must succeed");
    assert_eq!(n_applied, events.len(), "all 10 events must be applied");

    // CORR-08: observed_max must be Some and >= largest ts_ms in the batch.
    let engine_guard = state.engine.read();
    let observed = engine_guard
        .wm_observed_max("Txns")
        .expect("CORR-08: wm_observed_max(Txns) must be Some after replica_ingest_batch");

    let observed_ms = observed.duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;

    assert!(
        observed_ms >= largest_ts_ms,
        "CORR-08: observed_max ({}) must be >= largest ts_ms ({}) in the batch",
        observed_ms,
        largest_ts_ms
    );

    let _ = std::fs::remove_dir_all(&tmp);
}
