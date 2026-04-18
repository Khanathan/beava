//! Raw in-process benchmark for `replica_ingest_batch`.
//!
//! Answers the question: "what's the actual per-event cost of the replica
//! catchup path, stripped of subprocess spawn, TCP, snapshot IO, and
//! `/debug/ready` polling?"
//!
//! Builds 5M synthetic events against a `count_1h` pipeline over 1000 keys
//! (matching `benchmark/fork-replay` shape), then times:
//!   1. single-event `replica_ingest` — baseline
//!   2. `replica_ingest_batch` @ batch=1000 — post-perf path
//! and prints EPS, ns/event, and wall-clock split.
//!
//! Run with:
//!   cargo test --release --test bench_replica_ingest_raw \
//!     -- --nocapture --ignored raw_throughput_bench
//!
//! (The #[ignore] gate keeps it out of `cargo test --release` default so
//! CI doesn't spend 30 s on a 5 M-event loop.)

use std::sync::Arc;
use std::time::{Duration, Instant};

use serde_json::json;

use beava::engine::pipeline::{FeatureDef, PipelineEngine, StreamDefinition};
use beava::server::tcp::{
    make_concurrent_state_full, replica_ingest, replica_ingest_batch, BackfillTracker, SharedState,
};
use beava::state::event_log::{EventLog, LOG_FMT_JSON};
use beava::state::store::StateStore;

fn count_stream() -> StreamDefinition {
    StreamDefinition {
        name: "events".into(),
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
    }
}

fn make_state(event_log_enabled: bool) -> (SharedState, tempfile::TempDir) {
    let tmp = tempfile::tempdir().unwrap();
    let mut engine = PipelineEngine::new();
    engine.register(count_stream()).unwrap();
    let event_log = if event_log_enabled {
        let log = EventLog::new(tmp.path().to_path_buf()).unwrap();
        log.register_stream("events", None).unwrap();
        Some(log)
    } else {
        None
    };
    let state = make_concurrent_state_full(
        engine,
        StateStore::new(),
        event_log,
        tmp.path().join("snap"),
        Arc::new(BackfillTracker::default()),
        false,
        event_log_enabled,
        None,
        false,
    );
    (state, tmp)
}

fn wrap_json(v: &serde_json::Value) -> Vec<u8> {
    let body = serde_json::to_vec(v).unwrap();
    let mut out = Vec::with_capacity(1 + body.len());
    out.push(LOG_FMT_JSON);
    out.extend_from_slice(&body);
    out
}

fn build_events(n: usize, entities: usize) -> Vec<(String, u64, Vec<u8>)> {
    let base_ts: u64 = 1_700_000_000_000;
    let mut out = Vec::with_capacity(n);
    for i in 0..n {
        let uid = format!("u{}", i % entities);
        let ts_ms = base_ts + (i as u64) * 1_000;
        let wrapped = wrap_json(&json!({"user_id": uid, "amount": (i % 37) as i64}));
        out.push(("events".into(), ts_ms, wrapped));
    }
    out
}

fn fmt_eps(events: usize, elapsed: Duration) -> String {
    let eps = events as f64 / elapsed.as_secs_f64();
    let ns_per = elapsed.as_nanos() as f64 / events as f64;
    format!(
        "{:.3} s  ({:.0} EPS, {:.0} ns/event)",
        elapsed.as_secs_f64(),
        eps,
        ns_per
    )
}

#[test]
#[ignore] // gated — opt-in perf bench, not part of CI test run
fn raw_throughput_bench() {
    // Tunable via env so we can run a 500K warm-up or full 5M without
    // editing the source.
    let n: usize = std::env::var("BENCH_EVENTS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(5_000_000);
    let entities: usize = std::env::var("BENCH_ENTITIES")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1_000);
    let batch_size: usize = std::env::var("BENCH_BATCH")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1_000);
    let event_log: bool = std::env::var("BENCH_EVENT_LOG")
        .ok()
        .map(|s| s == "1")
        .unwrap_or(true);

    eprintln!(
        "raw-throughput bench: N={} entities={} batch={} event_log={}",
        n, entities, batch_size, event_log
    );

    eprintln!("building events...");
    let t0 = Instant::now();
    let events = build_events(n, entities);
    let build_s = t0.elapsed();
    let bytes: usize = events.iter().map(|(_, _, b)| b.len()).sum();
    eprintln!(
        "built {} events ({:.1} MiB) in {:.2}s",
        n,
        bytes as f64 / 1_048_576.0,
        build_s.as_secs_f64()
    );

    // ------- PATH A: single-event replica_ingest -------
    let (state_a, _tmp_a) = make_state(event_log);
    eprintln!("\n[A] single-event replica_ingest loop (no batching)");
    let t_a = Instant::now();
    for (stream, ts_ms, raw) in &events {
        replica_ingest(&state_a, stream, *ts_ms, raw).unwrap();
    }
    let a_elapsed = t_a.elapsed();
    eprintln!("    total: {}", fmt_eps(n, a_elapsed));

    // ------- PATH B: replica_ingest_batch @ configured batch size -------
    let (state_b, _tmp_b) = make_state(event_log);
    eprintln!(
        "\n[B] replica_ingest_batch @ batch={} (current production path)",
        batch_size
    );
    let t_b = Instant::now();
    for chunk in events.chunks(batch_size) {
        replica_ingest_batch(&state_b, chunk).unwrap();
    }
    let b_elapsed = t_b.elapsed();
    eprintln!("    total: {}", fmt_eps(n, b_elapsed));

    // ------- PATH C: one giant batch call (upper bound — no batching overhead) -------
    let (state_c, _tmp_c) = make_state(event_log);
    eprintln!(
        "\n[C] replica_ingest_batch @ batch={} (single mega-batch)",
        n
    );
    let t_c = Instant::now();
    replica_ingest_batch(&state_c, &events).unwrap();
    let c_elapsed = t_c.elapsed();
    eprintln!("    total: {}", fmt_eps(n, c_elapsed));

    eprintln!("\n=== summary ===");
    eprintln!("[A] single-event      : {}", fmt_eps(n, a_elapsed));
    eprintln!("[B] batch=1k          : {}", fmt_eps(n, b_elapsed));
    eprintln!("[C] mega-batch        : {}", fmt_eps(n, c_elapsed));
    eprintln!(
        "B vs A speedup: {:.2}×",
        a_elapsed.as_secs_f64() / b_elapsed.as_secs_f64()
    );
    eprintln!(
        "C vs B headroom: {:.2}×",
        b_elapsed.as_secs_f64() / c_elapsed.as_secs_f64()
    );
}
