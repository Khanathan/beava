//! Plan 12-07 Wave 8 — read-path criterion microbench.
//!
//! Measures `dispatch_get_single_sync` and `dispatch_get_batch_sync` against a
//! warm-cache `AppState` populated with a Txn -> TxnAgg(cnt) pipeline. Excludes
//! wire encode/decode + socket I/O — that's the realm of the throughput run
//! (`crates/beava-bench-v18`). Per CLAUDE.md §Performance Discipline (Phase 6+
//! rule), this anchor lives in `.planning/perf-baselines.md` for regression
//! detection (10% warn, 25% block thresholds in same hw-class).
//!
//! Bench groups:
//!   - read_path/get_single
//!   - read_path/get_batch/10x5
//!   - read_path/get_batch/100x1   (PERF-02 shape: 100 features × 1 entity)
//!   - read_path/get_batch/100x5

use beava_core::registry::Registry;
use beava_persistence::{WalSink, WalSinkConfig};
use beava_runtime_core::wal_buffer::WalBufferRing;
use beava_runtime_core::wal_lsn::WalLsn;
use beava_runtime_core::wire_request::WireRequest;
use beava_server::apply_shard::ApplyShard;
use beava_server::idem_cache::IdemCache;
use beava_server::runtime_core_glue::{dispatch_get_batch_sync, dispatch_get_single_sync};
use beava_server::AppState;
use bytes::Bytes;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use std::sync::Arc;

/// Build an AppState with `n_keys` entities each receiving `events_per_key` Txn
/// events.  Returned `(Arc<AppState>, _wal_dir, _rt)` — caller must keep the
/// returned tuple alive for the bench duration so WAL + tokio runtime stay live.
fn setup_warm_app_state(
    n_keys: usize,
    events_per_key: usize,
) -> (Arc<AppState>, tempfile::TempDir, tokio::runtime::Runtime) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .expect("rt");
    let _guard = rt.enter();

    let wal_dir = tempfile::tempdir().expect("wal");
    let (wal_sink, _wal_worker) = WalSink::spawn(WalSinkConfig {
        dir: wal_dir.path().to_path_buf(),
        initial_start_lsn: 1,
        initial_registry_version: 1,
        fsync_interval_ms: 100,
        fsync_bytes: 0,
        segment_bytes: 64 * 1024 * 1024,
        sync_mode: beava_persistence::SyncMode::Periodic,
    })
    .expect("wal spawn");

    let registry = Arc::new(Registry::new());
    let dev_agg = beava_server::registry_debug::DevAggState::new(Arc::clone(&registry));
    let idem_cache = Arc::new(IdemCache::new());
    let app_state = Arc::new(AppState::new(dev_agg, wal_sink, idem_cache));

    let wal_lsn = Arc::new(WalLsn::new());
    let wal_ring = Arc::new(WalBufferRing::new(3, 1 << 24, Arc::clone(&wal_lsn)));
    let shard = ApplyShard::new(Arc::clone(&app_state), wal_ring, wal_lsn);

    // Register Txn -> TxnAgg(cnt by user_id).
    let reg = serde_json::json!({
        "nodes": [
            {
                "kind": "event",
                "name": "Txn",
                "schema": {
                    "fields": {
                        "event_time": "i64",
                        "user_id": "str",
                        "amount": "f64"
                    },
                    "optional_fields": []
                },
                "event_time_field": "event_time"
            },
            {
                "kind": "derivation",
                "name": "TxnAgg",
                "output_kind": "table",
                "upstreams": ["Txn"],
                "ops": [{"op": "group_by", "keys": ["user_id"], "agg": {
                    "cnt": {"op": "count", "params": {}}
                }}],
                "schema": {
                    "fields": {"user_id": "str", "cnt": "i64"},
                    "optional_fields": []
                },
                "table_primary_key": ["user_id"]
            }
        ]
    });
    let _ = shard.dispatch_wire_request_with_row(
        WireRequest::Register {
            payload: Bytes::from(serde_json::to_vec(&reg).unwrap()),
        },
        None,
    );

    // Push events_per_key for each of n_keys.
    let mut event_time = 1_000i64;
    for k in 0..n_keys {
        let key_str = format!("alice_{k}");
        for _ in 0..events_per_key {
            let body = serde_json::json!({
                "event_time": event_time,
                "user_id": key_str,
                "amount": 1.0
            });
            let push_bytes = serde_json::to_vec(&body).unwrap();
            let _ = shard.dispatch_wire_request_with_row(
                WireRequest::HttpPush {
                    event_name: "Txn".to_string(),
                    body: Bytes::from(push_bytes),
                    body_format: beava_core::wire::CT_JSON,
                },
                None,
            );
            event_time += 1;
        }
    }

    drop(_guard);
    (app_state, wal_dir, rt)
}

fn bench_get_single(c: &mut Criterion) {
    // 1000 keys × 10 events per key = 10k pushes; alice_500 has cnt=10.
    let (app, _wal_dir, _rt) = setup_warm_app_state(1000, 10);
    c.bench_function("read_path/get_single", |b| {
        b.iter(|| {
            let resp = dispatch_get_single_sync(
                criterion::black_box(&app),
                criterion::black_box("cnt"),
                criterion::black_box("alice_500"),
                criterion::black_box(beava_core::wire::CT_JSON),
            );
            criterion::black_box(resp);
        })
    });
}

fn bench_get_batch(c: &mut Criterion) {
    let (app, _wal_dir, _rt) = setup_warm_app_state(1000, 10);
    for &(n_keys, n_features) in &[(10usize, 5usize), (100usize, 1usize), (100usize, 5usize)] {
        let keys: Vec<String> = (0..n_keys).map(|i| format!("alice_{i}")).collect();
        // Repeat the only feature ("cnt") n_features times so the benchmark
        // measures cell-count work without needing a multi-feature pipeline.
        let features: Vec<&str> = std::iter::repeat("cnt").take(n_features).collect();
        let body_json = serde_json::json!({"keys": keys, "features": features});
        let body = Bytes::from(serde_json::to_vec(&body_json).unwrap());
        c.bench_with_input(
            BenchmarkId::new("read_path/get_batch", format!("{n_keys}x{n_features}")),
            &body,
            |b, body| {
                b.iter(|| {
                    let resp = dispatch_get_batch_sync(
                        criterion::black_box(&app),
                        criterion::black_box(body),
                        criterion::black_box(beava_core::wire::CT_JSON),
                    );
                    criterion::black_box(resp);
                })
            },
        );
    }
}

criterion_group!(read_path_benches, bench_get_single, bench_get_batch);
criterion_main!(read_path_benches);
