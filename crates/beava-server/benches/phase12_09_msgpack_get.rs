//! Plan 12-09 Wave 7 — read-path msgpack vs JSON criterion microbench.
//!
//! Measures `dispatch_get_single_sync` and `dispatch_get_batch_sync` against
//! a warm-cache `AppState` populated with a Txn -> TxnAgg(cnt) pipeline,
//! comparing CT_JSON vs CT_MSGPACK body+response on each shape. Excludes
//! wire encode/decode + socket I/O.
//!
//! Bench groups (each shape × 2 codecs = 6 cells):
//!   - read_path/get_single_json   /  read_path/get_single_msgpack         (1 cell)
//!   - read_path/get_batch_10x5_json  /  …_msgpack                         (50 cells)
//!   - read_path/get_batch_100x5_json /  …_msgpack                         (500 cells)
//!
//! Per CLAUDE.md §Performance Discipline (Phase 6+ rule), this anchor lives
//! in `.planning/perf-baselines.md` for regression detection (10% warn,
//! 25% block thresholds in same hw-class).
//!
//! Predicted lift: msgpack path ≥ 40% faster than JSON path on the 100x5
//! shape (post-12-07 60.99 µs JSON → ≤ 36 µs msgpack target). The lift
//! comes entirely from elimination of `serde_json` parse on body + JSON
//! encode on response — `rmp_serde::from_slice` walks bytes in a tight
//! state machine vs `serde_json::from_slice`'s string-allocating path.

use beava_core::registry::Registry;
use beava_core::wire::{CT_JSON, CT_MSGPACK};
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

/// Build an AppState with `n_keys` entities × `events_per_key` Txn events.
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
                    body_format: CT_JSON,
                },
                None,
            );
            event_time += 1;
        }
    }

    drop(_guard);
    (app_state, wal_dir, rt)
}

/// 1-cell get_single bench, JSON vs MsgPack (response body only — single
/// path doesn't parse a request body, so the lift is purely on the encode
/// side).
fn bench_get_single(c: &mut Criterion) {
    let (app, _wal_dir, _rt) = setup_warm_app_state(1000, 10);
    c.bench_function("read_path/get_single_json", |b| {
        b.iter(|| {
            let resp = dispatch_get_single_sync(
                criterion::black_box(&app),
                criterion::black_box("cnt"),
                criterion::black_box("alice_500"),
                criterion::black_box(CT_JSON),
            );
            criterion::black_box(resp);
        })
    });
    c.bench_function("read_path/get_single_msgpack", |b| {
        b.iter(|| {
            let resp = dispatch_get_single_sync(
                criterion::black_box(&app),
                criterion::black_box("cnt"),
                criterion::black_box("alice_500"),
                criterion::black_box(CT_MSGPACK),
            );
            criterion::black_box(resp);
        })
    });
}

/// Batch get_single + multi-key/feature shapes — measures the parse + encode
/// savings together.
fn bench_get_batch(c: &mut Criterion) {
    let (app, _wal_dir, _rt) = setup_warm_app_state(1000, 10);
    for &(n_keys, n_features) in &[(10usize, 5usize), (100usize, 5usize)] {
        let keys: Vec<String> = (0..n_keys).map(|i| format!("alice_{i}")).collect();
        let features: Vec<&str> = std::iter::repeat("cnt").take(n_features).collect();
        let body_json_obj = serde_json::json!({"keys": keys, "features": features});
        let body_json = Bytes::from(serde_json::to_vec(&body_json_obj).unwrap());
        let body_msgpack = Bytes::from(rmp_serde::to_vec_named(&body_json_obj).unwrap());

        c.bench_with_input(
            BenchmarkId::new("read_path/get_batch_json", format!("{n_keys}x{n_features}")),
            &body_json,
            |b, body| {
                b.iter(|| {
                    let resp = dispatch_get_batch_sync(
                        criterion::black_box(&app),
                        criterion::black_box(body),
                        criterion::black_box(CT_JSON),
                    );
                    criterion::black_box(resp);
                })
            },
        );
        c.bench_with_input(
            BenchmarkId::new(
                "read_path/get_batch_msgpack",
                format!("{n_keys}x{n_features}"),
            ),
            &body_msgpack,
            |b, body| {
                b.iter(|| {
                    let resp = dispatch_get_batch_sync(
                        criterion::black_box(&app),
                        criterion::black_box(body),
                        criterion::black_box(CT_MSGPACK),
                    );
                    criterion::black_box(resp);
                })
            },
        );
    }
}

criterion_group!(msgpack_get_benches, bench_get_single, bench_get_batch);
criterion_main!(msgpack_get_benches);
