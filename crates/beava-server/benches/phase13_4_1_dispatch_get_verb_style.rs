//! Plan 13.4.1-05 Wave 4 (Task 5.c) — verb-style dispatch microbench
//! (perf-discipline gate per CLAUDE.md §Performance Discipline).
//!
//! Phase 13.4.1 migrated the server-side `POST /get` + `OP_GET` +
//! `POST /batch_get` + `OP_BATCH_GET` request bodies to the locked
//! verb-style wire contract (`{table, key, features?}` per-entry; FLAT
//! row response). The change is a serde shape flip that is structurally
//! NEUTRAL-to-POSITIVE on dispatch hot-path EPS:
//!   - `BatchGetReqEntry` custom `Deserialize` adds ~10 ns/entry vs the
//!     prior `derive(Deserialize)`.
//!   - The features-filter `iter().any(...)` per-feature check is
//!     O(features × filter_len), but `filter_len` is typically 1-3 names
//!     (~15 ns per feature).
//!   - The flattened response constructor (`Value::Object(feature_map)`
//!     push vs `json!({"table": ..., "entity_id": ..., "features": ...})`)
//!     is FASTER — one allocation vs four.
//!
//! The throughput-baselines small/tcp gate (Plan 13.4.1-05 Task 5.b) is
//! the canonical end-to-end perf gate for this phase. This focused
//! microbench is the criterion-level companion: it isolates the dispatch
//! hot path from the transport stack so that future regressions in JUST
//! the dispatch code (without touching the transport) will be caught
//! even if end-to-end EPS stays in the noise band.
//!
//! Cells:
//!   - `verb_style_dispatch/get_single_1feat`
//!     (single-row `dispatch_get_single_verb_style_sync` with a 1-feature filter)
//!   - `verb_style_dispatch/get_batch_10x1feat`
//!     (10-entry `dispatch_batch_get_sync` with per-entry 1-feature filter)
//!
//! Bench bootstrap mirrors `crates/beava-server/benches/phase12_09_msgpack_get.rs`
//! (warm `AppState` populated with a Txn → TxnAgg(cnt) pipeline, 1000 keys × 10
//! events). The verb-style migration is read-side only, so the warm-AppState
//! shape is reused unchanged.
//!
//! Per CLAUDE.md §Performance Discipline plan-checker contract for Phase 6+
//! (every plan MUST include at least one task whose `files_modified` contains
//! a path under `crates/*/benches/`); this file satisfies that contract for
//! Phase 13.4.1.
//!
//! See `.planning/phases/13.4.1-server-wire-spec-verb-style-migration/13.4.1-05-PLAN.md`.

use beava_core::registry::Registry;
use beava_core::wire::{CT_JSON, CT_MSGPACK};
use beava_persistence::{WalSink, WalSinkConfig};
use beava_runtime_core::wal_buffer::WalBufferRing;
use beava_runtime_core::wal_lsn::WalLsn;
use beava_runtime_core::wire_request::WireRequest;
use beava_server::apply_shard::{dispatch_batch_get_sync, ApplyShard};
use beava_server::idem_cache::IdemCache;
use beava_server::runtime_core_glue::dispatch_get_single_verb_style_sync;
use beava_server::AppState;
use bytes::Bytes;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use std::sync::Arc;

/// Build an AppState with `n_keys` entities × `events_per_key` Txn events
/// keyed by `user_id` and routed through a single `TxnAgg(cnt)` derivation.
/// Mirrors the warm-AppState bootstrap pattern in `phase12_09_msgpack_get.rs`.
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

/// Cell 1: `dispatch_get_single_verb_style_sync` with a 1-feature filter.
/// Exercises the verb-style single-row hot path landed by Plan 13.4.1-04.
fn bench_get_single_verb_style(c: &mut Criterion) {
    let (app, _wal_dir, _rt) = setup_warm_app_state(1000, 10);
    let features: Vec<String> = vec!["cnt".to_string()];
    c.bench_function("verb_style_dispatch/get_single_1feat", |b| {
        b.iter(|| {
            let resp = dispatch_get_single_verb_style_sync(
                criterion::black_box(&app),
                criterion::black_box("TxnAgg"),
                criterion::black_box("alice_500"),
                criterion::black_box(Some(features.as_slice())),
                criterion::black_box(CT_JSON),
            );
            criterion::black_box(resp);
        })
    });
}

/// Cell 2: `dispatch_batch_get_sync` with a 10-entry batch + per-entry
/// 1-feature filter. Exercises the verb-style batch hot path landed by
/// Plan 13.4.1-04 (FLAT-row response, per-entry features filter, custom
/// `BatchGetReqEntry` Deserialize).
fn bench_get_batch_verb_style(c: &mut Criterion) {
    let (app, _wal_dir, _rt) = setup_warm_app_state(1000, 10);
    let n_keys = 10usize;
    let requests: Vec<serde_json::Value> = (0..n_keys)
        .map(|i| {
            serde_json::json!({
                "table": "TxnAgg",
                "key": format!("alice_{i}"),
                "features": ["cnt"]
            })
        })
        .collect();
    let body_obj = serde_json::json!({"requests": requests});
    let body_json = Bytes::from(serde_json::to_vec(&body_obj).unwrap());
    let body_msgpack = Bytes::from(rmp_serde::to_vec_named(&body_obj).unwrap());

    c.bench_with_input(
        BenchmarkId::new(
            "verb_style_dispatch/get_batch_json",
            format!("{n_keys}x1feat"),
        ),
        &body_json,
        |b, body| {
            b.iter(|| {
                let resp = dispatch_batch_get_sync(
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
            "verb_style_dispatch/get_batch_msgpack",
            format!("{n_keys}x1feat"),
        ),
        &body_msgpack,
        |b, body| {
            b.iter(|| {
                let resp = dispatch_batch_get_sync(
                    criterion::black_box(&app),
                    criterion::black_box(body),
                    criterion::black_box(CT_MSGPACK),
                );
                criterion::black_box(resp);
            })
        },
    );
}

criterion_group!(
    phase13_4_1_dispatch_benches,
    bench_get_single_verb_style,
    bench_get_batch_verb_style
);
criterion_main!(phase13_4_1_dispatch_benches);
