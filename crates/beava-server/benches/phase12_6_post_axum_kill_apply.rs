//! Phase 12.6 Plan 11 — post-axum-kill apply microbench.
//!
//! Three cells covering a cross-section of operator work after the legacy
//! axum kill (Plan 12.6-07), event-time strip (Plan 12.6-06), and Path X
//! time-source swap (Plan 12.6-05):
//!
//! * `phase12_6/simple_counter` — `Tx.group_by(user_id).count()` per push.
//!   Dominant cost: HashMap lookup + Count bookkeeping. Sensitive to:
//!   `dispatch_push_sync` overhead (parse + descriptor lookup + WAL append).
//! * `phase12_6/sketch_heavy` — `Tx.group_by(user_id).count_distinct(session_id, window=1h)`.
//!   Dominant cost: HLL update (CountDistinct in HashSet mode after warmup).
//!   Sensitive to: identity-hasher path (Phase 19.4-A landed the fix).
//! * `phase12_6/windowed_60s_sum` — `Tx.group_by(user_id).sum(amount, window=60s)`.
//!   Dominant cost: WindowedOp bucket index + fold. Sensitive to: Path X
//!   parameter rename + the `SystemTime::now()` syscall in
//!   `dispatch_push_sync` (replaces the body-derived event_time read).
//!
//! Per CLAUDE.md §Performance Discipline (Phase 6+ rule), the captured
//! medians are recorded in `.planning/perf-baselines.md` § Phase 12.6 for
//! 10% / 25% regression detection in the same hw-class.
//!
//! ## Bench shape
//!
//! Each cell drives `ApplyShard::dispatch_wire_request_with_row` via a
//! criterion `iter_batched` loop with a 100-event batch (so per-iteration
//! amortization stays around 100×, well above criterion's noise floor). The
//! reported number is *per-batch ns*; divide by 100 for ns/event.
//!
//! ## What this bench excludes
//!
//! - Wire encode/decode (no socket I/O — that's the realm of
//!   `crates/beava-bench-v18` throughput runs).
//! - Cold-path register compile (a single Register dispatch happens during
//!   harness setup, then is amortized across all bench iters).
//! - Legacy axum data plane (deleted by Plan 12.6-07).
//!
//! Bootstrap is shared via the `common` module (see `common.rs`).

mod common;

use beava_runtime_core::wire_request::WireRequest;
use bytes::Bytes;
use criterion::{black_box, criterion_group, criterion_main, BatchSize, Criterion};
use serde_json::json;

/// Number of events per batch — big enough that criterion's per-iteration
/// overhead is amortized to <1% of measured time, small enough that warm
/// state-table growth doesn't dominate.
const BATCH_SIZE: usize = 100;

// ─── Cell 1: simple counter (HashMap-bookkeeping dominated) ──────────────────

fn bench_simple_counter(c: &mut Criterion) {
    let payload = json!({
        "nodes": [
            {
                "kind": "event",
                "name": "Tx",
                "schema": {
                    "fields": {"user_id": "str", "amount": "f64"},
                    "optional_fields": []
                },
            },
            {
                "kind": "derivation",
                "name": "TxCount",
                "output_kind": "table",
                "upstreams": ["Tx"],
                "ops": [{
                    "op": "group_by",
                    "keys": ["user_id"],
                    "agg": {
                        "cnt": {"op": "count", "params": {}}
                    }
                }],
                "schema": {
                    "fields": {"user_id": "str", "cnt": "i64"},
                    "optional_fields": []
                },
                "table_primary_key": ["user_id"]
            }
        ]
    });
    let harness = common::build_apply_shard_with_pipeline(payload);

    // Pre-warm 100 keys × 10 events (the per-entity init cost is amortized
    // across all bench iters; the bench measures the steady-state push).
    let prewarm_bodies: Vec<Bytes> = (0..1_000_u64)
        .map(|i| {
            let body = json!({
                "user_id": format!("u{}", i % 100),
                "amount": 42.0_f64,
            });
            Bytes::from(serde_json::to_vec(&body).unwrap())
        })
        .collect();
    for body in &prewarm_bodies {
        let req = WireRequest::HttpPush {
            event_name: harness.event_name.clone(),
            body: body.clone(),
            body_format: beava_core::wire::CT_JSON,
        };
        let _ = harness.shard.dispatch_wire_request_with_row(req, None);
    }

    c.bench_function("phase12_6/simple_counter/100_events", |b| {
        b.iter_batched(
            || {
                (0..BATCH_SIZE as u64)
                    .map(|i| {
                        let body = json!({
                            "user_id": format!("u{}", i % 100),
                            "amount": 42.0_f64,
                        });
                        Bytes::from(serde_json::to_vec(&body).unwrap())
                    })
                    .collect::<Vec<Bytes>>()
            },
            |bodies| {
                for body in bodies {
                    let req = WireRequest::HttpPush {
                        event_name: harness.event_name.clone(),
                        body,
                        body_format: beava_core::wire::CT_JSON,
                    };
                    let resp = harness.shard.dispatch_wire_request_with_row(req, None);
                    black_box(resp);
                }
            },
            BatchSize::SmallInput,
        );
    });
}

// ─── Cell 2: sketch_heavy (CountDistinct / HLL update path) ──────────────────

fn bench_sketch_heavy(c: &mut Criterion) {
    let payload = json!({
        "nodes": [
            {
                "kind": "event",
                "name": "Tx",
                "schema": {
                    "fields": {"user_id": "str", "session_id": "str"},
                    "optional_fields": []
                },
            },
            {
                "kind": "derivation",
                "name": "TxUniqueSessions",
                "output_kind": "table",
                "upstreams": ["Tx"],
                "ops": [{
                    "op": "group_by",
                    "keys": ["user_id"],
                    "agg": {
                        "uniq": {
                            "op": "n_unique",
                            "params": {"field": "session_id", "window": "1h"}
                        }
                    }
                }],
                "schema": {
                    "fields": {"user_id": "str", "uniq": "i64"},
                    "optional_fields": []
                },
                "table_primary_key": ["user_id"]
            }
        ]
    });
    let harness = common::build_apply_shard_with_pipeline(payload);

    // Pre-warm: drive 1000 events with 100 distinct user_ids and unique
    // session_ids so CountDistinct promotes past EXACT_THRESHOLD (16) into
    // HashSet mode — the hot path Plan 19.4-A's identity-hasher optimization
    // targets. Without this warmup, the bench would measure ExactArray mode
    // and miss the regression-tripwire purpose.
    for i in 0..1_000_u64 {
        let body = json!({
            "user_id": format!("u{}", i % 100),
            "session_id": format!("s{}", i),
        });
        let req = WireRequest::HttpPush {
            event_name: harness.event_name.clone(),
            body: Bytes::from(serde_json::to_vec(&body).unwrap()),
            body_format: beava_core::wire::CT_JSON,
        };
        let _ = harness.shard.dispatch_wire_request_with_row(req, None);
    }

    let mut iter_seed: u64 = 1_000;
    c.bench_function("phase12_6/sketch_heavy/100_events", |b| {
        b.iter_batched(
            || {
                let bodies: Vec<Bytes> = (0..BATCH_SIZE as u64)
                    .map(|i| {
                        // Use a continuously-incrementing seed so each batch
                        // draws fresh session_ids — keeps CountDistinct in
                        // HashSet mode (otherwise the dedup hits same-key
                        // fast paths after a few iters).
                        let seed = iter_seed.wrapping_add(i);
                        let body = json!({
                            "user_id": format!("u{}", seed % 100),
                            "session_id": format!("s{}", seed),
                        });
                        Bytes::from(serde_json::to_vec(&body).unwrap())
                    })
                    .collect();
                iter_seed = iter_seed.wrapping_add(BATCH_SIZE as u64);
                bodies
            },
            |bodies| {
                for body in bodies {
                    let req = WireRequest::HttpPush {
                        event_name: harness.event_name.clone(),
                        body,
                        body_format: beava_core::wire::CT_JSON,
                    };
                    let resp = harness.shard.dispatch_wire_request_with_row(req, None);
                    black_box(resp);
                }
            },
            BatchSize::SmallInput,
        );
    });
}

// ─── Cell 3: windowed_60s_sum (WindowedOp bucket fold + SystemTime::now) ─────

fn bench_windowed_60s_sum(c: &mut Criterion) {
    let payload = json!({
        "nodes": [
            {
                "kind": "event",
                "name": "Tx",
                "schema": {
                    "fields": {"user_id": "str", "amount": "f64"},
                    "optional_fields": []
                },
            },
            {
                "kind": "derivation",
                "name": "TxAmt60s",
                "output_kind": "table",
                "upstreams": ["Tx"],
                "ops": [{
                    "op": "group_by",
                    "keys": ["user_id"],
                    "agg": {
                        "amt_60s": {
                            "op": "sum",
                            "params": {"field": "amount", "window": "60s"}
                        }
                    }
                }],
                "schema": {
                    "fields": {"user_id": "str", "amt_60s": "f64"},
                    "optional_fields": []
                },
                "table_primary_key": ["user_id"]
            }
        ]
    });
    let harness = common::build_apply_shard_with_pipeline(payload);

    // Pre-warm: 1000 events across 100 user_ids so windowed buckets are
    // populated and per-entity allocation cost is amortized.
    for i in 0..1_000_u64 {
        let body = json!({
            "user_id": format!("u{}", i % 100),
            "amount": (i % 1000) as f64,
        });
        let req = WireRequest::HttpPush {
            event_name: harness.event_name.clone(),
            body: Bytes::from(serde_json::to_vec(&body).unwrap()),
            body_format: beava_core::wire::CT_JSON,
        };
        let _ = harness.shard.dispatch_wire_request_with_row(req, None);
    }

    let mut iter_seed: u64 = 1_000;
    c.bench_function("phase12_6/windowed_60s_sum/100_events", |b| {
        b.iter_batched(
            || {
                let bodies: Vec<Bytes> = (0..BATCH_SIZE as u64)
                    .map(|i| {
                        let seed = iter_seed.wrapping_add(i);
                        let body = json!({
                            "user_id": format!("u{}", seed % 100),
                            "amount": (seed % 1000) as f64,
                        });
                        Bytes::from(serde_json::to_vec(&body).unwrap())
                    })
                    .collect();
                iter_seed = iter_seed.wrapping_add(BATCH_SIZE as u64);
                bodies
            },
            |bodies| {
                for body in bodies {
                    let req = WireRequest::HttpPush {
                        event_name: harness.event_name.clone(),
                        body,
                        body_format: beava_core::wire::CT_JSON,
                    };
                    let resp = harness.shard.dispatch_wire_request_with_row(req, None);
                    black_box(resp);
                }
            },
            BatchSize::SmallInput,
        );
    });
}

criterion_group!(
    benches,
    bench_simple_counter,
    bench_sketch_heavy,
    bench_windowed_60s_sum
);
criterion_main!(benches);
