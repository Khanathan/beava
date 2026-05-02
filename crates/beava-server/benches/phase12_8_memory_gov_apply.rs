//! Phase 12.8 Plan 08 — memory governance apply microbench.
//!
//! Two cells covering the cold-entity TTL apply hot-path overhead introduced
//! by Plans 12.8-02 / 12.8-03 / 12.8-06:
//!
//! * `phase12_8/cold_ttl_disabled` — `Tx.group_by(user_id).count()` per push
//!   with `cold_after_ms = None` on the source (the v0 default — no opt-in).
//!   Measures the post-Plan-04 / post-Plan-06 baseline cost; expected to
//!   match Phase 12.7's `phase12_6/simple_counter` cell within ±5% so we can
//!   confirm Plan 02's `Option<u64>` field add is zero-cost on the read side
//!   and Plan 03's eviction check skips entirely when `cold_after_ms.is_none()`.
//!
//! * `phase12_8/cold_ttl_enabled` — same shape but with
//!   `cold_after_ms = Some(30 * 86_400_000)` (30 days, well above any
//!   100-event-batch wall-clock so no actual eviction fires; just measures
//!   the steady-state CHECK cost). Per Plan 03 the cost is a single
//!   `Option::is_some` branch (~1 ns) + 1 `last_seen_u64` HashMap lookup
//!   (FxBuildHasher on a `u64` key, ~10-15 ns) + 1 `last_seen_u64` HashMap
//!   insert/update via `raw_entry_mut::from_key` on the warm path
//!   (~15-20 ns). Total budget ~30-35 ns/event over the disabled baseline.
//!
//! Per CLAUDE.md §Performance Discipline (Phase 6+ rule), the captured
//! medians are recorded in `.planning/perf-baselines.md` § Phase 12.8 for
//! 10% / 25% regression detection in the same hw-class. Per CONTEXT.md
//! "inline-cheap" constraint (D-04 / D-01), the per-event regression
//! between cells must be <5%; otherwise Plan 03's claim that cold-TTL
//! eviction is "negligible amortized cost" is falsified.
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
//!   `crates/beava-bench/`'s throughput runs).
//! - Cold-path register compile (a single Register dispatch happens during
//!   harness setup, then is amortized across all bench iters).
//! - The actual eviction PATH (the 30-day TTL is well above the bench
//!   wall-clock; we measure the check cost, not the eviction cost — eviction
//!   is the cold path here).
//!
//! ## Why no separate windowed / sketch cells in 12.8?
//!
//! Plan 03's eviction check is the only new code on the apply hot path; it
//! runs once per `apply_event_to_aggregations` call regardless of which
//! AggOp variants the source has registered. Measuring 1 cell-pair on the
//! simple-counter shape isolates the new cost cleanly. The full
//! windowed / sketch coverage already exists at the
//! `phase12_6_post_axum_kill_apply.rs` site (and will continue to be
//! re-measured when downstream phases touch the windowed-op or sketch
//! state machines).
//!
//! Bootstrap is shared via the `common` module (see `common.rs`).

mod common;

use beava_runtime_core::wire_request::WireRequest;
use bytes::Bytes;
use criterion::{black_box, criterion_group, criterion_main, BatchSize, Criterion};
use serde_json::json;

/// Number of events per batch — big enough that criterion's per-iteration
/// overhead is amortized to <1% of measured time, small enough that warm
/// state-table growth doesn't dominate. Matches the Phase 12.6 / 12.7
/// `phase12_6_post_axum_kill_apply.rs` convention so cross-cell numbers
/// remain comparable.
const BATCH_SIZE: usize = 100;

/// 30 days in milliseconds. Used by the `cold_ttl_enabled` cell as the TTL
/// threshold. Picked specifically because:
///
/// 1. It's well above any 100-event-batch wall-clock — no actual eviction
///    will fire during the bench loop, so we measure the steady-state
///    *check* cost in isolation.
/// 2. It mirrors the documented v0 typical use case from CONTEXT.md:
///    `@bv.event(cold_after='30d')` — measuring at the realistic operating
///    point.
const COLD_TTL_30_DAYS_MS: u64 = 30 * 86_400_000;

/// Build a `register payload` for a Tx → TxCount (count grouped by user_id)
/// pipeline with the given optional cold_after_ms on the source.
///
/// Mirrors the shape used by `phase12_6/simple_counter` (and Phase 12.6 /
/// 12.7's bench file) so the disabled cell can be cross-validated against
/// the 12.7 baseline. The only difference is the `cold_after_ms` field on
/// the event node.
fn register_payload(cold_after_ms: Option<u64>) -> serde_json::Value {
    json!({
        "nodes": [
            {
                "kind": "event",
                "name": "Tx",
                "schema": {
                    "fields": {"user_id": "str", "amount": "f64"},
                    "optional_fields": []
                },
                "cold_after_ms": cold_after_ms,
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
    })
}

/// Drive a 100-event batch through `dispatch_wire_request_with_row` with the
/// given event_name. Shared between the two cells — the only inter-cell
/// difference is which harness was bootstrapped in setup.
#[inline(always)]
fn drive_batch(harness: &common::BenchHarness, bodies: Vec<Bytes>) {
    for body in bodies {
        let req = WireRequest::HttpPush {
            event_name: harness.event_name.clone(),
            body,
            body_format: beava_core::wire::CT_JSON,
        };
        let resp = harness.shard.dispatch_wire_request_with_row(req, None);
        black_box(resp);
    }
}

/// Build a fresh batch of 100 distinct-user events. Same shape as
/// `phase12_6/simple_counter`: 100 entities, round-robin via `i % 100`.
#[inline(always)]
fn build_batch() -> Vec<Bytes> {
    (0..BATCH_SIZE as u64)
        .map(|i| {
            let body = json!({
                "user_id": format!("u{}", i % 100),
                "amount": 42.0_f64,
            });
            Bytes::from(serde_json::to_vec(&body).unwrap())
        })
        .collect()
}

/// Pre-warm 1000 events (100 entities × 10 events) so the per-entity init
/// cost is amortized across all bench iters; matches the 12.6 / 12.7
/// `simple_counter` warm-up exactly.
fn prewarm(harness: &common::BenchHarness) {
    for i in 0..1_000_u64 {
        let body = json!({
            "user_id": format!("u{}", i % 100),
            "amount": 42.0_f64,
        });
        let req = WireRequest::HttpPush {
            event_name: harness.event_name.clone(),
            body: Bytes::from(serde_json::to_vec(&body).unwrap()),
            body_format: beava_core::wire::CT_JSON,
        };
        let _ = harness.shard.dispatch_wire_request_with_row(req, None);
    }
}

// ─── Cell 1: cold_ttl_disabled (cold_after_ms = None — Plan 02 baseline) ─────

fn bench_cold_ttl_disabled(c: &mut Criterion) {
    let harness = common::build_apply_shard_with_pipeline(register_payload(None));
    prewarm(&harness);

    c.bench_function("phase12_8/cold_ttl_disabled/100_events", |b| {
        b.iter_batched(build_batch, |bodies| drive_batch(&harness, bodies), BatchSize::SmallInput);
    });
}

// ─── Cell 2: cold_ttl_enabled (cold_after_ms = Some(30d) — Plan 03 check cost) ─

fn bench_cold_ttl_enabled(c: &mut Criterion) {
    let harness =
        common::build_apply_shard_with_pipeline(register_payload(Some(COLD_TTL_30_DAYS_MS)));
    prewarm(&harness);

    c.bench_function("phase12_8/cold_ttl_enabled/100_events", |b| {
        b.iter_batched(build_batch, |bodies| drive_batch(&harness, bodies), BatchSize::SmallInput);
    });
}

criterion_group!(benches, bench_cold_ttl_disabled, bench_cold_ttl_enabled);
criterion_main!(benches);
