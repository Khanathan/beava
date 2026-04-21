//! Phase 59.7 Wave 1 — unit tests for `TypedRingBuffer{I64,F64,Avg}` +
//! `TypedRingBufferEnum`.
//!
//! These live in an integration test binary (rather than an in-module
//! `#[cfg(test)]`) because the pre-existing Phase 60 salt sweep blocks
//! `cargo test --lib` builds (33 `E0063: missing field 'salt'` errors on
//! `StreamDefinition` literals in `src/`, documented as deferred in
//! `.planning/phases/59.6-typed-pipeline-records/deferred-items.md`). An
//! integration test binary compiles against the `beava` library with the
//! default (`--cfg test` only for tests/) build, sidestepping the
//! test-mode-only `salt` compile errors in the lib.
//!
//! Parity contract: these tests pin the ring-buffer semantics port from
//! `src/engine/window.rs::RingBuffer<T>` into the monomorphized typed
//! twins. The integration harness `tests/typed_windowed_aggregation_parity.rs`
//! closes the loop vs. Value-path ops.

use beava::engine::event_time::DropReason;
use beava::engine::operators_typed_aggs_windowed::{
    TypedRingBufferAvg, TypedRingBufferF64, TypedRingBufferI64, TypedRingBufferVariantHint,
};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

fn ts(secs: u64) -> SystemTime {
    UNIX_EPOCH + Duration::from_secs(secs)
}

#[test]
fn test_typed_ring_buffer_i64_steady_state() {
    // window=5s, bucket=1s → 5 buckets.
    let mut rb = TypedRingBufferI64::new(Duration::from_secs(5), Duration::from_secs(1));
    for s in 0..5u64 {
        rb.update_at_event_time(|b| *b += 1, ts(1000 + s));
    }
    assert_eq!(rb.sum_all(), 5);
    // Advance to t=1010 → full window past all prior buckets, they all expire.
    rb.update_at_event_time(|b| *b += 1, ts(1010));
    assert_eq!(rb.sum_all(), 1);
}

#[test]
fn test_typed_ring_buffer_f64_historical_bucket() {
    // Establish head at t=1003, then t=1004, then reach back to t=1002
    // (historical, in-window).
    let mut rb = TypedRingBufferF64::new(Duration::from_secs(5), Duration::from_secs(1));
    rb.update_at_event_time(|b| *b += 1.5, ts(1003));
    rb.update_at_event_time(|b| *b += 1.5, ts(1004));
    rb.update_at_event_time(|b| *b += 1.5, ts(1002));
    assert!((rb.sum_all() - 4.5).abs() < 1e-9);
}

#[test]
fn test_typed_ring_buffer_i64_too_old_drop() {
    // window=5s, bucket=1s. Advance to t=1010, then try to insert at
    // t=1003 (1010-5=1005 is the window floor; 1003 < 1005 → TooOld).
    let mut rb = TypedRingBufferI64::new(Duration::from_secs(5), Duration::from_secs(1));
    rb.update_at_event_time(|b| *b += 1, ts(1010));
    assert_eq!(rb.sum_all(), 1);
    rb.update_at_event_time(|b| *b += 99, ts(1003));
    assert_eq!(rb.sum_all(), 1, "event dropped, sum unchanged");
    assert_eq!(rb.take_last_drop(), Some(DropReason::TooOld));
}

#[test]
fn test_typed_ring_buffer_avg_packed() {
    let mut rb = TypedRingBufferAvg::new(Duration::from_secs(5), Duration::from_secs(1));
    rb.update_at_event_time(
        |b| {
            b.0 += 5.0;
            b.1 += 1;
        },
        ts(1000),
    );
    rb.update_at_event_time(
        |b| {
            b.0 += 10.0;
            b.1 += 1;
        },
        ts(1001),
    );
    rb.update_at_event_time(
        |b| {
            b.0 += 15.0;
            b.1 += 1;
        },
        ts(1002),
    );
    let (s, c) = rb.sum_all();
    assert!((s - 30.0).abs() < 1e-9);
    assert_eq!(c, 3);
    assert!((s / c as f64 - 10.0).abs() < 1e-9);
}

#[test]
fn test_allocated_bytes_reports_nonzero() {
    let rb_i64 = TypedRingBufferI64::new(Duration::from_secs(5), Duration::from_secs(1));
    let rb_f64 = TypedRingBufferF64::new(Duration::from_secs(5), Duration::from_secs(1));
    let rb_avg = TypedRingBufferAvg::new(Duration::from_secs(5), Duration::from_secs(1));
    assert!(rb_i64.allocated_bytes() >= 5 * std::mem::size_of::<i64>());
    assert!(rb_f64.allocated_bytes() >= 5 * std::mem::size_of::<f64>());
    assert!(rb_avg.allocated_bytes() >= 5 * std::mem::size_of::<(f64, i64)>());
}

#[test]
fn test_typed_ring_buffer_enum_variant_dispatch() {
    let mut e_i64 = TypedRingBufferVariantHint::I64
        .construct(Duration::from_secs(5), Duration::from_secs(1));
    e_i64.as_i64_mut().update_at_event_time(|b| *b += 7, ts(1000));
    assert_eq!(e_i64.as_i64().sum_all(), 7);

    let mut e_f64 = TypedRingBufferVariantHint::F64
        .construct(Duration::from_secs(5), Duration::from_secs(1));
    e_f64.as_f64_mut().update_at_event_time(|b| *b += 2.5, ts(1000));
    assert!((e_f64.as_f64().sum_all() - 2.5).abs() < 1e-9);

    let mut e_avg = TypedRingBufferVariantHint::Avg
        .construct(Duration::from_secs(5), Duration::from_secs(1));
    e_avg.as_avg_mut().update_at_event_time(
        |b| {
            b.0 += 4.0;
            b.1 += 1;
        },
        ts(1000),
    );
    let (s, c) = e_avg.as_avg().sum_all();
    assert!((s - 4.0).abs() < 1e-9);
    assert_eq!(c, 1);
}

#[test]
#[should_panic(expected = "variant mismatch")]
fn test_typed_ring_buffer_enum_variant_mismatch_panics() {
    let mut e = TypedRingBufferVariantHint::I64
        .construct(Duration::from_secs(5), Duration::from_secs(1));
    let _ = e.as_f64_mut();
}
