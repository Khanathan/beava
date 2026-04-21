//! Phase 59.7 Wave 1 (TPC-PERF-11 extension / TPC-CORR-07 extension) —
//! 10 windowed typed-agg parity tests. Wave 1 flips the first 4 GREEN
//! (Count, Sum i64, Sum f64, Avg f64); Wave 2 flips the remaining 6
//! (Min i64/f64, Max i64/f64, Last InlineStr, First InlineStr) GREEN as
//! the matching typed impls land.
//!
//! # Contract
//!
//! window = 5s, bucket = 1s → 5 buckets per entity.
//! 100K events across a 30s event-time range (event_time = start +
//! i * 300µs). Events expire mid-stream by construction (window fits
//! ~16K events). Checkpoints every 1.5s of event-time; at each
//! checkpoint we read `FeatureValue` from BOTH paths and `assert_eq!`.
//!
//! # Why Wave 1 only hits 4 of 10 tests
//!
//! Wave 1 ships the simple-numeric ring buffers (TypedRingBufferI64 /
//! TypedRingBufferF64 / TypedRingBufferAvg) + Count/Sum/Avg windowed
//! ops. Wave 2 ships the per-bucket-min/max wrappers + inline-str
//! sliding windows for Min/Max/Last/First. The 6 remaining tests stay
//! `#[ignore = "59.7-W2"]` until that lands.
//!
//! # Why drive through Shard (not the op directly)
//!
//! The windowed typed ops' state lives on `Shard::entity_ringbuffers_typed`
//! (side-map, per D-C1). Driving through `Shard` exercises the full
//! update_windowed / read_feature_windowed trait contract — the same
//! surface the Wave-4 cascade walker will call. Value-path ops are
//! driven through their `Operator::push` / `read` trait directly.

#![allow(unused_imports, dead_code)]

use beava::engine::operators::{
    AvgOp, CountOp as CountOpValue, Operator, SumOp as SumOpValue,
};
use beava::engine::operators_typed::TypedAggOp;
use beava::engine::operators_typed_aggs_windowed::{
    AvgOpTypedWindowedF64, CountOpTypedWindowed, SumOpTypedWindowedF64, SumOpTypedWindowedI64,
};
use beava::engine::schema::{FieldSpec, FieldTy, RegisteredSchema, Row};
use beava::shard::Shard;
use beava::types::FeatureValue;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const N_EVENTS: usize = 100_000;
const STREAM: &str = "Txns";
const ENTITY: &str = "u1";

fn window() -> Duration {
    Duration::from_secs(5)
}
fn bucket() -> Duration {
    Duration::from_secs(1)
}

/// Base event-time for the stream. Anchored far after UNIX_EPOCH so
/// bucket-start alignment is well-defined.
fn base_time() -> SystemTime {
    UNIX_EPOCH + Duration::from_secs(1_000_000_000)
}

/// Monotone event_time for index `i`: base + i * 300µs. At i=100K the
/// stream spans 30s (100_000 * 300µs).
fn event_time_at(i: usize) -> SystemTime {
    base_time() + Duration::from_micros((i * 300) as u64)
}

fn event_schema_num() -> Arc<RegisteredSchema> {
    // [user_id: inline_str@0 | amount: f64@16 | qty: i64@24]
    let s = RegisteredSchema {
        schema_id: 0,
        name: "Txns".into(),
        fields: vec![
            FieldSpec {
                name: "user_id".into(),
                ty: FieldTy::InlineStr,
                offset: 0,
                nullable: false,
            },
            FieldSpec {
                name: "amount".into(),
                ty: FieldTy::F64,
                offset: 16,
                nullable: false,
            },
            FieldSpec {
                name: "qty".into(),
                ty: FieldTy::I64,
                offset: 24,
                nullable: false,
            },
        ],
        inline_str_cap: 15,
        row_size: 32,
    };
    s.validate_layout().expect("valid");
    Arc::new(s)
}

fn state_schema_dummy() -> Arc<RegisteredSchema> {
    // Windowed ops store no state in the Row (state lives on the shard
    // side-map). One filler field keeps the schema well-formed.
    let s = RegisteredSchema {
        schema_id: 0,
        name: "WState".into(),
        fields: vec![FieldSpec {
            name: "_".into(),
            ty: FieldTy::I64,
            offset: 0,
            nullable: false,
        }],
        inline_str_cap: 15,
        row_size: 8,
    };
    s.validate_layout().expect("valid");
    Arc::new(s)
}

fn make_event(amount: f64, qty: i64) -> (Row, serde_json::Value) {
    let sch = event_schema_num();
    let mut r = Row::zeroed(&sch);
    r.write_inline_str(0, sch.inline_str_cap, ENTITY);
    r.write_f64(16, amount);
    r.write_i64(24, qty);
    let v = serde_json::json!({ "user_id": ENTITY, "amount": amount, "qty": qty });
    (r, v)
}

/// 20 event-time checkpoints: every 1.5s across the 30s stream.
fn checkpoints() -> Vec<SystemTime> {
    (1..=20)
        .map(|n| base_time() + Duration::from_millis(n * 1500))
        .collect()
}

#[cfg(feature = "state-inmem")]
fn fresh_shard() -> Shard {
    Shard::new()
}

#[cfg(not(feature = "state-inmem"))]
fn fresh_shard() -> Shard {
    // Open a temp fjall partition for the test. Minimal layout — one
    // keyspace + one partition; teardown via temp_dir drop.
    let dir = tempfile::tempdir().expect("tempdir");
    let ks = fjall::Config::new(dir.path())
        .open()
        .expect("fjall keyspace open");
    let ph = ks
        .open_partition("test_shard", fjall::PartitionCreateOptions::default())
        .expect("partition open");
    // Leak tempdir + keyspace so they outlive the Shard. Tests are
    // short-lived processes; the OS reclaims on exit.
    std::mem::forget(dir);
    std::mem::forget(ks);
    Shard::with_partition(ph)
}

// ---------------------------------------------------------------------------
// Wave-1 GREEN tests: Count, Sum i64, Sum f64, Avg f64.
// ---------------------------------------------------------------------------

#[test]
fn parity_count_typed_vs_value_windowed_5s_bucket_1s() {
    let event_schema = event_schema_num();
    let state_schema = state_schema_dummy();
    let typed_op = CountOpTypedWindowed {
        name: "count".into(),
        op_idx: 0,
        window: window(),
        bucket: bucket(),
    };
    let mut value_op = CountOpValue::new(window(), bucket());
    let mut shard = fresh_shard();

    let checkpoints = checkpoints();
    let mut cp_iter = checkpoints.iter().peekable();
    let dummy_state = Row::zeroed(&state_schema);

    for i in 0..N_EVENTS {
        let et = event_time_at(i);
        let (row, val) = make_event(i as f64, i as i64);
        typed_op.update_windowed(&mut shard, STREAM, ENTITY, &row, &event_schema, et);
        value_op.push(&val, None, et).expect("value push");

        // At each checkpoint boundary, read + compare before moving on.
        while let Some(&cp) = cp_iter.peek() {
            if et < *cp {
                break;
            }
            let t = typed_op.read_feature_windowed(
                &shard,
                STREAM,
                ENTITY,
                &dummy_state,
                &state_schema,
            );
            let v = value_op.read(*cp);
            assert_eq!(
                t, v,
                "CountOp windowed parity at cp={:?} i={}: typed={:?} value={:?}",
                cp, i, t, v
            );
            cp_iter.next();
        }
    }
}

#[test]
fn parity_sum_i64_typed_vs_value_windowed_5s_bucket_1s() {
    let event_schema = event_schema_num();
    let state_schema = state_schema_dummy();
    let typed_op = SumOpTypedWindowedI64 {
        name: "sum_qty".into(),
        op_idx: 1,
        window: window(),
        bucket: bucket(),
        input_offset: 24, // qty field
    };
    // Companion count ring on the typed side so read can emit Missing when
    // no events occurred in window (matching Value-path SumOp semantics).
    let typed_count = CountOpTypedWindowed {
        name: "cnt".into(),
        op_idx: 2,
        window: window(),
        bucket: bucket(),
    };
    let mut value_op = SumOpValue::new("qty", window(), bucket(), false);
    let mut shard = fresh_shard();

    let checkpoints = checkpoints();
    let mut cp_iter = checkpoints.iter().peekable();
    let dummy_state = Row::zeroed(&state_schema);

    for i in 0..N_EVENTS {
        let et = event_time_at(i);
        // qty >= 1 for every event so Value-path SumOp treats window as non-empty iff count > 0
        let qty = (i as i64) + 1;
        let (row, val) = make_event(0.0, qty);
        typed_op.update_windowed(&mut shard, STREAM, ENTITY, &row, &event_schema, et);
        typed_count.update_windowed(&mut shard, STREAM, ENTITY, &row, &event_schema, et);
        value_op
            .push(&val, None, et)
            .expect("value push");

        while let Some(&cp) = cp_iter.peek() {
            if et < *cp {
                break;
            }
            let cnt = typed_count.read_feature_windowed(
                &shard,
                STREAM,
                ENTITY,
                &dummy_state,
                &state_schema,
            );
            let t = match cnt {
                FeatureValue::Missing => FeatureValue::Missing,
                _ => {
                    // Sum ring → Int, but Value-path SumOp returns Float.
                    // Normalize for compare.
                    let FeatureValue::Int(n) = typed_op.read_feature_windowed(
                        &shard,
                        STREAM,
                        ENTITY,
                        &dummy_state,
                        &state_schema,
                    ) else {
                        panic!("expected Int from SumOpTypedWindowedI64");
                    };
                    FeatureValue::Float(n as f64)
                }
            };
            let v = value_op.read(*cp);
            assert_eq!(
                t, v,
                "SumI64 windowed parity at cp={:?} i={}: typed={:?} value={:?}",
                cp, i, t, v
            );
            cp_iter.next();
        }
    }
}

#[test]
fn parity_sum_f64_typed_vs_value_windowed_5s_bucket_1s() {
    let event_schema = event_schema_num();
    let state_schema = state_schema_dummy();
    let typed_op = SumOpTypedWindowedF64 {
        name: "sum_amt".into(),
        op_idx: 3,
        window: window(),
        bucket: bucket(),
        input_offset: 16, // amount field
    };
    let typed_count = CountOpTypedWindowed {
        name: "cnt".into(),
        op_idx: 4,
        window: window(),
        bucket: bucket(),
    };
    let mut value_op = SumOpValue::new("amount", window(), bucket(), false);
    let mut shard = fresh_shard();

    let checkpoints = checkpoints();
    let mut cp_iter = checkpoints.iter().peekable();
    let dummy_state = Row::zeroed(&state_schema);

    for i in 0..N_EVENTS {
        let et = event_time_at(i);
        let amount = (i as f64) + 1.0;
        let (row, val) = make_event(amount, 0);
        typed_op.update_windowed(&mut shard, STREAM, ENTITY, &row, &event_schema, et);
        typed_count.update_windowed(&mut shard, STREAM, ENTITY, &row, &event_schema, et);
        value_op.push(&val, None, et).expect("value push");

        while let Some(&cp) = cp_iter.peek() {
            if et < *cp {
                break;
            }
            let cnt = typed_count.read_feature_windowed(
                &shard,
                STREAM,
                ENTITY,
                &dummy_state,
                &state_schema,
            );
            let t = match cnt {
                FeatureValue::Missing => FeatureValue::Missing,
                _ => typed_op.read_feature_windowed(
                    &shard,
                    STREAM,
                    ENTITY,
                    &dummy_state,
                    &state_schema,
                ),
            };
            let v = value_op.read(*cp);
            // Both paths produce Float; small fp tolerance on large sums.
            match (t, v) {
                (FeatureValue::Missing, FeatureValue::Missing) => {}
                (FeatureValue::Float(tf), FeatureValue::Float(vf)) => {
                    assert!(
                        (tf - vf).abs() <= f64::EPSILON * tf.abs().max(vf.abs()).max(1.0) * 16.0,
                        "SumF64 windowed parity at cp={:?} i={}: typed={} value={} delta={}",
                        cp,
                        i,
                        tf,
                        vf,
                        (tf - vf).abs()
                    );
                }
                (a, b) => panic!(
                    "SumF64 parity shape mismatch at cp={:?} i={}: typed={:?} value={:?}",
                    cp, i, a, b
                ),
            }
            cp_iter.next();
        }
    }
}

#[test]
fn parity_avg_f64_typed_vs_value_windowed_5s_bucket_1s() {
    let event_schema = event_schema_num();
    let state_schema = state_schema_dummy();
    let typed_op = AvgOpTypedWindowedF64 {
        name: "avg_amt".into(),
        op_idx: 5,
        window: window(),
        bucket: bucket(),
        input_offset: 16,
    };
    let mut value_op = AvgOp::new("amount", window(), bucket(), false);
    let mut shard = fresh_shard();

    let checkpoints = checkpoints();
    let mut cp_iter = checkpoints.iter().peekable();
    let dummy_state = Row::zeroed(&state_schema);

    for i in 0..N_EVENTS {
        let et = event_time_at(i);
        let amount = (i as f64) + 1.0;
        let (row, val) = make_event(amount, 0);
        typed_op.update_windowed(&mut shard, STREAM, ENTITY, &row, &event_schema, et);
        value_op.push(&val, None, et).expect("value push");

        while let Some(&cp) = cp_iter.peek() {
            if et < *cp {
                break;
            }
            let t = typed_op.read_feature_windowed(
                &shard,
                STREAM,
                ENTITY,
                &dummy_state,
                &state_schema,
            );
            let v = value_op.read(*cp);
            match (t, v) {
                (FeatureValue::Missing, FeatureValue::Missing) => {}
                (FeatureValue::Float(tf), FeatureValue::Float(vf)) => {
                    assert!(
                        (tf - vf).abs() <= f64::EPSILON * tf.abs().max(vf.abs()).max(1.0) * 16.0,
                        "Avg windowed parity at cp={:?} i={}: typed={} value={} delta={}",
                        cp,
                        i,
                        tf,
                        vf,
                        (tf - vf).abs()
                    );
                }
                (a, b) => panic!(
                    "Avg parity shape mismatch at cp={:?} i={}: typed={:?} value={:?}",
                    cp, i, a, b
                ),
            }
            cp_iter.next();
        }
    }
}

// ---------------------------------------------------------------------------
// Wave-2 RED tests (still ignored): Min i64/f64, Max i64/f64, Last, First.
// ---------------------------------------------------------------------------

#[test]
#[ignore = "59.7-W2"]
fn parity_min_i64_typed_vs_value_windowed_5s_bucket_1s() {
    // Wave 2: MinOpTypedWindowedI64 + TypedRingBufferMin<i64> (bucket-local
    // min + parallel count buffer, mirroring MinOp's MinBucket semantics).
    unreachable!("59.7-W2 RED: MinOpTypedWindowedI64 not yet implemented");
}

#[test]
#[ignore = "59.7-W2"]
fn parity_min_f64_typed_vs_value_windowed_5s_bucket_1s() {
    unreachable!("59.7-W2 RED: MinOpTypedWindowedF64 not yet implemented");
}

#[test]
#[ignore = "59.7-W2"]
fn parity_max_i64_typed_vs_value_windowed_5s_bucket_1s() {
    unreachable!("59.7-W2 RED: MaxOpTypedWindowedI64 not yet implemented");
}

#[test]
#[ignore = "59.7-W2"]
fn parity_max_f64_typed_vs_value_windowed_5s_bucket_1s() {
    unreachable!("59.7-W2 RED: MaxOpTypedWindowedF64 not yet implemented");
}

#[test]
#[ignore = "59.7-W2"]
fn parity_last_inline_str_typed_vs_value_windowed_5s_bucket_1s() {
    unreachable!("59.7-W2 RED: LastOpTypedWindowedInlineStr not yet implemented");
}

#[test]
#[ignore = "59.7-W2"]
fn parity_first_inline_str_typed_vs_value_windowed_5s_bucket_1s() {
    unreachable!("59.7-W2 RED: FirstOpTypedWindowedInlineStr not yet implemented");
}
