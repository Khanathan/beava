//! Plan 22-03 Step 9: micro-benches for the three hybrid operators.
//!
//! This is **not** a perf gate — per the 22-03 execution decision (C=1),
//! measurements are recorded in 22-03-SUMMARY.md and re-validated on bare
//! metal in 22-04. Runs are marked `#[ignore]` so `cargo test` doesn't
//! include them by default; invoke with:
//!
//!     cargo test --test bench_hybrid_ops -- --ignored --nocapture
//!
//! Hardware at the time of capture: Debian 13 cloud VM.

use serde_json::json;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use beava::engine::hll::DistinctCountOp;
use beava::engine::operators::{Operator, PercentileOp, TopKOp};

fn ts(secs: u64) -> SystemTime {
    UNIX_EPOCH + Duration::from_secs(secs)
}

fn bench_iters<F: FnMut()>(label: &str, iters: usize, mut f: F) {
    // Warm-up
    for _ in 0..(iters / 10).max(1) {
        f();
    }
    let start = Instant::now();
    for _ in 0..iters {
        f();
    }
    let elapsed = start.elapsed();
    let per_op_ns = elapsed.as_nanos() as f64 / iters as f64;
    let ops_per_sec = 1_000_000_000.0 / per_op_ns;
    eprintln!(
        "[BENCH] {label:<50} iters={iters} total={:>7.2}ms  per_op={:>7.0}ns  ops/s={:>11.0}",
        elapsed.as_secs_f64() * 1000.0,
        per_op_ns,
        ops_per_sec
    );
}

#[ignore]
#[test]
fn bench_percentile_exact_mode_push() {
    let mut op = PercentileOp::new(
        "v",
        0.5,
        Duration::from_secs(3600),
        Duration::from_secs(60),
        false,
    );
    let t = ts(1_000_000);
    let mut i = 0i64;
    bench_iters("percentile.push (exact mode)", 100_000, || {
        op.push(&json!({ "v": i % 200 }), None, t).unwrap();
        i += 1;
    });
}

#[ignore]
#[test]
fn bench_percentile_sketch_mode_push() {
    let mut op = PercentileOp::new(
        "v",
        0.5,
        Duration::from_secs(3600),
        Duration::from_secs(60),
        false,
    );
    let t = ts(1_000_000);
    // Force sketch mode first.
    for i in 0..300 {
        op.push(&json!({ "v": i }), None, t).unwrap();
    }
    let mut i = 0i64;
    bench_iters("percentile.push (sketch mode)", 200_000, || {
        op.push(&json!({ "v": i }), None, t).unwrap();
        i += 1;
    });
}

#[ignore]
#[test]
fn bench_distinct_count_sketch_push() {
    let mut op = DistinctCountOp::new(
        "d",
        Duration::from_secs(3600),
        Duration::from_secs(60),
        false,
    );
    let t = ts(1_000_000);
    // Warm past HLL threshold.
    for i in 0..2000 {
        op.push(&json!({ "d": format!("w{}", i) }), None, t).unwrap();
    }
    let mut i = 2000i64;
    bench_iters("distinct_count.push (HLL mode)", 100_000, || {
        op.push(&json!({ "d": format!("u{}", i) }), None, t).unwrap();
        i += 1;
    });
}

#[ignore]
#[test]
fn bench_top_k_sketch_push() {
    let mut op = TopKOp::new(
        "m",
        10,
        Duration::from_secs(3600),
        Duration::from_secs(60),
        1024,
        2048,
        4,
        false,
    );
    let t = ts(1_000_000);
    // Force sketch mode.
    for i in 0..1100 {
        op.push(&json!({ "m": i }), None, t).unwrap();
    }
    let mut i = 1100i64;
    bench_iters("top_k.push (sketch mode)", 100_000, || {
        op.push(&json!({ "m": i % 2000 }), None, t).unwrap();
        i += 1;
    });
}

#[ignore]
#[test]
fn bench_percentile_read_sketch() {
    let mut op = PercentileOp::new(
        "v",
        0.95,
        Duration::from_secs(3600),
        Duration::from_secs(60),
        false,
    );
    let t = ts(1_000_000);
    for i in 0..10_000 {
        op.push(&json!({ "v": i }), None, t).unwrap();
    }
    bench_iters("percentile.read (sketch mode)", 10_000, || {
        let _ = op.read(t);
    });
}
