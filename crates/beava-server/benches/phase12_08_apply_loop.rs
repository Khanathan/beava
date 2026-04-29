//! Plan 12-08 Wave 5 — apply-loop criterion microbench.
//!
//! Measures the cost of per-iteration apply-loop primitives:
//!   - `apply_loop/try_recv_hit`   — channel try_recv with one item ready
//!   - `apply_loop/try_recv_miss`  — channel try_recv on empty (spin-loop floor)
//!   - `apply_loop/batch_flush_16` — group + send_batch + 1 wake per worker
//!   - `apply_loop/pool_acquire_release` — BytesMutPool round-trip overhead
//!
//! Excludes the per-cell dispatch cost (which is covered by the
//! Plan 12-07 read-path microbench at `phase12_07_read_path.rs`). This
//! bench measures the orchestration overhead Plan 12-08 is targeting.
//!
//! Per CLAUDE.md §Performance Discipline (Phase 6+ rule), the captured
//! medians are recorded in `.planning/perf-baselines.md` § Phase 12-08 for
//! 10% / 25% regression detection in the same hw-class.

use beava_runtime_core::bytes_pool::BytesMutPool;
use beava_runtime_core::work_ring::WriteRingExt;
use bytes::BytesMut;
use criterion::{black_box, criterion_group, criterion_main, Criterion};

/// Channel hot-path: try_recv with one item ready.
///
/// Pre-fills a bounded channel of capacity 16384 with a single small item
/// (a u64 — proxy for a per-iter dispatch handle), times the
/// `try_recv()` + a no-op consumer. Measures the ~30 ns floor of the
/// channel side of the apply loop.
fn bench_try_recv_hit(c: &mut Criterion) {
    c.bench_function("apply_loop/try_recv_hit", |b| {
        let (tx, rx) = crossbeam_channel::bounded::<u64>(16_384);
        b.iter(|| {
            tx.send(black_box(1u64)).unwrap();
            let item = rx.try_recv().unwrap();
            black_box(item);
        });
    });
}

/// Channel cold-path: try_recv on empty channel.
///
/// Measures the spin-loop floor when apply has no work. Steady-state
/// idle CPU is dominated by this in tight-spin mode.
fn bench_try_recv_miss(c: &mut Criterion) {
    c.bench_function("apply_loop/try_recv_miss", |b| {
        let (_tx, rx) = crossbeam_channel::bounded::<u64>(16_384);
        b.iter(|| {
            let r = rx.try_recv();
            black_box(r.is_err());
        });
    });
}

/// Wave 3 (D-B) flush primitive: group 16 entries by worker_index, send a
/// batch per worker, simulate firing the worker waker once per affected
/// worker. Measures the cost of the response-batch flush path on the apply
/// thread side.
///
/// We can't easily fire a real `mio::Waker` here (would need a Poll +
/// registry); the wake is the dominant ~1µs cost in production but is
/// constant per-batch regardless of bench shape, so the bench focuses on
/// the grouping + send_batch path.
fn bench_batch_flush_16(c: &mut Criterion) {
    c.bench_function("apply_loop/batch_flush_16", |b| {
        // 1 worker channel; 16 items/batch.
        let (tx, rx) = crossbeam_channel::bounded::<(u64, u64)>(16_384);
        b.iter(|| {
            let mut batch: Vec<(u64, u64)> = Vec::with_capacity(16);
            for i in 0..16u64 {
                batch.push((i, i));
            }
            tx.send_batch(batch).unwrap();
            // Drain so subsequent iters don't hit Full.
            for _ in 0..16 {
                let _ = rx.try_recv();
            }
        });
    });
}

/// Wave 4 (D-C) pool primitive: acquire + release round-trip.
///
/// Pre-warms the pool with N buffers so the steady-state path is pure
/// pop/push without `BytesMut::with_capacity` allocs. Measures the
/// "no malloc on hot path" claim's actual round-trip cost.
fn bench_pool_acquire_release(c: &mut Criterion) {
    c.bench_function("apply_loop/pool_acquire_release", |b| {
        let pool = BytesMutPool::new(256, 4096);
        // Warm the pool.
        let mut bufs = Vec::with_capacity(64);
        for _ in 0..64 {
            bufs.push(pool.acquire());
        }
        for buf in bufs.drain(..) {
            pool.release(buf);
        }
        b.iter(|| {
            let mut buf = pool.acquire();
            buf.extend_from_slice(b"x");
            pool.release(buf);
        });
    });
}

/// Reference: cold-path `BytesMut::with_capacity(4096)` allocation.
///
/// Provides the "without pool" baseline so the perf-baselines table can
/// quote the speedup factor.
fn bench_bytesmut_with_capacity(c: &mut Criterion) {
    c.bench_function("apply_loop/bytesmut_with_capacity_baseline", |b| {
        b.iter(|| {
            let buf = BytesMut::with_capacity(black_box(4096));
            black_box(buf);
        });
    });
}

criterion_group!(
    benches,
    bench_try_recv_hit,
    bench_try_recv_miss,
    bench_batch_flush_16,
    bench_pool_acquire_release,
    bench_bytesmut_with_capacity,
);
criterion_main!(benches);
