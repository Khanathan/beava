//! Phase 13.1: Regression test for the inline-fsync runtime-stall bug.
//!
//! Background: the Phase 6.1 `Periodic` durability mode is supposed to ACK an
//! `append_event_with_mode` call as soon as the in-memory append + LSN
//! assignment has happened, with the actual `sync_data()` syscall happening
//! lazily on the background timer. On the production `current_thread` tokio
//! runtime, however, the worker_loop's `fsync_batch` was calling
//! `writer.sync_data()` inline. macOS `F_FULLSYNC` blocks for ~7 ms and the
//! single-threaded tokio runtime can't make progress on any OTHER task while
//! that syscall is in-flight — including the HTTP push handler that's about
//! to ACK the next request.
//!
//! Contract enforced by this test: while the fsync worker is mid-fsync, other
//! tasks on the same `current_thread` runtime MUST continue to make
//! progress. We measure this by spawning a "ticker" task that increments a
//! counter every millisecond, then triggering many fsyncs over a short window
//! and asserting the counter advanced approximately as often as wall-clock
//! milliseconds elapsed.

use beava_persistence::{SyncMode, WalSink, WalSinkConfig};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

#[tokio::test(flavor = "current_thread")]
async fn fsync_does_not_block_runtime_tasks() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = WalSinkConfig {
        dir: dir.path().to_path_buf(),
        initial_start_lsn: 1,
        initial_registry_version: 1,
        // Tight timer — many fsyncs during the test window.
        fsync_interval_ms: 1,
        fsync_bytes: 1 << 20,
        segment_bytes: 1 << 20,
        sync_mode: SyncMode::PerEvent,
    };
    let (sink, handle) = WalSink::spawn(cfg).unwrap();

    // Ticker: increments every 1 ms. If the runtime stalls (because fsync
    // blocked the executor thread), the ticker can't tick. We sample its
    // count over a 200 ms window.
    let counter = Arc::new(AtomicU64::new(0));
    let counter_for_task = counter.clone();
    let stop = Arc::new(AtomicU64::new(0));
    let stop_for_task = stop.clone();
    let ticker = tokio::spawn(async move {
        while stop_for_task.load(Ordering::Relaxed) == 0 {
            tokio::time::sleep(Duration::from_millis(1)).await;
            counter_for_task.fetch_add(1, Ordering::Relaxed);
        }
    });

    // Push a steady stream of PerEvent appends — each forces an fsync.
    let appender = {
        let sink = sink.clone();
        tokio::spawn(async move {
            for i in 0..200u64 {
                let _ = sink
                    .append_event_with_mode(format!("payload-{i}").into_bytes(), SyncMode::PerEvent)
                    .await
                    .unwrap();
            }
        })
    };

    let t0 = Instant::now();
    appender.await.unwrap();
    let elapsed = t0.elapsed();

    stop.store(1, Ordering::Relaxed);
    let _ = ticker.await;

    let ticks = counter.load(Ordering::Relaxed);
    let elapsed_ms = elapsed.as_millis() as u64;

    // The ticker schedules itself every 1 ms. If the runtime is healthy
    // it should fire at least ~elapsed_ms / 4 times (giving a generous
    // margin for scheduler jitter). If the runtime was stalled by inline
    // fsync syscalls, we'd see a tiny fraction of that.
    let expected_floor = elapsed_ms / 4;
    assert!(
        ticks >= expected_floor,
        "ticker ran {ticks} times in {elapsed_ms} ms — runtime appears stalled by inline fsync; expected at least {expected_floor}"
    );

    sink.shutdown().await.unwrap();
    handle.await.unwrap();
}
