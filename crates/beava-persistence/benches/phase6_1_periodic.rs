//! Periodic-mode WAL append microbench.
//!
//! Measures per-append cost of `WalSink::append_event_with_mode(…,
//! SyncMode::Periodic)` — the default `/push` semantics. Unlike
//! `wal/append_fsync_default_coalesce` this benchmark does NOT wait for
//! fsync, so the headline is dominated by serialize + channel send +
//! in-memory BufWriter write + LSN ACK round-trip.

use beava_persistence::{SyncMode, WalSink, WalSinkConfig};
use criterion::{criterion_group, criterion_main, Criterion};

fn sample_payload() -> Vec<u8> {
    let mut v = Vec::with_capacity(256);
    for i in 0..256 {
        v.push((i % 256) as u8);
    }
    v
}

fn bench_periodic_append(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("rt");

    c.bench_function("wal/append_periodic_default", |b| {
        b.iter_custom(|iters| {
            let dir = tempfile::tempdir().expect("tempdir");
            let cfg = WalSinkConfig {
                dir: dir.path().to_path_buf(),
                initial_start_lsn: 1,
                initial_registry_version: 1,
                fsync_interval_ms: 2,
                fsync_bytes: 1 << 20,
                segment_bytes: 1024 << 20,
                sync_mode: SyncMode::Periodic,
            };
            let payload = sample_payload();

            let elapsed = rt.block_on(async {
                let (sink, handle) = WalSink::spawn(cfg).expect("spawn");
                let start = std::time::Instant::now();
                for _ in 0..iters {
                    sink.append_event_with_mode(payload.clone(), SyncMode::Periodic)
                        .await
                        .expect("append");
                }
                let elapsed = start.elapsed();
                sink.shutdown().await.expect("shutdown");
                handle.await.expect("join");
                elapsed
            });
            drop(dir);
            elapsed
        })
    });
}

fn bench_periodic_append_burst_1k(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("rt");

    c.bench_function("wal/append_periodic_burst_1k", |b| {
        b.iter_custom(|iters| {
            let dir = tempfile::tempdir().expect("tempdir");
            let cfg = WalSinkConfig {
                dir: dir.path().to_path_buf(),
                initial_start_lsn: 1,
                initial_registry_version: 1,
                fsync_interval_ms: 2,
                fsync_bytes: 1 << 20,
                segment_bytes: 1024 << 20,
                sync_mode: SyncMode::Periodic,
            };
            let payload = sample_payload();

            let elapsed = rt.block_on(async {
                let (sink, handle) = WalSink::spawn(cfg).expect("spawn");
                let start = std::time::Instant::now();
                for _ in 0..iters {
                    let mut tasks = Vec::with_capacity(1000);
                    for _ in 0..1000 {
                        let s = sink.clone();
                        let p = payload.clone();
                        tasks.push(tokio::spawn(async move {
                            s.append_event_with_mode(p, SyncMode::Periodic).await
                        }));
                    }
                    for t in tasks {
                        t.await.expect("task").expect("append");
                    }
                }
                let elapsed = start.elapsed();
                sink.shutdown().await.expect("shutdown");
                handle.await.expect("join");
                elapsed
            });
            drop(dir);
            elapsed
        })
    });
}

criterion_group!(
    benches,
    bench_periodic_append,
    bench_periodic_append_burst_1k
);
criterion_main!(benches);
