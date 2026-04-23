//! Phase 6 WAL hot-path microbench (CLAUDE.md §Performance Discipline, mandatory for Phase 6+).
//!
//! Three benchmarks:
//! - `wal/append_nofsync`: raw WAL frame encode + write + CRC, no fsync. Measures
//!   serialization cost in isolation.
//! - `wal/append_fsync_default_coalesce`: single-writer append awaited through
//!   the default WalSink (2ms coalesce / 1 MiB). Headline P50 fsync overhead —
//!   the success-criterion-#3 check (<2ms target).
//! - `wal/append_fsync_burst_1k`: amortized fsync cost per push under load. 1000
//!   concurrent appends awaited together; criterion time / 1000 = per-push cost.

use beava_persistence::{RecordType, WalRecord, WalSink, WalSinkConfig, WalWriter};
use criterion::{criterion_group, criterion_main, Criterion};

fn sample_payload() -> Vec<u8> {
    // ~256 bytes, matches the CONTEXT.md default bench payload size.
    let mut v = Vec::with_capacity(256);
    for i in 0..256 {
        v.push((i % 256) as u8);
    }
    v
}

fn bench_append_nofsync(c: &mut Criterion) {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut w = WalWriter::open(dir.path(), 1, 1).expect("open writer");
    let mut lsn: u64 = 1;
    let payload = sample_payload();

    c.bench_function("wal/append_nofsync", |b| {
        b.iter(|| {
            let rec = WalRecord {
                lsn,
                record_type: RecordType::Event,
                payload: payload.clone(),
            };
            w.append(&rec).expect("append");
            lsn += 1;
        })
    });

    // Keep dir alive until end of bench.
    drop(w);
    drop(dir);
}

fn bench_append_fsync_default_coalesce(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("rt");

    c.bench_function("wal/append_fsync_default_coalesce", |b| {
        b.iter_custom(|iters| {
            let dir = tempfile::tempdir().expect("tempdir");
            let cfg = WalSinkConfig {
                dir: dir.path().to_path_buf(),
                initial_start_lsn: 1,
                initial_registry_version: 1,
                fsync_interval_ms: 2,
                fsync_bytes: 1 << 20,
                segment_bytes: 128 << 20,
            };
            let payload = sample_payload();

            let elapsed = rt.block_on(async {
                let (sink, handle) = WalSink::spawn(cfg).expect("spawn");
                let start = std::time::Instant::now();
                for _ in 0..iters {
                    sink.append_event(payload.clone()).await.expect("append");
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

fn bench_append_fsync_burst_1k(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("rt");

    c.bench_function("wal/append_fsync_burst_1k", |b| {
        b.iter_custom(|iters| {
            let dir = tempfile::tempdir().expect("tempdir");
            let cfg = WalSinkConfig {
                dir: dir.path().to_path_buf(),
                initial_start_lsn: 1,
                initial_registry_version: 1,
                fsync_interval_ms: 2,
                fsync_bytes: 1 << 20,
                segment_bytes: 1024 << 20,
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
                        tasks.push(tokio::spawn(async move { s.append_event(p).await }));
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
    bench_append_nofsync,
    bench_append_fsync_default_coalesce,
    bench_append_fsync_burst_1k
);
criterion_main!(benches);
