//! `SyncMode` dispatch on `WalSink`.
//!
//! Periodic mode: `append_event_with_mode(payload, SyncMode::Periodic)`
//! returns the assigned LSN as soon as the in-memory append is done — it
//! does NOT wait for fsync. The background timer eventually fsyncs.
//!
//! PerEvent mode: `append_event_with_mode(payload, SyncMode::PerEvent)`
//! resolves only after fsync — the strict ACK-after-fsync invariant.

use beava_persistence::{SyncMode, WalReader, WalSink, WalSinkConfig};
use std::time::{Duration, Instant};

fn config_for_test(dir: std::path::PathBuf) -> WalSinkConfig {
    WalSinkConfig {
        dir,
        initial_start_lsn: 1,
        initial_registry_version: 1,
        // Make the periodic timer effectively never fire within the test
        // window — verifies that periodic-mode appends DO NOT wait on it.
        fsync_interval_ms: 5_000,
        fsync_bytes: 1 << 20,
        segment_bytes: 1 << 20,
        sync_mode: SyncMode::Periodic,
    }
}

fn find_segment(dir: &std::path::Path) -> std::path::PathBuf {
    std::fs::read_dir(dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .find(|p| {
            p.file_name()
                .and_then(|n| n.to_str())
                .map(|s| s.starts_with("wal-") && s.ends_with(".log"))
                .unwrap_or(false)
        })
        .expect("at least one segment")
}

#[test]
fn sync_mode_default_is_periodic() {
    let cfg = WalSinkConfig::default();
    assert_eq!(cfg.sync_mode, SyncMode::Periodic);
}

#[tokio::test(flavor = "current_thread")]
async fn periodic_append_returns_before_fsync() {
    let dir = tempfile::tempdir().unwrap();
    let (sink, handle) = WalSink::spawn(config_for_test(dir.path().to_path_buf())).unwrap();

    let t0 = Instant::now();
    let lsn = sink
        .append_event_with_mode(b"x".to_vec(), SyncMode::Periodic)
        .await
        .unwrap();
    let elapsed = t0.elapsed();

    assert_eq!(lsn, 1);
    // Periodic must NOT wait for the 5_000ms timer — it should return
    // essentially immediately (well under 100 ms).
    assert!(
        elapsed < Duration::from_millis(500),
        "periodic append should not block on fsync timer; took {elapsed:?}"
    );

    sink.shutdown().await.unwrap();
    handle.await.unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn per_event_append_blocks_on_fsync() {
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = config_for_test(dir.path().to_path_buf());
    cfg.fsync_interval_ms = 400;
    let (sink, handle) = WalSink::spawn(cfg).unwrap();

    let t0 = Instant::now();
    let lsn = sink
        .append_event_with_mode(b"x".to_vec(), SyncMode::PerEvent)
        .await
        .unwrap();
    let elapsed = t0.elapsed();
    assert_eq!(lsn, 1);
    // PerEvent must wait for the fsync timer to fire.
    assert!(
        elapsed >= Duration::from_millis(300),
        "per-event append must block on fsync; took {elapsed:?}"
    );

    sink.shutdown().await.unwrap();
    handle.await.unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn periodic_records_eventually_fsync_via_shutdown() {
    let dir = tempfile::tempdir().unwrap();
    let (sink, handle) = WalSink::spawn(config_for_test(dir.path().to_path_buf())).unwrap();

    for _ in 0..5 {
        sink.append_event_with_mode(b"p".to_vec(), SyncMode::Periodic)
            .await
            .unwrap();
    }
    // Shutdown drains pending + fsyncs.
    sink.shutdown().await.unwrap();
    handle.await.unwrap();

    let seg = find_segment(dir.path());
    let r = WalReader::open(&seg).unwrap();
    let recs: Vec<_> = r.collect::<Result<_, _>>().unwrap();
    assert_eq!(recs.len(), 5);
}
