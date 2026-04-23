//! WAL segment rotation + truncate_up_to tests.

use beava_persistence::{WalSink, WalSinkConfig};

fn count_segments(dir: &std::path::Path) -> usize {
    std::fs::read_dir(dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| {
            e.file_name()
                .to_str()
                .map(|s| s.starts_with("wal-") && s.ends_with(".log"))
                .unwrap_or(false)
        })
        .count()
}

fn list_segment_start_lsns(dir: &std::path::Path) -> Vec<u64> {
    let mut v: Vec<u64> = std::fs::read_dir(dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter_map(|e| {
            let n = e.file_name();
            let s = n.to_str()?;
            if s.starts_with("wal-") && s.ends_with(".log") {
                let hex = &s[4..s.len() - 4];
                u64::from_str_radix(hex, 16).ok()
            } else {
                None
            }
        })
        .collect();
    v.sort();
    v
}

#[tokio::test(flavor = "current_thread")]
async fn rotation_creates_new_segment() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = WalSinkConfig {
        dir: dir.path().to_path_buf(),
        initial_start_lsn: 1,
        initial_registry_version: 1,
        fsync_interval_ms: 1,
        fsync_bytes: 1 << 20,
        segment_bytes: 2048, // force rotation fast
    };
    let (sink, handle) = WalSink::spawn(cfg).unwrap();

    // Each record is ~100 bytes (padding). 40 records > 2048 bytes.
    let big_payload = vec![b'p'; 100];
    for _ in 0..40 {
        sink.append_event(big_payload.clone()).await.unwrap();
    }

    sink.shutdown().await.unwrap();
    handle.await.unwrap();

    assert!(
        count_segments(dir.path()) >= 2,
        "expected >= 2 segments, got {}",
        count_segments(dir.path())
    );
}

#[tokio::test(flavor = "current_thread")]
async fn truncate_up_to_deletes_closed_only() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = WalSinkConfig {
        dir: dir.path().to_path_buf(),
        initial_start_lsn: 1,
        initial_registry_version: 1,
        fsync_interval_ms: 1,
        fsync_bytes: 1 << 20,
        segment_bytes: 1024, // ~10 records per seg
    };
    let (sink, handle) = WalSink::spawn(cfg).unwrap();

    let big_payload = vec![b'p'; 100];
    // Rotate through multiple segments
    for _ in 0..40 {
        sink.append_event(big_payload.clone()).await.unwrap();
    }
    let starts_before = list_segment_start_lsns(dir.path());
    assert!(
        starts_before.len() >= 3,
        "need >=3 segments to test closed-only semantics, got {starts_before:?}"
    );

    // Truncate up to the start_lsn of the final (current) segment.
    // All prior (closed) segments should be removed; current stays.
    let current_start = *starts_before.last().unwrap();
    let removed = sink.truncate_up_to(current_start).await.unwrap();
    assert_eq!(
        removed as usize,
        starts_before.len() - 1,
        "expected all closed segments deleted"
    );

    let starts_after = list_segment_start_lsns(dir.path());
    assert_eq!(starts_after.len(), 1);
    assert_eq!(starts_after[0], current_start);

    // Re-calling with any lsn <= current_start deletes zero more.
    let removed2 = sink.truncate_up_to(current_start).await.unwrap();
    assert_eq!(removed2, 0);

    sink.shutdown().await.unwrap();
    handle.await.unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn truncate_preserves_segment_covering_lsn() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = WalSinkConfig {
        dir: dir.path().to_path_buf(),
        initial_start_lsn: 1,
        initial_registry_version: 1,
        fsync_interval_ms: 1,
        fsync_bytes: 1 << 20,
        segment_bytes: 1024,
    };
    let (sink, handle) = WalSink::spawn(cfg).unwrap();

    let big_payload = vec![b'p'; 100];
    for _ in 0..40 {
        sink.append_event(big_payload.clone()).await.unwrap();
    }
    let starts = list_segment_start_lsns(dir.path());
    assert!(starts.len() >= 3);

    // Pick a covered_lsn that falls INSIDE the second segment — it must NOT be deleted.
    // Second segment start = starts[1]. An lsn strictly between starts[1] and starts[2] is inside seg[1].
    let covered_lsn = starts[1] + 1;
    let removed = sink.truncate_up_to(covered_lsn).await.unwrap();
    // Only segment 0 fully covered (its last_lsn = starts[1] - 1 < covered_lsn)
    assert_eq!(removed, 1, "only first segment is fully covered");
    let after = list_segment_start_lsns(dir.path());
    assert!(
        after.contains(&starts[1]),
        "segment covering covered_lsn must remain: {after:?} should contain {}",
        starts[1]
    );

    sink.shutdown().await.unwrap();
    handle.await.unwrap();
}
