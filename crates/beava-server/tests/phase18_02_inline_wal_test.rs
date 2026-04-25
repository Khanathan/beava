//! Phase 18-02 Tasks 2.3 + 2.4 — WalWriter + runtime_core_glue WAL integration.
//!
//! Task 2.3 tests: writer thread starts, pops sealed buffers, writes to fd,
//! advances written_lsn, fsyncs, advances synced_lsn, returns buffer to free.
//!
//! Task 2.4 tests: dispatch_push writes WAL record and returns committed_lsn;
//! dispatch_push_sync blocks until synced_lsn advances.
//!
//! RED state: WalWriter is a stub with no implementation.
//! Tests will fail to compile or panic until Tasks 2.3 + 2.4 GREEN.

use beava_runtime_core::wal_buffer::WalBufferRing;
use beava_runtime_core::wal_lsn::WalLsn;
use beava_runtime_core::wal_writer::WalWriter;
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;

// ── Task 2.3: WalWriter thread ────────────────────────────────────────────────

/// WalWriter::spawn starts a background thread.
/// After writing data to the ring and sealing it, the writer thread
/// advances written_lsn and synced_lsn within 2 × tick_ms.
#[test]
fn wal_writer_advances_written_and_synced_lsn() {
    let dir = TempDir::new().unwrap();
    let lsn = Arc::new(WalLsn::new());
    let ring = Arc::new(WalBufferRing::new(3, 16 * 1024, Arc::clone(&lsn)));

    // tick_ms = 10 for fast test; default is 5 ms in production.
    let tick_ms: u64 = 10;
    let writer = WalWriter::new(dir.path(), Arc::clone(&ring), Arc::clone(&lsn), tick_ms)
        .expect("WalWriter::new should succeed on local FS");

    let _handle = writer.spawn();

    // Append some data and commit into the ring.
    let payload = b"test-wal-record-payload";
    let committed = ring.append(payload);
    assert_eq!(committed, payload.len() as u64);

    // Wait up to 3 × tick_ms for the writer thread to process the tick-seal
    // and write + fsync the buffer.
    let deadline = Duration::from_millis(tick_ms * 6);
    let start = std::time::Instant::now();
    while start.elapsed() < deadline {
        if lsn.synced_at_least(committed) {
            break;
        }
        std::thread::sleep(Duration::from_millis(1));
    }

    assert!(
        lsn.written() >= committed,
        "written_lsn {written} should be ≥ committed {committed} after writer tick",
        written = lsn.written(),
    );
    assert!(
        lsn.synced() >= committed,
        "synced_lsn {synced} should be ≥ committed {committed} after fsync",
        synced = lsn.synced(),
    );
}

/// The writer creates a WAL file in the configured directory.
#[test]
fn wal_writer_creates_wal_file() {
    let dir = TempDir::new().unwrap();
    let lsn = Arc::new(WalLsn::new());
    let ring = Arc::new(WalBufferRing::new(3, 16 * 1024, Arc::clone(&lsn)));

    let writer = WalWriter::new(dir.path(), Arc::clone(&ring), Arc::clone(&lsn), 5)
        .expect("WalWriter::new should succeed");
    let _handle = writer.spawn();

    ring.append(b"hello");

    // Wait briefly for writer tick.
    std::thread::sleep(Duration::from_millis(30));

    // At least one file should exist in the WAL directory.
    let entries: Vec<_> = std::fs::read_dir(dir.path())
        .unwrap()
        .filter_map(|e| e.ok())
        .collect();
    assert!(
        !entries.is_empty(),
        "expected at least one WAL file in {dir:?}",
        dir = dir.path()
    );
}

/// Multiple rounds of append + seal → write → fsync advance LSN correctly.
#[test]
fn wal_writer_multiple_rounds() {
    let dir = TempDir::new().unwrap();
    let lsn = Arc::new(WalLsn::new());
    let ring = Arc::new(WalBufferRing::new(3, 16 * 1024, Arc::clone(&lsn)));

    let writer = WalWriter::new(dir.path(), Arc::clone(&ring), Arc::clone(&lsn), 10)
        .expect("WalWriter::new failed");
    let _handle = writer.spawn();

    let mut last_committed = 0u64;
    for i in 0u8..5 {
        let payload = vec![i; 128];
        last_committed = ring.append(&payload);
        std::thread::sleep(Duration::from_millis(5));
    }

    // Wait for writer to catch up.
    let deadline = Duration::from_millis(200);
    let start = std::time::Instant::now();
    while start.elapsed() < deadline && !lsn.synced_at_least(last_committed) {
        std::thread::sleep(Duration::from_millis(2));
    }

    assert!(
        lsn.synced_at_least(last_committed),
        "synced_lsn {} did not reach committed {}",
        lsn.synced(),
        last_committed
    );
}

/// Refuse to open WAL on a network filesystem (NFS/SMB/FUSE).
///
/// This test is platform-specific and only runs on systems where we can
/// detect NFS. On macOS/Linux with a local tmpdir, the guard should NOT fire.
#[test]
fn wal_writer_allows_local_fs() {
    let dir = TempDir::new().unwrap();
    let lsn = Arc::new(WalLsn::new());
    let ring = Arc::new(WalBufferRing::new(3, 16 * 1024, Arc::clone(&lsn)));

    // Should succeed on a local FS (tmpdir is always local).
    let result = WalWriter::new(dir.path(), ring, lsn, 5);
    assert!(
        result.is_ok(),
        "WalWriter::new failed on local FS: {:?}",
        result.err()
    );
}

// ── Task 2.4: runtime_core_glue WAL integration ───────────────────────────────

/// dispatch_push serializes a record into the WAL ring and returns committed_lsn.
/// Periodic mode: does NOT wait for synced_lsn.
#[test]
fn dispatch_push_periodic_returns_committed_lsn() {
    use beava_server::runtime_core_glue::{GlueResponse, WalGlue};

    let _dir = TempDir::new().unwrap();
    let lsn = Arc::new(WalLsn::new());
    let ring = Arc::new(WalBufferRing::new(3, 16 * 1024, Arc::clone(&lsn)));

    let glue = WalGlue::new(Arc::clone(&ring), Arc::clone(&lsn));

    // Append a mock serialized event record (~64 bytes).
    let fake_record = b"{\"event\":\"txn\",\"amount\":100}";
    let resp = glue.wal_append_periodic(fake_record);

    match resp {
        GlueResponse::PushAck { ack_lsn, .. } => {
            assert!(ack_lsn > 0, "ack_lsn should be > 0 after WAL append");
            assert_eq!(
                ack_lsn,
                lsn.committed(),
                "ack_lsn should equal committed_lsn"
            );
        }
        other => panic!("expected PushAck, got {other:?}"),
    }
}

/// dispatch_push_sync blocks until synced_lsn reaches the request_lsn.
///
/// Starts a WalWriter with a short tick, appends via push-sync path, verifies
/// the response only arrives after the WAL writer has fsynced.
#[test]
fn dispatch_push_sync_waits_for_synced_lsn() {
    use beava_server::runtime_core_glue::{GlueResponse, WalGlue};

    let dir = TempDir::new().unwrap();
    let lsn = Arc::new(WalLsn::new());
    let ring = Arc::new(WalBufferRing::new(3, 16 * 1024, Arc::clone(&lsn)));

    // Start a real WalWriter so synced_lsn actually advances.
    let writer = WalWriter::new(
        dir.path(),
        Arc::clone(&ring),
        Arc::clone(&lsn),
        10, // 10 ms tick
    )
    .expect("WalWriter::new failed");
    let _handle = writer.spawn();

    let glue = WalGlue::new(Arc::clone(&ring), Arc::clone(&lsn));
    let fake_record = b"{\"event\":\"txn\",\"amount\":50}";

    // push-sync should block until writer fsyncs; timeout = 2 s.
    let resp = glue.wal_append_per_event(fake_record, Duration::from_secs(2));

    match resp {
        GlueResponse::PushAck { ack_lsn, .. } => {
            // After push-sync returns, synced_lsn must be ≥ ack_lsn.
            assert!(
                lsn.synced() >= ack_lsn,
                "push-sync returned before fsync: synced={} ack_lsn={}",
                lsn.synced(),
                ack_lsn
            );
        }
        other => panic!("expected PushAck, got {other:?}"),
    }
}

/// push-sync returns 503 if synced_lsn doesn't advance within timeout.
#[test]
fn dispatch_push_sync_times_out_returns_error() {
    use beava_server::runtime_core_glue::{GlueResponse, WalGlue};

    let lsn = Arc::new(WalLsn::new());
    let ring = Arc::new(WalBufferRing::new(3, 16 * 1024, Arc::clone(&lsn)));
    // No WalWriter spawned — synced_lsn will never advance.

    let glue = WalGlue::new(Arc::clone(&ring), Arc::clone(&lsn));
    let fake_record = b"event";

    let resp = glue.wal_append_per_event(fake_record, Duration::from_millis(50));

    match resp {
        GlueResponse::PushError { code, .. } => {
            assert_eq!(code, "wal_sync_timeout", "expected wal_sync_timeout error");
        }
        other => panic!("expected PushError(wal_sync_timeout), got {other:?}"),
    }
}
