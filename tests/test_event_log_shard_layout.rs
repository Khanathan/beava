//! Per-shard event log layout tests (Phase 52-02, TPC-INFRA-06).
//!
//! Covers:
//!   - Task 1: `stream_log_path` accessor, new `shard-N/streams/{name}/log.bin` layout
//!   - Task 2: `migrate_legacy_layout`, `cleanup_legacy_dir` (D-01, D-02)

use beava::state::event_log::{EventLog, migrate_legacy_layout, cleanup_legacy_dir};
use std::fs;
use std::path::Path;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

fn ts(secs: u64) -> SystemTime {
    UNIX_EPOCH + Duration::from_secs(secs)
}

// ============================================================
// Task 1: stream_log_path path accessor
// ============================================================

/// Test 1: stream_log_path with shard=0 and a clean name.
#[test]
fn test_stream_log_path_shard0_clean_name() {
    let tmp = tempfile::TempDir::new().unwrap();
    let path = EventLog::stream_log_path(tmp.path(), 0, "Transactions");
    assert_eq!(
        path,
        tmp.path().join("shard-0/streams/Transactions/log.bin"),
        "shard-0 path for 'Transactions' should be data_dir/shard-0/streams/Transactions/log.bin"
    );
}

/// Test 2: stream_log_path applies the same sanitize_stream_name logic.
/// "my stream!" — slash/backslash are not present, but the space and `!` are allowed by the
/// existing sanitizer (it only replaces `/`, `\`, NUL, and `..`).
/// We re-verify against a name with `/` to confirm sanitization.
#[test]
fn test_stream_log_path_shard7_sanitizes_name() {
    let tmp = tempfile::TempDir::new().unwrap();
    // "my/stream" → "my_stream" after sanitize_stream_name
    let path = EventLog::stream_log_path(tmp.path(), 7, "my/stream");
    assert_eq!(
        path,
        tmp.path().join("shard-7/streams/my_stream/log.bin"),
        "slash in stream name must be sanitized to underscore"
    );
}

/// Test 3: EventLog::new_for_shard opens/creates files under
/// data_dir/shard-{N}/streams/ instead of the legacy flat layout.
#[test]
fn test_new_for_shard_creates_directory_tree() {
    let tmp = tempfile::TempDir::new().unwrap();
    let log = EventLog::new_for_shard(tmp.path().to_path_buf(), 0).unwrap();
    // Register a stream — this should create shard-0/streams/Txn/log.bin
    log.register_stream("Txn", None).unwrap();
    let expected = tmp.path().join("shard-0/streams/Txn/log.bin");
    assert!(
        expected.exists(),
        "log file should be at shard-0/streams/Txn/log.bin, not at root"
    );
    // Confirm the legacy flat path does NOT exist
    assert!(
        !tmp.path().join("Txn.log").exists(),
        "legacy flat Txn.log must NOT be created with new_for_shard"
    );
}

/// Test 4: Appending and reading entries round-trips correctly under the new layout.
#[test]
fn test_new_for_shard_append_read_roundtrip() {
    let tmp = tempfile::TempDir::new().unwrap();
    let log = EventLog::new_for_shard(tmp.path().to_path_buf(), 2).unwrap();
    log.register_stream("Orders", None).unwrap();

    let now = ts(1_000);
    assert!(log.append("Orders", b"order_payload", now).unwrap());

    let entries = log.read_entries("Orders").unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].payload, b"order_payload");
    assert_eq!(entries[0].timestamp, now);

    // Confirm the file is at the new path
    let path = EventLog::stream_log_path(tmp.path(), 2, "Orders");
    assert!(path.exists(), "log.bin must exist at shard-2/streams/Orders/log.bin");
}

// ============================================================
// Task 2: migrate_legacy_layout + cleanup_legacy_dir
// ============================================================

/// Test 5: migrate_legacy_layout moves legacy `data/logs/Transactions.log` to
/// `data/shard-0/streams/Transactions/log.bin`. Original file is gone.
#[test]
fn test_migrate_legacy_layout_moves_file() {
    let tmp = tempfile::TempDir::new().unwrap();
    let data_dir = tmp.path();

    // Create legacy layout: data/logs/Transactions.log
    let legacy_dir = data_dir.join("logs");
    fs::create_dir_all(&legacy_dir).unwrap();
    let legacy_file = legacy_dir.join("Transactions.log");
    fs::write(&legacy_file, b"legacy_data").unwrap();

    migrate_legacy_layout(data_dir).unwrap();

    // New location must exist with the data
    let new_path = EventLog::stream_log_path(data_dir, 0, "Transactions");
    assert!(new_path.exists(), "migrated file must exist at shard-0/streams/Transactions/log.bin");
    assert_eq!(fs::read(&new_path).unwrap(), b"legacy_data");

    // Original file must be gone
    assert!(
        !legacy_file.exists(),
        "original data/logs/Transactions.log must be removed after migration"
    );
}

/// Test 6: migrate_legacy_layout is idempotent — calling twice does not error or duplicate.
#[test]
fn test_migrate_legacy_layout_idempotent() {
    let tmp = tempfile::TempDir::new().unwrap();
    let data_dir = tmp.path();

    let legacy_dir = data_dir.join("logs");
    fs::create_dir_all(&legacy_dir).unwrap();
    fs::write(legacy_dir.join("S.log"), b"content").unwrap();

    // First call
    migrate_legacy_layout(data_dir).unwrap();
    // Second call — must not error (data/logs/ is now empty or gone)
    let result = migrate_legacy_layout(data_dir);
    assert!(result.is_ok(), "second call to migrate_legacy_layout must not error");

    // Data must not be duplicated
    let new_path = EventLog::stream_log_path(data_dir, 0, "S");
    assert_eq!(fs::read(&new_path).unwrap(), b"content");
}

/// Test 7: cleanup_legacy_dir removes data/logs/ when it is empty.
#[test]
fn test_cleanup_legacy_dir_removes_empty_dir() {
    let tmp = tempfile::TempDir::new().unwrap();
    let data_dir = tmp.path();

    let legacy_dir = data_dir.join("logs");
    fs::create_dir_all(&legacy_dir).unwrap();
    // Directory is empty — cleanup should remove it
    cleanup_legacy_dir(data_dir).unwrap();
    assert!(
        !legacy_dir.exists(),
        "empty data/logs/ must be removed by cleanup_legacy_dir"
    );
}

/// Test 8 (D-02 safety check): cleanup_legacy_dir is a no-op when data/logs/
/// still contains files (safety: never delete operator data).
#[test]
fn test_cleanup_legacy_dir_noop_if_nonempty() {
    let tmp = tempfile::TempDir::new().unwrap();
    let data_dir = tmp.path();

    let legacy_dir = data_dir.join("logs");
    fs::create_dir_all(&legacy_dir).unwrap();
    let leftover = legacy_dir.join("unprocessed.log");
    fs::write(&leftover, b"unprocessed").unwrap();

    // Must NOT error and must NOT delete the directory
    cleanup_legacy_dir(data_dir).unwrap();
    assert!(
        legacy_dir.exists(),
        "non-empty data/logs/ must NOT be removed"
    );
    assert!(
        leftover.exists(),
        "files inside non-empty data/logs/ must be preserved"
    );
}
