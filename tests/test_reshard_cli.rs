//! TDD test suite for `tally reshard` — Phase 52-04.
//!
//! Task 1 tests (Tests 1–5): unit tests for `rehash_to_shard` + `reshard_data_dir`
//! Task 2 tests (Tests 6–9): end-to-end CLI tests for `tally reshard` subcommand

use std::fs;
use std::path::Path;

// ──────────────────────────────────────────────────────────────────────────────
// Helper: read all log entries from a shard dir using raw postcard framing
// (mirrors EventLog::read_entries without requiring a live EventLog instance).
// ──────────────────────────────────────────────────────────────────────────────

fn count_entries_in_log(path: &Path) -> usize {
    use std::io::{BufReader, Read as IoRead};
    if !path.exists() {
        return 0;
    }
    let file = fs::File::open(path).unwrap();
    let mut reader = BufReader::new(file);
    let mut count = 0;
    loop {
        let mut len_buf = [0u8; 4];
        match reader.read_exact(&mut len_buf) {
            Ok(()) => {}
            Err(ref e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(e) => panic!("read error: {}", e),
        }
        let len = u32::from_be_bytes(len_buf) as usize;
        let mut data = vec![0u8; len];
        reader.read_exact(&mut data).unwrap();
        count += 1;
    }
    count
}

// ──────────────────────────────────────────────────────────────────────────────
// Helper: write a minimal v8 snapshot to `dir/snapshot.bin`
// ──────────────────────────────────────────────────────────────────────────────

fn write_v8_snapshot(dir: &Path, shard_count: u8) {
    use beava::state::snapshot::{
        save_base_snapshot_v8, BaseSnapshotStateV8, SnapshotHeader, SnapshotType,
    };
    let snap = BaseSnapshotStateV8 {
        header: SnapshotHeader {
            snapshot_type: SnapshotType::Base,
            sequence: 1,
        },
        entities: vec![],
        pipelines: vec![],
        backfill_complete: vec![],
        shard_count: shard_count as u16,
        replica_lsn_map: std::collections::HashMap::new(),
    };
    let bytes = save_base_snapshot_v8(&snap).expect("save_base_snapshot_v8 failed");
    fs::write(dir.join("snapshot.bin"), bytes).expect("write snapshot.bin failed");
}

// ──────────────────────────────────────────────────────────────────────────────
// Helper: write N log entries to a per-shard stream log
// ──────────────────────────────────────────────────────────────────────────────

fn write_log_entries(dir: &Path, shard_id: u8, stream_name: &str, keys: &[&str]) {
    use beava::state::event_log::LogEntry;
    use std::io::{BufWriter, Write};
    use std::time::SystemTime;

    let stream_dir = dir
        .join(format!("shard-{}/streams/{}", shard_id, stream_name));
    fs::create_dir_all(&stream_dir).unwrap();
    let log_path = stream_dir.join("log.bin");
    let file = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&log_path)
        .unwrap();
    let mut writer = BufWriter::new(file);
    for key in keys {
        let payload = serde_json::json!({"key": key, "value": 1}).to_string().into_bytes();
        let entry = LogEntry {
            timestamp: SystemTime::now(),
            payload,
        };
        let encoded = postcard::to_stdvec(&entry).expect("postcard encode");
        writer
            .write_all(&(encoded.len() as u32).to_be_bytes())
            .unwrap();
        writer.write_all(&encoded).unwrap();
    }
    writer.flush().unwrap();
}

// ══════════════════════════════════════════════════════════════════════════════
// Task 1 — Tests 1–5: rehash_to_shard unit tests + reshard_data_dir lock test
// ══════════════════════════════════════════════════════════════════════════════

/// Test 1: `rehash_to_shard("user-abc", 8)` is deterministic across 1000 calls.
#[test]
fn test_reshard_rehash_determinism() {
    use beava::reshard::rehash_to_shard;
    let first = rehash_to_shard("user-abc", 8);
    for _ in 0..1000 {
        assert_eq!(
            rehash_to_shard("user-abc", 8),
            first,
            "rehash_to_shard must be deterministic"
        );
    }
}

/// Test 2: `rehash_to_shard(key, 1)` always returns 0 for any key (N=1 identity).
#[test]
fn test_reshard_n1_identity() {
    use beava::reshard::rehash_to_shard;
    let keys = [
        "user-abc", "user-xyz", "order-001", "customer-99", "", "🔑",
    ];
    for key in &keys {
        assert_eq!(
            rehash_to_shard(key, 1),
            0,
            "rehash_to_shard(_, 1) must always return 0; key={:?}",
            key
        );
    }
}

/// Test 3: N=1→N=8 redistribution — 10 000 random keys all land in [0, 7];
/// distribution is non-degenerate (each shard receives >500 keys on average,
/// so no shard is starved — actual expectation is 1250 each).
#[test]
fn test_reshard_n8_distribution() {
    use beava::reshard::rehash_to_shard;

    // Deterministic key generation (no external rand dep in tests — use simple hash)
    let mut counts = [0usize; 8];
    for i in 0u64..10_000 {
        let key = format!("key-{:016x}", i.wrapping_mul(6364136223846793005));
        let shard = rehash_to_shard(&key, 8);
        assert!(
            shard < 8,
            "rehash_to_shard result {} out of range [0, 7]",
            shard
        );
        counts[shard as usize] += 1;
    }

    // Each shard should receive at least 500 entries (1000× minimum per plan)
    for (i, &count) in counts.iter().enumerate() {
        assert!(
            count >= 500,
            "shard {} received only {} keys (expected ≥500)",
            i,
            count
        );
    }
}

/// Test 4: Round-trip identity — rehash_to_shard(key, 1) returns 0 on both calls.
#[test]
fn test_reshard_n1_round_trip() {
    use beava::reshard::rehash_to_shard;
    let key = "some-entity-key";
    let first = rehash_to_shard(key, 1);
    assert_eq!(first, 0);
    let second = rehash_to_shard(key, 1);
    assert_eq!(second, 0);
}

/// Test 5: `reshard_data_dir` returns Err containing "held by a running server"
/// when the source dir is locked by another process (simulated here by holding
/// the exclusive lock in the test itself before calling reshard).
#[test]
fn test_reshard_data_dir_refuses_locked_dir() {
    use beava::reshard::reshard_data_dir;
    use fs2::FileExt;

    let src = tempfile::tempdir().unwrap();
    let dst = tempfile::tempdir().unwrap();

    // Write a minimal valid v8 snapshot so reshard_data_dir can parse it
    write_v8_snapshot(src.path(), 1);

    // Simulate a live server by locking .beava.lock before calling reshard
    let lock_path = src.path().join(".beava.lock");
    let lock_file = fs::File::create(&lock_path).unwrap();
    lock_file
        .try_lock_exclusive()
        .expect("test should be able to grab the lock initially");

    let result = reshard_data_dir(1, 2, src.path(), dst.path());
    assert!(result.is_err(), "reshard_data_dir should fail when dir is locked");
    let msg = format!("{}", result.unwrap_err());
    assert!(
        msg.contains("held by a running server"),
        "error message should contain 'held by a running server'; got: {}",
        msg
    );
}

// ══════════════════════════════════════════════════════════════════════════════
// Task 2 — Tests 6–9: CLI end-to-end tests
// ══════════════════════════════════════════════════════════════════════════════

/// Test 6: End-to-end — create N=1 data dir with one stream log (10 entries),
/// run reshard 1→8, assert exit code 0 and 8 shard dirs with entries summing
/// to original count.
#[test]
fn test_reshard_cli_e2e_1_to_8() {
    use beava::reshard::reshard_data_dir;

    let src = tempfile::tempdir().unwrap();
    let dst = tempfile::tempdir().unwrap();

    // Set up N=1 source: snapshot + one stream log with 10 entries
    write_v8_snapshot(src.path(), 1);
    let keys: Vec<String> = (0..10).map(|i| format!("user-{:04}", i)).collect();
    let key_refs: Vec<&str> = keys.iter().map(|s| s.as_str()).collect();
    write_log_entries(src.path(), 0, "Transactions", &key_refs);

    // Run reshard 1 → 8
    let result = reshard_data_dir(1, 8, src.path(), dst.path());
    assert!(
        result.is_ok(),
        "reshard_data_dir failed: {:?}",
        result.unwrap_err()
    );

    // Assert 8 shard directories were created
    for s in 0..8u8 {
        let shard_dir = dst.path().join(format!("shard-{}", s));
        assert!(
            shard_dir.exists(),
            "expected shard dir {} to exist",
            shard_dir.display()
        );
    }

    // Assert total log entries across all shards == original 10
    let mut total = 0usize;
    for s in 0..8u8 {
        let log_path = dst
            .path()
            .join(format!("shard-{}/streams/Transactions/log.bin", s));
        total += count_entries_in_log(&log_path);
    }
    assert_eq!(total, 10, "total entries after reshard should equal original 10");

    // Assert output snapshot has shard_count = 8
    use beava::state::snapshot::{load_snapshot_file, SnapshotFile};
    let snap_bytes = fs::read(dst.path().join("snapshot.bin")).expect("snapshot.bin missing");
    match load_snapshot_file(&snap_bytes) {
        Some(SnapshotFile::Base(v8)) => {
            assert_eq!(v8.shard_count, 8, "output snapshot shard_count should be 8");
        }
        other => panic!("expected SnapshotFile::Base, got {:?}", other),
    }
}

/// Test 7: Missing required args trigger usage + exit 1.
/// We test the parse helper directly to stay hermetic.
#[test]
fn test_reshard_cli_missing_args_returns_error() {
    use beava::reshard::parse_reshard_args;

    // Missing --to
    let args: Vec<String> = ["beava", "reshard", "--from", "1", "--data-dir", "/tmp/src", "--out-dir", "/tmp/dst"]
        .iter()
        .map(|s| s.to_string())
        .collect();
    assert!(parse_reshard_args(&args).is_err(), "should fail without --to");

    // Missing --from
    let args: Vec<String> = ["beava", "reshard", "--to", "8", "--data-dir", "/tmp/src", "--out-dir", "/tmp/dst"]
        .iter()
        .map(|s| s.to_string())
        .collect();
    assert!(parse_reshard_args(&args).is_err(), "should fail without --from");

    // Missing --data-dir
    let args: Vec<String> = ["beava", "reshard", "--from", "1", "--to", "8", "--out-dir", "/tmp/dst"]
        .iter()
        .map(|s| s.to_string())
        .collect();
    assert!(parse_reshard_args(&args).is_err(), "should fail without --data-dir");

    // Missing --out-dir
    let args: Vec<String> = ["beava", "reshard", "--from", "1", "--to", "8", "--data-dir", "/tmp/src"]
        .iter()
        .map(|s| s.to_string())
        .collect();
    assert!(parse_reshard_args(&args).is_err(), "should fail without --out-dir");
}

/// Test 8: `--replace` flag renames out_dir → data_dir and data_dir → data_dir.bak.
#[test]
fn test_reshard_cli_replace_atomic_swap() {
    use beava::reshard::{reshard_data_dir, swap_replace};

    let src = tempfile::tempdir().unwrap();
    let dst = tempfile::tempdir().unwrap();

    write_v8_snapshot(src.path(), 1);
    write_log_entries(src.path(), 0, "Events", &["key-a", "key-b"]);

    reshard_data_dir(1, 2, src.path(), dst.path()).expect("reshard failed");

    let src_path = src.path().to_path_buf();
    let dst_path = dst.path().to_path_buf();
    // Keep tempdir alive but release the cleanup guard so we can rename
    let _ = (src.into_path(), dst.into_path());

    // Perform the atomic swap
    swap_replace(&src_path, &dst_path).expect("swap_replace failed");

    // data_dir.bak should exist (old src)
    let bak = src_path.with_extension("bak");
    // Note: `with_extension` on a dir path appends correctly only if the dir
    // path has no extension. Use format instead for safety.
    let bak_path = std::path::PathBuf::from(format!("{}.bak", src_path.display()));
    assert!(
        bak_path.exists() || bak.exists(),
        "data_dir.bak should exist after --replace swap"
    );

    // data_dir (old src_path) should now hold what was in out_dir
    assert!(
        src_path.join("snapshot.bin").exists(),
        "data_dir should now contain output snapshot"
    );
}

/// Test 9: Locked data-dir returns Err with "held by a running server".
/// (Duplicates Test 5 at the CLI interface level — we test `reshard_data_dir`
/// which is the shared kernel of the CLI dispatch.)
#[test]
fn test_reshard_cli_locked_dir_error() {
    use beava::reshard::reshard_data_dir;
    use fs2::FileExt;

    let src = tempfile::tempdir().unwrap();
    let dst = tempfile::tempdir().unwrap();

    write_v8_snapshot(src.path(), 1);

    let lock_path = src.path().join(".beava.lock");
    let lock_file = fs::File::create(&lock_path).unwrap();
    lock_file.try_lock_exclusive().unwrap();

    let result = reshard_data_dir(1, 4, src.path(), dst.path());
    assert!(result.is_err());
    let msg = format!("{}", result.unwrap_err());
    assert!(
        msg.contains("held by a running server"),
        "expected 'held by a running server' in error; got: {}",
        msg
    );
}
