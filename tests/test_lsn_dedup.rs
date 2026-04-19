//! Phase 52-06: LSN-based dedup tests (TPC-CORR-06).
//!
//! Tests 1–3: LSN tagging, pack/unpack, and seq monotonicity in EventLog.
//! Tests 4–6: Replica dedup filter — no double-apply on reconnect.
//!
//! TDD: tests written RED before implementation; implementation makes them GREEN.

use beava::state::event_log::{lsn_pack, lsn_unpack, EventLog, LogEntry};
use beava::server::replica::{dedup_drop_count, reset_dedup_drop_count, LsnDedupFilter};
use beava::state::snapshot::{
    save_base_snapshot_v8, load_snapshot_file, BaseSnapshotStateV8,
    SnapshotFile, SnapshotHeader, SnapshotType,
};
use std::collections::HashMap;
use std::time::{Duration, UNIX_EPOCH};

fn ts(secs: u64) -> std::time::SystemTime {
    UNIX_EPOCH + Duration::from_secs(secs)
}

// ---------------------------------------------------------------------------
// Test 1: Appending 5 entries for stream S on shard 2 produces correct LSNs.
// Each entry must have upstream_shard_id=2 in bits 63-56, correct stream_ord
// in bits 55-40, and seq values 0..4 in bits 39-0.
// ---------------------------------------------------------------------------
#[test]
fn test_lsn_tagging() {
    let tmp = tempfile::TempDir::new().unwrap();
    let mut log = EventLog::new(tmp.path().to_path_buf()).unwrap();

    let upstream_shard_id: u8 = 2;
    let stream_ord: u16 = 7;
    // Initialize seq counters from an empty replica_lsn_map (fresh start).
    log.load_seq_counters(&HashMap::new());

    log.register_stream("orders", None).unwrap();

    let now = ts(1000);
    let mut lsns = Vec::new();
    for i in 0..5u64 {
        let lsn = log
            .append_lsn_tagged("orders", b"payload", now, upstream_shard_id, stream_ord)
            .unwrap();
        lsns.push(lsn);
    }

    for (i, &lsn) in lsns.iter().enumerate() {
        let (shard, ord, seq) = lsn_unpack(lsn);
        assert_eq!(shard, upstream_shard_id, "entry {}: upstream_shard_id mismatch", i);
        assert_eq!(ord, stream_ord, "entry {}: stream_ord mismatch", i);
        assert_eq!(seq, i as u64, "entry {}: seq mismatch (expected {})", i, i);
    }
}

// ---------------------------------------------------------------------------
// Test 2: lsn_pack / lsn_unpack round-trip correctness.
// ---------------------------------------------------------------------------
#[test]
fn test_lsn_pack_unpack() {
    let upstream_shard_id: u8 = 2;
    let stream_ord: u16 = 5;
    let seq: u64 = 1000;

    let packed = lsn_pack(upstream_shard_id, stream_ord, seq);
    let (u, s, q) = lsn_unpack(packed);
    assert_eq!(u, upstream_shard_id, "upstream_shard_id mismatch");
    assert_eq!(s, stream_ord, "stream_ord mismatch");
    assert_eq!(q, seq, "seq mismatch");
}

// ---------------------------------------------------------------------------
// Test 3: seq monotonicity across simulated restart.
// After appending 5 events (seq 0..4), persist seq counters, reload them,
// then append 1 more entry and assert seq == 5.
// ---------------------------------------------------------------------------
#[test]
fn test_lsn_seq_monotonic() {
    let tmp = tempfile::TempDir::new().unwrap();
    let upstream_shard_id: u8 = 2;
    let stream_ord: u16 = 3;

    // --- Pre-restart phase ---
    {
        let mut log = EventLog::new(tmp.path().to_path_buf()).unwrap();
        log.load_seq_counters(&HashMap::new());
        log.register_stream("events", None).unwrap();

        let now = ts(1000);
        for _ in 0..5 {
            log.append_lsn_tagged("events", b"data", now, upstream_shard_id, stream_ord)
                .unwrap();
        }

        // Export seq counters → simulate snapshot save.
        let lsn_map = log.current_lsn_map();
        // The seq counter for (stream, shard) after 5 appends should be 5.
        let key: (String, u8) = ("events".to_string(), upstream_shard_id);
        assert_eq!(
            *lsn_map.get(&key).unwrap_or(&0),
            5,
            "seq counter should be 5 after 5 appends"
        );

        // Persist via snapshot (simulate).
        let snap = BaseSnapshotStateV8 {
            header: SnapshotHeader {
                snapshot_type: SnapshotType::Base,
                sequence: 1,
            },
            entities: vec![],
            pipelines: vec![],
            backfill_complete: vec![],
            shard_count: 1,
            replica_lsn_map: lsn_map,
        };
        let _ = save_base_snapshot_v8(&snap).unwrap(); // just validate it serializes
    }

    // --- Post-restart phase: load seq counters from the saved map ---
    {
        let mut log = EventLog::new(tmp.path().to_path_buf()).unwrap();
        let mut initial_map: HashMap<(String, u8), u64> = HashMap::new();
        initial_map.insert(("events".to_string(), upstream_shard_id), 5);
        log.load_seq_counters(&initial_map);

        log.register_stream("events", None).unwrap();
        let lsn = log
            .append_lsn_tagged("events", b"data", ts(1001), upstream_shard_id, stream_ord)
            .unwrap();
        let (_, _, seq) = lsn_unpack(lsn);
        assert_eq!(seq, 5, "seq after restart should continue from 5");
    }
}

// ---------------------------------------------------------------------------
// Test 4: No feature doubling on reconnect (dedup filter rejects stale LSNs).
//
// We simulate the reconnect by applying the same batch of events twice via
// LsnDedupFilter. The second pass should produce zero accepted events.
// ---------------------------------------------------------------------------
#[test]
fn test_lsn_dedup_no_doubling_on_reconnect() {
    reset_dedup_drop_count();

    let stream_name = "clicks";
    let upstream_shard_id: u8 = 0;
    let stream_ord: u16 = 0;

    // Build 100 events with sequential LSNs.
    let events: Vec<u64> = (0..100)
        .map(|i| lsn_pack(upstream_shard_id, stream_ord, i))
        .collect();

    let mut filter = LsnDedupFilter::new(HashMap::new());

    // First pass: all events accepted (count tracker simulates feature mutations).
    let mut accepted_first = 0u64;
    for &lsn in &events {
        if filter.accept(stream_name, upstream_shard_id, lsn) {
            accepted_first += 1;
        }
    }
    assert_eq!(accepted_first, 100, "all 100 events should be accepted on first pass");

    // Second pass (simulate reconnect replay): all events are stale, should be dropped.
    let mut accepted_second = 0u64;
    for &lsn in &events {
        if filter.accept(stream_name, upstream_shard_id, lsn) {
            accepted_second += 1;
        }
    }
    assert_eq!(accepted_second, 0, "no events should be accepted on reconnect replay");
}

// ---------------------------------------------------------------------------
// Test 5: Events with LSN <= max_lsn_seen are silently dropped; dedup_drop_count
// increments for each such drop.
// ---------------------------------------------------------------------------
#[test]
fn test_lsn_dedup_drop_count() {
    reset_dedup_drop_count();
    let drops_before = dedup_drop_count();

    let stream_name = "transactions";
    let upstream_shard_id: u8 = 1;
    let stream_ord: u16 = 2;

    // Pre-set max_lsn_seen for (stream, shard) = lsn_pack(1, 2, 49).
    let max_lsn = lsn_pack(upstream_shard_id, stream_ord, 49);
    let mut initial_map: HashMap<(String, u8), u64> = HashMap::new();
    initial_map.insert((stream_name.to_string(), upstream_shard_id), max_lsn);

    let mut filter = LsnDedupFilter::new(initial_map);

    // Send events 0..99 — the first 50 (seq 0..49) are stale, last 50 are new.
    let mut accepted = 0u64;
    for seq in 0..100u64 {
        let lsn = lsn_pack(upstream_shard_id, stream_ord, seq);
        if filter.accept(stream_name, upstream_shard_id, lsn) {
            accepted += 1;
        }
    }

    let drops_after = dedup_drop_count();
    // Events with LSN <= max_lsn (seq 0..49, i.e. 50 events) should be dropped.
    assert_eq!(
        drops_after - drops_before,
        50,
        "50 stale events should have been dropped"
    );
    assert_eq!(accepted, 50, "50 new events should be accepted");
}

// ---------------------------------------------------------------------------
// Test 6: After accepting 100 events, snapshot.replica_lsn_map contains correct
// max_lsn_seen values.
// ---------------------------------------------------------------------------
#[test]
fn test_lsn_snapshot_persistence() {
    let stream_name = "logins";
    let upstream_shard_id: u8 = 3;
    let stream_ord: u16 = 1;

    let mut filter = LsnDedupFilter::new(HashMap::new());

    // Accept 100 events with seq 0..99.
    for seq in 0..100u64 {
        let lsn = lsn_pack(upstream_shard_id, stream_ord, seq);
        filter.accept(stream_name, upstream_shard_id, lsn);
    }

    // Extract the lsn map and verify it contains max_lsn for (stream, shard).
    let lsn_map = filter.current_lsn_map();
    let key = (stream_name.to_string(), upstream_shard_id);
    let stored_lsn = lsn_map.get(&key).copied().unwrap_or(0);

    let expected_lsn = lsn_pack(upstream_shard_id, stream_ord, 99);
    assert_eq!(
        stored_lsn, expected_lsn,
        "max_lsn_seen after 100 events should be lsn_pack({}, {}, 99)",
        upstream_shard_id, stream_ord
    );

    // Also verify it round-trips through a snapshot.
    let snap = BaseSnapshotStateV8 {
        header: SnapshotHeader {
            snapshot_type: SnapshotType::Base,
            sequence: 1,
        },
        entities: vec![],
        pipelines: vec![],
        backfill_complete: vec![],
        shard_count: 1,
        replica_lsn_map: lsn_map,
    };
    let bytes = save_base_snapshot_v8(&snap).unwrap();
    match load_snapshot_file(&bytes) {
        Some(SnapshotFile::Base(restored)) => {
            let restored_lsn = restored
                .replica_lsn_map
                .get(&key)
                .copied()
                .unwrap_or(0);
            assert_eq!(
                restored_lsn, expected_lsn,
                "LSN map must survive snapshot round-trip"
            );
        }
        other => panic!("expected Base snapshot, got {:?}", other.is_some()),
    }
}
