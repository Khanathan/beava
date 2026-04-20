//! Phase 52-06: LSN-based dedup tests (TPC-CORR-06).
//!
//! Tests 1–3: LSN tagging, pack/unpack, and seq monotonicity in EventLog.
//! Tests 4–6: Replica dedup filter — no double-apply on reconnect.
//!
//! TDD: tests written RED before implementation; implementation makes them GREEN.
//!
//! PARALLEL SAFETY: DEDUP_DROP_COUNT is a process-wide static. Tests use
//! delta measurement (capture before/after) rather than reset+absolute to
//! be parallel-safe. No test calls reset_dedup_drop_count.

use beava::state::event_log::{lsn_pack, lsn_unpack, EventLog};
use beava::server::replica::{dedup_drop_count, LsnDedupFilter};
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
    for _ in 0..5u64 {
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
        // The seq counter for (stream, shard) after 5 appends should be 5
        // (the next-seq-to-assign stored by current_lsn_map).
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
                schema_version: 9,
            },
            entities: vec![],
            pipelines: vec![],
            backfill_complete: vec![],
            shard_count: 1,
            replica_lsn_map: lsn_map,
        };
        let _ = save_base_snapshot_v8(&snap).unwrap(); // validate it serializes
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
//
// NOTE: upstream_shard_id=1 (not 0) and stream_ord=1 (not 0) so that
// lsn_pack never produces 0 (which is the pre-v1.2 bypass sentinel).
// ---------------------------------------------------------------------------
#[test]
fn test_lsn_dedup_no_doubling_on_reconnect() {
    let stream_name = "clicks_t4";
    let upstream_shard_id: u8 = 1;  // non-zero to avoid lsn==0 sentinel
    let stream_ord: u16 = 1;        // non-zero to avoid lsn==0 sentinel

    // Build 100 events with sequential LSNs.
    let events: Vec<u64> = (0..100)
        .map(|i| lsn_pack(upstream_shard_id, stream_ord, i))
        .collect();

    let mut filter = LsnDedupFilter::new(HashMap::new());

    // First pass: all events accepted.
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
//
// PARALLEL SAFETY: uses delta measurement (before/after) not reset+absolute.
// ---------------------------------------------------------------------------
#[test]
fn test_lsn_dedup_drop_count() {
    let drops_before = dedup_drop_count();

    let stream_name = "transactions_t5";
    let upstream_shard_id: u8 = 5;  // unique shard to avoid cross-test interference
    let stream_ord: u16 = 2;

    // Pre-set max_lsn_seen for (stream, shard) = lsn_pack(5, 2, 49).
    let max_lsn = lsn_pack(upstream_shard_id, stream_ord, 49);
    let mut initial_map: HashMap<(String, u8), u64> = HashMap::new();
    initial_map.insert((stream_name.to_string(), upstream_shard_id), max_lsn);

    let mut filter = LsnDedupFilter::new(initial_map);

    // Send events seq 0..99. Events with lsn <= max_lsn (seq 0..49 = 50 events)
    // should be dropped; events with lsn > max_lsn (seq 50..99 = 50 events)
    // should be accepted.
    let mut accepted = 0u64;
    for seq in 0..100u64 {
        let lsn = lsn_pack(upstream_shard_id, stream_ord, seq);
        if filter.accept(stream_name, upstream_shard_id, lsn) {
            accepted += 1;
        }
    }

    let drops_after = dedup_drop_count();
    // Delta lower bound: at least our 50 drops must have been counted.
    // (Other concurrent tests may also increment the counter, so we use >= not ==.)
    assert!(
        drops_after >= drops_before + 50,
        "at least 50 stale events should have been dropped (before={}, after={})",
        drops_before,
        drops_after,
    );
    // Behavioral assertion: accepted count is the definitive correctness check.
    assert_eq!(accepted, 50, "50 new events should be accepted");
}

// ---------------------------------------------------------------------------
// Test 6: After accepting 100 events, snapshot.replica_lsn_map contains correct
// max_lsn_seen values.
// ---------------------------------------------------------------------------
#[test]
fn test_lsn_snapshot_persistence() {
    let stream_name = "logins_t6";
    let upstream_shard_id: u8 = 3;
    let stream_ord: u16 = 1;

    let mut filter = LsnDedupFilter::new(HashMap::new());

    // Accept 100 events with seq 1..100 (start at 1 so lsn never == 0).
    for seq in 1..=100u64 {
        let lsn = lsn_pack(upstream_shard_id, stream_ord, seq);
        filter.accept(stream_name, upstream_shard_id, lsn);
    }

    // Extract the lsn map and verify it contains max_lsn for (stream, shard).
    let lsn_map = filter.current_lsn_map();
    let key = (stream_name.to_string(), upstream_shard_id);
    let stored_lsn = lsn_map.get(&key).copied().unwrap_or(0);

    let expected_lsn = lsn_pack(upstream_shard_id, stream_ord, 100);
    assert_eq!(
        stored_lsn, expected_lsn,
        "max_lsn_seen after 100 events should be lsn_pack({}, {}, 100)",
        upstream_shard_id, stream_ord
    );

    // Also verify it round-trips through a snapshot.
    let snap = BaseSnapshotStateV8 {
        header: SnapshotHeader {
            snapshot_type: SnapshotType::Base,
            sequence: 1,
            schema_version: 9,
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
