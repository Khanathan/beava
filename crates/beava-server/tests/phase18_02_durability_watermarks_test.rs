//! Phase 18-02 Task 2.1 — `WalLsn` durability-watermarks tests.
//!
//! Tests that `WalLsn` exposes four atomic-loadable watermarks
//! (`committed`, `written`, `synced`; `acked` is derived policy).
//!
//! RED state: `beava_runtime_core::wal_lsn` does not exist yet.
//! All tests reference `WalLsn` from the runtime-core crate and will
//! fail to compile until Task 2.1 GREEN is complete.

use beava_runtime_core::wal_lsn::WalLsn;
use std::sync::Arc;
use std::time::Duration;

// ── basic watermark ordering ──────────────────────────────────────────────────

/// Freshly constructed `WalLsn` starts at zero on all watermarks.
#[test]
fn wal_lsn_starts_at_zero() {
    let lsn = WalLsn::new();
    assert_eq!(lsn.committed(), 0);
    assert_eq!(lsn.written(), 0);
    assert_eq!(lsn.synced(), 0);
}

/// `record(n)` advances `committed` by `n` bytes and returns the new high LSN.
#[test]
fn record_advances_committed_lsn() {
    let lsn = WalLsn::new();
    let pos1 = lsn.record(100);
    assert_eq!(pos1, 100);
    assert_eq!(lsn.committed(), 100);

    let pos2 = lsn.record(200);
    assert_eq!(pos2, 300);
    assert_eq!(lsn.committed(), 300);
}

/// `mark_written` advances `written_lsn`; does NOT touch `synced`.
#[test]
fn mark_written_advances_written_only() {
    let lsn = WalLsn::new();
    let pos = lsn.record(512);
    lsn.mark_written(pos);
    assert_eq!(lsn.written(), 512);
    // synced must still be 0 until mark_synced is called
    assert_eq!(lsn.synced(), 0);
}

/// `mark_synced` advances `synced_lsn`; marks durability fence.
#[test]
fn mark_synced_advances_synced() {
    let lsn = WalLsn::new();
    let pos = lsn.record(512);
    lsn.mark_written(pos);
    lsn.mark_synced(pos);
    assert_eq!(lsn.synced(), 512);
}

/// `synced_at_least` returns false when synced < requested lsn, true when ≥.
#[test]
fn synced_at_least_predicate() {
    let lsn = WalLsn::new();
    let pos = lsn.record(512);
    assert!(!lsn.synced_at_least(pos));
    lsn.mark_written(pos);
    assert!(!lsn.synced_at_least(pos));
    lsn.mark_synced(pos);
    assert!(lsn.synced_at_least(pos));
    // anything ≤ synced is also satisfied
    assert!(lsn.synced_at_least(1));
}

// ── PerEvent waiter wakeup semantics ─────────────────────────────────────────

/// A `wait_for_synced` call on a separate thread unblocks once `mark_synced`
/// advances past `request_lsn`.
///
/// This is the key invariant for `/push-sync` durability.
#[test]
fn wait_for_synced_wakes_when_synced_advances() {
    let lsn = Arc::new(WalLsn::new());

    // Commit 128 bytes into the buffer (apply thread would do this).
    let request_lsn = lsn.record(128);

    // Spawn a waiter thread that calls wait_for_synced.
    let lsn_clone = Arc::clone(&lsn);
    let waiter = std::thread::spawn(move || {
        lsn_clone.wait_for_synced(request_lsn, Duration::from_secs(5))
    });

    // Simulate writer thread: write → mark_written, fsync → mark_synced.
    std::thread::sleep(Duration::from_millis(20));
    lsn.mark_written(request_lsn);
    lsn.mark_synced(request_lsn);

    // Waiter must unblock within a reasonable window.
    let result = waiter.join().expect("waiter panicked");
    assert!(result.is_ok(), "wait_for_synced timed out unexpectedly: {result:?}");
}

/// A `wait_for_synced` call returns `Err` if the timeout fires before synced
/// advances (simulated by using a very short timeout and never calling mark_synced).
#[test]
fn wait_for_synced_times_out() {
    let lsn = Arc::new(WalLsn::new());
    let request_lsn = lsn.record(128);

    // Don't mark_written or mark_synced — let it timeout.
    let result = lsn.wait_for_synced(request_lsn, Duration::from_millis(50));
    assert!(
        result.is_err(),
        "expected timeout but got Ok; synced_lsn = {}",
        lsn.synced()
    );
}

/// Multiple concurrent PerEvent waiters each wake when the synced watermark
/// reaches their specific LSN (not before, not much after).
#[test]
fn multiple_waiters_wake_at_correct_lsn() {
    let lsn = Arc::new(WalLsn::new());

    // Two sequential "events" → two different LSNs.
    let lsn_a = lsn.record(100);
    let lsn_b = lsn.record(200);

    let lsn_a_clone = Arc::clone(&lsn);
    let waiter_a = std::thread::spawn(move || {
        lsn_a_clone.wait_for_synced(lsn_a, Duration::from_secs(5))
    });

    let lsn_b_clone = Arc::clone(&lsn);
    let waiter_b = std::thread::spawn(move || {
        lsn_b_clone.wait_for_synced(lsn_b, Duration::from_secs(5))
    });

    std::thread::sleep(Duration::from_millis(20));

    // Advance synced past lsn_a but not yet lsn_b — waiter A should wake.
    lsn.mark_written(lsn_a);
    lsn.mark_synced(lsn_a);

    let result_a = waiter_a.join().expect("waiter_a panicked");
    assert!(result_a.is_ok(), "waiter_a timed out: {result_a:?}");

    // Now advance synced past lsn_b — waiter B should wake.
    lsn.mark_written(lsn_b);
    lsn.mark_synced(lsn_b);

    let result_b = waiter_b.join().expect("waiter_b panicked");
    assert!(result_b.is_ok(), "waiter_b timed out: {result_b:?}");
}
