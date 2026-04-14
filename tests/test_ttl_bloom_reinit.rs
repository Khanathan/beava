//! Phase 25-02 Task 1: per-Table bloom filter tracks evicted keys; reinit
//! detection bumps the counter; false-positive rate is bounded; generation
//! rotation drops keys beyond the 7d window.

use std::sync::atomic::Ordering;
use std::time::{Duration, SystemTime};
use tally::state::eviction_tracker::{EvictionTracker, ROTATE_INTERVAL};

#[test]
fn evicted_then_reinit_bumps_counter_within_window() {
    let t = EvictionTracker::new();
    // Evict 100 keys from Table "Users"
    for i in 0..100 {
        t.record_eviction("Users", &format!("u{}", i));
    }
    // Re-push 50 of those
    for i in 0..50 {
        t.check_reinit("Users", &format!("u{}", i));
    }
    // Plus 50 fresh keys
    for i in 100..150 {
        t.check_reinit("Users", &format!("u{}", i));
    }
    let reinits = t.reinit_count("Users");
    // Expect 50 hits; allow a handful of bloom FPs (1% of 50 = 0.5 → at
    // most ~2 false positives in practice on a fresh filter).
    assert!(
        (50..=53).contains(&reinits),
        "reinit_count={} outside expected 50..=53 range",
        reinits
    );
}

#[test]
fn fp_rate_below_1pct() {
    let t = EvictionTracker::new();
    for i in 0..10_000 {
        t.record_eviction("Users", &format!("inserted_{}", i));
    }
    // Reset the reinit counter so we only observe FPs on fresh queries.
    t.reinits
        .entry("Users".to_string())
        .or_default()
        .store(0, Ordering::Relaxed);
    let mut fps = 0;
    for i in 0..10_000 {
        if t.check_reinit("Users", &format!("query_{}", i)) {
            fps += 1;
        }
    }
    let rate = fps as f64 / 10_000.0;
    assert!(
        rate <= 0.01,
        "false positive rate {:.4} exceeded 1% threshold (fps={})",
        rate,
        fps
    );
}

#[test]
fn generation_rotation_drops_old_key_after_7d() {
    let t = EvictionTracker::new();
    t.record_eviction("Users", "u1");
    assert!(t.check_reinit("Users", "u1"));
    // Reset the reinit counter so we can observe the post-rotation state.
    t.reinits
        .entry("Users".to_string())
        .or_default()
        .store(0, Ordering::Relaxed);

    // Simulate two full rotations (2 × ROTATE_INTERVAL ≈ 7 days total).
    // Each rotate_generation advances per-Table `rotated_at`, so the second
    // call must be another full ROTATE_INTERVAL past the first.
    let future1 = SystemTime::now() + ROTATE_INTERVAL + Duration::from_secs(60);
    t.rotate_generation(future1);
    let future2 = future1 + ROTATE_INTERVAL + Duration::from_secs(60);
    t.rotate_generation(future2);

    // After two rotations the original key should be fully dropped.
    assert!(!t.check_reinit("Users", "u1"));
    assert_eq!(t.reinit_count("Users"), 0);
}

#[test]
fn never_evicted_key_yields_no_reinit() {
    let t = EvictionTracker::new();
    t.record_eviction("Users", "u1");
    assert!(!t.check_reinit("Users", "never_evicted_42"));
    assert_eq!(t.reinit_count("Users"), 0);
}

#[test]
fn unknown_table_yields_no_reinit() {
    let t = EvictionTracker::new();
    // No record_eviction call for "Orphans" — the bloom never gets created.
    assert!(!t.check_reinit("Orphans", "u1"));
    assert_eq!(t.reinit_count("Orphans"), 0);
}

#[test]
fn eviction_counter_is_per_table_and_monotone() {
    let t = EvictionTracker::new();
    t.record_eviction("Users", "u1");
    t.record_eviction("Users", "u2");
    t.record_eviction("Orders", "o1");
    assert_eq!(t.eviction_count("Users"), 2);
    assert_eq!(t.eviction_count("Orders"), 1);
}

#[test]
fn memory_bytes_grows_with_tables() {
    let t = EvictionTracker::new();
    assert_eq!(t.memory_bytes(), 0);
    t.record_eviction("Users", "u1");
    let m1 = t.memory_bytes();
    assert!(m1 > 0);
    t.record_eviction("Orders", "o1");
    let m2 = t.memory_bytes();
    assert!(m2 > m1);
}
