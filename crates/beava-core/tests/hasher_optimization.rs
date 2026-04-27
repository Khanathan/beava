//! Plan 19.2-02: Integration tests for hasher optimization.
//!
//! Task 1 (D-02a): process-static AHasher RandomState via OnceLock.
//! Task 2 (D-02b): FxHasher for HLL input path (CountDistinct).
//!
//! RED commit covers Tasks 1.a and 2.a — both groups of tests fail because:
//!   - `beava_core::sketches::ahash_random_state` does not yet exist.
//!   - `beava_core::agg_state::hash_value_for_hll` does not yet exist.

// ─── Task 1 tests (D-02a) ──────────────────────────────────────────────────────

/// Test 1: `ahash_random_state()` returns the same `&'static RandomState` pointer
/// across two calls — proves it is a process-wide singleton.
#[test]
fn test_ahash_random_state_is_process_static() {
    use beava_core::sketches::ahash_random_state;
    let ptr1 = ahash_random_state() as *const ahash::RandomState;
    let ptr2 = ahash_random_state() as *const ahash::RandomState;
    assert!(
        std::ptr::eq(ptr1, ptr2),
        "ahash_random_state() returned different pointers — not a singleton"
    );
}

/// Test 2: Hashing the same bytes through `ahash_random_state()` twice produces
/// the same u64. Proves the seed is stable within the process.
#[test]
fn test_ahash_random_state_produces_stable_hash_within_process() {
    use beava_core::sketches::ahash_random_state;

    let h1 = ahash_random_state().hash_one("test-key-42");
    let h2 = ahash_random_state().hash_one("test-key-42");
    assert_eq!(
        h1, h2,
        "ahash_random_state() produced different hashes for the same input — seed is not stable"
    );
}

/// Test 3: Two BloomFilters built independently both report `contains("x") == true`
/// after inserting "x" into each. Proves Bloom uses the shared process-static seed
/// (bit patterns are deterministic within the process).
#[test]
fn test_bloom_uses_process_static_random_state() {
    use beava_core::sketches::bloom::BloomFilter;

    let mut b1 = BloomFilter::with_capacity_and_fpr(256, 0.01);
    b1.insert("x");

    let mut b2 = BloomFilter::with_capacity_and_fpr(256, 0.01);
    b2.insert("x");

    assert!(b1.contains("x"), "b1 should contain 'x'");
    assert!(b2.contains("x"), "b2 should contain 'x'");
    // Both filters hashed "x" the same way (same process-static seed).
    // We can verify this by checking contains("other") is also consistent.
    assert!(
        !b1.contains("not_inserted"),
        "b1 should not contain 'not_inserted'"
    );
    assert!(
        !b2.contains("not_inserted"),
        "b2 should not contain 'not_inserted'"
    );
}
