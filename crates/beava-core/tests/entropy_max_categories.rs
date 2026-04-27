//! Plan 19.2-06 Task 2 — D-05a: entropy max_categories cap + Prometheus counter.
//!
//! RED commit: all tests fail before Task 2.b adds:
//!   - `max_categories: usize` field to `EntropyStateWrap`
//!   - `beava_entropy_categories_capped_total{op}` AtomicU64 process-static counter
//!   - drop-new behaviour when `inner.distinct_count() >= max_categories` AND the
//!     incoming value is not already tracked
//!
//! GREEN commit: Task 2.b makes all tests pass.
//!
//! NOTE: `EntropyStateWrap::new(max_categories)` constructor must be added in Task 2.b.
//! `EntropyStateWrap::default()` continues to use max_categories = 1024 (no behaviour change).

use beava_core::agg_state::EntropyStateWrap;
use beava_core::row::{Row, Value};

fn row_str(field: &str, v: &str) -> Row {
    Row::new().with_field(field, Value::Str(v.into()))
}

// ── Test 1: constructor exposes max_categories ────────────────────────────────

/// `EntropyStateWrap::new(max_categories)` must exist and expose the cap.
/// RED: `EntropyStateWrap::new` does not exist today.
#[test]
fn entropy_wrap_new_exposes_max_categories() {
    let wrap = EntropyStateWrap::new(4);
    // The cap must be reflected back; we read it through the public inner field.
    assert_eq!(
        wrap.max_categories(),
        4,
        "EntropyStateWrap::new(4) must set max_categories to 4"
    );
}

// ── Test 2: drop-new behaviour at cap ────────────────────────────────────────

/// When distinct categories == max_categories, inserting a NEW category must be
/// silently dropped (not inserted, not counted in entropy). Existing categories
/// continue to accumulate counts.
///
/// RED: Today there is no max_categories field or drop-new guard.
#[test]
fn entropy_wrap_drops_new_category_at_cap() {
    // Cap at 2 distinct categories.
    let mut wrap = EntropyStateWrap::new(2);
    let field = "cat";

    // Insert category "a" and "b" — both should be accepted.
    wrap.update(&row_str(field, "a"), 0, Some(field), true);
    wrap.update(&row_str(field, "b"), 0, Some(field), true);

    // At cap=2 now. Insert "c" — must be dropped.
    wrap.update(&row_str(field, "c"), 0, Some(field), true);

    // Entropy of two equally-observed categories is 1.0 bit.
    // If "c" were accepted it would change the entropy.
    let v = wrap.query();
    let entropy = match v {
        Value::F64(f) => f,
        other => panic!("expected F64, got {other:?}"),
    };
    assert!(
        (entropy - 1.0).abs() < 1e-9,
        "expected entropy=1.0 bits (2 equal cats), got {entropy}"
    );
}

// ── Test 3: existing category at cap still accumulates ───────────────────────

/// When at cap, inserting an ALREADY-TRACKED category must still increment
/// that category's count (only new keys are dropped).
///
/// RED: No drop-new guard today.
#[test]
fn entropy_wrap_existing_category_accumulates_at_cap() {
    let mut wrap = EntropyStateWrap::new(2);
    let field = "cat";

    // "a" × 3, "b" × 1 — then cap is reached after "b".
    for _ in 0..3 {
        wrap.update(&row_str(field, "a"), 0, Some(field), true);
    }
    wrap.update(&row_str(field, "b"), 0, Some(field), true);
    // At cap=2. Now insert "a" again — must still count.
    wrap.update(&row_str(field, "a"), 0, Some(field), true);
    // Try "c" — must be dropped.
    wrap.update(&row_str(field, "c"), 0, Some(field), true);

    // a:4, b:1 total=5 → H = -(4/5)*log2(4/5) - (1/5)*log2(1/5)
    let expected_h = -(4.0_f64 / 5.0) * (4.0_f64 / 5.0_f64).log2()
        - (1.0_f64 / 5.0) * (1.0_f64 / 5.0_f64).log2();
    let v = wrap.query();
    let entropy = match v {
        Value::F64(f) => f,
        other => panic!("expected F64, got {other:?}"),
    };
    assert!(
        (entropy - expected_h).abs() < 1e-9,
        "expected entropy={expected_h:.6}, got {entropy:.6}"
    );
}

// ── Test 4: global cap-hit counter increments on each drop ───────────────────

/// `EntropyStateWrap::categories_capped_count()` returns a process-global
/// AtomicU64 that increments each time a new category is silently dropped due
/// to the cap. This counter is used to populate the Prometheus
/// `beava_entropy_categories_capped_total` metric.
///
/// RED: The counter does not exist today.
#[test]
fn entropy_wrap_cap_hit_counter_increments() {
    let before = EntropyStateWrap::categories_capped_count();

    let mut wrap = EntropyStateWrap::new(1);
    let field = "cat";

    // First insert accepted (cap=1, 0 → 1 category).
    wrap.update(&row_str(field, "x"), 0, Some(field), true);
    // These three are new categories beyond cap → each increments counter.
    wrap.update(&row_str(field, "y"), 0, Some(field), true);
    wrap.update(&row_str(field, "z"), 0, Some(field), true);
    wrap.update(&row_str(field, "w"), 0, Some(field), true);

    let after = EntropyStateWrap::categories_capped_count();
    // The global counter is process-wide; parallel tests may add their own drops.
    // Assert at least 3 increments came from this test's 3 dropped categories.
    assert!(
        after >= before + 3,
        "expected at least 3 cap-hit counter increments from this test, got {}",
        after - before
    );
}
