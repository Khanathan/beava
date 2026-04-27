/// Integration tests for UDDSketch's flat sorted Vec storage (Plan 19.2-04).
///
/// These tests verify:
/// - Internal storage is Vec<(i32, u64)> (not BTreeMap)
/// - Buckets remain sorted after inserts
/// - Decrement removes zero-count buckets
/// - Quantile accuracy is preserved under the storage swap
/// - Collapse works correctly under bucket pressure
///
/// RED commit: pos_buckets_for_test() accessor doesn't exist on BTreeMap-backed
/// UDDSketch — these tests must fail to compile before the Vec migration lands.
use beava_core::sketches::uddsketch::UDDSketch;
use proptest::prelude::*;

/// Test 1: Internal storage returns a &[(i32, u64)] slice — proves Vec storage.
#[test]
fn test_uddsketch_flat_vec_buckets() {
    let mut sketch = UDDSketch::default();
    for i in 1..=100 {
        sketch.insert(i as f64);
    }
    // This accessor returns &[(i32, u64)]. The BTreeMap-backed version has no
    // such method, so this test is RED until the Vec migration lands.
    let buckets: &[(i32, u64)] = sketch.pos_buckets_for_test();
    assert!(!buckets.is_empty(), "should have positive buckets after inserting [1..100]");
    // Confirm every element is (i32, u64) — the type constraint is the test.
    for &(key, count) in buckets {
        assert!(count > 0, "every stored bucket should have count > 0; got key={key} count={count}");
        let _ = key; // i32 key is a log-scale bucket index
    }
}

/// Test 2: pos_buckets_for_test() is monotonically sorted by i32 key.
#[test]
fn test_uddsketch_buckets_remain_sorted_after_inserts() {
    let mut sketch = UDDSketch::default();
    // Insert 1000 values spread across several log-scale ranges.
    for i in 1u64..=1000 {
        sketch.insert(i as f64);
    }
    let buckets = sketch.pos_buckets_for_test();
    assert!(!buckets.is_empty());
    // Verify strict ascending order on the i32 key.
    for window in buckets.windows(2) {
        let (k0, _) = window[0];
        let (k1, _) = window[1];
        assert!(
            k0 < k1,
            "buckets must be monotonically sorted: found key {k0} >= {k1} in adjacent entries"
        );
    }
}

/// Test 3: Decrement of the only occurrence removes the bucket; total_count = 0.
#[test]
fn test_uddsketch_decrement_removes_bucket_at_zero_count() {
    let mut sketch = UDDSketch::default();
    sketch.insert(5.0);
    assert_eq!(sketch.total_count(), 1);
    sketch.decrement(5.0);
    assert_eq!(sketch.total_count(), 0, "total_count should be 0 after decrementing the only element");
    assert!(
        sketch.pos_buckets_for_test().is_empty(),
        "pos_buckets should be empty after the only element is decremented to zero"
    );
}

/// Test 4: Quantile accuracy is preserved (deterministic uniform distribution).
///
/// Inserts 10,000 uniform values in [1, 1000]. Checks that q=0.5/0.95/0.99
/// are within 2% relative error of the true quantiles.
#[test]
fn test_uddsketch_quantile_accuracy_preserved() {
    let mut sketch = UDDSketch::default();
    let n = 10_000u64;
    // Uniform [1..1000] via simple step: values 1..=10000 scaled to [0.1..1000].
    for i in 1..=n {
        sketch.insert(i as f64 / 10.0); // values in [0.1, 1000.0]
    }
    assert_eq!(sketch.total_count(), n);

    // True quantiles for uniform [0.1, 1000.0]:
    //   q = 0.50 → ~500.0
    //   q = 0.95 → ~950.0
    //   q = 0.99 → ~990.0
    let cases = [(0.5_f64, 500.0_f64), (0.95, 950.0), (0.99, 990.0)];
    for (q, true_val) in cases {
        let est = sketch.quantile(q).expect("non-empty sketch should return Some");
        let rel_err = (est - true_val).abs() / true_val;
        assert!(
            rel_err <= 0.02,
            "q={q}: estimate={est:.3} true={true_val:.3} rel_err={rel_err:.4} > 0.02"
        );
    }
}

/// Test 5 (proptest): For any sequence of 100–1000 finite positive f64 values,
/// query_quantile(0.5) is within α=0.02 of the true median.
///
/// Uses proptest with ~100 cases. The accuracy bound is 2×α₀ = 0.02 (worst-case
/// after potential collapse rounds).
proptest! {
    #[test]
    fn test_uddsketch_quantile_alpha_bound_proptest(
        // Generate a Vec of finite positive f64 values in the range (0, 1e6].
        values in prop::collection::vec(
            (1e-3f64..1e6f64),
            100..=500,
        )
    ) {
        let mut sketch = UDDSketch::default();
        let mut sorted = values.clone();
        for &v in &values {
            sketch.insert(v);
        }
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let n = sorted.len();
        let true_median = {
            if n % 2 == 1 {
                sorted[n / 2]
            } else {
                (sorted[n / 2 - 1] + sorted[n / 2]) / 2.0
            }
        };
        if true_median == 0.0 {
            return Ok(()); // skip degenerate
        }
        if let Some(est) = sketch.quantile(0.5) {
            let rel_err = (est - true_median).abs() / true_median;
            prop_assert!(
                rel_err <= 0.02,
                "median estimate {est:.3} vs true {true_median:.3}: rel_err={rel_err:.4} > 0.02"
            );
        }
    }
}

/// Test 6: Collapse fires when bucket count exceeds max_buckets.
///
/// Inserts 3000 distinct-bucket values into a default (max_buckets=2048) sketch.
/// After insertion: current_alpha() > alpha0() (collapse happened) AND
/// pos_buckets_for_test().len() <= max_buckets.
#[test]
fn test_uddsketch_collapse_at_cap() {
    let mut sketch = UDDSketch::default(); // max_buckets = 2048
    let alpha0 = sketch.alpha0();

    // Insert values spanning many distinct log-scale buckets.
    // Values 1.0 * (1.003)^i for i in 0..3000 span ~3000 distinct buckets
    // before collapse.
    for i in 0..3000i32 {
        let v = (1.003_f64).powi(i);
        sketch.insert(v);
    }

    assert!(
        sketch.current_alpha() > alpha0,
        "current_alpha ({}) should be > alpha0 ({}) after collapse",
        sketch.current_alpha(),
        alpha0
    );
    let bucket_count = sketch.pos_buckets_for_test().len() + sketch.neg_buckets_for_test().len();
    assert!(
        bucket_count <= 2048,
        "total bucket count {} should be <= max_buckets=2048",
        bucket_count
    );
}
