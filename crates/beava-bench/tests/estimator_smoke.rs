//! Phase 13.5 Plan 10: memory estimator regression tests.

use beava_bench::cli::estimator;

#[test]
fn test_fraud_medium_estimate() {
    // fraud workload (Plan 09 — fraud-team config) + size=medium (~100K
    // entities) should predict somewhere in 200-2000 MB. Loose bounds since
    // the estimator is a back-of-envelope tool.
    let est =
        estimator::estimate_memory("fraud", "medium").expect("estimator must support fraud/medium");
    let predicted_mb = est.expected_rss_bytes / (1024 * 1024);
    assert!(
        predicted_mb >= 50,
        "fraud/medium predicted only {} MB; expected ≥ 50 MB",
        predicted_mb
    );
    assert!(
        est.bytes_per_entity > 0,
        "must have a non-zero per-entity estimate"
    );
    assert!(
        est.entity_count_estimate >= 50_000,
        "medium size = ~100K entities; got {}",
        est.entity_count_estimate
    );
}

#[test]
fn test_adtech_small_estimate() {
    let est =
        estimator::estimate_memory("adtech", "small").expect("estimator must support adtech/small");
    let predicted_mb = est.expected_rss_bytes / (1024 * 1024);
    assert!(
        predicted_mb < 500,
        "adtech/small predicted {predicted_mb} MB; ≤ 500 MB sanity"
    );
}

#[test]
fn test_ecommerce_large_estimate() {
    let est = estimator::estimate_memory("ecommerce", "large")
        .expect("estimator must support ecommerce/large");
    let predicted_mb = est.expected_rss_bytes / (1024 * 1024);
    assert!(
        predicted_mb >= 100,
        "large size should predict ≥ 100 MB; got {predicted_mb}"
    );
}

#[test]
fn test_unknown_workload_errors() {
    assert!(estimator::estimate_memory("nonexistent_xyz", "small").is_err());
}

#[test]
fn test_unknown_size_errors() {
    assert!(estimator::estimate_memory("fraud", "huge_xyz").is_err());
}

#[test]
fn test_estimate_includes_per_derivation_breakdown() {
    let est = estimator::estimate_memory("fraud", "medium").unwrap();
    assert!(
        !est.per_derivation_breakdown.is_empty(),
        "must include per-derivation cost breakdown"
    );
}
