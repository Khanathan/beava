//! Phase 13.5 Plan 09 smoke tests: dataset workloads register + generate events.

use beava_bench::workloads;

#[test]
fn test_adtech_registers() {
    let w = workloads::load_by_name("adtech").expect("adtech workload must be loadable");
    assert_eq!(w.name, "adtech");
    assert!(
        !w.derivations.is_empty(),
        "adtech must have ≥ 1 derivation; got {}",
        w.derivations.len()
    );
    let events: Vec<_> = (w.event_generator)(100).collect();
    assert!(
        events.len() >= 100,
        "event_generator(100) must produce ≥ 100 events"
    );
}

#[test]
fn test_fraud_registers() {
    let w = workloads::load_by_name("fraud").expect("fraud workload must be loadable");
    assert!(
        w.derivations.len() >= 5,
        "fraud (fraud-team config) has ≥ 5 derivations; got {}",
        w.derivations.len()
    );
    let events: Vec<_> = (w.event_generator)(100).collect();
    assert!(events.len() >= 100);
}

#[test]
fn test_ecommerce_registers() {
    let w = workloads::load_by_name("ecommerce").expect("ecommerce workload must be loadable");
    assert!(!w.derivations.is_empty());
    let events: Vec<_> = (w.event_generator)(100).collect();
    assert!(events.len() >= 100);
}

#[test]
fn test_unknown_workload_errors() {
    assert!(workloads::load_by_name("nonexistent_xyz").is_err());
}

#[test]
fn test_legacy_size_workloads_still_load() {
    for name in ["small", "medium", "large"] {
        workloads::load_by_name(name)
            .unwrap_or_else(|_| panic!("{name} workload must remain loadable"));
    }
}

#[test]
fn test_workload_exercises_diverse_op_families() {
    let f = workloads::load_by_name("fraud").unwrap();
    let op_names: std::collections::HashSet<String> = f
        .derivations
        .iter()
        .flat_map(|d| d.op_kinds().map(String::from))
        .collect();
    let families = ["geo", "sketch", "recency", "decay", "core"];
    for fam in families {
        let has_family_op = op_names.iter().any(|op| matches_family(op, fam));
        assert!(
            has_family_op,
            "fraud workload missing {fam} family op; got ops: {op_names:?}"
        );
    }
}

fn matches_family(op: &str, fam: &str) -> bool {
    let mapping: &[(&str, &[&str])] = &[
        (
            "core",
            &["count", "sum", "mean", "min", "max", "var", "std", "ratio"],
        ),
        (
            "sketch",
            &["n_unique", "quantile", "top_k", "entropy", "bloom_member"],
        ),
        (
            "recency",
            &[
                "first_seen",
                "last_seen",
                "age",
                "streak",
                "time_since",
                "first_seen_in_window",
            ],
        ),
        (
            "decay",
            &[
                "ewma",
                "ewvar",
                "ew_zscore",
                "decayed_sum",
                "decayed_count",
                "twa",
            ],
        ),
        (
            "geo",
            &[
                "geo_velocity",
                "geo_distance",
                "geo_spread",
                "distance_from_home",
            ],
        ),
    ];
    mapping
        .iter()
        .find(|(f, _)| *f == fam)
        .map(|(_, ops)| ops.contains(&op))
        .unwrap_or(false)
}
