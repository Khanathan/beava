//! Phase 25-02 — SignalRegistry unit tests.
//!
//! Covers the core observability-bus contract from `25-02-PLAN.md`:
//!
//! * record → new signal stored with given fields
//! * dedupe-by-id preserves `first_seen`, refreshes `last_seen`, overwrites
//!   title/detail/action/evidence, and allows severity escalation only
//! * age_out drops signals older than the observation window
//! * snapshot_sorted returns severity-desc, first_seen-asc ordering
//! * category filter narrows the returned vec
//! * rate_since_last bootstraps on first call, then returns per-sec rate

use std::time::{Duration, SystemTime};

use tally::server::signals::{
    Category, Severity, Signal, SignalRegistry, DEFAULT_OBSERVATION_WINDOW,
};

fn mk_signal(id: &str, sev: Severity, cat: Category, first_seen: SystemTime) -> Signal {
    let mut s = Signal::new(
        id,
        sev,
        cat,
        "title",
        "detail",
        serde_json::json!({"k": "v"}),
    );
    s.first_seen = first_seen;
    s.last_seen = first_seen;
    s
}

#[test]
fn test_record_new_signal() {
    let mut r = SignalRegistry::new_default();
    let t = SystemTime::now();
    r.record(mk_signal("a", Severity::Warning, Category::Safety, t));
    assert_eq!(r.len(), 1);
    let snap = r.snapshot_sorted(t, None);
    assert_eq!(snap.len(), 1);
    assert_eq!(snap[0].id, "a");
    assert_eq!(snap[0].severity, Severity::Warning);
    assert_eq!(snap[0].category, Category::Safety);
}

#[test]
fn test_dedupe_preserves_first_seen() {
    let mut r = SignalRegistry::new_default();
    let t0 = SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let t1 = t0 + Duration::from_secs(60);
    r.record(mk_signal("a", Severity::Warning, Category::Safety, t0));
    r.record(mk_signal("a", Severity::Warning, Category::Safety, t1));
    let snap = r.snapshot_sorted(t1, None);
    assert_eq!(snap.len(), 1);
    assert_eq!(snap[0].first_seen, t0, "first_seen must be preserved");
}

#[test]
fn test_dedupe_advances_last_seen() {
    let mut r = SignalRegistry::new_default();
    let t0 = SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let t1 = t0 + Duration::from_secs(120);
    r.record(mk_signal("a", Severity::Warning, Category::Safety, t0));
    r.record(mk_signal("a", Severity::Warning, Category::Safety, t1));
    let snap = r.snapshot_sorted(t1, None);
    assert_eq!(snap[0].last_seen, t1);
}

#[test]
fn test_dedupe_overwrites_evidence() {
    let mut r = SignalRegistry::new_default();
    let t = SystemTime::now();
    let mut s1 = mk_signal("a", Severity::Warning, Category::Safety, t);
    s1.detail = "old detail".into();
    s1.evidence = serde_json::json!({"v": 1});
    r.record(s1);

    let mut s2 = mk_signal("a", Severity::Warning, Category::Safety, t);
    s2.detail = "new detail".into();
    s2.evidence = serde_json::json!({"v": 2});
    r.record(s2);

    let snap = r.snapshot_sorted(t, None);
    assert_eq!(snap.len(), 1);
    assert_eq!(snap[0].detail, "new detail");
    assert_eq!(snap[0].evidence["v"], 2);
}

#[test]
fn test_age_out_drops_stale() {
    let mut r = SignalRegistry::new(Duration::from_secs(60));
    let old = SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let now = old + Duration::from_secs(120);
    r.record(mk_signal("old", Severity::Warning, Category::Safety, old));
    r.age_out(now);
    assert_eq!(r.len(), 0);
}

#[test]
fn test_age_out_keeps_fresh() {
    let mut r = SignalRegistry::new(Duration::from_secs(60));
    let t = SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let now = t + Duration::from_secs(30);
    r.record(mk_signal("fresh", Severity::Warning, Category::Safety, t));
    r.age_out(now);
    assert_eq!(r.len(), 1);
}

#[test]
fn test_sort_by_severity_critical_first() {
    let mut r = SignalRegistry::new_default();
    let t = SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    r.record(mk_signal("info", Severity::Info, Category::Config, t));
    r.record(mk_signal("warn", Severity::Warning, Category::Config, t));
    r.record(mk_signal("err", Severity::Error, Category::Config, t));
    r.record(mk_signal("crit", Severity::Critical, Category::Config, t));
    let snap = r.snapshot_sorted(t, None);
    let ids: Vec<&str> = snap.iter().map(|s| s.id.as_str()).collect();
    assert_eq!(ids, vec!["crit", "err", "warn", "info"]);
}

#[test]
fn test_sort_stable_by_first_seen_within_severity() {
    let mut r = SignalRegistry::new_default();
    let base = SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    r.record(mk_signal(
        "b",
        Severity::Warning,
        Category::Safety,
        base + Duration::from_secs(20),
    ));
    r.record(mk_signal(
        "a",
        Severity::Warning,
        Category::Safety,
        base + Duration::from_secs(10),
    ));
    r.record(mk_signal(
        "c",
        Severity::Warning,
        Category::Safety,
        base + Duration::from_secs(30),
    ));
    let snap = r.snapshot_sorted(base + Duration::from_secs(40), None);
    let ids: Vec<&str> = snap.iter().map(|s| s.id.as_str()).collect();
    assert_eq!(ids, vec!["a", "b", "c"]);
}

#[test]
fn test_filter_by_category() {
    let mut r = SignalRegistry::new_default();
    let t = SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    r.record(mk_signal("s1", Severity::Warning, Category::Safety, t));
    r.record(mk_signal(
        "d1",
        Severity::Warning,
        Category::DataQuality,
        t,
    ));
    r.record(mk_signal(
        "o1",
        Severity::Warning,
        Category::Operational,
        t,
    ));
    let only_safety = r.snapshot_sorted(t, Some(Category::Safety));
    assert_eq!(only_safety.len(), 1);
    assert_eq!(only_safety[0].id, "s1");
    let only_dq = r.snapshot_sorted(t, Some(Category::DataQuality));
    assert_eq!(only_dq.len(), 1);
    assert_eq!(only_dq[0].id, "d1");
}

#[test]
fn test_empty_registry_returns_empty_vec() {
    let r = SignalRegistry::new_default();
    let t = SystemTime::now();
    assert!(r.is_empty());
    assert!(r.snapshot_sorted(t, None).is_empty());
    assert!(r.snapshot_sorted(t, Some(Category::Config)).is_empty());
}

#[test]
fn test_severity_escalation_on_redup() {
    // Second record with higher severity should win.
    let mut r = SignalRegistry::new_default();
    let t = SystemTime::now();
    r.record(mk_signal("a", Severity::Info, Category::Safety, t));
    r.record(mk_signal("a", Severity::Critical, Category::Safety, t));
    let snap = r.snapshot_sorted(t, None);
    assert_eq!(snap[0].severity, Severity::Critical);
}

#[test]
fn test_severity_no_silent_downgrade() {
    // Second record with lower severity should NOT downgrade the stored
    // signal — it stays at the escalated level.
    let mut r = SignalRegistry::new_default();
    let t = SystemTime::now();
    r.record(mk_signal("a", Severity::Error, Category::Safety, t));
    r.record(mk_signal("a", Severity::Info, Category::Safety, t));
    let snap = r.snapshot_sorted(t, None);
    assert_eq!(snap[0].severity, Severity::Error);
}

#[test]
fn test_categories_present_reports_all_wired_categories() {
    let mut r = SignalRegistry::new_default();
    let t = SystemTime::now();
    r.record(mk_signal("c", Severity::Info, Category::Config, t));
    r.record(mk_signal("d", Severity::Info, Category::DataQuality, t));
    r.record(mk_signal("o", Severity::Info, Category::Operational, t));
    r.record(mk_signal("s", Severity::Info, Category::Safety, t));
    r.record(mk_signal("p", Severity::Info, Category::Performance, t));
    let cats = r.categories_present();
    assert_eq!(cats.len(), 5);
    for c in [
        Category::Config,
        Category::DataQuality,
        Category::Operational,
        Category::Safety,
        Category::Performance,
    ] {
        assert!(cats.contains(&c), "missing category {:?}", c);
    }
}

#[test]
fn test_rate_since_last_bootstraps_first_call() {
    let mut r = SignalRegistry::new_default();
    let t0 = SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    // First sample: no prior → None.
    assert_eq!(r.rate_since_last("late_drop.s", 0, t0), None);
}

#[test]
fn test_rate_since_last_computes_per_second_rate() {
    let mut r = SignalRegistry::new_default();
    let t0 = SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let t1 = t0 + Duration::from_secs(10);
    assert_eq!(r.rate_since_last("late_drop.s", 100, t0), None);
    let rate = r.rate_since_last("late_drop.s", 200, t1).unwrap();
    // 100 drops over 10s = 10/s.
    assert!(
        (rate - 10.0).abs() < 1e-9,
        "expected 10.0, got {}",
        rate
    );
}

#[test]
fn test_rate_since_last_handles_counter_reset() {
    let mut r = SignalRegistry::new_default();
    let t0 = SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let t1 = t0 + Duration::from_secs(10);
    r.rate_since_last("x", 500, t0);
    // Counter went backwards → None (treat as reset, skip emission).
    assert_eq!(r.rate_since_last("x", 100, t1), None);
}

#[test]
fn test_default_observation_window_is_7_days() {
    assert_eq!(
        DEFAULT_OBSERVATION_WINDOW,
        Duration::from_secs(7 * 86400)
    );
    let r = SignalRegistry::new_default();
    assert_eq!(r.observation_window(), Duration::from_secs(7 * 86400));
}

#[test]
fn test_record_no_io() {
    // record() must perform no disk I/O — fundamental to prevent
    // snapshot-failure recursion. We can't "detect" I/O directly, but we
    // can assert the function completes in well under the time any disk
    // write would take even on a ramdisk.
    let mut r = SignalRegistry::new_default();
    let start = std::time::Instant::now();
    for i in 0..10_000 {
        r.record(Signal::new(
            format!("id.{}", i),
            Severity::Info,
            Category::Operational,
            "t",
            "d",
            serde_json::json!({}),
        ));
    }
    let elapsed = start.elapsed();
    assert!(
        elapsed < Duration::from_secs(1),
        "10k records took {:?} — suggests I/O in hot path",
        elapsed
    );
    assert_eq!(r.len(), 10_000);
}

#[test]
fn test_category_parse_roundtrip() {
    for (s, c) in [
        ("config", Category::Config),
        ("data_quality", Category::DataQuality),
        ("operational", Category::Operational),
        ("safety", Category::Safety),
        ("performance", Category::Performance),
    ] {
        assert_eq!(Category::parse(s), Some(c));
    }
    assert_eq!(Category::parse("bogus"), None);
}
