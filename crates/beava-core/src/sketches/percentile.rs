//! PercentileState: 2-mode hybrid (Exact Vec<f64> ≤256 → UDDSketch).
//! Serde rename tags `v0_percentile_exact` / `v0_percentile_uddsketch`
//! for snapshot stability across versions.

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn starts_in_exact_mode() {
        let s = PercentileState::new(256, 0.01);
        assert_eq!(s.mode_name(), "v0_percentile_exact");
        assert!(s.quantile(0.5).is_none());
    }
    #[test]
    fn exact_mode_quantile_is_exact() {
        let mut s = PercentileState::new(256, 0.01);
        for v in 1..=100 {
            s.insert(v as f64);
        }
        let p50 = s.quantile(0.5).unwrap();
        assert!((p50 - 50.5).abs() < 1.0, "p50={}", p50);
    }
    #[test]
    fn promotes_to_sketch_at_threshold() {
        let mut s = PercentileState::new(256, 0.01);
        for i in 1..=300 {
            s.insert(i as f64);
        }
        assert_eq!(s.mode_name(), "v0_percentile_uddsketch");
        let p99 = s.quantile(0.99).unwrap();
        let err = (p99 - 297.0).abs() / 297.0;
        assert!(err < 0.05, "p99={} err={}", p99, err);
    }
    #[test]
    fn promotion_preserves_quantile_close_to_exact() {
        let mut e = PercentileState::new(256, 0.01);
        for v in 1..=200 {
            e.insert(v as f64);
        }
        let p50_exact = e.quantile(0.5).unwrap();
        for v in 201..=300 {
            e.insert(v as f64);
        }
        assert_eq!(e.mode_name(), "v0_percentile_uddsketch");
        let p50_after = e.quantile(0.5).unwrap();
        assert!((p50_after - 150.0).abs() / 150.0 < 0.05);
        assert!((p50_exact - 100.5).abs() < 1.0);
    }
    #[test]
    fn bincode_round_trip_exact() {
        let mut s = PercentileState::new(256, 0.01);
        for v in 1..=50 {
            s.insert(v as f64);
        }
        let bytes = bincode::serialize(&s).unwrap();
        let s2: PercentileState = bincode::deserialize(&bytes).unwrap();
        assert_eq!(s2.mode_name(), "v0_percentile_exact");
        assert!((s.quantile(0.5).unwrap() - s2.quantile(0.5).unwrap()).abs() < 1e-9);
    }
    #[test]
    fn bincode_round_trip_sketch() {
        let mut s = PercentileState::new(256, 0.01);
        for v in 1..=1_000 {
            s.insert(v as f64);
        }
        assert_eq!(s.mode_name(), "v0_percentile_uddsketch");
        let bytes = bincode::serialize(&s).unwrap();
        let s2: PercentileState = bincode::deserialize(&bytes).unwrap();
        assert_eq!(s2.mode_name(), "v0_percentile_uddsketch");
        assert!((s.quantile(0.5).unwrap() - s2.quantile(0.5).unwrap()).abs() < 1e-9);
    }
    #[test]
    fn serde_tag_in_json() {
        let mut s = PercentileState::new(256, 0.01);
        s.insert(1.0);
        let j = serde_json::to_string(&s).unwrap();
        assert!(j.contains("v0_percentile_exact"));
    }
}
