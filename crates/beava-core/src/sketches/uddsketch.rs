//! UDDSketch: Uniform Dyadic Distribution Sketch with retraction (decrement).
//! Ported from main:src/engine/uddsketch.rs (Apache 2.0).

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn empty_quantile_returns_none() {
        let s = UDDSketch::default();
        assert!(s.quantile(0.5).is_none());
        assert_eq!(s.total_count(), 0);
    }
    #[test]
    fn uniform_p50_within_2pct() {
        let mut s = UDDSketch::default();
        for i in 1..=10_000 {
            s.insert(i as f64);
        }
        let p50 = s.quantile(0.5).unwrap();
        let err = (p50 - 5_000.0).abs() / 5_000.0;
        assert!(err < 0.02, "p50={} err={}", p50, err);
    }
    #[test]
    fn uniform_p99_within_2pct() {
        let mut s = UDDSketch::default();
        for i in 1..=10_000 {
            s.insert(i as f64);
        }
        let p99 = s.quantile(0.99).unwrap();
        let err = (p99 - 9_900.0).abs() / 9_900.0;
        assert!(err < 0.02, "p99={} err={}", p99, err);
    }
    #[test]
    fn pareto_p99_within_10pct() {
        let mut s = UDDSketch::default();
        let xm = 1.0;
        let alpha = 1.5;
        for i in 0..10_000 {
            let u = (i as f64 + 0.5) / 10_000.0;
            let x = xm / (1.0 - u).powf(1.0 / alpha);
            s.insert(x);
        }
        let true_p99 = 1.0 / 0.01_f64.powf(1.0 / 1.5);
        let p99 = s.quantile(0.99).unwrap();
        let err = (p99 - true_p99).abs() / true_p99;
        assert!(err < 0.10, "p99={} true={} err={}", p99, true_p99, err);
    }
    #[test]
    fn decrement_drops_total_and_buckets() {
        let mut s = UDDSketch::default();
        for v in &[1.0_f64, 2.0, 3.0, 4.0, 5.0] {
            s.insert(*v);
        }
        assert_eq!(s.total_count(), 5);
        s.decrement(1.0);
        s.decrement(5.0);
        assert_eq!(s.total_count(), 3);
        let p50 = s.quantile(0.5).unwrap();
        assert!((p50 - 3.0).abs() / 3.0 < 0.05);
    }
    #[test]
    fn merge_combines_distributions() {
        let mut a = UDDSketch::default();
        let mut b = UDDSketch::default();
        for i in 1..=5_000 {
            a.insert(i as f64);
        }
        for i in 5_001..=10_000 {
            b.insert(i as f64);
        }
        a.merge(&b);
        assert_eq!(a.total_count(), 10_000);
        let p50 = a.quantile(0.5).unwrap();
        let err = (p50 - 5_000.0).abs() / 5_000.0;
        assert!(err < 0.02);
    }
    #[test]
    fn bincode_round_trip() {
        let mut s = UDDSketch::default();
        for i in 1..=1_000 {
            s.insert(i as f64);
        }
        let bytes = bincode::serialize(&s).unwrap();
        let s2: UDDSketch = bincode::deserialize(&bytes).unwrap();
        assert_eq!(s2.total_count(), s.total_count());
        let p50a = s.quantile(0.5).unwrap();
        let p50b = s2.quantile(0.5).unwrap();
        assert!((p50a - p50b).abs() < 1e-9);
    }
    #[test]
    fn alpha_collapses_under_pressure() {
        let mut s = UDDSketch::new(0.01, 64);
        for i in 0..2_000 {
            let v = (1.006_f64).powi(i - 1000);
            s.insert(v);
        }
        assert!(s.current_alpha() > 0.01, "alpha should have grown via collapse");
    }
}
