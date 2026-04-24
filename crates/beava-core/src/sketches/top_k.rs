//! TopKState: 2-mode hybrid (BTreeMap exact ≤1024 distinct → CMS+TopKHeap).

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sketches::cms::TopKValue;
    #[test]
    fn starts_in_exact_mode() {
        let s = TopKState::new(3, 1024, 2048, 4);
        assert_eq!(s.mode_name(), "v0_top_k_exact");
        assert_eq!(s.top(), Vec::<(TopKValue, u64)>::new());
    }
    #[test]
    fn exact_mode_returns_top_k() {
        let mut s = TopKState::new(3, 1024, 2048, 4);
        for _ in 0..100 {
            s.insert(TopKValue::Str("a".into()));
        }
        for _ in 0..50 {
            s.insert(TopKValue::Str("b".into()));
        }
        for _ in 0..30 {
            s.insert(TopKValue::Str("c".into()));
        }
        for _ in 0..10 {
            s.insert(TopKValue::Str("d".into()));
        }
        let top = s.top();
        assert_eq!(top.len(), 3);
        assert_eq!(top[0].0, TopKValue::Str("a".into()));
        assert_eq!(top[0].1, 100);
        assert_eq!(top[1], (TopKValue::Str("b".into()), 50));
        assert_eq!(top[2], (TopKValue::Str("c".into()), 30));
    }
    #[test]
    fn promotes_to_hybrid_at_1024_distinct() {
        let mut s = TopKState::new(5, 1024, 2048, 4);
        for i in 0..1100 {
            s.insert(TopKValue::Str(format!("k{}", i)));
        }
        assert_eq!(s.mode_name(), "v0_top_k_hybrid");
    }
    #[test]
    fn heavy_hitters_correct_in_hybrid_mode() {
        let mut s = TopKState::new(3, 100, 2048, 4);
        for _ in 0..5000 {
            s.insert(TopKValue::Str("dominant".into()));
        }
        for j in 0..9 {
            for _ in 0..500 {
                s.insert(TopKValue::Str(format!("mid{}", j)));
            }
        }
        for i in 0..1000 {
            s.insert(TopKValue::Str(format!("noise{}", i)));
        }
        assert_eq!(s.mode_name(), "v0_top_k_hybrid");
        let top = s.top();
        assert_eq!(top.len(), 3);
        assert_eq!(top[0].0, TopKValue::Str("dominant".into()));
        assert!(top[0].1 >= 5000);
        for (v, _) in &top[1..] {
            if let TopKValue::Str(s) = v {
                assert!(s.starts_with("mid"), "got {}", s);
            }
        }
    }
    #[test]
    fn promotion_preserves_dominant_key() {
        let mut s = TopKState::new(3, 100, 2048, 4);
        for _ in 0..1000 {
            s.insert(TopKValue::Str("dominant".into()));
        }
        for i in 0..200 {
            s.insert(TopKValue::Str(format!("k{}", i)));
        }
        assert_eq!(s.mode_name(), "v0_top_k_hybrid");
        let top = s.top();
        assert_eq!(top[0].0, TopKValue::Str("dominant".into()));
    }
    #[test]
    fn bincode_round_trip_exact() {
        let mut s = TopKState::new(3, 1024, 2048, 4);
        for _ in 0..10 {
            s.insert(TopKValue::Str("a".into()));
        }
        let bytes = bincode::serialize(&s).unwrap();
        let s2: TopKState = bincode::deserialize(&bytes).unwrap();
        assert_eq!(s2.mode_name(), "v0_top_k_exact");
        assert_eq!(s2.top(), s.top());
    }
    #[test]
    fn bincode_round_trip_hybrid() {
        let mut s = TopKState::new(3, 50, 2048, 4);
        for i in 0..100 {
            s.insert(TopKValue::Str(format!("k{}", i)));
        }
        for _ in 0..1000 {
            s.insert(TopKValue::Str("k0".into()));
        }
        assert_eq!(s.mode_name(), "v0_top_k_hybrid");
        let bytes = bincode::serialize(&s).unwrap();
        let s2: TopKState = bincode::deserialize(&bytes).unwrap();
        assert_eq!(s2.mode_name(), "v0_top_k_hybrid");
        assert_eq!(s2.top()[0].0, s.top()[0].0);
    }
    #[test]
    fn serde_tag_in_json() {
        let s = TopKState::new(3, 1024, 2048, 4);
        let j = serde_json::to_string(&s).unwrap();
        assert!(j.contains("v0_top_k_exact"));
    }
}
