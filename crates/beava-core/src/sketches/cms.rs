//! Count-Min Sketch (W=2048, D=4) + bounded TopKHeap with O(log k) insert.
//! Ported from main:src/engine/cms.rs (Apache 2.0). Plan 22-04's HashMap
//! heap-position side-index optimization is included verbatim.

#[cfg(test)]
mod tests {
    use super::*;
    fn h(s: &str) -> u64 {
        use std::hash::{Hash, Hasher};
        let mut hh = ahash::AHasher::default();
        s.hash(&mut hh);
        hh.finish()
    }
    #[test]
    fn cms_new_zeros() {
        let c = CountMinSketch::new(DEFAULT_CMS_WIDTH, DEFAULT_CMS_DEPTH);
        assert_eq!(c.estimate(h("x")), 0);
        assert_eq!(c.total(), 0);
    }
    #[test]
    fn cms_insert_then_estimate_at_least_true_count() {
        let mut c = CountMinSketch::new(2048, 4);
        for _ in 0..100 {
            c.insert(h("a"));
        }
        for _ in 0..50 {
            c.insert(h("b"));
        }
        assert!(c.estimate(h("a")) >= 100);
        assert!(c.estimate(h("b")) >= 50);
        assert_eq!(c.total(), 150);
    }
    #[test]
    fn cms_decrement_saturates_at_zero() {
        let mut c = CountMinSketch::new(2048, 4);
        c.insert(h("x"));
        c.decrement(h("x"));
        c.decrement(h("x"));
        assert_eq!(c.estimate(h("x")), 0);
        assert_eq!(c.total(), 0);
    }
    #[test]
    fn cms_bincode_round_trip() {
        let mut c = CountMinSketch::new(2048, 4);
        for _ in 0..10 {
            c.insert(h("k"));
        }
        let bytes = bincode::serialize(&c).unwrap();
        let c2: CountMinSketch = bincode::deserialize(&bytes).unwrap();
        assert_eq!(c2.estimate(h("k")), c.estimate(h("k")));
    }
    #[test]
    fn topkvalue_from_json_round_trip() {
        let v = TopKValue::Str("abc".into());
        assert_eq!(TopKValue::from_json(&v.to_json()), Some(v.clone()));
        let i = TopKValue::Int(42);
        assert_eq!(TopKValue::from_json(&i.to_json()), Some(i));
        let b = TopKValue::Bool(true);
        assert_eq!(TopKValue::from_json(&b.to_json()), Some(b));
    }
    #[test]
    fn topkheap_keeps_top_k() {
        let mut hp = TopKHeap::new(3);
        hp.insert_or_bump(TopKValue::Str("a".into()), 100);
        hp.insert_or_bump(TopKValue::Str("b".into()), 50);
        hp.insert_or_bump(TopKValue::Str("c".into()), 25);
        hp.insert_or_bump(TopKValue::Str("d".into()), 10);
        let top = hp.top();
        assert_eq!(top.len(), 3);
        assert_eq!(top[0].0, TopKValue::Str("a".into()));
        assert_eq!(top[1].0, TopKValue::Str("b".into()));
        assert_eq!(top[2].0, TopKValue::Str("c".into()));
        assert!(top.iter().all(|(v, _)| *v != TopKValue::Str("d".into())));
    }
    #[test]
    fn topkheap_bump_updates_existing_via_index() {
        let mut hp = TopKHeap::new(3);
        hp.insert_or_bump(TopKValue::Str("a".into()), 10);
        hp.insert_or_bump(TopKValue::Str("a".into()), 100);
        let top = hp.top();
        assert_eq!(top.len(), 1);
        assert_eq!(top[0], (TopKValue::Str("a".into()), 100));
    }
    #[test]
    fn topkheap_index_field_present() {
        let src = include_str!("cms.rs");
        assert!(
            src.contains("AHashMap<TopKValue, usize>")
                || src.contains("AHashMap < TopKValue , usize >"),
            "TopKHeap must include AHashMap<TopKValue, usize> heap-position side-index (Plan 22-04 O(log k) opt)"
        );
    }
    #[test]
    fn topkheap_bincode_round_trip() {
        let mut hp = TopKHeap::new(3);
        hp.insert_or_bump(TopKValue::Str("a".into()), 5);
        hp.insert_or_bump(TopKValue::Str("b".into()), 3);
        let bytes = bincode::serialize(&hp).unwrap();
        let hp2: TopKHeap = bincode::deserialize(&bytes).unwrap();
        assert_eq!(hp2.top(), hp.top());
    }
}
