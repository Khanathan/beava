//! Count-Min Sketch + TopKHeap for frequency estimation and heavy-hitters.
//!
//! # CountMinSketch
//!
//! 2D i32 counter matrix `[D rows][W cols]`. Insert rotates the hash per row
//! and increments `counters[row][col]`. Estimate returns `min(counters[row][col])`
//! across rows. Supports `decrement` (negative delta) with per-cell saturation
//! at zero, enabling ring-buffer retraction.
//!
//! Defaults: W=2048, D=4 → 32KB per sketch. With 4 pairwise-independent hash
//! functions (seeded `const` MurmurHash3 finalizers), error bound:
//!   P(estimate - true > ε·N) ≤ δ   with  ε = e/W,  δ = e^-D
//! → ε ≈ 0.00133, δ ≈ 0.018.
//!
//! # TopKHeap
//!
//! Binary min-heap of `(estimated_count, Value)` bounded at k. On insert, if
//! the new value's CMS estimate exceeds the heap's smallest element (or the
//! heap is under capacity), push and trim. Maintains an `AHashSet<Value>` of
//! candidates (values ever considered for top-k) so that on bucket expiry we
//! can rebuild the heap by re-querying CMS estimates for each candidate.

use ordered_float::OrderedFloat;
use serde::{Deserialize, Serialize};
use std::cmp::Reverse;
use std::collections::BinaryHeap;

/// Default CMS width (columns per row).
pub const DEFAULT_CMS_WIDTH: usize = 2048;
/// Default CMS depth (number of rows / hash functions).
pub const DEFAULT_CMS_DEPTH: usize = 4;

/// Four pairwise-independent MurmurHash3 finalizer seeds. Deterministic so
/// collisions require offline discovery (out of v0 threat model).
const CMS_SEEDS: [u64; 8] = [
    0x9E3779B97F4A7C15,
    0xBF58476D1CE4E5B9,
    0x94D049BB133111EB,
    0xD1B54A32D192ED03,
    0xBEA225F9EB34556D,
    0xA24BAED4963EE407,
    0x85EBCA6B9FE1A285,
    0xC2B2AE3D27D4EB4F,
];

/// A canonical value type for Top-K bookkeeping. Strings, integers, floats,
/// bools all collapse to this enum so heap/set comparisons are total.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub enum TopKValue {
    Str(String),
    Int(i64),
    /// Float wrapped in OrderedFloat so Eq/Hash/Ord work.
    Float(OrderedFloat<f64>),
    Bool(bool),
}

impl TopKValue {
    /// Build from a serde_json::Value, returning `None` if the value is not a
    /// scalar we can index.
    pub fn from_json(value: &serde_json::Value) -> Option<Self> {
        match value {
            serde_json::Value::String(s) => Some(TopKValue::Str(s.clone())),
            serde_json::Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    Some(TopKValue::Int(i))
                } else {
                    n.as_f64().map(|f| TopKValue::Float(OrderedFloat(f)))
                }
            }
            serde_json::Value::Bool(b) => Some(TopKValue::Bool(*b)),
            _ => None,
        }
    }

    /// Emit as a JSON value.
    pub fn to_json(&self) -> serde_json::Value {
        match self {
            TopKValue::Str(s) => serde_json::Value::String(s.clone()),
            TopKValue::Int(i) => serde_json::Value::Number((*i).into()),
            TopKValue::Float(OrderedFloat(f)) => serde_json::Number::from_f64(*f)
                .map(serde_json::Value::Number)
                .unwrap_or(serde_json::Value::Null),
            TopKValue::Bool(b) => serde_json::Value::Bool(*b),
        }
    }

    /// Stable 64-bit hash of the value (single pass of AHash), used as CMS key.
    pub fn hash64(&self) -> u64 {
        use std::hash::{Hash, Hasher};
        let mut h = ahash::AHasher::default();
        self.hash(&mut h);
        h.finish()
    }
}

// ======================== CountMinSketch ========================

/// Count-Min Sketch with signed-int counters (saturates at 0 on decrement).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CountMinSketch {
    width: usize,
    depth: usize,
    /// Flattened row-major: `counters[row * width + col]`.
    counters: Vec<i64>,
    /// Running total of values inserted (minus decrements; saturated at 0).
    total: u64,
}

impl CountMinSketch {
    /// Create a new sketch with the given dimensions.
    pub fn new(width: usize, depth: usize) -> Self {
        assert!(width > 0 && depth > 0 && depth <= CMS_SEEDS.len());
        Self {
            width,
            depth,
            counters: vec![0; width * depth],
            total: 0,
        }
    }

    pub fn width(&self) -> usize {
        self.width
    }
    pub fn depth(&self) -> usize {
        self.depth
    }
    pub fn total(&self) -> u64 {
        self.total
    }

    /// MurmurHash3 finalizer applied with a per-row seed. Cheap, pairwise-independent.
    #[inline]
    fn rehash(hash: u64, seed: u64) -> u64 {
        let mut h = hash ^ seed;
        h ^= h >> 33;
        h = h.wrapping_mul(0xff51afd7ed558ccd);
        h ^= h >> 33;
        h = h.wrapping_mul(0xc4ceb9fe1a85ec53);
        h ^= h >> 33;
        h
    }

    /// Compute the column for a given row.
    #[inline]
    fn col(&self, hash: u64, row: usize) -> usize {
        (Self::rehash(hash, CMS_SEEDS[row]) as usize) % self.width
    }

    /// Insert (or decrement via negative delta). Counters saturate at 0.
    pub fn update(&mut self, hash: u64, delta: i64) {
        for row in 0..self.depth {
            let col = self.col(hash, row);
            let idx = row * self.width + col;
            let new = self.counters[idx].saturating_add(delta);
            self.counters[idx] = new.max(0);
        }
        if delta > 0 {
            self.total = self.total.saturating_add(delta as u64);
        } else if delta < 0 {
            self.total = self.total.saturating_sub((-delta) as u64);
        }
    }

    /// Convenience: increment by 1.
    #[inline]
    pub fn insert(&mut self, hash: u64) {
        self.update(hash, 1);
    }

    /// Convenience: decrement by 1 (saturates at 0).
    #[inline]
    pub fn decrement(&mut self, hash: u64) {
        self.update(hash, -1);
    }

    /// Point-query: minimum across rows.
    pub fn estimate(&self, hash: u64) -> i64 {
        let mut min = i64::MAX;
        for row in 0..self.depth {
            let col = self.col(hash, row);
            let idx = row * self.width + col;
            if self.counters[idx] < min {
                min = self.counters[idx];
            }
        }
        if min == i64::MAX {
            0
        } else {
            min
        }
    }

    /// Estimated heap footprint.
    pub fn estimated_bytes(&self) -> usize {
        std::mem::size_of::<Self>() + self.counters.capacity() * std::mem::size_of::<i64>()
    }
}

// ======================== TopKHeap ========================

/// Tracks approximate top-k heavy hitters backed by a CMS.
///
/// **Plan 22-04 optimization:** a side `AHashMap<TopKValue, usize>` index lets
/// `observe` check for existing candidates in O(1) (previously O(|candidates|)
/// linear scan — the bottleneck in the 22-03 micro-bench). The eviction path
/// at capacity still needs to find the current worst candidate by CMS estimate
/// (which changes on every decrement, so there's no stable position to heap);
/// that path is still O(max_candidates) but is hit at most once per insert
/// after saturation and max_candidates is bounded at `max(k*8, 64)` — ~64-640
/// ops worst case, well below the hot-path overhead the linear contains was
/// contributing on every single insert.
///
/// The index is **not** serialized — it's reconstructed on deserialize via
/// `#[serde(skip)]` + `post_deserialize_rebuild_index` invoked lazily on the
/// first mutating op.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopKHeap {
    k: usize,
    /// Candidate values (every value ever considered). Bounded in practice by
    /// `max_candidates`; on overflow, we drop the lowest-estimate candidate.
    /// Stored as a `Vec` (not a HashSet) so we can `#[derive(Serialize)]`
    /// without extra ahash features. Dedup is enforced at insert time.
    candidates: Vec<TopKValue>,
    /// Cap on candidate set (protects memory when adversarial inputs produce
    /// many heavy-hitter transitions).
    max_candidates: usize,
    /// Value → index-in-`candidates` side map. Not serialized; rebuilt lazily
    /// on first access after deserialize via `ensure_index`.
    #[serde(skip)]
    index: ahash::AHashMap<TopKValue, usize>,
    /// True once `index` is known to be in sync with `candidates`. Postcard
    /// round-tripped instances start with this false and trigger a rebuild
    /// on first mutating op.
    #[serde(skip, default)]
    index_ready: bool,
    /// Cached worst-candidate index + estimate. `observe` compares against this
    /// on the at-capacity path so the common "new value isn't heavy enough"
    /// case is O(1). The cache is conservatively maintained: actual worst may
    /// have shifted due to decrements between inserts, in which case we may
    /// skip admitting a value that's slightly heavier than what we think is
    /// the current worst. This is behaviorally equivalent to CMS's own error
    /// envelope — top_k read always does a full re-scan, so correctness of
    /// the final answer is preserved; only admission recall is affected.
    ///
    /// Invalidated (set to None) on `prune_empty` and when we evict (so the
    /// next at-capacity hit recomputes).
    #[serde(skip, default)]
    worst_cache: Option<(usize, i64)>,
}

impl TopKHeap {
    /// Create a new empty TopKHeap tracking at most `k` winners.
    pub fn new(k: usize) -> Self {
        Self {
            k,
            candidates: Vec::new(),
            max_candidates: (k * 8).max(64),
            index: ahash::AHashMap::new(),
            index_ready: true,
            worst_cache: None,
        }
    }

    /// Max candidate cap (testing visibility).
    pub fn max_candidates(&self) -> usize {
        self.max_candidates
    }

    /// Ensure the `index` side map is in sync with `candidates` (rebuild if
    /// the struct was just deserialized). O(n) one-shot; amortized O(1) per
    /// subsequent op.
    #[inline]
    fn ensure_index(&mut self) {
        if self.index_ready {
            return;
        }
        self.index.clear();
        self.index.reserve(self.candidates.len());
        for (i, c) in self.candidates.iter().enumerate() {
            self.index.insert(c.clone(), i);
        }
        self.index_ready = true;
    }

    #[inline]
    fn contains(&self, v: &TopKValue) -> bool {
        if self.index_ready {
            self.index.contains_key(v)
        } else {
            self.candidates.iter().any(|c| c == v)
        }
    }

    pub fn k(&self) -> usize {
        self.k
    }

    pub fn num_candidates(&self) -> usize {
        self.candidates.len()
    }

    /// Note a value as a candidate for top-k. Actual ranking is computed on
    /// read via `top_k` using CMS estimates.
    ///
    /// **Fast path (O(1)):** value already in candidates — HashMap hit, return.
    /// **Below capacity (O(1) amortized):** push + index insert.
    /// **At capacity, common case (O(1)):** new estimate compared against
    /// cached `worst_cache`; if it doesn't exceed the cached worst, drop.
    /// **At capacity, admission (O(max_candidates)):** evict worst, rescan
    /// to refresh cache. Hit only when the new value is actually heavier
    /// than the cached worst — rare for skewed workloads.
    pub fn observe(&mut self, value: &TopKValue, cms: &CountMinSketch) {
        self.ensure_index();
        if self.index.contains_key(value) {
            return;
        }
        if self.candidates.len() < self.max_candidates {
            let idx = self.candidates.len();
            self.candidates.push(value.clone());
            self.index.insert(value.clone(), idx);
            // Invalidate cache — new candidate may be the new worst.
            self.worst_cache = None;
            return;
        }
        // At capacity: consult the worst_cache first. O(1) if the new value
        // fails to beat the cached worst estimate.
        let new_est = cms.estimate(value.hash64());
        let (worst_idx, worst_est) = match self.worst_cache {
            Some(c) => c,
            None => {
                // First at-capacity op after cache invalidation: full O(max_candidates) scan.
                let mut wi: usize = 0;
                let mut we = cms.estimate(self.candidates[0].hash64());
                for (i, c) in self.candidates.iter().enumerate().skip(1) {
                    let e = cms.estimate(c.hash64());
                    if e < we {
                        we = e;
                        wi = i;
                    }
                }
                self.worst_cache = Some((wi, we));
                (wi, we)
            }
        };
        if new_est <= worst_est {
            // Common case on rotating / cycling workloads: drop fast.
            return;
        }
        // Admit: evict old candidate, replace in place, update index.
        let old = std::mem::replace(&mut self.candidates[worst_idx], value.clone());
        self.index.remove(&old);
        self.index.insert(value.clone(), worst_idx);
        // Invalidate the cache — the actual worst is now some OTHER candidate
        // whose estimate is <= new_est (but we don't know which without a
        // rescan). Next observe at capacity does one O(max_candidates) scan
        // to refresh, then the cache amortizes subsequent rejections to O(1).
        self.worst_cache = None;
    }

    /// Remove candidates whose current CMS estimate has dropped to zero.
    /// Call after bulk decrements to keep the set small.
    pub fn prune_empty(&mut self, cms: &CountMinSketch) {
        self.ensure_index();
        // Collect indices to evict (reverse order for swap_remove safety).
        let to_remove: Vec<usize> = self
            .candidates
            .iter()
            .enumerate()
            .filter_map(|(i, c)| {
                if cms.estimate(c.hash64()) == 0 {
                    Some(i)
                } else {
                    None
                }
            })
            .collect();
        // swap_remove from highest index so preceding indices stay valid.
        for &i in to_remove.iter().rev() {
            let removed = self.candidates.swap_remove(i);
            self.index.remove(&removed);
            // The value that was at the tail (if any) is now at index i; fix its index.
            if i < self.candidates.len() {
                let moved = self.candidates[i].clone();
                self.index.insert(moved, i);
            }
        }
        // Decrements shifted CMS estimates; cached worst is stale.
        self.worst_cache = None;
    }

    /// Test/debug helper: check membership.
    pub fn contains_value(&self, v: &TopKValue) -> bool {
        self.contains(v)
    }

    /// Return the current top-k `(value, estimated_count)` pairs in descending
    /// order by count. Re-queries the CMS for every candidate (O(|candidates|)
    /// per read; acceptable because candidates is bounded at `max_candidates`).
    pub fn top_k(&self, cms: &CountMinSketch) -> Vec<(TopKValue, i64)> {
        // Build a min-heap of (count, value), then extract in descending order.
        let mut heap: BinaryHeap<Reverse<(i64, TopKValue)>> = BinaryHeap::new();
        for c in &self.candidates {
            let est = cms.estimate(c.hash64());
            if est <= 0 {
                continue;
            }
            if heap.len() < self.k {
                heap.push(Reverse((est, c.clone())));
            } else if let Some(Reverse((min_est, _))) = heap.peek() {
                if est > *min_est {
                    heap.pop();
                    heap.push(Reverse((est, c.clone())));
                }
            }
        }
        let mut out: Vec<(TopKValue, i64)> =
            heap.into_iter().map(|Reverse((c, v))| (v, c)).collect();
        out.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));
        out
    }

    /// Estimated heap footprint.
    pub fn estimated_bytes(&self) -> usize {
        std::mem::size_of::<Self>()
            + self.candidates.len() * (std::mem::size_of::<TopKValue>() + 32)
    }
}

// ======================== Tests ========================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cms_basic_insert_estimate() {
        let mut cms = CountMinSketch::new(2048, 4);
        let h = TopKValue::Str("apple".into()).hash64();
        for _ in 0..100 {
            cms.insert(h);
        }
        assert!(cms.estimate(h) >= 100);
        assert_eq!(cms.total(), 100);
    }

    #[test]
    fn test_cms_decrement_saturates_at_zero() {
        let mut cms = CountMinSketch::new(2048, 4);
        let h = TopKValue::Str("x".into()).hash64();
        cms.insert(h);
        cms.decrement(h);
        cms.decrement(h); // underflow
        cms.decrement(h);
        assert_eq!(cms.estimate(h), 0);
    }

    #[test]
    fn test_cms_unknown_key_estimate_zero() {
        let cms = CountMinSketch::new(2048, 4);
        let h = TopKValue::Str("never-inserted".into()).hash64();
        assert_eq!(cms.estimate(h), 0);
    }

    #[test]
    fn test_cms_many_distinct_small_error() {
        let mut cms = CountMinSketch::new(2048, 4);
        // Insert 10k distinct values; then hot-key "hot" 1000 times.
        for i in 0..10_000 {
            cms.insert(TopKValue::Str(format!("v_{}", i)).hash64());
        }
        let hot = TopKValue::Str("hot".into()).hash64();
        for _ in 0..1_000 {
            cms.insert(hot);
        }
        // With w=2048, d=4: overestimate bound e/w * N ≈ 0.00133 * 11_000 ≈ 14.6
        let est = cms.estimate(hot);
        assert!(est >= 1000, "estimate {} below true count", est);
        assert!(est <= 1050, "estimate {} far above true count", est);
    }

    #[test]
    fn test_topk_heavy_hitters_zipf() {
        let mut cms = CountMinSketch::new(2048, 4);
        let mut heap = TopKHeap::new(5);
        // Zipf-like: value i gets (1000 - i*100) inserts for i=0..10.
        for i in 0..10 {
            let count = 1000 - i * 100;
            let v = TopKValue::Int(i as i64);
            for _ in 0..count {
                cms.insert(v.hash64());
            }
            heap.observe(&v, &cms);
        }
        let top = heap.top_k(&cms);
        assert_eq!(top.len(), 5);
        // Top should be value 0 (count 1000) at position 0.
        assert_eq!(top[0].0, TopKValue::Int(0));
        assert_eq!(top[1].0, TopKValue::Int(1));
    }

    #[test]
    fn test_topk_prune_empty() {
        let mut cms = CountMinSketch::new(2048, 4);
        let mut heap = TopKHeap::new(3);
        for i in 0..5 {
            let v = TopKValue::Int(i);
            cms.insert(v.hash64());
            heap.observe(&v, &cms);
        }
        // Decrement everything to zero.
        for i in 0..5 {
            let v = TopKValue::Int(i);
            cms.decrement(v.hash64());
        }
        heap.prune_empty(&cms);
        assert_eq!(heap.num_candidates(), 0);
    }

    #[test]
    fn test_topk_candidate_cap_evicts_lowest() {
        // k=2 → max_candidates=64 (default). Test the tight branch at max.
        let mut cms = CountMinSketch::new(2048, 4);
        let mut heap = TopKHeap::new(2);
        // Fill to max_candidates with low-weight values.
        for i in 0..heap.max_candidates {
            let v = TopKValue::Int(i as i64);
            cms.insert(v.hash64());
            heap.observe(&v, &cms);
        }
        assert_eq!(heap.num_candidates(), heap.max_candidates);
        // Insert one massively-heavy hitter; should evict a weak one.
        let hot = TopKValue::Str("super_hot".into());
        for _ in 0..10_000 {
            cms.insert(hot.hash64());
        }
        heap.observe(&hot, &cms);
        assert!(heap.candidates.contains(&hot));
        assert_eq!(heap.num_candidates(), heap.max_candidates);
    }

    #[test]
    fn test_topk_value_roundtrip_json() {
        for v in [
            TopKValue::Str("hello".into()),
            TopKValue::Int(-42),
            TopKValue::Float(OrderedFloat(2.5)),
            TopKValue::Bool(true),
        ] {
            let j = v.to_json();
            let back = TopKValue::from_json(&j).unwrap();
            assert_eq!(v, back);
        }
    }
}
