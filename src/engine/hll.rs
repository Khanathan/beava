//! Adaptive distinct counting: Exact → HashSet → HLL++ (ClickHouse-style).
//!
//! Three phases, automatically promoted:
//! 1. **Exact** (≤16 elements): Flat sorted array. Zero error, ~128 bytes max.
//! 2. **HashSet** (≤HASH_THRESHOLD elements): AHashSet of u64 hashes. Zero error,
//!    ~8 bytes per unique.
//! 3. **HLL** (unlimited): HyperLogLog++ with bias correction. ~0.8% error at p=14.
//!
//! Why this works for fraud: most entities per time bucket have low cardinality
//! (a user visits ~5 merchants per hour). Phases 1-2 handle this with zero error
//! and less memory than any probabilistic sketch.
//!
//! Memory profile per sketch:
//! - 5 uniques:   40 bytes (exact array)
//! - 50 uniques:  400 bytes (hash set)
//! - 500 uniques: 4 KB (hash set)
//! - 5000 uniques: 12 KB (HLL dense)
//!
//! Combined with 30-bucket windowed ring buffer:
//! - Typical fraud user (20 merchants/hour): 30 × ~160B = ~5 KB per feature
//! - High-cardinality merchant (2000 users/day): 30 × ~4KB = ~120 KB per feature
//!
//! Also contains `DistinctCountOp` which wraps `RingBuffer<Hll>` for
//! windowed approximate distinct counting.

use crate::engine::operators::Operator;
use crate::engine::window::RingBuffer;
use crate::error::TallyError;
use crate::types::FeatureValue;
use serde::{Deserialize, Serialize};
use std::time::{Duration, SystemTime};

// ======================== Constants ========================

/// HLL precision: 12 bits = 4096 registers. ~1.6% error. 4 KB dense.
/// Same as ClickHouse default (uniqHLL12). Good balance of accuracy vs memory.
const HLL_P: usize = 12;
/// Number of registers
const HLL_M: usize = 1 << HLL_P;
/// Alpha correction constant for m=4096
const HLL_ALPHA: f64 = 0.7213 / (1.0 + 1.079 / HLL_M as f64);

/// Phase 1 max: flat array. ClickHouse uses 16.
const EXACT_THRESHOLD: usize = 16;
/// Phase 2 max: hash set → HLL dense transition.
///
/// Plan 22-03 bumps this from 512 to **1024** per the v0 hybrid-operator
/// locked spec: exact distinct-count up to 1024 uniques, then HLL. The
/// hash-set memory cost at 1024 × 8B = 8 KB is still comparable to the
/// 4 KB HLL dense representation (both small absolute costs); the win is
/// that zero-error distinct-count covers the vast majority of per-bucket
/// fraud workloads.
const HASH_THRESHOLD: usize = 1024;

/// HLL++ linear counting threshold for p=12 (from Google's zetasketch).
/// Below this raw estimate, linear counting is preferred.
const LC_THRESHOLD: f64 = 3100.0;

/// HLL++ bias correction data for p=12 (from Google's zetasketch / Heule et al. 2013).
/// 201 pairs of (raw_estimate, bias). Used for KNN interpolation in the transition zone.
/// Generated empirically by Google via Monte Carlo simulation.
const RAW_ESTIMATE_DATA: [f64; 201] = [
    2954.0, 3003.4782, 3053.3568, 3104.3666, 3155.324, 3206.9598, 3259.648, 3312.539, 3366.1474,
    3420.2576, 3474.8376, 3530.6076, 3586.451, 3643.38, 3700.4104, 3757.5638, 3815.9676, 3875.193,
    3934.838, 3994.8548, 4055.018, 4117.1742, 4178.4482, 4241.1294, 4304.4776, 4367.4044,
    4431.8724, 4496.3732, 4561.4304, 4627.5326, 4693.949, 4761.5532, 4828.7256, 4897.6182,
    4965.5186, 5034.4528, 5104.865, 5174.7164, 5244.6828, 5316.6708, 5387.8312, 5459.9036,
    5532.476, 5604.8652, 5679.6718, 5753.757, 5830.2072, 5905.2828, 5980.0434, 6056.6264,
    6134.3192, 6211.5746, 6290.0816, 6367.1176, 6447.9796, 6526.5576, 6606.1858, 6686.9144,
    6766.1142, 6847.0818, 6927.9664, 7010.9096, 7091.0816, 7175.3962, 7260.3454, 7344.018,
    7426.4214, 7511.3106, 7596.0686, 7679.8094, 7765.818, 7852.4248, 7936.834, 8022.363, 8109.5066,
    8200.4554, 8288.5832, 8373.366, 8463.4808, 8549.7682, 8642.0522, 8728.3288, 8820.9528,
    8907.727, 9001.0794, 9091.2522, 9179.988, 9269.852, 9362.6394, 9453.642, 9546.9024, 9640.6616,
    9732.6622, 9824.3254, 9917.7484, 10007.9392, 10106.7508, 10196.2152, 10289.8114, 10383.5494,
    10482.3064, 10576.8734, 10668.7872, 10764.7156, 10862.0196, 10952.793, 11049.9748, 11146.0702,
    11241.4492, 11339.2772, 11434.2336, 11530.741, 11627.6136, 11726.311, 11821.5964, 11918.837,
    12015.3724, 12113.0162, 12213.0424, 12306.9804, 12408.4518, 12504.8968, 12604.586, 12700.9332,
    12798.705, 12898.5142, 12997.0488, 13094.788, 13198.475, 13292.7764, 13392.9698, 13486.8574,
    13590.1616, 13686.5838, 13783.6264, 13887.2638, 13992.0978, 14081.0844, 14189.9956, 14280.0912,
    14382.4956, 14486.4384, 14588.1082, 14686.2392, 14782.276, 14888.0284, 14985.1864, 15088.8596,
    15187.0998, 15285.027, 15383.6694, 15495.8266, 15591.3736, 15694.2008, 15790.3246, 15898.4116,
    15997.4522, 16095.5014, 16198.8514, 16291.7492, 16402.6424, 16499.1266, 16606.2436, 16697.7186,
    16796.3946, 16902.3376, 17005.7672, 17100.814, 17206.8282, 17305.8262, 17416.0744, 17508.4092,
    17617.0178, 17715.4554, 17816.758, 17920.1748, 18012.9236, 18119.7984, 18223.2248, 18324.2482,
    18426.6276, 18525.0932, 18629.8976, 18733.2588, 18831.0466, 18940.1366, 19032.2696, 19131.729,
    19243.4864, 19349.6932, 19442.866, 19547.9448, 19653.2798, 19754.4034, 19854.0692, 19965.1224,
    20065.1774, 20158.2212, 20253.353, 20366.3264, 20463.22,
];

const BIAS_DATA: [f64; 201] = [
    2953.0, 2900.4782, 2848.3568, 2796.3666, 2745.324, 2694.9598, 2644.648, 2595.539, 2546.1474,
    2498.2576, 2450.8376, 2403.6076, 2357.451, 2311.38, 2266.4104, 2221.5638, 2176.9676, 2134.193,
    2090.838, 2048.8548, 2007.018, 1966.1742, 1925.4482, 1885.1294, 1846.4776, 1807.4044,
    1768.8724, 1731.3732, 1693.4304, 1657.5326, 1621.949, 1586.5532, 1551.7256, 1517.6182,
    1483.5186, 1450.4528, 1417.865, 1385.7164, 1352.6828, 1322.6708, 1291.8312, 1260.9036,
    1231.476, 1201.8652, 1173.6718, 1145.757, 1119.2072, 1092.2828, 1065.0434, 1038.6264,
    1014.3192, 988.5746, 965.0816, 940.1176, 917.9796, 894.5576, 871.1858, 849.9144, 827.1142,
    805.0818, 783.9664, 763.9096, 742.0816, 724.3962, 706.3454, 688.018, 667.4214, 650.3106,
    633.0686, 613.8094, 597.818, 581.4248, 563.834, 547.363, 531.5066, 520.4554, 505.5832, 488.366,
    476.4808, 459.7682, 450.0522, 434.3288, 423.9528, 408.727, 399.0794, 387.2522, 373.988,
    360.852, 351.6394, 339.642, 330.9024, 322.6616, 311.6622, 301.3254, 291.7484, 279.9392,
    276.7508, 263.2152, 254.8114, 245.5494, 242.3064, 234.8734, 223.7872, 217.7156, 212.0196,
    200.793, 195.9748, 189.0702, 182.4492, 177.2772, 170.2336, 164.741, 158.6136, 155.311,
    147.5964, 142.837, 137.3724, 132.0162, 130.0424, 121.9804, 120.4518, 114.8968, 111.586,
    105.9332, 101.705, 98.5142, 95.0488, 89.788, 91.475, 83.7764, 80.9698, 72.8574, 73.1616,
    67.5838, 62.6264, 63.2638, 66.0978, 52.0844, 58.9956, 47.0912, 46.4956, 48.4384, 47.1082,
    43.2392, 37.276, 40.0284, 35.1864, 35.8596, 32.0998, 28.027, 23.6694, 33.8266, 26.3736,
    27.2008, 21.3246, 26.4116, 23.4522, 19.5014, 19.8514, 10.7492, 18.6424, 13.1266, 18.2436,
    6.7186, 3.3946, 6.3376, 7.7672, 0.814, 3.8282, 0.8262, 8.0744, -1.5908, 5.0178, 0.4554, -0.242,
    0.1748, -9.0764, -4.2016, -3.7752, -4.7518, -5.3724, -8.9068, -6.1024, -5.7412, -9.9534,
    -3.8634, -13.7304, -16.271, -7.5136, -3.3068, -13.134, -10.0552, -6.7202, -8.5966, -10.9308,
    -1.8776, -4.8226, -13.7788, -21.647, -10.6736, -15.78,
];

/// KNN neighbor count for bias interpolation (same as Google's zetasketch).
const KNN_K: usize = 6;

// ======================== Hash ========================

/// Hash a string value. Returns u64 used for both exact-mode dedup and HLL register selection.
fn hash_value(value: &str) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut hasher = ahash::AHasher::default();
    value.hash(&mut hasher);
    hasher.finish()
}

// ======================== Hll struct ========================

/// Three-phase adaptive distinct count sketch.
///
/// Exact (sorted array) → HashSet (AHash u64) → HLL++ (bias-corrected, p=14).
/// Phases promote automatically on insert. Merge handles cross-phase combinations.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Hll {
    phase: HllPhase,
}

/// Backward-compatible alias.
pub type Ull = Hll;

#[derive(Debug, Clone, Serialize, Deserialize)]
enum HllPhase {
    /// ≤16 elements. Sorted Vec of hashes. Zero error.
    Exact(Vec<u64>),
    /// ≤HASH_THRESHOLD elements. Unsorted Vec of unique hashes (acts as hash set).
    /// Using Vec instead of HashSet for serde compatibility.
    HashSet(Vec<u64>),
    /// HLL++ with p=14. 6-bit registers packed into bytes.
    /// Stored as raw u8 array (1 byte per register for simplicity; top 2 bits unused).
    Dense(Vec<u8>),
}

impl Default for Hll {
    fn default() -> Self {
        Hll {
            phase: HllPhase::Exact(Vec::new()),
        }
    }
}

impl Hll {
    pub fn new() -> Self {
        Self::default()
    }

    /// Extract HLL register index and rank from a hash.
    #[inline]
    fn hash_to_register(hash: u64) -> (usize, u8) {
        let index = (hash >> (64 - HLL_P)) as usize;
        let remaining = (hash << HLL_P) | (1 << (HLL_P - 1));
        let rank = remaining.leading_zeros() as u8 + 1;
        (index, rank)
    }

    /// Promote Exact → HashSet.
    fn promote_to_hashset(&mut self) {
        if let HllPhase::Exact(arr) = &self.phase {
            let hashes = arr.clone();
            self.phase = HllPhase::HashSet(hashes);
        }
    }

    /// Promote HashSet → Dense HLL.
    fn promote_to_dense(&mut self) {
        let hashes = match &self.phase {
            HllPhase::Exact(arr) => arr.clone(),
            HllPhase::HashSet(set) => set.clone(),
            HllPhase::Dense(_) => return,
        };
        let mut registers = vec![0u8; HLL_M];
        for hash in &hashes {
            let (idx, rank) = Self::hash_to_register(*hash);
            registers[idx] = registers[idx].max(rank);
        }
        self.phase = HllPhase::Dense(registers);
    }

    /// Insert a string value.
    pub fn insert(&mut self, value: &str) {
        let hash = hash_value(value);

        match &mut self.phase {
            HllPhase::Exact(arr) => {
                // Binary search for dedup in sorted array
                match arr.binary_search(&hash) {
                    Ok(_) => {} // Already present
                    Err(pos) => {
                        arr.insert(pos, hash);
                        if arr.len() > EXACT_THRESHOLD {
                            self.promote_to_hashset();
                        }
                    }
                }
            }
            HllPhase::HashSet(set) => {
                // Linear scan for dedup (small enough to be fast)
                if !set.contains(&hash) {
                    set.push(hash);
                    if set.len() > HASH_THRESHOLD {
                        self.promote_to_dense();
                    }
                }
            }
            HllPhase::Dense(registers) => {
                let (idx, rank) = Self::hash_to_register(hash);
                registers[idx] = registers[idx].max(rank);
            }
        }
    }

    /// Estimate cardinality.
    /// - Exact/HashSet: returns exact count (zero error).
    /// - Dense: HLL++ with bias correction.
    pub fn count(&self) -> f64 {
        match &self.phase {
            HllPhase::Exact(arr) => arr.len() as f64,
            HllPhase::HashSet(set) => set.len() as f64,
            HllPhase::Dense(registers) => self.hll_count(registers),
        }
    }

    /// HLL++ cardinality estimate with Google's bias correction (Heule et al. 2013).
    ///
    /// Algorithm:
    /// 1. Compute raw harmonic mean estimate
    /// 2. If raw < threshold and zeros exist → use linear counting
    /// 3. Otherwise → subtract bias via KNN interpolation of precomputed table
    fn hll_count(&self, registers: &[u8]) -> f64 {
        let sum: f64 = registers.iter().map(|&r| 2.0_f64.powi(-(r as i32))).sum();
        let raw = HLL_ALPHA * (HLL_M as f64) * (HLL_M as f64) / sum;
        let zeros = registers.iter().filter(|&&r| r == 0).count();

        // Linear counting estimate (used when zeros exist)
        let lc = if zeros > 0 {
            (HLL_M as f64) * (HLL_M as f64 / zeros as f64).ln()
        } else {
            0.0
        };

        // Decision logic from HLL++ paper:
        if raw <= LC_THRESHOLD {
            // Very small range: linear counting is more accurate
            if zeros > 0 {
                return lc;
            }
        }

        // In the bias correction range: subtract estimated bias
        if raw <= RAW_ESTIMATE_DATA[RAW_ESTIMATE_DATA.len() - 1] {
            let bias = Self::estimate_bias_knn(raw);
            let corrected = raw - bias;
            // If linear counting is available and gives a smaller estimate, prefer it
            if zeros > 0 && lc < corrected {
                return lc;
            }
            return corrected;
        }

        // Large range: raw estimate is unbiased
        raw
    }

    /// KNN-based bias estimation from Google's precomputed table.
    /// Finds the K nearest raw estimates in the table and averages their biases.
    fn estimate_bias_knn(raw: f64) -> f64 {
        let n = RAW_ESTIMATE_DATA.len();

        // Find K nearest neighbors by distance to raw
        let mut distances: Vec<(f64, usize)> = RAW_ESTIMATE_DATA
            .iter()
            .enumerate()
            .map(|(i, &est)| ((raw - est).abs(), i))
            .collect();
        distances.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());

        // Average the biases of the K nearest
        let k = KNN_K.min(n);
        let bias_sum: f64 = distances[..k].iter().map(|&(_, idx)| BIAS_DATA[idx]).sum();
        bias_sum / k as f64
    }

    /// Merge another sketch into this one (union semantics).
    pub fn merge(&mut self, other: &Hll) {
        match (&mut self.phase, &other.phase) {
            // Both exact: merge sorted arrays
            (HllPhase::Exact(a), HllPhase::Exact(b)) => {
                for &hash in b {
                    if let Err(pos) = a.binary_search(&hash) {
                        a.insert(pos, hash);
                    }
                }
                if a.len() > EXACT_THRESHOLD {
                    self.promote_to_hashset();
                }
                // Check if hashset also overflows
                if let HllPhase::HashSet(set) = &self.phase {
                    if set.len() > HASH_THRESHOLD {
                        self.promote_to_dense();
                    }
                }
            }
            // Both hashset: merge
            (HllPhase::HashSet(a), HllPhase::HashSet(b)) => {
                for &hash in b {
                    if !a.contains(&hash) {
                        a.push(hash);
                    }
                }
                if a.len() > HASH_THRESHOLD {
                    self.promote_to_dense();
                }
            }
            // Both dense: register-wise max
            (HllPhase::Dense(a), HllPhase::Dense(b)) => {
                for i in 0..HLL_M {
                    a[i] = a[i].max(b[i]);
                }
            }
            // Cross-phase: promote lower phase up, then merge
            (HllPhase::Exact(_), HllPhase::HashSet(_)) => {
                self.promote_to_hashset();
                self.merge(other);
            }
            (HllPhase::Exact(_), HllPhase::Dense(_))
            | (HllPhase::HashSet(_), HllPhase::Dense(_)) => {
                self.promote_to_dense();
                self.merge(other);
            }
            (HllPhase::HashSet(a), HllPhase::Exact(b)) => {
                for &hash in b {
                    if !a.contains(&hash) {
                        a.push(hash);
                    }
                }
                if a.len() > HASH_THRESHOLD {
                    self.promote_to_dense();
                }
            }
            (HllPhase::Dense(a), HllPhase::Exact(b))
            | (HllPhase::Dense(a), HllPhase::HashSet(b)) => {
                for &hash in b {
                    let (idx, rank) = Self::hash_to_register(hash);
                    a[idx] = a[idx].max(rank);
                }
            }
        }
    }

    /// Check if the sketch has had no insertions.
    pub fn is_empty(&self) -> bool {
        match &self.phase {
            HllPhase::Exact(arr) => arr.is_empty(),
            HllPhase::HashSet(set) => set.is_empty(),
            HllPhase::Dense(regs) => regs.iter().all(|&r| r == 0),
        }
    }

    /// Approximate memory footprint in bytes.
    pub fn size_bytes(&self) -> usize {
        match &self.phase {
            HllPhase::Exact(arr) => arr.len() * 8,
            HllPhase::HashSet(set) => set.len() * 8,
            HllPhase::Dense(_) => HLL_M,
        }
    }

    /// Which phase the sketch is in.
    pub fn phase_name(&self) -> &'static str {
        match &self.phase {
            HllPhase::Exact(_) => "exact",
            HllPhase::HashSet(_) => "hashset",
            HllPhase::Dense(_) => "hll",
        }
    }
}

// ======================== DistinctCountOp ========================

/// Windowed approximate distinct count using RingBuffer<Hll>.
///
/// Each bucket holds an adaptive three-phase sketch. On push, the value is
/// inserted into the current bucket. On read, all non-empty buckets are
/// merged and cardinality is estimated.
///
/// Memory per entity (30 buckets):
/// - Low cardinality (5 uniques/bucket):  30 × 40B   = 1.2 KB
/// - Mid cardinality (100 uniques/bucket): 30 × 800B  = 24 KB
/// - High cardinality (5000+/bucket):      30 × 16KB  = 480 KB
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DistinctCountOp {
    field: String,
    buffer: RingBuffer<Hll>,
    event_count: RingBuffer<u64>,
    optional: bool,
}

impl DistinctCountOp {
    pub fn new(
        field: impl Into<String>,
        window_duration: Duration,
        bucket_duration: Duration,
        optional: bool,
    ) -> Self {
        Self {
            field: field.into(),
            buffer: RingBuffer::new(window_duration, bucket_duration),
            event_count: RingBuffer::new(window_duration, bucket_duration),
            optional,
        }
    }
}

impl Operator for DistinctCountOp {
    fn push(
        &mut self,
        event: &serde_json::Value,
        enrichment: Option<&ahash::AHashMap<String, serde_json::Value>>,
        now: SystemTime,
    ) -> Result<(), TallyError> {
        match crate::engine::operators::resolve_field(&self.field, event, enrichment) {
            None => {
                if self.optional {
                    Ok(())
                } else {
                    Err(TallyError::Type {
                        field: self.field.clone(),
                        expected: "string or numeric".into(),
                        got: "absent".into(),
                    })
                }
            }
            Some(val) => {
                let str_val = match val {
                    serde_json::Value::String(s) => s.clone(),
                    serde_json::Value::Number(n) => n.to_string(),
                    serde_json::Value::Bool(b) => b.to_string(),
                    _ => {
                        return Err(TallyError::Type {
                            field: self.field.clone(),
                            expected: "string or numeric".into(),
                            got: format!("{}", val),
                        })
                    }
                };
                self.buffer.update_current(|hll| hll.insert(&str_val), now);
                self.event_count.add_to_current(1u64, now);
                Ok(())
            }
        }
    }

    fn read(&mut self, now: SystemTime) -> FeatureValue {
        self.buffer.advance_to(now);
        self.event_count.advance_to(now);
        if self.event_count.sum_all() == 0 {
            return FeatureValue::Missing;
        }
        let mut merged = Hll::new();
        for bucket in self.buffer.buckets_iter() {
            if !bucket.is_empty() {
                merged.merge(bucket);
            }
        }
        FeatureValue::Float(merged.count())
    }

    fn estimated_bytes(&self) -> usize {
        let n = self.buffer.num_buckets();
        // RingBuffer<Hll> + RingBuffer<u64>
        // Each Hll has variable size depending on phase
        let mut total = n * std::mem::size_of::<u64>();
        for bucket in self.buffer.buckets_iter() {
            total += bucket.size_bytes() + std::mem::size_of::<Hll>();
        }
        total
    }

    fn num_buckets(&self) -> usize {
        self.buffer.num_buckets()
    }

    fn hybrid_telemetry(&self) -> Option<crate::engine::operators::HybridTelemetry> {
        // Union-of-buckets mode classification: if any bucket has already
        // promoted to Dense (HLL), report "sketch"; otherwise "exact".
        // exact_count is the sum of currently-tracked exact/hashset uniques.
        let mut any_dense = false;
        let mut exact_count = 0usize;
        for bucket in self.buffer.buckets_iter() {
            match bucket.phase_name() {
                "hll" => {
                    any_dense = true;
                }
                _ => {
                    exact_count += bucket.count() as usize;
                }
            }
        }
        Some(crate::engine::operators::HybridTelemetry {
            op: "distinct_count",
            mode: if any_dense { "sketch" } else { "exact" },
            exact_count,
            transition_at: HASH_THRESHOLD,
            sketch_alpha_current: None,
            memory_bytes: self.estimated_bytes(),
        })
    }
}

impl DistinctCountOp {
    /// `"exact"` if no per-bucket sketch has promoted to dense HLL yet,
    /// `"sketch"` otherwise. Matches the hybrid-op taxonomy in plan 22-03.
    pub fn mode_name(&self) -> &'static str {
        for bucket in self.buffer.buckets_iter() {
            if bucket.phase_name() == "hll" {
                return "sketch";
            }
        }
        "exact"
    }

    /// Transition threshold (unique values per bucket before promoting to
    /// dense HLL). Exposed for tests.
    pub fn transition_at(&self) -> usize {
        HASH_THRESHOLD
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ======================== Phase transition tests ========================

    #[test]
    fn test_new_is_empty_exact() {
        let h = Hll::new();
        assert!(h.is_empty());
        assert_eq!(h.phase_name(), "exact");
        assert_eq!(h.size_bytes(), 0);
        assert_eq!(h.count(), 0.0);
    }

    #[test]
    fn test_exact_phase_small() {
        let mut h = Hll::new();
        for i in 0..5 {
            h.insert(&format!("item_{}", i));
        }
        assert_eq!(h.phase_name(), "exact");
        assert_eq!(h.count(), 5.0); // Zero error
        assert_eq!(h.size_bytes(), 40); // 5 × 8 bytes
    }

    #[test]
    fn test_exact_to_hashset_promotion() {
        let mut h = Hll::new();
        for i in 0..20 {
            h.insert(&format!("item_{}", i));
        }
        assert_eq!(h.phase_name(), "hashset");
        assert_eq!(h.count(), 20.0); // Still zero error
    }

    #[test]
    fn test_hashset_phase() {
        let mut h = Hll::new();
        for i in 0..500 {
            h.insert(&format!("item_{}", i));
        }
        assert_eq!(h.phase_name(), "hashset");
        assert_eq!(h.count(), 500.0); // Zero error
        assert_eq!(h.size_bytes(), 500 * 8);
    }

    #[test]
    fn test_hashset_to_dense_promotion() {
        let mut h = Hll::new();
        for i in 0..2000 {
            h.insert(&format!("item_{}", i));
        }
        assert_eq!(h.phase_name(), "hll");
        let count = h.count();
        // HLL p=14: ~0.8% error, allow 5% for test
        assert!(
            (1900.0..=2100.0).contains(&count),
            "Expected ~2000, got {}",
            count
        );
    }

    #[test]
    fn test_dense_10000_unique() {
        let mut h = Hll::new();
        for i in 0..10000 {
            h.insert(&format!("item_{}", i));
        }
        assert_eq!(h.phase_name(), "hll");
        let count = h.count();
        assert!(
            (9500.0..=10500.0).contains(&count),
            "Expected ~10000, got {}",
            count
        );
    }

    #[test]
    fn test_duplicates_stay_exact() {
        let mut h = Hll::new();
        for _ in 0..100 {
            h.insert("same");
        }
        assert_eq!(h.phase_name(), "exact");
        assert_eq!(h.count(), 1.0);
        assert_eq!(h.size_bytes(), 8); // 1 hash
    }

    // ======================== Merge tests ========================

    #[test]
    fn test_merge_exact_exact() {
        let mut a = Hll::new();
        let mut b = Hll::new();
        for i in 0..5 {
            a.insert(&format!("a_{}", i));
        }
        for i in 0..5 {
            b.insert(&format!("b_{}", i));
        }
        a.merge(&b);
        assert_eq!(a.count(), 10.0);
        assert_eq!(a.phase_name(), "exact"); // 10 ≤ 16
    }

    #[test]
    fn test_merge_exact_promotes_to_hashset() {
        let mut a = Hll::new();
        let mut b = Hll::new();
        for i in 0..10 {
            a.insert(&format!("a_{}", i));
        }
        for i in 0..10 {
            b.insert(&format!("b_{}", i));
        }
        a.merge(&b);
        assert_eq!(a.count(), 20.0);
        assert_eq!(a.phase_name(), "hashset"); // 20 > 16
    }

    #[test]
    fn test_merge_overlapping_exact() {
        let mut a = Hll::new();
        let mut b = Hll::new();
        for i in 0..5 {
            a.insert(&format!("item_{}", i));
            b.insert(&format!("item_{}", i));
        }
        a.merge(&b);
        assert_eq!(a.count(), 5.0); // Union, not sum
    }

    #[test]
    fn test_merge_hashset_hashset() {
        let mut a = Hll::new();
        let mut b = Hll::new();
        for i in 0..100 {
            a.insert(&format!("a_{}", i));
        }
        for i in 0..100 {
            b.insert(&format!("b_{}", i));
        }
        a.merge(&b);
        assert_eq!(a.count(), 200.0);
        assert_eq!(a.phase_name(), "hashset");
    }

    #[test]
    fn test_merge_cross_phase_exact_into_dense() {
        let mut dense = Hll::new();
        let mut exact = Hll::new();
        for i in 0..2000 {
            dense.insert(&format!("d_{}", i));
        }
        for i in 0..5 {
            exact.insert(&format!("e_{}", i));
        }
        assert_eq!(dense.phase_name(), "hll");
        assert_eq!(exact.phase_name(), "exact");
        dense.merge(&exact);
        assert_eq!(dense.phase_name(), "hll");
    }

    #[test]
    fn test_merge_exact_into_dense_promotes() {
        let mut exact = Hll::new();
        let mut dense = Hll::new();
        for i in 0..5 {
            exact.insert(&format!("e_{}", i));
        }
        for i in 0..2000 {
            dense.insert(&format!("d_{}", i));
        }
        exact.merge(&dense);
        assert_eq!(exact.phase_name(), "hll");
    }

    // ======================== Memory tests ========================

    #[test]
    fn test_memory_fraud_typical_user() {
        // Typical: user visits 5 merchants per time bucket
        let mut h = Hll::new();
        for i in 0..5 {
            h.insert(&format!("merchant_{}", i));
        }
        assert_eq!(h.phase_name(), "exact");
        assert!(
            h.size_bytes() <= 40,
            "5 merchants should be ≤ 40 bytes, got {}",
            h.size_bytes()
        );
    }

    #[test]
    fn test_memory_fraud_active_merchant() {
        // Active merchant: 200 unique users per bucket
        let mut h = Hll::new();
        for i in 0..200 {
            h.insert(&format!("user_{}", i));
        }
        assert_eq!(h.phase_name(), "hashset");
        assert!(
            h.size_bytes() <= 1700,
            "200 users should be ≤ 1.7 KB, got {}",
            h.size_bytes()
        );
    }

    // ======================== Serialization tests ========================

    #[test]
    fn test_postcard_round_trip_exact() {
        let mut h = Hll::new();
        for i in 0..10 {
            h.insert(&format!("item_{}", i));
        }
        let before = h.count();
        let bytes = postcard::to_allocvec(&h).unwrap();
        let restored: Hll = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(before, restored.count());
        assert_eq!(restored.phase_name(), "exact");
        assert!(
            bytes.len() < 200,
            "Exact 10 items should serialize small, got {}",
            bytes.len()
        );
    }

    #[test]
    fn test_postcard_round_trip_hashset() {
        let mut h = Hll::new();
        for i in 0..100 {
            h.insert(&format!("item_{}", i));
        }
        let before = h.count();
        let bytes = postcard::to_allocvec(&h).unwrap();
        let restored: Hll = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(before, restored.count());
        assert_eq!(restored.phase_name(), "hashset");
    }

    #[test]
    fn test_postcard_round_trip_dense() {
        let mut h = Hll::new();
        for i in 0..5000 {
            h.insert(&format!("item_{}", i));
        }
        let before = h.count();
        let bytes = postcard::to_allocvec(&h).unwrap();
        let restored: Hll = postcard::from_bytes(&bytes).unwrap();
        assert!((before - restored.count()).abs() < f64::EPSILON);
        assert_eq!(restored.phase_name(), "hll");
    }

    // ======================== DistinctCountOp tests ========================

    use std::time::UNIX_EPOCH;

    fn ts(secs: u64) -> SystemTime {
        UNIX_EPOCH + Duration::from_secs(secs)
    }

    fn make_op(optional: bool) -> DistinctCountOp {
        DistinctCountOp::new(
            "merchant_id",
            Duration::from_secs(5 * 60),
            Duration::from_secs(60),
            optional,
        )
    }

    #[test]
    fn test_dc_5_unique() {
        let mut op = make_op(false);
        let t0 = ts(60000);
        for i in 0..5 {
            op.push(
                &serde_json::json!({"merchant_id": format!("m{}", i)}),
                None,
                t0,
            )
            .unwrap();
        }
        match op.read(t0) {
            FeatureValue::Float(v) => assert_eq!(v, 5.0, "Exact mode should give 5.0"),
            other => panic!("Expected Float, got {:?}", other),
        }
    }

    #[test]
    fn test_dc_duplicates() {
        let mut op = make_op(false);
        let t0 = ts(60000);
        for _ in 0..5 {
            op.push(&serde_json::json!({"merchant_id": "same"}), None, t0)
                .unwrap();
        }
        match op.read(t0) {
            FeatureValue::Float(v) => assert_eq!(v, 1.0, "Exact mode should give 1.0"),
            other => panic!("Expected Float, got {:?}", other),
        }
    }

    #[test]
    fn test_dc_missing() {
        let mut op = make_op(false);
        assert_eq!(op.read(ts(60000)), FeatureValue::Missing);
    }

    #[test]
    fn test_dc_expiry() {
        let mut op = make_op(false);
        let t0 = ts(60000);
        op.push(&serde_json::json!({"merchant_id": "m1"}), None, t0)
            .unwrap();
        assert_ne!(op.read(t0), FeatureValue::Missing);
        assert_eq!(
            op.read(t0 + Duration::from_secs(600)),
            FeatureValue::Missing
        );
    }

    #[test]
    fn test_dc_cross_bucket_merge() {
        let mut op = make_op(false);
        let t0 = ts(60000);
        for i in 0..3 {
            op.push(
                &serde_json::json!({"merchant_id": format!("m{}", i)}),
                None,
                t0 + Duration::from_secs(i * 60),
            )
            .unwrap();
        }
        match op.read(t0 + Duration::from_secs(120)) {
            FeatureValue::Float(v) => assert_eq!(v, 3.0, "Should merge exact buckets to 3"),
            other => panic!("Expected Float, got {:?}", other),
        }
    }

    #[test]
    fn test_dc_optional_skips_absent() {
        let mut op = make_op(true);
        assert!(op
            .push(&serde_json::json!({"other": "val"}), None, ts(60000))
            .is_ok());
        assert_eq!(op.read(ts(60000)), FeatureValue::Missing);
    }

    #[test]
    fn test_dc_required_errors_on_absent() {
        let mut op = make_op(false);
        assert!(op
            .push(&serde_json::json!({"other": "val"}), None, ts(60000))
            .is_err());
    }

    #[test]
    fn test_dc_numeric_values() {
        let mut op = make_op(false);
        let t0 = ts(60000);
        for i in 0..5 {
            op.push(&serde_json::json!({"merchant_id": i}), None, t0)
                .unwrap();
        }
        match op.read(t0) {
            FeatureValue::Float(v) => assert_eq!(v, 5.0),
            other => panic!("Expected Float, got {:?}", other),
        }
    }

    #[test]
    fn test_dc_postcard_round_trip() {
        let mut op = make_op(false);
        let t0 = ts(60000);
        for i in 0..10 {
            op.push(
                &serde_json::json!({"merchant_id": format!("m{}", i)}),
                None,
                t0,
            )
            .unwrap();
        }
        let before = op.read(t0);
        let bytes = postcard::to_allocvec(&op).unwrap();
        let mut restored: DistinctCountOp = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(before, restored.read(t0));
    }
}
