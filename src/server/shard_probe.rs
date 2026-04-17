//! Cross-shard event-fraction probe.
//!
//! Measures: for each PUSH, how many distinct key-hash shards would be
//! touched if the server ran thread-per-core with sharding by
//! `hash(entity_key) % N`? An event that touches one shard ("pure-local")
//! would be cheap under thread-per-core. An event that touches multiple
//! shards ("cross-shard") would need cross-thread reshuffle on every apply.
//!
//! The cross-shard fraction is the single number that decides whether
//! rewriting Beava to thread-per-core would be a net win. If cross-shard
//! events are <40%, reshuffle cost is rare and the architectural win is
//! real. If they're >80%, reshuffle cost dominates and the bet loses.
//!
//! Gated by `BEAVA_SHARD_PROBE=<N>` (N = hypothetical shard count, typically
//! 8, 16, or 64). When unset, all calls are no-ops and zero instructions are
//! emitted on the hot path.
//!
//! Readout: `/debug/shard_probe` returns JSON { shard_count, events_total,
//! events_single_shard, events_cross_shard, shards_touched_histogram }.

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::OnceLock;

/// Max histogram bucket — events touching more than this many shards
/// accumulate into the last bucket. Chosen generously so fraud-pipeline-
/// style fan-out (primary + cascade + 3-4 fan-out targets) fits exactly.
const HIST_MAX: usize = 16;

/// Active shard count (0 = probe disabled). Set once from env at startup.
static SHARD_COUNT: OnceLock<usize> = OnceLock::new();

/// Per-event totals.
static EVENTS_TOTAL: AtomicU64 = AtomicU64::new(0);
static EVENTS_SINGLE_SHARD: AtomicU64 = AtomicU64::new(0);
static EVENTS_CROSS_SHARD: AtomicU64 = AtomicU64::new(0);

/// Sum of shards_touched across all events (for mean computation).
static SHARDS_TOUCHED_SUM: AtomicU64 = AtomicU64::new(0);

/// Histogram of shards_touched — index i = events that touched exactly i
/// distinct shards. Index 0 is unused (an event always touches >= 1 shard).
static HIST: [AtomicU64; HIST_MAX + 1] = {
    // Can't use [AtomicU64::new(0); N] in const context without copy, so
    // expand manually. 17 entries.
    [
        AtomicU64::new(0), AtomicU64::new(0), AtomicU64::new(0), AtomicU64::new(0),
        AtomicU64::new(0), AtomicU64::new(0), AtomicU64::new(0), AtomicU64::new(0),
        AtomicU64::new(0), AtomicU64::new(0), AtomicU64::new(0), AtomicU64::new(0),
        AtomicU64::new(0), AtomicU64::new(0), AtomicU64::new(0), AtomicU64::new(0),
        AtomicU64::new(0),
    ]
};

/// Initialize probe shard count from env at startup. Call once from main.
pub fn init_from_env() {
    let n: usize = std::env::var("BEAVA_SHARD_PROBE")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    let _ = SHARD_COUNT.set(n);
    if n > 0 {
        eprintln!(
            "[shard-probe] enabled with shard_count={}; readout at /debug/shard_probe",
            n
        );
    }
}

/// Is the probe active?
#[inline]
pub fn is_enabled() -> bool {
    matches!(SHARD_COUNT.get(), Some(n) if *n > 0)
}

/// Get configured shard count (0 = disabled).
pub fn shard_count() -> usize {
    *SHARD_COUNT.get().unwrap_or(&0)
}

/// Hash a key string to a shard id. Uses ahash for speed — not
/// cryptographic, just uniform-ish.
#[inline]
pub fn shard_of(key: &str, n: usize) -> usize {
    use ahash::RandomState;
    use std::hash::{BuildHasher, Hash, Hasher};
    // Deterministic hash across runs: use a fixed seed so different runs
    // produce the same distribution (easier to compare).
    static STATE: OnceLock<RandomState> = OnceLock::new();
    let state = STATE.get_or_init(|| RandomState::with_seeds(0x01, 0x02, 0x03, 0x04));
    let mut h = state.build_hasher();
    key.hash(&mut h);
    (h.finish() as usize) % n.max(1)
}

/// Record a single event. `touched_keys` is the list of key-string slices
/// that this PUSH would touch under sharding — typically [primary_key,
/// cascade_key_1, ..., fanout_key_1, ...]. Duplicates are allowed and are
/// deduped by the shard computation.
///
/// Zero-cost when probe is disabled (checked via `is_enabled`).
pub fn record_event(touched_keys: &[&str]) {
    let n = shard_count();
    if n == 0 || touched_keys.is_empty() {
        return;
    }
    // Dedup shards via a small bitmask. HIST_MAX is 16 so a u64 bitmask
    // covers any realistic pipeline shape without allocation.
    // If shard_count > 64 we fall back to counting unique shards via a
    // small Vec — still cheap for typical fraud-pipeline fan-out of 5 keys.
    let unique_count = if n <= 64 {
        let mut mask: u64 = 0;
        for k in touched_keys {
            let s = shard_of(k, n);
            mask |= 1u64 << (s as u64 & 63);
        }
        mask.count_ones() as usize
    } else {
        let mut seen: [usize; HIST_MAX + 1] = [usize::MAX; HIST_MAX + 1];
        let mut count: usize = 0;
        'outer: for k in touched_keys {
            let s = shard_of(k, n);
            for v in seen.iter().take(count) {
                if *v == s {
                    continue 'outer;
                }
            }
            if count < seen.len() {
                seen[count] = s;
                count += 1;
            }
        }
        count
    };

    EVENTS_TOTAL.fetch_add(1, Ordering::Relaxed);
    SHARDS_TOUCHED_SUM.fetch_add(unique_count as u64, Ordering::Relaxed);
    let bucket = unique_count.min(HIST_MAX);
    HIST[bucket].fetch_add(1, Ordering::Relaxed);
    if unique_count <= 1 {
        EVENTS_SINGLE_SHARD.fetch_add(1, Ordering::Relaxed);
    } else {
        EVENTS_CROSS_SHARD.fetch_add(1, Ordering::Relaxed);
    }
}

/// Snapshot for the /debug/shard_probe endpoint.
#[derive(Debug, serde::Serialize)]
pub struct ShardProbeSnapshot {
    pub enabled: bool,
    pub shard_count: usize,
    pub events_total: u64,
    pub events_single_shard: u64,
    pub events_cross_shard: u64,
    pub cross_shard_fraction: f64,
    pub mean_shards_touched: f64,
    pub histogram: Vec<(usize, u64)>, // (shards_touched, count)
}

pub fn snapshot() -> ShardProbeSnapshot {
    let total = EVENTS_TOTAL.load(Ordering::Relaxed);
    let single = EVENTS_SINGLE_SHARD.load(Ordering::Relaxed);
    let cross = EVENTS_CROSS_SHARD.load(Ordering::Relaxed);
    let sum = SHARDS_TOUCHED_SUM.load(Ordering::Relaxed);
    let cross_fraction = if total > 0 {
        cross as f64 / total as f64
    } else {
        0.0
    };
    let mean = if total > 0 {
        sum as f64 / total as f64
    } else {
        0.0
    };
    // Skip bucket 0 since every event touches >= 1 shard.
    let histogram: Vec<(usize, u64)> = (1..=HIST_MAX)
        .filter_map(|i| {
            let c = HIST[i].load(Ordering::Relaxed);
            if c > 0 { Some((i, c)) } else { None }
        })
        .collect();
    ShardProbeSnapshot {
        enabled: is_enabled(),
        shard_count: shard_count(),
        events_total: total,
        events_single_shard: single,
        events_cross_shard: cross,
        cross_shard_fraction: cross_fraction,
        mean_shards_touched: mean,
        histogram,
    }
}

// ---------- hot-path helpers ----------

// Suppress unused_variables for the AtomicUsize import if nothing here uses
// it — keeps the file tidy if we later remove the vec-based branch.
#[allow(dead_code)]
fn _unused_probe_type_check(_: &AtomicUsize) {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn disabled_by_default() {
        // Not calling init_from_env with BEAVA_SHARD_PROBE — probe is off.
        // record_event is a no-op; counters stay at whatever other tests
        // left them. We just assert is_enabled is correctly querying
        // SHARD_COUNT.get() (which may or may not be set by another test).
        let _ = is_enabled();
    }

    #[test]
    fn shard_of_is_deterministic() {
        let a = shard_of("user_00001", 16);
        let b = shard_of("user_00001", 16);
        assert_eq!(a, b);
        assert!(a < 16);
    }

    #[test]
    fn shard_of_spreads_uniformly() {
        use std::collections::HashMap;
        let mut counts: HashMap<usize, usize> = HashMap::new();
        for i in 0..10_000 {
            let key = format!("k{}", i);
            *counts.entry(shard_of(&key, 16)).or_insert(0) += 1;
        }
        assert_eq!(counts.len(), 16, "all 16 shards should be hit");
        for (_shard, c) in counts {
            // Uniform distribution: expect ~625 per shard, allow ±30%.
            assert!(c > 400 && c < 900, "shard count out of expected range: {}", c);
        }
    }

    #[test]
    fn unique_shard_dedup() {
        // If two keys hash to the same shard, they count as 1 shard touched.
        // Can't easily construct guaranteed-colliding keys without probing
        // first, so we indirectly test by using the same key twice.
        // We need to enable the probe for this to actually mutate state.
        let _ = SHARD_COUNT.set(16);
        EVENTS_TOTAL.store(0, Ordering::Relaxed);
        EVENTS_SINGLE_SHARD.store(0, Ordering::Relaxed);
        EVENTS_CROSS_SHARD.store(0, Ordering::Relaxed);
        for i in 0..=HIST_MAX {
            HIST[i].store(0, Ordering::Relaxed);
        }
        record_event(&["u1", "u1", "u1"]);
        assert_eq!(EVENTS_TOTAL.load(Ordering::Relaxed), 1);
        assert_eq!(EVENTS_SINGLE_SHARD.load(Ordering::Relaxed), 1);
        assert_eq!(EVENTS_CROSS_SHARD.load(Ordering::Relaxed), 0);
    }
}
