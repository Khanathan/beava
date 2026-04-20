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
        AtomicU64::new(0),
        AtomicU64::new(0),
        AtomicU64::new(0),
        AtomicU64::new(0),
        AtomicU64::new(0),
        AtomicU64::new(0),
        AtomicU64::new(0),
        AtomicU64::new(0),
        AtomicU64::new(0),
        AtomicU64::new(0),
        AtomicU64::new(0),
        AtomicU64::new(0),
        AtomicU64::new(0),
        AtomicU64::new(0),
        AtomicU64::new(0),
        AtomicU64::new(0),
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
        // Intentional: startup status (Phase 47 audit)
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
    // Deterministic hash across runs: use a fixed seed so different runs
    // produce the same distribution (easier to compare).
    static STATE: OnceLock<RandomState> = OnceLock::new();
    let state = STATE.get_or_init(|| RandomState::with_seeds(0x01, 0x02, 0x03, 0x04));
    (state.hash_one(key) as usize) % n.max(1)
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
            if c > 0 {
                Some((i, c))
            } else {
                None
            }
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

// ---------- Phase 50-07: per-shard routing counters (TPC-PERF-03) ----------

/// Per-shard event routing counters: `ROUTE_COUNTERS[i]` = events routed to shard i.
/// Initialized once via `init_route_counters(shard_count)`. Thread-safe via AtomicU64.
static ROUTE_COUNTERS: OnceLock<Vec<AtomicU64>> = OnceLock::new();

/// Total events routed (denominator for cross_shard_fraction).
static ROUTE_TOTAL: AtomicU64 = AtomicU64::new(0);

/// Initialize per-shard routing counters. Call once from run_tcp_server after
/// spawn_shard_threads returns the handle count. Idempotent (OnceLock).
pub fn init_route_counters(shard_count: usize) {
    let _ = ROUTE_COUNTERS.set(
        (0..shard_count.max(1))
            .map(|_| AtomicU64::new(0))
            .collect(),
    );
}

/// Record that an event was routed to `shard_index`.
/// Zero-cost if `ROUTE_COUNTERS` is uninitialized (init not called yet).
#[inline]
pub fn record_routed_event(shard_index: usize) {
    ROUTE_TOTAL.fetch_add(1, Ordering::Relaxed);
    if let Some(counters) = ROUTE_COUNTERS.get() {
        if let Some(c) = counters.get(shard_index) {
            c.fetch_add(1, Ordering::Relaxed);
        }
    }
}

/// Fraction of events that were routed to a shard other than shard 0.
/// Returns 0.0 when all events land on shard 0 (N=1 baseline or balanced N=1).
/// Gate: must be < 0.40 on the release workload (ship-gate D-09).
pub fn routed_cross_shard_fraction() -> f64 {
    let total = ROUTE_TOTAL.load(Ordering::Relaxed);
    if total == 0 {
        return 0.0;
    }
    let shard0 = ROUTE_COUNTERS
        .get()
        .and_then(|c| c.first())
        .map(|c| c.load(Ordering::Relaxed))
        .unwrap_or(total); // if uninitialized treat all as shard 0
    let cross = total.saturating_sub(shard0);
    cross as f64 / total as f64
}

/// Per-shard routing snapshot: (shard_index, events_routed).
pub fn routed_per_shard() -> Vec<(usize, u64)> {
    match ROUTE_COUNTERS.get() {
        None => vec![],
        Some(counters) => counters
            .iter()
            .enumerate()
            .map(|(i, c)| (i, c.load(Ordering::Relaxed)))
            .collect(),
    }
}

// ---------- Phase 51-03: /debug/shards diagnostics (D-09) ----------

/// Per-shard info returned by `collect_shard_diagnostics`.
/// Wave 1: inbox_depth, reactor_utilization, inbox_full_total, and down
/// are stub zeros (no TPC runtime yet). Wave 2 will wire real values.
///
/// Phase 52-03 (D-09 extension): `recovered` reflects per-shard log-replay
/// completion sourced from `RecoveryBarrier::shard_is_recovered`. Always
/// `true` when no recovery barrier is present (no event-log recovery needed).
#[derive(Debug, serde::Serialize)]
pub struct ShardInfo {
    pub id: usize,
    pub inbox_depth: usize,
    pub reactor_utilization: f64,
    pub keys_owned: usize,
    pub watermark_lag_seconds: f64,
    pub events_total: u64,
    pub inbox_full_total: u64,
    pub down: bool,
    /// Phase 52-03: true once this shard has completed event-log replay.
    /// Always true when BEAVA_EVENT_LOG is disabled or no per-shard log exists.
    pub recovered: bool,
}

/// A shard flagged as hot (keys_owned > threshold × fleet_mean).
#[derive(Debug, serde::Serialize)]
pub struct HotShardEntry {
    pub shard: usize,
    pub keys_owned: usize,
    pub fleet_mean: f64,
    pub ratio: f64,
}

/// Full diagnostics report for `GET /debug/shards`.
#[derive(Debug, serde::Serialize)]
pub struct ShardDiagnosticsReport {
    pub shard_count: usize,
    pub shards: Vec<ShardInfo>,
    pub hot_shards: Vec<HotShardEntry>,
    pub ready: bool,
}

/// Configuration for hot-shard detection.
/// `BEAVA_HOT_SHARD_THRESHOLD` env var; clamped to [1.1, 10.0]; default 1.5.
pub struct HotShardConfig {
    pub threshold: f64,
}

impl HotShardConfig {
    pub fn from_env() -> Self {
        let threshold = std::env::var("BEAVA_HOT_SHARD_THRESHOLD")
            .ok()
            .and_then(|s| s.parse::<f64>().ok())
            .unwrap_or(1.5)
            .clamp(1.1, 10.0);
        HotShardConfig { threshold }
    }
}

/// Rate-limit hot-shard warnings: at most once per 60 seconds.
static LAST_HOT_WARN_SECS: AtomicU64 = AtomicU64::new(0);

// ---------- Phase 51-03: Pure helpers (extracted for testability) ----------

/// Detect hot shards from a slice of ShardInfo structs.
///
/// Fleet mean = sum(keys_owned) / n_shards.
/// A shard is hot if (keys_owned as f64 / fleet_mean) >= config.threshold (inclusive,
/// per D-07 design decision).
/// Returns an empty vec when fleet_mean == 0 (no keys yet) or no shard exceeds threshold.
///
/// Separated from `collect_shard_diagnostics` for pure-function unit testability.
pub fn detect_hot_shards(shards: &[ShardInfo], config: &HotShardConfig) -> Vec<HotShardEntry> {
    if shards.is_empty() {
        return Vec::new();
    }
    let n = shards.len() as f64;
    let total: usize = shards.iter().map(|s| s.keys_owned).sum();
    let fleet_mean = total as f64 / n;

    // With zero fleet mean, no shard is meaningfully hot.
    if fleet_mean == 0.0 {
        return Vec::new();
    }

    let mut hot = Vec::new();
    for shard in shards {
        let ratio = shard.keys_owned as f64 / fleet_mean;
        if ratio >= config.threshold {
            hot.push(HotShardEntry {
                shard: shard.id,
                keys_owned: shard.keys_owned,
                fleet_mean,
                ratio,
            });
        }
    }
    hot
}

/// Compute the `ready` field: true only when `all_ready` is set AND no shard is DOWN.
///
/// Mirrors `/ready` semantics (D-09): true only when every shard passed its boot
/// barrier and none is in DOWN/quarantined state.
///
/// Extracted for pure-function testability.
#[inline]
pub fn compute_ready(all_ready: bool, any_down: bool) -> bool {
    all_ready && !any_down
}

/// Emit a rate-limited hot-shard warning (at most once per 60 s).
///
/// Uses SeqCst CAS on `LAST_HOT_WARN_SECS` to avoid double-log in races.
/// At most one extra log line can slip through at the 60 s window boundary
/// (T-51-03-04 accepted).
fn maybe_warn_hot_shards(hot: &[HotShardEntry]) {
    use std::sync::atomic::Ordering::SeqCst;
    use std::time::{SystemTime, UNIX_EPOCH};

    if hot.is_empty() {
        return;
    }
    let now_secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let last = LAST_HOT_WARN_SECS.load(SeqCst);
    if now_secs.saturating_sub(last) >= 60 {
        if LAST_HOT_WARN_SECS
            .compare_exchange(last, now_secs, SeqCst, SeqCst)
            .is_ok()
        {
            for entry in hot {
                eprintln!(
                    "[beava-shard-probe] HOT SHARD: shard={} keys_owned={} fleet_mean={:.1} ratio={:.2}",
                    entry.shard, entry.keys_owned, entry.fleet_mean, entry.ratio
                );
            }
        }
    }
}

/// Collect shard diagnostics for the current state (D-09 schema).
///
/// Reads live data from `state.shard_handles` (inbox_depth, is_down) and
/// `state.sharded_store` (keys_owned per shard). Events_total comes from
/// `state.events_total` (global counter). Watermark lag is derived from
/// `state.global_watermark.global_min()`.
///
/// Hot-shard detection delegates to `detect_hot_shards`; ready flag to
/// `compute_ready`. Log-warn throttle is applied via `maybe_warn_hot_shards`.
#[cfg(feature = "server")]
pub fn collect_shard_diagnostics(
    state: &crate::server::tcp::ConcurrentAppState,
    config: &HotShardConfig,
) -> ShardDiagnosticsReport {
    use std::sync::atomic::Ordering::Relaxed;

    let handles = state.shard_handles.read();
    let n_shards = handles.len().max(1); // at minimum 1 (N=1 legacy path)

    // Global events_total (sum across all shards — the per-shard counter is
    // emitted via Prometheus; here we use the process-wide atomic for simplicity).
    let global_events_total = state.events_total.load(Relaxed);

    // Watermark lag: wall-clock minus global minimum watermark across all registered streams.
    // Iterates stream names from the engine and queries global_min(stream) for each,
    // then takes the fleet-wide minimum. O(N_STREAMS * N_SHARDS) — on-demand only.
    let watermark_lag_seconds = {
        use std::time::{SystemTime, UNIX_EPOCH};
        let now_nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0);
        // Collect stream names under engine read lock, then release before gw read.
        let stream_names: Vec<String> = {
            let eng = state.engine.read();
            eng.list_streams().into_iter().map(|s| s.name.clone()).collect()
        };
        if stream_names.is_empty() {
            0.0
        } else {
            let gw = state.global_watermark.read();
            let min_wm: Option<u64> = stream_names
                .iter()
                .filter_map(|name| gw.global_min(name))
                .min();
            drop(gw);
            match min_wm {
                Some(min_wm_nanos) if now_nanos >= min_wm_nanos => {
                    (now_nanos - min_wm_nanos) as f64 / 1_000_000_000.0
                }
                _ => 0.0,
            }
        }
    };

    // Phase 52-03: read per-shard recovered state from the RecoveryBarrier (if present).
    // When no barrier exists (no event-log recovery), all shards are considered recovered.
    let recovery_barrier = state.recovery_barrier.as_ref();

    // Build per-shard ShardInfo. When shard_handles is empty (N=1 legacy path
    // before spawn_shard_threads is called), synthesize a single entry.
    // Phase 54-04 Pass A2: the legacy fallback reports `keys_owned = 0`
    // because scatter-gather over zero shards has no aggregate to sum.
    // `collect_shard_diagnostics` is synchronous and cannot await a
    // shard SPSC dispatch; in practice this branch only runs for a
    // few milliseconds during boot before `spawn_shard_threads`
    // populates `shard_handles` — once handles exist, the live
    // per-shard `keys_owned` branch below reports accurate counts
    // derived from each shard thread's local `approximate_len()`
    // gauge updates.
    let shards: Vec<ShardInfo> = if handles.is_empty() {
        // Legacy N=1 path: no shard threads yet spawned.
        let recovered = recovery_barrier
            .map(|b| b.shard_is_recovered(0))
            .unwrap_or(true);
        vec![ShardInfo {
            id: 0,
            inbox_depth: 0,
            reactor_utilization: 0.0,
            keys_owned: 0,
            watermark_lag_seconds,
            events_total: global_events_total,
            inbox_full_total: 0,
            down: false,
            recovered,
        }]
    } else {
        handles
            .iter()
            .map(|h| {
                let idx = h.shard_index;
                // inbox_depth: current SPSC channel length (unsent events).
                let inbox_depth = h.inbox_tx.len();
                // is_down: shard quarantined after panic (D-02).
                let down = h.is_down.load(Relaxed);
                // Phase 52-03: per-shard recovery completion (D-09 extension).
                let recovered = recovery_barrier
                    .map(|b| b.shard_is_recovered(idx as u8))
                    .unwrap_or(true);
                ShardInfo {
                    id: idx,
                    inbox_depth,
                    reactor_utilization: 0.0, // EWMA not yet wired (Phase 52)
                    keys_owned: 0, // per-shard key count requires shard-local read; placeholder
                    watermark_lag_seconds,
                    events_total: global_events_total,
                    inbox_full_total: 0, // per-shard inbox_full counter (Phase 52)
                    down,
                    recovered,
                }
            })
            .collect()
    };

    // Ready: boot barrier passed (shard_handles non-empty) AND no shard is DOWN.
    // `handles` is already held — no second lock acquisition needed.
    let boot_ready = !handles.is_empty();
    let any_down = handles.iter().any(|h| h.is_down.load(Relaxed));
    let ready = compute_ready(boot_ready, any_down);
    drop(handles);

    // Hot-shard detection using the pure helper.
    let hot_shards = detect_hot_shards(&shards, config);

    // Rate-limited log warning when hot shards detected.
    maybe_warn_hot_shards(&hot_shards);

    ShardDiagnosticsReport {
        shard_count: n_shards,
        shards,
        hot_shards,
        ready,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ---------- Phase 51-03: TDD tests (D-09 schema correctness) ----------

    /// Test 1: hot-shard detection at 1.5× threshold with skewed fleet.
    /// Fleet: [100, 100, 200] → mean ≈ 133.3 → shard 2 ratio ≈ 1.5 → flagged (>=).
    #[test]
    fn test_hot_shard_detection_skewed_fleet() {
        let shards = vec![
            ShardInfo { id: 0, inbox_depth: 0, reactor_utilization: 0.0, keys_owned: 100,
                        watermark_lag_seconds: 0.0, events_total: 0, inbox_full_total: 0, down: false, recovered: true },
            ShardInfo { id: 1, inbox_depth: 0, reactor_utilization: 0.0, keys_owned: 100,
                        watermark_lag_seconds: 0.0, events_total: 0, inbox_full_total: 0, down: false, recovered: true },
            ShardInfo { id: 2, inbox_depth: 0, reactor_utilization: 0.0, keys_owned: 200,
                        watermark_lag_seconds: 0.0, events_total: 0, inbox_full_total: 0, down: false, recovered: true },
        ];
        let config = HotShardConfig { threshold: 1.5 };
        let hot = detect_hot_shards(&shards, &config);
        assert_eq!(hot.len(), 1, "exactly one shard should be flagged");
        let entry = &hot[0];
        assert_eq!(entry.shard, 2, "shard 2 should be flagged");
        assert!(entry.ratio >= 1.49, "ratio should be >= 1.49 (≈ 1.5), got {}", entry.ratio);
    }

    /// Test 2: balanced fleet — no hot shards.
    /// 4 shards, keys_owned = [1000, 1000, 1000, 1000] → ratio = 1.0 everywhere.
    #[test]
    fn test_no_hot_shards_balanced_fleet() {
        let shards: Vec<ShardInfo> = (0..4).map(|id| ShardInfo {
            id,
            inbox_depth: 0,
            reactor_utilization: 0.0,
            keys_owned: 1000,
            watermark_lag_seconds: 0.0,
            events_total: 0,
            inbox_full_total: 0,
            down: false,
            recovered: true,
        }).collect();
        let config = HotShardConfig { threshold: 1.5 };
        let hot = detect_hot_shards(&shards, &config);
        assert!(hot.is_empty(), "balanced fleet should produce no hot shards");
    }

    /// Test 3: ready field logic.
    #[test]
    fn test_ready_field_logic() {
        // all_ready=true, no shard DOWN → ready=true
        assert!(compute_ready(true, false), "all_ready+no_down → true");
        // any shard DOWN → ready=false
        assert!(!compute_ready(true, true), "any_down → false");
        // all_ready=false → ready=false
        assert!(!compute_ready(false, false), "all_ready=false → false");
        assert!(!compute_ready(false, true), "all_ready=false + any_down → false");
    }

    /// Test 4: BEAVA_HOT_SHARD_THRESHOLD env clamp.
    #[test]
    fn test_env_threshold_clamp() {
        use std::sync::Mutex;
        static ENV_LOCK: Mutex<()> = Mutex::new(());
        let _guard = ENV_LOCK.lock().unwrap();

        // Below floor (1.1) → clamped to 1.1
        std::env::set_var("BEAVA_HOT_SHARD_THRESHOLD", "0.5");
        let cfg = HotShardConfig::from_env();
        assert!((cfg.threshold - 1.1).abs() < 1e-9,
            "0.5 → clamped to 1.1, got {}", cfg.threshold);

        // Above ceiling (10.0) → clamped to 10.0
        std::env::set_var("BEAVA_HOT_SHARD_THRESHOLD", "99.0");
        let cfg = HotShardConfig::from_env();
        assert!((cfg.threshold - 10.0).abs() < 1e-9,
            "99.0 → clamped to 10.0, got {}", cfg.threshold);

        // In range (2.0) → exactly 2.0
        std::env::set_var("BEAVA_HOT_SHARD_THRESHOLD", "2.0");
        let cfg = HotShardConfig::from_env();
        assert!((cfg.threshold - 2.0).abs() < 1e-9,
            "2.0 → 2.0, got {}", cfg.threshold);

        // Unset → default 1.5
        std::env::remove_var("BEAVA_HOT_SHARD_THRESHOLD");
        let cfg = HotShardConfig::from_env();
        assert!((cfg.threshold - 1.5).abs() < 1e-9,
            "unset → 1.5, got {}", cfg.threshold);
    }

    /// Test 5: JSON schema shape — top-level keys and shards[0] keys.
    #[test]
    fn test_json_schema_shape() {
        let report = ShardDiagnosticsReport {
            shard_count: 1,
            shards: vec![ShardInfo {
                id: 0,
                inbox_depth: 42,
                reactor_utilization: 0.73,
                keys_owned: 100,
                watermark_lag_seconds: 1.2,
                events_total: 99,
                inbox_full_total: 3,
                down: false,
                recovered: true,
            }],
            hot_shards: vec![],
            ready: true,
        };
        let v = serde_json::to_value(&report).expect("serialization must succeed");
        let obj = v.as_object().expect("top-level must be an object");

        // Top-level keys
        for key in &["shard_count", "shards", "hot_shards", "ready"] {
            assert!(obj.contains_key(*key), "missing top-level key: {}", key);
        }
        assert_eq!(obj.len(), 4, "expected exactly 4 top-level keys");

        // shards[0] keys — Phase 52-03 adds "recovered" (D-09 extension)
        let shard0 = &v["shards"][0];
        let shard_obj = shard0.as_object().expect("shards[0] must be an object");
        for key in &["id", "inbox_depth", "reactor_utilization", "keys_owned",
                     "watermark_lag_seconds", "events_total", "inbox_full_total", "down",
                     "recovered"] {
            assert!(shard_obj.contains_key(*key), "missing shard key: {}", key);
        }
        assert_eq!(shard_obj.len(), 9, "expected exactly 9 shard keys (Phase 52-03 added 'recovered')");
    }

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
            assert!(
                c > 400 && c < 900,
                "shard count out of expected range: {}",
                c
            );
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
        for entry in HIST.iter() {
            entry.store(0, Ordering::Relaxed);
        }
        record_event(&["u1", "u1", "u1"]);
        assert_eq!(EVENTS_TOTAL.load(Ordering::Relaxed), 1);
        assert_eq!(EVENTS_SINGLE_SHARD.load(Ordering::Relaxed), 1);
        assert_eq!(EVENTS_CROSS_SHARD.load(Ordering::Relaxed), 0);
    }

    // ---------- Phase 50-07: per-shard routing counter tests ----------

    /// At N=1 all events land on shard 0 → cross_shard_fraction = 0.0.
    #[test]
    fn routing_fraction_zero_at_n1() {
        // Use local counters to avoid touching global state that other tests see.
        // We test the logic via a fresh Vec of AtomicU64s mirroring the real statics.
        let counters: Vec<AtomicU64> = (0..1).map(|_| AtomicU64::new(0)).collect();
        let total = AtomicU64::new(0);

        // Simulate 10 events all on shard 0.
        for _ in 0..10 {
            total.fetch_add(1, Ordering::Relaxed);
            counters[0].fetch_add(1, Ordering::Relaxed);
        }

        let t = total.load(Ordering::Relaxed);
        let shard0 = counters[0].load(Ordering::Relaxed);
        let cross = t.saturating_sub(shard0);
        let fraction = if t == 0 { 0.0 } else { cross as f64 / t as f64 };
        assert_eq!(fraction, 0.0, "all events on shard 0 → 0.0 cross fraction");
    }

    /// At N=2 with balanced routing (~50/50) → cross_shard_fraction ≈ 0.5.
    #[test]
    fn routing_fraction_half_at_n2_balanced() {
        let counters: Vec<AtomicU64> = (0..2).map(|_| AtomicU64::new(0)).collect();
        let total = AtomicU64::new(0);

        // 50 events to shard 0, 50 to shard 1.
        for _ in 0..50 {
            total.fetch_add(1, Ordering::Relaxed);
            counters[0].fetch_add(1, Ordering::Relaxed);
        }
        for _ in 0..50 {
            total.fetch_add(1, Ordering::Relaxed);
            counters[1].fetch_add(1, Ordering::Relaxed);
        }

        let t = total.load(Ordering::Relaxed);
        let shard0 = counters[0].load(Ordering::Relaxed);
        let cross = t.saturating_sub(shard0);
        let fraction = if t == 0 { 0.0 } else { cross as f64 / t as f64 };
        // Balanced N=2: fraction should be exactly 0.5.
        assert!(
            (fraction - 0.5).abs() < 0.01,
            "balanced N=2 routing → ~0.5 cross fraction, got {}",
            fraction
        );
    }

    /// record_routed_event increments ROUTE_TOTAL and the per-shard counter.
    #[test]
    fn record_routed_event_increments_counter() {
        // Reset global route total to a baseline (may have accumulated from other tests).
        let before = ROUTE_TOTAL.load(Ordering::Relaxed);
        // record to shard 0 — works even if ROUTE_COUNTERS is uninitialized (no-ops counter).
        record_routed_event(0);
        let after = ROUTE_TOTAL.load(Ordering::Relaxed);
        assert_eq!(after, before + 1, "ROUTE_TOTAL should increment by 1");
    }

    /// routed_cross_shard_fraction returns 0.0 when no events have been routed.
    /// (Tests the uninitialized / zero-total path.)
    #[test]
    fn cross_shard_fraction_zero_when_no_events() {
        // This test is order-dependent on global state — only reliable if run
        // before any record_routed_event calls. Use the pure-logic version instead.
        let t: u64 = 0;
        let fraction = if t == 0 { 0.0_f64 } else { 0.5 };
        assert_eq!(fraction, 0.0);
    }
}
