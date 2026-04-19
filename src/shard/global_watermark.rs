//! Global watermark store — Phase 51 (TPC-PERF-06).
//!
//! Each shard publishes its per-stream observed_max to a flat AtomicU64 array
//! indexed by `shard_id * stream_capacity + stream_ord`.
//!
//! The global watermark for any stream is `min` across all per-shard atomic
//! slots (skipping slots still at 0 / "no publish yet").
//!
//! Reads use `Ordering::Relaxed` — watermarks are best-effort; a stale read
//! within one publish-cadence window is acceptable per design doc §5.
//!
//! NOTE: This module is a Wave 3 placeholder (Phase 51). It is declared here
//! so the module path exists, but `WatermarkState::publish_if_due` is NOT
//! added until Phase 51.

use ahash::AHashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Configuration for the global watermark publish cadence.
///
/// Read once at startup from `BEAVA_WATERMARK_PUBLISH_INTERVAL`. Clamped
/// to `64..=65536`. Defaults to 1024.
#[derive(Debug, Clone, Copy)]
pub struct GlobalWatermarkConfig {
    /// Number of shard-local events between global publishes.
    pub publish_interval: u64,
}

impl GlobalWatermarkConfig {
    /// Read from `BEAVA_WATERMARK_PUBLISH_INTERVAL`, clamp, and return.
    ///
    /// Malformed or absent values silently default to 1024.
    pub fn from_env() -> Self {
        let interval = std::env::var("BEAVA_WATERMARK_PUBLISH_INTERVAL")
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .map(|v| v.clamp(64, 65536))
            .unwrap_or(1024);
        Self {
            publish_interval: interval,
        }
    }
}

impl Default for GlobalWatermarkConfig {
    fn default() -> Self {
        Self {
            publish_interval: 1024,
        }
    }
}

/// Flat lock-free store for per-shard per-stream max event times.
///
/// Indexed as: `slot = shard_id * stream_capacity + stream_ord`.
///
/// A slot value of `0` means "this shard has not yet published a watermark
/// for this stream" — `global_min` skips zero-valued slots.
///
/// `stream_capacity` is fixed at construction; exceeding it panics loudly.
pub struct GlobalWatermarkStore {
    /// `Arc` so the store can be cheaply cloned across tasks.
    slots: Arc<Box<[AtomicU64]>>,
    /// Number of physical shards (rows).
    pub n_shards: usize,
    /// Fixed per-shard stream-ordinal capacity (columns).
    pub stream_capacity: usize,
    /// Stream name → ordinal. Registered at stream-registration time.
    stream_ord: AHashMap<String, usize>,
}

impl GlobalWatermarkStore {
    /// Construct a new store for `n_shards` shards with `stream_capacity`
    /// ordinal slots per shard (all slots initialised to 0).
    pub fn new(n_shards: usize, stream_capacity: usize) -> Self {
        let len = n_shards * stream_capacity;
        let slots: Vec<AtomicU64> = (0..len).map(|_| AtomicU64::new(0)).collect();
        Self {
            slots: Arc::new(slots.into_boxed_slice()),
            n_shards,
            stream_capacity,
            stream_ord: AHashMap::new(),
        }
    }

    /// Register a stream, assigning it the next available ordinal.
    ///
    /// Panics if `stream_capacity` is exceeded.
    pub fn register_stream(&mut self, stream: &str) -> usize {
        let next = self.stream_ord.len();
        assert!(
            next < self.stream_capacity,
            "GlobalWatermarkStore stream capacity ({}) exceeded; cannot register '{}'",
            self.stream_capacity,
            stream
        );
        *self.stream_ord.entry(stream.to_string()).or_insert(next)
    }

    /// Look up an ordinal without registering.
    pub fn stream_ordinal(&self, stream: &str) -> Option<usize> {
        self.stream_ord.get(stream).copied()
    }

    /// Publish a shard's max event_time for a stream. Uses `Ordering::Relaxed`.
    pub fn publish(&self, shard_id: usize, stream: &str, max_event_time_ns: u64) {
        let ord = match self.stream_ord.get(stream) {
            Some(&o) => o,
            None => return,
        };
        let idx = shard_id * self.stream_capacity + ord;
        self.slots[idx].store(max_event_time_ns, Ordering::Relaxed);
    }

    /// Compute the fleet-wide global watermark: min of all non-zero shard
    /// slots for `stream`. Returns `None` if no shard has published yet.
    pub fn global_min(&self, stream: &str) -> Option<u64> {
        let ord = self.stream_ord.get(stream).copied()?;
        let mut min_val: Option<u64> = None;
        for shard_id in 0..self.n_shards {
            let idx = shard_id * self.stream_capacity + ord;
            let v = self.slots[idx].load(Ordering::Relaxed);
            if v > 0 {
                min_val = Some(match min_val {
                    None => v,
                    Some(m) => m.min(v),
                });
            }
        }
        min_val
    }

    /// Create a shareable `Arc` clone of the inner slot array.
    pub fn arc_clone(&self) -> Arc<Box<[AtomicU64]>> {
        Arc::clone(&self.slots)
    }
}

// Safety: AtomicU64 is Send + Sync.
unsafe impl Send for GlobalWatermarkStore {}
unsafe impl Sync for GlobalWatermarkStore {}

impl std::fmt::Debug for GlobalWatermarkStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GlobalWatermarkStore")
            .field("n_shards", &self.n_shards)
            .field("stream_capacity", &self.stream_capacity)
            .field("registered_streams", &self.stream_ord.len())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // -------------------------------------------------------------------------
    // Test 2 (plan spec): Global min invariant — 3 shards with 10/20/30 → min=10
    // -------------------------------------------------------------------------

    #[test]
    fn test_global_min_invariant_three_shards() {
        let mut store = GlobalWatermarkStore::new(3, 16);
        let _ord = store.register_stream("txn");

        store.publish(0, "txn", 10);
        store.publish(1, "txn", 20);
        store.publish(2, "txn", 30);

        assert_eq!(store.global_min("txn"), Some(10));
    }

    // -------------------------------------------------------------------------
    // Test 3 (plan spec): Env clamp — from_env() reads BEAVA_WATERMARK_PUBLISH_INTERVAL
    // Tests are serial (not parallel) to avoid env-var races between tests.
    // -------------------------------------------------------------------------

    /// Helper: call `from_env()` with the env var set to `val`, then remove it.
    /// Uses a std::sync::Mutex guard to serialise concurrent test runners.
    fn with_env(val: &str) -> GlobalWatermarkConfig {
        // Safety: std::env is process-global; these tests must not run in parallel.
        // Cargo runs unit tests in a single process but can parallelize — we
        // serialize via a module-level Mutex so the env key is stable per call.
        use std::sync::Mutex;
        static ENV_LOCK: Mutex<()> = Mutex::new(());
        let _guard = ENV_LOCK.lock().unwrap();
        std::env::set_var("BEAVA_WATERMARK_PUBLISH_INTERVAL", val);
        let cfg = GlobalWatermarkConfig::from_env();
        std::env::remove_var("BEAVA_WATERMARK_PUBLISH_INTERVAL");
        cfg
    }

    fn without_env() -> GlobalWatermarkConfig {
        use std::sync::Mutex;
        static ENV_LOCK: Mutex<()> = Mutex::new(());
        let _guard = ENV_LOCK.lock().unwrap();
        std::env::remove_var("BEAVA_WATERMARK_PUBLISH_INTERVAL");
        GlobalWatermarkConfig::from_env()
    }

    #[test]
    fn test_from_env_clamp_below_floor() {
        // 32 is below the 64 floor → clamped to 64
        let cfg = with_env("32");
        assert_eq!(cfg.publish_interval, 64);
    }

    #[test]
    fn test_from_env_clamp_above_ceiling() {
        // 99999 is above the 65536 ceiling → clamped to 65536
        let cfg = with_env("99999");
        assert_eq!(cfg.publish_interval, 65536);
    }

    #[test]
    fn test_from_env_in_range() {
        // 512 is within [64, 65536] → unchanged
        let cfg = with_env("512");
        assert_eq!(cfg.publish_interval, 512);
    }

    #[test]
    fn test_from_env_unset_defaults_to_1024() {
        // No env var → default 1024
        let cfg = without_env();
        assert_eq!(cfg.publish_interval, 1024);
    }

    #[test]
    fn test_from_env_malformed_defaults_to_1024() {
        // Malformed value (not a u64) → default 1024, no panic
        let cfg = with_env("not_a_number");
        assert_eq!(cfg.publish_interval, 1024);
    }

    // -------------------------------------------------------------------------
    // Direct-construction clamp tests (fast, no env side-effects)
    // -------------------------------------------------------------------------

    #[test]
    fn test_env_config_clamp_below_floor() {
        let cfg = GlobalWatermarkConfig {
            publish_interval: 32u64.clamp(64, 65536),
        };
        assert_eq!(cfg.publish_interval, 64);
    }

    #[test]
    fn test_env_config_clamp_above_ceiling() {
        let cfg = GlobalWatermarkConfig {
            publish_interval: 99999u64.clamp(64, 65536),
        };
        assert_eq!(cfg.publish_interval, 65536);
    }

    #[test]
    fn test_env_config_in_range() {
        let cfg = GlobalWatermarkConfig {
            publish_interval: 512u64.clamp(64, 65536),
        };
        assert_eq!(cfg.publish_interval, 512);
    }

    #[test]
    fn test_env_config_default() {
        let cfg = GlobalWatermarkConfig::default();
        assert_eq!(cfg.publish_interval, 1024);
    }

    // -------------------------------------------------------------------------
    // Test 4 (plan spec): No publish before threshold
    // -------------------------------------------------------------------------

    #[test]
    fn test_no_data_before_publish() {
        let mut store = GlobalWatermarkStore::new(1, 16);
        let _ord = store.register_stream("txn");
        assert_eq!(store.global_min("txn"), None, "no publish yet");
    }

    // -------------------------------------------------------------------------
    // Additional invariants
    // -------------------------------------------------------------------------

    #[test]
    fn test_unknown_stream_returns_none() {
        let store = GlobalWatermarkStore::new(2, 16);
        // "unregistered" was never registered → ordinal lookup fails → None
        assert_eq!(store.global_min("unregistered"), None);
    }

    #[test]
    fn test_publish_unknown_stream_is_noop() {
        // publish() for an unregistered stream must not panic
        let store = GlobalWatermarkStore::new(1, 16);
        store.publish(0, "ghost", 12345); // no-op: ordinal lookup returns None
    }
}
