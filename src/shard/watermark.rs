//! Per-shard watermark state (v1.2 TPC Wave 1 — D-04/D-05/D-06).
//!
//! Replaces `WatermarkTracker` (event_time.rs) with a single-writer AHashMap-backed impl.
//! At N=1: global watermark for any stream = shard-0's WatermarkState value (identity).
//! Wave 3 (Phase 51) adds the lazy global-publish mechanism atop this — purely additive.
//!
//! API mirrors WatermarkTracker exactly so call sites require minimal changes:
//! observe / watermark / observed_max / propagate_stateless / propagate_join /
//! attach_to_table / set_lateness / iter_streams / last_event_time.

use ahash::AHashMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::engine::event_time::WATERMARK_LATENESS;

/// Per-shard watermark state. Single writer — no DashMap, no atomics.
///
/// Replaces `WatermarkTracker` (DashMap<String, AtomicU64>) with plain AHashMap.
/// Safe because each Shard is owned by exactly one thread (Wave 1: same tokio thread
/// as today; Wave 2: pinned shard thread with message-passing inbox).
#[derive(Debug)]
pub struct WatermarkState {
    /// Monotonic max event time per stream (nanos since UNIX_EPOCH).
    observed_max: AHashMap<String, u64>,
    /// Per-stream watermark lateness override.
    watermark_lateness: AHashMap<String, Duration>,
    /// Last event time per stream (for lag metric — separate from max).
    last_event_time: AHashMap<String, u64>,
}

impl WatermarkState {
    pub fn new() -> Self {
        WatermarkState {
            observed_max: AHashMap::new(),
            watermark_lateness: AHashMap::new(),
            last_event_time: AHashMap::new(),
        }
    }

    fn nanos_to_system_time(nanos: u64) -> SystemTime {
        UNIX_EPOCH + Duration::from_nanos(nanos)
    }

    fn system_time_to_nanos(t: SystemTime) -> u64 {
        t.duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos().min(u64::MAX as u128) as u64)
            .unwrap_or(0)
    }

    /// Look up the per-stream lateness override, falling back to the global default.
    pub fn lateness_for(&self, stream: &str) -> Duration {
        self.watermark_lateness
            .get(stream)
            .copied()
            .unwrap_or(WATERMARK_LATENESS)
    }

    /// Record event time for stream. Monotonic max — never decreases.
    pub fn observe(&mut self, stream: &str, event_time: SystemTime) {
        let nanos = Self::system_time_to_nanos(event_time);
        // last_event_time — last-writer-wins.
        *self
            .last_event_time
            .entry(stream.to_string())
            .or_insert(0) = nanos;
        // observed_max — monotonic.
        let entry = self.observed_max.entry(stream.to_string()).or_insert(0);
        if nanos > *entry {
            *entry = nanos;
        }
    }

    /// Current watermark: `observed_max(stream) - lateness_for(stream)`.
    /// Returns `None` if the stream has never been observed.
    pub fn watermark(&self, stream: &str) -> Option<SystemTime> {
        let max_nanos = self.observed_max.get(stream).copied()?;
        if max_nanos == 0 {
            return None;
        }
        let max = Self::nanos_to_system_time(max_nanos);
        let lateness = self.lateness_for(stream);
        // Clamp to UNIX_EPOCH to avoid a pre-epoch watermark.
        Some(match max.duration_since(UNIX_EPOCH) {
            Ok(d) if d >= lateness => max - lateness,
            _ => UNIX_EPOCH,
        })
    }

    /// Observed max event time for stream (no lateness applied).
    pub fn observed_max(&self, stream: &str) -> Option<SystemTime> {
        let nanos = self.observed_max.get(stream).copied()?;
        if nanos == 0 {
            return None;
        }
        Some(Self::nanos_to_system_time(nanos))
    }

    /// Most recent event_time observed on stream (not necessarily the max).
    pub fn last_event_time(&self, stream: &str) -> Option<SystemTime> {
        let nanos = self.last_event_time.get(stream).copied()?;
        if nanos == 0 {
            return None;
        }
        Some(Self::nanos_to_system_time(nanos))
    }

    /// Set per-stream watermark lateness override.
    pub fn set_lateness(&mut self, stream: &str, lateness: Duration) {
        self.watermark_lateness.insert(stream.to_string(), lateness);
    }

    /// γ: stateless op — output stream inherits the input stream's current watermark.
    pub fn propagate_stateless(&mut self, from: &str, to: &str) {
        if let Some(&max) = self.observed_max.get(from) {
            if max > 0 {
                let to_entry = self.observed_max.entry(to.to_string()).or_insert(0);
                if max > *to_entry {
                    *to_entry = max;
                }
            }
        }
    }

    /// γ: join — output watermark = min(left_max, right_max). Monotonic on output.
    pub fn propagate_join(&mut self, left: &str, right: &str, output: &str) {
        let l = self.observed_max.get(left).copied().unwrap_or(0);
        let r = self.observed_max.get(right).copied().unwrap_or(0);
        if l > 0 && r > 0 {
            let min_max = l.min(r);
            let entry = self.observed_max.entry(output.to_string()).or_insert(0);
            if min_max > *entry {
                *entry = min_max;
            }
        }
    }

    /// γ: aggregation — the output Table inherits the source stream's watermark.
    pub fn attach_to_table(&mut self, source_stream: &str, output_table: &str) {
        if let Some(&max) = self.observed_max.get(source_stream) {
            if max > 0 {
                let entry = self.observed_max.entry(output_table.to_string()).or_insert(0);
                if max > *entry {
                    *entry = max;
                }
            }
        }
    }

    /// Propagate watermark from one stream to another (for derived streams).
    pub fn propagate_from(&mut self, from: &str, to: &str) {
        self.propagate_stateless(from, to);
    }

    /// Merge join watermark: min(left, right) → join stream.
    pub fn merge_join(&mut self, left: &str, right: &str, join_stream: &str) {
        self.propagate_join(left, right, join_stream);
    }

    /// Copy watermark from another stream (for fork/replica).
    pub fn copy_from(&mut self, source: &str, dest: &str) {
        if let Some(&max) = self.observed_max.get(source) {
            self.observed_max.insert(dest.to_string(), max);
        }
    }

    /// List every stream that has an observed watermark. Used by debug endpoints.
    pub fn iter_streams(&self) -> Vec<(String, SystemTime)> {
        self.observed_max
            .iter()
            .filter(|(_, &v)| v > 0)
            .map(|(k, &v)| (k.clone(), Self::nanos_to_system_time(v)))
            .collect()
    }

    /// Iterate over (stream, observed_max) pairs.
    pub fn iter_max(&self) -> impl Iterator<Item = (&str, SystemTime)> {
        self.observed_max
            .iter()
            .filter(|(_, &v)| v > 0)
            .map(|(k, &v)| (k.as_str(), Self::nanos_to_system_time(v)))
    }
}

impl Default for WatermarkState {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sec(s: u64) -> SystemTime {
        UNIX_EPOCH + Duration::from_secs(s)
    }

    #[test]
    fn no_observation_returns_none() {
        let wm = WatermarkState::new();
        assert_eq!(wm.observed_max("s"), None);
        assert_eq!(wm.watermark("s"), None);
    }

    #[test]
    fn observe_then_observed_max() {
        let mut wm = WatermarkState::new();
        wm.observe("s", sec(100));
        assert_eq!(wm.observed_max("s"), Some(sec(100)));
    }

    #[test]
    fn monotonic_max_does_not_regress() {
        let mut wm = WatermarkState::new();
        wm.observe("s", sec(110));
        wm.observe("s", sec(80)); // older — must not change max
        assert_eq!(wm.observed_max("s"), Some(sec(110)));
    }

    #[test]
    fn watermark_is_max_minus_5s_default_lateness() {
        let mut wm = WatermarkState::new();
        wm.observe("s", sec(110));
        // Default lateness is 5 seconds
        assert_eq!(wm.watermark("s"), Some(sec(105)));
    }

    #[test]
    fn watermark_absent_for_fresh_stream() {
        let wm = WatermarkState::new();
        assert!(wm.watermark("never_seen").is_none());
    }

    #[test]
    fn watermark_underflow_clamps_to_epoch() {
        let mut wm = WatermarkState::new();
        // event_time < 5s — lateness would underflow; clamps to UNIX_EPOCH.
        wm.observe("s", sec(3));
        let wm_t = wm.watermark("s").unwrap();
        assert!(wm_t >= UNIX_EPOCH, "watermark must not underflow UNIX_EPOCH");
    }

    /// GOLDEN REGRESSION TEST (D-04): N=1 Wave 1 behavior must be identical to
    /// the pre-Wave-1 WatermarkTracker for all observe/query sequences.
    #[test]
    fn golden_n1_watermark_sequence() {
        let mut wm = WatermarkState::new();
        wm.observe("s", sec(100));
        wm.observe("s", sec(110));
        assert_eq!(wm.observed_max("s"), Some(sec(110)));
        assert_eq!(wm.watermark("s"), Some(sec(105))); // 110 - 5 default lateness
        assert_eq!(wm.watermark("other"), None);
    }

    #[test]
    fn join_watermark_is_min_of_both_sides() {
        let mut wm = WatermarkState::new();
        wm.observe("left", sec(100));
        wm.observe("right", sec(80));
        wm.merge_join("left", "right", "join");
        assert_eq!(wm.observed_max("join"), Some(sec(80)));
    }

    #[test]
    fn propagate_from_advances_derived() {
        let mut wm = WatermarkState::new();
        wm.observe("source", sec(200));
        wm.propagate_from("source", "derived");
        assert_eq!(wm.observed_max("derived"), Some(sec(200)));
    }

    #[test]
    fn propagate_stateless_copies_watermark() {
        let mut wm = WatermarkState::new();
        wm.observe("in", sec(100));
        wm.propagate_stateless("in", "out");
        assert_eq!(wm.watermark("out"), Some(sec(95)));
    }

    #[test]
    fn propagate_join_takes_min() {
        let mut wm = WatermarkState::new();
        wm.observe("l", sec(100));
        wm.observe("r", sec(200));
        wm.propagate_join("l", "r", "j");
        assert_eq!(wm.observed_max("j"), Some(sec(100)));
        assert_eq!(wm.watermark("j"), Some(sec(95)));
    }

    #[test]
    fn attach_to_table_inherits_stream_watermark() {
        let mut wm = WatermarkState::new();
        wm.observe("s", sec(110));
        wm.attach_to_table("s", "agg_out");
        assert_eq!(wm.watermark("agg_out"), Some(sec(105)));
    }
}
