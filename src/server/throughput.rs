//! Per-stream throughput tracker with EWMA over 5 s / 60 s / 300 s windows.
//!
//! Used by `GET /debug/throughput` (Plan 03) to report live messages/sec.
//! Updated by the Push arm of `handle_sync_command` in `src/server/tcp.rs`.
//!
//! Correctness contract (RESEARCH §Pitfall 4): a single PUSH may touch the
//! primary stream PLUS cascade targets PLUS fan-out targets. The tracker MUST
//! count each stream at most ONCE per push. Use `bump_unique` (see below)
//! which dedupes via a caller-provided iterator.

use ahash::AHashMap;
use std::time::Instant;

/// Time constants (seconds) for the three EWMAs.
/// Chosen to match CONTEXT.md's 5s / 1m / 5m window labels.
const TAU_5S: f64 = 5.0;
const TAU_1M: f64 = 60.0;
const TAU_5M: f64 = 300.0;

/// Per-stream rolling throughput state.
#[derive(Debug, Default, Clone, Copy)]
pub struct StreamThroughput {
    /// Timestamp of the most recent bump (None before the first event).
    pub last_update: Option<Instant>,
    /// EWMA (events per second) over a ~5 s time constant.
    pub ewma_5s: f64,
    /// EWMA over a ~60 s time constant.
    pub ewma_1m: f64,
    /// EWMA over a ~300 s time constant.
    pub ewma_5m: f64,
    /// Test-only counter: total number of single bumps observed since the
    /// tracker was created. Not exposed via snapshot(); only used by the
    /// correctness unit tests to prove dedup semantics.
    #[cfg(test)]
    pending_total: u64,
}

/// Per-stream EWMA tracker.
#[derive(Debug, Default)]
pub struct ThroughputTracker {
    streams: AHashMap<String, StreamThroughput>,
}

impl ThroughputTracker {
    pub fn new() -> Self {
        Self::default()
    }

    /// Bump the counter for a single stream. Callers that touch multiple
    /// streams in one push MUST use `bump_unique` instead to avoid double
    /// counting when cascade + fan-out paths overlap (RESEARCH §Pitfall 4).
    pub fn bump(&mut self, stream_name: &str, now: Instant) {
        let entry = self
            .streams
            .entry(stream_name.to_string())
            .or_insert_with(StreamThroughput::default);
        Self::fold_event(entry, now);
    }

    /// Bump the counter ONCE per unique stream name in the iterator. Duplicate
    /// entries (e.g. the primary stream listed again in cascade_targets)
    /// are silently skipped. This is the canonical call site from
    /// `handle_sync_command`'s Push arm.
    pub fn bump_unique<'a, I: IntoIterator<Item = &'a str>>(
        &mut self,
        stream_names: I,
        now: Instant,
    ) {
        // std::collections::HashSet is fine here -- small N (<= 1 + cascade + fan-out),
        // and we need `.contains()` before inserting.
        let mut seen: std::collections::HashSet<&str> = std::collections::HashSet::new();
        for name in stream_names {
            if seen.insert(name) {
                self.bump(name, now);
            }
        }
    }

    /// Fold one event into a stream's EWMAs using standard time-variable
    /// alpha-mixing. For each time constant `tau`, the per-step mixing weight
    /// is `alpha = 1 - exp(-dt / tau)` and the update is
    /// `ewma += alpha * (instantaneous - ewma)`, where the instantaneous rate
    /// is `1 / dt` events per second. At steady-state arrival rate `r`, this
    /// converges to `ewma = r` — unlike the naive `ewma * exp(-dt/tau) + 1/dt`
    /// recurrence, which converges to `r / (1 - exp(-1/(r*tau)))` and
    /// over-reports rates by roughly a factor of `r*tau` at high rates
    /// (Phase 10 review WR-01).
    ///
    /// First-ever event initializes `last_update` and leaves the EWMAs at
    /// their default 0.0 — we cannot compute an instantaneous rate without a
    /// prior inter-arrival time.
    fn fold_event(entry: &mut StreamThroughput, now: Instant) {
        #[cfg(test)]
        {
            entry.pending_total += 1;
        }

        match entry.last_update {
            None => {
                // First event: cannot compute an instantaneous rate (dt is
                // undefined). Leave EWMAs at their default 0.0; the NEXT
                // event will measure the inter-arrival time.
                entry.last_update = Some(now);
            }
            Some(prev) => {
                let dt = now.saturating_duration_since(prev).as_secs_f64();
                if dt <= 0.0 {
                    // Two bumps in the same Instant -- leave EWMAs untouched
                    // and update last_update so the next real dt captures
                    // this burst. Guards against division by zero (RESEARCH
                    // §Pattern 3: "never compute EWMA with dt = 0").
                    entry.last_update = Some(now);
                    return;
                }
                let instantaneous = 1.0 / dt;
                let alpha_5s = 1.0 - (-dt / TAU_5S).exp();
                let alpha_1m = 1.0 - (-dt / TAU_1M).exp();
                let alpha_5m = 1.0 - (-dt / TAU_5M).exp();
                entry.ewma_5s += alpha_5s * (instantaneous - entry.ewma_5s);
                entry.ewma_1m += alpha_1m * (instantaneous - entry.ewma_1m);
                entry.ewma_5m += alpha_5m * (instantaneous - entry.ewma_5m);
                entry.last_update = Some(now);
            }
        }
    }

    /// Decay every stream's EWMAs to `now`. Used by `/debug/throughput` just
    /// before reading so idle streams report declining rates even when no
    /// recent push has happened to drive an update.
    pub fn decay_all(&mut self, now: Instant) {
        for entry in self.streams.values_mut() {
            if let Some(prev) = entry.last_update {
                let dt = now.saturating_duration_since(prev).as_secs_f64();
                if dt > 0.0 {
                    entry.ewma_5s *= (-dt / TAU_5S).exp();
                    entry.ewma_1m *= (-dt / TAU_1M).exp();
                    entry.ewma_5m *= (-dt / TAU_5M).exp();
                    entry.last_update = Some(now);
                }
            }
        }
    }

    /// Snapshot every stream's current EWMAs as `(name, state)` pairs.
    /// Consumed by `/debug/throughput` in Plan 03.
    pub fn snapshot(&self) -> Vec<(String, StreamThroughput)> {
        self.streams.iter().map(|(k, v)| (k.clone(), *v)).collect()
    }

    /// Test-only accessor for the cascade/fan-out dedup correctness test.
    #[cfg(test)]
    pub(crate) fn pending_total_for_test(&self, stream_name: &str) -> u64 {
        self.streams
            .get(stream_name)
            .map(|s| s.pending_total)
            .unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn bump_increments_pending_and_folds_into_ewma() {
        let mut t = ThroughputTracker::new();
        let start = Instant::now();
        t.bump("A", start);
        t.bump("A", start + Duration::from_secs(1));
        let snap = t.snapshot();
        assert_eq!(snap.len(), 1);
        let (_, s) = &snap[0];
        assert!(
            s.ewma_5s > 0.0,
            "ewma_5s should be > 0 after 1s inter-arrival"
        );
        assert!(s.ewma_1m > 0.0);
        assert!(s.ewma_5m > 0.0);
    }

    #[test]
    fn decay_all_reduces_idle_streams_toward_zero() {
        let mut t = ThroughputTracker::new();
        let start = Instant::now();
        // Burst of bumps close together to build up EWMA values.
        for i in 0..10 {
            t.bump("A", start + Duration::from_millis(i * 100));
        }
        let before = t.snapshot()[0].1;
        assert!(before.ewma_5s > 0.0);

        // Jump 60 seconds into the future and decay everything.
        t.decay_all(start + Duration::from_secs(60) + Duration::from_millis(900));
        let after = t.snapshot()[0].1;

        assert!(
            after.ewma_5s < 0.01,
            "ewma_5s should be near zero after 60s idle: {}",
            after.ewma_5s
        );
        assert!(
            after.ewma_1m < before.ewma_1m,
            "ewma_1m should have decayed"
        );
        assert!(
            after.ewma_5m < before.ewma_5m,
            "ewma_5m should have decayed"
        );
        // Longer time constant decays more slowly; after 60s, 5m EWMA should
        // retain relatively more of its value than 1m EWMA.
        let ratio_1m = after.ewma_1m / before.ewma_1m;
        let ratio_5m = after.ewma_5m / before.ewma_5m;
        assert!(
            ratio_5m > ratio_1m,
            "5m EWMA should decay slower than 1m EWMA"
        );
    }

    #[test]
    fn snapshot_returns_all_tracked_streams() {
        let mut t = ThroughputTracker::new();
        let now = Instant::now();
        t.bump("A", now);
        t.bump("B", now);
        t.bump("C", now);
        assert_eq!(t.snapshot().len(), 3);
    }

    #[test]
    fn does_not_double_count_cascade() {
        // This is the exact regression test called out in RESEARCH §Pitfall 4
        // and VALIDATION.md: a single push that touches primary + 2 cascade
        // targets must bump each stream exactly ONCE.
        let mut t = ThroughputTracker::new();
        let now = Instant::now();
        let touched = ["Transactions", "Alerts", "FraudScore"];
        t.bump_unique(touched.iter().copied(), now);
        assert_eq!(t.pending_total_for_test("Transactions"), 1);
        assert_eq!(t.pending_total_for_test("Alerts"), 1);
        assert_eq!(t.pending_total_for_test("FraudScore"), 1);
    }

    #[test]
    fn bump_unique_deduplicates_repeated_targets() {
        let mut t = ThroughputTracker::new();
        let now = Instant::now();
        let with_dupes = ["A", "A", "B", "B", "B"];
        t.bump_unique(with_dupes.iter().copied(), now);
        assert_eq!(t.pending_total_for_test("A"), 1);
        assert_eq!(t.pending_total_for_test("B"), 1);
    }

    #[test]
    fn first_bump_initializes_without_panic() {
        let mut t = ThroughputTracker::new();
        let now = Instant::now();
        // Two bumps at the same Instant must NOT divide by zero.
        t.bump("A", now);
        t.bump("A", now);
        // Third bump at same Instant also safe.
        t.bump("A", now);
        // Implicit: no panic.
        assert_eq!(t.snapshot().len(), 1);
    }

    #[test]
    fn ewma_calibrates_to_steady_state_rate() {
        // Phase 10 review WR-01 regression: the old fold_event formula
        // (`ewma * exp(-dt/tau) + 1/dt`) converged to `r / (1 - exp(-1/(r*tau)))`
        // which is wildly inflated at high rates. The corrected alpha-mixing
        // formula converges to the actual arrival rate `r` (events/sec).
        //
        // We drive 2000 events spaced exactly 10 ms apart — a steady state of
        // 100 events/sec — and cover ~20 s of simulated wall time. That is
        // roughly 4 time constants for the 5 s EWMA, which converges to
        // `100 * (1 - exp(-4)) ≈ 98.17`. We assert the realised value lands
        // within ±20 % of the true rate (80–120 band), which is easy to hit
        // with the correct math and impossible with the old formula.
        use std::collections::HashSet;
        use std::time::Duration;

        let mut tracker = ThroughputTracker::new();
        let start = Instant::now();
        // 2000 events, exactly 10 ms apart → steady state r = 100 ev/s.
        for i in 0..2000u64 {
            let now = start + Duration::from_millis(i * 10);
            let mut touched: HashSet<&str> = HashSet::new();
            touched.insert("stream_a");
            tracker.bump_unique(touched.into_iter(), now);
        }
        // No decay step: we want to observe the EWMA at the last fold point.
        let snap = tracker.snapshot();
        let (_, s) = snap
            .iter()
            .find(|(name, _)| name == "stream_a")
            .expect("stream_a must be tracked");
        // With tau_5s = 5s and ~20s of steady input, the 5 s EWMA should be
        // well within the 80-120 band for a true 100 ev/s stream.
        assert!(
            s.ewma_5s > 80.0 && s.ewma_5s < 120.0,
            "ewma_5s {} not within 80-120 band for 100 ev/s input",
            s.ewma_5s
        );
    }
}
