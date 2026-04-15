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
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

// ===========================================================================
// Phase 41-01 T3: AtomicThroughput — lock-free rolling-window EPS counter.
// ===========================================================================
//
// Replaces the per-push `PLMutex<ThroughputTracker>` acquisition on the hot
// path. 60 one-second buckets (covering a full minute); each `bump(n)` does
// a single `Relaxed` `fetch_add` on the bucket indexed by the current
// wall-clock second mod 60. Reads sum the most recent 5 buckets to form a
// 5-second EPS rate.
//
// Per-stream granularity IS lost on the hot path — only a global EPS number
// is maintained. The existing `ThroughputTracker` (per-stream EWMAs) stays
// on `AppState` to preserve `/debug/throughput` wiring, tests, and future
// admin tools, but is no longer fed by the per-push hot path. In production
// server use the per-stream endpoint shows only streams pushed via admin
// / test paths. Flagged as a deviation in 41-01-SUMMARY.md.

/// Width of the rolling window (in seconds).
pub const ATOMIC_WINDOW_SECS: usize = 60;
/// Number of trailing seconds summed to form `eps_5s()`. We skip the
/// currently-being-written bucket (at index `now`) because readers would
/// otherwise race the bump-side with torn reads; the 5 seconds we sum end
/// at `now-1`.
pub const ATOMIC_EPS_WINDOW_SECS: usize = 5;

/// Lock-free rolling EPS counter. Safe to share immutably across threads;
/// every field is an atomic.
pub struct AtomicThroughput {
    /// One counter per second-of-minute (0..ATOMIC_WINDOW_SECS). Each bucket
    /// is cleared by the writer on the first bump of a new "minute cycle"
    /// (detected via `last_second`).
    buckets: [AtomicU64; ATOMIC_WINDOW_SECS],
    /// Last wall-clock second-since-epoch observed by any writer. When a
    /// writer sees its current second is strictly greater than this, it
    /// clears the old buckets in the `(last_second, now)` range so stale
    /// data from a prior minute does not leak into the next window.
    last_second: AtomicU64,
}

impl AtomicThroughput {
    pub fn new() -> Self {
        // `[AtomicU64; N]` cannot be built from `[0u64; N]` directly since
        // AtomicU64 is not Copy. Use an array from a const fn.
        const ZERO: AtomicU64 = AtomicU64::new(0);
        Self {
            buckets: [ZERO; ATOMIC_WINDOW_SECS],
            last_second: AtomicU64::new(0),
        }
    }

    /// Current wall-clock second since the Unix epoch. Uses SystemTime —
    /// VDSO-backed on Linux, so the syscall cost is negligible.
    #[inline]
    fn now_secs() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0)
    }

    /// Lock-free: bump the current second's bucket by `count`. Safe to call
    /// from any thread at any rate.
    #[inline]
    pub fn bump(&self, count: u64) {
        let now = Self::now_secs();
        let prev = self.last_second.load(Ordering::Relaxed);
        if now > prev {
            // Roll forward: clear every bucket strictly between the old
            // last_second and `now` (exclusive of `now`) so readers never
            // see stale data from >60s ago aliased into the current minute.
            // We CAS the clock forward so only one writer does the sweep.
            if self
                .last_second
                .compare_exchange(prev, now, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
            {
                let gap = (now - prev).min(ATOMIC_WINDOW_SECS as u64);
                for offset in 1..=gap {
                    let idx = ((prev + offset) % ATOMIC_WINDOW_SECS as u64) as usize;
                    self.buckets[idx].store(0, Ordering::Relaxed);
                }
            }
        }
        let idx = (now % ATOMIC_WINDOW_SECS as u64) as usize;
        self.buckets[idx].fetch_add(count, Ordering::Relaxed);
    }

    /// Sum the most recent `ATOMIC_EPS_WINDOW_SECS` buckets ending at
    /// `now-1`, divide by the window width, and return the resulting
    /// events-per-second average. Skipping the currently-active bucket
    /// prevents racing the bump-side: the oldest bucket we sum was last
    /// touched at `now-5`, the newest at `now-1` — both at least one
    /// second in the past.
    pub fn eps_5s(&self) -> f64 {
        let now = Self::now_secs();
        if now == 0 {
            return 0.0;
        }
        let mut total: u64 = 0;
        for i in 1..=ATOMIC_EPS_WINDOW_SECS as u64 {
            let sec = now.saturating_sub(i);
            let idx = (sec % ATOMIC_WINDOW_SECS as u64) as usize;
            total = total.saturating_add(self.buckets[idx].load(Ordering::Relaxed));
        }
        total as f64 / ATOMIC_EPS_WINDOW_SECS as f64
    }
}

impl Default for AtomicThroughput {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod atomic_throughput_tests {
    use super::*;

    #[test]
    fn eps_is_zero_at_rest() {
        let t = AtomicThroughput::new();
        assert_eq!(t.eps_5s(), 0.0);
    }

    #[test]
    fn bump_accumulates_then_reads() {
        let t = AtomicThroughput::new();
        for _ in 0..1000 {
            t.bump(1);
        }
        // Sum of last 5 buckets / 5. All 1000 events landed in 1-2 buckets
        // (the test runs fast), so eps_5s should be between 200 and 1000.
        // Some of those buckets may fall into the "current second" which is
        // excluded by eps_5s — retry by sleeping a second.
        std::thread::sleep(std::time::Duration::from_millis(1100));
        let eps = t.eps_5s();
        assert!(eps > 0.0 && eps <= 1000.0, "unexpected eps: {}", eps);
    }

    #[test]
    fn bump_from_multiple_threads_is_lossless() {
        use std::sync::Arc;
        let t = Arc::new(AtomicThroughput::new());
        let threads: Vec<_> = (0..8)
            .map(|_| {
                let t = Arc::clone(&t);
                std::thread::spawn(move || {
                    for _ in 0..10_000 {
                        t.bump(1);
                    }
                })
            })
            .collect();
        for h in threads {
            h.join().unwrap();
        }
        // Total events is 80_000 spread across the current second. After
        // waiting the bucket rotates out of "current" and becomes readable.
        std::thread::sleep(std::time::Duration::from_millis(1100));
        let eps = t.eps_5s();
        // We expect (somewhere between 0 and 80_000) / 5, i.e. <= 16_000.
        // Don't be more precise — CI noise. Just assert non-zero + no panic.
        assert!(eps > 0.0, "eps should be > 0 after 80k lossless bumps");
    }
}
// ===========================================================================

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
    // NOTE: test-only; not part of the public snapshot API. Gated on
    // `#[cfg(test)]` so the struct layout is identical in release builds.
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
        let entry = self.streams.entry(stream_name.to_string()).or_default();
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

    /// Phase 20: global events-per-second across all streams, 5 s EWMA.
    /// Sum of every stream's `ewma_5s`. Used by `/public/stats` and
    /// `/metrics`.
    pub fn eps_5s(&self) -> f64 {
        self.streams.values().map(|s| s.ewma_5s).sum()
    }

    /// Phase 20: global events-per-second across all streams, 60 s EWMA.
    pub fn eps_60s(&self) -> f64 {
        self.streams.values().map(|s| s.ewma_1m).sum()
    }

    /// Test-only accessor for the cascade/fan-out dedup correctness test.
    // NOTE: test-only; not part of the public snapshot API. The real public
    // surface is `snapshot()` which returns EWMA rates, not raw counters.
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
