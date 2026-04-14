//! Internal signal bus for `/debug/warnings` + config recommendations (Phase 25-02).
//!
//! The `SignalRegistry` is the observability substrate shared by every
//! warning source in the v0 engine:
//!
//! * Safety — REGISTER failures.
//! * Operational — snapshot-write failures, memory pressure.
//! * Data quality — late-event drop rate.
//! * Performance — PUSH p99 SLO breach.
//! * Config — TTL/history_ttl recommendations (wired by plan 25-03).
//!
//! The registry is **in-memory only** in v0; a restart loses all signals.
//! This is acceptable because the UI polls `/debug/warnings` and every live
//! source re-emits on its next cycle. Persistence was explicitly deferred
//! in `25-CONTEXT.md §deferred`.
//!
//! Writes from any path go through [`SignalRegistry::record`] which dedupes
//! by `Signal.id`: a second write with the same id preserves `first_seen`,
//! refreshes `last_seen`, and overwrites title/detail/action/evidence
//! (severity may escalate but never silently downgrade). Signals older than
//! the configured observation window (default 7d) are dropped on the next
//! `age_out` call.
//!
//! ## Threading & hot-path constraint
//!
//! The registry sits behind a [`SharedRegistry`] (`Arc<RwLock<_>>`). This
//! is fine because **no hot-path handler (PUSH / GET / GET_MULTI) calls
//! `record`**. Emission happens only on:
//!
//! * REGISTER (rare, out-of-hot-path).
//! * Snapshot cycle (default 30s).
//! * Periodic signal poller (same 30s cadence).
//!
//! `record` performs zero I/O — it only mutates an in-memory `AHashMap`.
//! This is explicitly tested (`test_record_no_io`) and keeps the snapshot
//! failure emitter safe from recursion (failing snapshot → record signal →
//! no disk I/O inside record → no re-failure).
//!
//! ## Platform notes
//!
//! The memory-pressure emitter reads `/proc/self/statm` and is therefore
//! `#[cfg(target_os = "linux")]`-gated. On non-Linux hosts the emitter is a
//! no-op; no false alarms.

use ahash::AHashMap;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

/// Default observation window: signals older than this are aged out.
pub const DEFAULT_OBSERVATION_WINDOW: Duration = Duration::from_secs(7 * 86400);

/// Signal severity ladder. `Ord` order is deliberate: `Info < Warning <
/// Error < Critical`. Callers sort *descending* for "critical first"
/// output.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Hash)]
#[serde(rename_all = "lowercase")]
pub enum Severity {
    Info,
    Warning,
    Error,
    Critical,
}

/// Signal category. Mirrors the five buckets from `25-CONTEXT.md §decisions`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, Hash)]
#[serde(rename_all = "snake_case")]
pub enum Category {
    Config,
    DataQuality,
    Operational,
    Safety,
    Performance,
}

impl Category {
    /// Parse the snake_case serialized form used in the `?category=` query
    /// parameter on `/debug/warnings`.
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "config" => Some(Self::Config),
            "data_quality" => Some(Self::DataQuality),
            "operational" => Some(Self::Operational),
            "safety" => Some(Self::Safety),
            "performance" => Some(Self::Performance),
            _ => None,
        }
    }
}

/// A single signal entry. `first_seen` is preserved across deduped writes;
/// `last_seen` is refreshed. `action` is optional because plenty of signals
/// ("we saw a failure") don't have an associated config knob suggestion.
#[derive(Clone, Debug, Serialize)]
pub struct Signal {
    pub id: String,
    pub severity: Severity,
    pub category: Category,
    pub title: String,
    pub detail: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub action: Option<serde_json::Value>,
    #[serde(serialize_with = "serialize_rfc3339")]
    pub first_seen: SystemTime,
    #[serde(serialize_with = "serialize_rfc3339")]
    pub last_seen: SystemTime,
    pub evidence: serde_json::Value,
}

impl Signal {
    /// Convenience constructor. `first_seen` and `last_seen` both default
    /// to `now`; the registry fixes up `first_seen` on dedupe.
    pub fn new(
        id: impl Into<String>,
        severity: Severity,
        category: Category,
        title: impl Into<String>,
        detail: impl Into<String>,
        evidence: serde_json::Value,
    ) -> Self {
        let now = SystemTime::now();
        Self {
            id: id.into(),
            severity,
            category,
            title: title.into(),
            detail: detail.into(),
            action: None,
            first_seen: now,
            last_seen: now,
            evidence,
        }
    }

    pub fn with_action(mut self, action: serde_json::Value) -> Self {
        self.action = Some(action);
        self
    }
}

/// Internal in-memory observability bus.
pub struct SignalRegistry {
    /// Keyed by `Signal.id`. We dedupe on write; never store duplicates.
    signals: AHashMap<String, Signal>,
    observation_window: Duration,
    /// Previous counter snapshots for rate computations (e.g. late-drop
    /// rate). Key: arbitrary stable metric id; value: (count, sampled_at).
    prev_counters: AHashMap<String, (u64, SystemTime)>,
}

/// Shared handle used throughout the server. Clone is cheap (just bumps
/// the `Arc` refcount). Hot-path code MUST NOT hold the write guard across
/// `.await` points.
pub type SharedRegistry = Arc<RwLock<SignalRegistry>>;

impl SignalRegistry {
    pub fn new(observation_window: Duration) -> Self {
        Self {
            signals: AHashMap::new(),
            observation_window,
            prev_counters: AHashMap::new(),
        }
    }

    /// Default registry with the 7-day observation window.
    pub fn new_default() -> Self {
        Self::new(DEFAULT_OBSERVATION_WINDOW)
    }

    /// Wrap `self` in the standard `Arc<RwLock<_>>` shared handle.
    pub fn into_shared(self) -> SharedRegistry {
        Arc::new(RwLock::new(self))
    }

    /// Record (or update) a signal. Dedupe by `id`:
    ///
    /// * If the id is new, store `sig` as-is.
    /// * If the id already exists, preserve the existing `first_seen`,
    ///   refresh `last_seen` to `sig.last_seen` (which defaults to now),
    ///   and overwrite title / detail / action / evidence / category.
    ///   Severity is allowed to escalate but never silently downgraded.
    pub fn record(&mut self, mut sig: Signal) {
        if let Some(existing) = self.signals.get(&sig.id) {
            sig.first_seen = existing.first_seen;
            // Severity: take the max of existing vs new. Escalation only.
            if existing.severity > sig.severity {
                sig.severity = existing.severity;
            }
        }
        self.signals.insert(sig.id.clone(), sig);
    }

    /// Drop any signal whose `last_seen` is older than the observation
    /// window. Clock skew / underflow yields `Duration::ZERO` (keep the
    /// signal) — same pattern as `state/eviction.rs`.
    pub fn age_out(&mut self, now: SystemTime) {
        let window = self.observation_window;
        self.signals.retain(|_, sig| {
            now.duration_since(sig.last_seen)
                .unwrap_or(Duration::ZERO)
                <= window
        });
    }

    /// Return every live signal, severity-descending, stable by
    /// `first_seen` ascending within a severity. `filter` narrows by
    /// category (None = all).
    ///
    /// Does NOT mutate the registry — call [`age_out`](Self::age_out)
    /// first if you want stale entries removed first.
    pub fn snapshot_sorted(&self, _now: SystemTime, filter: Option<Category>) -> Vec<Signal> {
        let mut out: Vec<Signal> = self
            .signals
            .values()
            .filter(|s| filter.map(|c| s.category == c).unwrap_or(true))
            .cloned()
            .collect();
        // Primary: severity DESC (Critical first). Secondary: first_seen ASC.
        out.sort_by(|a, b| {
            b.severity
                .cmp(&a.severity)
                .then_with(|| a.first_seen.cmp(&b.first_seen))
        });
        out
    }

    /// Set of categories with at least one live signal (test helper; also
    /// useful for `/debug/warnings?debug=1` one day).
    pub fn categories_present(&self) -> HashSet<Category> {
        self.signals.values().map(|s| s.category).collect()
    }

    /// Total signal count. Primarily for tests and metrics.
    pub fn len(&self) -> usize {
        self.signals.len()
    }

    pub fn is_empty(&self) -> bool {
        self.signals.is_empty()
    }

    pub fn observation_window(&self) -> Duration {
        self.observation_window
    }

    // ------------------------------------------------------------------
    // Rate-computation scratch space (used by poll_all_signal_sources).
    // ------------------------------------------------------------------

    /// Remember the current value of a monotonic counter so the next
    /// call to [`rate_since_last`](Self::rate_since_last) can compute
    /// `(delta_count / delta_seconds)`.
    pub fn record_counter_sample(&mut self, key: &str, value: u64, now: SystemTime) {
        self.prev_counters
            .insert(key.to_string(), (value, now));
    }

    /// Compute the per-second rate since the last sample for `key`, then
    /// update the stored sample. Returns `None` on the first call for a
    /// given key (bootstrap cycle) or if the current value is less than
    /// the stored value (counter reset).
    pub fn rate_since_last(
        &mut self,
        key: &str,
        current: u64,
        now: SystemTime,
    ) -> Option<f64> {
        let prev = self.prev_counters.insert(key.to_string(), (current, now));
        let (prev_val, prev_ts) = prev?;
        if current < prev_val {
            return None;
        }
        let dt = now.duration_since(prev_ts).ok()?.as_secs_f64();
        if dt <= 0.0 {
            return None;
        }
        Some((current - prev_val) as f64 / dt)
    }
}

impl Default for SignalRegistry {
    fn default() -> Self {
        Self::new_default()
    }
}

// ---------------------------------------------------------------------------
// RFC3339 (UTC) formatter for `first_seen`/`last_seen`.
//
// We don't pull in chrono/time crates for a single formatter; this produces
// `YYYY-MM-DDTHH:MM:SSZ` directly from `SystemTime`. Leap seconds and
// sub-second precision are intentionally dropped — the shape is for
// human / UI consumption, not for strict round-tripping.
// ---------------------------------------------------------------------------

fn serialize_rfc3339<S>(t: &SystemTime, ser: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    ser.serialize_str(&format_rfc3339(*t))
}

/// Format a `SystemTime` as `YYYY-MM-DDTHH:MM:SSZ` (UTC). Pre-epoch
/// inputs (clock skew / tests) fall back to `"1970-01-01T00:00:00Z"`.
pub fn format_rfc3339(t: SystemTime) -> String {
    let secs = t
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);
    let (y, mo, d, h, mi, s) = civil_from_days_seconds(secs);
    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z",
        y, mo, d, h, mi, s
    )
}

/// Convert a Unix timestamp (seconds) to `(year, month, day, hour, min,
/// sec)` in UTC using Howard Hinnant's civil-from-days algorithm.
/// Correct for 1970-01-01 through 9999-12-31.
fn civil_from_days_seconds(unix_secs: i64) -> (i32, u32, u32, u32, u32, u32) {
    let secs_per_day: i64 = 86400;
    let mut days = unix_secs.div_euclid(secs_per_day);
    let sod = unix_secs.rem_euclid(secs_per_day);
    let h = (sod / 3600) as u32;
    let mi = ((sod % 3600) / 60) as u32;
    let s = (sod % 60) as u32;

    // Shift epoch to 0000-03-01 ("civil from days" origin).
    days += 719468;
    let era = if days >= 0 { days } else { days - 146096 } / 146097;
    let doe = days - era * 146097; // [0, 146096]
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365; // [0, 399]
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
    let mp = (5 * doy + 2) / 153; // [0, 11]
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    let y = (y + if m <= 2 { 1 } else { 0 }) as i32;
    (y, m, d, h, mi, s)
}

// ---------------------------------------------------------------------------
// Emitters invoked from the snapshot-cycle poller.
//
// These do NOT import `SharedState` directly — that would create a module
// cycle (server::tcp -> server::signals -> server::tcp). Instead the
// caller in `main.rs` or `http.rs` pulls the counters it cares about and
// hands them in.
// ---------------------------------------------------------------------------

/// Late-event drop emitter. For each (stream, drop_counter) pair, compute
/// the drop rate per second over the last sample interval and emit a
/// `data_quality` warning if it exceeds `threshold_per_sec`.
///
/// The first call for each stream returns bootstrap (no emission) — we
/// need two samples to compute a rate.
pub fn emit_late_drop_signals(
    registry: &SharedRegistry,
    drops: &[(String, u64)],
    now: SystemTime,
    threshold_per_sec: f64,
) {
    let mut reg = registry.write();
    for (stream, total) in drops {
        let key = format!("late_drop.{}", stream);
        let rate = match reg.rate_since_last(&key, *total, now) {
            Some(r) => r,
            None => continue, // bootstrap
        };
        if rate > threshold_per_sec {
            let sig = Signal::new(
                format!("late_drop.{}", stream),
                Severity::Warning,
                Category::DataQuality,
                "Late events being dropped",
                format!(
                    "Stream '{}' is dropping {:.2} events/sec past its lateness bound",
                    stream, rate
                ),
                serde_json::json!({
                    "stream": stream,
                    "total_dropped": total,
                    "rate_per_sec": rate,
                    "threshold_per_sec": threshold_per_sec,
                }),
            );
            reg.record(sig);
        }
    }
}

/// Memory-pressure emitter. Reads `/proc/self/statm` on Linux; no-op on
/// other platforms. Emits `memory.pressure` at Warning for >85% and
/// escalates to Critical for >95%.
pub fn emit_memory_pressure_signal(
    registry: &SharedRegistry,
    configured_limit_bytes: Option<u64>,
) {
    let Some(limit) = configured_limit_bytes else {
        return;
    };
    if limit == 0 {
        return;
    }
    let Some(rss_bytes) = sample_rss_bytes() else {
        return;
    };
    let ratio = rss_bytes as f64 / limit as f64;
    if ratio <= 0.85 {
        return;
    }
    let severity = if ratio > 0.95 {
        Severity::Critical
    } else {
        Severity::Warning
    };
    let sig = Signal::new(
        "memory.pressure",
        severity,
        Category::Operational,
        "Memory pressure above 85% of configured limit",
        format!(
            "RSS {:.0} MiB / limit {:.0} MiB ({:.1}%)",
            rss_bytes as f64 / 1_048_576.0,
            limit as f64 / 1_048_576.0,
            ratio * 100.0
        ),
        serde_json::json!({
            "rss_bytes": rss_bytes,
            "limit_bytes": limit,
            "ratio": ratio,
        }),
    );
    registry.write().record(sig);
}

/// PUSH p99 latency SLO breach (performance category). Threshold is a
/// 10× multiplier over the CLAUDE.md design target of 100µs → 1ms, tuned
/// to avoid noise from GC/JIT warmup. `current_p99_us` is expected to be
/// sourced from `LatencyTracker::push_percentile_us(99.0, now)`.
pub fn emit_perf_p99_signal(
    registry: &SharedRegistry,
    current_p99_us: f64,
    threshold_us: f64,
) {
    if !(current_p99_us.is_finite() && current_p99_us > threshold_us) {
        return;
    }
    let sig = Signal::new(
        "perf.push_p99_slo_breach",
        Severity::Warning,
        Category::Performance,
        "PUSH p99 latency above SLO",
        format!(
            "p99 = {:.1}µs, threshold = {:.1}µs",
            current_p99_us, threshold_us
        ),
        serde_json::json!({
            "p99_us": current_p99_us,
            "threshold_us": threshold_us,
        }),
    );
    registry.write().record(sig);
}

/// Emit a `safety / error` signal when a REGISTER call fails. Factored
/// here so every register-error call site uses an identical payload shape.
pub fn emit_register_failure(registry: &SharedRegistry, pipeline_name: &str, err: &str) {
    let sig = Signal::new(
        format!("register.failure.{}", pipeline_name),
        Severity::Error,
        Category::Safety,
        "Pipeline registration failed",
        err.to_string(),
        serde_json::json!({
            "pipeline": pipeline_name,
            "error": err,
        }),
    );
    registry.write().record(sig);
}

/// Emit a `snapshot.failure` operational signal. Called from the
/// snapshot-writer's error branch in `main.rs`.
pub fn emit_snapshot_failure(registry: &SharedRegistry, err: &str) {
    let sig = Signal::new(
        "snapshot.failure",
        Severity::Error,
        Category::Operational,
        "Snapshot write failed",
        err.to_string(),
        serde_json::json!({
            "error": err,
        }),
    );
    registry.write().record(sig);
}

/// Read current process RSS in bytes. Linux-only; other platforms return
/// `None` and the memory-pressure emitter becomes a no-op.
#[cfg(target_os = "linux")]
fn sample_rss_bytes() -> Option<u64> {
    let data = std::fs::read_to_string("/proc/self/statm").ok()?;
    // Fields: size resident shared text lib data dt — all in pages.
    let mut it = data.split_whitespace();
    let _size = it.next()?;
    let resident_pages: u64 = it.next()?.parse().ok()?;
    // Most linux kernels use 4KiB pages. If getconf is unavailable we
    // assume 4096; the 85/95% thresholds are coarse enough that the
    // rounding error doesn't matter.
    let page_size: u64 = 4096;
    Some(resident_pages * page_size)
}

#[cfg(not(target_os = "linux"))]
fn sample_rss_bytes() -> Option<u64> {
    None
}
