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
#[derive(Debug)]
pub struct SignalRegistry {
    /// Keyed by `Signal.id`. We dedupe on write; never store duplicates.
    signals: AHashMap<String, Signal>,
    observation_window: Duration,
    /// Previous counter snapshots for rate computations (e.g. late-drop
    /// rate). Key: arbitrary stable metric id; value: (count, sampled_at).
    prev_counters: AHashMap<String, (u64, SystemTime)>,
    /// Phase 56 D-C1 — structured list of cross-shard-join warnings,
    /// surfaced as a sibling field to `warnings` on `GET /debug/warnings`.
    /// Dedupe by `join_id` (T-56-03-01). Exposed read-only via
    /// `cross_shard_joins_snapshot`.
    cross_shard_joins:
        Vec<crate::engine::join_validator::CrossShardJoinWarning>,
    /// Phase 57 Wave 3 (TPC-CORR-10) — structured list of retraction-beyond-
    /// history warnings, surfaced as a sibling field to `warnings` on
    /// `GET /debug/warnings`. 60-second dedupe by
    /// `(operator, reason_class)`. Exposed read-only via
    /// `retraction_beyond_history_snapshot`.
    retraction_beyond_history: Vec<RetractionBeyondHistoryWarning>,
}

/// Phase 57 Wave 3 (TPC-CORR-10): surface emitted when a retraction is
/// skipped because the contributing event is older than
/// `watermark - history_ttl`. Dedupe'd at 60s by `(operator, reason_class)`
/// — count aggregates within-window bursts so dashboards don't flood.
#[derive(Clone, Debug, Serialize)]
pub struct RetractionBeyondHistoryWarning {
    /// Downstream stream being retracted (operator label). Matches the
    /// `operator` metric label on `beava_retraction_beyond_history_total`.
    pub operator: String,
    /// Retract reason class — `"source_table_delete"` / `"entity_tombstone"` /
    /// `"primary_event_retract"`. Matches the `reason` metric label on
    /// `beava_retractions_sent_total`.
    pub reason_class: String,
    /// Unix epoch millis of the first retraction in this dedupe window.
    pub first_seen_ms: u64,
    /// Count of beyond-history retractions aggregated into this window.
    /// Bumped on every emission within the 60s window.
    pub count: u64,
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
            cross_shard_joins: Vec::new(),
            retraction_beyond_history: Vec::new(),
        }
    }

    /// Phase 57 Wave 3 (TPC-CORR-10): dedupe'd push of a retraction-beyond-
    /// history warning. Keyed on `(operator, reason_class)`. If an existing
    /// entry within the 60s window matches, bumps `count` and refreshes
    /// nothing else; otherwise appends a fresh entry with `count = 1`.
    ///
    /// This mirrors the Phase 51 / 56-03 dedup cadence — 60s is the
    /// operational-dashboard refresh interval, long enough to collapse
    /// per-event burst noise, short enough for an operator to see each
    /// distinct mode of late-retraction within a few refreshes.
    pub fn push_retraction_beyond_history(
        &mut self,
        operator: &str,
        reason_class: &str,
    ) {
        const DEDUPE_WINDOW_MS: u64 = 60_000;
        let now_ms: u64 = SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        // Find an existing entry within the dedupe window.
        for existing in self.retraction_beyond_history.iter_mut() {
            if existing.operator == operator
                && existing.reason_class == reason_class
                && now_ms.saturating_sub(existing.first_seen_ms) < DEDUPE_WINDOW_MS
            {
                existing.count = existing.count.saturating_add(1);
                return;
            }
        }
        self.retraction_beyond_history.push(RetractionBeyondHistoryWarning {
            operator: operator.to_string(),
            reason_class: reason_class.to_string(),
            first_seen_ms: now_ms,
            count: 1,
        });
    }

    /// Phase 57 Wave 3 (TPC-CORR-10): read-only snapshot of the current
    /// retraction-beyond-history warning list, consumed by the
    /// `/debug/warnings` HTTP handler.
    pub fn retraction_beyond_history_snapshot(
        &self,
    ) -> Vec<RetractionBeyondHistoryWarning> {
        self.retraction_beyond_history.clone()
    }

    /// Phase 56 D-C1 — dedupe-aware push of a `CrossShardJoinWarning`.
    /// Only the first occurrence of a `join_id` is retained (T-56-03-01).
    pub fn push_cross_shard_join(
        &mut self,
        warning: crate::engine::join_validator::CrossShardJoinWarning,
    ) {
        if self
            .cross_shard_joins
            .iter()
            .any(|w| w.join_id == warning.join_id)
        {
            return;
        }
        self.cross_shard_joins.push(warning);
    }

    /// Phase 56 D-C1 — read-only snapshot of the current cross-shard-join
    /// warning list, consumed by the `/debug/warnings` HTTP handler.
    pub fn cross_shard_joins_snapshot(
        &self,
    ) -> Vec<crate::engine::join_validator::CrossShardJoinWarning> {
        self.cross_shard_joins.clone()
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
        self.signals
            .retain(|_, sig| now.duration_since(sig.last_seen).unwrap_or(Duration::ZERO) <= window);
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
        self.prev_counters.insert(key.to_string(), (value, now));
    }

    /// Compute the per-second rate since the last sample for `key`, then
    /// update the stored sample. Returns `None` on the first call for a
    /// given key (bootstrap cycle) or if the current value is less than
    /// the stored value (counter reset).
    pub fn rate_since_last(&mut self, key: &str, current: u64, now: SystemTime) -> Option<f64> {
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
    format!("{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z", y, mo, d, h, mi, s)
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
pub fn emit_memory_pressure_signal(registry: &SharedRegistry, configured_limit_bytes: Option<u64>) {
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
pub fn emit_perf_p99_signal(registry: &SharedRegistry, current_p99_us: f64, threshold_us: f64) {
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
/// Phase 51-04: emit a JoinShardKeyMismatch signal (D-12 locked message).
/// Severity=Error, Category=Safety. Signal id is stable per stream pair so
/// repeated mis-registration dedupes in the registry.
///
/// **Phase 56 D-C2 note:** this emitter is retained for back-compat. Its
/// Phase-51 caller in `register()` has been swapped to
/// `emit_cross_shard_join_warning` (non-fatal). Any external embedder still
/// using the deprecated `JoinShardKeyMismatch` type can still call this fn.
#[allow(deprecated)]
pub fn emit_join_shard_key_mismatch(
    registry: &SharedRegistry,
    mismatch: &crate::engine::join_validator::JoinShardKeyMismatch,
) {
    let id = format!(
        "join.shard_key_mismatch.{}.{}",
        mismatch.stream_a, mismatch.stream_b
    );
    let sig = Signal::new(
        id,
        Severity::Error,
        Category::Safety,
        "Join shard_key mismatch",
        mismatch.message.clone(),
        serde_json::json!({
            "stream_a": mismatch.stream_a,
            "stream_b": mismatch.stream_b,
            "key_a": mismatch.key_a,
            "key_b": mismatch.key_b,
            "suggested_common": mismatch.suggested_common,
        }),
    );
    registry.write().record(sig);
}

/// Phase 56 D-B4 / D-C1 — emit a non-fatal `CrossShardJoinWarning` to the
/// signal registry. Dual-wire surface:
///
/// 1. Record a `Category::Safety` / `Severity::Warning` signal (shape
///    mirrors `emit_join_shard_key_mismatch` so the unified `/debug/warnings`
///    feed already renders the warning).
/// 2. Push onto the dedicated `cross_shard_joins` Vec (dedupe by
///    `join_id`) so the `/debug/warnings` handler can surface the
///    structured array (D-C1 contract).
///
/// Severity is Warning (not Error) because the runtime path (Wave 1's
/// `ssj_insert_at_shard`) routes both sides to `hash(join.on) % N` and
/// produces correct output — the warning is a perf / co-location hint, not
/// a correctness breach.
/// Phase 57 Wave 3 (TPC-CORR-10): emit a dedupe'd retraction-beyond-history
/// warning. Dual-wire surface mirroring `emit_cross_shard_join_warning`:
///
/// 1. stderr log-line (`[WARN] RetractionBeyondHistory ...`) — grep-target
///    for runtime observability.
/// 2. Registry-side dedupe'd push via
///    `push_retraction_beyond_history`, which backs the
///    `/debug/warnings.retraction_beyond_history` JSON array.
///
/// The `RETRACTION_BEYOND_HISTORY_TOTAL` metric counter is NOT bumped
/// here — that already happens at the single emission site in
/// `pipeline.rs::retract_downstream_at_shard` (same-shard fast path) and
/// `shard/thread.rs::RetractDownstream` (cross-shard dispatch arm) so a
/// double-bump is avoided.
pub fn emit_retraction_beyond_history_warning(
    registry: &SharedRegistry,
    operator: &str,
    reason_class: &str,
) {
    eprintln!(
        "[WARN] beava::retract RetractionBeyondHistory: operator={} reason_class={}",
        operator, reason_class
    );
    registry
        .write()
        .push_retraction_beyond_history(operator, reason_class);
}

pub fn emit_cross_shard_join_warning(
    registry: &SharedRegistry,
    warning: &crate::engine::join_validator::CrossShardJoinWarning,
) {
    let id = format!("crossshard_join.{}", warning.join_id);
    let sig = Signal::new(
        id,
        Severity::Warning,
        Category::Safety,
        "Cross-shard join registered (perf hint)",
        warning.message.clone(),
        serde_json::json!({
            "join_id": warning.join_id,
            "stream_a": warning.stream_a,
            "stream_b": warning.stream_b,
            "left_shard_key": warning.left_shard_key,
            "right_shard_key": warning.right_shard_key,
            "on_field": warning.on_field,
            "perf_note": warning.perf_note,
        }),
    );
    let mut reg = registry.write();
    reg.record(sig);
    reg.push_cross_shard_join(warning.clone());
}

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

/// Plan 25-03: fan `recommend_config` output into the signal registry as
/// `Category::Config` / `Severity::Info` warnings. One signal per
/// recommendation; id is stable per knob so re-observation dedupes against
/// the previous cycle instead of creating duplicates.
///
/// The signal carries a `config_change` action so Debug-UI consumers can
/// render a copy-paste button directly from `/debug/warnings` without
/// re-fetching `/debug/config-recommendations`.
///
/// If a previously-recommended knob no longer crosses threshold, the old
/// signal is allowed to age out via `age_out()`; we do not proactively
/// resolve config signals mid-cycle because the recommendation feed itself
/// is the source of truth and a disappearing knob simply stops refreshing
/// `last_seen`.
pub fn emit_config_recommendations(
    registry: &SharedRegistry,
    recs: &[crate::engine::recommend::ConfigRecommendation],
) {
    if recs.is_empty() {
        return;
    }
    let mut reg = registry.write();
    for r in recs {
        // Signal id deliberately stable per knob. We want Table
        // `UserProfile.ttl` recommendations to dedupe across polling cycles.
        let id = format!("config.{}", r.knob);
        // Anchor `evidence_url` at the existing `/debug/config-recommendations`
        // endpoint with a fragment matching the knob, so the UI can scroll
        // the recommendation into view when the operator clicks through
        // from the warnings pane.
        let evidence = serde_json::json!({
            "knob": r.knob,
            "current": r.current,
            "suggested": r.suggested,
            "confidence": r.confidence,
            "reason": r.reason,
            "copy_paste": r.copy_paste,
            "evidence_url": format!("/debug/config-recommendations#{}", r.knob),
        });
        let title = if r.knob.ends_with(".ttl") {
            "TTL too short".to_string()
        } else if r.knob.ends_with(".history_ttl") {
            "history_ttl too short".to_string()
        } else {
            format!("Config recommendation: {}", r.knob)
        };
        let sig = Signal::new(
            id,
            Severity::Info,
            Category::Config,
            title,
            r.reason.clone(),
            evidence,
        )
        .with_action(serde_json::json!({
            "type": "config_change",
            "knob": r.knob,
            "current": r.current,
            "suggested": r.suggested,
            "copy_paste": r.copy_paste,
        }));
        reg.record(sig);
    }
}

/// Phase 27-02: operational/warning signal when a replica subscriber is
/// dropped because its bounded mpsc channel (cap 10_000) filled up. The
/// drain task is not reading fast enough — back-pressure is on the
/// subscriber's side, not the ingest path (the push never blocks).
pub fn emit_replica_drop_backpressure(registry: &SharedRegistry, conn_id: u64) {
    let sig = Signal::new(
        format!("replica.drop.backpressure.{}", conn_id),
        Severity::Warning,
        Category::Operational,
        "Replica subscriber dropped (backpressure)",
        format!(
            "Subscriber conn_id={} dropped because its 10_000-slot \
             notification buffer filled. The client is not draining fast \
             enough; the server does NOT block ingest on slow subscribers.",
            conn_id
        ),
        serde_json::json!({
            "conn_id": conn_id,
            "reason": "backpressure",
            "buffer_capacity": 10_000,
        }),
    );
    registry.write().record(sig);
}

/// Phase 27-02: safety/error signal when a replica subscriber (or
/// snapshot fetch) fails admin-token authentication. `peer` is the
/// remote socket address as a string (or `"unknown"` if not available).
pub fn emit_replica_auth_failure(registry: &SharedRegistry, peer: &str) {
    let sig = Signal::new(
        format!("replica.auth.failure.{}", peer),
        Severity::Error,
        Category::Safety,
        "Replica auth failure",
        format!(
            "Peer {} failed admin-token check on a replica opcode \
             (SUBSCRIBE / SNAPSHOT_FETCH).",
            peer
        ),
        serde_json::json!({
            "peer": peer,
            "reason": "admin_token_mismatch",
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

/// Phase 50-06 (D-11/D-12, TPC-DX-02): emit a ShardKeyMissingWarning for
/// `stream_name` at most ONCE per stream (deduped by stable signal id).
///
/// No-op if `shard_count <= 1` (D-12: silent at N=1 so single-shard operators
/// never see this warning).
///
/// Called at stream registration time when the stream has no declared shard_key
/// and `BEAVA_SHARDS > 1`. The warning fires once per stream, not per event.
pub fn emit_shard_key_missing_warning(
    registry: &SharedRegistry,
    stream_name: &str,
    shard_count: usize,
) {
    if shard_count <= 1 {
        return; // D-12: silent at N=1
    }
    let id = format!("shard_key_missing:{}", stream_name);
    let detail = format!(
        "ShardKeyMissingWarning: stream \"{}\" has no shard_key; \
         events distribute randomly across shards (aggregations reshuffle via \
         cross-shard state ops). For better locality and lower cross-shard \
         traffic, declare @bv.stream(shard_key=\"<fieldname>\").",
        stream_name
    );
    let sig = Signal::new(
        id,
        Severity::Warning,
        Category::Operational,
        format!("ShardKeyMissingWarning: stream \"{}\"", stream_name),
        detail,
        serde_json::json!({ "stream": stream_name, "shard_count": shard_count }),
    );
    registry.write().record(sig);
}

#[cfg(test)]
mod shard_key_warning_tests {
    use super::*;

    #[test]
    fn warning_silent_at_n1() {
        let reg = SignalRegistry::new_default().into_shared();
        emit_shard_key_missing_warning(&reg, "my_stream", 1);
        assert!(reg.read().is_empty(), "no warning should fire at N=1 (D-12)");
    }

    #[test]
    fn warning_fires_at_n2() {
        let reg = SignalRegistry::new_default().into_shared();
        emit_shard_key_missing_warning(&reg, "orders", 2);
        assert!(!reg.read().is_empty(), "warning should fire at N=2");
    }

    #[test]
    fn warning_deduped_on_second_call() {
        let reg = SignalRegistry::new_default().into_shared();
        emit_shard_key_missing_warning(&reg, "orders", 2);
        emit_shard_key_missing_warning(&reg, "orders", 2);
        // Registry dedupes by id — still only 1 signal after 2 calls.
        assert_eq!(reg.read().len(), 1, "second call should dedupe");
    }
}
