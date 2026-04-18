//! Event-time parsing and per-stream watermark tracking (Phase 24-04).
//!
//! Streamlet's v0 correctness model is event-time primary, wall-clock
//! fallback. Every PUSH / PUSH_TABLE / DELETE_TABLE payload is scanned
//! for the reserved `_event_time` JSON field; if present it is parsed
//! into a `SystemTime` and used as the event's event-time. If absent
//! or unparseable, the server's wall-clock arrival time is used as a
//! transparent fallback (documented in CONTEXT.md §Event-time wire
//! format).
//!
//! # Watermark model
//!
//! Per-stream watermark = `max(event_time observed) − 5s`. The
//! `WATERMARK_LATENESS` constant is locked at 5 seconds for v0; per-
//! stream tunable lateness ships post-v0 (see Phase 24 CONTEXT.md §Late
//! event handling). Events whose `event_time < watermark` are dropped
//! by the TCP dispatcher with a `beava_late_events_dropped_total{stream}`
//! counter increment; the counter is exported through the existing
//! `/metrics` endpoint.
//!
//! # γ propagation (wire boundaries only)
//!
//! Watermarks propagate at join / aggregation boundaries; stateless ops
//! pass through. The helpers here (`propagate_stateless`,
//! `propagate_join`, `attach_to_table`) are called from
//! `src/engine/pipeline.rs::push_with_cascade_internal` at each cascade
//! step.

use dashmap::DashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Convert a `SystemTime` to nanoseconds since UNIX_EPOCH. Values before
/// the epoch are clamped to 0. Saturates at `u64::MAX` for times beyond
/// year ~2554 — not a concern for v0.
#[inline]
fn sys_to_nanos(t: SystemTime) -> u64 {
    t.duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos().min(u64::MAX as u128) as u64)
        .unwrap_or(0)
}

#[inline]
fn nanos_to_sys(n: u64) -> SystemTime {
    UNIX_EPOCH + Duration::from_nanos(n)
}

/// Fixed lateness tolerance for v0. Locked per CONTEXT.md §Late event
/// handling; a per-stream tunable lands post-v0.
pub const WATERMARK_LATENESS: Duration = Duration::from_secs(5);

/// Reserved JSON field name carrying the event's event-time.
pub const EVENT_TIME_FIELD: &str = "_event_time";

/// Threshold for distinguishing unix-seconds vs unix-milliseconds in
/// numeric `_event_time` values. Numbers `< 2^31` are interpreted as
/// seconds (covers all dates up to ~2038); numbers `>= 2^31` are
/// interpreted as milliseconds.
const UNIX_SEC_MS_THRESHOLD: f64 = (1u64 << 31) as f64;

/// Parse the `_event_time` field from an event payload, falling back
/// to `fallback` on absent / unparseable / out-of-range values.
///
/// Accepted forms:
/// - ISO8601 string: `"2026-04-14T12:34:56Z"` or `"2026-04-14T12:34:56.789Z"`
/// - Unix integer (i64): interpreted as seconds if `< 2^31`, otherwise ms
/// - Unix float (f64):   interpreted as seconds if `< 2^31`, otherwise ms
///
/// Nested objects, arrays, booleans, null, and garbage strings → fallback.
/// Never errors — watermarks must be resilient to client-supplied garbage
/// (T-24-04-01: we accept user timestamps at face value; clamp is post-v0).
pub fn parse_event_time(
    payload: &serde_json::Value,
    fallback: SystemTime,
) -> SystemTime {
    let field = match payload.get(EVENT_TIME_FIELD) {
        Some(v) => v,
        None => return fallback,
    };
    match field {
        serde_json::Value::String(s) => parse_iso8601(s).unwrap_or(fallback),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                if i < 0 {
                    return fallback;
                }
                if (i as f64) < UNIX_SEC_MS_THRESHOLD {
                    UNIX_EPOCH.checked_add(Duration::from_secs(i as u64)).unwrap_or(fallback)
                } else {
                    UNIX_EPOCH
                        .checked_add(Duration::from_millis(i as u64))
                        .unwrap_or(fallback)
                }
            } else if let Some(f) = n.as_f64() {
                if !f.is_finite() || f < 0.0 {
                    return fallback;
                }
                if f < UNIX_SEC_MS_THRESHOLD {
                    // seconds
                    let secs = f.trunc() as u64;
                    let nanos = ((f - f.trunc()) * 1e9).round().max(0.0) as u32;
                    UNIX_EPOCH
                        .checked_add(Duration::new(secs, nanos.min(999_999_999)))
                        .unwrap_or(fallback)
                } else {
                    // milliseconds
                    let ms = f as u64;
                    UNIX_EPOCH.checked_add(Duration::from_millis(ms)).unwrap_or(fallback)
                }
            } else {
                fallback
            }
        }
        _ => fallback,
    }
}

/// Minimal ISO8601 parser for `YYYY-MM-DDTHH:MM:SS[.fff]Z` and the
/// no-offset variant. Returns `None` for any other format — clients
/// should prefer unix-ms for non-UTC timestamps.
fn parse_iso8601(s: &str) -> Option<SystemTime> {
    // Accept trailing 'Z' or 'z' (UTC) or bare (interpret as UTC per v0).
    let trimmed = s.trim();
    let body = trimmed
        .strip_suffix('Z')
        .or_else(|| trimmed.strip_suffix('z'))
        .unwrap_or(trimmed);

    // Split date and time halves on 'T' (also accept space).
    let (date_part, time_part) = body.split_once(['T', 't', ' '])?;

    // Parse date: YYYY-MM-DD.
    let mut date_iter = date_part.split('-');
    let year: i64 = date_iter.next()?.parse().ok()?;
    let month: u32 = date_iter.next()?.parse().ok()?;
    let day: u32 = date_iter.next()?.parse().ok()?;
    if date_iter.next().is_some() {
        return None;
    }
    if !(1970..=9999).contains(&year)
        || !(1..=12).contains(&month)
        || !(1..=31).contains(&day)
    {
        return None;
    }

    // Parse time: HH:MM:SS[.frac]. Ignore any trailing '+hh:mm' offset
    // (treated as UTC per v0 simplification).
    let time_body = match time_part.find(['+', '-']) {
        Some(i) => &time_part[..i],
        None => time_part,
    };
    let mut time_iter = time_body.split(':');
    let hour: u32 = time_iter.next()?.parse().ok()?;
    let minute: u32 = time_iter.next()?.parse().ok()?;
    let sec_part = time_iter.next()?;
    if time_iter.next().is_some() {
        return None;
    }
    let (sec_int, nanos) = match sec_part.split_once('.') {
        Some((s, frac)) => {
            let si: u32 = s.parse().ok()?;
            // Pad/truncate fractional digits to 9.
            let mut digits = String::with_capacity(9);
            for c in frac.chars().take(9) {
                if !c.is_ascii_digit() {
                    return None;
                }
                digits.push(c);
            }
            while digits.len() < 9 {
                digits.push('0');
            }
            let ns: u32 = digits.parse().ok()?;
            (si, ns)
        }
        None => (sec_part.parse().ok()?, 0u32),
    };
    if hour >= 24 || minute >= 60 || sec_int >= 60 {
        return None;
    }

    // Convert (year, month, day) → days since 1970-01-01 via the
    // Howard Hinnant civil_from_days inverse (well-known public-domain
    // algorithm). Keeps us off chrono.
    let y = if month <= 2 { year - 1 } else { year };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400; // [0, 399]
    let m = month as i64;
    let d = day as i64;
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days_since_epoch = era * 146097 + doe - 719468;
    if days_since_epoch < 0 {
        return None;
    }

    let secs_in_day = (hour as u64) * 3600 + (minute as u64) * 60 + (sec_int as u64);
    let total_secs = (days_since_epoch as u64) * 86_400 + secs_in_day;
    UNIX_EPOCH.checked_add(Duration::new(total_secs, nanos))
}

/// Per-stream watermark state tracked on the engine.
///
/// Stores `max(event_time observed)` per stream; watermark is derived
/// on read as `observed_max − lateness_for(stream)`. Storing the max
/// rather than the derived watermark is intentional: it keeps the
/// data model monotone (max is easier to reason about than a derived
/// quantity that could regress under underflow).
#[derive(Debug)]
pub struct WatermarkTracker {
    /// `max(event_time observed)` per stream, stored as nanoseconds since
    /// UNIX_EPOCH in an `AtomicU64` for lock-free `fetch_max` updates.
    /// DashMap gives per-stream shard-level concurrency; the inner atomic
    /// means no lock is held on the hot path.
    observed_max: DashMap<String, AtomicU64>,
    /// Most recent event_time observed (not necessarily the max — this
    /// is the "last event" pointer for debug visibility). Last-writer-wins.
    last_event_time: DashMap<String, AtomicU64>,
    /// D-10 / CORR-03: per-stream watermark lateness override. Absent entry
    /// falls back to the global `WATERMARK_LATENESS` constant (5 s).
    /// Populated by `PipelineEngine::register` when
    /// `StreamDefinition.watermark_lateness` is `Some`.
    watermark_lateness: DashMap<String, Duration>,
}

impl Default for WatermarkTracker {
    fn default() -> Self {
        Self::new()
    }
}

impl WatermarkTracker {
    pub fn new() -> Self {
        Self {
            observed_max: DashMap::new(),
            last_event_time: DashMap::new(),
            watermark_lateness: DashMap::new(),
        }
    }

    /// Record an event_time observation for `stream`. Updates the
    /// running max (and thus the watermark) and the last-seen pointer.
    /// Lock-free hot path — Phase 43.
    pub fn observe(&self, stream: &str, event_time: SystemTime) {
        let ns = sys_to_nanos(event_time);
        // last_event_time — last-writer-wins, just store.
        match self.last_event_time.get(stream) {
            Some(entry) => entry.value().store(ns, Ordering::Relaxed),
            None => {
                self.last_event_time
                    .entry(stream.to_string())
                    .or_insert_with(|| AtomicU64::new(ns))
                    .value()
                    .store(ns, Ordering::Relaxed);
            }
        }
        // observed_max — monotonic max via fetch_max.
        match self.observed_max.get(stream) {
            Some(entry) => {
                entry.value().fetch_max(ns, Ordering::Relaxed);
            }
            None => {
                // Race-safe: or_insert_with + fetch_max afterwards keeps
                // monotonic semantics even if two threads race the insert.
                self.observed_max
                    .entry(stream.to_string())
                    .or_insert_with(|| AtomicU64::new(0))
                    .value()
                    .fetch_max(ns, Ordering::Relaxed);
            }
        }
    }

    /// D-10 / CORR-03: set the per-stream watermark lateness override.
    /// Called by `PipelineEngine::register` when `StreamDefinition.watermark_lateness`
    /// is `Some`. Lock-free insert into the DashMap.
    pub fn set_lateness(&self, stream: &str, lateness: Duration) {
        self.watermark_lateness.insert(stream.to_string(), lateness);
    }

    /// D-10 / CORR-03: look up the per-stream lateness override.
    /// Falls back to the global `WATERMARK_LATENESS` constant (5 s) when the
    /// stream has no override registered.
    pub fn lateness_for(&self, stream: &str) -> Duration {
        self.watermark_lateness
            .get(stream)
            .map(|e| *e.value())
            .unwrap_or(WATERMARK_LATENESS)
    }

    /// Current watermark for `stream`, i.e. `observed_max - lateness_for(stream)`.
    /// Returns `None` if the stream has never been observed — in which case no
    /// event should be considered late (the first event always seeds the tracker).
    pub fn watermark(&self, stream: &str) -> Option<SystemTime> {
        let entry = self.observed_max.get(stream)?;
        let ns = entry.value().load(Ordering::Relaxed);
        if ns == 0 {
            return None;
        }
        let max = nanos_to_sys(ns);
        let lateness = self.lateness_for(stream);
        // Clamp to UNIX_EPOCH to avoid a pre-epoch watermark that
        // would then "late-drop" the very first event.
        Some(match max.duration_since(UNIX_EPOCH) {
            Ok(d) if d >= lateness => max - lateness,
            _ => UNIX_EPOCH,
        })
    }

    /// Most recent `event_time` observed on `stream`, or `None`.
    pub fn last_event_time(&self, stream: &str) -> Option<SystemTime> {
        let entry = self.last_event_time.get(stream)?;
        let ns = entry.value().load(Ordering::Relaxed);
        if ns == 0 {
            return None;
        }
        Some(nanos_to_sys(ns))
    }

    /// `max(event_time observed)` on `stream`, or `None`.
    pub fn observed_max(&self, stream: &str) -> Option<SystemTime> {
        let entry = self.observed_max.get(stream)?;
        let ns = entry.value().load(Ordering::Relaxed);
        if ns == 0 {
            return None;
        }
        Some(nanos_to_sys(ns))
    }

    /// γ: stateless op — output stream inherits the input stream's
    /// current watermark verbatim. No-op if the input has not yet been
    /// observed (the output also has no watermark yet).
    pub fn propagate_stateless(&self, from: &str, to: &str) {
        if let Some(max) = self.observed_max(from) {
            self.observe(to, max);
        }
    }

    /// γ: join — output watermark = min(left_wm, right_wm). If either
    /// input is un-observed, the join cannot advance; no-op. Uses
    /// `fetch_max` on the output so it remains monotonic even if
    /// concurrent updates on the inputs interleave.
    pub fn propagate_join(&self, left: &str, right: &str, output: &str) {
        match (self.observed_max(left), self.observed_max(right)) {
            (Some(l), Some(r)) => {
                let min_max = l.min(r);
                // Use observe (fetch_max) to keep the output monotonic.
                // Note: this means once the output advances, we never
                // regress — even if a subsequent left/right min call
                // would yield a lower min. This matches the original
                // semantics: watermarks only advance.
                self.observe(output, min_max);
            }
            _ => {
                // One side un-observed; join output watermark stays unset.
            }
        }
    }

    /// γ: aggregation — the output Table inherits the source stream's
    /// current watermark.
    pub fn attach_to_table(&self, source_stream: &str, output_table: &str) {
        if let Some(max) = self.observed_max(source_stream) {
            self.observe(output_table, max);
        }
    }

    /// List every stream that has an observed watermark. Used by
    /// /debug/key/:key and /debug/streams/:name.
    pub fn iter_streams(&self) -> Vec<(String, SystemTime)> {
        self.observed_max
            .iter()
            .map(|entry| {
                (
                    entry.key().clone(),
                    nanos_to_sys(entry.value().load(Ordering::Relaxed)),
                )
            })
            .collect()
    }
}

/// Counter of late-event drops per stream. Exported via the existing
/// `/metrics` endpoint as `beava_late_events_dropped_total{stream="..."}`.
#[derive(Debug)]
pub struct LateDropCounters {
    per_stream: DashMap<String, AtomicU64>,
}

impl Default for LateDropCounters {
    fn default() -> Self {
        Self::new()
    }
}

impl LateDropCounters {
    pub fn new() -> Self {
        Self {
            per_stream: DashMap::new(),
        }
    }

    /// Lock-free increment via DashMap entry + AtomicU64 fetch_add.
    pub fn increment(&self, stream: &str) {
        match self.per_stream.get(stream) {
            Some(entry) => {
                entry.value().fetch_add(1, Ordering::Relaxed);
            }
            None => {
                self.per_stream
                    .entry(stream.to_string())
                    .or_insert_with(|| AtomicU64::new(0))
                    .value()
                    .fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    pub fn get(&self, stream: &str) -> u64 {
        self.per_stream
            .get(stream)
            .map(|e| e.value().load(Ordering::Relaxed))
            .unwrap_or(0)
    }

    pub fn snapshot(&self) -> Vec<(String, u64)> {
        self.per_stream
            .iter()
            .map(|e| (e.key().clone(), e.value().load(Ordering::Relaxed)))
            .collect()
    }
}

/// Reason a ring-buffer operator dropped an event (D-05 / OBS-01).
///
/// Hard compile-time enum — keeps label cardinality bounded at 3 regardless
/// of event count or stream count (Pitfall 3 / D-05 guard).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DropReason {
    /// event_time falls before the ring's earliest retained bucket (event is
    /// too far in the past). This is redundant with the watermark late-drop
    /// gate at `tcp.rs:~1753` but can fire for backfill paths that bypass
    /// the gate.
    TooOld,
    /// event_time is so far in the future that computing the bucket offset
    /// overflows (et_bucket_start > current_bucket_start after alignment).
    /// Extremely rare in practice; included for completeness.
    TooNew,
    /// event_time is before UNIX_EPOCH (negative timestamp). The ring buffer
    /// alignment arithmetic uses `unwrap_or(Duration::ZERO)` which maps these
    /// to epoch, producing a large `delta_buckets` and thus a TooOld drop in
    /// most rings — but we detect the root cause explicitly here.
    PreEpoch,
}

impl DropReason {
    /// Returns the Prometheus label string for this reason.
    /// Exactly one of "too_old" | "too_new" | "pre_epoch" — never a dynamic
    /// string, so label cardinality is compile-time bounded.
    pub fn as_label(self) -> &'static str {
        match self {
            Self::TooOld => "too_old",
            Self::TooNew => "too_new",
            Self::PreEpoch => "pre_epoch",
        }
    }
}

/// Per `(stream, operator_kind, DropReason)` drop counter for ring-buffer
/// operators (OBS-01). Mirrors `LateDropCounters` but with a 3-element
/// tuple key.
///
/// `operator_kind` is the operator CLASS (e.g. `"count"`, `"sum"`) — NOT a
/// per-instance UUID (Pitfall 3 / D-05). This keeps label cardinality bounded
/// by `num_streams × num_operator_kinds × 3`.
///
/// D-06: Counter handles are pre-allocated at stream registration time via
/// `register()`. The hot drop path calls `fetch_add(1, Relaxed)` on a cached
/// `Arc<AtomicU64>` — zero DashMap lookup overhead.
#[derive(Debug)]
pub struct RingBufferDropCounters {
    /// Key = (stream_name, operator_kind, reason).
    per_label: DashMap<(String, String, DropReason), Arc<AtomicU64>>,
}

impl Default for RingBufferDropCounters {
    fn default() -> Self {
        Self::new()
    }
}

impl RingBufferDropCounters {
    pub fn new() -> Self {
        Self {
            per_label: DashMap::new(),
        }
    }

    /// Pre-allocate and cache the three drop-reason `Arc<AtomicU64>` handles
    /// for a `(stream, operator_kind)` pair. Call once at stream/operator
    /// registration time (D-06). Returns `(too_old, too_new, pre_epoch)`.
    pub fn register(
        &self,
        stream: &str,
        operator_kind: &str,
    ) -> (Arc<AtomicU64>, Arc<AtomicU64>, Arc<AtomicU64>) {
        let too_old = Arc::clone(
            self.per_label
                .entry((stream.to_string(), operator_kind.to_string(), DropReason::TooOld))
                .or_insert_with(|| Arc::new(AtomicU64::new(0)))
                .value(),
        );
        let too_new = Arc::clone(
            self.per_label
                .entry((stream.to_string(), operator_kind.to_string(), DropReason::TooNew))
                .or_insert_with(|| Arc::new(AtomicU64::new(0)))
                .value(),
        );
        let pre_epoch = Arc::clone(
            self.per_label
                .entry((
                    stream.to_string(),
                    operator_kind.to_string(),
                    DropReason::PreEpoch,
                ))
                .or_insert_with(|| Arc::new(AtomicU64::new(0)))
                .value(),
        );
        (too_old, too_new, pre_epoch)
    }

    /// Increment a specific `(stream, operator_kind, reason)` counter.
    /// Used on the drop path (cold path); DashMap lookup is acceptable here.
    pub fn increment(&self, stream: &str, operator_kind: &str, reason: DropReason) {
        match self
            .per_label
            .get(&(stream.to_string(), operator_kind.to_string(), reason))
        {
            Some(entry) => {
                entry.value().fetch_add(1, Ordering::Relaxed);
            }
            None => {
                // Not pre-registered (e.g. backfill path before registration).
                // Insert lazily.
                self.per_label
                    .entry((stream.to_string(), operator_kind.to_string(), reason))
                    .or_insert_with(|| Arc::new(AtomicU64::new(0)))
                    .value()
                    .fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    /// Snapshot all non-zero counters. Used by the `/metrics` endpoint.
    pub fn snapshot(&self) -> Vec<((String, String, DropReason), u64)> {
        self.per_label
            .iter()
            .map(|e| (e.key().clone(), e.value().load(Ordering::Relaxed)))
            .collect()
    }

    /// Total drops across all labels. Useful for integration tests.
    pub fn total(&self) -> u64 {
        self.per_label
            .iter()
            .map(|e| e.value().load(Ordering::Relaxed))
            .sum()
    }
}

/// Type aliases retained for call-site compatibility. Phase 43: the
/// underlying types now use interior mutability (DashMap + AtomicU64),
/// so no outer lock is needed. Call sites that previously did
/// `.read()` / `.write()` now call methods directly on `&WatermarkTracker`
/// / `&LateDropCounters`.
pub type SharedWatermarks = WatermarkTracker;
pub type SharedLateDrops = LateDropCounters;

#[cfg(test)]
mod tests {
    use super::*;

    fn sec(s: u64) -> SystemTime {
        UNIX_EPOCH + Duration::from_secs(s)
    }

    #[test]
    fn parses_iso8601_simple() {
        let t = parse_iso8601("2026-04-14T00:00:00Z").unwrap();
        // 2026-04-14 is 20557 days after epoch.
        let expected = UNIX_EPOCH + Duration::from_secs(20557 * 86_400);
        assert_eq!(t, expected);
    }

    #[test]
    fn parses_iso8601_with_fractional() {
        let t = parse_iso8601("1970-01-01T00:00:01.500Z").unwrap();
        assert_eq!(t, UNIX_EPOCH + Duration::new(1, 500_000_000));
    }

    #[test]
    fn rejects_garbage_iso8601() {
        assert!(parse_iso8601("not-a-date").is_none());
        assert!(parse_iso8601("2026/04/14").is_none());
    }

    #[test]
    fn event_time_absent_returns_fallback() {
        let fallback = sec(42);
        let payload = serde_json::json!({"user_id": "u1"});
        assert_eq!(parse_event_time(&payload, fallback), fallback);
    }

    #[test]
    fn event_time_unix_seconds_integer() {
        let fallback = sec(0);
        let payload = serde_json::json!({"_event_time": 1_000_000i64});
        assert_eq!(parse_event_time(&payload, fallback), sec(1_000_000));
    }

    #[test]
    fn event_time_unix_ms_integer_above_threshold() {
        let fallback = sec(0);
        // 3_000_000_000 is > 2^31; interpret as ms → 3,000,000 seconds.
        let payload = serde_json::json!({"_event_time": 3_000_000_000i64});
        assert_eq!(parse_event_time(&payload, fallback), sec(3_000_000));
    }

    #[test]
    fn event_time_unix_seconds_float() {
        let fallback = sec(0);
        let payload = serde_json::json!({"_event_time": 1000.5});
        let et = parse_event_time(&payload, fallback);
        assert_eq!(et, UNIX_EPOCH + Duration::new(1000, 500_000_000));
    }

    #[test]
    fn event_time_nested_object_returns_fallback() {
        let fallback = sec(99);
        let payload = serde_json::json!({"_event_time": {"nested": 1}});
        assert_eq!(parse_event_time(&payload, fallback), fallback);
    }

    #[test]
    fn watermark_tracks_max_minus_5s() {
        let wm = WatermarkTracker::new();
        wm.observe("s", sec(100));
        wm.observe("s", sec(110));
        wm.observe("s", sec(105));
        assert_eq!(wm.observed_max("s"), Some(sec(110)));
        assert_eq!(wm.watermark("s"), Some(sec(105)));
    }

    #[test]
    fn watermark_absent_for_fresh_stream() {
        let wm = WatermarkTracker::new();
        assert!(wm.watermark("never-seen").is_none());
    }

    #[test]
    fn watermark_underflow_clamps_to_epoch() {
        let wm = WatermarkTracker::new();
        wm.observe("s", UNIX_EPOCH + Duration::from_secs(2));
        assert_eq!(wm.watermark("s"), Some(UNIX_EPOCH));
    }

    #[test]
    fn propagate_stateless_copies_watermark() {
        let wm = WatermarkTracker::new();
        wm.observe("in", sec(100));
        wm.propagate_stateless("in", "out");
        assert_eq!(wm.watermark("out"), Some(sec(95)));
    }

    #[test]
    fn propagate_join_takes_min() {
        let wm = WatermarkTracker::new();
        wm.observe("l", sec(100));
        wm.observe("r", sec(200));
        wm.propagate_join("l", "r", "j");
        assert_eq!(wm.observed_max("j"), Some(sec(100)));
        assert_eq!(wm.watermark("j"), Some(sec(95)));
    }

    #[test]
    fn attach_to_table_inherits_stream_watermark() {
        let wm = WatermarkTracker::new();
        wm.observe("s", sec(110));
        wm.attach_to_table("s", "agg_out");
        assert_eq!(wm.watermark("agg_out"), Some(sec(105)));
    }

    #[test]
    fn late_drop_counter_increments() {
        let c = LateDropCounters::new();
        assert_eq!(c.get("s"), 0);
        c.increment("s");
        c.increment("s");
        c.increment("other");
        assert_eq!(c.get("s"), 2);
        assert_eq!(c.get("other"), 1);
    }
}
