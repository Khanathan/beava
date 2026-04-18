//! Per-shard Prometheus metrics — Phase 50 (Wave 2), D-07.
//!
//! All series emitted via the `metrics` crate global recorder installed in
//! Plan 50-01. The hand-rolled /metrics path remains functional in parallel
//! through Wave 3 (D-06 parallel period).
//!
//! Metric name constants are the single source of truth — no magic strings
//! at call sites.

// ---- metric name constants ----
/// Per-shard reactor utilization gauge (0..1).
pub const SHARD_REACTOR_UTILIZATION: &str = "beava_shard_reactor_utilization";
/// Per-shard SPSC inbox backlog depth gauge.
pub const SHARD_INBOX_DEPTH: &str = "beava_shard_inbox_depth";
/// Per-shard event counter (outcome: accepted|dropped).
pub const SHARD_EVENTS_TOTAL: &str = "beava_shard_events_total";
/// Per-shard owned-key count gauge.
pub const SHARD_KEYS_OWNED: &str = "beava_shard_keys_owned";
/// Per-shard watermark lag in seconds gauge.
pub const SHARD_WATERMARK_LAG_SECONDS: &str = "beava_shard_watermark_lag_seconds";
/// Per-shard inbox-full drop counter (backpressure drops).
pub const SHARD_INBOX_FULL_TOTAL: &str = "beava_shard_inbox_full_total";
/// Per-shard DOWN counter (D-02 panic quarantine events).
pub const SHARD_DOWN_TOTAL: &str = "beava_shard_down_total";
/// Global events-dropped counter with reason label.
pub const EVENTS_DROPPED_TOTAL: &str = "beava_events_dropped_total";
/// Cross-shard fanout counter — DEFINED here, NOT incremented until Wave 3.
pub const CROSS_SHARD_FANOUT_TOTAL: &str = "beava_cross_shard_fanout_total";

/// Outcome of a shard event dispatch.
#[derive(Clone, Copy, Debug)]
pub enum Outcome {
    /// Event was accepted and dispatched to the shard.
    Accepted,
    /// Event was dropped (before or after routing).
    Dropped,
}

impl Outcome {
    fn as_str(self) -> &'static str {
        match self {
            Outcome::Accepted => "accepted",
            Outcome::Dropped => "dropped",
        }
    }
}

/// Reason an event was dropped at the ingest boundary.
#[derive(Clone, Copy, Debug)]
pub enum DropReason {
    /// Tuple shard_key field missing from event payload (D-10).
    ShardKeyMissing,
    /// Shard SPSC inbox was full (D-08 backpressure).
    InboxFull,
    /// Malformed routing — shard_hint could not be resolved.
    MalformedRouting,
}

impl DropReason {
    fn as_str(self) -> &'static str {
        match self {
            DropReason::ShardKeyMissing => "shard_key_missing",
            DropReason::InboxFull => "inbox_full",
            DropReason::MalformedRouting => "malformed_routing",
        }
    }
}

/// Call once at startup after install_prometheus_recorder(), before shards start.
/// Touches all series with zero so they appear in /metrics even before the first event.
pub fn register_shard_metrics(shard_count: usize) {
    for shard in 0..shard_count {
        let s = shard.to_string();
        // Touch each gauge/counter so it appears in the scrape immediately.
        metrics::gauge!(SHARD_REACTOR_UTILIZATION, "shard" => s.clone()).set(0.0);
        metrics::gauge!(SHARD_INBOX_DEPTH, "shard" => s.clone()).set(0.0);
        metrics::counter!(SHARD_EVENTS_TOTAL, "shard" => s.clone(), "outcome" => "accepted")
            .increment(0);
        metrics::counter!(SHARD_EVENTS_TOTAL, "shard" => s.clone(), "outcome" => "dropped")
            .increment(0);
        metrics::gauge!(SHARD_KEYS_OWNED, "shard" => s.clone()).set(0.0);
        metrics::gauge!(SHARD_WATERMARK_LAG_SECONDS, "shard" => s.clone()).set(0.0);
        metrics::counter!(SHARD_INBOX_FULL_TOTAL, "shard" => s.clone()).increment(0);
        metrics::counter!(SHARD_DOWN_TOTAL, "shard" => s).increment(0);
    }
    // Global reason-labeled drop counter — touch all label variants.
    for reason in &["shard_key_missing", "inbox_full", "malformed_routing"] {
        metrics::counter!(EVENTS_DROPPED_TOTAL, "reason" => *reason).increment(0);
    }
    // Cross-shard fanout counter — defined here, first increment is Wave 3.
    metrics::counter!(CROSS_SHARD_FANOUT_TOTAL, "op" => "list_streams").increment(0);
}

// ---- update helpers called from hot path ----

/// Record one event processed by `shard_index` with the given outcome.
#[inline]
pub fn record_shard_event(shard_index: usize, outcome: Outcome) {
    let s = shard_index.to_string();
    metrics::counter!(SHARD_EVENTS_TOTAL, "shard" => s, "outcome" => outcome.as_str())
        .increment(1);
}

/// Record an inbox-full drop: increments both the per-shard counter and
/// the global beava_events_dropped_total{reason="inbox_full"}.
#[inline]
pub fn record_inbox_full(shard_index: usize) {
    let s = shard_index.to_string();
    metrics::counter!(SHARD_INBOX_FULL_TOTAL, "shard" => s).increment(1);
    metrics::counter!(EVENTS_DROPPED_TOTAL, "reason" => "inbox_full").increment(1);
}

/// Record an event dropped at ingest because the shard_key field was missing (D-10).
#[inline]
pub fn record_shard_key_missing() {
    metrics::counter!(EVENTS_DROPPED_TOTAL, "reason" => "shard_key_missing").increment(1);
}

/// Record a shard panic / DOWN transition (D-02).
#[inline]
pub fn record_shard_down(shard_index: usize) {
    let s = shard_index.to_string();
    metrics::counter!(SHARD_DOWN_TOTAL, "shard" => s).increment(1);
}

/// Update gauge-type metrics for a shard (called periodically from shard loop, not per-event).
#[inline]
pub fn update_shard_gauges(
    shard_index: usize,
    reactor_utilization: f64,
    inbox_depth: usize,
    keys_owned: usize,
    watermark_lag_seconds: f64,
) {
    let s = shard_index.to_string();
    metrics::gauge!(SHARD_REACTOR_UTILIZATION, "shard" => s.clone()).set(reactor_utilization);
    metrics::gauge!(SHARD_INBOX_DEPTH, "shard" => s.clone()).set(inbox_depth as f64);
    metrics::gauge!(SHARD_KEYS_OWNED, "shard" => s.clone()).set(keys_owned as f64);
    metrics::gauge!(SHARD_WATERMARK_LAG_SECONDS, "shard" => s).set(watermark_lag_seconds);
}

#[cfg(test)]
mod tests {
    use super::*;

    // We deliberately do NOT call install_prometheus_recorder() in unit tests
    // to avoid global-state conflicts across parallel test runs.
    // The helpers must not panic when no global recorder is installed.

    #[test]
    fn metric_name_constants_are_correct() {
        // Compile-time: verify constants match D-07 naming.
        assert_eq!(SHARD_REACTOR_UTILIZATION, "beava_shard_reactor_utilization");
        assert_eq!(SHARD_INBOX_DEPTH, "beava_shard_inbox_depth");
        assert_eq!(SHARD_EVENTS_TOTAL, "beava_shard_events_total");
        assert_eq!(SHARD_KEYS_OWNED, "beava_shard_keys_owned");
        assert_eq!(SHARD_WATERMARK_LAG_SECONDS, "beava_shard_watermark_lag_seconds");
        assert_eq!(SHARD_INBOX_FULL_TOTAL, "beava_shard_inbox_full_total");
        assert_eq!(SHARD_DOWN_TOTAL, "beava_shard_down_total");
        assert_eq!(EVENTS_DROPPED_TOTAL, "beava_events_dropped_total");
        assert_eq!(CROSS_SHARD_FANOUT_TOTAL, "beava_cross_shard_fanout_total");
    }

    #[test]
    fn outcome_strings_correct() {
        assert_eq!(Outcome::Accepted.as_str(), "accepted");
        assert_eq!(Outcome::Dropped.as_str(), "dropped");
    }

    #[test]
    fn drop_reason_strings_correct() {
        assert_eq!(DropReason::ShardKeyMissing.as_str(), "shard_key_missing");
        assert_eq!(DropReason::InboxFull.as_str(), "inbox_full");
        assert_eq!(DropReason::MalformedRouting.as_str(), "malformed_routing");
    }

    #[test]
    fn helpers_dont_panic_without_recorder() {
        // With no global recorder installed, metrics! macros use a no-op recorder.
        // These calls must not panic.
        record_shard_event(0, Outcome::Accepted);
        record_inbox_full(0);
        record_shard_key_missing();
        record_shard_down(0);
        update_shard_gauges(0, 0.5, 100, 200, 0.01);
    }

    #[test]
    fn register_shard_metrics_no_panic_without_recorder() {
        // register_shard_metrics must not panic even without a global recorder.
        register_shard_metrics(4);
    }
}
