//! Phase 25-02: configuration recommendation engine.
//!
//! Reads observed counters from [`EvictionTracker`] + `Metrics` and emits
//! copy-pasteable decorator overrides when the signals cross the locked
//! thresholds (v0-restructure-spec §7.2, §8).
//!
//! Exposed via:
//! - `GET /debug/config-recommendations` (admin-gated)
//! - `tally suggest-config` CLI (wraps the HTTP endpoint)
//! - startup advisory log (one terse line per knob)

use crate::engine::pipeline::PipelineEngine;
use crate::state::eviction_tracker::EvictionTracker;
use serde::Serialize;

/// Threshold above which we suggest doubling the Table `ttl`.
/// 5% reinit rate ≈ "plenty of users are being forgotten too early".
pub const REINIT_RATE_THRESHOLD: f64 = 0.05;

/// Minimum evictions before the reinit-rate signal is trusted. Below this we
/// lack statistical power — a single reinit on a 10-eviction sample is noise.
pub const MIN_EVICTIONS_FOR_SIGNAL: u64 = 100;

/// A single actionable recommendation for one configuration knob. The schema
/// is the wire contract for `/debug/config-recommendations`.
#[derive(Debug, Clone, Serialize)]
pub struct ConfigRecommendation {
    /// Dotted knob name — e.g., `"UserProfile.ttl"`.
    pub knob: String,
    /// Current value — e.g., `"30d"`.
    pub current: String,
    /// Suggested value — e.g., `"60d"`.
    pub suggested: String,
    /// Confidence in the recommendation, in [0.0, 1.0]. A zero-confidence
    /// recommendation is informational (e.g., tombstone grace — locked in v0).
    pub confidence: f64,
    /// Human-readable reason — shown in the CLI.
    pub reason: String,
    /// Raw evidence (counters, rates). JSON blob so future knobs can carry
    /// their own shapes without version bumps.
    pub evidence: serde_json::Value,
    /// Copy-pasteable decorator line.
    pub copy_paste: String,
}

/// Compute the full list of recommendations. Returns empty Vec when no signal
/// crosses threshold. Deterministic ordering: Table-TTL suggestions first,
/// then stream-history suggestions, grouped by pipeline name alphabetically.
pub fn recommend_config(
    engine: &PipelineEngine,
    tracker: &EvictionTracker,
) -> Vec<ConfigRecommendation> {
    let mut out = Vec::new();

    // 1. Table TTL too short — reinit_rate > 5% over bloom window.
    let mut table_recs: Vec<(String, ConfigRecommendation)> = Vec::new();
    for stream in engine.list_streams() {
        // Only Tables have a ttl that this recommendation applies to. We
        // detect Table-ness via the stored raw REGISTER JSON — the v0 SDK
        // sets kind=table for Table sources/derivations.
        if !engine.has_registered_table(&stream.name) {
            continue;
        }
        let evictions = tracker.eviction_count(&stream.name);
        let reinits = tracker.reinit_count(&stream.name);
        if evictions < MIN_EVICTIONS_FOR_SIGNAL {
            continue;
        }
        let rate = reinits as f64 / evictions as f64;
        if rate <= REINIT_RATE_THRESHOLD {
            continue;
        }
        let current_secs = stream.entity_ttl.map(|d| d.as_secs()).unwrap_or(0);
        let current_human = humanize_duration_secs(current_secs);
        // Suggestion: double the current TTL. Capped at 365d.
        let suggested_secs = (current_secs * 2).min(365 * 86400);
        let suggested_human = humanize_duration_secs(suggested_secs);
        let confidence = (rate * 10.0).min(1.0);
        let copy_paste = format!(
            "@tl.table(key=..., ttl=\"{}\")",
            suggested_human
        );
        let rec = ConfigRecommendation {
            knob: format!("{}.ttl", stream.name),
            current: current_human,
            suggested: suggested_human,
            confidence,
            reason: format!(
                "{:.1}% of TTL-evicted keys reactivated within the bloom window (>{:.0}% threshold)",
                rate * 100.0,
                REINIT_RATE_THRESHOLD * 100.0
            ),
            evidence: serde_json::json!({
                "evictions": evictions,
                "reinits": reinits,
                "reinit_rate": rate,
            }),
            copy_paste,
        };
        table_recs.push((stream.name.clone(), rec));
    }
    table_recs.sort_by(|a, b| a.0.cmp(&b.0));
    out.extend(table_recs.into_iter().map(|(_, r)| r));

    // 2. Stream history_ttl < max(downstream Table ttl).
    // Build a per-Stream set of downstream Table ttls by walking depends_on
    // backwards: for every Table in `streams`, each `depends_on` entry points
    // at a Stream whose history must cover the Table's ttl.
    let mut upstream_to_downstream_ttl: ahash::AHashMap<String, std::time::Duration> =
        ahash::AHashMap::new();
    for table in engine.list_streams() {
        if !engine.has_registered_table(&table.name) {
            continue;
        }
        let Some(ttl) = table.entity_ttl else { continue };
        if let Some(deps) = &table.depends_on {
            for dep in deps {
                let slot = upstream_to_downstream_ttl
                    .entry(dep.clone())
                    .or_insert(std::time::Duration::ZERO);
                if ttl > *slot {
                    *slot = ttl;
                }
            }
        }
    }
    let mut stream_recs: Vec<(String, ConfigRecommendation)> = Vec::new();
    for stream in engine.list_streams() {
        if engine.has_registered_table(&stream.name) {
            continue;
        }
        let Some(history_ttl) = stream.history_ttl else {
            continue;
        };
        let Some(required) = upstream_to_downstream_ttl.get(&stream.name) else {
            continue;
        };
        if history_ttl >= *required {
            continue;
        }
        let current_human = humanize_duration_secs(history_ttl.as_secs());
        let suggested_human = humanize_duration_secs(required.as_secs());
        let rec = ConfigRecommendation {
            knob: format!("{}.history_ttl", stream.name),
            current: current_human,
            suggested: suggested_human.clone(),
            confidence: 1.0,
            reason: format!(
                "history_ttl is shorter than the max downstream Table ttl ({}); \
                 backfills on register will lose events",
                suggested_human
            ),
            evidence: serde_json::json!({
                "stream_history_ttl_secs": history_ttl.as_secs(),
                "max_downstream_table_ttl_secs": required.as_secs(),
            }),
            copy_paste: format!("@tl.stream(history_ttl=\"{}\")", suggested_human),
        };
        stream_recs.push((stream.name.clone(), rec));
    }
    stream_recs.sort_by(|a, b| a.0.cmp(&b.0));
    out.extend(stream_recs.into_iter().map(|(_, r)| r));

    out
}

/// Format a duration in seconds as a human-friendly string.
/// Prefers the largest unit that divides evenly — 86400 s → "1d", 3600 s → "1h",
/// 7200 s → "2h", 90 s → "90s".
pub fn humanize_duration_secs(secs: u64) -> String {
    if secs == 0 {
        return "0".to_string();
    }
    if secs % 86400 == 0 {
        return format!("{}d", secs / 86400);
    }
    if secs % 3600 == 0 {
        return format!("{}h", secs / 3600);
    }
    if secs % 60 == 0 {
        return format!("{}m", secs / 60);
    }
    format!("{}s", secs)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn humanize_duration_basic() {
        assert_eq!(humanize_duration_secs(86400), "1d");
        assert_eq!(humanize_duration_secs(30 * 86400), "30d");
        assert_eq!(humanize_duration_secs(3600), "1h");
        assert_eq!(humanize_duration_secs(7200), "2h");
        assert_eq!(humanize_duration_secs(90), "90s");
        assert_eq!(humanize_duration_secs(60), "1m");
        assert_eq!(humanize_duration_secs(0), "0");
    }

    #[test]
    fn empty_engine_yields_no_recommendations() {
        let engine = PipelineEngine::new();
        let tracker = EvictionTracker::new();
        let recs = recommend_config(&engine, &tracker);
        assert!(recs.is_empty());
    }
}
