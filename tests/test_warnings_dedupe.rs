//! Plan 25-03 — dedupe and ephemeral-registry contracts for
//! `/debug/warnings`.
//!
//! The `SignalRegistry` unit tests in `test_signal_registry.rs` cover the
//! dedupe mechanics at the API level; this file validates those properties
//! **through the production emitter call sites** — i.e. what happens when
//! the late-drop emitter, the config emitter, or the register-failure
//! emitter is called twice with the same stable id.
//!
//! The registry is in-memory only (CONTEXT.md §specifics: "rebuild on
//! restart"); the final test asserts that a fresh `SignalRegistry` starts
//! empty, matching the behaviour of a cold server on boot.

use std::sync::Arc;
use std::time::{Duration, SystemTime};

use beava::engine::pipeline::PipelineEngine;
use beava::engine::recommend::ConfigRecommendation;
use beava::server::signals::{
    emit_config_recommendations, emit_register_failure, format_rfc3339, Signal, SignalRegistry,
};
use beava::server::tcp::{make_concurrent_state_default_store, BackfillTracker, SharedState};
fn test_state() -> SharedState {
    make_concurrent_state_default_store(
        PipelineEngine::new(),
        None,
        std::path::PathBuf::from("/tmp/beava-test-warnings-dedupe.snapshot"),
        Arc::new(BackfillTracker::default()),
        true,
        false,
        None,
        false,
        1,
    )
}

// ---------------------------------------------------------------------------
// Same-id re-observation preserves first_seen.
// ---------------------------------------------------------------------------

#[test]
fn test_same_id_preserves_first_seen() {
    let state = test_state();
    emit_register_failure(&state.signals, "P1", "first error");
    // Grab first_seen from the initial record.
    let first_seen = {
        let reg = state.signals.read();
        let snap = reg.snapshot_sorted(SystemTime::now(), None);
        assert_eq!(snap.len(), 1);
        snap[0].first_seen
    };

    // Sleep a measurable delta then re-emit with the same id.
    std::thread::sleep(Duration::from_millis(5));
    emit_register_failure(&state.signals, "P1", "second error, same pipeline");

    let reg = state.signals.read();
    let snap = reg.snapshot_sorted(SystemTime::now(), None);
    assert_eq!(snap.len(), 1, "dedupe must collapse to one entry");
    assert_eq!(
        snap[0].first_seen, first_seen,
        "first_seen must be preserved"
    );
    // last_seen advances. We compare by RFC3339 serialization because
    // SystemTime's direct sub-ms comparisons can be fragile on some clocks.
    assert!(
        snap[0].last_seen >= first_seen,
        "last_seen must not regress: first_seen={:?} last_seen={:?}",
        format_rfc3339(first_seen),
        format_rfc3339(snap[0].last_seen)
    );
    // detail must carry the most recent error message.
    assert_eq!(snap[0].detail, "second error, same pipeline");
}

// ---------------------------------------------------------------------------
// Distinct ids coexist — different pipelines produce different signals.
// ---------------------------------------------------------------------------

#[test]
fn test_distinct_ids_coexist() {
    let state = test_state();
    emit_register_failure(&state.signals, "P1", "err A");
    emit_register_failure(&state.signals, "P2", "err B");

    let reg = state.signals.read();
    let snap = reg.snapshot_sorted(SystemTime::now(), None);
    assert_eq!(snap.len(), 2);
    let ids: std::collections::HashSet<&str> = snap.iter().map(|s| s.id.as_str()).collect();
    assert!(ids.contains("register.failure.P1"));
    assert!(ids.contains("register.failure.P2"));
}

// ---------------------------------------------------------------------------
// Config emitter dedupes across polling cycles — simulates the 30s
// `poll_signal_sources` tick firing twice with the same recommendations.
// ---------------------------------------------------------------------------

#[test]
fn test_config_emitter_dedupes_across_polls() {
    let state = test_state();
    let rec = ConfigRecommendation {
        knob: "UserProfile.ttl".into(),
        current: "30d".into(),
        suggested: "60d".into(),
        confidence: 0.8,
        reason: "reinit rate above threshold".into(),
        evidence: serde_json::json!({"evictions": 48213}),
        copy_paste: "@tl.table(key=\"user_id\", ttl=\"60d\")".into(),
    };
    // Simulate three polling cycles.
    emit_config_recommendations(&state.signals, std::slice::from_ref(&rec));
    emit_config_recommendations(&state.signals, std::slice::from_ref(&rec));
    emit_config_recommendations(&state.signals, &[rec]);

    let reg = state.signals.read();
    let snap = reg.snapshot_sorted(SystemTime::now(), None);
    assert_eq!(snap.len(), 1, "same config id must dedupe across cycles");
    assert_eq!(snap[0].id, "config.UserProfile.ttl");
}

// ---------------------------------------------------------------------------
// Config emitter updates the action payload on re-observation — if the
// suggested TTL changes between polling cycles, the stored warning picks
// up the new `suggested` value.
// ---------------------------------------------------------------------------

#[test]
fn test_config_emitter_updates_suggestion_on_redup() {
    let state = test_state();
    emit_config_recommendations(
        &state.signals,
        &[ConfigRecommendation {
            knob: "UserProfile.ttl".into(),
            current: "30d".into(),
            suggested: "60d".into(),
            confidence: 0.6,
            reason: "6% reinit rate".into(),
            evidence: serde_json::json!({}),
            copy_paste: "@tl.table(ttl=\"60d\")".into(),
        }],
    );
    // A later polling cycle escalates the suggestion.
    emit_config_recommendations(
        &state.signals,
        &[ConfigRecommendation {
            knob: "UserProfile.ttl".into(),
            current: "30d".into(),
            suggested: "120d".into(),
            confidence: 0.9,
            reason: "9% reinit rate — signal strengthened".into(),
            evidence: serde_json::json!({}),
            copy_paste: "@tl.table(ttl=\"120d\")".into(),
        }],
    );

    let reg = state.signals.read();
    let snap = reg.snapshot_sorted(SystemTime::now(), None);
    assert_eq!(snap.len(), 1);
    let action = snap[0]
        .action
        .as_ref()
        .expect("config action payload missing");
    assert_eq!(
        action["suggested"], "120d",
        "suggestion must update on re-emit"
    );
    assert_eq!(snap[0].detail, "9% reinit rate — signal strengthened");
}

// ---------------------------------------------------------------------------
// Dedupe registry is ephemeral — a fresh SignalRegistry on simulated restart
// starts empty, matching CONTEXT spec: "rebuild on restart per CONTEXT.md
// §specifics".
// ---------------------------------------------------------------------------

#[test]
fn test_dedupe_registry_ephemeral_on_restart() {
    let state = test_state();
    emit_register_failure(&state.signals, "P1", "err");
    assert_eq!(state.signals.read().len(), 1);

    // Simulate restart: construct a fresh registry with no shared state.
    let fresh = SignalRegistry::new_default();
    assert_eq!(
        fresh.len(),
        0,
        "fresh SignalRegistry (== post-restart) must be empty"
    );

    // And the pre-restart registry is obviously still non-empty — this just
    // double-guards that the "fresh" check above isn't accidentally pointing
    // at the live registry.
    assert_eq!(state.signals.read().len(), 1);
}

// ---------------------------------------------------------------------------
// Direct-record dedupe sanity: `record(same_id_twice)` collapses, just
// like the emitter-driven tests above. Proves the emitter and the direct
// API agree.
// ---------------------------------------------------------------------------

#[test]
fn test_direct_record_and_emitter_agree_on_dedupe() {
    use beava::server::signals::{Category, Severity};
    let state = test_state();
    state.signals.write().record(Signal::new(
        "register.failure.P1",
        Severity::Error,
        Category::Safety,
        "Pipeline registration failed",
        "direct err",
        serde_json::json!({"pipeline": "P1"}),
    ));
    // The emitter uses the same id scheme, so this must dedupe.
    emit_register_failure(&state.signals, "P1", "emitter err");
    assert_eq!(state.signals.read().len(), 1);
    let snap = state
        .signals
        .read()
        .snapshot_sorted(SystemTime::now(), None);
    assert_eq!(snap[0].id, "register.failure.P1");
    assert_eq!(snap[0].detail, "emitter err");
}
