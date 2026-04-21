//! Phase 57 Wave 4 — BEAVA_PERF_GATE=1 env-gated EPS-floor check for the
//! retraction hot path.
//!
//! Mirrors `tests/crossshard_enrich_perf_smoke.rs::crossshard_enrich_eps_floor`
//! from Phase 56 — subprocess-invokes `benchmark/fraud-pipeline/run_bench.sh`
//! with the default fraud-pipeline scenario (ZERO retractions firing, per
//! Phase 57 D-D3 contract: the perf floor is the write-path overhead of the
//! contributing_inputs tracking + tombstone-detection branch when no
//! retraction actually executes).
//!
//! The Phase 57 floor is **1,076,322 EPS** — 90 % of the Phase 56 close
//! baseline of 1,195,914 EPS (see `.planning/phases/56-.../56-PERF-GATE.md`).
//! The 10 % overhead budget is the user's locked non-negotiable — see
//! `.planning/phases/57-.../57-04-PLAN.md <user_decision_fidelity>`.
//!
//! Gated by `BEAVA_PERF_GATE=1` so normal `cargo test --release` runs skip
//! it; the bench drives an 8-client × 8-worker fabric for 65 s (5 s warmup
//! + 60 s measurement) which would dominate CI time and conflict with any
//! concurrent server on TCP 6400.

use std::process::Command;

/// Phase 56 close measured baseline (default fraud pipeline, 60 s window).
#[allow(dead_code)]
const PHASE_56_BASELINE_EPS: u64 = 1_195_914;

/// Phase 57 floor — 90 % of Phase 56 baseline (D-D3 — user-locked, non-
/// negotiable).
const PHASE_57_FLOOR_EPS: u64 = 1_076_322;

/// Extracted from Phase 56 bench harness — per-1000-event client batch
/// p99 latency observed at Phase 56 close. Not a hard gate here (smoke
/// scope is aggregate EPS), but documented so the Phase 58 Tokio-rewrite
/// plan has a concrete number to improve on.
#[allow(dead_code)]
const BASELINE_P99_MICROS: u64 = 28_224;

/// Default perf-gate invocation (matches Phase 56 gate + the 57-04 plan).
const PERF_GATE_MODE: &str = "complex";
const PERF_GATE_DURATION: &str = "60";
const PERF_GATE_CPUS: &str = "8";
const PERF_GATE_CLIENTS: &str = "8";
const PERF_GATE_INBOX_SIZE: &str = "1048576";

fn parse_aggregate_eps(stdout: &str) -> Option<u64> {
    // `benchmark/fraud-pipeline/run_bench.sh` emits a machine-parseable
    // `Aggregate EPS: <N>` line (added in Phase 56 Wave 4).
    for line in stdout.lines() {
        if let Some(rest) = line.trim_start().strip_prefix("Aggregate EPS:") {
            if let Ok(n) = rest.trim().parse::<u64>() {
                return Some(n);
            }
        }
    }
    None
}

#[test]
#[ignore = "perf-gate-only: set BEAVA_PERF_GATE=1 to run the 65-s subprocess bench"]
fn retraction_eps_floor() {
    if std::env::var("BEAVA_PERF_GATE").is_err() {
        eprintln!("BEAVA_PERF_GATE not set — skipping (no-op).");
        return;
    }

    let output = Command::new("bash")
        .args(["benchmark/fraud-pipeline/run_bench.sh"])
        .env("MODE", PERF_GATE_MODE)
        .env("DURATION", PERF_GATE_DURATION)
        .env("CPUS", PERF_GATE_CPUS)
        .env("CLIENTS", PERF_GATE_CLIENTS)
        .env("BEAVA_SHARD_INBOX_SIZE", PERF_GATE_INBOX_SIZE)
        .output()
        .expect("failed to invoke benchmark/fraud-pipeline/run_bench.sh");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    let aggregate = parse_aggregate_eps(&stdout).unwrap_or_else(|| {
        panic!(
            "no 'Aggregate EPS: <N>' line found in run_bench.sh stdout.\n\
             --- stdout (tail) ---\n{}\n--- stderr (tail) ---\n{}",
            stdout.lines().rev().take(40).collect::<Vec<_>>().join("\n"),
            stderr.lines().rev().take(40).collect::<Vec<_>>().join("\n"),
        );
    });

    assert!(
        aggregate >= PHASE_57_FLOOR_EPS,
        "Phase 57 EPS floor breach: candidate = {} EPS; floor = {} EPS (= 90 % of Phase 56 \
         baseline {} EPS). The contributing_inputs tracking + tombstone-detection branch \
         introduced by Phase 57 is costing >10 % on the zero-retractions-firing write path. \
         See 57-PERF-GATE.md contingency ladder (C1 batch coalesce / C2 inline fast-check / \
         C3 human_needed).",
        aggregate, PHASE_57_FLOOR_EPS, PHASE_56_BASELINE_EPS,
    );

    eprintln!(
        "Phase 57 perf gate PASSED: candidate = {} EPS >= floor {} EPS (+{:.1}% headroom; \
         delta vs Phase 56 baseline {} EPS = {:+.1}%)",
        aggregate,
        PHASE_57_FLOOR_EPS,
        ((aggregate as f64 - PHASE_57_FLOOR_EPS as f64) / PHASE_57_FLOOR_EPS as f64) * 100.0,
        PHASE_56_BASELINE_EPS,
        ((aggregate as f64 - PHASE_56_BASELINE_EPS as f64) / PHASE_56_BASELINE_EPS as f64)
            * 100.0,
    );
}
