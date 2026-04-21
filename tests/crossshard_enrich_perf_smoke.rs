//! Phase 56 SC-4 + SC-5 — cross-shard EnrichFromTable perf smoke + gate.
//!
//! SC-4: p99 latency of a 10_000-event synthetic workload with cross-shard
//! enrichments MUST be ≤ 2 × `BASELINE_P99_MICROS`. The baseline is a
//! Phase-55 spot measurement (50 µs — the engineering baseline at Phase 55
//! close; will be re-measured during Wave 4 of Phase 56 and this constant
//! updated in-place if the true baseline shifts).
//!
//! SC-5: the benchmark harness `benchmark/fraud-pipeline/run_bench.sh` with
//! `MODE=complex DURATION=60 CPUS=8 CLIENTS=8 BEAVA_SHARD_INBOX_SIZE=1048576`
//! + forced cross-shard enrichment scenario MUST report Aggregate EPS
//! ≥ 1_059_261 (85% of the Phase 55 perf-gate candidate 1_246_190 EPS).
//! Gated by `BEAVA_PERF_GATE=1` env var.
//!
//! RED until Wave 4 (plan 56-04) finalizes the perf-gate envelope.
//! Passes at Wave 4.
//!
//! Run:
//!   cargo test --release --test crossshard_enrich_perf_smoke -- --ignored --test-threads=1
//!   BEAVA_PERF_GATE=1 cargo test --release --test crossshard_enrich_perf_smoke crossshard_enrich_eps_floor -- --ignored --test-threads=1

#![cfg(not(feature = "state-inmem"))]

/// Phase-55 baseline p99 (engineering spot measurement). Wave 4 re-measures
/// and updates this constant in-place; the test's 2× tolerance factor stays.
#[allow(dead_code)]
const BASELINE_P99_MICROS: u64 = 50;

/// Phase 55 perf-gate candidate (committed at Phase 55 close = 1_246_190 EPS).
/// SC-5 floor = 85% of baseline = 1_059_261 EPS.
#[allow(dead_code)]
const PHASE_55_EPS_BASELINE: u64 = 1_246_190;
#[allow(dead_code)]
const SC5_EPS_FLOOR: u64 = 1_059_261;

/// SC-4 primary — synthetic 10K-event workload with forced cross-shard
/// enrichment. Measure per-event wall-clock latency, compute p99, assert
/// p99 ≤ 2 × BASELINE_P99_MICROS.
///
/// Budget (T-56-00-03 mitigation): entire test MUST complete in ≤ 5 seconds
/// of wall-clock time; fails fast if exceeded (unbounded loops in the
/// operator hot path would exceed this budget and are caught here).
///
/// Wave 4 assertion hooks:
///   - Spawn 4 shards, register 10 source-tables.
///   - Register `Txns(shard_key=user_id)` stream with 5 EnrichFromTable
///     features joining against 5 of the source-tables (all forced cross-shard
///     via key-space partition).
///   - UPSERT 1_000 rows into each source-table.
///   - Push 10_000 events, collect per-event wall-clock latencies into a
///     `Vec<u64>` of microseconds.
///   - Sort, pick p99 (index 9899).
///   - Assert p99_micros ≤ 2 × BASELINE_P99_MICROS.
///   - Assert total wall-clock ≤ 5_000_000 µs (5 s budget).
#[test]
#[ignore = "56-W4"]
fn crossshard_enrich_p99_under_2x_baseline() {
    // Wave 4 wiring:
    //   1. Spawn 4-shard engine via the standard harness.
    //   2. Register 10 source tables with per-table key spaces.
    //   3. UPSERT 1_000 rows/table, spread across all 4 shards.
    //   4. Register Txns stream with 5 enrich features (pick keys so
    //      every push is cross-shard for at least one enrichment).
    //   5. Loop 10_000 times:
    //         let t0 = Instant::now();
    //         engine.push(&Txn_event);
    //         latencies.push(t0.elapsed().as_micros() as u64);
    //   6. latencies.sort_unstable(); let p99 = latencies[9899];
    //   7. assert!(p99 <= 2 * BASELINE_P99_MICROS, "p99={p99}µs > {}", 2 * BASELINE_P99_MICROS);
    //   8. assert!(start.elapsed().as_secs() < 5, "exceeded 5s wall-clock budget");
    todo!(
        "56-W4: wire 10K-event cross-shard enrich perf smoke. Baseline p99 \
         = {BASELINE_P99_MICROS} µs; acceptance: p99 <= 2×baseline."
    );
}

/// SC-5 — the ship-gate perf check. Runs the full fraud-pipeline bench
/// harness and asserts Aggregate EPS ≥ SC5_EPS_FLOOR (1_059_261). Gated
/// by `BEAVA_PERF_GATE=1` because it invokes the external bench script.
///
/// Wave 4 assertion hooks:
///   - `std::env::var("BEAVA_PERF_GATE") == Ok("1")` — early-return if unset.
///   - `std::process::Command::new("bash").arg("benchmark/fraud-pipeline/run_bench.sh")`
///     with envs: MODE=complex DURATION=60 CPUS=8 CLIENTS=8
///     BEAVA_SHARD_INBOX_SIZE=1048576 BEAVA_ENRICH_CROSSSHARD_SCENARIO=1.
///   - Parse stdout for `"Aggregate EPS: <N>"`.
///   - Assert N ≥ SC5_EPS_FLOOR.
#[test]
#[ignore = "56-W4"]
fn crossshard_enrich_eps_floor() {
    if std::env::var("BEAVA_PERF_GATE").ok().as_deref() != Some("1") {
        eprintln!("skip: BEAVA_PERF_GATE != 1");
        return;
    }

    // Wave 4 wiring:
    //   1. let out = Command::new("bash")
    //          .arg("benchmark/fraud-pipeline/run_bench.sh")
    //          .env("MODE", "complex")
    //          .env("DURATION", "60")
    //          .env("CPUS", "8")
    //          .env("CLIENTS", "8")
    //          .env("BEAVA_SHARD_INBOX_SIZE", "1048576")
    //          .env("BEAVA_ENRICH_CROSSSHARD_SCENARIO", "1")
    //          .output()
    //          .expect("run_bench.sh must be runnable");
    //   2. let stdout = String::from_utf8_lossy(&out.stdout);
    //   3. Parse "Aggregate EPS: <N>" via regex.
    //   4. Assert N >= SC5_EPS_FLOOR (1_059_261).
    todo!(
        "56-W4: wire perf-gate harness invocation. Contract: aggregate \
         EPS >= {SC5_EPS_FLOOR} (85% of Phase 55 baseline {PHASE_55_EPS_BASELINE})."
    );
}
