//! Phase 18 Plan 01 — Task 1.6 samply profiling gate test.
//!
//! This test is intentionally `#[ignore]` — it requires a manual samply run
//! and cannot be automated in CI. It documents the performance assertion that
//! the hand-rolled event loop's reactor + scheduler overhead must be < 15% of
//! total CPU (down from 43% measured on tokio Phase 13.3 baseline).
//!
//! ## How to run
//!
//! See `.planning/phases/18-redis-hand-roll/18-01-perf-profile.md` for the
//! full procedure. Quick reference:
//!
//! ```bash
//! # 1. Build with debug symbols
//! cargo build --release --features hand-rolled-runtime -p beava-bench
//!
//! # 2. Profile with samply
//! samply record cargo bench -p beava-bench \
//!   --features hand-rolled-runtime \
//!   -- --pipeline small --transport http --duration-secs 30
//!
//! # 3. Open the samply report and locate the "mio::Poll::poll" + scheduler
//! #    frames; sum their % of total CPU samples.
//! #    Pass criterion: sum < 15%.
//! ```
//!
//! TDD: this file is the RED commit for Task 1.6. The test is always `#[ignore]`
//! but it encodes the assertion contract as a doc-level gate.

/// Asserts that samply profiling shows the reactor + scheduler cost is < 15%
/// of total CPU samples on a 30-second hand-rolled bench run.
///
/// **This test must be run manually — it is always `#[ignore]` in CI.**
///
/// Procedure: run the bench under samply (see module doc), read the
/// `mio::Poll::poll` + tokio-scheduler frame percentages, verify sum < 15%.
///
/// Phase 13.3 tokio baseline (M4 MacBook Pro): reactor + scheduler ≈ 43%.
/// Phase 18 target: < 15% (the hand-rolled reactor eliminates the async
/// task-spawn overhead per event that drives tokio's cost).
#[test]
#[ignore = "manual samply run required — see 18-01-perf-profile.md"]
fn samply_reactor_cost_under_15_percent() {
    // This test body is intentionally a panic with instructions.
    // A human reviewer runs samply, reads the flame graph, and passes/fails
    // this gate by examining the profile manually.
    //
    // When the profile shows reactor_pct < 15.0, the gate passes.
    // Update the baseline in .planning/perf-baselines.md with the measured value.
    panic!(
        "Manual gate: run samply per 18-01-perf-profile.md, \
         verify mio::Poll::poll + scheduler frames < 15% of CPU samples. \
         Phase 13.3 tokio baseline: ~43%. Phase 18 target: < 15%."
    );
}
