//! Phase 58 Wave 0 RED: asserts the samply-probed TCP PUSH path has
//! `tokio::runtime::task::*` share ≤ 15 % of leaf samples (TPC-PERF-08 D-C4).
//!
//! Ignore marker: `#[ignore = "58-W1"]` — flips GREEN at Wave 1 (Linux) /
//! Wave 2 (macOS). Wave 4's perf gate cross-checks via the same probe.
//!
//! Invocation:
//!   cargo test --release --test tokio_spawn_absence_smoke -- --ignored
//!
//! Wave-0 expected outcome: FAIL.
//!   - Either `TOKIO_SHARE_PCT > 15` (today's tokio-per-conn-spawn path
//!     pushes the share up around ~60 % per Phase 54 pprof notes), OR
//!   - `TOKIO_SHARE_PCT=unknown` if the operator has no `samply` CLI —
//!     we panic with an actionable install hint, NOT silently skip. The
//!     Phase 58 D-C4 gate is not bypassable by a missing toolchain.
//!
//! Helper script: `scripts/samply-probe-tokio-share.sh`. It wraps
//! `tests/profile_ingest.rs` and emits a final `TOKIO_SHARE_PCT=<num>`
//! line. See that script's --help for details.

use std::process::Command;

/// TPC-PERF-08 D-C4 gate: `tokio::runtime::task` combined leaf-sample share
/// MUST be ≤ 15 % on the TCP PUSH profile.
#[test]
#[ignore = "58-W1"]
fn tokio_share_on_push_path_under_15_pct() {
    // The smoke test spawns the in-repo probe script. Running it via `bash`
    // avoids a PATH-dependent shebang resolution and lets us capture
    // stderr alongside stdout for diagnostics.
    let out = Command::new("bash")
        .arg("scripts/samply-probe-tokio-share.sh")
        .output()
        .expect(
            "scripts/samply-probe-tokio-share.sh must exist + be executable. \
             Task 1 of Phase 58 Wave 0 commits it; see the plan.",
        );

    assert!(
        out.status.success(),
        "probe script exited non-zero.\n--- stdout ---\n{}\n--- stderr ---\n{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );

    let stdout = String::from_utf8_lossy(&out.stdout);
    let pct_line = stdout
        .lines()
        .rev()
        .find(|l| l.starts_with("TOKIO_SHARE_PCT="))
        .unwrap_or_else(|| {
            panic!(
                "probe script did not emit a `TOKIO_SHARE_PCT=<val>` line. \
                 Full stdout:\n{}",
                stdout
            )
        });

    let pct_str = pct_line.trim_start_matches("TOKIO_SHARE_PCT=").trim();

    // Samply-missing case: we PANIC with a hint instead of silently passing.
    // Phase 58 D-C4 is a hard gate — a missing CLI must not paper over the
    // failure mode the test exists to catch.
    if pct_str == "unknown" {
        panic!(
            "TOKIO_SHARE_PCT=unknown (samply CLI not installed on this host, \
             or the profile harness did not emit /tmp/beava_ingest.top.txt). \
             Install via `cargo install samply` and re-run. The Phase 58 \
             TPC-PERF-08 D-C4 gate does NOT silent-skip on missing tooling."
        );
    }

    let pct: f64 = pct_str.parse().unwrap_or_else(|_| {
        panic!(
            "unparseable TOKIO_SHARE_PCT value {pct_str:?} \
             (expected a float like '12.3' or the literal 'unknown')"
        )
    });

    // ------------------------------------------------------------------
    // Probe-coverage sentinel (Phase 58 Wave 0 RED mechanism).
    //
    // Today's `tests/profile_ingest.rs` harness calls `handle_push_batch`
    // directly on 8 OS threads — it never goes through the TCP accept
    // loop, so `tokio::runtime::task::*` frames are essentially absent
    // (~0.0 % self-samples) regardless of whether tokio per-conn spawn
    // is in use on the real TCP PUSH path.
    //
    // That makes a naive "TOKIO_SHARE_PCT ≤ 15" gate trivially GREEN on
    // today's harness — it would NOT catch a Wave-1 regression that
    // reintroduces `tokio::spawn` per connection, because the probe
    // never observes the TCP accept path.
    //
    // Wave-0 RED contract (D-C1): fail the test UNTIL the probe has been
    // extended to drive real TCP connections through the push loop and
    // produce non-trivial tokio runtime-task self-sample coverage. The
    // threshold is intentionally generous (1.0 %) — any real TCP+tokio
    // activity will blow past it, while a pure direct-`handle_push_batch`
    // harness stays at 0.0 %.
    //
    // Wave 1 (Linux) introduces per-shard `current_thread` runtimes +
    // `FuturesUnordered` accept loops; Wave 4's perf gate re-runs this
    // probe over a real TCP driver and flips both the coverage sentinel
    // AND the ≤ 15 % share gate GREEN simultaneously.
    const COVERAGE_FLOOR_PCT: f64 = 1.0;
    assert!(
        pct >= COVERAGE_FLOOR_PCT,
        "TPC-PERF-08 D-C4 probe-coverage sentinel FAIL: \
         TOKIO_SHARE_PCT={pct:.1}% is below the {COVERAGE_FLOOR_PCT:.1}% coverage floor. \
         This means the samply probe harness is NOT exercising the TCP accept / \
         tokio runtime-task path — the gate is not load-bearing. Wave 1 must \
         extend scripts/samply-probe-tokio-share.sh (or the upstream \
         tests/profile_ingest.rs harness) to drive PUSH traffic over a real \
         `TcpStream`, so `tokio::runtime::task::*` frames actually appear in \
         the profile, before the ≤ 15 %% ceiling is meaningful. \
         (Wave 0 RED signal per D-C1.)"
    );

    // Phase 58 gate: once the coverage sentinel is satisfied, enforce the
    // ≤ 15 % tokio-task leaf-sample ceiling. Pre-Wave-1 baseline ≈ 60 %
    // per Phase 54 pprof; Wave 4 perf gate re-runs under full load.
    assert!(
        pct <= 15.0,
        "TPC-PERF-08 D-C4 gate FAIL: TOKIO_SHARE_PCT={pct:.1}% exceeds 15 % ceiling. \
         tokio per-conn task churn is not yet eliminated from the TCP PUSH path. \
         Expected (Wave 4 close): ≤ 15 %."
    );
}
