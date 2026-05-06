//! Plan 13.7.6-32 — assert `beava-bench throughput --parallel N` is the
//! production harness (v18's worker-pool / Pool=N / continuous-pipeline
//! sender+receiver), not a no-op smoke surface.
//!
//! ## Background
//!
//! Plan 13.7.6-24 stripped `--parallel` from `beava-bench throughput` because
//! it was a lying flag (advertised in `--help`, silently discarded by the
//! smoke-test harness at `let _ = parallel;`). Plan 13.7.6-32 reverses that
//! choice (Option A from WEBSITE-GAPS Gap 44 — wire it through, NOT strip):
//! v18's production harness (sustained-mode + continuous-pipeline) is merged
//! into `beava-bench throughput` via `harness::production`, the v18 standalone
//! binary is deleted, and the unified `beava-bench throughput --parallel 16
//! --duration-secs 60` reproduces Plan 13.7.6-28's 660K EPS sustained baseline.
//!
//! These three tests pin the contract:
//!
//! 1. `--help` for the throughput subcommand MUST advertise `--parallel`.
//! 2. clap MUST accept `--parallel 32` without rejecting it as unknown.
//! 3. End-to-end: `beava-bench throughput --parallel 16 --duration-secs 5`
//!    against an in-process server MUST emit `sustained_eps:` AND parse to
//!    a value > 100,000 EPS — proves --parallel is genuinely wired through
//!    to the production harness (a smoke-test no-op would produce ~1K EPS
//!    on the same hardware).
//!
//! Pre-Plan-32 these all fail (subcommand rejects --parallel). Post-Plan-32
//! all three pass against the consolidated `beava-bench` binary.

use std::process::Command;
use std::sync::{Mutex, MutexGuard, OnceLock};
use std::time::{Duration, Instant};

use assert_cmd::Command as AssertCommand;

/// Each end-to-end test in this file boots a full ServerV18 + N workers + N×K
/// in-flight pushes. Running concurrent integration tests on a developer
/// machine starves the runtime and produces sporadic timeouts unrelated to
/// the bug under test. Serializing via a process-global Mutex (mirrors the
/// pattern in `sustained_mode_terminates.rs`) keeps the suite reliable
/// without forcing every consumer to remember `--test-threads=1`.
fn serial_lock() -> MutexGuard<'static, ()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap_or_else(|p| p.into_inner())
}

/// Path to the release binary built by `cargo build -p beava-bench --release`.
/// Tests assume the binary already exists; tests fail fast with a clear error
/// otherwise. We don't use `assert_cmd::cargo_bin` for the long-running
/// integration test because we want a hard wall-clock kill via `Child::kill`.
fn bench_binary() -> std::path::PathBuf {
    let manifest = std::env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR set by cargo");
    let workspace = std::path::PathBuf::from(manifest)
        .parent()
        .expect("crates dir")
        .parent()
        .expect("workspace root")
        .to_path_buf();
    let bin = workspace.join("target").join("release").join("beava-bench");
    assert!(
        bin.exists(),
        "release binary not found at {} — run `cargo build -p beava-bench --release` first",
        bin.display()
    );
    bin
}

/// Run the binary with the given args; kill if it doesn't exit within
/// `wall_clock_limit`. Returns `(stdout, stderr, exit_code, elapsed)`.
fn run_with_timeout(
    args: &[&str],
    wall_clock_limit: Duration,
) -> (String, String, Option<i32>, Duration) {
    let start = Instant::now();
    let mut child = Command::new(bench_binary())
        .args(args)
        // beava-bench-v18 reads `./configs/{pipeline}.json` relative to CWD,
        // and the consolidated `beava-bench throughput` migrated harness
        // inherits that lookup behaviour. Tests run from CARGO_MANIFEST_DIR
        // (= crates/beava-bench), where `configs/` lives.
        .current_dir(std::env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR"))
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .expect("spawn beava-bench");

    loop {
        if let Some(status) = child.try_wait().expect("try_wait") {
            let elapsed = start.elapsed();
            let output = child.wait_with_output().expect("wait_with_output");
            return (
                String::from_utf8_lossy(&output.stdout).to_string(),
                String::from_utf8_lossy(&output.stderr).to_string(),
                status.code(),
                elapsed,
            );
        }
        if start.elapsed() >= wall_clock_limit {
            let _ = child.kill();
            let elapsed = start.elapsed();
            let output = child
                .wait_with_output()
                .expect("wait_with_output after kill");
            return (
                String::from_utf8_lossy(&output.stdout).to_string(),
                String::from_utf8_lossy(&output.stderr).to_string(),
                None,
                elapsed,
            );
        }
        std::thread::sleep(Duration::from_millis(100));
    }
}

/// Test 1 — `--help` must advertise `--parallel` again (reverses Plan 24).
#[test]
fn throughput_help_advertises_parallel_flag() {
    let mut cmd = AssertCommand::cargo_bin("beava-bench").unwrap();
    cmd.args(["throughput", "--help"]);
    let assert = cmd.assert().success();
    let stdout = String::from_utf8(assert.get_output().stdout.clone()).unwrap();
    assert!(
        stdout.contains("--parallel"),
        "throughput --help MUST advertise --parallel \
         (Plan 13.7.6-32 reverses Plan 24's strip — production harness migrated \
         into the throughput subcommand). got:\n{stdout}"
    );
}

/// Test 2 — clap must accept `--parallel 32` without rejecting it as unknown.
///
/// We use `--help` after `--parallel 32` so the run is fast and side-effect
/// free; if the flag parses, clap's --help short-circuit fires and we get
/// a successful exit. If the flag is unknown (Plan 24's contract), clap
/// rejects with non-zero exit and a `unexpected argument` / `unrecognized` /
/// `--parallel` error.
#[test]
fn throughput_accepts_parallel_argument() {
    let mut cmd = AssertCommand::cargo_bin("beava-bench").unwrap();
    cmd.args(["throughput", "--parallel", "32", "--help"]);
    let output = cmd.output().expect("spawn beava-bench throughput --help");
    let stderr = String::from_utf8_lossy(&output.stderr);
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        output.status.success(),
        "beava-bench throughput --parallel 32 --help MUST succeed (clap accepts the flag). \
         exit={:?}\nstdout:\n{stdout}\nstderr:\n{stderr}",
        output.status.code()
    );
    assert!(
        !stderr.contains("unexpected argument") && !stderr.contains("unrecognized"),
        "clap MUST NOT reject --parallel as unknown post-Plan-32. \
         stderr:\n{stderr}\nstdout:\n{stdout}"
    );
}

/// Test 3 — full integration: spawn `beava-bench throughput --parallel 16
/// --duration-secs 5 ...` against an in-process server, assert
/// `sustained_eps:` is in the output AND the parsed value is > 100,000 EPS.
///
/// 100K EPS is a generous floor: Plan 13.7.6-28's Apple-M4 baseline reports
/// ~660K EPS for `--parallel 16 --duration-secs 60` on small/tcp; even on
/// CI hardware with ~5x lower throughput the signal is unambiguous (smoke-
/// test no-op produces ~1K EPS). The 5 s window keeps the test bounded.
///
/// Failing this test means either:
///   - `--parallel` is still being silently discarded (Plan 24's bug
///     resurfaced), or
///   - the production harness is not wired into `beava-bench throughput`
///     (Plan 32's migration regressed), or
///   - the `sustained_eps:` label discipline (Plan 27) was not preserved
///     across the migration.
///
/// `#[ignore]` because this test requires the **release** binary at
/// `target/release/beava-bench` (debug mode is 10-100× slower and won't
/// produce meaningful sustained_eps in 5 seconds). Default `cargo test`
/// only builds debug artifacts, so a fresh clone would fail this test
/// without first running `cargo build -p beava-bench --release`. Run via:
///
///     cargo build -p beava-bench --release
///     cargo test -p beava-bench --test parallel_wired_through -- --ignored
///
/// CI invokes both steps explicitly. Local development runs default
/// `cargo test` and skips this test (the contract is still locked by
/// the two non-ignored sibling tests above + Plan 13.7.6-32 SUMMARY's
/// end-to-end measurement gate).
#[test]
#[ignore = "requires target/release/beava-bench; run with --ignored after `cargo build --release`"]
fn throughput_parallel_produces_sustained_eps() {
    let _guard = serial_lock();
    let (stdout, stderr, exit_code, elapsed) = run_with_timeout(
        &[
            "throughput",
            "--pipeline",
            "small",
            "--transport",
            "tcp",
            "--wire-format",
            "msgpack",
            "--duration-secs",
            "5",
            "--parallel",
            "16",
            "--pipeline-depth",
            "256",
            "--no-ledger",
        ],
        Duration::from_secs(60),
    );

    assert!(
        exit_code.is_some(),
        "beava-bench throughput --parallel 16 --duration-secs 5 timed out \
         (took {:?}). The bench should run for ~5-10 s; a timeout means \
         either the production harness deadlocked or --parallel hangs.\nstdout:\n{}\nstderr:\n{}",
        elapsed,
        stdout,
        stderr
    );
    assert_eq!(
        exit_code,
        Some(0),
        "beava-bench throughput must exit cleanly. \
         elapsed={:?} exit={:?}\nstdout:\n{}\nstderr:\n{}",
        elapsed,
        exit_code,
        stdout,
        stderr
    );

    let combined = format!("{}\n{}", stdout, stderr);
    assert!(
        combined.contains("sustained_eps:"),
        "5 s deadline-bound run with --parallel 16 MUST report sustained_eps: \
         (Plan 27's label discipline preserved across migration). \
         stdout+stderr:\n{}",
        combined
    );

    // Parse the EPS value from the `sustained_eps:` line. Format (per
    // format_report in the migrated harness):
    //   sustained_eps: 657797
    let eps = parse_sustained_eps(&combined).unwrap_or_else(|| {
        panic!(
            "could not parse sustained_eps: value from output. \
             stdout+stderr:\n{}",
            combined
        )
    });
    assert!(
        eps > 100_000.0,
        "sustained_eps={} is below 100K floor — --parallel 16 produced smoke-test \
         throughput, which means --parallel is not actually wired through to the \
         production harness. Plan 13.7.6-28 baseline is 660K EPS on Apple-M4 \
         small/tcp; CI hardware should clear 100K easily. \
         stdout+stderr:\n{}",
        eps,
        combined
    );
}

/// Parse the numeric value from the `sustained_eps:` line of the human report.
/// Returns `None` if the line is missing or unparseable.
fn parse_sustained_eps(combined: &str) -> Option<f64> {
    for line in combined.lines() {
        let trimmed = line.trim_start();
        if let Some(rest) = trimmed.strip_prefix("sustained_eps:") {
            let value = rest.trim();
            if let Ok(n) = value.parse::<f64>() {
                return Some(n);
            }
        }
    }
    None
}
