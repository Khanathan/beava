//! End-to-end smoke for the Phase 19 wiring of the `blast_shape` module into
//! `beava-bench-v18`.
//!
//! Three subprocess tests exercise the CLI with `--total-events`,
//! `--blast-shape`, and `--isolation-mode`. Each asserts the D-13 invariant
//! tuple `{requested, pushed, acked}` ends up at the same value, and that the
//! D-12 receiver-flips-stop pattern terminates the process well before the
//! `--duration-secs` safety upper bound.
//!
//! See `.planning/phases/19-1m-bench/19-02-PLAN.md` § Task 1.a for the contract.

use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

/// Path to the bench binary as set up by Cargo for `[[bin]] beava-bench-v18`.
fn bench_v18_path() -> std::path::PathBuf {
    std::path::PathBuf::from(env!("CARGO_BIN_EXE_beava-bench-v18"))
}

/// Spawn `Command`, kill if it doesn't terminate within `timeout`, and return
/// `(exit_status, stdout, stderr)`. Used as a stall guard so a test failure
/// surfaces as a normal panic rather than a hang.
fn run_with_timeout(mut cmd: Command, timeout: Duration) -> (Option<i32>, String, String) {
    cmd.stdout(Stdio::piped()).stderr(Stdio::piped());
    let mut child = cmd.spawn().expect("spawn bench-v18");
    let start = Instant::now();
    loop {
        match child.try_wait() {
            Ok(Some(status)) => {
                let out = child.wait_with_output().expect("collect output");
                return (
                    status.code(),
                    String::from_utf8_lossy(&out.stdout).to_string(),
                    String::from_utf8_lossy(&out.stderr).to_string(),
                );
            }
            Ok(None) => {
                if start.elapsed() >= timeout {
                    let _ = child.kill();
                    let out = child.wait_with_output().expect("collect output after kill");
                    let stdout = String::from_utf8_lossy(&out.stdout).to_string();
                    let stderr = String::from_utf8_lossy(&out.stderr).to_string();
                    panic!(
                        "stall — bench-v18 did not exit within {:?}\nstdout:\n{}\nstderr:\n{}",
                        timeout, stdout, stderr
                    );
                }
                std::thread::sleep(Duration::from_millis(50));
            }
            Err(e) => panic!("try_wait failed: {e}"),
        }
    }
}

#[test]
fn bench_v18_total_events_smoke_zipfian_msgpack_continuous() {
    let bin = bench_v18_path();
    let mut cmd = Command::new(&bin);
    cmd.args([
        "--total-events",
        "1000",
        "--blast-shape",
        "zipfian",
        "--transport",
        "tcp",
        "--wire-format",
        "msgpack",
        "--pipeline",
        "small",
        "--duration-secs",
        "30",
        "--parallel",
        "4",
        "--pipeline-depth",
        "16",
        "--continuous-pipeline",
        "true",
        "--no-ledger",
        "--isolation-mode",
        "--zipf-alpha",
        "1.0",
        "--cardinality",
        "100",
    ]);
    let (code, stdout, stderr) = run_with_timeout(cmd, Duration::from_secs(10));
    assert_eq!(
        code,
        Some(0),
        "exit code != 0; stdout=\n{stdout}\nstderr=\n{stderr}"
    );
    assert!(
        stderr.contains("requested=1000"),
        "missing requested=1000 in:\n{stderr}"
    );
    assert!(
        stderr.contains("pushed=1000"),
        "missing pushed=1000 in:\n{stderr}"
    );
    assert!(
        stderr.contains("acked=1000"),
        "missing acked=1000 in:\n{stderr}"
    );
    assert!(
        stderr.contains("wall_clock_ms="),
        "missing wall_clock_ms= in:\n{stderr}"
    );
    assert!(
        stderr.contains("send_drain_ms="),
        "missing send_drain_ms= in:\n{stderr}"
    );
    assert!(
        stderr.contains("ack_lag_ms="),
        "missing ack_lag_ms= in:\n{stderr}"
    );
    assert!(
        !stderr.to_lowercase().contains("stall"),
        "stderr mentions 'stall':\n{stderr}"
    );
}

#[test]
fn bench_v18_total_events_smoke_fixed_burst_json() {
    let bin = bench_v18_path();
    let mut cmd = Command::new(&bin);
    cmd.args([
        "--total-events",
        "1000",
        "--blast-shape",
        "fixed",
        "--transport",
        "tcp",
        "--wire-format",
        "json",
        "--pipeline",
        "small",
        "--duration-secs",
        "30",
        "--parallel",
        "4",
        "--pipeline-depth",
        "16",
        "--continuous-pipeline",
        "false",
        "--no-ledger",
        "--isolation-mode",
        "--cardinality",
        "100",
    ]);
    let (code, stdout, stderr) = run_with_timeout(cmd, Duration::from_secs(10));
    assert_eq!(
        code,
        Some(0),
        "exit code != 0; stdout=\n{stdout}\nstderr=\n{stderr}"
    );
    assert!(
        stderr.contains("requested=1000"),
        "missing requested=1000 in:\n{stderr}"
    );
    assert!(
        stderr.contains("pushed=1000"),
        "missing pushed=1000 in:\n{stderr}"
    );
    assert!(
        stderr.contains("acked=1000"),
        "missing acked=1000 in:\n{stderr}"
    );
    assert!(
        stderr.contains("wall_clock_ms="),
        "missing wall_clock_ms= in:\n{stderr}"
    );
    assert!(
        stderr.contains("send_drain_ms="),
        "missing send_drain_ms= in:\n{stderr}"
    );
    assert!(
        stderr.contains("ack_lag_ms="),
        "missing ack_lag_ms= in:\n{stderr}"
    );
    assert!(
        !stderr.to_lowercase().contains("stall"),
        "stderr mentions 'stall':\n{stderr}"
    );
}

#[test]
fn bench_v18_legacy_duration_path_unchanged() {
    let bin = bench_v18_path();
    let mut cmd = Command::new(&bin);
    cmd.args([
        "--duration-secs",
        "2",
        "--pipeline",
        "small",
        "--transport",
        "tcp",
        "--wire-format",
        "json",
        "--parallel",
        "2",
        "--pipeline-depth",
        "4",
        "--no-ledger",
    ]);
    let (code, stdout, stderr) = run_with_timeout(cmd, Duration::from_secs(10));
    assert_eq!(
        code,
        Some(0),
        "exit code != 0; stdout=\n{stdout}\nstderr=\n{stderr}"
    );
    assert!(
        stderr.contains("sustained_eps:"),
        "legacy run must still print 'sustained_eps:' in human summary; stderr=\n{stderr}"
    );
}
