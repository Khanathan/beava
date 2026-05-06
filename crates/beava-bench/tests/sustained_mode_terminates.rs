//! Plan 13.7.6-27 — sustained-mode hang fix + burst-vs-sustained label discipline.
//!
//! ## Bug 1 — sustained-mode hang
//!
//! `beava-bench-v18 --duration-secs N` (no `--total-events` cap) was observed
//! 2026-05-05 to hang at ~0.1% CPU for 14+ minutes before being killed.
//!
//! Root cause: in `run_tcp_continuous_push_worker`, the sender task awaits on
//! `Semaphore::acquire_owned()` to gate `pipeline_depth` in-flight pushes. The
//! receiver releases permits only on each ack (`add_permits(1)`) and closes the
//! semaphore (`sem.close()`) only when `total_cap` is reached. When the
//! deadline-only path fires (`stop = true`, no cap), neither happens — the
//! sender is left parked on `acquire_owned().await` forever, and the receiver
//! awaits the sender via `sender_handle.await` at function exit. Deadlock.
//!
//! Fix: receiver closes the semaphore before awaiting the sender, regardless
//! of whether the exit was via cap-cross or deadline.
//!
//! Test: spawn the binary with `--duration-secs 5` and no event cap, assert it
//! terminates within a generous slack window (≤ 30 s wall-clock — covers the
//! 5 s deadline + worst-case pool build + tokio teardown on slow CI).
//!
//! ## Bug 2 — burst-vs-sustained label discipline
//!
//! When `--total-events N` is passed, the run finishes in `N / EPS` seconds
//! (~1.5 s for 1M events @ 600K EPS), well before `--duration-secs` (default
//! 60). The `sustained_eps` field in the human report is misleading — the
//! number reported is a 1.5 s burst rate, not a sustained rate.
//!
//! Fix: when `elapsed < duration_secs * 0.95`, the human report emits
//! `burst_eps:` instead of `sustained_eps:`. This way the label itself flags
//! the methodology distinction; baselines committed with `sustained_eps:` were
//! genuinely run for the full duration, baselines with `burst_eps:` were
//! capped early by `--total-events`.
//!
//! Test: spawn the binary with `--total-events 100` and `--duration-secs 60`
//! (run finishes in <1 s), assert stdout contains `burst_eps:` and NOT
//! `sustained_eps:`. Then spawn with `--duration-secs 2` (no cap), assert the
//! opposite.

use std::process::Command;
use std::sync::{Mutex, MutexGuard, OnceLock};
use std::time::{Duration, Instant};

/// Each test in this file boots a full ServerV18 + spawns N workers + N×K
/// in-flight pushes. Running all three tests concurrently on a developer
/// machine (especially with `cargo test`'s default test-thread parallelism)
/// starves the runtime and produces sporadic timeouts that aren't related
/// to the bug the tests pin. Serializing them via a process-global Mutex
/// keeps the suite reliable without forcing every consumer to remember
/// `--test-threads=1`.
fn serial_lock() -> MutexGuard<'static, ()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
        .lock()
        // A poisoned lock just means a sibling test panicked — we still want
        // to run, so recover the guard.
        .unwrap_or_else(|p| p.into_inner())
}

/// Path to the release binary built by `cargo build -p beava-bench --release`.
/// Tests assume the binary already exists; tests fail fast with a clear error
/// otherwise. We don't use `assert_cmd::cargo_bin` here because a long-running
/// bench started under that machinery isn't easy to reliably bound by
/// wall-clock — we want a hard timeout, which means a `Child` we can kill.
fn bench_binary() -> std::path::PathBuf {
    // CARGO_MANIFEST_DIR points at crates/beava-bench at test compile time.
    let manifest = std::env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR set by cargo");
    let workspace = std::path::PathBuf::from(manifest)
        .parent()
        .expect("crates dir")
        .parent()
        .expect("workspace root")
        .to_path_buf();
    let bin = workspace
        .join("target")
        .join("release")
        .join("beava-bench-v18");
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
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .expect("spawn beava-bench-v18");

    // Poll for exit with a small sleep so we don't busy-spin.
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
            // Kill and report timeout.
            let _ = child.kill();
            let elapsed = start.elapsed();
            let output = child
                .wait_with_output()
                .expect("wait_with_output after kill");
            return (
                String::from_utf8_lossy(&output.stdout).to_string(),
                String::from_utf8_lossy(&output.stderr).to_string(),
                None, // None == timed out
                elapsed,
            );
        }
        std::thread::sleep(Duration::from_millis(100));
    }
}

/// Bug 1 — sustained-mode hang regression test.
///
/// Pre-13.7.6-27, this scenario hung for 14+ minutes; we cap the test wall-
/// clock at 30 s. After the fix, the bench should exit in ~5–8 s (5 s deadline
/// + pool-build + tokio teardown).
///
/// Parallelism is intentionally modest (`--parallel 4 --pipeline-depth 16`)
/// — the bench-side deadlock the fix addresses is independent of these knobs,
/// and modest values let `cargo test` run all three tests concurrently without
/// the runtime starving on a 10-core developer machine. The original
/// `--parallel 32 --pipeline-depth 1024` heavy shape exposes a separate
/// server-side starvation under extreme fan-out which is out of scope here.
#[test]
fn sustained_mode_5s_deadline_terminates_in_bounded_time() {
    let _guard = serial_lock();
    let (stdout, stderr, exit_code, elapsed) = run_with_timeout(
        &[
            "--pipeline",
            "small",
            "--transport",
            "tcp",
            "--wire-format",
            "msgpack",
            "--duration-secs",
            "5",
            "--parallel",
            "4",
            "--pipeline-depth",
            "16",
            "--no-ledger",
        ],
        Duration::from_secs(30),
    );

    assert!(
        exit_code.is_some(),
        "bench-v18 sustained-mode (--duration-secs 5, no --total-events) timed out — \
         the pre-13.7.6-27 hang has regressed. \
         elapsed={:?}\nstdout:\n{}\nstderr:\n{}",
        elapsed,
        stdout,
        stderr
    );
    assert_eq!(
        exit_code,
        Some(0),
        "bench-v18 must exit cleanly. \
         elapsed={:?} exit={:?}\nstdout:\n{}\nstderr:\n{}",
        elapsed,
        exit_code,
        stdout,
        stderr
    );
    // Should take roughly 5s + slack, definitely not 30s+.
    assert!(
        elapsed < Duration::from_secs(20),
        "bench-v18 took too long ({:?}) — deadline path may still be inefficient",
        elapsed
    );
    // Sustained-mode run must report sustained_eps (the run actually achieved
    // its full --duration-secs window).
    assert!(
        stderr.contains("sustained_eps:") || stdout.contains("sustained_eps:"),
        "5 s deadline-bound run must report sustained_eps: \
         (the duration actually elapsed). \
         stdout:\n{}\nstderr:\n{}",
        stdout,
        stderr
    );
}

/// Bug 2 — `--total-events` capped runs must report `burst_eps:` (not
/// `sustained_eps:`) when the run finished before the deadline.
///
/// 100 events @ any reasonable EPS finishes in well under 1 s; the default
/// `--duration-secs` is 60, so `elapsed / duration` is tiny — clearly a burst.
#[test]
fn total_events_capped_run_reports_burst_eps_label() {
    let _guard = serial_lock();
    let (stdout, stderr, exit_code, elapsed) = run_with_timeout(
        &[
            "--pipeline",
            "small",
            "--transport",
            "tcp",
            "--wire-format",
            "msgpack",
            "--total-events",
            "100",
            "--duration-secs",
            "60",
            "--parallel",
            "4",
            "--pipeline-depth",
            "16",
            "--no-ledger",
        ],
        Duration::from_secs(30),
    );

    assert_eq!(
        exit_code,
        Some(0),
        "bench-v18 total-events run must exit cleanly. \
         elapsed={:?} exit={:?}\nstdout:\n{}\nstderr:\n{}",
        elapsed,
        exit_code,
        stdout,
        stderr
    );
    let combined = format!("{}\n{}", stdout, stderr);
    assert!(
        combined.contains("burst_eps:"),
        "total-events run finishing well before --duration-secs MUST emit burst_eps: \
         (the methodology bug fix). stdout+stderr:\n{}",
        combined
    );
    assert!(
        !combined.contains("sustained_eps:"),
        "total-events run finishing well before --duration-secs MUST NOT emit \
         sustained_eps: (it's a burst rate, not a sustained one). stdout+stderr:\n{}",
        combined
    );
}

/// Negative case for Bug 2 — when the run actually elapses for the full
/// duration (no `--total-events`), the human report still uses `sustained_eps:`.
/// Covered by `sustained_mode_5s_deadline_terminates_in_bounded_time` already
/// — keeping a focused check here makes the contract explicit.
#[test]
fn full_duration_run_reports_sustained_eps_label() {
    let _guard = serial_lock();
    let (stdout, stderr, exit_code, _elapsed) = run_with_timeout(
        &[
            "--pipeline",
            "small",
            "--transport",
            "tcp",
            "--wire-format",
            "msgpack",
            "--duration-secs",
            "3",
            "--parallel",
            "4",
            "--pipeline-depth",
            "16",
            "--no-ledger",
        ],
        Duration::from_secs(30),
    );

    assert_eq!(exit_code, Some(0), "bench must exit cleanly");
    let combined = format!("{}\n{}", stdout, stderr);
    assert!(
        combined.contains("sustained_eps:"),
        "deadline-bound run that elapsed for the full duration MUST emit \
         sustained_eps:. stdout+stderr:\n{}",
        combined
    );
    assert!(
        !combined.contains("burst_eps:"),
        "full-duration run must NOT emit burst_eps:. stdout+stderr:\n{}",
        combined
    );
}
