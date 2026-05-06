//! Plan 13.7.6-24 — assert `--parallel` is gone from the `beava-bench
//! throughput` subcommand.
//!
//! Pre-13.7.6-24, `beava-bench throughput --parallel N` advertised
//! "Number of concurrent push workers (default 16)" but the harness
//! discarded the value at `crates/beava-bench/src/harness/mod.rs:44-46`
//! with a `let _ = parallel;` no-op. Result: `--parallel 32` ran the
//! exact same single-threaded loop as `--parallel 1`, producing ~1K
//! EPS instead of the ~125K EPS the equivalent run on
//! `beava-bench-v18 --parallel 32` produces. The polished CLI is for
//! smoke-testing; production benchmarking goes through the standalone
//! `beava-bench-v18` / `beava-bench-v2` binaries.
//!
//! These tests pin the fix:
//!
//! 1. `--help` for the throughput subcommand must NOT advertise a
//!    `--parallel` flag.
//! 2. `--parallel 32` must be rejected by the clap parser (unknown
//!    argument).

use assert_cmd::Command;
use predicates::prelude::*;

fn beava_bench() -> Command {
    Command::cargo_bin("beava-bench").unwrap()
}

#[test]
fn throughput_help_does_not_advertise_parallel_flag() {
    let mut cmd = beava_bench();
    cmd.args(["throughput", "--help"]);
    let assert = cmd.assert().success();
    let stdout = String::from_utf8(assert.get_output().stdout.clone()).unwrap();
    assert!(
        !stdout.contains("--parallel"),
        "throughput --help must NOT advertise --parallel \
         (it was a lying flag pre-13.7.6-24); got:\n{stdout}"
    );
}

#[test]
fn throughput_rejects_parallel_argument() {
    // `--parallel 32` should now be an unknown argument error.
    beava_bench()
        .args([
            "throughput",
            "--workload=small",
            "--duration=1s",
            "--yes",
            "--parallel=32",
        ])
        .assert()
        .failure()
        .stderr(
            predicate::str::contains("unexpected argument")
                .or(predicate::str::contains("unrecognized"))
                .or(predicate::str::contains("found argument"))
                .or(predicate::str::contains("--parallel")),
        );
}
