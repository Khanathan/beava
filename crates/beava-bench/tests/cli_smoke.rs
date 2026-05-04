//! Phase 13.5 Plan 08 smoke tests: `beava bench` CLI subcommands parse + run.
//!
//! Each test invokes the CLI with `--duration=1s` (or `--total-events=100`) so
//! the test completes in seconds, not minutes. Uses `assert_cmd` to spawn the
//! built binary.

use assert_cmd::Command;
use predicates::prelude::*;

fn beava_bench() -> Command {
    Command::cargo_bin("beava-bench").unwrap()
}

#[test]
fn test_help_subcommand_lists_four_modes() {
    let mut cmd = beava_bench();
    cmd.arg("--help");
    let assert = cmd.assert().success();
    let output = String::from_utf8(assert.get_output().stdout.clone()).unwrap();
    assert!(
        output.contains("throughput"),
        "help output must list 'throughput' subcommand"
    );
    assert!(
        output.contains("mixed"),
        "help output must list 'mixed' subcommand"
    );
    assert!(
        output.contains("memory"),
        "help output must list 'memory' subcommand"
    );
    assert!(
        output.contains("fsync"),
        "help output must list 'fsync' subcommand (D-03 amendment)"
    );
}

#[test]
fn test_throughput_subcommand_parses() {
    beava_bench()
        .args(["throughput", "--workload=small", "--duration=1s", "--yes"])
        .assert()
        .success();
}

#[test]
fn test_mixed_subcommand_parses() {
    beava_bench()
        .args([
            "mixed",
            "--workload=small",
            "--duration=1s",
            "--read-write-ratio=70/30",
            "--yes",
        ])
        .assert()
        .success();
}

#[test]
fn test_memory_subcommand_parses() {
    beava_bench()
        .args(["memory", "--workload=small", "--entities=100", "--yes"])
        .assert()
        .success();
}

#[test]
fn test_fsync_subcommand_parses() {
    beava_bench()
        .args(["fsync", "--workload=small", "--duration=1s", "--yes"])
        .assert()
        .success();
}

#[test]
fn test_json_output_format() {
    let mut cmd = beava_bench();
    cmd.args([
        "throughput",
        "--workload=small",
        "--duration=1s",
        "--yes",
        "--json",
    ]);
    let assert = cmd.assert().success();
    let stdout = String::from_utf8(assert.get_output().stdout.clone()).unwrap();
    // Output must be valid JSON.
    serde_json::from_str::<serde_json::Value>(&stdout)
        .expect("--json output must be valid JSON");
}

#[test]
fn test_unknown_workload_errors_clearly() {
    beava_bench()
        .args([
            "throughput",
            "--workload=nonexistent_xyz",
            "--duration=1s",
            "--yes",
        ])
        .assert()
        .failure()
        .stderr(
            predicate::str::contains("unknown workload")
                .or(predicate::str::contains("not found")),
        );
}
