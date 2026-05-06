//! Phase 13.5 Plan 08 smoke tests: `beava bench` CLI subcommands parse + run.
//!
//! Each test invokes the CLI with a small duration / event cap so the test
//! completes in seconds, not minutes. Uses `assert_cmd` to spawn the built
//! binary.
//!
//! Plan 13.7.6-32 reshaped the throughput subcommand to use v18's flag set
//! (`--pipeline / --transport / --duration-secs / --parallel / ...`) when
//! v18 was merged into `beava-bench throughput`. The mixed / memory / fsync
//! subcommands still use the smoke-test flag shape (`--workload / --duration
//! / --yes`).

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
    // Plan 13.7.6-32 reshaped throughput to use v18's flag set: --pipeline
    // (not --workload), --duration-secs (not --duration), no --yes. Pipeline
    // depth = 1 + parallel = 1 keeps the run cheap; --no-ledger avoids the
    // markdown stdout row so the test asserts only on success exit.
    // current_dir(CARGO_MANIFEST_DIR) makes `./configs/small.json` resolvable.
    beava_bench()
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .args([
            "throughput",
            "--pipeline=small",
            "--duration-secs=1",
            "--parallel=1",
            "--pipeline-depth=1",
            "--no-ledger",
        ])
        .timeout(std::time::Duration::from_secs(30))
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

// Plan 13.7.6-32 NOTE: `test_json_output_format` deleted — the smoke-test
// `--json` output flag was a feature of the old single-threaded throughput
// harness (BenchResult JSON serialization in cli/output.rs). The migrated
// production harness (v18 surface) emits a markdown ledger row + human
// summary by design; JSON output is not in scope for v0. The mixed / memory /
// fsync subcommands still support `--json` via their unchanged smoke-test
// path; that surface is covered by their respective parse-tests above.

#[test]
fn test_unknown_pipeline_errors_clearly() {
    // Plan 13.7.6-32 — production harness reads `./configs/{pipeline}.json`,
    // so an unknown pipeline name surfaces as a "read pipeline config" error
    // from `load_pipeline`.
    beava_bench()
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .args([
            "throughput",
            "--pipeline=nonexistent_xyz",
            "--duration-secs=1",
            "--parallel=1",
            "--pipeline-depth=1",
            "--no-ledger",
        ])
        .timeout(std::time::Duration::from_secs(15))
        .assert()
        .failure()
        .stderr(
            predicate::str::contains("read pipeline config")
                .or(predicate::str::contains("No such file"))
                .or(predicate::str::contains("not found")),
        );
}
