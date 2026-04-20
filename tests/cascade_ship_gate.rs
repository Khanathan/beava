//! Phase 55 ship-gate — grep-based architectural invariants.
//!
//! Mirrors the Phase 54 `tests/ship_gate.rs` pattern: this file hosts
//! the structural invariants that enforce the Phase 55 architectural
//! cleanup post-Wave-4. At Wave 0 the gate was marked with a wave-scoped
//! ignore attribute; at Wave 4 close (plan 55-04 Task 2) the marker is
//! removed and the gate enforces on every default run.
//!
//! Invariants enforced:
//!   - grep of Phase 55 wave-scoped ignore attribute in tests/ returns
//!     zero files (all wave-scoped markers flipped).
//!   - scripts/verify-source-lsn-echoed.sh exits 0.
//!   - All 5 Phase 55 cascade metric name-literals emit somewhere in src/.
//!   - All 4 Phase 55 TCP source-table opcodes pinned in protocol.rs.
//!
//! Run:
//!   cargo test --release --test cascade_ship_gate phase_55_grep_gates_pass

/// Phase 55 grep-gates — flipped GREEN at Wave 4 close; all wave-scoped
/// ignore markers have been removed from tests/ and the architectural
/// invariants below are enforced on every default test run.
#[test]
fn phase_55_grep_gates_pass() {
    // Gate 1: No Phase 55 wave-scoped ignore attribute remains in tests/.
    // The pattern matches the literal attribute syntax that Wave 0 stamped
    // on every RED test (one marker per wave, W0..W4). Wave 4 close
    // removes all of them; this file was the last to flip.
    let out = std::process::Command::new("bash")
        .arg("-c")
        .arg(r#"grep -rl '#\[ignore = "55-W[0-9]"\]' tests/ 2>/dev/null | wc -l"#)
        .output()
        .expect("grep must run");
    let remaining: usize = String::from_utf8_lossy(&out.stdout)
        .trim()
        .parse()
        .unwrap_or(0);
    assert_eq!(
        remaining, 0,
        "Phase 55 ship-gate: {remaining} files still carry a Phase 55 wave-scoped ignore attribute"
    );

    // Gate 2: source_lsn-echoed grep script exits 0.
    let status = std::process::Command::new("bash")
        .arg("scripts/verify-source-lsn-echoed.sh")
        .status()
        .expect("grep script must run");
    assert!(
        status.success(),
        "scripts/verify-source-lsn-echoed.sh exited non-zero"
    );

    // Gate 3: all five Phase 55 metric name-literals emit in src/.
    for metric in &[
        "beava_cascade_cross_shard_total",
        "beava_cascade_intra_shard_total",
        "beava_cascade_queue_depth",
        "beava_cascade_lag_seconds",
        "beava_shard_inbox_high_watermark_total",
    ] {
        let status = std::process::Command::new("bash")
            .arg("-c")
            .arg(format!(r#"grep -rq '"{metric}"' src/"#))
            .status()
            .expect("grep run");
        assert!(status.success(), "metric {metric} not emitted in src/");
    }

    // Gate 4: all four Phase 55 source-table TCP opcodes pinned in protocol.rs.
    for op in &[
        "OP_UPSERT_TABLE_ROW",
        "OP_DELETE_TABLE_ROW",
        "OP_UPSERT_TABLE_BATCH",
        "OP_DELETE_TABLE_BATCH",
    ] {
        let status = std::process::Command::new("bash")
            .arg("-c")
            .arg(format!("grep -q '{op}' src/server/protocol.rs"))
            .status()
            .expect("grep run");
        assert!(status.success(), "{op} missing from src/server/protocol.rs");
    }
}
