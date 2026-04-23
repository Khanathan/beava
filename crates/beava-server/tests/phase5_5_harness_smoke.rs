//! Phase 5.5 Plan 01 — harness scaffolding smoke test.
//!
//! Asserts that criterion workspace wiring, bench targets, perf-baselines.md,
//! and CLAUDE.md §Performance Discipline are all present. All five assertions
//! must fail before plan 01 green lands (RED gate).

use std::path::PathBuf;

fn repo_root() -> PathBuf {
    // CARGO_MANIFEST_DIR = crates/beava-server → ../../ = repo root
    let manifest = env!("CARGO_MANIFEST_DIR");
    PathBuf::from(manifest)
        .join("../..")
        .canonicalize()
        .expect("canonicalize repo root")
}

#[test]
fn harness_scaffolding_present() {
    let root = repo_root();

    // ── 1. Workspace Cargo.toml pins criterion 0.5 ──────────────────────────
    let ws_cargo = root.join("Cargo.toml");
    let ws_contents =
        std::fs::read_to_string(&ws_cargo).unwrap_or_else(|e| panic!("read {ws_cargo:?}: {e}"));
    assert!(
        ws_contents.contains(r#"criterion = { version = "0.5""#),
        r#"expected `criterion = {{ version = "0.5"` in {ws_cargo:?}"#
    );

    // ── 2. beava-core/Cargo.toml has [[bench]] + criterion dev-dep ───────────
    let core_cargo = root.join("crates/beava-core/Cargo.toml");
    let core_contents =
        std::fs::read_to_string(&core_cargo).unwrap_or_else(|e| panic!("read {core_cargo:?}: {e}"));
    assert!(
        core_contents.contains("[[bench]]"),
        "expected [[bench]] in {core_cargo:?}"
    );
    assert!(
        core_contents.contains("criterion = { workspace = true }"),
        "expected `criterion = {{ workspace = true }}` in {core_cargo:?}"
    );

    // ── 3. beava-server/Cargo.toml has [[bench]] + criterion dev-dep ─────────
    let server_cargo = root.join("crates/beava-server/Cargo.toml");
    let server_contents = std::fs::read_to_string(&server_cargo)
        .unwrap_or_else(|e| panic!("read {server_cargo:?}: {e}"));
    assert!(
        server_contents.contains("[[bench]]"),
        "expected [[bench]] in {server_cargo:?}"
    );
    assert!(
        server_contents.contains("criterion = { workspace = true }"),
        "expected `criterion = {{ workspace = true }}` in {server_cargo:?}"
    );

    // ── 4. .planning/perf-baselines.md exists and has hw-class header ────────
    let baselines = root.join(".planning/perf-baselines.md");
    let baselines_contents =
        std::fs::read_to_string(&baselines).unwrap_or_else(|e| panic!("read {baselines:?}: {e}"));
    assert!(
        baselines_contents.contains("## hw-class:"),
        "expected `## hw-class:` header in {baselines:?}"
    );

    // ── 5. CLAUDE.md documents §Performance Discipline ───────────────────────
    let claude_md = root.join("CLAUDE.md");
    let claude_contents =
        std::fs::read_to_string(&claude_md).unwrap_or_else(|e| panic!("read {claude_md:?}: {e}"));
    assert!(
        claude_contents.contains("Performance Discipline"),
        "expected `Performance Discipline` heading in {claude_md:?}"
    );
}
