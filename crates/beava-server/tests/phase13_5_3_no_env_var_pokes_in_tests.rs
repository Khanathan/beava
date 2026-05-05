//! Phase 13.5.3 architectural tripwire — assert no `std::env::set_var(` /
//! `env::set_var(` calls survive in `crates/beava-server/tests/`.
//!
//! **Locked architectural commitment:** the mio data-plane stack reads
//! `BEAVA_*` env vars at config-load time in production `main.rs` ONLY
//! (via `ServerV18Config::from_env()`). Per-test config is plumbed
//! through `TestServerBuilder` builder methods (`wal_buffers`,
//! `wal_buffer_size_mb`, `wal_tick_ms`, `io_threads`, `test_mode`,
//! `memory_governance_enforce`), not via `std::env::set_var`.
//!
//! **Why this matters:** `std::env::*` is process-global. `cargo test`
//! parallelizes both across binaries (each integration-test file is a
//! separate process — set_var leaks DON'T cross binaries) AND within a
//! binary via `--test-threads=N` (set_var IN binary A leaks across all
//! threads inside A). `TestServer::spawn()` previously read several
//! `BEAVA_*` env vars at boot; tests that set/unset them mid-run leaked
//! state into peer tests' `TestServer` boots, manifesting as the rotating
//! workspace-parallelism flake population documented in
//! `.planning/ideas/workspace-test-determinism.md`.
//!
//! Phase 13.5.3 closes that smell by plumbing every per-server tunable
//! through the `ServerV18Config` struct (mirrors the `tcp_max_frame_bytes`
//! plumbing from commit `acac4254`). Production env reads happen exactly
//! once at boot in `ServerV18Config::from_env()`. This test guards
//! against any future `set_var` resurfacing inside
//! `crates/beava-server/tests/`.
//!
//! ## Verification probe (TDD red-form for an architectural-invariant test)
//!
//! Architectural-invariant tests are confirmation tests, not
//! behaviour-driven RED-GREEN tests. To convince yourself the test
//! actually catches violations:
//!
//! **Probe 1 — set_var in a test file.** In any non-allowlisted file
//! under `crates/beava-server/tests/` (e.g. `phase12_8_metrics_endpoint.rs`),
//! temporarily add a `std::env::set_var("BEAVA_FOO", "bar");` line inside
//! a `fn` body. Run `cargo test -p beava-server --test
//! phase13_5_3_no_env_var_pokes_in_tests`. Confirm
//! `no_env_set_var_in_beava_server_tests` FAILS with the file's path in
//! the violation list. REVERT the edit.
//!
//! **Probe 2 — `env::set_var` shorthand.** Same file, add `use std::env;`
//! then `env::set_var("BEAVA_X", "y");`. Run the test. Confirm both
//! pattern variants are caught. REVERT the edit.
//!
//! Probe 1 was exercised during Phase 13.5.3 Task 3 RED → Task 2 GREEN
//! transition: with set_var calls still in 6 test files (Task 2 not yet
//! landed), this test FAILED with all 6 violators listed; once Task 2
//! GREEN landed and stripped the set_var pokes, this test became GREEN
//! by construction.
//!
//! **Reviving `set_var` in `crates/beava-server/tests/` requires:** explicit
//! user override + new ADR + companion update to
//! `ServerV18Config::from_env()` semantics. There is no legitimate use
//! for set_var in this directory — every per-server tunable has an
//! equivalent `TestServerBuilder` builder method.
//!
//! Companion: `phase12_6_mio_only_dataplane.rs` (architectural test for
//! the mio-only hot-path invariant). Same shape: workspace_root() +
//! collect_rs_files() + strip_line_comments() + greps a target dir for a
//! forbidden pattern.

use std::path::{Path, PathBuf};

/// Workspace root — two parents up from `CARGO_MANIFEST_DIR`
/// (`crates/beava-server`).
fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .to_path_buf()
}

/// Recursively collect every `.rs` file under `dir`.
fn collect_rs_files(dir: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    fn visit(dir: &Path, out: &mut Vec<PathBuf>) {
        if let Ok(rd) = std::fs::read_dir(dir) {
            for entry in rd.flatten() {
                let p = entry.path();
                if p.is_dir() {
                    visit(&p, out);
                } else if p.extension().and_then(|s| s.to_str()) == Some("rs") {
                    out.push(p);
                }
            }
        }
    }
    visit(dir, &mut out);
    out
}

/// Drop lines whose first non-whitespace characters are `//` (line comments)
/// or `///` / `//!` (doc comments). Block comments are not stripped — they
/// are rare in this codebase and any false positive in a `/* ... */` block
/// would be flagged by the violation list, prompting the author to either
/// move the comment or update the test. Mirrors the `phase12_6_mio_only_dataplane.rs`
/// helper verbatim.
fn strip_line_comments(src: &str) -> String {
    src.lines()
        .filter(|l| {
            let t = l.trim_start();
            !(t.starts_with("//") || t.starts_with("///") || t.starts_with("//!"))
        })
        .collect::<Vec<_>>()
        .join("\n")
}

/// Locate `crates/beava-server/tests/`.
fn server_tests() -> PathBuf {
    workspace_root().join("crates/beava-server/tests")
}

/// Test 1 — assert no `std::env::set_var(` or `env::set_var(` survives in
/// `crates/beava-server/tests/`. Allowlisted: this file itself (its
/// assertion-error-message string literals contain the literal pattern).
#[test]
fn no_env_set_var_in_beava_server_tests() {
    let dir = server_tests();
    let files = collect_rs_files(&dir);
    // Allowlist: this test file is the only legitimate place where the
    // literal pattern survives in non-comment source — the assertion-error
    // string mentions it.
    const ALLOWLIST: &[&str] = &["phase13_5_3_no_env_var_pokes_in_tests.rs"];
    // Patterns covering the two common shapes for set_var calls.
    const PATTERNS: &[&str] = &["std::env::set_var(", "env::set_var("];

    let mut violations = Vec::new();
    for f in &files {
        let basename = f
            .file_name()
            .and_then(|s| s.to_str())
            .unwrap_or("")
            .to_string();
        if ALLOWLIST.contains(&basename.as_str()) {
            continue;
        }
        let content = match std::fs::read_to_string(f) {
            Ok(s) => s,
            Err(_) => continue,
        };
        let stripped = strip_line_comments(&content);
        for needle in PATTERNS {
            if stripped.contains(needle) {
                violations.push(format!("{}: contains '{}'", f.display(), needle));
            }
        }
    }

    assert!(
        violations.is_empty(),
        "Architectural regression: `std::env::set_var(` / `env::set_var(` calls \
         in `crates/beava-server/tests/` re-introduce the cross-test process-env \
         pollution Phase 13.5.3 closed (Path A architectural fix from \
         .planning/ideas/workspace-test-determinism.md). Per-test config MUST \
         be plumbed via `TestServerBuilder` builder methods \
         (`.wal_buffers(n)` / `.wal_buffer_size_mb(mb)` / `.wal_tick_ms(ms)` / \
         `.io_threads(n)` / `.test_mode(b)` / `.memory_governance_enforce(b)`); \
         production reads `BEAVA_*` env vars at boot via \
         `ServerV18Config::from_env()` (the SOLE legitimate env-read site).\n\
         Violations:\n{}",
        violations.join("\n")
    );
}

/// Test 2 — sanity check that the test infrastructure itself works: this
/// file's stripped source DOES contain the literal grep patterns. If this
/// fails, the test has been silently rendered a no-op (e.g. a future
/// refactor moved the pattern strings into a comment, or factored them
/// into a constant whose definition the strip_line_comments() pass
/// removes).
#[test]
fn sanity_test_file_contains_grep_pattern() {
    let this_file =
        workspace_root().join("crates/beava-server/tests/phase13_5_3_no_env_var_pokes_in_tests.rs");
    let src = std::fs::read_to_string(&this_file).expect("read this test file");
    let stripped = strip_line_comments(&src);
    assert!(
        stripped.contains("std::env::set_var("),
        "sanity: this test file (post-strip) must literally contain the \
         `std::env::set_var(` pattern it greps for. If this fails, the \
         pattern strings have been moved into a place the strip pass \
         removes (e.g. a doc comment) and the test is silently a no-op."
    );
    assert!(
        stripped.contains("env::set_var("),
        "sanity: this test file (post-strip) must literally contain the \
         `env::set_var(` pattern it greps for. Same caveat as above."
    );
}
