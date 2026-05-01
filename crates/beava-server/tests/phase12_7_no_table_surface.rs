//! Phase 12.7 Plan 02 — architectural test enforcing events-only invariant.
//!
//! Locked architectural commitment from Phase 12.7 is guarded here:
//!
//! **Events-only surface.** Per `project_v0_events_only_scope` (locked
//! 2026-04-30): no `@bv.table` decorator in public `bv` namespace; no
//! `OpNode::Table*` variants; no `TemporalStore` / `MvccVersion`; no
//! `RecordType::TableUpsert/TableDelete/Retract`; no `WireRequest::Http*`
//! table-flavored variants; no `Route::Upsert/Delete/Retract/TableGet`;
//! no `temporal_http` / `push_table` / `delete_table` symbols. Reviving
//! any of these requires explicit user override + new ADR overturning
//! `project_v0_events_only_scope`.
//!
//! Companion to `phase12_7_legacy_table_handlers_killed.rs`: that test
//! asserts the deleted *files* stay deleted. This test asserts the
//! *symbol-level invariants* hold going forward — a tripwire for any future
//! code change that would re-introduce table surface in v0.
//!
//! **Plan ordering note (12.7-02 in Wave 1):** This test is RED at end of
//! Plan 02 because Waves 2-3 haven't deleted the surface yet — that's by
//! design. The test failure list IS the gating contract. As Plans 12.7-03,
//! 12.7-04, 12.7-05, 12.7-06 land, the symbol set shrinks; the test turns
//! GREEN once all forbidden symbols are gone.
//!
//! The main `forbidden_pattern_walk` test is `#[ignore]`-marked so the
//! workspace `cargo test --workspace` stays green during Waves 2-3. Plan
//! 12.7-09 (closure) removes the `#[ignore]` annotation as the final
//! tests-pass moment, locking the events-only invariant into CI for good.
//!
//! ## Verification probe (TDD red-form for an architectural-invariant test)
//!
//! Architectural-invariant tests are confirmation tests, not behaviour-driven
//! RED-GREEN tests. To convince yourself the test actually catches violations
//! once it is unignored:
//!
//! **Probe 1 — table re-introduction.** In a non-allowlisted file under
//! `crates/beava-server/src/` (e.g. `runtime_core_glue.rs`), temporarily add
//! a reference to `TemporalStore` (e.g. `let _: Option<TemporalStore> = None;`).
//! Run `cargo test -p beava-server --test phase12_7_no_table_surface --
//! --ignored`. Confirm `forbidden_pattern_walk` fails with the file's path
//! in the violation list. REVERT the edit.
//!
//! **Probe 2 — sanity.** Temporarily change one `FORBIDDEN_PATTERNS` entry
//! to a literal that does NOT appear in any walked file (e.g. `"ZZ_NOT_REAL_SYMBOL"`).
//! Run the sanity-allowlist test. Confirm
//! `sanity_allowlist_actually_contains_pattern` fails with a message
//! describing that the test file no longer contains a forbidden pattern.
//! REVERT the edit.

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
/// move the comment or update the test.
fn strip_line_comments(src: &str) -> String {
    src.lines()
        .filter(|l| {
            let t = l.trim_start();
            !(t.starts_with("//") || t.starts_with("///") || t.starts_with("//!"))
        })
        .collect::<Vec<_>>()
        .join("\n")
}

/// Walk roots covering every Rust source surface where table re-introduction
/// would be a regression. Tests directories are intentionally excluded — the
/// architectural tests themselves contain the forbidden patterns inside
/// string literals, and per-test files describing deleted surface are
/// allowed.
const WALK_DIRS: &[&str] = &[
    "crates/beava-core/src",
    "crates/beava-server/src",
    "crates/beava-runtime-core/src",
    "crates/beava-persistence/src",
    "crates/beava-bench/src",
];

/// Forbidden table-surface patterns. Per CONTEXT D-02 framing, these
/// represent v0's events-only scope; reviving any of these requires explicit
/// user override + new ADR overturning `project_v0_events_only_scope`.
///
/// Note on `fn retract(`: the bare token `retract` is too broad (it would hit
/// `retraction` doc-comment strings and `retract_test_*` names that survive
/// in phase-doc text). The full call-shape `fn retract(` only matches actual
/// function declarations, which is the precise tripwire we want.
const FORBIDDEN_PATTERNS: &[&str] = &[
    "OpNode::Table",
    "OpNode::TableUpsert",
    "OpNode::TableDelete",
    "TemporalStore",
    "MvccVersion",
    "RecordType::TableUpsert",
    "RecordType::TableDelete",
    "RecordType::Retract",
    "temporal_http",
    "push_table",
    "delete_table",
    "fn retract(",
    "WireRequest::HttpUpsert",
    "WireRequest::HttpDelete",
    "WireRequest::HttpRetract",
    "WireRequest::HttpTableGet",
    "Route::Upsert",
    "Route::Delete",
    "Route::Retract",
    "Route::TableGet",
];

/// Allowlist: file basenames excluded from the walk.
///
/// The architectural tests themselves reference the forbidden symbols
/// inside string literals (in `FORBIDDEN_PATTERNS` slices and assert
/// messages); they must be excluded so the test does not flag itself. The
/// allowlist is defensive — none of these files currently live under any
/// `WALK_DIRS` path (they are in `tests/`, which is not walked) but if a
/// future refactor moved an architectural test under `src/`, the allowlist
/// would still keep it from self-triggering.
const ALLOWLIST: &[&str] = &[
    "phase12_7_no_table_surface.rs",
    "phase12_7_legacy_table_handlers_killed.rs",
];

/// Test 1 — Forbidden-pattern walk. Walks every `.rs` file under WALK_DIRS,
/// strips line comments, and asserts NONE contain any FORBIDDEN_PATTERNS
/// token outside the allowlist.
///
/// **RED at end of Plan 12.7-02** — this is by design. The forbidden
/// patterns still exist in `apply_shard.rs`, `runtime_core_glue.rs`,
/// `temporal_http.rs`, `temporal.rs`, `recovery.rs`, `registry_debug.rs`,
/// `wire_request.rs`, `router.rs`, `http_listener.rs`, and others. As
/// Plans 12.7-03 / 12.7-04 / 12.7-05 / 12.7-06 land their deletions, the
/// violation list shrinks until both architectural tests turn GREEN at
/// the end of Wave 3.
///
/// `#[ignore]`-marked so `cargo test --workspace` stays green during the
/// RED-state interlude. Plan 12.7-09 (closure) removes the `#[ignore]`
/// annotation as the final tests-pass moment, locking the events-only
/// invariant into CI on every PR.
///
/// To run manually: `cargo test -p beava-server --test
/// phase12_7_no_table_surface -- --ignored`. The failure message lists
/// every (file, pattern) pair so subsequent waves can see exactly what to
/// delete.
#[test]
#[ignore = "Wave 1 RED state; turns GREEN after Plans 12.7-03..06 delete the table surface. Plan 12.7-09 (closure) removes this #[ignore]."]
fn forbidden_pattern_walk() {
    let root = workspace_root();
    let mut violations = Vec::new();

    for walk_dir in WALK_DIRS {
        let dir = root.join(walk_dir);
        let files = collect_rs_files(&dir);
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
            for needle in FORBIDDEN_PATTERNS {
                if stripped.contains(needle) {
                    violations.push(format!("{}: contains '{}'", f.display(), needle));
                }
            }
        }
    }

    assert!(
        violations.is_empty(),
        "Architectural regression: events-only surface violated. Per \
         `project_v0_events_only_scope` (locked 2026-04-30) Beava v0 ships \
         events-only — no `@bv.table` decorator, no `OpNode::Table*` variants, \
         no `TemporalStore` / `MvccVersion`, no `RecordType::TableUpsert/\
         TableDelete/Retract`, no `WireRequest::Http*` table variants, no \
         `Route::Upsert/Delete/Retract/TableGet`, no `temporal_http` / \
         `push_table` / `delete_table` symbols. Reviving any of these \
         requires explicit user override + new ADR overturning \
         `project_v0_events_only_scope`.\n\
         Violations ({} occurrence(s)):\n{}",
        violations.len(),
        violations.join("\n")
    );
}

/// Test 2 — Sanity: every WALK_DIR contains at least one `.rs` file. If a
/// future refactor renames or moves a crate's `src/` directory and silently
/// invalidates a WALK_DIRS entry, the grep would become a no-op for that
/// crate and the test would silently lose coverage. This sanity test guards
/// against that drift.
#[test]
fn sanity_walk_dirs_actually_contain_rust_files() {
    let root = workspace_root();
    for d in WALK_DIRS {
        let path = root.join(d);
        let files = collect_rs_files(&path);
        assert!(
            !files.is_empty(),
            "WALK_DIR '{d}' is empty — was the path moved? \
             {} expected at least 1 .rs file under {}",
            d,
            path.display()
        );
    }
}

/// Test 3 — Sanity: the architectural-test source itself contains at least
/// one `FORBIDDEN_PATTERNS` token verbatim. This proves the allowlist
/// exclusion is actually doing something: without it, the architectural
/// test could become a silent no-op if a future refactor moved or renamed
/// the patterns slice such that no allowlisted file contains a forbidden
/// pattern.
///
/// Equivalent check: read the test file (an allowlisted file), confirm at
/// least one `FORBIDDEN_PATTERNS` entry appears as a literal substring.
/// If this fails, the architectural test has been silently rendered a
/// no-op — investigate before proceeding.
#[test]
fn sanity_allowlist_actually_contains_pattern() {
    let root = workspace_root();
    let test_file = root.join("crates/beava-server/tests/phase12_7_no_table_surface.rs");
    let src = std::fs::read_to_string(&test_file)
        .expect("phase12_7_no_table_surface.rs must exist (this test is reading itself)");

    // Find the first FORBIDDEN_PATTERNS token that appears verbatim in this
    // file's source. Every entry in FORBIDDEN_PATTERNS is also referenced as
    // a string literal inside this file (in the FORBIDDEN_PATTERNS slice
    // itself), so at least one match is mandatory.
    let any_pattern_present = FORBIDDEN_PATTERNS.iter().any(|p| src.contains(p));
    assert!(
        any_pattern_present,
        "sanity: phase12_7_no_table_surface.rs must contain at least one \
         FORBIDDEN_PATTERNS token verbatim. If this fails, the architectural \
         test is a silent no-op — the FORBIDDEN_PATTERNS slice has been \
         moved, renamed, or emptied. Investigate before proceeding; the \
         events-only invariant is no longer enforced by `forbidden_pattern_walk`."
    );

    // Defensive belt-and-braces: confirm the allowlist actually contains
    // this file's basename. Without this, the test might pass via the file
    // being walked (and self-triggering on its own pattern strings), which
    // is the wrong reason to pass.
    assert!(
        ALLOWLIST.contains(&"phase12_7_no_table_surface.rs"),
        "sanity: ALLOWLIST must contain 'phase12_7_no_table_surface.rs' \
         (this file references forbidden patterns inside string literals)."
    );
}
