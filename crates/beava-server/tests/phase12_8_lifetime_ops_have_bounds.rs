//! Phase 12.8 Plan 05 — architectural test enforcing the lifetime-bounds
//! coverage invariant.
//!
//! **Locked architectural commitment from Phase 12.8 (D-03 hard reject at
//! register-time):** every op-string registered in
//! `crates/beava-core/src/agg_compile.rs::parse_agg_kind` MUST have a
//! corresponding match arm in
//! `crates/beava-core/src/register_validate.rs::lifetime_bound_for_op_str`
//! that returns a non-`Unbounded` variant. v0 commits to "users size their
//! box up front": every operator declares its lifetime memory ceiling at
//! register-time.
//!
//! This is a coverage tripwire — if a future PR adds a new op (extends
//! `AggKind` and `parse_agg_kind`) without also extending
//! `lifetime_bound_for_op_str`, this test fails with a list of missing
//! op-strings. Forces the bound table to stay in lockstep with the op
//! catalogue.
//!
//! **Sister phase pattern:** Phase 12.6 Plan 10
//! (`phase12_6_mio_only_dataplane.rs`) and Phase 12.7 Plan 02
//! (`phase12_7_no_table_surface.rs`) used the same architectural-test-as-
//! grep approach. The companion-file pattern from those phases was for
//! asserting deleted files stay deleted — Plan 12.8 has no analogous
//! deletion target, so a single-file test is sufficient (per CONTEXT.md
//! decisions, "Architectural test pair vs single test: Single test
//! sufficient — 12.6/12.7 used companion file-absence tests because there
//! were files to assert deleted; 12.8 has no analogous deletion target.").
//!
//! ## Verification probe (TDD red-form for an architectural-invariant test)
//!
//! Architectural-invariant tests are confirmation tests, not behaviour-driven
//! RED-GREEN tests. To convince yourself the test actually catches violations:
//!
//! **Probe 1 — drop a bound classification.** Temporarily delete one match
//! arm from `lifetime_bound_for_op_str` (e.g. the `"first" | "last" =>`
//! arm). Run `cargo test -p beava-server --test
//! phase12_8_lifetime_ops_have_bounds`. Confirm
//! `every_op_str_in_agg_compile_has_bound_in_register_validate` fails with
//! the dropped op(s) listed in the missing set. REVERT the edit.
//!
//! **Probe 2 — sanity grep no-op.** Temporarily rename the function in
//! `agg_compile.rs` from `parse_agg_kind` to `parse_agg_kind_renamed`. Run
//! the sanity test `sanity_agg_compile_has_at_least_30_op_strings`. Confirm
//! it fails with a "function moved or renamed" message and zero op-strings
//! extracted. REVERT the edit.
//!
//! ## What this test does NOT enforce
//!
//! - It does NOT check that the classification is CORRECT (e.g.
//!   `histogram` should be `BoundedByRequiredKwarg("buckets")`, not `O1` —
//!   that's Plan 04's unit test surface in
//!   `op_lifetime_bounds_test.rs`).
//! - It does NOT validate the `AggKind` enum itself (no enum-coverage check
//!   here; Plan 04's classification tests cover that).
//!
//! This test specifically guards the
//! agg_compile↔register_validate lockstep — the agg_compile op_str list is
//! the source of truth for "what op-strings the wire accepts"; the bound
//! table must cover every entry.

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

/// Drop lines whose first non-whitespace characters are `//` (line comments)
/// or `///` / `//!` (doc comments). This prevents a doc-comment example
/// like `// "count" =>` inside register_validate from matching the grep.
/// Block comments are not stripped — they are rare in this codebase and
/// any false positive in a `/* ... */` block would surface as a noisy
/// extra op-string, prompting the author to investigate.
fn strip_line_comments(src: &str) -> String {
    src.lines()
        .filter(|l| {
            let t = l.trim_start();
            !(t.starts_with("//") || t.starts_with("///") || t.starts_with("//!"))
        })
        .collect::<Vec<_>>()
        .join("\n")
}

/// Slice `src` from the start of `fn_name`'s body (the opening `{`) to its
/// matching closing `}` (inclusive). Walks brace depth so nested blocks
/// inside the function body don't confuse the parser. Returns `None` if
/// the function isn't found.
fn extract_fn_body<'a>(src: &'a str, fn_name: &str) -> Option<&'a str> {
    let needle = format!("fn {fn_name}");
    let fn_start = src.find(&needle)?;
    let body_start = src[fn_start..].find('{')? + fn_start;
    let mut depth = 0_usize;
    let mut body_end = body_start;
    for (i, ch) in src[body_start..].char_indices() {
        match ch {
            '{' => depth += 1,
            '}' => {
                depth -= 1;
                if depth == 0 {
                    body_end = body_start + i;
                    break;
                }
            }
            _ => {}
        }
    }
    if body_end == body_start {
        return None;
    }
    Some(&src[body_start..=body_end])
}

/// Extract op-strings from `agg_compile::parse_agg_kind` — every quoted
/// string immediately followed (after optional whitespace) by `=>` is a
/// recognized op-string. Returns a sorted, deduped Vec.
fn extract_agg_compile_op_strings() -> Vec<String> {
    let path = workspace_root().join("crates/beava-core/src/agg_compile.rs");
    let src = std::fs::read_to_string(&path).expect("agg_compile.rs exists");
    let stripped = strip_line_comments(&src);

    let body = extract_fn_body(&stripped, "parse_agg_kind")
        .expect("parse_agg_kind function exists in agg_compile.rs");

    // Walk the body looking for `"<op_str>" =>` patterns. The match arms in
    // parse_agg_kind also use `|` to chain alternatives (e.g.
    // `"ewma" | "ema" => ...`); both forms appear in the body and we must
    // capture every quoted alternative.
    extract_op_strings_from_match_body(body)
}

/// Extract op-strings from `register_validate::lifetime_bound_for_op_str` —
/// every quoted string that appears as a match-arm pattern (i.e. followed
/// by `=>` directly, or followed by ` | ` chaining to another quoted
/// alternative which eventually leads to `=>`). Returns a sorted, deduped
/// Vec.
///
/// The function body also contains string literals INSIDE
/// `OpLifetimeBound::BoundedByRequiredKwarg("buckets")`-style RHS
/// expressions (e.g. `"buckets"`, `"n"`, `"samples"`, `"k"`,
/// `"max_categories"`). Those are kwarg-name parameters, NOT op-strings, and
/// must be excluded. The extractor distinguishes them by walking match-arm
/// patterns specifically: it scans for `"<x>"` followed (after whitespace)
/// by `=>` or `|`, and stops collecting when it sees `=>` — anything
/// after `=>` until the next match arm is RHS expression, ignored.
fn extract_register_validate_op_strings() -> Vec<String> {
    let path = workspace_root().join("crates/beava-core/src/register_validate.rs");
    let src = std::fs::read_to_string(&path).expect("register_validate.rs exists");
    let stripped = strip_line_comments(&src);

    let body = extract_fn_body(&stripped, "lifetime_bound_for_op_str")
        .expect("lifetime_bound_for_op_str function exists in register_validate.rs");

    extract_op_strings_from_match_body(body)
}

/// Shared extractor for `match` bodies. Scans for `"<x>"` (a quoted string)
/// at a position where the NEXT non-whitespace token is either `=>` (the
/// arm body) or `|` (chaining to another quoted alternative). Stops
/// collecting on a given match arm when it consumes `=>` — everything
/// between `=>` and the next arm separator (`,`) is RHS expression and
/// ignored.
///
/// This handles three variants:
/// - `"foo" => ...`            — single-pattern arm
/// - `"foo" | "bar" => ...`    — chained-alternatives arm
/// - `"foo" | "bar" | "baz" => ...` — N-way chain
///
/// And rejects:
/// - `"foo"` inside an RHS expression (e.g. `BoundedByRequiredKwarg("n")`)
///   because it's not followed by `=>` or `|`.
/// - `_ =>` (catch-all) because there's no quoted string pattern.
fn extract_op_strings_from_match_body(body: &str) -> Vec<String> {
    let mut ops: Vec<String> = Vec::new();
    let bytes = body.as_bytes();
    let mut i = 0_usize;

    while i < bytes.len() {
        let ch = bytes[i] as char;
        if ch == '"' {
            // Find closing quote (no escapes expected in op-string literals
            // — they're all simple identifiers).
            let start = i + 1;
            let mut end = start;
            while end < bytes.len() && bytes[end] as char != '"' {
                end += 1;
            }
            if end >= bytes.len() {
                break;
            }
            let candidate = &body[start..end];

            // Look ahead past whitespace for `=>` or `|`.
            let mut j = end + 1;
            while j < bytes.len() && (bytes[j] as char).is_whitespace() {
                j += 1;
            }
            // Followed by `=>` ⇒ single-pattern arm (or last in a chain).
            // Followed by `|` ⇒ chain-alternative; the next quoted string
            // is also a pattern.
            let is_arm_pattern = if j + 1 < bytes.len() {
                let next2 = &body[j..j + 2.min(bytes.len() - j)];
                next2 == "=>" || (bytes[j] as char == '|')
            } else {
                false
            };
            if is_arm_pattern {
                ops.push(candidate.to_string());
            }
            i = end + 1;
            continue;
        }
        i += 1;
    }

    ops.sort();
    ops.dedup();
    ops
}

/// **Test 1 — Coverage check.** Every op-string registered in
/// `agg_compile::parse_agg_kind` MUST appear in
/// `register_validate::lifetime_bound_for_op_str`. If any are missing, the
/// architectural lockstep is broken and the test fails with a precise
/// missing-list.
///
/// GREEN at HEAD post Plan 12.8-04 (which populated the 53-variant /
/// 54-op-string classification table). Plan 12.8-01 had a placeholder
/// returning `Unbounded` for every op; Plan 12.8-04 replaced it with the
/// full match. This test locks the lockstep into CI: any future op added
/// to `parse_agg_kind` without a corresponding bound classification fails
/// the workspace test suite.
#[test]
fn every_op_str_in_agg_compile_has_bound_in_register_validate() {
    let agg_compile_ops = extract_agg_compile_op_strings();
    let register_validate_ops = extract_register_validate_op_strings();

    assert!(
        !agg_compile_ops.is_empty(),
        "Source-grep of agg_compile.rs::parse_agg_kind returned no \
         op-strings — was the function moved or renamed? The architectural \
         test cannot enforce coverage without an op-string list to check."
    );

    let missing: Vec<String> = agg_compile_ops
        .iter()
        .filter(|op| !register_validate_ops.contains(op))
        .cloned()
        .collect();

    assert!(
        missing.is_empty(),
        "Phase 12.8 D-03 lifetime-bounds coverage gap: agg_compile.rs::\
         parse_agg_kind registers these op-strings but \
         register_validate.rs::lifetime_bound_for_op_str does NOT classify \
         them: {missing:?}.\n\n\
         Fix: add a match arm for each missing op-string in \
         lifetime_bound_for_op_str returning the appropriate \
         OpLifetimeBound variant. See \
         .planning/phases/12.8-memory-governance/12.8-CONTEXT.md memory-bound \
         classes for the canonical mapping. Per CLAUDE.md TDD discipline, \
         add the failing test first (extending Plan 04's unit-test surface \
         in op_lifetime_bounds_test.rs) before adding the match arm.\n\n\
         agg_compile_ops ({}): {agg_compile_ops:?}\n\
         register_validate_ops ({}): {register_validate_ops:?}",
        agg_compile_ops.len(),
        register_validate_ops.len(),
    );
}

/// **Test 2 — Sanity: agg_compile op-string extractor isn't a no-op.**
/// Confirm the source-grep extracted at least 30 op-strings from
/// `parse_agg_kind`. If a future refactor moves or renames the function,
/// the extractor returns 0 op-strings and the coverage check above would
/// silently pass via vacuous-truth ("nothing to check, nothing missing").
/// This sanity guard catches that drift.
///
/// Threshold (30) is well below the actual count (54 op-strings: 53
/// AggKind variants + the "ema" alias for AggKind::Ewma, per Plan 04's
/// classification doc-comment). Picked conservatively low so a deliberate
/// op removal doesn't trip this guard accidentally; the real signal is
/// "near-zero count, function moved" rather than "exactly 54".
#[test]
fn sanity_agg_compile_has_at_least_30_op_strings() {
    let ops = extract_agg_compile_op_strings();
    assert!(
        ops.len() >= 30,
        "Sanity check failed: extracted only {} op-strings from \
         agg_compile.rs::parse_agg_kind — expected ≥ 30 (54 in the \
         catalogue per CONTEXT.md). Was the function moved or renamed? \
         If parse_agg_kind was renamed, update extract_agg_compile_op_strings \
         in this test file to match. Got: {ops:?}",
        ops.len()
    );
}

/// **Test 3 — Sanity: register_validate op-string extractor isn't a no-op.**
/// Same drift-guard as Test 2, applied to `lifetime_bound_for_op_str`. If
/// the function were renamed or moved out of `register_validate.rs`, the
/// extractor returns 0 op-strings and the coverage check would fail with a
/// noisy "everything missing" assertion (which is correct for that state)
/// — but this sanity guard surfaces the root cause directly.
#[test]
fn sanity_register_validate_has_lifetime_bound_function() {
    let ops = extract_register_validate_op_strings();
    assert!(
        ops.len() >= 30,
        "Sanity check failed: extracted only {} op-strings from \
         register_validate.rs::lifetime_bound_for_op_str — expected ≥ 30 \
         (54 in the catalogue per CONTEXT.md). Was the function moved or \
         renamed? If lifetime_bound_for_op_str was renamed, update \
         extract_register_validate_op_strings in this test file to match. \
         Got: {ops:?}",
        ops.len()
    );
}
