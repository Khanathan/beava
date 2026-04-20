// Phase 54 TPC-ARCH-01 grep-ZERO gates.
//
// This integration-test crate hosts the three structural invariants that
// enforce the Phase 54 architectural cleanup:
//
//   1. `dashmap_not_in_src`      — no `DashMap` symbol in src/ (Success Criterion 1).
//   2. `state_store_struct_deleted` — no `struct StateStore` definition in src/ (SC 2).
//   3. `legacy_push_helpers_deleted` — no legacy push helpers defined in src/ (SC 3).
//
// Wave 0 (plan 54-00): added with `#[ignore]` (RED) so they didn't fail a
// pre-migration `cargo test --lib`. Wave 4 close (plan 54-04): `#[ignore]`
// removed — all three gates are GREEN and enforce on every default run.
//
// The pre-existing SHIP-01 integration test (live-ingest → crash → recover
// parity) was removed at the Wave 4 close because it read features through
// the deleted `state.store` compat shim. A shard-based rewrite of that test
// is tracked as a 54-NEXT migration item.

// ---------------------------------------------------------------------------
// Walker: collect (file, line_number, line_text) tuples where the predicate
// matches. Comment lines (//, //!, *, /*) are skipped.
// ---------------------------------------------------------------------------

fn collect_violations<F>(src_root: &std::path::Path, pred: F) -> Vec<(String, usize, String)>
where
    F: Fn(&str) -> bool,
{
    fn walk<F>(
        dir: &std::path::Path,
        pred: &F,
        out: &mut Vec<(String, usize, String)>,
    ) where
        F: Fn(&str) -> bool,
    {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                walk(&path, pred, out);
            } else if path.extension().and_then(|e| e.to_str()) == Some("rs") {
                let Ok(contents) = std::fs::read_to_string(&path) else {
                    continue;
                };
                for (lineno, line) in contents.lines().enumerate() {
                    let trimmed = line.trim_start();
                    // Skip comment lines.
                    if trimmed.starts_with("//")
                        || trimmed.starts_with("*")
                        || trimmed.starts_with("/*")
                    {
                        continue;
                    }
                    if pred(line) {
                        out.push((path.display().to_string(), lineno + 1, line.to_string()));
                    }
                }
            }
        }
    }

    let mut out = Vec::new();
    walk(src_root, &pred, &mut out);
    out
}

// ---------------------------------------------------------------------------
// Phase 54 Success Criterion #1 — no `DashMap` symbol in src/.
// Wave 0: RED. Wave 4 close: GREEN (enforced on every `cargo test`).
// ---------------------------------------------------------------------------
#[test]
fn dashmap_not_in_src() {
    let src_root = std::path::Path::new("src");
    let hits = collect_violations(src_root, |line| line.contains("DashMap"));
    assert!(
        hits.is_empty(),
        "TPC-ARCH-01 SC#1: DashMap references found in src/ ({} hits). \
         First 10:\n{}",
        hits.len(),
        hits.iter()
            .take(10)
            .map(|(f, n, l)| format!("  {f}:{n}: {}", l.trim()))
            .collect::<Vec<_>>()
            .join("\n")
    );
}

// ---------------------------------------------------------------------------
// Phase 54 Success Criterion #2 — no `struct StateStore` definition in src/.
// Type aliases (`type StateStore = ...`) are allowed — only a fresh struct
// definition is forbidden.
// Wave 0: RED. Wave 4 close: GREEN.
// ---------------------------------------------------------------------------
#[test]
fn state_store_struct_deleted() {
    let src_root = std::path::Path::new("src");
    let hits = collect_violations(src_root, |line| {
        let trimmed = line.trim_start();
        trimmed.starts_with("pub struct StateStore")
            || trimmed.starts_with("struct StateStore")
            || trimmed.starts_with("pub(crate) struct StateStore")
    });
    assert!(
        hits.is_empty(),
        "TPC-ARCH-01 SC#2: StateStore struct definition found in src/:\n{}",
        hits.iter()
            .map(|(f, n, l)| format!("  {f}:{n}: {}", l.trim()))
            .collect::<Vec<_>>()
            .join("\n")
    );
}

// ---------------------------------------------------------------------------
// Phase 54 Success Criterion #3 — no legacy push helpers defined in src/.
// The three forbidden helpers are:
//   - `push_internal`
//   - `push_batch_with_cascade_no_features`
//   - `push_with_cascade_internal`
// `push_internal_on_shard` is the Phase 50.5 shard-thread helper — NOT
// legacy — so the predicate matches on function-definition form (`fn name(`)
// to avoid substring false positives.
// Wave 0: RED. Wave 4 close: GREEN — only `push_with_cascade_on_shard`
// remains as the shard thread entry point.
// ---------------------------------------------------------------------------
#[test]
fn legacy_push_helpers_deleted() {
    let src_root = std::path::Path::new("src");
    let hits = collect_violations(src_root, |line| {
        let forbidden = [
            "fn push_internal(",
            "fn push_batch_with_cascade_no_features(",
            "fn push_with_cascade_internal(",
        ];
        forbidden.iter().any(|f| line.contains(f))
    });
    assert!(
        hits.is_empty(),
        "TPC-ARCH-01 SC#3: legacy push helpers still defined in src/:\n{}",
        hits.iter()
            .map(|(f, n, l)| format!("  {f}:{n}: {}", l.trim()))
            .collect::<Vec<_>>()
            .join("\n")
    );
}
