//! Static regression test: the Phase 19.1 rebaseline section MUST stay in
//! `.planning/throughput-baselines.md` once Plan 19.1-05 lands. Catches accidental
//! ledger truncation in future phases (per Phase 7.5 D-09 append-only ledger
//! discipline + Phase 19.1 D-22).
//!
//! Pre-Plan-19.1-05: the section doesn't exist; this test FAILS (RED).
//! Post-Plan-19.1-05 GREEN: the section exists with 5 rows; test PASSES.
//!
//! What we assert:
//! 1. The section header `## 1M-event blast (rebaseline 19.1)` is in the ledger.
//! 2. Within that section's bounds (header up to next `## ` header, EOF, or end),
//!    each of the 5 expected pipeline names (`small`, `medium`, `large`,
//!    `large_phase9`, `fraud-team`) appears in at least one ledger row.
//! 3. The section has at least 5 rows tagged with `| 19.1 |` (Phase column).

use std::path::PathBuf;

#[test]
#[ignore = "requires .planning/throughput-baselines.md from the private planning tree (stripped from public OSS clone); run with --ignored in the private repo only"]
fn test_19_1_section_in_ledger() {
    // CARGO_MANIFEST_DIR = $WORKSPACE_ROOT/crates/beava-bench, so workspace root is two up.
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let workspace_root = manifest
        .parent()
        .and_then(|p| p.parent())
        .unwrap_or_else(|| panic!("CARGO_MANIFEST_DIR has no two-up ancestor: {manifest:?}"));
    let ledger = workspace_root.join(".planning/throughput-baselines.md");

    let content = std::fs::read_to_string(&ledger)
        .unwrap_or_else(|e| panic!("failed to read {}: {e}", ledger.display()));

    // (1) Section header must exist.
    let header = "## 1M-event blast (rebaseline 19.1)";
    assert!(
        content.contains(header),
        "throughput-baselines.md missing section `{header}` — Plan 19.1-05 should append it. \
         Path: {}",
        ledger.display()
    );

    // (2) Bound the section: from the header to the next `\n## ` (or EOF).
    let section_start = content
        .find(header)
        .expect("header presence already asserted");
    let after_header = &content[section_start..];
    // Skip past the header itself before searching for the next `\n## `, so we
    // don't accidentally clamp the section to its own line.
    let header_len = header.len();
    let search_window = &after_header[header_len..];
    let section_end_rel = search_window
        .find("\n## ")
        .map(|p| header_len + p)
        .unwrap_or(after_header.len());
    let section = &after_header[..section_end_rel];

    // (3) Each pipeline name must appear in some row of this section.
    for name in &["small", "medium", "large", "large_phase9", "fraud-team"] {
        let needle = format!("| {name} |");
        assert!(
            section.contains(&needle),
            "rebaseline section missing row for pipeline `{name}` (looking for `{needle}`)"
        );
    }

    // (4) At least 5 rows tagged `| 19.1 |` (Phase column).
    let row_count = section.matches("| 19.1 |").count();
    assert!(
        row_count >= 5,
        "rebaseline section has {row_count} rows tagged `| 19.1 |`; expected ≥ 5"
    );
}
