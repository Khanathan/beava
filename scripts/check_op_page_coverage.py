#!/usr/bin/env python3
"""
Phase 13.0 Plan 05 — operator-catalog coverage tripwire.

Asserts 1:1 between docs/operators/<family>/<op>.md page files and the
known-good 54-path catalogue (defined in scripts/scaffold_op_pages.py;
54 paths = 53 unique AggKind variants + ema alias inline in ewma.md).
ema is special-cased — must be documented INSIDE ewma.md as an alias.

Used as CI tripwire post-Plan 13.0-15. If a new op gets added to the engine
without a corresponding docs page, this fails and forces the addition.
Conversely, if an op page is added or removed without updating the
canonical OPS list, this fails too.

Usage: python3 scripts/check_op_page_coverage.py
Exits 0 on coverage match; exits 1 with diagnostics on mismatch.
"""
from __future__ import annotations

import pathlib
import sys

# Import OPS list from sibling scaffold script (single source of truth).
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
from scaffold_op_pages import OPS  # noqa: E402


# Files allowed under docs/operators/ that are NOT op-page stubs.
NON_OP_PAGES = {"index.md", "cost-class.md", ".keep"}


def main() -> int:
    repo_root = pathlib.Path(__file__).resolve().parent.parent
    docs_dir = repo_root / "docs" / "operators"

    expected_paths = {
        docs_dir / family / f"{op}.md"
        for family, op, _, _ in OPS
    }

    actual_paths = {
        p for p in docs_dir.rglob("*.md")
        if p.name not in NON_OP_PAGES
    }

    missing = expected_paths - actual_paths
    extra = actual_paths - expected_paths

    if missing or extra:
        print("FAIL — op-page coverage mismatch:", file=sys.stderr)
        for p in sorted(missing):
            print(f"  MISSING: {p.relative_to(repo_root)}", file=sys.stderr)
        for p in sorted(extra):
            print(f"  UNEXPECTED: {p.relative_to(repo_root)}", file=sys.stderr)
        print(
            "\n  hint: update OPS in scripts/scaffold_op_pages.py to match the "
            "engine catalogue, then re-run scaffold_op_pages.py",
            file=sys.stderr,
        )
        return 1

    # Verify ewma has Aliases section with bv.ema (the alias is inline, not a separate page).
    ewma_path = docs_dir / "decay" / "ewma.md"
    ewma_text = ewma_path.read_text()
    if "## Aliases" not in ewma_text or "bv.ema" not in ewma_text:
        print(
            f"FAIL — {ewma_path.relative_to(repo_root)} missing '## Aliases' "
            "section or 'bv.ema' reference (the ema alias must be documented inline; "
            "it is not a separate page).",
            file=sys.stderr,
        )
        return 1

    print(
        f"OK — {len(expected_paths)} op pages match catalogue "
        "(53 unique AggKind variants + ema alias inline in ewma.md = 54 page paths)"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
