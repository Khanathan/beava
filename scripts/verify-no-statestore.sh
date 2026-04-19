#!/usr/bin/env bash
set -euo pipefail
# Phase 54 — Grep-ZERO gate for Success Criterion #2 (TPC-ARCH-01).
#
# Asserts: NO `struct StateStore` (with optional `pub`) defined in src/.
# Type aliases like `pub type StateStore = ...` are allowed (used by Wave 3
# migration-compat layer).
#
# Wave 0 expectation: EXIT 1 (RED). StateStore struct is still present.
# Wave 4 expectation: EXIT 0 (GREEN) — struct definition deleted.

HITS=$(grep -rn -E "^[[:space:]]*(pub )?struct StateStore\b" src/ --include="*.rs" || true)

if [ -n "$HITS" ]; then
    echo "FAIL: StateStore struct definition found in src/:" >&2
    echo "$HITS" >&2
    exit 1
fi

echo "OK: zero StateStore struct definitions in src/"
