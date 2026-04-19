#!/usr/bin/env bash
set -euo pipefail
# Phase 54 — Grep-ZERO gate for Success Criterion #1 (TPC-ARCH-01).
#
# Asserts: NO `DashMap` reference in any .rs file under src/, except occurrences
# inside line-comments (// or //!) and block-comment continuations (leading `*`).
#
# Wave 0 expectation: EXIT 1 (RED). DashMap is still present in ~140 hits.
# Wave 4 expectation: EXIT 0 (GREEN) — all DashMap references deleted.

HITS=$(grep -rn "DashMap" src/ \
    --include="*.rs" \
    | grep -v -E '^[^:]+:[0-9]+:[[:space:]]*//' \
    | grep -v -E '^[^:]+:[0-9]+:[[:space:]]*\*' \
    || true)

if [ -n "$HITS" ]; then
    echo "FAIL: DashMap references found in src/ (outside comments):" >&2
    echo "$HITS" | head -50 >&2
    total=$(echo "$HITS" | wc -l | tr -d ' ')
    echo "Total hits: $total" >&2
    exit 1
fi

echo "OK: zero DashMap references in src/ (excluding comments)"
