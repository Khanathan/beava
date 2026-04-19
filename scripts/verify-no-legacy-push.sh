#!/usr/bin/env bash
set -euo pipefail
# Phase 54 — Grep-ZERO gate for Success Criterion #3 (TPC-ARCH-01).
#
# Asserts: NO legacy push helpers defined in src/:
#   - fn push_internal
#   - fn push_batch_with_cascade_no_features
#   - fn push_with_cascade_internal
#
# Wave 0 expectation: EXIT 1 (RED). All three helpers still present.
# Wave 4 expectation: EXIT 0 (GREEN) — all deleted; only
# push_with_cascade_on_shard remains as the shard-thread entry point.

HITS=$(grep -rn -E "\bfn (push_internal|push_batch_with_cascade_no_features|push_with_cascade_internal)\b" src/ --include="*.rs" || true)

if [ -n "$HITS" ]; then
    echo "FAIL: legacy push helpers still defined in src/:" >&2
    echo "$HITS" >&2
    exit 1
fi

echo "OK: zero legacy push helpers defined in src/"
