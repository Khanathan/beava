#!/usr/bin/env bash
# Phase 57 Wave 4 — retraction metrics surface check.
#
# Grep-style invariant: the 5 Phase-57 retraction counter names MUST be
# registered under src/ AND must be pre-seeded by the shard bootstrap so
# `/metrics` surfaces them even before any retraction traffic. This is the
# cheap operator-level gate — mirrors scripts/verify-crossshard-metrics.sh
# for Phase 56.
#
# Invariants enforced:
#   1. Each of the 5 Phase-57 counter name-literals appears in src/.
#   2. Each counter is pre-seeded with a 0-increment at init (so the
#      /metrics surface is non-empty on a cold server).
#   3. The Rust const defining each counter is exported from
#      src/shard/metrics.rs as a `pub const`.
#
# Exit 0 on success; non-zero with a single-line diagnostic on failure.
#
# Invocation:
#   bash scripts/verify-retraction-metrics.sh
#
# This is a fast static check (no server spin-up, no port binding, no
# durable state). The full wire-level counter scrape is exercised by the
# Phase-57 Wave-1/2/3 integration tests.
set -uo pipefail

FAIL=0
REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO"

# The 5 counters shipped in Phase 57 (D-D2 + ROADMAP-locked).
COUNTERS=(
    beava_retractions_sent_total
    beava_retractions_applied_total
    beava_retractions_nooped_total
    beava_retraction_beyond_history_total
    beava_retraction_depth_exceeded_total
)

# Const names that declare those counters in src/shard/metrics.rs.
CONSTS=(
    RETRACTIONS_SENT_TOTAL
    RETRACTIONS_APPLIED_TOTAL
    RETRACTIONS_NOOPED_TOTAL
    RETRACTION_BEYOND_HISTORY_TOTAL
    RETRACTION_DEPTH_EXCEEDED_TOTAL
)

# 1. All 5 counter name-literals MUST appear in src/ (literal must appear
#    inside a double-quoted string).
for m in "${COUNTERS[@]}"; do
    if ! grep -rq "\"$m\"" src/ 2>/dev/null; then
        echo "FAIL: counter '$m' not emitted anywhere in src/"
        FAIL=1
    fi
done

# 2. All 5 must be pre-seeded in the metrics bootstrap. Rustfmt wraps long
#    counter!() calls over multiple lines, so we flatten the file by
#    replacing newlines with spaces and grep for
#    `counter!(<CONST>,<anything>).increment(0)` on the collapsed buffer.
FLAT_METRICS="$(tr '\n' ' ' < src/shard/metrics.rs)"
for c in "${CONSTS[@]}"; do
    if ! grep -q "$c" src/shard/metrics.rs 2>/dev/null; then
        echo "FAIL: const $c missing from src/shard/metrics.rs"
        FAIL=1
        continue
    fi
    # Some Phase-57 counters are pre-seeded without any label pair (e.g.
    # RETRACTION_DEPTH_EXCEEDED_TOTAL uses `counter!(CONST).increment(0)`
    # with no label), others have one or more `"name" => "__init__"` label
    # pairs and may be formatted across multiple lines by rustfmt. Tolerate
    # all shapes: match `counter!(` + optional whitespace + const + boundary
    # + any content up to the first `)` + optional whitespace + `.increment(0)`.
    if ! printf '%s' "$FLAT_METRICS" | grep -Eq "counter!\([[:space:]]*$c[^a-zA-Z0-9_][^)]*\)[[:space:]]*\.increment\(0\)|counter!\([[:space:]]*$c[[:space:]]*\)[[:space:]]*\.increment\(0\)"; then
        echo "FAIL: const $c not pre-seeded with increment(0) in src/shard/metrics.rs"
        FAIL=1
    fi
done

# 3. Each const must be declared `pub const ... = "..."` in metrics.rs so
#    downstream modules can reference it.
for c in "${CONSTS[@]}"; do
    if ! grep -E "^pub const $c" src/shard/metrics.rs >/dev/null 2>&1; then
        echo "FAIL: $c is not declared as 'pub const ...' in src/shard/metrics.rs"
        FAIL=1
    fi
done

if [ "$FAIL" -eq 0 ]; then
    echo "OK — all 5 Phase-57 retraction counter names registered + pre-seeded in src/shard/metrics.rs (${COUNTERS[*]})"
    exit 0
else
    exit 1
fi
