#!/usr/bin/env bash
# Phase 56 Wave 4 — cross-shard metrics surface check.
#
# Grep-style invariant: the 5 Phase-56 counter names MUST be registered
# under src/ AND must be pre-seeded by the shard bootstrap (so /metrics
# always surfaces them, even before any cross-shard traffic). This is
# the cheap operator-level gate that the metrics plumbing survived
# through Wave 3 close and wasn't accidentally stripped.
#
# Invariants enforced:
#   1. Each of the 5 Phase-56 counter name-literals appears in src/.
#   2. Each counter is pre-seeded with a 0-increment at init (so the
#      /metrics surface is non-empty on a cold server).
#   3. The Rust const defining each counter is exported from
#      src/shard/metrics.rs as a `pub const`.
#
# Exit 0 on success; non-zero with a single-line diagnostic on failure.
#
# Invocation:
#   bash scripts/verify-crossshard-metrics.sh
#
# This is a fast static check (no server spin-up, no port binding, no
# durable state). The full wire-level counter scrape is exercised by
# the `cargo test --release --test cascade_metrics` suite and by the
# Phase-56 Wave-2/Wave-3 integration tests.
set -uo pipefail

FAIL=0
REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO"

# The 5 counters shipped in Phase 56 (D-D4 + ROADMAP-locked).
COUNTERS=(
    beava_enrich_cross_shard_total
    beava_enrich_intra_shard_total
    beava_enrich_missing_total
    beava_ssj_cross_shard_total
    beava_crossshard_joins_registered_total
)

# Const names that declare those counters in src/shard/metrics.rs.
CONSTS=(
    ENRICH_CROSS_SHARD_TOTAL
    ENRICH_INTRA_SHARD_TOTAL
    ENRICH_MISSING_TOTAL
    SSJ_CROSS_SHARD_TOTAL
    CROSSSHARD_JOINS_REGISTERED_TOTAL
)

# 1. All 5 counter name-literals MUST appear in src/ (anchored grep:
#    literal must appear inside a double-quoted string).
for m in "${COUNTERS[@]}"; do
    if ! grep -rq "\"$m\"" src/ 2>/dev/null; then
        echo "FAIL: counter '$m' not emitted anywhere in src/"
        FAIL=1
    fi
done

# 2. All 5 must be pre-seeded in the metrics bootstrap. Rustfmt wraps long
#    counter!() calls over multiple lines, so we flatten the file by replacing
#    newlines with spaces and grep for `counter!(<CONST>,<anything>).increment(0)`
#    on the collapsed buffer.
FLAT_METRICS="$(tr '\n' ' ' < src/shard/metrics.rs)"
for c in "${CONSTS[@]}"; do
    if ! grep -q "$c" src/shard/metrics.rs 2>/dev/null; then
        echo "FAIL: const $c missing from src/shard/metrics.rs"
        FAIL=1
        continue
    fi
    if ! printf '%s' "$FLAT_METRICS" | grep -Eq "counter!\($c[^a-zA-Z0-9_][^)]*\)[[:space:]]*\.increment\(0\)"; then
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
    echo "OK — all 5 Phase-56 counter names registered + pre-seeded in src/shard/metrics.rs (${COUNTERS[*]})"
    exit 0
else
    exit 1
fi
