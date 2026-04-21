#!/usr/bin/env bash
# Phase 60 TPC-PERF-10 — grep-ZERO ship-gate for hot-key salting.
#
# Exits 0 when every expected salt-feature call site is present.
# Exits 1 during Waves 0..3 as each grep target is wired up.
#
# Checks (all must pass for exit 0):
#   1. parse_shard_key_with_salt in src/engine/join_validator.rs (Wave 1).
#   2. salt_cardinality (>= 3 occurrences) in src/engine/pipeline.rs (Wave 1 + 2).
#   3. shard_hint_for_event_salted OR salt_cardinality in src/routing/shard_hint.rs (Wave 2).
#   4. salted_streams in src/server/shard_probe.rs (Wave 4).
#   5. 3 new metrics in src/shard/metrics.rs (Wave 4):
#        beava_shard_hot_key_owner_ratio
#        beava_salt_fanout_reads_total
#        beava_salt_ingest_writes_total
#   6. pareto_salted_c8_x8 group in benches/pareto_workload.rs (Wave 0 stub, Wave 4 real).

set -u

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT" || { echo "cd to repo root failed"; exit 1; }

fail=0

check() {
    local label="$1"
    local path="$2"
    local pattern="$3"
    local min="$4"
    local count
    if [[ ! -f "$path" ]]; then
        echo "FAIL [$label]: $path missing"
        fail=$((fail + 1))
        return
    fi
    count=$(grep -c -E "$pattern" "$path" 2>/dev/null)
    count="${count:-0}"
    if (( count >= min )); then
        echo "OK   [$label]: $path has $count match(es) for '$pattern' (>= $min)"
    else
        echo "FAIL [$label]: $path has $count match(es) for '$pattern' (< $min)"
        fail=$((fail + 1))
    fi
}

check "parser-rust" src/engine/join_validator.rs "parse_shard_key_with_salt" 1
check "stream-def-field" src/engine/pipeline.rs "salt_cardinality" 3
check "shard-hint" src/routing/shard_hint.rs "shard_hint_for_event_salted|salt_cardinality" 1
check "shard-probe" src/server/shard_probe.rs "salted_streams" 1
check "metrics" src/shard/metrics.rs \
    "beava_shard_hot_key_owner_ratio|beava_salt_fanout_reads_total|beava_salt_ingest_writes_total" 3
check "bench-group" benches/pareto_workload.rs "pareto_salted_c8_x8" 1

if (( fail > 0 )); then
    echo ""
    echo "verify-salt-feature-complete: $fail check(s) failed — exit 1"
    exit 1
fi

echo ""
echo "verify-salt-feature-complete: all checks passed — exit 0"
exit 0
