#!/usr/bin/env bash
# Phase 19.1 rebaseline runner — re-runs the targeted matrix per CONTEXT D-21
# against the 5 canonical pipelines after Plans 19.1-01..19.1-04 have landed.
#
# Output: 5 ledger rows for the new `## 1M-event blast (rebaseline 19.1)` section
# in .planning/throughput-baselines.md.
#
# Usage:
#   ./crates/beava-bench/scripts/run_19_1_rebaseline.sh           # run all 5
#   ./crates/beava-bench/scripts/run_19_1_rebaseline.sh small     # one pipeline
#
# Runs in --no-ledger mode (caller appends rows manually to keep the ledger's
# append-only audit-trail discipline transparent — no auto-magical writes).
#
# Per CONTEXT D-21..D-25:
# - Canonical matrix: small + medium + large + large_phase9 + fraud-team
# - Single blast shape: zipfian (zipf-alpha=1.0)
# - Single transport+format: tcp + msgpack
# - Single mode: continuous
# - parallel=16, pipeline-depth=1024, total-events=1_000_000
# - cardinality=1_000_000 for {small,medium,large,large_phase9}; cardinality=10_000
#   for fraud-team (steady warm-key state — K=1M would be memory-pressure dominated
#   by the 14-node × 110-feature pipeline footprint per orchestrator decision)
# - --isolation-mode for wall_clock_ms / send_drain_ms / ack_lag_ms columns
# - --no-ledger so the executor appends ledger rows manually after parsing stdout.
#
# Threshold gates apply to the canonical small zipfian cell only:
#   small ≤ 2 s     (BLOCKING — Phase 19.1 verdict-flip per D-24)
#   medium ≤ 4 s    (capture-only)
#   large ≤ 8 s     (capture-only)
#   large_phase9 ≤ 12 s   (capture-only)
#   fraud-team — no threshold; first baseline establishes the floor.

set -euo pipefail

# Resolve workspace root (scripts/ -> beava-bench/ -> crates/ -> root)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORKSPACE_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
cd "$WORKSPACE_ROOT"

BIN="${BIN:-./target/release/beava-bench-v18}"
N="${N:-1000000}"
PARALLEL="${PARALLEL:-16}"
PD="${PD:-1024}"
ZIPF_ALPHA="${ZIPF_ALPHA:-1.0}"

# Default cardinality for simple pipelines; fraud-team overrides below.
DEFAULT_CARDINALITY="${CARDINALITY:-1000000}"

PIPELINES_ALL=(small medium large large_phase9 fraud-team)
if [[ $# -gt 0 ]]; then
  PIPELINES=("$@")
else
  PIPELINES=("${PIPELINES_ALL[@]}")
fi

if [[ ! -x "$BIN" ]]; then
  echo "ERROR: $BIN is not executable. Build first:" >&2
  echo "  cargo build --release -p beava-bench --bin beava-bench-v18" >&2
  exit 1
fi

DATE_ISO="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
COMMIT_SHA="$(git rev-parse --short HEAD 2>/dev/null || echo unknown)"

echo "# Phase 19.1 rebaseline matrix"
echo "# date=$DATE_ISO commit=$COMMIT_SHA"
echo "# bin=$BIN N=$N parallel=$PARALLEL pd=$PD zipf_alpha=$ZIPF_ALPHA"
echo "# Pipelines: ${PIPELINES[*]}"
echo

for pipeline in "${PIPELINES[@]}"; do
  config="crates/beava-bench/configs/${pipeline}.json"
  if [[ ! -f "$config" ]]; then
    echo "ERROR: config not found: $config" >&2
    exit 1
  fi

  # fraud-team uses K=10_000 zipfian to land in steady warm-key state; the
  # 14-node 110-feature pipeline at K=1_000_000 is memory-pressure-dominated
  # rather than apply-cost-dominated (orchestrator decision per CONTEXT D-21).
  if [[ "$pipeline" == "fraud-team" ]]; then
    cell_cardinality="${FRAUD_TEAM_CARDINALITY:-10000}"
  else
    cell_cardinality="$DEFAULT_CARDINALITY"
  fi

  echo "=== $pipeline (cardinality=$cell_cardinality) ==="

  # NB: stderr carries the human summary (including isolation_mode line);
  # stdout would carry the markdown ledger row but we pass --no-ledger so
  # nothing prints to stdout. Capture both via 2>&1 to a single block.
  "$BIN" \
    --pipeline "$config" \
    --transport tcp \
    --wire-format msgpack \
    --parallel "$PARALLEL" \
    --pipeline-depth "$PD" \
    --total-events "$N" \
    --blast-shape zipfian \
    --zipf-alpha "$ZIPF_ALPHA" \
    --cardinality "$cell_cardinality" \
    --continuous-pipeline true \
    --isolation-mode \
    --no-ledger 2>&1
  echo
done

echo "# Done."
echo "# Manually append rows to .planning/throughput-baselines.md under section:"
echo "#   ## 1M-event blast (rebaseline 19.1)"
