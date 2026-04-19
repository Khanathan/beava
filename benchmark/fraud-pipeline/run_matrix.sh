#!/usr/bin/env bash
# Phase 46 D-03: 9-cell benchmark matrix runner.
# Drives the explicit 9-cell grid documented in 46-RESEARCH.md Gap 12.
# Writes per-cell summary.json under results/${BACKEND}/matrix-<timestamp>/<cell>/.
# Exit 0 if every cell passes the -5% baseline gate; exit 1 otherwise.
#
# The 9 cells (mode, cpus, clients):
#   (simple,1,1)  (simple,4,4)  (simple,8,8)
#   (simple,1,4)  (simple,4,1)  (simple,4,8)
#   (complex,1,1) (complex,4,4) (complex,8,8)
#
# Usage:
#   bash benchmark/fraud-pipeline/run_matrix.sh
#   DURATION=30 bash benchmark/fraud-pipeline/run_matrix.sh
#   bash benchmark/fraud-pipeline/run_matrix.sh --backend fjall
#   bash benchmark/fraud-pipeline/run_matrix.sh --backend inmem
#
# Phase 50 TPC multi-shard support:
#   Set BEAVA_SHARDS=N to run with N shard threads (default: 1, preserves Phase 49 behavior).
#   Set BEAVA_SHARDS=auto to use physical CPU count (ship-gate workload):
#     BEAVA_SHARDS=auto DURATION=30 bash benchmark/fraud-pipeline/run_matrix.sh
#
# Phase 53 fjall backend selection (--backend flag, Plan 53-06 Task 1):
#   --backend fjall  (default)  — builds the release binary with default features (fjall-backed state)
#   --backend inmem             — builds with --features state-inmem (Phase 49 AHashMap legacy path)
#   Results land under results/${BACKEND}/matrix-<timestamp>/.  The two runs can be compared
#   cell-by-cell to compute the Plan 53-06 -15% regression gate.
#
# Ship-gate criteria (Phase 50):
#   complex-c8-x8 at N=CPU_COUNT: >= 918,621 EPS (3× Phase 49 baseline of 306,207 EPS)
#   shard_probe cross_shard_fraction: < 0.40

set -euo pipefail

# --------------------------------------------------------------------------
# Parse --backend flag (Phase 53-06 Task 1)
# --------------------------------------------------------------------------
BACKEND="${BEAVA_MATRIX_BACKEND:-fjall}"   # fjall | inmem
while [[ $# -gt 0 ]]; do
    case "$1" in
        --backend)
            if [[ $# -lt 2 ]]; then
                echo "[run_matrix] --backend requires an argument (fjall|inmem)" >&2
                exit 2
            fi
            BACKEND="$2"
            shift 2
            ;;
        --backend=*)
            BACKEND="${1#--backend=}"
            shift
            ;;
        *)
            echo "[run_matrix] unknown argument: $1" >&2
            shift
            ;;
    esac
done
case "$BACKEND" in
    fjall|inmem) ;;
    *) echo "[run_matrix] --backend must be 'fjall' or 'inmem', got: $BACKEND" >&2; exit 2 ;;
esac
export BACKEND

# --------------------------------------------------------------------------
# Build the server for the chosen backend (Phase 53-06).  We force a release
# build before entering the cell loop so every cell uses an identical binary
# and the per-cell run_bench.sh paths never race on a partial build.
# --------------------------------------------------------------------------
# Move to the repo root so `cargo build` sees the Cargo.toml.  We record the
# pre-cd directory so we can switch back to this script's dir for the run loop.
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

if [[ "$BACKEND" == "inmem" ]]; then
    CARGO_FEATURES_FLAG="--features state-inmem"
else
    CARGO_FEATURES_FLAG=""   # default build = fjall
fi

echo "[run_matrix] backend=${BACKEND} (cargo_features=${CARGO_FEATURES_FLAG:-<default>})"
(
    cd "$REPO_ROOT"
    # Build both beava bin (post-rename) and fall back if not present.
    # We keep both names for robustness with compare-runs.
    if ! cargo build --release --bin beava ${CARGO_FEATURES_FLAG} 2>&1 | tail -20; then
        if ! cargo build --release --bin tally ${CARGO_FEATURES_FLAG} 2>&1 | tail -20; then
            echo "[run_matrix] FATAL: cargo build --release ${CARGO_FEATURES_FLAG} failed" >&2
            exit 2
        fi
    fi
)
# Force run_bench.sh to skip its own build step (backend selection is ours).
export SKIP_BUILD=1
# Signal the server (informational; production build is always fjall, inmem
# is a Cargo feature so the binary *is* the switch) which backend it is.
export BEAVA_STATE_BACKEND="${BACKEND}"

cd "$SCRIPT_DIR"

# Phase 50: resolve BEAVA_SHARDS.
# "auto" → physical CPU count; unset → 1 (regression baseline).
_RAW_SHARDS="${BEAVA_SHARDS:-1}"
if [ "$_RAW_SHARDS" = "auto" ]; then
    BEAVA_SHARDS="$(nproc 2>/dev/null || sysctl -n hw.physicalcpu 2>/dev/null || echo 1)"
    echo "[run_matrix] BEAVA_SHARDS=auto resolved to ${BEAVA_SHARDS} (physical CPU count)"
else
    BEAVA_SHARDS="$_RAW_SHARDS"
fi
export BEAVA_SHARDS

DURATION="${DURATION:-60}"
TS="$(date +%Y%m%d-%H%M%S)"
OUT="results/${BACKEND}/matrix-${TS}"
mkdir -p "$OUT"

echo "=== Phase 46 9-cell benchmark matrix (backend=${BACKEND}, duration=${DURATION}s, BEAVA_SHARDS=${BEAVA_SHARDS}) ==="
echo "    results -> ${OUT}"
echo

# Explicit 9 cells: each row is "MODE CPUS CLIENTS"
CELLS=(
    "simple  1 1"
    "simple  4 4"
    "simple  8 8"
    "simple  1 4"
    "simple  4 1"
    "simple  4 8"
    "complex 1 1"
    "complex 4 4"
    "complex 8 8"
)

FAIL=0

for cell_spec in "${CELLS[@]}"; do
    read -r mode cpus clients <<< "$cell_spec"
    CELL="${mode}-c${cpus}-x${clients}"
    CELL_DIR="${OUT}/${CELL}"
    mkdir -p "$CELL_DIR"

    echo "=== cell ${CELL} ==="
    if ! MODE="$mode" CPUS="$cpus" CLIENTS="$clients" DURATION="$DURATION" \
           OUTPUT_DIR="$CELL_DIR" ./run_bench.sh; then
        echo "  BENCH FAILED in cell ${CELL}"
        FAIL=1
        continue
    fi

    CELL_SUMMARY="${CELL_DIR}/summary.json"
    if [ ! -f "$CELL_SUMMARY" ]; then
        echo "  MISSING summary.json in cell ${CELL}"
        FAIL=1
        continue
    fi

    if ! ./compare_baseline.sh "$CELL_SUMMARY"; then
        echo "  REGRESSION in cell ${CELL}"
        FAIL=1
    fi
done

echo
if [ "$FAIL" -eq 0 ]; then
    echo "ALL 9 CELLS WITHIN -5% OF BASELINE"
else
    echo "REGRESSION DETECTED — do not merge"
    exit 1
fi
