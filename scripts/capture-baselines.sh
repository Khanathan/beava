#!/usr/bin/env bash
# Beava v2 — Perf Baseline Capture Script
#
# Usage:
#   ./scripts/capture-baselines.sh
#   ./scripts/capture-baselines.sh > /tmp/baseline-block.md
#
# Runs `cargo bench --workspace` and `pytest --benchmark-only` on the
# current machine, then emits a Markdown section to stdout in the shape
# expected by .planning/perf-baselines.md.
#
# To append a new hw-class section to the baselines file, run:
#   ./scripts/capture-baselines.sh >> .planning/perf-baselines.md
#
# Idempotency: the script only emits a new markdown block; it does not
# modify .planning/perf-baselines.md itself.  Editing the file to remove
# placeholder rows or update existing hw-class numbers is a manual step.
#
# Dependencies: cargo, python3 (with pytest-benchmark installed), jq (optional)
# All other dependencies are part of the repo dev stack.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PYBENCH_JSON="/tmp/beava-pybench.json"
CAPTURE_DATE="$(date +%Y-%m-%d)"

# ─── hw-class detection (CONTEXT D-03) ────────────────────────────────────────

detect_hw_class() {
  if [[ "$(uname -s)" == "Darwin" ]]; then
    local cpu ncpu uname_sr
    cpu="$(sysctl -n machdep.cpu.brand_string 2>/dev/null | tr ' ' '-')"
    ncpu="$(sysctl -n hw.ncpu 2>/dev/null)"
    uname_sr="$(uname -sr | tr ' ' '-')"
    echo "${cpu} / ${uname_sr} / ${ncpu} cores"
  else
    local cpu nproc_out uname_sr
    # Linux: use lscpu for model name, nproc for core count.
    if command -v lscpu &>/dev/null; then
      cpu="$(lscpu | awk -F: '/Model name/ {print $2}' | xargs | tr ' ' '-')"
    else
      cpu="unknown-cpu"
    fi
    nproc_out="$(nproc 2>/dev/null || echo '?')"
    uname_sr="$(uname -sr | tr ' ' '-')"
    echo "${cpu} / ${uname_sr} / ${nproc_out} cores"
  fi
}

HW_CLASS="$(detect_hw_class)"

# ─── JSON parsing helper ───────────────────────────────────────────────────────

# extract_criterion_median path/to/estimates.json
# Returns the median point estimate in nanoseconds (as float).
extract_criterion_median() {
  local estimates_json="$1"
  if command -v jq &>/dev/null; then
    jq -r '.median.point_estimate' "$estimates_json"
  else
    python3 -c "import json,sys; d=json.load(open('$estimates_json')); print(d['median']['point_estimate'])"
  fi
}

# ns_to_human ns_value  → e.g. "18.5 ns", "72.6 ns", "17.5 µs", "3.3 ms"
ns_to_human() {
  python3 - "$1" <<'PYEOF'
import sys
ns = float(sys.argv[1])
if ns < 1000:
    print(f"{ns:.1f} ns")
elif ns < 1_000_000:
    print(f"{ns/1000:.2f} µs")
else:
    print(f"{ns/1_000_000:.2f} ms")
PYEOF
}

# ─── Run Rust benches ─────────────────────────────────────────────────────────

echo "Running cargo bench --workspace ..." >&2
(cd "$REPO_ROOT" && cargo bench --workspace) 2>&1 | grep -E '^(Benchmarking|error)' >&2 || true
echo "Rust benches done." >&2

CRITERION_DIR="$REPO_ROOT/target/criterion"

# Map: bench_id -> criterion_dir_path
# Criterion stores group/function under <group>/<function> in the criterion dir;
# bench_function (flat) stores as the name with '/' replaced by '_'.
get_criterion_path() {
  local bench_id="$1"
  # First try direct path (group-style: encode/register_small → encode/register_small)
  local direct="${CRITERION_DIR}/${bench_id}/base/estimates.json"
  if [[ -f "$direct" ]]; then
    echo "$direct"
    return
  fi
  # Fallback: flat name (agg_op/count → agg_op_count)
  local flat="${CRITERION_DIR}/$(echo "$bench_id" | tr '/' '_')/base/estimates.json"
  if [[ -f "$flat" ]]; then
    echo "$flat"
    return
  fi
  echo ""
}

# All 27 Rust bench IDs (plans 02-04)
RUST_BENCHES=(
  "encode/register_small"        "2.5"
  "encode/register_medium"       "2.5"
  "encode/register_near_limit"   "2.5"
  "decode/register_small"        "2.5"
  "decode/register_medium"       "2.5"
  "decode/register_near_limit"   "2.5"
  "parse/small"                  "4"
  "parse/medium"                 "4"
  "parse/deep"                   "4"
  "eval/arith"                   "4"
  "eval/compare"                 "4"
  "eval/boolean"                 "4"
  "eval/nullcheck"               "4"
  "eval/cast"                    "4"
  "op_chain/compile_4op"         "4"
  "op_chain/apply_4op"           "4"
  "agg_op/count"                 "5"
  "agg_op/sum"                   "5"
  "agg_op/avg"                   "5"
  "agg_op/min"                   "5"
  "agg_op/max"                   "5"
  "agg_op/variance"              "5"
  "agg_op/stddev"                "5"
  "agg_op/ratio"                 "5"
  "windowed/fold_count_5m_1Mevt" "5"
  "windowed/fold_sum_5m_1Mevt"   "5"
  "apply/3agg_100ent_1Kevt"      "5"
)

# ─── Run Python bench ─────────────────────────────────────────────────────────

echo "Running Python bench ..." >&2
(cd "$REPO_ROOT" && python -m pytest python/tests/bench_register_compile.py \
  --benchmark-only \
  --benchmark-json="$PYBENCH_JSON" \
  -q 2>&1) >&2 || true
echo "Python bench done." >&2

# ─── Emit markdown block ──────────────────────────────────────────────────────

echo ""
echo "## hw-class: ${HW_CLASS}"
echo ""
echo "Captured: ${CAPTURE_DATE}"
echo ""
echo "| Bench | Median | Captured | Phase | Notes |"
echo "|---|---|---|---|---|"

# Rust rows
for (( i=0; i<${#RUST_BENCHES[@]}; i+=2 )); do
  bench_id="${RUST_BENCHES[$i]}"
  phase="${RUST_BENCHES[$((i+1))]}"

  path="$(get_criterion_path "$bench_id")"
  if [[ -z "$path" ]]; then
    echo "| ${bench_id} | #skipped | ${CAPTURE_DATE} | ${phase} | estimates.json not found |"
    continue
  fi

  raw_ns="$(extract_criterion_median "$path" 2>/dev/null || echo '')"
  if [[ -z "$raw_ns" ]]; then
    echo "| ${bench_id} | #skipped | ${CAPTURE_DATE} | ${phase} | could not parse median |"
    continue
  fi

  human="$(ns_to_human "$raw_ns")"
  echo "| ${bench_id} | ${human} | ${CAPTURE_DATE} | ${phase} | |"
done

# Python row
if [[ -f "$PYBENCH_JSON" ]]; then
  py_median_s="$(python3 -c "
import json, sys
d = json.load(open('$PYBENCH_JSON'))
b = d['benchmarks'][0]
print(b['stats']['median'])
" 2>/dev/null || echo '')"

  if [[ -n "$py_median_s" ]]; then
    # pytest-benchmark median is in seconds; convert to ns then human
    py_ns="$(python3 -c "print(float('$py_median_s') * 1e9)")"
    py_human="$(ns_to_human "$py_ns")"
    echo "| test_register_compile_10_descriptors | ${py_human} | ${CAPTURE_DATE} | 3 | pytest-benchmark median |"
  else
    echo "| test_register_compile_10_descriptors | #skipped | ${CAPTURE_DATE} | 3 | could not parse pybench JSON |"
  fi
else
  echo "| test_register_compile_10_descriptors | #skipped | ${CAPTURE_DATE} | 3 | pybench JSON not found |"
fi

echo ""
echo "> Regression thresholds: +10% = WARNING (flag in VERIFICATION.md); +25% = BLOCKER. Compare within same hw-class only."
echo ""
