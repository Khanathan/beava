#!/usr/bin/env bash
# Phase 46 D-03: compare a fresh summary.json to committed baseline.
# Reads aggregate_eps from both files and exits 1 if fresh < base * 0.95
# (i.e., more than -5% regression).
#
# Usage:
#   compare_baseline.sh <path-to-fresh-summary.json>
#
# aggregate_eps may appear at the top level OR nested under "throughput":
#   { "aggregate_eps": 314931, ... }           <- old flat shape
#   { "throughput": { "aggregate_eps": 314931 } }  <- new nested shape
# Both are handled by the extractor below.

set -euo pipefail

# Resolve FRESH before changing directory so callers can pass a relative path.
FRESH="${1:?usage: compare_baseline.sh <summary.json>}"
# Convert to absolute if it isn't already.
[[ "$FRESH" = /* ]] || FRESH="$(pwd)/${FRESH}"

cd "$(dirname "$0")"
BASE="results/baseline/summary.json"

[ -f "$FRESH" ] || { echo "ERROR: missing $FRESH"; exit 1; }
[ -f "$BASE"  ] || { echo "ERROR: missing $BASE";  exit 1; }

# Pure python extraction — no jq dep required.
# Handles both flat and throughput-nested aggregate_eps.
extract_eps() {
    python3 - "$1" <<'PY'
import json, sys
data = json.load(open(sys.argv[1]))
if "aggregate_eps" in data:
    print(data["aggregate_eps"])
elif "throughput" in data and "aggregate_eps" in data["throughput"]:
    print(data["throughput"]["aggregate_eps"])
else:
    print("ERROR: aggregate_eps not found in " + sys.argv[1], file=sys.stderr)
    sys.exit(1)
PY
}

FRESH_EPS="$(extract_eps "$FRESH")"
BASE_EPS="$(extract_eps "$BASE")"

# Regression gate: fresh < base * 0.95 => exit 1
python3 - "$FRESH_EPS" "$BASE_EPS" <<'PY'
import sys
fresh, base = float(sys.argv[1]), float(sys.argv[2])
delta_pct = (fresh - base) / base * 100.0
thresh_pct = -5.0
ok = delta_pct >= thresh_pct
print(f"fresh={fresh:.0f}  base={base:.0f}  delta={delta_pct:+.2f}%  threshold={thresh_pct}%  ok={ok}")
sys.exit(0 if ok else 1)
PY
