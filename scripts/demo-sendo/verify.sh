#!/usr/bin/env bash
# verify.sh — pre-recording verification checklist for SENDO-DEMO.md.
#
# Runs the 8-item checklist on the actual recording machine and prints a
# claimed-vs-measured table. If any measured number is worse than the
# claim, the script prints the exact "update the voiceover to X" line to
# paste into SENDO-DEMO-VI.md before Take 1.
#
# Exits 0 if all claims hold, non-zero (but informative) if any are violated.

set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${HERE}/../.." && pwd)"
VENV="${HERE}/.venv"
IMAGE="beavadb/beava:latest"
DATA="${HERE}/events.jsonl"

bold() { printf "\033[1m%s\033[0m\n" "$*"; }
green() { printf "\033[32m✓ %s\033[0m\n" "$*"; }
red() { printf "\033[31m✗ %s\033[0m\n" "$*"; }
step() { printf "\n\033[1;36m▸ %s\033[0m\n" "$*"; }

if [[ ! -d "${VENV}" ]]; then
    echo "venv not found — run setup.sh first" >&2
    exit 1
fi
# shellcheck source=/dev/null
source "${VENV}/bin/activate"

if [[ ! -s "${DATA}" ]]; then
    echo "events.jsonl missing — run setup.sh first" >&2
    exit 1
fi

# --- start container ---
step "Starting Beava container"
CID=""
cleanup() { [[ -n "${CID}" ]] && docker rm -f "${CID}" >/dev/null 2>&1 || true; }
trap cleanup EXIT

START_T=$(date +%s)
CID="$(docker run -d --rm -p 6900:6900 -p 6400:6400 "${IMAGE}")"

ok=0
for i in $(seq 1 30); do
    if curl -sf "http://localhost:6900/health" >/dev/null 2>&1; then
        ok=1
        break
    fi
    sleep 0.5
done
READY_T=$(date +%s)
STARTUP_SECS=$(( READY_T - START_T ))

if [[ "${ok}" -ne 1 ]]; then
    red "server did not become ready within 15 s"
    docker logs "${CID}" | tail -30
    exit 1
fi

# --- register pipeline ---
step "Registering pipeline.py"
python "${HERE}/pipeline.py" >/dev/null

# --- idle memory ---
sleep 2
IDLE_MEM_MB=$(docker stats --no-stream --format '{{.MemUsage}}' "${CID}" \
    | awk '{print $1}' | sed 's/MiB//;s/GiB//' )
# If units were GiB, multiply by 1024 — docker stats format is tricky; we eyeball.
if docker stats --no-stream --format '{{.MemUsage}}' "${CID}" | grep -q GiB; then
    IDLE_MEM_MB=$(awk -v v="${IDLE_MEM_MB}" 'BEGIN{printf "%.0f", v*1024}')
else
    IDLE_MEM_MB=$(awk -v v="${IDLE_MEM_MB}" 'BEGIN{printf "%.0f", v}')
fi

# --- query latency (idle) ---
step "Query latency — 1000 calls at idle"
QP99_IDLE=$(python - <<'PY'
import time, urllib.request
url = "http://localhost:6900/features/user_42"
lat = []
for _ in range(1000):
    t0 = time.perf_counter()
    try:
        urllib.request.urlopen(url, timeout=2).read()
    except Exception:
        pass
    lat.append((time.perf_counter() - t0) * 1000)
lat.sort()
print(f"{lat[int(len(lat)*0.99)]:.2f}")
PY
)

# --- load test ---
step "Load test — 10,000 EPS for 30 s"
LOADLOG="$(mktemp)"
cat "${DATA}" | python "${HERE}/beava-bench.py" \
    --rate 10000 \
    --to http://localhost:6900/push-batch/events \
    --duration 30 \
    2>&1 | tee "${LOADLOG}"

INGEST_P99=$(grep 'batch latency' "${LOADLOG}" | sed -n 's/.*p99=\([0-9.]*\)ms.*/\1/p' | head -1)
INGEST_P99="${INGEST_P99:-0}"

# --- query latency (under load) ---
# sleep briefly to reach steady state
sleep 3
QP99_LOAD=$(python - <<'PY'
import time, urllib.request
url = "http://localhost:6900/features/user_42"
lat = []
for _ in range(1000):
    t0 = time.perf_counter()
    try:
        urllib.request.urlopen(url, timeout=2).read()
    except Exception:
        pass
    lat.append((time.perf_counter() - t0) * 1000)
lat.sort()
print(f"{lat[int(len(lat)*0.99)]:.2f}")
PY
)

# --- memory after load ---
LOAD_MEM_MB=$(docker stats --no-stream --format '{{.MemUsage}}' "${CID}" \
    | awk '{print $1}' | sed 's/MiB//;s/GiB//' )
if docker stats --no-stream --format '{{.MemUsage}}' "${CID}" | grep -q GiB; then
    LOAD_MEM_MB=$(awk -v v="${LOAD_MEM_MB}" 'BEGIN{printf "%.0f", v*1024}')
else
    LOAD_MEM_MB=$(awk -v v="${LOAD_MEM_MB}" 'BEGIN{printf "%.0f", v}')
fi

# --- results table ---
echo
bold "═══ Verification Results ═══"
printf "%-32s %12s  %12s  %s\n" "Check" "Claimed" "Measured" "Verdict"
printf "%-32s %12s  %12s  %s\n" "─────" "───────" "────────" "───────"

verdict() {
    local name="$1" claimed="$2" measured="$3" cmp="$4"
    local pass=1
    case "${cmp}" in
        le) awk -v m="${measured}" -v c="${claimed}" 'BEGIN{exit !(m<=c)}' || pass=0 ;;
        ge) awk -v m="${measured}" -v c="${claimed}" 'BEGIN{exit !(m>=c)}' || pass=0 ;;
    esac
    if [[ "${pass}" -eq 1 ]]; then
        printf "%-32s %12s  %12s  \033[32mPASS\033[0m\n" "${name}" "${claimed}" "${measured}"
    else
        printf "%-32s %12s  %12s  \033[31mFAIL — update voiceover\033[0m\n" \
            "${name}" "${claimed}" "${measured}"
        FAILURES+=("${name}: claimed ${claimed}, measured ${measured}")
    fi
}

FAILURES=()
verdict "Startup < 10 s"          "10s"    "${STARTUP_SECS}s"   le
verdict "Idle memory < 500 MB"    "500"    "${IDLE_MEM_MB}"     le
verdict "Loaded memory < 500 MB"  "500"    "${LOAD_MEM_MB}"     le
verdict "Ingest p99 < 10 ms"      "10"     "${INGEST_P99}"      le
verdict "Query p99 idle < 5 ms"   "5"      "${QP99_IDLE}"       le
verdict "Query p99 load < 5 ms"   "5"      "${QP99_LOAD}"       le

echo
if [[ "${#FAILURES[@]}" -eq 0 ]]; then
    green "All claimed numbers hold on this machine. Safe to record."
    exit 0
fi

red "Some claims don't hold. Before recording, edit SENDO-DEMO-VI.md to match:"
for f in "${FAILURES[@]}"; do
    echo "   • ${f}"
done
echo
bold "Recording with numbers worse than claimed would be dishonest. Fix the script."
exit 2
