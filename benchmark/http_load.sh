#!/usr/bin/env bash
# benchmark/http_load.sh — HTTP ingest load test via oha
#
# Measures sustained EPS on POST /push-batch/{stream} with 1000-event batches.
# Target (reference box): EPS >= 100,000.
#
# Two modes:
#   LOAD_TEST_REFERENCE_BOX_REQUIRED=1  — full 30s run; exits non-zero if EPS < 100000.
#   (default, unset)                    — smoke mode: 100 events, 5s run, no EPS gate.
#
# Prerequisites:
#   cargo install oha        (https://github.com/hatoo/oha)
#   python3 (stdlib only)
#   jq
#
# Usage:
#   bash benchmark/http_load.sh
#   LOAD_TEST_REFERENCE_BOX_REQUIRED=1 DURATION=30s bash benchmark/http_load.sh
#   STREAM=my_stream PORT=7001 bash benchmark/http_load.sh
set -euo pipefail

PORT="${PORT:-6401}"
TOKEN="${BEAVA_ADMIN_TOKEN:-test-admin}"
STREAM="${STREAM:-bench_stream}"
CONCURRENCY="${CONCURRENCY:-64}"
REFERENCE_BOX="${LOAD_TEST_REFERENCE_BOX_REQUIRED:-0}"
PAYLOAD=/tmp/beava-bench-batch.json
RESULT=/tmp/beava-bench-oha.json
EPS_TARGET=100000

# In smoke mode use smaller parameters; reference box uses full settings.
if [ "${REFERENCE_BOX}" = "1" ]; then
    DURATION="${DURATION:-30s}"
    EVENTS_PER_BATCH=1000
    echo "== Mode: REFERENCE BOX (EPS gate active: >= ${EPS_TARGET}) =="
else
    DURATION="${DURATION:-5s}"
    EVENTS_PER_BATCH=100
    echo "== Mode: SMOKE (no EPS gate; set LOAD_TEST_REFERENCE_BOX_REQUIRED=1 for full run) =="
fi

# ── Dependency checks ──────────────────────────────────────────────────────────
command -v oha >/dev/null 2>&1 || {
    echo "ERROR: oha not found. Install: cargo install oha" >&2
    exit 1
}
command -v jq >/dev/null 2>&1 || {
    echo "ERROR: jq not found. Install via your package manager." >&2
    exit 1
}
command -v python3 >/dev/null 2>&1 || {
    echo "ERROR: python3 not found." >&2
    exit 1
}

# ── Generate batch payload ─────────────────────────────────────────────────────
echo "== Generating ${EVENTS_PER_BATCH}-event batch payload =="
python3 - <<PYEOF > "${PAYLOAD}"
import json, sys, time
n = ${EVENTS_PER_BATCH}
now_ms = int(time.time() * 1000)
events = [
    {"user": f"u{i % 1000}", "_event_time": now_ms - (n - i) * 10, "amount": float(i)}
    for i in range(n)
]
sys.stdout.write(json.dumps(events))
PYEOF
echo "   payload written to ${PAYLOAD} ($(wc -c < "${PAYLOAD}") bytes)"

# ── Server health check ────────────────────────────────────────────────────────
echo "== Checking server on :${PORT} =="
curl -sf "http://127.0.0.1:${PORT}/health" >/dev/null || {
    echo "ERROR: Beava server not running on port ${PORT}." >&2
    echo "  Start with: ./target/release/beava serve" >&2
    exit 1
}

# ── Register bench stream if not already registered ───────────────────────────
echo "== Registering bench stream '${STREAM}' (idempotent) =="
curl -sfS -X POST "http://127.0.0.1:${PORT}/pipelines" \
    -H 'Content-Type: application/json' \
    -H "Authorization: Bearer ${TOKEN}" \
    -d "{
      \"name\": \"${STREAM}\",
      \"key_field\": \"user\",
      \"definition_type\": \"stream\",
      \"features\": [
        {\"name\": \"count_1h\", \"type\": \"count\",  \"window\": \"1h\", \"bucket\": \"1m\"},
        {\"name\": \"sum_1h\",   \"type\": \"sum\", \"field\": \"amount\", \"window\": \"1h\", \"bucket\": \"1m\"}
      ]
    }" >/dev/null
echo "   done"

# ── Run oha ───────────────────────────────────────────────────────────────────
echo "== Running oha: concurrency=${CONCURRENCY}, duration=${DURATION} =="
echo "   endpoint: POST http://127.0.0.1:${PORT}/push-batch/${STREAM}"
oha -z "${DURATION}" \
    -c "${CONCURRENCY}" \
    -m POST \
    -H "Content-Type: application/json" \
    -H "Authorization: Bearer ${TOKEN}" \
    -D "${PAYLOAD}" \
    --no-tui -j \
    "http://127.0.0.1:${PORT}/push-batch/${STREAM}" \
    > "${RESULT}"

# ── Parse results ──────────────────────────────────────────────────────────────
RPS=$(jq -r '.summary.requestsPerSec' "${RESULT}")
EPS=$(python3 -c "print(int(float('${RPS}') * ${EVENTS_PER_BATCH}))")
SUCCESS_COUNT=$(jq -r '.summary.successRate // 1' "${RESULT}" || echo "1")
TOTAL_REQUESTS=$(jq -r '.summary.total // "?"' "${RESULT}" || echo "?")

echo ""
echo "======================================================================"
echo "  HTTP /push-batch Load Test Results"
echo "======================================================================"
echo "  Requests:     ${TOTAL_REQUESTS} total"
echo "  RPS:          ${RPS} req/s"
echo "  Events/batch: ${EVENTS_PER_BATCH}"
echo "  EPS:          ${EPS} events/s"
echo "  Success rate: ${SUCCESS_COUNT}"
echo "======================================================================"

# ── EPS gate (reference box only) ─────────────────────────────────────────────
if [ "${REFERENCE_BOX}" = "1" ]; then
    if [ "${EPS}" -lt "${EPS_TARGET}" ]; then
        echo "HTTP-09 FAIL: EPS ${EPS} < ${EPS_TARGET} target" >&2
        echo ""
        echo "Profiling hints:" >&2
        echo "  1. Confirm release build: cargo build --release" >&2
        echo "  2. Serde overhead: http_ingest.rs already uses Bytes extractor." >&2
        echo "  3. Try reducing CONCURRENCY (default 64) if connection overhead dominates." >&2
        echo "  4. Check server CPU saturation with: top -pid \$(pgrep beava)" >&2
        exit 1
    fi
    echo "HTTP-09 PASS: ${EPS} EPS >= ${EPS_TARGET} target"

    # Append result row to benchmark/README.md
    MACHINE_SPEC="$(uname -s) $(uname -m) cpu=$(nproc 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo '?') mem=$(grep MemTotal /proc/meminfo 2>/dev/null | awk '{print $2 " " $3}' || sysctl -n hw.memsize 2>/dev/null | python3 -c 'import sys; v=int(sys.stdin.read()); print(f"{v//1073741824} GB")' || echo '?')"
    RUN_DATE=$(date -u +%Y-%m-%d)
    SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
    README="${SCRIPT_DIR}/README.md"

    cat >> "${README}" <<ROW

## Phase 45 HTTP /push-batch load (${RUN_DATE})

| Metric      | Value |
|-------------|-------|
| EPS         | ${EPS} |
| RPS         | ${RPS} req/s |
| Events/req  | ${EVENTS_PER_BATCH} |
| Concurrency | ${CONCURRENCY} |
| Duration    | ${DURATION} |
| Machine     | ${MACHINE_SPEC} |
| Tool        | oha (hatoo/oha) |
| HTTP-09     | PASS: EPS ${EPS} >= ${EPS_TARGET} |

ROW
    echo "   Result appended to ${README}"
else
    echo "(Smoke mode: EPS ${EPS} recorded but not gated against ${EPS_TARGET} target)"
    echo "(Re-run with LOAD_TEST_REFERENCE_BOX_REQUIRED=1 on reference box to record official number)"
fi
