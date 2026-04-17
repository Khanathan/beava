#!/usr/bin/env bash
# examples/curl-ingest/run.sh
#
# End-to-end smoke test for the six Phase 45 HTTP endpoints:
#   1. Register stream via HTTP /pipelines (sample-pipeline.py uses urllib)
#   2. POST /push/{stream}          — single event
#   3. POST /push-batch/{stream}    — 3-event batch (?sync=1)
#   4. POST /push/{stream}/ndjson   — 5-event NDJSON stream
#   5. GET  /features/{key}         — all tables
#   6. GET  /features/{key}?table=X — single-table filter
#   7. GET  /streams                — list all streams
#   8. GET  /streams/{name}         — stream detail
#
# Prerequisites:
#   - A Beava server running on localhost:${HTTP_PORT} (default 6401)
#   - BEAVA_ADMIN_TOKEN exported (or defaults to "test-admin" for loopback)
#   - python3 available on PATH (stdlib only — no extra packages needed)
#   - curl available on PATH
#
# Usage:
#   bash examples/curl-ingest/run.sh
#   HTTP_PORT=7001 BEAVA_ADMIN_TOKEN=secret bash examples/curl-ingest/run.sh
#
# Exit codes:
#   0  All 8 steps passed
#   1  Server unreachable, assertion failed, or prerequisite missing
set -euo pipefail

PORT="${PORT:-6401}"
HTTP_PORT="${HTTP_PORT:-${PORT}}"
TOKEN="${BEAVA_ADMIN_TOKEN:-test-admin}"
BASE="http://127.0.0.1:${HTTP_PORT}"
AUTH="Authorization: Bearer ${TOKEN}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# ------------------------------------------------------------------
# Helper: assert JSON field present in a response file
# Usage: assert_field FILE PATTERN
# ------------------------------------------------------------------
assert_field() {
    local file="$1"
    local pattern="$2"
    if ! grep -q "${pattern}" "${file}"; then
        echo "FAIL: expected '${pattern}' in:" >&2
        cat "${file}" >&2
        exit 1
    fi
}

echo "== 0. Wait for server on :${PORT} =="
for i in $(seq 1 30); do
    if curl -sf "${BASE}/health" >/dev/null 2>&1; then
        echo "   server ready"
        break
    fi
    if [ "${i}" -eq 30 ]; then
        echo "ERROR: server not ready after 30s on :${PORT}" >&2
        exit 1
    fi
    sleep 1
done

echo ""
echo "== 1. Register Transactions stream via HTTP /pipelines =="
python3 "${SCRIPT_DIR}/sample-pipeline.py" "${HTTP_PORT}" "${TOKEN}" \
    | tee /tmp/cis-register.json
echo ""
echo "   PASS"

echo ""
echo "== 2. POST /push/Transactions (single event) =="
curl -sfS -X POST "${BASE}/push/Transactions" \
    -H 'Content-Type: application/json' \
    -H "${AUTH}" \
    -d '{"user":"alice","amount":10.5,"_event_time":1700000000000}' \
    | tee /tmp/cis-push-single.json
echo ""
assert_field /tmp/cis-push-single.json '"ok":true'
echo "   PASS"

echo ""
echo "== 3. POST /push-batch/Transactions?sync=1 (3-event batch) =="
curl -sfS -X POST "${BASE}/push-batch/Transactions?sync=1" \
    -H 'Content-Type: application/json' \
    -H "${AUTH}" \
    -d '[{"user":"alice","amount":5},{"user":"bob","amount":20},{"user":"alice","amount":7.5}]' \
    | tee /tmp/cis-push-batch.json
echo ""
assert_field /tmp/cis-push-batch.json '"accepted":3'
assert_field /tmp/cis-push-batch.json '"rejected":0'
echo "   PASS"

echo ""
echo "== 4. POST /push/Transactions/ndjson (5-event NDJSON stream) =="
printf '%s\n' \
    '{"user":"alice","amount":1}' \
    '{"user":"alice","amount":2}' \
    '{"user":"carol","amount":100}' \
    '{"user":"bob","amount":3}' \
    '{"user":"alice","amount":4}' \
    | curl -sfS -X POST "${BASE}/push/Transactions/ndjson" \
        -H 'Content-Type: application/x-ndjson' \
        -H "${AUTH}" \
        --data-binary @- \
    | tee /tmp/cis-push-ndjson.json
echo ""
assert_field /tmp/cis-push-ndjson.json '"accepted":5'
assert_field /tmp/cis-push-ndjson.json '"rejected":0'
echo "   PASS"

echo ""
echo "== 5. GET /features/alice (all tables) =="
curl -sfS "${BASE}/features/alice" \
    -H "${AUTH}" \
    | tee /tmp/cis-features.json
echo ""
assert_field /tmp/cis-features.json '"ok":true'
assert_field /tmp/cis-features.json '"alice"'
echo "   PASS"

echo ""
echo "== 6. GET /features/alice?table=Transactions (single-table filter) =="
curl -sfS "${BASE}/features/alice?table=Transactions" \
    -H "${AUTH}" \
    | tee /tmp/cis-features-filtered.json
echo ""
assert_field /tmp/cis-features-filtered.json '"ok":true'
assert_field /tmp/cis-features-filtered.json '"alice"'
echo "   PASS"

echo ""
echo "== 7. GET /streams (list all) =="
curl -sfS "${BASE}/streams" \
    -H "${AUTH}" \
    | tee /tmp/cis-streams.json
echo ""
assert_field /tmp/cis-streams.json 'Transactions'
echo "   PASS"

echo ""
echo "== 8. GET /streams/Transactions (stream detail) =="
curl -sfS "${BASE}/streams/Transactions" \
    -H "${AUTH}" \
    | tee /tmp/cis-stream-detail.json
echo ""
assert_field /tmp/cis-stream-detail.json '"name":"Transactions"'
echo "   PASS"

echo ""
echo "============================================"
echo "  ALL GREEN (HTTP-08) — 8/8 steps passed"
echo "============================================"
