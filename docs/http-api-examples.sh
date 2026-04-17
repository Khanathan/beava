#!/usr/bin/env bash
# docs/http-api-examples.sh
#
# HTTP-10 live-code validation harness.
#
# Extracts the first ```go, ```javascript, and ```bash fenced code blocks from
# docs/http-api.md, compiles the Go snippet, and executes all three against a
# running Beava server to prove the documentation examples are working code.
#
# Exits 0 iff all three language examples succeed against the server.
#
# Usage:
#   # Start a server first:
#   #   ./target/release/beava serve &
#   #   python3 examples/curl-ingest/sample-pipeline.py 6401
#   bash docs/http-api-examples.sh
#   PORT=7001 BEAVA_ADMIN_TOKEN=secret bash docs/http-api-examples.sh
#
# Prerequisites:
#   - go    (https://golang.org)
#   - node  (https://nodejs.org) — v18+ for top-level await in .mjs
#   - curl
#   - A Beava server on localhost:${PORT} with a registered stream
set -euo pipefail

PORT="${PORT:-6401}"
TOKEN="${BEAVA_ADMIN_TOKEN:-test-admin}"
DOCS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DOC="${DOCS_DIR}/http-api.md"

# ── Dependency checks ──────────────────────────────────────────────────────────
command -v go   >/dev/null 2>&1 || { echo "ERROR: go not found — install from https://golang.org" >&2; exit 1; }
command -v node >/dev/null 2>&1 || { echo "ERROR: node not found — install from https://nodejs.org" >&2; exit 1; }
command -v curl >/dev/null 2>&1 || { echo "ERROR: curl not found" >&2; exit 1; }
test -f "${DOC}" || { echo "ERROR: ${DOC} not found" >&2; exit 1; }

# ── Temp workspace ─────────────────────────────────────────────────────────────
TMP=$(mktemp -d)
trap 'rm -rf "${TMP}"' EXIT

echo "== Extracting first code block of each language from docs/http-api.md =="

# Extract FIRST ```go block only (stop awk after the closing fence)
awk '/^```go$/{flag=1;next} flag && /^```/{exit} flag' "${DOC}" > "${TMP}/push_example_raw.go"
test -s "${TMP}/push_example_raw.go" || { echo "ERROR: no \`\`\`go block found in ${DOC}" >&2; exit 1; }
# Substitute localhost:6401 → 127.0.0.1:${PORT} for portability on macOS (IPv6 localhost issues)
sed "s|localhost:6401|127.0.0.1:${PORT}|g" "${TMP}/push_example_raw.go" > "${TMP}/push_example.go"
echo "   go:         $(wc -l < "${TMP}/push_example.go") lines"

# Extract FIRST ```javascript block only
awk '/^```javascript$/{flag=1;next} flag && /^```/{exit} flag' "${DOC}" > "${TMP}/push_example_raw.mjs"
test -s "${TMP}/push_example_raw.mjs" || { echo "ERROR: no \`\`\`javascript block found in ${DOC}" >&2; exit 1; }
sed "s|localhost:6401|127.0.0.1:${PORT}|g" "${TMP}/push_example_raw.mjs" > "${TMP}/push_example.mjs"
echo "   javascript: $(wc -l < "${TMP}/push_example.mjs") lines"

# Extract FIRST ```bash block only (the Quickstart curl demo)
awk '/^```bash$/{flag=1;next} flag && /^```/{exit} flag' "${DOC}" > "${TMP}/curl_example_raw.sh"
test -s "${TMP}/curl_example_raw.sh" || { echo "ERROR: no \`\`\`bash block found in ${DOC}" >&2; exit 1; }
sed "s|localhost:6401|127.0.0.1:${PORT}|g" "${TMP}/curl_example_raw.sh" > "${TMP}/curl_example.sh"
echo "   bash/curl:  $(wc -l < "${TMP}/curl_example.sh") lines"

echo ""

# ── Ensure server is reachable ─────────────────────────────────────────────────
curl -sf "http://127.0.0.1:${PORT}/health" >/dev/null || {
    echo "ERROR: Beava server not running on port ${PORT}." >&2
    echo "  Start with: ./target/release/beava serve" >&2
    exit 1
}

# ── Register the example stream (lowercase 'transactions' matches doc examples) ──
echo "== Registering 'transactions' stream (matches doc examples) =="
curl -sfS -X POST "http://127.0.0.1:${PORT}/pipelines" \
    -H 'Content-Type: application/json' \
    -H "Authorization: Bearer ${TOKEN}" \
    -d '{
      "name": "transactions",
      "key_field": "user",
      "definition_type": "stream",
      "features": [
        {"name": "tx_count_1h", "type": "count", "window": "1h", "bucket": "1m"},
        {"name": "tx_sum_1h",   "type": "sum",   "field": "amount", "window": "1h", "bucket": "1m"}
      ]
    }' >/dev/null
echo "   done"
echo ""

# ── Go: compile ───────────────────────────────────────────────────────────────
echo "== Compiling Go example =="
(
    cd "${TMP}"
    go mod init example >/dev/null 2>&1
    go build -o push_example push_example.go
) || {
    echo "FAIL: Go compile failed" >&2
    echo "--- push_example.go ---" >&2
    cat "${TMP}/push_example.go" >&2
    exit 1
}
echo "   PASS (compiled to ${TMP}/push_example)"

# ── Go: run ───────────────────────────────────────────────────────────────────
echo "== Running Go example =="
BEAVA_ADMIN_TOKEN="${TOKEN}" \
    "${TMP}/push_example" 2>&1 | tee /tmp/http-api-examples-go.txt || {
    echo "FAIL: Go example exited non-zero" >&2
    exit 1
}
echo "   PASS"

# ── Node: run ─────────────────────────────────────────────────────────────────
echo "== Running Node (fetch) example =="
# Node 18+ handles top-level await in .mjs files natively.
BEAVA_ADMIN_TOKEN="${TOKEN}" \
    node "${TMP}/push_example.mjs" 2>&1 | tee /tmp/http-api-examples-node.txt || {
    echo "FAIL: Node example exited non-zero" >&2
    exit 1
}
echo "   PASS"

# ── curl: run (strip comment lines, substitute env vars) ──────────────────────
echo "== Running curl example =="
# Strip comment-only lines (lines starting with #) and blank lines.
# Substitute ${BEAVA_ADMIN_TOKEN} with the actual token value.
grep -v '^[[:space:]]*#\|^[[:space:]]*$' "${TMP}/curl_example.sh" \
    | sed "s|\\\${BEAVA_ADMIN_TOKEN}|${TOKEN}|g" \
    | bash > /tmp/http-api-examples-curl.txt 2>&1 || {
    echo "FAIL: curl example exited non-zero" >&2
    cat /tmp/http-api-examples-curl.txt >&2
    exit 1
}
echo "   output: $(cat /tmp/http-api-examples-curl.txt)"
echo "   PASS"

echo ""
echo "=========================================="
echo "  HTTP-10 PASS: Go + Node + curl examples"
echo "  from docs/http-api.md are live code."
echo "=========================================="
