#!/usr/bin/env bash
# examples/session-features/run.sh
#
# End-to-end session-features example against a fresh Beava container.
#
# Prerequisites:
#   - Docker (image beavadb/beava:latest pulled or available)
#   - Python 3.10+ with pip
#
# Usage:
#   bash examples/session-features/run.sh
set -euo pipefail

cd "$(dirname "$0")"

HTTP_BASE="http://localhost:6900"
CONTAINER_NAME="beava-session"

# ---------------------------------------------------------------------------
# 1. Start Beava if not already running
# ---------------------------------------------------------------------------
echo "==> Checking Beava server at ${HTTP_BASE}/health"
if curl -fsS "${HTTP_BASE}/health" >/dev/null 2>&1; then
    echo "    server already running."
else
    echo "    starting beava container (docker run beavadb/beava:latest)..."
    docker run -d --rm \
        -p 6900:6900 \
        -p 6400:6400 \
        --name "${CONTAINER_NAME}" \
        beavadb/beava:latest
    echo -n "    waiting for server"
    for i in $(seq 1 30); do
        if curl -fsS "${HTTP_BASE}/health" >/dev/null 2>&1; then
            echo " ready."
            break
        fi
        echo -n "."
        sleep 1
        if [ "${i}" -eq 30 ]; then
            echo " TIMEOUT"
            echo "ERROR: server did not start within 30s" >&2
            docker logs "${CONTAINER_NAME}" >&2 || true
            exit 1
        fi
    done
fi

# ---------------------------------------------------------------------------
# 2. Install Python dependencies
# ---------------------------------------------------------------------------
echo ""
echo "==> Installing Python dependencies (requests)"
python3 -m pip install --quiet requests

# ---------------------------------------------------------------------------
# 3. Register the pipeline
# ---------------------------------------------------------------------------
echo ""
echo "==> Registering pipeline (Click stream + SessionFeatures table)"
PYTHONPATH="$(cd ../.. && pwd)/python:${PYTHONPATH:-}" python3 pipeline.py

# ---------------------------------------------------------------------------
# 4. Push synthetic click events
# ---------------------------------------------------------------------------
echo ""
echo "==> Pushing 1000 synthetic click events via HTTP /push-batch/Click"
HTTP_BASE="${HTTP_BASE}" python3 push.py

# ---------------------------------------------------------------------------
# 5. Read features
# ---------------------------------------------------------------------------
echo ""
echo "==> Features for session-001:"
curl -s "${HTTP_BASE}/features/session-001" | python3 -m json.tool || \
    curl -s "${HTTP_BASE}/features/session-001"

echo ""
echo "==> Done."
echo ""
echo "    Explore more:"
echo "      curl ${HTTP_BASE}/features/session-005"
echo "      curl ${HTTP_BASE}/streams"
echo ""
echo "    Next: see examples/fraud-scoring/ for a multi-stream, multi-table example."
echo ""
echo "    Cleanup: docker stop ${CONTAINER_NAME}"
