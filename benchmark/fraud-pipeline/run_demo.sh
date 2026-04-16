#!/usr/bin/env bash
# Clone-and-run fraud demo. Builds (if needed), starts a local Beava,
# runs fraud_demo.py, and tears down on exit.
#
# Usage:
#   ./benchmark/fraud-pipeline/run_demo.sh                # 60s demo
#   DURATION=30 ./benchmark/fraud-pipeline/run_demo.sh    # custom length
set -euo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BIN="$REPO/target/release/beava"
DATA_DIR="${DATA_DIR:-/tmp/beava-fraud-demo}"
TCP_PORT="${TCP_PORT:-6400}"
HTTP_PORT="${HTTP_PORT:-6401}"
TOKEN="${BEAVA_ADMIN_TOKEN:-dev-admin-token}"
DURATION="${DURATION:-60}"

cd "$REPO"

# 1. Build the server if the binary is missing.
if [[ ! -x "$BIN" ]]; then
  echo "==> Building beava (release)..."
  cargo build --release --bin beava
fi

# 2. Fresh data dir.
rm -rf "$DATA_DIR"

# 3. Start the server in the background. Trap ensures cleanup on any exit.
LOG="$(mktemp -t beava-demo.XXXXXX.log)"
echo "==> Starting beava server (log: $LOG)"
BEAVA_ADMIN_TOKEN="$TOKEN" "$BIN" serve \
  --http-port "$HTTP_PORT" \
  --tcp-port "$TCP_PORT" \
  --data-dir "$DATA_DIR" \
  > "$LOG" 2>&1 &
SERVER_PID=$!

cleanup() {
  echo
  echo "==> Stopping beava server (pid $SERVER_PID)"
  kill "$SERVER_PID" 2>/dev/null || true
  wait "$SERVER_PID" 2>/dev/null || true
}
trap cleanup EXIT INT TERM

# 4. Wait for /debug/ready.
echo -n "==> Waiting for server"
for i in $(seq 1 30); do
  if curl -sf "http://127.0.0.1:$HTTP_PORT/debug/ready" >/dev/null 2>&1; then
    echo " ready."
    break
  fi
  echo -n "."
  sleep 0.5
  if [[ "$i" == "30" ]]; then
    echo " TIMEOUT"
    echo "--- server log ---"
    cat "$LOG"
    exit 1
  fi
done

# 5. Run the demo. Python SDK is pure-Python — no pip install needed.
echo "==> Running fraud_demo.py (duration=${DURATION}s)"
PYTHONPATH="$REPO/python:$REPO/benchmark/fraud-pipeline" \
  python3 "$REPO/benchmark/fraud-pipeline/fraud_demo.py" \
  --host "localhost:$TCP_PORT" \
  --duration "$DURATION"

echo
echo "==> Demo complete."
echo "    HTTP debug:  http://127.0.0.1:$HTTP_PORT/debug/ready"
echo "    Try:         curl -H 'Authorization: Bearer $TOKEN' \\"
echo "                      http://127.0.0.1:$HTTP_PORT/debug/key/user_fraud_001"
