#!/usr/bin/env bash
# Start beava server + demo proxy for the operator playground.
#
# Usage: ./start.sh  (from anywhere; uses absolute paths)
# Stops both processes on Ctrl+C.

set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(cd "$HERE/../.." && pwd)"

WAL_DIR="$(mktemp -d -t beava-demo-wal-XXXXXX)"
SNAPSHOT_DIR="$(mktemp -d -t beava-demo-snap-XXXXXX)"
CFG="$HERE/.beava.demo.yaml"
LOG_DIR="$HERE/.logs"
mkdir -p "$LOG_DIR"

cleanup() {
  echo
  echo "[demo] stopping…"
  if [[ -n "${BEAVA_PID:-}" ]]; then kill "$BEAVA_PID" 2>/dev/null || true; fi
  if [[ -n "${PROXY_PID:-}" ]]; then kill "$PROXY_PID" 2>/dev/null || true; fi
  rm -rf "$WAL_DIR" "$SNAPSHOT_DIR" 2>/dev/null || true
  echo "[demo] cleaned up WAL+snapshot dirs."
}
trap cleanup EXIT INT TERM

cat > "$CFG" <<EOF
listen_addr: "127.0.0.1:8080"
log_level: info
EOF

echo "[demo] launching beava server (port 8080)…"
echo "[demo]   WAL dir:      $WAL_DIR"
echo "[demo]   snapshot dir: $SNAPSHOT_DIR"
BEAVA_DEV_ENDPOINTS=1 \
BEAVA_WAL_DIR="$WAL_DIR" \
BEAVA_SNAPSHOT_DIR="$SNAPSHOT_DIR" \
BEAVA_WAL_FSYNC_INTERVAL_MS=5 \
"$REPO/target/release/beava" --config "$CFG" \
  > "$LOG_DIR/beava.log" 2>&1 &
BEAVA_PID=$!
echo "[demo]   beava pid: $BEAVA_PID  (log: $LOG_DIR/beava.log)"

# Wait for /ready — must be OUR beava (verify pid still alive each iter)
echo "[demo] waiting for /ready…"
for i in $(seq 1 40); do
  if ! kill -0 "$BEAVA_PID" 2>/dev/null; then
    echo "[demo] ✗ beava (pid $BEAVA_PID) died — see $LOG_DIR/beava.log"
    tail -20 "$LOG_DIR/beava.log"
    exit 1
  fi
  if curl -sf http://127.0.0.1:8080/ready >/dev/null 2>&1; then
    echo "[demo] beava ready after ${i}×100ms"
    break
  fi
  sleep 0.1
  if [[ $i -eq 40 ]]; then
    echo "[demo] ✗ beava did not become ready in 4s — see $LOG_DIR/beava.log"
    tail -20 "$LOG_DIR/beava.log"
    exit 1
  fi
done

# Pre-register the demo pipeline
echo "[demo] registering demo pipeline (count/sum/avg/min/max/variance/stddev/ratio per user)…"
RESP=$(curl -sS -X POST http://127.0.0.1:8080/register \
  -H "Content-Type: application/json" \
  -d @"$HERE/register.json")
echo "[demo]   register response: $RESP"

echo "[demo] launching proxy (port 9001)…"
python3 "$HERE/proxy.py" --port 9001 --backend http://127.0.0.1:8080 \
  > "$LOG_DIR/proxy.log" 2>&1 &
PROXY_PID=$!
echo "[demo]   proxy pid: $PROXY_PID  (log: $LOG_DIR/proxy.log)"

sleep 0.3
echo
echo "================================================================"
echo "  ✓ Beava operator playground is live"
echo "  ✓ Open: http://127.0.0.1:9001/"
echo "  ✓ Wire log streams in the right panel"
echo "  ✓ Press Ctrl+C here to stop both processes."
echo "================================================================"

# Keep the script alive
wait "$BEAVA_PID" "$PROXY_PID"
