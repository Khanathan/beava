#!/usr/bin/env bash
# Clone-and-run throughput benchmark.
#
# Spins up 8 independent python3 client processes that push events in
# parallel to a Rust Tally server configured with worker threads equal to
# the host CPU count. Runs the SIMPLE (2 features) and COMPLEX (40+
# features, HLL, stddev, multi-window) pipelines back-to-back against a
# fresh server, and prints throughput + a sample feature vector + memory.
#
# Usage:
#   ./benchmark/fraud-pipeline/run_bench.sh                # defaults
#   EVENTS=1000000 ./benchmark/fraud-pipeline/run_bench.sh # bigger run
set -euo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BIN="$REPO/target/release/tally"
TOKEN="${TALLY_ADMIN_TOKEN:-dev-admin-token}"
TCP_PORT="${TCP_PORT:-6400}"
HTTP_PORT="${HTTP_PORT:-6401}"
EVENTS="${EVENTS:-200000}"

# CPU detection — mac first, linux fallback.
if CPUS=$(sysctl -n hw.ncpu 2>/dev/null); then :
elif CPUS=$(nproc 2>/dev/null); then :
else CPUS=4; fi

# Fixed client fan-out; server gets all cores.
CLIENTS=8
THREADS="$CPUS"

cd "$REPO"

echo "==> Host CPUs: $CPUS  |  server threads: $THREADS  |  client procs: $CLIENTS  |  events: $EVENTS"

# Build if needed.
if [[ ! -x "$BIN" ]]; then
  echo "==> Building tally (release)..."
  cargo build --release --bin tally
fi

rm -rf "$REPO/events"

LOG="$(mktemp -t tally-bench.XXXXXX.log)"
echo "==> Starting server (log: $LOG)"
TALLY_ADMIN_TOKEN="$TOKEN" TALLY_WORKER_THREADS="$THREADS" \
  "$BIN" serve --http-port "$HTTP_PORT" --tcp-port "$TCP_PORT" \
  > "$LOG" 2>&1 &
SERVER_PID=$!

cleanup() {
  echo
  echo "==> Stopping server (pid $SERVER_PID)"
  kill "$SERVER_PID" 2>/dev/null || true
  wait "$SERVER_PID" 2>/dev/null || true
  rm -rf "$REPO/events"
}
trap cleanup EXIT INT TERM

echo -n "==> Waiting for /debug/ready"
for _ in $(seq 1 30); do
  if curl -sf "http://127.0.0.1:$HTTP_PORT/debug/ready" >/dev/null 2>&1; then
    echo " ready."; break
  fi
  echo -n "."; sleep 0.5
done

export PYTHONPATH="$REPO/python:$REPO/benchmark/fraud-pipeline"

# Per-client event count (integer division; total may round down slightly).
PER_CLIENT=$(( EVENTS / CLIENTS ))
TOTAL=$(( PER_CLIENT * CLIENTS ))

run_mode() {
  local MODE="$1"

  # Fresh server between modes so numbers aren't cumulative.
  kill "$SERVER_PID" 2>/dev/null || true
  wait "$SERVER_PID" 2>/dev/null || true
  rm -rf "$REPO/events"
  TALLY_ADMIN_TOKEN="$TOKEN" TALLY_WORKER_THREADS="$THREADS" \
    "$BIN" serve --http-port "$HTTP_PORT" --tcp-port "$TCP_PORT" \
    > "$LOG" 2>&1 &
  SERVER_PID=$!
  for _ in $(seq 1 30); do
    curl -sf "http://127.0.0.1:$HTTP_PORT/debug/ready" >/dev/null 2>&1 && break
    sleep 0.3
  done

  echo
  echo "=== $(echo "$MODE" | tr '[:lower:]' '[:upper:]') pipeline benchmark ==="
  echo "  Clients:    $CLIENTS independent OS processes"
  echo "  Per-client: $(printf "%'d" "$PER_CLIENT") events"
  echo "  Total:      $(printf "%'d" "$TOTAL") events"
  echo

  # Spawn N independent python3 processes in parallel. Each writes its
  # JSON result to a dedicated tmpfile. We wait for all, then parse.
  local TMPDIR
  TMPDIR=$(mktemp -d -t tally-bench.XXXXXX)
  local PIDS=()
  local T0 T1
  T0=$(python3 -c 'import time; print(time.monotonic())')
  for i in $(seq 0 $(( CLIENTS - 1 ))); do
    python3 "$REPO/benchmark/fraud-pipeline/bench_two.py" \
      --mode "$MODE" \
      --events "$PER_CLIENT" \
      --proc-id "$i" \
      --host "localhost:$TCP_PORT" \
      > "$TMPDIR/out-$i.json" 2>"$TMPDIR/err-$i.log" &
    PIDS+=($!)
  done
  for pid in "${PIDS[@]}"; do wait "$pid" || true; done
  T1=$(python3 -c 'import time; print(time.monotonic())')

  # Aggregate + print via python so formatting is consistent with bench_two.
  python3 - "$TMPDIR" "$T0" "$T1" "$TOTAL" "$HTTP_PORT" "$TOKEN" <<'PY'
import json, sys, urllib.request, time
tmp, t0, t1, total, http_port, token = sys.argv[1:]
t0, t1, total = float(t0), float(t1), int(total)
wall = t1 - t0

import glob
rows = []
for f in sorted(glob.glob(f"{tmp}/out-*.json")):
    try:
        rows.append(json.loads(open(f).read().splitlines()[-1]))
    except Exception:
        pass

rows.sort(key=lambda r: r["proc_id"])
for r in rows:
    eps = r["events"] / r["elapsed"]
    print(f"  [client-{r['proc_id']}] {r['events']:>9,} events in "
          f"{r['elapsed']:>6.2f}s  =>  {eps:>10,.0f} eps")

print()
print(f"  Wall time:  {wall:.2f}s")
print(f"  Aggregate:  {total / wall:,.0f} events/sec")
print(f"  Per event:  {wall / total * 1e6:.1f} µs")

time.sleep(0.3)
print()
print("  --- Sample features (key=user_000001) ---")
try:
    req = urllib.request.Request(
        f"http://127.0.0.1:{http_port}/debug/key/user_000001",
        headers={"Authorization": f"Bearer {token}"},
    )
    body = json.loads(urllib.request.urlopen(req, timeout=5).read())
    feats = body.get("computed_features") or {}
    for k in sorted(feats):
        print(f"    {k}: {feats[k]}")
except Exception as e:
    print(f"    (could not read features: {e})")

try:
    mem = json.loads(urllib.request.urlopen(
        f"http://127.0.0.1:{http_port}/debug/memory", timeout=5).read())
    b = mem.get("estimated_bytes", 0)
    ents = mem.get("entity_count", 0)
    per = b / ents if ents else 0
    print()
    print(f"  Memory:     {b / 1024 / 1024:.1f} MB across "
          f"{ents:,} entities  ({per:,.0f} B/entity)")
except Exception:
    pass
PY

  rm -rf "$TMPDIR"
}

run_mode simple
run_mode complex

echo
echo "==> Benchmark complete."
