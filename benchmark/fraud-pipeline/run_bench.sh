#!/usr/bin/env bash
# Clone-and-run throughput benchmark. Duration-based, fixed wall-time.
#
# Usage:
#   ./run_bench.sh                          # defaults: MODE=complex, CPUS=host
#   MODE=simple ./run_bench.sh              # 1-table / 2-feature pipeline
#   MODE=complex ./run_bench.sh             # 5-table / ~40-feature pipeline
#   CPUS=8 ./run_bench.sh                   # override fan-out + server threads
#   WARMUP=10 MEASURE=30 ./run_bench.sh     # longer run if you need one
#
# Timing (defaults):
#   build (if needed)   ~10s one-time
#   server start        ~1s
#   warmup              ~5s
#   measure             ~15s     (EPS printed live every 2s)
#   ──────────────────────
#   total               ~21s     reports stable steady-state eps
set -euo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BIN="$REPO/target/release/tally"
BENCH="$REPO/benchmark/fraud-pipeline/bench.py"
TOKEN="${TALLY_ADMIN_TOKEN:-dev-admin-token}"
TCP_PORT="${TCP_PORT:-6400}"
HTTP_PORT="${HTTP_PORT:-6401}"
MODE="${MODE:-complex}"
WARMUP="${WARMUP:-5}"
MEASURE="${MEASURE:-15}"
# Entity-cardinality knobs. Raise these to reduce DashMap shard contention on
# hot Zipfian keys, or lower them to stress-test the hot-key path.
USERS="${USERS:-10000}"
MERCHANTS="${MERCHANTS:-2000}"
DEVICES="${DEVICES:-5000}"
IPS="${IPS:-8000}"
ZIPF="${ZIPF:-1.2}"

# Detect host CPUs.
if   _c=$(sysctl -n hw.ncpu 2>/dev/null); then CPUS_HOST=$_c
elif _c=$(nproc 2>/dev/null);              then CPUS_HOST=$_c
else CPUS_HOST=4; fi
CPUS="${CPUS:-$CPUS_HOST}"

cd "$REPO"

# Build if binary is missing.
if [[ ! -x "$BIN" ]]; then
  echo "==> Building tally (release)..."
  cargo build --release --bin tally
fi

# Clean slate on ports + data dir.
pkill -9 -f bench.py 2>/dev/null || true
pkill -9 -f "target/release/tally serve" 2>/dev/null || true
sleep 1
rm -rf "$REPO/events"

echo "==> MODE=$MODE  CPUS=$CPUS  warmup=${WARMUP}s  measure=${MEASURE}s"
echo "    users=$USERS  merchants=$MERCHANTS  devices=$DEVICES  ips=$IPS  zipf=$ZIPF"
SRV_LOG="$(mktemp -t tally-bench.XXXXXX.log)"
TALLY_ADMIN_TOKEN="$TOKEN" TALLY_WORKER_THREADS="$CPUS" \
  "$BIN" serve --http-port "$HTTP_PORT" --tcp-port "$TCP_PORT" \
  > "$SRV_LOG" 2>&1 &
SERVER_PID=$!

cleanup() {
  pkill -9 -f bench.py 2>/dev/null || true
  kill "$SERVER_PID" 2>/dev/null || true
  wait "$SERVER_PID" 2>/dev/null || true
}
trap cleanup EXIT INT TERM

for _ in $(seq 1 30); do
  curl -sf "http://127.0.0.1:$HTTP_PORT/debug/ready" >/dev/null 2>&1 && break
  sleep 0.3
done

export PYTHONPATH="$REPO/python:$REPO/benchmark/fraud-pipeline"
TMPDIR=$(mktemp -d -t tally-bench.XXXXXX)

CLIENT_PIDS=()
spawn_clients() {
  local dur="$1"; local tag="$2"
  CLIENT_PIDS=()
  for i in $(seq 0 $((CPUS - 1))); do
    python3 "$BENCH" --mode "$MODE" --duration "$dur" --proc-id "$i" \
      --host "localhost:$TCP_PORT" \
      --users "$USERS" --merchants "$MERCHANTS" \
      --devices "$DEVICES" --ips "$IPS" \
      --zipf-alpha "$ZIPF" \
      > "$TMPDIR/$tag-$i.jsonl" 2>&1 &
    CLIENT_PIDS+=($!)
  done
}
wait_clients() {
  # `wait` with no args waits for every backgrounded child (including the
  # tally server, which never exits). Pass the client pids explicitly so we
  # only block on them.
  for pid in "${CLIENT_PIDS[@]}"; do wait "$pid" 2>/dev/null || true; done
}

# ── Warmup ── clients run, output discarded. Pays cold-start entity init.
echo "==> Warming up (${WARMUP}s)..."
spawn_clients "$WARMUP" warmup
wait_clients
rm -f "$TMPDIR"/warmup-*.jsonl

# ── Measure ── clients stream checkpoint JSONL lines; shell aggregates.
echo "==> Measuring (${MEASURE}s) — live EPS:"
spawn_clients "$MEASURE" measure

# Poll checkpoint files every 2s and print live aggregate eps. Disable
# pipefail/errexit here — transient "no data yet" states are normal and the
# loop should keep trying, not abort the whole run.
set +e
t_start=$(python3 -c 'import time; print(time.monotonic())')
last_events=0
last_t=0
while :; do
  # Exit when all clients have emitted a "final" line.
  finals=$(grep -l '"phase": "final"' "$TMPDIR"/measure-*.jsonl 2>/dev/null | wc -l)
  if [[ "$finals" -ge "$CPUS" ]]; then break; fi

  sleep 2
  # Sum latest checkpoint's event count across clients. `read` returns
  # non-zero on empty input (no checkpoints yet) — tolerate under set -e.
  t_now=""; total=""
  # Guard with || true — read returns non-zero on empty input.
  { read -r t_now total || true; } <<<"$(python3 - "$TMPDIR" <<'PY'
import glob, json, sys
tmp = sys.argv[1]
total = 0; t = 0.0
for f in glob.glob(f"{tmp}/measure-*.jsonl"):
    ckpts = [l for l in open(f).read().splitlines() if '"phase"' in l]
    if not ckpts: continue
    last = json.loads(ckpts[-1])
    total += last["events"]; t = max(t, last["t"])
print(f"{t:.2f} {total}")
PY
)"
  if [[ -n "$total" && "$total" != "0" ]]; then
    dt=$(python3 -c "print(max(0.01, $t_now - $last_t))")
    inst=$(python3 -c "print(int(($total - $last_events) / $dt))")
    avg=$(python3 -c "print(int($total / max(0.01, $t_now)))")
    printf "   t=%5.1fs  total=%12d events  instant=%10s eps  avg=%10s eps\n" \
      "$t_now" "$total" "$(printf "%'d" "$inst")" "$(printf "%'d" "$avg")"
    last_events=$total; last_t=$t_now
  fi
done
wait_clients

# Final steady-state eps from the "final" lines — this is the authoritative number.
python3 - "$TMPDIR" "$MODE" "$CPUS" "$HTTP_PORT" "$TOKEN" <<'PY'
import glob, json, sys, urllib.request
tmp, mode, cpus, http, token = sys.argv[1:]
finals = []
for f in sorted(glob.glob(f"{tmp}/measure-*.jsonl")):
    for line in reversed(open(f).read().splitlines()):
        if '"phase": "final"' in line:
            finals.append(json.loads(line)); break

total = sum(r["events"] for r in finals)
wall  = max(r["t"] for r in finals)
print()
print(f"==> STEADY-STATE ({mode}, {cpus} clients):")
print(f"    Events:     {total:,}")
print(f"    Wall:       {wall:.2f}s")
print(f"    Aggregate:  {int(total / wall):,} events/sec")
print(f"    Per event:  {wall / total * 1e6:.1f} µs")

try:
    req = urllib.request.Request(
        f"http://127.0.0.1:{http}/debug/key/user_000001",
        headers={"Authorization": f"Bearer {token}"},
    )
    body = json.loads(urllib.request.urlopen(req, timeout=5).read())
    feats = body.get("computed_features") or {}
    print()
    print("    Sample features (key=user_000001):")
    for k in sorted(feats):
        print(f"      {k}: {feats[k]}")
except Exception:
    pass

try:
    mem = json.loads(urllib.request.urlopen(f"http://127.0.0.1:{http}/debug/memory").read())
    b = mem.get("estimated_bytes", 0); ents = mem.get("entity_count", 0)
    per = b / ents if ents else 0
    print()
    print(f"    Memory:     {b / 1024 / 1024:.1f} MB across {ents:,} entities  ({per:,.0f} B/entity)")

    # Per-stream and per-operator-type breakdown. Sorted hottest-first so the
    # eye lands on where the bytes actually are.
    streams = sorted(
        (s for s in mem.get("per_stream", []) if s.get("estimated_bytes", 0) > 0),
        key=lambda s: s["estimated_bytes"], reverse=True,
    )
    for s in streams:
        sb = s["estimated_bytes"]; sk = s.get("key_count", 0)
        print(f"\n    [{s['name']}]  {sb / 1024 / 1024:6.1f} MB  "
              f"{sk:,} keys  ({s.get('per_entity_avg_bytes', 0):,} B/key)")
        ops = sorted(s.get("operator_breakdown", []),
                     key=lambda o: o["total_bytes"], reverse=True)
        for op in ops:
            share = 100 * op["total_bytes"] / sb if sb else 0
            print(f"        {op['type']:<18} {op['total_bytes'] / 1024 / 1024:6.1f} MB  "
                  f"({share:5.1f}%)  n={op['count']:,}")

        # Top 5 individual features by total_bytes — useful when one window
        # size or one HLL is dominating the stream.
        feats = sorted(s.get("features", []),
                       key=lambda f: f["total_bytes"], reverse=True)[:5]
        if feats:
            print(f"        Top features:")
            for f in feats:
                avg = f.get("avg_bytes_per_key", 0)
                buckets = f" buckets={f['num_buckets']}" if f.get("num_buckets") else ""
                print(f"          {f['name']:<28} {f['total_bytes'] / 1024 / 1024:5.1f} MB"
                      f"  ({avg:,} B/key{buckets})  [{f['operator_type']}]")
except Exception as e:
    print(f"    (memory breakdown unavailable: {e})")
PY

rm -rf "$TMPDIR"
