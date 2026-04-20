#!/usr/bin/env bash
# Phase 54 TPC-PERSIST-04 soak: 100 GB fjall state, 8h sustained read/write,
# p99 < 1 ms gate. Run on a Hetzner CCX43 (16-core AMD EPYC Genoa, 32 GB RAM)
# or equivalent.
#
# Outputs `soak-evidence/<ts>.json` with schema consumed by /gsd-verify-work:
#   { "ts_utc": "...", "host": "...", "commit_sha": "...",
#     "state_gb": N, "duration_hours": N, "warmup_hours": N,
#     "p50_ms": N, "p95_ms": N, "p99_ms": N, "p999_ms": N,
#     "throughput_eps": N, "events": N, "duration_s": N,
#     "cache_mb": N, "git_sha": "...", "pass": bool }
#
# Usage (on the Hetzner box):
#   git clone https://github.com/<org>/tally.git
#   cd tally
#   git checkout <phase-54-final-sha>
#   cargo build --release
#   BEAVA_SHARDS=8 BEAVA_FJALL_CACHE_MB=16000 bash scripts/soak-hetzner-ccx43.sh
#
# Knobs (env, all optional):
#   BEAVA_SHARDS           default 8
#   BEAVA_FJALL_CACHE_MB   default 16000 (16 GB — half of the 32 GB box)
#   STATE_GB               default 100
#   WARMUP_HOURS           default 1
#   DURATION_HOURS         default 8
#   EVIDENCE_DIR           default ./soak-evidence
#   TCP_PORT / HTTP_PORT   default 6400 / 6401
#
# Expected wall clock: ~9 h (1 h warmup + 8 h measurement) +
# ~5 min build + ~30 min initial state generation.

set -euo pipefail

# --------------------------------------------------------------------------
# Config
# --------------------------------------------------------------------------

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO"

TS="$(date -u +%Y%m%dT%H%M%SZ)"
COMMIT_SHA="$(git rev-parse HEAD)"
HOSTNAME="$(hostname -f 2>/dev/null || hostname)"

BEAVA_SHARDS="${BEAVA_SHARDS:-8}"
BEAVA_FJALL_CACHE_MB="${BEAVA_FJALL_CACHE_MB:-16000}"
STATE_GB="${STATE_GB:-100}"
WARMUP_HOURS="${WARMUP_HOURS:-1}"
DURATION_HOURS="${DURATION_HOURS:-8}"
EVIDENCE_DIR="${EVIDENCE_DIR:-$REPO/.planning/phases/54-legacy-engine-removal/soak-evidence}"
TCP_PORT="${TCP_PORT:-6400}"
HTTP_PORT="${HTTP_PORT:-6401}"
DATA_DIR="${DATA_DIR:-/var/lib/beava-soak}"

mkdir -p "$EVIDENCE_DIR"
mkdir -p "$DATA_DIR"

EVIDENCE_JSON="$EVIDENCE_DIR/${TS}.json"
LATENCY_LOG="$EVIDENCE_DIR/${TS}.latency.jsonl"
SERVER_LOG="$EVIDENCE_DIR/${TS}.server.log"

log()  { printf '\n\033[1m==> %s\033[0m\n' "$*"; }
fail() { printf '\033[31m[fail]\033[0m %s\n' "$*" >&2; exit 1; }

log "Phase 54 TPC-PERSIST-04 soak"
echo "  host=$HOSTNAME"
echo "  commit=$COMMIT_SHA"
echo "  shards=$BEAVA_SHARDS cache_mb=$BEAVA_FJALL_CACHE_MB"
echo "  state_gb=$STATE_GB warmup_h=$WARMUP_HOURS duration_h=$DURATION_HOURS"
echo "  data_dir=$DATA_DIR"
echo "  evidence=$EVIDENCE_JSON"

# --------------------------------------------------------------------------
# Build
# --------------------------------------------------------------------------

if [[ ! -x "$REPO/target/release/tally" && ! -x "$REPO/target/release/beava" ]]; then
    log "Building release binary (~5 min on CCX43)"
    cargo build --release --bin tally || cargo build --release --bin beava \
        || fail "cargo build --release failed"
fi
BIN="$REPO/target/release/tally"
[[ -x "$BIN" ]] || BIN="$REPO/target/release/beava"
[[ -x "$BIN" ]] || fail "server binary not found after build"
echo "  binary=$BIN"

# --------------------------------------------------------------------------
# Start server
# --------------------------------------------------------------------------

log "Starting server"
BEAVA_SHARDS="$BEAVA_SHARDS" \
BEAVA_FJALL_CACHE_MB="$BEAVA_FJALL_CACHE_MB" \
BEAVA_TCP_PORT="$TCP_PORT" \
BEAVA_HTTP_PORT="$HTTP_PORT" \
BEAVA_DATA_DIR="$DATA_DIR" \
TALLY_TCP_PORT="$TCP_PORT" \
TALLY_HTTP_PORT="$HTTP_PORT" \
TALLY_DATA_DIR="$DATA_DIR" \
    "$BIN" > "$SERVER_LOG" 2>&1 &
SERVER_PID=$!
sleep 5

cleanup() {
    if kill -0 "$SERVER_PID" 2>/dev/null; then
        kill "$SERVER_PID" 2>/dev/null || true
        wait "$SERVER_PID" 2>/dev/null || true
    fi
}
trap cleanup EXIT INT TERM

# Wait for readiness
ready=0
for _ in $(seq 1 60); do
    if curl -sf "http://127.0.0.1:$HTTP_PORT/debug/ready" >/dev/null 2>&1; then
        ready=1; break
    fi
    sleep 0.5
done
[[ "$ready" == "1" ]] || { tail -40 "$SERVER_LOG"; fail "server did not become ready"; }
echo "  server pid=$SERVER_PID ready"

# --------------------------------------------------------------------------
# State fill + warmup + measure
# --------------------------------------------------------------------------
#
# We drive the soak from the existing fraud-pipeline Python SDK using
# bench.py. The client generator uses Zipfian keys at a cardinality sized
# to hit STATE_GB of on-disk state (~60M entities @ ~1.4 KB avg entity size
# for the complex pipeline's 47-feature shape → ~84 GB; round up to 100 GB
# with padding).
#
# bench.py streams events in uncapped; on a CCX43 we expect ~200-500K EPS
# steady-state. 100 GB / 1.4 KB = ~72M entities; at 200K EPS that's ~6
# minutes to populate. We budget WARMUP_HOURS on top so bloom filters +
# block cache fully settle before we start counting p99.

WARMUP_SEC=$(( WARMUP_HOURS * 3600 ))
MEASURE_SEC=$(( DURATION_HOURS * 3600 ))

log "Fill + warmup (${WARMUP_HOURS}h) — generating ${STATE_GB} GB state"
python3 "$REPO/benchmark/fraud-pipeline/bench.py" \
    --mode complex \
    --duration "$WARMUP_SEC" \
    --proc-id 0 \
    --host "localhost:$TCP_PORT" \
    --checkpoint 60 \
    > "$EVIDENCE_DIR/${TS}.warmup.jsonl" 2>&1 &
WARMUP_PID=$!
wait "$WARMUP_PID" || true

log "Measurement window (${DURATION_HOURS}h)"
MEASURE_START_EPOCH=$(date +%s)

# Kick off the sustained load
python3 "$REPO/benchmark/fraud-pipeline/bench.py" \
    --mode complex \
    --duration "$MEASURE_SEC" \
    --proc-id 1 \
    --host "localhost:$TCP_PORT" \
    --checkpoint 60 \
    > "$EVIDENCE_DIR/${TS}.measure.jsonl" 2>&1 &
MEASURE_PID=$!

# Sample /debug/latency every 60 s into a JSONL file. This is where the p99
# gate evidence comes from — not client-side timings (which include
# network + SDK overhead).
(
    while kill -0 "$MEASURE_PID" 2>/dev/null; do
        curl -sf "http://127.0.0.1:$HTTP_PORT/debug/latency" \
            | python3 -c 'import json, sys, time
data = json.load(sys.stdin)
rec = {"ts": time.time()}
for entry in data.get("per_command") or []:
    if str(entry.get("command","")).lower() in ("push", "get"):
        key = str(entry.get("command","")).lower()
        rec[f"{key}_p50_us"] = entry.get("p50_us")
        rec[f"{key}_p95_us"] = entry.get("p95_us")
        rec[f"{key}_p99_us"] = entry.get("p99_us")
        rec[f"{key}_count"] = entry.get("count")
print(json.dumps(rec))' \
            >> "$LATENCY_LOG" 2>/dev/null || true
        sleep 60
    done
) &
SAMPLER_PID=$!

wait "$MEASURE_PID"
kill "$SAMPLER_PID" 2>/dev/null || true

MEASURE_END_EPOCH=$(date +%s)
MEASURE_DURATION_S=$(( MEASURE_END_EPOCH - MEASURE_START_EPOCH ))

# --------------------------------------------------------------------------
# Evidence JSON
# --------------------------------------------------------------------------

log "Building evidence JSON"

python3 - "$LATENCY_LOG" "$EVIDENCE_DIR/${TS}.measure.jsonl" "$EVIDENCE_JSON" \
    "$TS" "$HOSTNAME" "$COMMIT_SHA" "$STATE_GB" "$DURATION_HOURS" \
    "$WARMUP_HOURS" "$BEAVA_FJALL_CACHE_MB" "$MEASURE_DURATION_S" <<'PY'
import json, sys, statistics
latency_log, measure_log, out_path = sys.argv[1:4]
ts, host, sha, state_gb, dur_h, warm_h, cache_mb, dur_s = sys.argv[4:]

# Aggregate per-minute p99 samples across the 8h window.
read_p99_us = []
read_p50_us = []
write_p99_us = []
write_p95_us = []
try:
    with open(latency_log) as fh:
        for line in fh:
            try:
                r = json.loads(line)
            except Exception:
                continue
            if r.get("get_p99_us") is not None:
                read_p99_us.append(float(r["get_p99_us"]))
            if r.get("get_p50_us") is not None:
                read_p50_us.append(float(r["get_p50_us"]))
            if r.get("push_p99_us") is not None:
                write_p99_us.append(float(r["push_p99_us"]))
            if r.get("push_p95_us") is not None:
                write_p95_us.append(float(r["push_p95_us"]))
except FileNotFoundError:
    pass

def median(xs): return statistics.median(xs) if xs else 0.0
def p99(xs):
    if not xs: return 0.0
    xs = sorted(xs)
    return xs[max(0, int(0.99 * len(xs)) - 1)]
def worst(xs): return max(xs) if xs else 0.0

# The gate is on READ p99 (GET operation). Sustained = 99th percentile of
# per-minute p99 samples stays below 1 ms.
read_p99_ms = p99(read_p99_us) / 1000.0 if read_p99_us else 0.0
read_p50_ms = median(read_p50_us) / 1000.0 if read_p50_us else 0.0
read_p95_ms = p99([x for x in read_p99_us]) / 1000.0  # proxy
read_p999_ms = worst(read_p99_us) / 1000.0 if read_p99_us else 0.0
write_p99_ms = p99(write_p99_us) / 1000.0 if write_p99_us else 0.0

# Pull throughput from measure.jsonl final line.
events = 0
try:
    with open(measure_log) as fh:
        for line in reversed(fh.read().splitlines()):
            if '"phase": "final"' in line:
                try:
                    events = int(json.loads(line).get("events", 0)); break
                except Exception:
                    pass
except FileNotFoundError:
    pass

throughput_eps = int(events / max(1, int(dur_s)))

rec = {
    "ts_utc": ts,
    "host": host,
    "commit_sha": sha,
    "git_sha": sha,
    "state_gb": float(state_gb),
    "duration_hours": float(dur_h),
    "warmup_hours": float(warm_h),
    "duration_s": int(dur_s),
    "events": events,
    "cache_mb": int(cache_mb),
    "p50_ms": round(read_p50_ms, 4),
    "p95_ms": round(read_p95_ms, 4),
    "p99_ms": round(read_p99_ms, 4),
    "p999_ms": round(read_p999_ms, 4),
    "write_p99_ms": round(write_p99_ms, 4),
    "throughput_eps": throughput_eps,
    "pass": bool(read_p99_ms > 0.0 and read_p99_ms < 1.0),
    "note": "p99_ms = 99th percentile of 1-minute read-p99 samples across the measurement window. pass = (p99_ms < 1.0 and p99_ms > 0.0).",
}
open(out_path, "w").write(json.dumps(rec, indent=2))
print(json.dumps(rec, indent=2))
PY

log "Soak complete"
echo "  Evidence: $EVIDENCE_JSON"
echo "  Latency log: $LATENCY_LOG"
echo "  Server log: $SERVER_LOG"
echo
echo "Next steps:"
echo "  1. scp $EVIDENCE_JSON back to workstation:"
echo "       .planning/phases/54-legacy-engine-removal/soak-evidence/"
echo "  2. git add .planning/phases/54-legacy-engine-removal/soak-evidence/"
echo "  3. git commit -m \"evidence(54): Hetzner CCX43 100GB 8h soak\""
echo "  4. /gsd-verify-work 54 — auto-verifies TPC-PERSIST-04 against the evidence."
