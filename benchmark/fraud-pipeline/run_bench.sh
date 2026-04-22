#!/usr/bin/env bash
# Beava fraud-pipeline benchmark — single-command reproducible throughput test.
#
# What it does:
#   1. Builds the server in release mode if missing.
#   2. Starts a fresh server on a scratch data directory.
#   3. Spawns N client processes (default: one per CPU) that each register
#      the 47-feature fraud pipeline and push events as fast as they can.
#   4. Collects per-client p50/p99/p99.9 latency samples plus live EPS.
#   5. Writes a machine-readable summary.json + stdout.log + per-client
#      JSONL to benchmark/fraud-pipeline/results/<timestamp>/.
#   6. Optionally generates a flamegraph if `cargo flamegraph` is installed.
#
# Usage:
#   bash benchmark/fraud-pipeline/run_bench.sh
#   MODE=simple bash benchmark/fraud-pipeline/run_bench.sh
#   CPUS=4 DURATION=30 bash benchmark/fraud-pipeline/run_bench.sh
#   CLIENTS=8 bash benchmark/fraud-pipeline/run_bench.sh  # override client count separate from server threads
#
# Environment variables (all optional):
#   MODE         simple|complex  (default complex — full 47-feature pipeline)
#   CPUS         server worker threads (default: host CPU count, capped at 8 for the run label)
#   CLIENTS      parallel client processes (default: same as CPUS)
#   WARMUP       warmup seconds, not measured (default 5)
#   DURATION     measurement window seconds (default 60)
#   CHECKPOINT   live-EPS print interval seconds (default 5)
#   TCP_PORT     server TCP port (default 6400)
#   HTTP_PORT    server HTTP management port (default 6401)
#   BEAVA_BIN    path to beava/tally server binary (default: auto-detected)
#   SKIP_BUILD   set to 1 to skip the cargo build step
#   NO_FLAMEGRAPH set to 1 to skip the flamegraph step even if cargo-flamegraph is available
#
# Exit codes:
#   0  success
#   1  bench failed (server died, clients errored, or no events measured)
#   2  build failed
#   3  environment/prerequisite problem (Python SDK missing, port in use, etc.)
#
# Typical wall-clock on an 8-core laptop:
#   build (first time)  ~30-60s
#   bench run           WARMUP + DURATION + ~5s overhead = ~70s at defaults
#   total               under 2min first run, ~70s on re-runs

set -uo pipefail

# --------------------------------------------------------------------------
# Config
# --------------------------------------------------------------------------

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BENCH_DIR="$REPO/benchmark/fraud-pipeline"
# Phase 56 Wave 4: select the cross-shard enrichment scenario when
# BEAVA_ENRICH_CROSSSHARD_SCENARIO=1. That scenario registers Countries
# (source_table with shard_key=country_code) + Txns (stream with
# shard_key=user_id) — uniform country_code × Zipf user_id drives ~87.5%
# of enrichment reads across shard boundaries at N=8. Other scenarios
# (the default fraud pipeline) stay on bench.py.
if [[ "${BEAVA_ENRICH_CROSSSHARD_SCENARIO:-0}" = "1" ]]; then
    BENCH="$BENCH_DIR/scenario_crossshard_enrich.py"
    echo "[bench] Phase 56 cross-shard enrichment scenario selected ($BENCH)"
else
    BENCH="$BENCH_DIR/bench.py"
fi

MODE="${MODE:-complex}"
WARMUP="${WARMUP:-5}"
DURATION="${DURATION:-60}"
CHECKPOINT="${CHECKPOINT:-5}"
TCP_PORT="${TCP_PORT:-6400}"
HTTP_PORT="${HTTP_PORT:-6401}"
SKIP_BUILD="${SKIP_BUILD:-0}"
NO_FLAMEGRAPH="${NO_FLAMEGRAPH:-0}"

# Detect host CPUs.
if   _c=$(sysctl -n hw.ncpu 2>/dev/null); then CPUS_HOST=$_c
elif _c=$(nproc 2>/dev/null);              then CPUS_HOST=$_c
else CPUS_HOST=4; fi
# We cap the default at 8 clients because the server side saturates on ~8
# workers for the 47-feature pipeline per the Phase 42 measurements; more
# than that adds contention without raw throughput gain. The user can still
# override via CPUS= or CLIENTS=.
CPUS="${CPUS:-$(( CPUS_HOST < 8 ? CPUS_HOST : 8 ))}"
CLIENTS="${CLIENTS:-$CPUS}"

TS="$(date -u +%Y%m%dT%H%M%SZ)"
# OUTPUT_DIR override lets callers (e.g. run_matrix.sh) aim per-cell results at
# a specific directory instead of the auto-timestamped default. Matrix runner
# expects ${CELL_DIR}/summary.json — without this override it silently landed
# in results/<ts>/ and the matrix's existence check failed.
RESULTS_DIR="${OUTPUT_DIR:-$BENCH_DIR/results/$TS}"
mkdir -p "$RESULTS_DIR"
STDOUT_LOG="$RESULTS_DIR/stdout.log"
SUMMARY_JSON="$RESULTS_DIR/summary.json"

# Tee everything to both the terminal and the per-run stdout.log. `exec >`
# redirects the rest of this script's stdout; stderr piggybacks via 2>&1
# at the tee boundary so the user sees a single combined stream.
exec > >(tee -a "$STDOUT_LOG") 2>&1

# --------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------

log()  { printf '\n\033[1m==> %s\033[0m\n' "$*"; }
warn() { printf '\033[33m[warn]\033[0m %s\n' "$*" >&2; }
fail() { printf '\033[31m[fail]\033[0m %s\n' "$*" >&2; exit "${2:-1}"; }

has_cmd() { command -v "$1" >/dev/null 2>&1; }

# Server binary: prefer BEAVA_BIN override; fall back to target/release/beava
# or target/release/tally (the worktree may be mid-rename).
resolve_server_bin() {
    if [[ -n "${BEAVA_BIN:-}" && -x "$BEAVA_BIN" ]]; then
        echo "$BEAVA_BIN"; return
    fi
    for candidate in "$REPO/target/release/beava" "$REPO/target/release/tally"; do
        if [[ -x "$candidate" ]]; then echo "$candidate"; return; fi
    done
    echo ""
}

# --------------------------------------------------------------------------
# Prerequisite checks
# --------------------------------------------------------------------------

log "Beava fraud-pipeline benchmark — $(date -u +%Y-%m-%d\ %H:%M:%SZ)"
echo "Host: $(uname -sm), $CPUS_HOST cores"
echo "Config: MODE=$MODE CLIENTS=$CLIENTS WORKER_THREADS=$CPUS WARMUP=${WARMUP}s DURATION=${DURATION}s"
echo "Results: $RESULTS_DIR"

if ! has_cmd python3; then
    fail "python3 not found on PATH — install Python 3.10 or newer" 3
fi

if ! python3 -c "import tally" 2>/dev/null; then
    # The Python SDK is co-located with the repo; bench.py sets PYTHONPATH so
    # a clone-and-run works without `pip install -e python/`. Warn only, don't
    # abort — bench.py handles the sys.path hack.
    :
fi

if lsof -i ":$TCP_PORT" >/dev/null 2>&1; then
    fail "TCP port $TCP_PORT is already in use. Stop the other process or set TCP_PORT=... to pick a different one" 3
fi
if lsof -i ":$HTTP_PORT" >/dev/null 2>&1; then
    fail "HTTP port $HTTP_PORT is already in use. Stop the other process or set HTTP_PORT=... to pick a different one" 3
fi

# --------------------------------------------------------------------------
# Build the server if needed
# --------------------------------------------------------------------------

BIN="$(resolve_server_bin)"
if [[ -z "$BIN" && "$SKIP_BUILD" != "1" ]]; then
    if ! has_cmd cargo; then
        fail "cargo not found on PATH and no pre-built binary at target/release/{beava,tally} — install Rust toolchain or set BEAVA_BIN to point at a built binary" 3
    fi
    log "Building server in release mode (one-time, ~30-60s)..."
    if ! cargo build --release --bin tally 2>&1 | tee -a "$STDOUT_LOG" >/dev/null; then
        # Try beava binary name as fallback (post-rename tree).
        if ! cargo build --release --bin beava 2>&1 | tee -a "$STDOUT_LOG" >/dev/null; then
            fail "cargo build --release failed. See $STDOUT_LOG for details" 2
        fi
    fi
    BIN="$(resolve_server_bin)"
fi

if [[ -z "$BIN" ]]; then
    fail "server binary not found after build attempt (expected target/release/beava or target/release/tally)" 2
fi
echo "Binary: $BIN"

# --------------------------------------------------------------------------
# Start the server
# --------------------------------------------------------------------------

DATA_DIR="$(mktemp -d -t beava-bench-data.XXXXXX)"
SRV_LOG="$RESULTS_DIR/server.log"

# env vars: support both BEAVA_* (post-rename) and TALLY_* (pre-rename). We
# set both so the binary picks up whichever it reads.
log "Starting server on TCP=$TCP_PORT HTTP=$HTTP_PORT threads=$CPUS"
TALLY_ADMIN_TOKEN="${TALLY_ADMIN_TOKEN:-dev-admin-token}" \
BEAVA_ADMIN_TOKEN="${BEAVA_ADMIN_TOKEN:-dev-admin-token}" \
TALLY_WORKER_THREADS="$CPUS" BEAVA_WORKER_THREADS="$CPUS" \
TALLY_TCP_PORT="$TCP_PORT"    BEAVA_TCP_PORT="$TCP_PORT" \
TALLY_HTTP_PORT="$HTTP_PORT"  BEAVA_HTTP_PORT="$HTTP_PORT" \
TALLY_DATA_DIR="$DATA_DIR"    BEAVA_DATA_DIR="$DATA_DIR" \
BEAVA_SHARD_INBOX_SIZE="${BEAVA_SHARD_INBOX_SIZE:-}" \
    "$BIN" > "$SRV_LOG" 2>&1 &
SERVER_PID=$!

cleanup() {
    # Kill any lingering client processes first so their stdout doesn't race
    # with the server shutdown.
    pkill -9 -P $$ 2>/dev/null || true
    if kill -0 "$SERVER_PID" 2>/dev/null; then
        kill "$SERVER_PID" 2>/dev/null || true
        wait "$SERVER_PID" 2>/dev/null || true
    fi
    # Scratch data dir can grow (event log + snapshots). Remove it so repeat
    # runs don't accumulate junk on /tmp.
    if [[ -n "${DATA_DIR:-}" && -d "$DATA_DIR" ]]; then
        rm -rf "$DATA_DIR"
    fi
}
trap cleanup EXIT INT TERM

# Poll /debug/ready until the server reports up or we give up at 15s.
ready=0
for _ in $(seq 1 50); do
    if curl -sf "http://127.0.0.1:$HTTP_PORT/debug/ready" >/dev/null 2>&1; then
        ready=1; break
    fi
    sleep 0.3
done
if [[ "$ready" != "1" ]]; then
    echo "--- server.log (last 40 lines) ---"
    tail -40 "$SRV_LOG" || true
    fail "server did not become ready within 15s. See $SRV_LOG" 1
fi
echo "Server ready (pid=$SERVER_PID, data=$DATA_DIR)"

# --------------------------------------------------------------------------
# Spawn clients
# --------------------------------------------------------------------------

CLIENT_TMP="$(mktemp -d -t beava-bench-clients.XXXXXX)"
trap 'cleanup; rm -rf "$CLIENT_TMP"' EXIT INT TERM

spawn_clients() {
    local dur="$1"; local tag="$2"
    CLIENT_PIDS=()
    for i in $(seq 0 $((CLIENTS - 1))); do
        python3 "$BENCH" \
            --mode "$MODE" \
            --duration "$dur" \
            --proc-id "$i" \
            --host "localhost:$TCP_PORT" \
            --checkpoint "$CHECKPOINT" \
            > "$CLIENT_TMP/$tag-$i.jsonl" 2>&1 &
        CLIENT_PIDS+=($!)
    done
}
wait_clients() {
    local exit_code=0
    for pid in "${CLIENT_PIDS[@]}"; do
        if ! wait "$pid" 2>/dev/null; then exit_code=1; fi
    done
    return $exit_code
}

log "Warmup ${WARMUP}s (output discarded)"
spawn_clients "$WARMUP" warmup
wait_clients || warn "one or more warmup clients exited non-zero"
rm -f "$CLIENT_TMP"/warmup-*.jsonl

log "Measuring ${DURATION}s (live EPS every ${CHECKPOINT}s)"

# Capture server-truth counter BEFORE spawning measurement clients.
# Source of truth: /debug/processed-events counts events the shard threads
# actually processed end-to-end, distinct from events_total which
# double-counts inbox-accept + shard-process. Fire-and-forget OP_PUSH_ASYNC
# in bench.py means client-reported EPS is "events submitted" not "events
# processed" — most events hit inbox_tx.try_send → Full → silently rejected
# at saturation. Server-truth EPS is the only number that reflects actual
# throughput. 2026-04-21.
SERVER_TRUTH_BEFORE=$(curl -s -H "Authorization: Bearer ${BEAVA_ADMIN_TOKEN:-dev-admin-token}" \
    "http://127.0.0.1:$HTTP_PORT/debug/processed-events" 2>/dev/null \
    | python3 -c "import json,sys
try: print(json.load(sys.stdin).get('server_processed_events', 0))
except: print(0)" 2>/dev/null || echo 0)
SERVER_TRUTH_T0_NS=$(python3 -c "import time; print(int(time.time()*1e9))")

spawn_clients "$DURATION" measure

# Poll checkpoint files on a 5s cadence; print live aggregate eps until all
# clients have emitted a final line.
set +e
last_total=0
last_t=0
while :; do
    finals=$(grep -l '"phase": "final"' "$CLIENT_TMP"/measure-*.jsonl 2>/dev/null | wc -l | tr -d ' ')
    if [[ "$finals" -ge "$CLIENTS" ]]; then break; fi
    sleep "$CHECKPOINT"

    # Sum the last checkpoint from each client.
    read -r t_now total < <(python3 - "$CLIENT_TMP" <<'PY'
import glob, json, sys
tmp = sys.argv[1]
total = 0; t = 0.0
for f in glob.glob(f"{tmp}/measure-*.jsonl"):
    with open(f, encoding="utf-8") as fh:
        ckpts = [l for l in fh.read().splitlines() if '"phase"' in l]
    if not ckpts: continue
    try:
        last = json.loads(ckpts[-1])
    except Exception:
        continue
    total += int(last.get("events", 0))
    t = max(t, float(last.get("t", 0.0)))
print(f"{t:.2f} {total}")
PY
)
    if [[ -n "${total:-}" && "${total:-0}" != "0" ]]; then
        dt=$(python3 -c "print(max(0.01, $t_now - $last_t))")
        inst=$(python3 -c "print(int(($total - $last_total) / $dt))")
        avg=$(python3 -c "print(int($total / max(0.01, $t_now)))")
        printf "    t=%5.1fs  events=%12d  instant=%10s eps  avg=%10s eps\n" \
            "$t_now" "$total" "$inst" "$avg"
        last_total=$total; last_t=$t_now
    fi
done
wait_clients
clients_exit=$?
set -uo pipefail

# Capture server-truth counter AFTER measurement clients finished.
SERVER_TRUTH_AFTER=$(curl -s -H "Authorization: Bearer ${BEAVA_ADMIN_TOKEN:-dev-admin-token}" \
    "http://127.0.0.1:$HTTP_PORT/debug/processed-events" 2>/dev/null \
    | python3 -c "import json,sys
try: print(json.load(sys.stdin).get('server_processed_events', 0))
except: print(0)" 2>/dev/null || echo 0)
SERVER_TRUTH_T1_NS=$(python3 -c "import time; print(int(time.time()*1e9))")
export SERVER_TRUTH_BEFORE SERVER_TRUTH_AFTER SERVER_TRUTH_T0_NS SERVER_TRUTH_T1_NS

if [[ "$clients_exit" != "0" ]]; then
    warn "some clients exited non-zero — partial data may be missing"
fi

# Copy per-client JSONL into the results dir for post-hoc inspection.
cp "$CLIENT_TMP"/measure-*.jsonl "$RESULTS_DIR/" 2>/dev/null || true

# --------------------------------------------------------------------------
# Aggregate + write summary.json
# --------------------------------------------------------------------------

log "Aggregating"
if ! python3 - "$CLIENT_TMP" "$MODE" "$CLIENTS" "$CPUS" "$HTTP_PORT" "$SUMMARY_JSON" "$TS" "$DURATION" "$WARMUP" "$SERVER_TRUTH_BEFORE" "$SERVER_TRUTH_AFTER" "$SERVER_TRUTH_T0_NS" "$SERVER_TRUTH_T1_NS" <<'PY'
import glob, json, socket, sys, urllib.request, urllib.error
from pathlib import Path

tmp, mode, clients, threads, http, out_path, ts, duration, warmup, st_before, st_after, st_t0, st_t1 = sys.argv[1:]
clients_i = int(clients)
# Server-truth EPS — the ground-truth counter. Clients' events/sec reflects
# submissions (which include inbox-rejected batches); server_processed_events
# counts only events the shard threads actually handled end-to-end.
st_before_i = int(st_before)
st_after_i = int(st_after)
st_delta = max(0, st_after_i - st_before_i)
st_dt = max(1e-9, (int(st_t1) - int(st_t0)) / 1e9)
server_truth_eps = st_delta / st_dt

finals = []
for f in sorted(glob.glob(f"{tmp}/measure-*.jsonl")):
    with open(f, encoding="utf-8") as fh:
        for line in reversed(fh.read().splitlines()):
            if '"phase": "final"' in line:
                try:
                    finals.append(json.loads(line))
                except Exception:
                    pass
                break

if not finals:
    print("ERROR: no client produced a final record", file=sys.stderr)
    sys.exit(1)

total_events = sum(r.get("events", 0) for r in finals)
wall = max(r.get("t", 0.0) for r in finals)
if wall <= 0 or total_events <= 0:
    print(f"ERROR: bogus bench output (wall={wall}, events={total_events})", file=sys.stderr)
    sys.exit(1)

agg_eps = int(total_events / wall)
per_event_us = wall / total_events * 1e6

# Per-client latency aggregation: merge batch-timing samples across all
# clients by taking worst-case percentiles (max of each client's p99 is a
# better "what any single client saw" number than averaging).
def stat(name):
    vals = [r.get(name, 0.0) for r in finals if r.get("sample_count", 0) > 0]
    return {
        "min": round(min(vals), 2) if vals else 0.0,
        "median": round(sorted(vals)[len(vals)//2], 2) if vals else 0.0,
        "max": round(max(vals), 2) if vals else 0.0,
    }

p50 = stat("p50_us")
p99 = stat("p99_us")
p999 = stat("p999_us")

# Pull server-side PUSH latency from /debug/latency if available.
server_push = {"p50_us": None, "p95_us": None, "p99_us": None, "count": None}
try:
    with urllib.request.urlopen(f"http://127.0.0.1:{http}/debug/latency", timeout=5) as resp:
        body = json.loads(resp.read())
    for entry in (body.get("per_command") or []):
        if str(entry.get("command", "")).lower() == "push":
            server_push = {
                "p50_us": entry.get("p50_us"),
                "p95_us": entry.get("p95_us"),
                "p99_us": entry.get("p99_us"),
                "count": entry.get("count"),
            }
            break
except (urllib.error.URLError, urllib.error.HTTPError, socket.error, ValueError) as exc:
    print(f"note: could not fetch /debug/latency: {exc}", file=sys.stderr)

# Pull memory footprint.
memory = {"estimated_bytes": None, "entity_count": None}
try:
    with urllib.request.urlopen(f"http://127.0.0.1:{http}/debug/memory", timeout=5) as resp:
        mem_body = json.loads(resp.read())
    total_bytes = 0
    entities = 0
    for s in mem_body.get("per_stream") or []:
        total_bytes += int(s.get("estimated_bytes") or 0)
        entities = max(entities, int(s.get("key_count") or 0))
    memory = {
        "estimated_bytes": total_bytes,
        "entity_count": entities,
    }
except (urllib.error.URLError, urllib.error.HTTPError, socket.error, ValueError) as exc:
    print(f"note: could not fetch /debug/memory: {exc}", file=sys.stderr)

summary = {
    "timestamp": ts,
    "host": {
        "hostname": socket.gethostname(),
        "platform": sys.platform,
    },
    "config": {
        "mode": mode,
        "clients": clients_i,
        "worker_threads": int(threads),
        "warmup_seconds": float(warmup),
        "duration_seconds": float(duration),
    },
    "throughput": {
        "total_events": int(total_events),
        "wall_seconds": round(float(wall), 3),
        "aggregate_eps": agg_eps,
        "per_event_us": round(per_event_us, 2),
        # Server-truth — events the shard threads actually processed
        # end-to-end during the measurement window. Authoritative.
        "server_truth_events": st_delta,
        "server_truth_seconds": round(st_dt, 3),
        "server_truth_eps": int(server_truth_eps),
        "server_truth_per_event_us": round(st_dt / st_delta * 1e6, 2) if st_delta > 0 else None,
        "client_over_server_ratio": round(agg_eps / server_truth_eps, 2) if server_truth_eps > 0 else None,
    },
    "client_push_latency_us": {
        "note": "per-push_many call time in microseconds (batch=1000 events). Each client samples every 64th call.",
        "p50_across_clients": p50,
        "p99_across_clients": p99,
        "p999_across_clients": p999,
        "sample_counts": [int(r.get("sample_count", 0)) for r in finals],
    },
    "server_push_latency_us": server_push,
    "memory": memory,
    "per_client": [
        {
            "proc_id": r.get("proc_id"),
            "events": int(r.get("events", 0)),
            "t_seconds": round(float(r.get("t", 0.0)), 3),
            "eps": int(r.get("events", 0) / r.get("t", 1.0)) if r.get("t", 0) > 0 else 0,
            "p50_us": r.get("p50_us"),
            "p99_us": r.get("p99_us"),
            "p999_us": r.get("p999_us"),
            "error": r.get("error"),
            "error_msg": r.get("error_msg"),
        }
        for r in sorted(finals, key=lambda x: x.get("proc_id", 0))
    ],
    "errored_clients": sum(1 for r in finals if r.get("error")),
}

Path(out_path).write_text(json.dumps(summary, indent=2))
PY
then
    fail "aggregation failed — see the error above" 1
fi

# --------------------------------------------------------------------------
# Human-readable summary printed to stdout
# --------------------------------------------------------------------------

log "STEADY-STATE SUMMARY"
python3 - "$SUMMARY_JSON" <<'PY'
import json, sys
s = json.loads(open(sys.argv[1]).read())

cfg = s["config"]
tp = s["throughput"]
cl = s["client_push_latency_us"]
srv = s["server_push_latency_us"]
mem = s["memory"]

print(f"    Config:       {cfg['mode']} pipeline, {cfg['clients']} clients, {cfg['worker_threads']} server threads")
print(f"    Duration:     {cfg['duration_seconds']:.0f}s measured (+{cfg['warmup_seconds']:.0f}s warmup)")
print(f"    Events:       {tp['total_events']:,}")
print(f"    Aggregate:    {tp['aggregate_eps']:,} events/sec   [CLIENT-REPORTED]")
print(f"    Per event:    {tp['per_event_us']:.2f} microseconds (client-reported)")
print()
# Server-truth — the authoritative counter. Clients count submission attempts
# (fire-and-forget OP_PUSH_ASYNC, no ACK), so client-reported EPS includes
# events that hit inbox_tx.try_send → Full and were silently rejected. The
# server_truth_eps counter increments only after shard threads process the
# event end-to-end via engine.push_*_on_shard.
st_ev = tp.get('server_truth_events', 0)
st_eps = tp.get('server_truth_eps', 0)
ratio = tp.get('client_over_server_ratio')
print(f"    SERVER-TRUE:  {st_eps:>12,} events/sec  [{st_ev:,} events in {tp.get('server_truth_seconds', 0):.1f}s]")
if tp.get('server_truth_per_event_us') is not None:
    print(f"    Per event:    {tp['server_truth_per_event_us']:.2f} microseconds (server-true)")
if ratio is not None and ratio > 1.5:
    print(f"    ⚠ Client over-reports by {ratio:.1f}x — most push attempts hit inbox backpressure and were silently rejected.")
print()
# Phase 56 Wave 4: machine-parseable line for the perf-gate test harness
# (crossshard_enrich_eps_floor) to grep out. Kept as CLIENT-reported
# aggregate for backwards compatibility with existing grep gates.
print(f"Aggregate EPS: {tp['aggregate_eps']}")
print(f"Server-Truth EPS: {st_eps}")
print()
print(f"    Client push_many latency (microseconds per 1000-event batch call):")
print(f"                   median across clients    worst across clients")
print(f"      p50       :  {cl['p50_across_clients']['median']:>12,.1f}            {cl['p50_across_clients']['max']:>12,.1f}")
print(f"      p99       :  {cl['p99_across_clients']['median']:>12,.1f}            {cl['p99_across_clients']['max']:>12,.1f}")
print(f"      p99.9     :  {cl['p999_across_clients']['median']:>12,.1f}            {cl['p999_across_clients']['max']:>12,.1f}")
print()
if srv.get("p50_us") is not None:
    print(f"    Server-side PUSH latency (microseconds; from /debug/latency):")
    print(f"      p50       :  {srv['p50_us']:>12,.1f}")
    print(f"      p95       :  {srv['p95_us']:>12,.1f}")
    print(f"      p99       :  {srv['p99_us']:>12,.1f}")
    print(f"      count     :  {srv['count']:>12,}")
    print()
if mem.get("estimated_bytes"):
    mb = mem['estimated_bytes'] / (1024 * 1024)
    ents = mem['entity_count'] or 1
    print(f"    Memory:       {mb:,.1f} MB across ~{mem['entity_count']:,} per-stream keys")
    print(f"                  ({mem['estimated_bytes']/ents:,.0f} bytes per entity)")
    print()

print(f"    Per-client throughput:")
for row in s["per_client"]:
    err = f"  [ERROR {row['error']}: {row.get('error_msg') or ''}]" if row.get("error") else ""
    print(f"      proc-{row['proc_id']}: {row['events']:>9,} events in {row['t_seconds']:>6.2f}s = {row['eps']:>9,} eps{err}")
if s.get("errored_clients", 0):
    print(f"    NOTE: {s['errored_clients']}/{cfg['clients']} client(s) exited with error — aggregate EPS includes their partial data")
print()
PY

# --------------------------------------------------------------------------
# Optional flamegraph
# --------------------------------------------------------------------------

if [[ "$NO_FLAMEGRAPH" != "1" ]] && has_cmd cargo-flamegraph; then
    log "Generating flamegraph (cargo-flamegraph detected) — this adds ~10s"
    FLAMEGRAPH_OUT="$RESULTS_DIR/flamegraph.svg"
    # Short (5s) sample while a single client is running; we reuse the
    # already-loaded state instead of a fresh warmup so the flame captures
    # steady-state work, not pipeline-registration cost.
    FLAME_CLIENT_LOG="$CLIENT_TMP/flame-client.jsonl"
    python3 "$BENCH" --mode "$MODE" --duration 10 --proc-id 99 \
        --host "localhost:$TCP_PORT" --checkpoint 10 > "$FLAME_CLIENT_LOG" 2>&1 &
    FLAME_CLIENT_PID=$!
    if ! cargo flamegraph --pid "$SERVER_PID" --output "$FLAMEGRAPH_OUT" -- sleep 5 >/dev/null 2>&1; then
        warn "cargo flamegraph failed (typically needs sudo / perf permissions) — skipping"
    else
        echo "    Wrote $FLAMEGRAPH_OUT"
    fi
    wait "$FLAME_CLIENT_PID" 2>/dev/null || true
elif [[ "$NO_FLAMEGRAPH" != "1" ]]; then
    echo "note: cargo-flamegraph not installed — skipping flame step (install with \`cargo install flamegraph\`)"
fi

# --------------------------------------------------------------------------
# Done
# --------------------------------------------------------------------------

log "DONE"
echo "    summary.json:  $SUMMARY_JSON"
echo "    stdout.log:    $STDOUT_LOG"
echo "    server.log:    $SRV_LOG"
echo "    per-client:    $RESULTS_DIR/measure-*.jsonl"
echo
echo "Compare throughput against the Phase-42 Hetzner baseline (544K eps, 16-core)."
echo "On laptops, 60-200K eps is typical; numbers scale near-linearly with core count up to"
echo "~8 client/worker pairs before lock and kernel-network overhead flattens the curve."
