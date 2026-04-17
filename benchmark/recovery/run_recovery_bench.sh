#!/usr/bin/env bash
# Recovery-time benchmark (Phase 43 T5 coverage gap closer).
#
# Measures end-to-end recovery wall-clock on the same machine that runs the
# fraud-pipeline throughput bench, using the same 47-feature complex pipeline
# at the same peak EPS (8 clients × 8 worker threads).
#
# Flow:
#   1. Start beava on a fresh scratch data dir (snapshots + event_log enabled).
#   2. Push N events via bench.py at peak EPS.
#   3. Trigger POST /snapshot (admin-gated) so durable state is known-exact.
#   4. kill -9 the server (ungraceful crash simulation).
#   5. Record on-disk state size (snapshot dir + event log dir bytes).
#   6. Restart beava on the same data dir; time from spawn to /debug/ready 200.
#   7. Sanity-check entity_count after restart matches pre-crash.
#   8. Write recovery_summary.json with the measurements.
#
# What this does NOT measure:
#   - WAL replay time for events newer than the last snapshot. Beava recovery
#     is snapshot-based; per-event WAL replay of fresh events into operator
#     state is NOT an automatic startup pass today. The WAL is used for
#     backfilling NEW features against history (registered post-restart),
#     not for resurrecting operator state since the last snapshot. So this
#     benchmark measures snapshot-load recovery, which is the real
#     restart-to-ready wall-clock dominating factor.
#
# Environment variables (all optional):
#   DURATION   push seconds before kill (default 30)
#   CLIENTS    parallel pusher processes (default 8)
#   CPUS       server worker threads (default min(host, 8))
#   TCP_PORT   server TCP port (default 6600)
#   HTTP_PORT  server HTTP port (default 6601)
#   BEAVA_BIN  override binary path
#   SKIP_BUILD skip cargo build --release

set -uo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BENCH_PY="$REPO/benchmark/fraud-pipeline/bench.py"

DURATION="${DURATION:-30}"
CLIENTS="${CLIENTS:-8}"
TCP_PORT="${TCP_PORT:-6600}"
HTTP_PORT="${HTTP_PORT:-6601}"
SKIP_BUILD="${SKIP_BUILD:-0}"
ADMIN_TOKEN="${BEAVA_ADMIN_TOKEN:-dev-admin-token}"

if   _c=$(sysctl -n hw.ncpu 2>/dev/null); then CPUS_HOST=$_c
elif _c=$(nproc 2>/dev/null);              then CPUS_HOST=$_c
else CPUS_HOST=4; fi
CPUS="${CPUS:-$(( CPUS_HOST < 8 ? CPUS_HOST : 8 ))}"

TS="$(date -u +%Y%m%dT%H%M%SZ)"
RESULTS_DIR="$REPO/benchmark/recovery/results/$TS"
mkdir -p "$RESULTS_DIR"
STDOUT_LOG="$RESULTS_DIR/stdout.log"
SUMMARY_JSON="$RESULTS_DIR/recovery_summary.json"

exec > >(tee -a "$STDOUT_LOG") 2>&1

log()  { printf '\n\033[1m==> %s\033[0m\n' "$*"; }
warn() { printf '\033[33m[warn]\033[0m %s\n' "$*" >&2; }
fail() { printf '\033[31m[fail]\033[0m %s\n' "$*" >&2; exit "${2:-1}"; }

resolve_bin() {
    if [[ -n "${BEAVA_BIN:-}" && -x "$BEAVA_BIN" ]]; then
        echo "$BEAVA_BIN"; return
    fi
    for c in "$REPO/target/release/beava" "$REPO/target/release/tally"; do
        [[ -x "$c" ]] && { echo "$c"; return; }
    done
    echo ""
}

log "Recovery-time benchmark — $(date -u +%Y-%m-%d\ %H:%M:%SZ)"
echo "Host: $(uname -sm), $CPUS_HOST cores"
echo "Config: DURATION=${DURATION}s CLIENTS=$CLIENTS THREADS=$CPUS"
echo "Results: $RESULTS_DIR"

BIN="$(resolve_bin)"
if [[ -z "$BIN" && "$SKIP_BUILD" != "1" ]]; then
    log "Building release binary..."
    (cd "$REPO" && cargo build --release --bin beava) || fail "cargo build failed" 2
    BIN="$(resolve_bin)"
fi
[[ -z "$BIN" ]] && fail "beava binary not found" 2

if lsof -i ":$TCP_PORT" >/dev/null 2>&1; then
    fail "TCP port $TCP_PORT already in use" 3
fi

DATA_DIR="$(mktemp -d -t beava-recovery-data.XXXXXX)"
SRV_LOG="$RESULTS_DIR/server.log"
SRV_RESTART_LOG="$RESULTS_DIR/server-restart.log"
CLIENT_TMP="$(mktemp -d -t beava-recovery-clients.XXXXXX)"

start_server() {
    local logfile="$1"
    # Collocate snapshot + event log under DATA_DIR so the restart finds
    # the exact same files the pre-crash server wrote. BEAVA_SNAPSHOT_PATH
    # defaults to `./beava.snapshot` (CWD), which would be fragile here.
    BEAVA_ADMIN_TOKEN="$ADMIN_TOKEN" \
    BEAVA_WORKER_THREADS="$CPUS" \
    BEAVA_TCP_PORT="$TCP_PORT" \
    BEAVA_HTTP_PORT="$HTTP_PORT" \
    BEAVA_DATA_DIR="$DATA_DIR" \
    BEAVA_SNAPSHOT_PATH="$DATA_DIR/beava.snapshot" \
    BEAVA_EVENT_LOG=1 \
    BEAVA_SNAPSHOT=1 \
        "$BIN" > "$logfile" 2>&1 &
    echo $!
}

wait_ready() {
    local pid=$1
    local deadline=$2
    local start_ns
    start_ns=$(python3 -c 'import time;print(int(time.monotonic_ns()))')
    while :; do
        if curl -sf "http://127.0.0.1:$HTTP_PORT/debug/ready" >/dev/null 2>&1; then
            python3 -c "print((int(__import__('time').monotonic_ns()) - $start_ns) / 1e9)"
            return 0
        fi
        if ! kill -0 "$pid" 2>/dev/null; then
            warn "server died during readiness poll"
            return 1
        fi
        local now_s
        now_s=$(date +%s)
        if (( now_s > deadline )); then
            warn "server did not reach /debug/ready within deadline"
            return 1
        fi
        sleep 0.05
    done
}

cleanup() {
    pkill -9 -P $$ 2>/dev/null || true
    rm -rf "$CLIENT_TMP" "$DATA_DIR"
}
trap cleanup EXIT INT TERM

# ------------------------------------------------------------------------
# Phase 1: warm up the server with peak load
# ------------------------------------------------------------------------

log "Starting server (PID capture)"
SERVER_PID=$(start_server "$SRV_LOG")
DEADLINE=$(($(date +%s) + 15))
READY_INIT=$(wait_ready "$SERVER_PID" "$DEADLINE")
if [[ -z "$READY_INIT" ]]; then
    tail -40 "$SRV_LOG"; fail "initial startup failed" 1
fi
printf "    Initial startup: %.3fs\n" "$READY_INIT"

log "Pushing events for ${DURATION}s at peak EPS"
CLIENT_PIDS=()
for i in $(seq 0 $((CLIENTS - 1))); do
    python3 "$BENCH_PY" \
        --mode complex \
        --duration "$DURATION" \
        --proc-id "$i" \
        --host "localhost:$TCP_PORT" \
        --checkpoint "$DURATION" \
        > "$CLIENT_TMP/c-$i.jsonl" 2>&1 &
    CLIENT_PIDS+=($!)
done
for pid in "${CLIENT_PIDS[@]}"; do wait "$pid" 2>/dev/null || true; done

TOTAL_EVENTS=$(python3 - <<'PY' "$CLIENT_TMP"
import glob, json, sys
total = 0
for f in glob.glob(f"{sys.argv[1]}/c-*.jsonl"):
    with open(f) as fh:
        for line in reversed(fh.read().splitlines()):
            if '"phase": "final"' in line:
                try:
                    total += int(json.loads(line).get("events", 0))
                except Exception:
                    pass
                break
print(total)
PY
)
echo "    Events pushed: $TOTAL_EVENTS"

ENTITY_COUNT_PRE=$(curl -sf "http://127.0.0.1:$HTTP_PORT/metrics" \
    | awk '/^beava_keys_total / {print $2}')
echo "    Entities in memory: $ENTITY_COUNT_PRE"

# ------------------------------------------------------------------------
# Phase 2: force a snapshot so durable state is known-exact
# ------------------------------------------------------------------------

log "Triggering POST /snapshot (admin-gated, retries on 409 = in-progress)"
SNAP_START=$(python3 -c 'import time;print(int(time.monotonic_ns()))')
SNAP_HTTP=""
for attempt in 1 2 3 4 5; do
    # `-f` returns non-zero on 4xx/5xx and drops the body; we drop it to
    # preserve the status byte via -w. 409 means "already in progress" —
    # the periodic timer's snapshot is good enough; retry to wait it out.
    SNAP_HTTP=$(curl -s -o /dev/null -w "%{http_code}" -X POST \
        "http://127.0.0.1:$HTTP_PORT/snapshot")
    if [[ "$SNAP_HTTP" == "200" || "$SNAP_HTTP" == "202" ]]; then
        break
    fi
    if [[ "$SNAP_HTTP" == "409" ]]; then
        # Wait for the in-progress snapshot to drain before retrying.
        sleep 1
        continue
    fi
    warn "snapshot trigger returned HTTP $SNAP_HTTP — will retry"
    sleep 1
done
SNAP_SECONDS=$(python3 -c "print((int(__import__('time').monotonic_ns()) - $SNAP_START) / 1e9)")
echo "    POST /snapshot => HTTP $SNAP_HTTP after ${SNAP_SECONDS}s"
# Brief moment for the blocking write to hit disk before we SIGKILL.
sleep 0.5

# ------------------------------------------------------------------------
# Phase 3: kill -9 and measure on-disk size
# ------------------------------------------------------------------------

log "kill -9 PID=$SERVER_PID (ungraceful crash)"
kill -9 "$SERVER_PID" 2>/dev/null || true
wait "$SERVER_PID" 2>/dev/null || true

STATE_BYTES=$(du -sk "$DATA_DIR" 2>/dev/null | awk '{print $1 * 1024}')
echo "    State on disk: $STATE_BYTES bytes"

# macOS keeps SIGKILLed sockets in TIME_WAIT for ~60s. `lsof` without
# `-T` misses TIME_WAIT, so poll by actually trying to bind — the only
# reliable signal that the kernel has released the port for reuse.
# Excluded from the recovery-seconds measurement: this is an OS-specific
# socket-cleanup wait, not a beava-specific load-from-disk cost.
log "Waiting for TCP/HTTP ports to become bindable (TIME_WAIT drain)"
PORT_WAIT_START=$(date +%s)
while :; do
    if python3 - "$TCP_PORT" "$HTTP_PORT" <<'PY' 2>/dev/null
import socket, sys
for port in (int(sys.argv[1]), int(sys.argv[2])):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        s.bind(("127.0.0.1", port))
    except OSError:
        sys.exit(1)
    finally:
        s.close()
sys.exit(0)
PY
    then
        break
    fi
    if (( $(date +%s) - PORT_WAIT_START > 120 )); then
        warn "ports still in TIME_WAIT after 120s; restart may fail"
        break
    fi
    sleep 1
done
echo "    Ports bindable after $(( $(date +%s) - PORT_WAIT_START ))s"

# ------------------------------------------------------------------------
# Phase 4: restart and time to ready
# ------------------------------------------------------------------------

log "Restarting server on same data dir — timing to /debug/ready"
SERVER_PID=$(start_server "$SRV_RESTART_LOG")
DEADLINE=$(($(date +%s) + 60))
RECOVERY_SECONDS=$(wait_ready "$SERVER_PID" "$DEADLINE")
if [[ -z "$RECOVERY_SECONDS" ]]; then
    tail -60 "$SRV_RESTART_LOG"; fail "restart failed to reach /debug/ready" 1
fi
printf "    Recovery wall-clock: %.3fs\n" "$RECOVERY_SECONDS"

ENTITY_COUNT_POST=$(curl -sf "http://127.0.0.1:$HTTP_PORT/metrics" \
    | awk '/^beava_keys_total / {print $2}')
echo "    Entities after restart: $ENTITY_COUNT_POST"

kill "$SERVER_PID" 2>/dev/null || true

# ------------------------------------------------------------------------
# Phase 5: write summary
# ------------------------------------------------------------------------

python3 - "$SUMMARY_JSON" "$TS" "$TOTAL_EVENTS" "$STATE_BYTES" "$ENTITY_COUNT_PRE" "$ENTITY_COUNT_POST" "$READY_INIT" "$RECOVERY_SECONDS" "$SNAP_SECONDS" "$DURATION" "$CLIENTS" "$CPUS" <<'PY'
import json, socket, sys
(out, ts, events, state_bytes, ent_pre, ent_post, ready_init,
 recov, snap_s, duration, clients, cpus) = sys.argv[1:]
summary = {
    "timestamp": ts,
    "host": {"hostname": socket.gethostname(), "platform": sys.platform},
    "config": {
        "duration_seconds": int(duration),
        "clients": int(clients),
        "worker_threads": int(cpus),
    },
    "load": {
        "total_events": int(events),
        "state_bytes_on_disk": int(state_bytes),
        "entities_pre_crash": int(ent_pre),
    },
    "snapshot": {
        "forced_write_seconds": float(snap_s),
    },
    "recovery": {
        "initial_startup_seconds": float(ready_init),
        "recovery_seconds": float(recov),
        "entities_after_restart": int(ent_post),
        "entities_preserved_pct": round(100.0 * int(ent_post) / max(1, int(ent_pre)), 2),
    },
}
with open(out, "w") as fh:
    json.dump(summary, fh, indent=2)
PY

log "DONE"
echo "    summary:   $SUMMARY_JSON"
echo "    logs:      $SRV_LOG / $SRV_RESTART_LOG"
python3 -c "
import json
s = json.load(open('$SUMMARY_JSON'))
print()
print('    Events pushed:      ', f\"{s['load']['total_events']:,}\")
print('    State on disk:      ', f\"{s['load']['state_bytes_on_disk']/(1024*1024):.1f} MB\")
print('    Entities pre-crash: ', f\"{s['load']['entities_pre_crash']:,}\")
print('    Forced snapshot:    ', f\"{s['snapshot']['forced_write_seconds']:.3f}s\")
print('    Initial startup:    ', f\"{s['recovery']['initial_startup_seconds']:.3f}s (empty state)\")
print('    Recovery wall-clock:', f\"{s['recovery']['recovery_seconds']:.3f}s\")
print('    Entities preserved: ', f\"{s['recovery']['entities_preserved_pct']:.1f}% ({s['recovery']['entities_after_restart']:,} / {s['load']['entities_pre_crash']:,})\")
"
