#!/usr/bin/env bash
# Fork-replay benchmark.
#
# Flow:
#   1. Start an upstream beava server (snapshot + event log on).
#   2. Register the `Event` stream + `UserCounts` table, push events at
#      RATE events/sec for DURATION seconds via push_rate.py.
#   3. Spawn `bv.fork(block_until_catchup=true)` via fork_driver.py and
#      time the wait-for-ready. That's the catchup wall-clock.
#   4. Report replay EPS = events_pushed / catchup_seconds.
#   5. Cross-check `beava_keys_total` on fork vs. expected entity count.
#
# Environment variables (all optional):
#   RATE       events/sec (default 1000)
#   DURATION   push seconds (default 30)
#   ENTITIES   distinct user_ids (default 1000)
#   CPUS       server worker threads (default min(host, 8))
#   TCP_PORT   upstream TCP port (default 6800)
#   HTTP_PORT  upstream HTTP port (default 6801)
#   FORK_PORT  fork TCP port (HTTP = FORK_PORT+1; default 7400)
#   BEAVA_BIN  override binary path
#   SKIP_BUILD set to 1 to skip cargo build

set -uo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
PUSH_PY="$REPO/benchmark/fork-replay/push_rate.py"
FORK_PY="$REPO/benchmark/fork-replay/fork_driver.py"

RATE="${RATE:-1000}"
DURATION="${DURATION:-30}"
ENTITIES="${ENTITIES:-1000}"
# Optional event-count cap. When TARGET_EVENTS is set, push_rate.py stops
# once N events have been pushed (DURATION remains an upper bound). Pair
# with RATE=0 for "push 5 M events as fast as this single Python client
# can manage". At 5 M events and ~40 K single-client EPS, expect ~2 min.
TARGET_EVENTS="${TARGET_EVENTS:-0}"
TCP_PORT="${TCP_PORT:-6800}"
HTTP_PORT="${HTTP_PORT:-6801}"
# `beava fork` convention: --local-port is the HTTP port; TCP is HTTP+1.
# We expose FORK_HTTP_PORT as the configurable knob since that is what
# fork_driver.py and /metrics queries land on.
FORK_HTTP_PORT="${FORK_HTTP_PORT:-7400}"
FORK_TCP_PORT=$((FORK_HTTP_PORT + 1))
SKIP_BUILD="${SKIP_BUILD:-0}"
ADMIN_TOKEN="${BEAVA_ADMIN_TOKEN:-dev-admin-token}"

if   _c=$(sysctl -n hw.ncpu 2>/dev/null); then CPUS_HOST=$_c
elif _c=$(nproc 2>/dev/null);              then CPUS_HOST=$_c
else CPUS_HOST=4; fi
CPUS="${CPUS:-$(( CPUS_HOST < 8 ? CPUS_HOST : 8 ))}"

TS="$(date -u +%Y%m%dT%H%M%SZ)"
RESULTS_DIR="$REPO/benchmark/fork-replay/results/$TS"
mkdir -p "$RESULTS_DIR"
STDOUT_LOG="$RESULTS_DIR/stdout.log"
SUMMARY_JSON="$RESULTS_DIR/replay_summary.json"

exec > >(tee -a "$STDOUT_LOG") 2>&1

log()  { printf '\n\033[1m==> %s\033[0m\n' "$*"; }
warn() { printf '\033[33m[warn]\033[0m %s\n' "$*" >&2; }
fail() { printf '\033[31m[fail]\033[0m %s\n' "$*" >&2; exit "${2:-1}"; }

resolve_bin() {
    if [[ -n "${BEAVA_BIN:-}" && -x "$BEAVA_BIN" ]]; then echo "$BEAVA_BIN"; return; fi
    for c in "$REPO/target/release/beava" "$REPO/target/release/tally"; do
        [[ -x "$c" ]] && { echo "$c"; return; }
    done
    echo ""
}

log "Fork-replay benchmark — $(date -u +%Y-%m-%d\ %H:%M:%SZ)"
echo "Host: $(uname -sm), $CPUS_HOST cores"
echo "Upstream: RATE=$RATE eps, DURATION=${DURATION}s, ENTITIES=$ENTITIES"
echo "Ports: upstream TCP=$TCP_PORT HTTP=$HTTP_PORT, fork HTTP=$FORK_HTTP_PORT TCP=$FORK_TCP_PORT"
echo "Results: $RESULTS_DIR"

BIN="$(resolve_bin)"
if [[ -z "$BIN" && "$SKIP_BUILD" != "1" ]]; then
    log "Building release binary..."
    (cd "$REPO" && cargo build --release --bin beava) || fail "cargo build failed" 2
    BIN="$(resolve_bin)"
fi
[[ -z "$BIN" ]] && fail "beava binary not found" 2
echo "Binary: $BIN"

for p in "$TCP_PORT" "$HTTP_PORT" "$FORK_HTTP_PORT" "$FORK_TCP_PORT"; do
    if lsof -i ":$p" >/dev/null 2>&1; then
        fail "port $p already in use" 3
    fi
done

DATA_DIR="$(mktemp -d -t beava-replay-upstream.XXXXXX)"
UP_LOG="$RESULTS_DIR/upstream.log"

cleanup() {
    pkill -9 -P $$ 2>/dev/null || true
    if [[ -n "${UPSTREAM_PID:-}" ]] && kill -0 "$UPSTREAM_PID" 2>/dev/null; then
        kill "$UPSTREAM_PID" 2>/dev/null || true
    fi
    rm -rf "$DATA_DIR"
}
trap cleanup EXIT INT TERM

# ------------------------------------------------------------------------
# 1. Start upstream
# ------------------------------------------------------------------------

log "Starting upstream server"
BEAVA_ADMIN_TOKEN="$ADMIN_TOKEN" \
BEAVA_WORKER_THREADS="$CPUS" \
BEAVA_TCP_PORT="$TCP_PORT" \
BEAVA_HTTP_PORT="$HTTP_PORT" \
BEAVA_DATA_DIR="$DATA_DIR" \
BEAVA_SNAPSHOT_PATH="$DATA_DIR/beava.snapshot" \
BEAVA_EVENT_LOG=1 \
BEAVA_SNAPSHOT=1 \
    "$BIN" > "$UP_LOG" 2>&1 &
UPSTREAM_PID=$!

DEADLINE=$(($(date +%s) + 15))
while ! curl -sf "http://127.0.0.1:$HTTP_PORT/debug/ready" >/dev/null 2>&1; do
    if ! kill -0 "$UPSTREAM_PID" 2>/dev/null; then
        tail -30 "$UP_LOG"; fail "upstream died during readiness poll" 1
    fi
    if (( $(date +%s) > DEADLINE )); then
        tail -30 "$UP_LOG"; fail "upstream not ready within 15s" 1
    fi
    sleep 0.1
done
echo "Upstream ready (PID=$UPSTREAM_PID)"

# ------------------------------------------------------------------------
# 2. Register + push at rate
# ------------------------------------------------------------------------

log "Pushing events at ${RATE} eps for ${DURATION}s"
PUSH_JSON=$(python3 "$PUSH_PY" \
    --host "localhost:$TCP_PORT" \
    --rate "$RATE" \
    --duration "$DURATION" \
    --target-events "$TARGET_EVENTS" \
    --entities "$ENTITIES" \
    --register 2>> "$STDOUT_LOG")
echo "push result: $PUSH_JSON"
EVENTS_PUSHED=$(python3 -c "import json,sys; print(json.loads(sys.argv[1])['events_pushed'])" "$PUSH_JSON")
PUSH_WALL=$(python3 -c "import json,sys; print(json.loads(sys.argv[1])['wall_seconds'])" "$PUSH_JSON")

UPSTREAM_KEYS=$(curl -sf "http://127.0.0.1:$HTTP_PORT/metrics" | awk '/^beava_keys_total / {print $2}')
echo "Upstream beava_keys_total: $UPSTREAM_KEYS"

# ------------------------------------------------------------------------
# 3. Spawn fork, time catchup
# ------------------------------------------------------------------------

log "Spawning fork with block_until_catchup=true"
BEAVA_REPLICA_TOKEN="$ADMIN_TOKEN" python3 "$FORK_PY" \
    --remote "localhost:$TCP_PORT" \
    --local-port "$FORK_HTTP_PORT" \
    --token "$ADMIN_TOKEN" \
    --key-prefix "u" \
    > "$RESULTS_DIR/fork_raw.json" 2>> "$STDOUT_LOG"
FORK_JSON=$(cat "$RESULTS_DIR/fork_raw.json")
echo "fork result: $FORK_JSON"
CATCHUP_S=$(python3 -c "import json,sys; print(json.loads(sys.argv[1])['catchup_seconds'])" "$FORK_JSON")
FORK_KEYS=$(python3 -c "import json,sys; print(json.loads(sys.argv[1])['keys_total'])" "$FORK_JSON")

REPLAY_EPS=$(python3 -c "print(int(float('$EVENTS_PUSHED')/max(0.001,float('$CATCHUP_S'))))")

# ------------------------------------------------------------------------
# 4. Summary
# ------------------------------------------------------------------------

python3 - "$SUMMARY_JSON" "$TS" "$EVENTS_PUSHED" "$PUSH_WALL" "$CATCHUP_S" "$REPLAY_EPS" "$UPSTREAM_KEYS" "$FORK_KEYS" "$RATE" "$DURATION" "$ENTITIES" "$RESULTS_DIR/fork_raw.json" <<'PY'
import json, socket, sys
(out, ts, events, pw, cs, reps, up_keys, fork_keys, rate, dur, ents, fork_raw) = sys.argv[1:]
# Pull the feature-diff block that fork_driver emitted. It's the proof
# that catchup is semantically correct, not just entity-count-matching.
feature_diff = None
try:
    with open(fork_raw) as fh:
        feature_diff = json.load(fh).get("feature_diff")
except Exception:
    pass
summary = {
    "timestamp": ts,
    "host": {"hostname": socket.gethostname(), "platform": sys.platform},
    "config": {"target_rate": int(rate), "duration_seconds": int(dur), "entities": int(ents)},
    "push": {"events_pushed": int(events), "wall_seconds": float(pw),
             "achieved_eps": round(int(events)/max(0.001, float(pw)), 1),
             "upstream_keys_total": int(up_keys)},
    "replay": {"catchup_seconds": float(cs), "events_replayed": int(events),
               "replay_eps": int(reps), "fork_keys_total": int(fork_keys),
               "keys_preserved_pct": round(100.0 * int(fork_keys) / max(1, int(up_keys)), 2),
               "feature_diff": feature_diff},
}
with open(out, "w") as fh:
    json.dump(summary, fh, indent=2)
PY

log "DONE"
cat "$SUMMARY_JSON"
