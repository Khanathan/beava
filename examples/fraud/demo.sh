#!/usr/bin/env bash
# Beava fraud-detection demo — the 60-second flow.
#
#   1. Make sure a Beava server is running on localhost:6400 / :6401
#      (starts one via `docker compose up -d` if not).
#   2. Register the UserFeatures pipeline from pipeline.json.
#   3. Push 200 hand-crafted transaction events via the Python SDK.
#   4. Query u123's real-time features and print them.
#
# Usage: bash examples/fraud/demo.sh

set -euo pipefail

# Resolve paths so this script works from any cwd.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

TCP_PORT="${BEAVA_TCP_PORT:-6400}"
HTTP_PORT="${BEAVA_HTTP_PORT:-6401}"
HTTP="http://localhost:${HTTP_PORT}"

step() { printf "\n\033[1;34m==>\033[0m %s\n" "$*"; }
info() { printf "    %s\n" "$*"; }
die()  { printf "\033[1;31merror:\033[0m %s\n" "$*" >&2; exit 1; }

# ---------------------------------------------------------------------------
# 1. Make sure Beava is up.
# ---------------------------------------------------------------------------
step "Checking Beava server at ${HTTP}/health"

if curl -fsS "${HTTP}/health" >/dev/null 2>&1; then
    info "server is already running."
else
    info "server not reachable — trying 'docker compose up -d'..."
    (cd "${REPO_ROOT}" && docker compose up -d) \
        || die "failed to start server via docker compose (is Docker running?)"
    info "waiting for server to come up..."
    for i in {1..60}; do
        if curl -fsS "${HTTP}/health" >/dev/null 2>&1; then
            info "server is up."
            break
        fi
        sleep 1
        if [[ $i -eq 60 ]]; then
            die "server did not start within 60s (check 'docker compose logs')"
        fi
    done
fi

# ---------------------------------------------------------------------------
# 2. Register the pipeline + push events via the Python SDK.
# ---------------------------------------------------------------------------
# ``push_events.py`` does both: it reads ``pipeline.json`` and sends an
# OP_REGISTER frame over TCP, then pushes every event in
# ``sample_events.jsonl``. We route registration through TCP instead of
# HTTP POST /pipelines because the HTTP admin gate requires a loopback
# peer IP or an admin token — and the Docker bridge gateway (seen by the
# server inside the container) is neither. TCP has no such gate, so the
# demo works unchanged whether you're running bare-metal or via compose.

PYCMD="$(command -v python3 || command -v python || true)"
[[ -n "${PYCMD}" ]] || die "no python3 on PATH (install Python 3.10+)"

if ! "${PYCMD}" -c "import beava" 2>/dev/null; then
    info "beava not installed — using the repo copy at python/"
    export PYTHONPATH="${REPO_ROOT}/python${PYTHONPATH:+:${PYTHONPATH}}"
fi

step "Registering UserFeatures pipeline (from pipeline.json)"
info "$(head -c 200 "${SCRIPT_DIR}/pipeline.json" | tr -s ' ' | tr -d '\n' | head -c 180)..."

step "Pushing 200 sample events to stream UserFeatures (tcp ${TCP_PORT})"
"${PYCMD}" "${SCRIPT_DIR}/push_events.py" --url "localhost:${TCP_PORT}"

# ---------------------------------------------------------------------------
# 4. Query u123 features — the aha moment.
# ---------------------------------------------------------------------------
step "Fetching features for u123"

FEATURES="$(curl -fsS "${HTTP}/public/features/u123")"

# Pretty-print the handful of features we care about. Uses python to format
# so we don't hard-depend on jq.
"${PYCMD}" - <<PY
import json, sys
data = json.loads('''${FEATURES}''')
f = data.get("features", {})
def show(label, key, fmt="{}"):
    v = f.get(key)
    if v is None:
        val = "(missing)"
    elif isinstance(v, float):
        val = fmt.format(v)
    else:
        val = fmt.format(v)
    print(f"    {label:<24} {val}")

print()
print(f"    user_id                  {data.get('key')}")
show("tx_count_1h",      "tx_count_1h")
show("tx_sum_1h",        "tx_sum_1h",     "\$ {:,.2f}")
show("avg_amount",       "avg_amount",    "\$ {:,.2f}")
show("max_amount_1h",    "max_amount_1h", "\$ {:,.2f}")
show("unique_merchants", "unique_merchants", "{:.0f}")
show("last_merchant",    "last_merchant")
show("last_amount",      "last_amount",   "\$ {:,.2f}")
print()
PY

step "Poke around"
info "  Public features:  ${HTTP}/public/features/u123"
info "  Debug (loopback): ${HTTP}/debug/key/u123"
info "  Pipelines list:   ${HTTP}/pipelines"
info "  Memory rollup:    ${HTTP}/debug/memory"
echo
echo "  See examples/fraud/README.md for what just happened."
echo
