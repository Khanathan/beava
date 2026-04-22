#!/usr/bin/env bash
# setup.sh — one-shot prep for the Sendo demo recording.
#
# Idempotent: re-running after success finishes in under ~30s.
#
# Steps:
#   1. preflight — check docker, python, curl, jq, disk, RAM
#   2. build     — docker build -t beavadb/beava:latest .
#   3. venv      — create scripts/demo-sendo/.venv, install httpx etc.
#   4. data      — call download-otto.sh unless events.jsonl is ready
#   5. smoke     — start container, hit /health, stop container
#   6. print     — next-step instructions (record, verify, VN script)

set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${HERE}/../.." && pwd)"
VENV="${HERE}/.venv"
IMAGE="beavadb/beava:latest"

bold() { printf "\033[1m%s\033[0m\n" "$*"; }
green() { printf "\033[32m%s\033[0m\n" "$*"; }
red() { printf "\033[31m%s\033[0m\n" "$*" >&2; }
step() { printf "\n\033[1;36m▸ %s\033[0m\n" "$*"; }

# ──────────────────────────────────────────────────────────────────────────
# 1. Preflight
# ──────────────────────────────────────────────────────────────────────────
step "1/5  Preflight checks"

missing=0
need() {
    if ! command -v "$1" >/dev/null 2>&1; then
        red "  ✗ missing: $1 — install hint: $2"
        missing=1
    else
        green "  ✓ $1"
    fi
}

need docker "https://www.docker.com/products/docker-desktop/"
need python3 "brew install python@3.12   (or use uv)"
need curl "pre-installed on macOS/Linux"
need jq "brew install jq   /   apt-get install jq"
need tar "pre-installed"

# Python >= 3.10
py_ver="$(python3 -c 'import sys;print("%d.%d"%sys.version_info[:2])')"
py_major="${py_ver%.*}"
py_minor="${py_ver#*.}"
if [[ "${py_major}" -lt 3 || ( "${py_major}" -eq 3 && "${py_minor}" -lt 10 ) ]]; then
    red "  ✗ python ${py_ver} found; need >= 3.10"
    missing=1
else
    green "  ✓ python ${py_ver}"
fi

# Disk >= 8 GB free
if command -v df >/dev/null; then
    free_gb=$(df -g "${REPO_ROOT}" | awk 'NR==2 {print $4}' 2>/dev/null || echo 0)
    if [[ -n "${free_gb}" && "${free_gb}" -lt 8 ]]; then
        red "  ✗ only ${free_gb} GB free; need >= 8"
        missing=1
    else
        green "  ✓ disk ${free_gb} GB free"
    fi
fi

# Docker daemon reachable
if ! docker info >/dev/null 2>&1; then
    red "  ✗ docker daemon not running — open Docker Desktop"
    missing=1
else
    green "  ✓ docker daemon up"
fi

if [[ "${missing}" -ne 0 ]]; then
    red ""
    red "Fix the items above, then re-run: bash scripts/demo-sendo/setup.sh"
    exit 1
fi

# ──────────────────────────────────────────────────────────────────────────
# 2. Build image
# ──────────────────────────────────────────────────────────────────────────
step "2/5  Build Docker image  (${IMAGE})"

if docker image inspect "${IMAGE}" >/dev/null 2>&1; then
    bold "  image already built — skipping"
    bold "  (force rebuild: docker rmi ${IMAGE})"
else
    ( cd "${REPO_ROOT}" && docker build -t "${IMAGE}" . )
fi

# ──────────────────────────────────────────────────────────────────────────
# 3. Python venv
# ──────────────────────────────────────────────────────────────────────────
step "3/5  Python venv + dependencies"

if [[ ! -d "${VENV}" ]]; then
    python3 -m venv "${VENV}"
fi
# shellcheck source=/dev/null
source "${VENV}/bin/activate"
python3 -m pip install --quiet --upgrade pip
python3 -m pip install --quiet httpx tqdm
# Install the local Beava Python SDK so pipeline.py can `import beava as bv`.
python3 -m pip install --quiet -e "${REPO_ROOT}/python"
green "  ✓ venv ready at ${VENV}"

# ──────────────────────────────────────────────────────────────────────────
# 4. Synthesized events
# ──────────────────────────────────────────────────────────────────────────
step "4/5  Sendo-Farm-style events → events.jsonl"

bash "${HERE}/generate-events.sh"

# ──────────────────────────────────────────────────────────────────────────
# 5. Smoke test
# ──────────────────────────────────────────────────────────────────────────
step "5/5  Smoke test  (start container, GET /health, stop)"

CID=""
cleanup() { [[ -n "${CID}" ]] && docker rm -f "${CID}" >/dev/null 2>&1 || true; }
trap cleanup EXIT

CID="$(docker run -d --rm -p 6900:6900 -p 6400:6400 "${IMAGE}")"

ok=0
for i in $(seq 1 20); do
    if curl -sf "http://localhost:6900/health" >/dev/null 2>&1; then
        ok=1
        break
    fi
    sleep 0.5
done

if [[ "${ok}" -ne 1 ]]; then
    red "  ✗ /health did not respond within 10 s"
    docker logs "${CID}" | tail -50 >&2
    exit 1
fi

green "  ✓ server reachable on :6900"
docker rm -f "${CID}" >/dev/null
CID=""

# ──────────────────────────────────────────────────────────────────────────
# Done
# ──────────────────────────────────────────────────────────────────────────
cat <<EOF

$(bold "✅ Setup complete.")

Next steps (in order):

  1. Verify the claimed numbers on this machine:
       bash scripts/demo-sendo/verify.sh

     Any number that comes back worse than the script claim, edit
     SENDO-DEMO-VI.md to the true measured value BEFORE recording.

  2. Read the Vietnamese script:
       open SENDO-DEMO-VI.md     # macOS
       xdg-open SENDO-DEMO-VI.md # Linux

  3. When you are ready to record, in three terminals:
       # terminal A — the server
       docker run -p 6900:6900 -p 6400:6400 ${IMAGE}

       # terminal B — register the pipeline (Scene 3)
       source scripts/demo-sendo/.venv/bin/activate
       python scripts/demo-sendo/pipeline.py

       # terminal C — the load test (Scene 4)
       cat scripts/demo-sendo/events.jsonl | \\
         python scripts/demo-sendo/beava-bench.py \\
           --rate 10000 \\
           --to http://localhost:6900/push-batch/events \\
           --duration 60

  4. Rehearse 3× before Take 1.

EOF
