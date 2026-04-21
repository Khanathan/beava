#!/usr/bin/env bash
# samply-probe-tokio-share.sh — Phase 58 perf gate helper (D-C4)
#
# Runs the pprof/samply profiling harness (tests/profile_ingest.rs) and
# reports the percentage of leaf samples spent in `tokio::runtime::task::*`
# frames. Used by tests/tokio_spawn_absence_smoke.rs (RED → GREEN across
# Phase 58) and by Wave 4's samply re-run.
#
# Phase 58 gate (TPC-PERF-08 D-C4):
#   TOKIO_SHARE_PCT ≤ 15.0
#
# Baseline (pre-Wave-1, per Phase 54 pprof notes): ~60 %.
#
# Usage:
#   scripts/samply-probe-tokio-share.sh [--duration-s N] [--shards N]
#   scripts/samply-probe-tokio-share.sh --help
#
# Output (machine-parseable LAST line):
#   TOKIO_SHARE_PCT=12.3
#   TOKIO_SHARE_PCT=unknown       # samply CLI absent — test harness treats
#                                 # this as a hard skip with an install hint.
#
# Exit codes:
#   0 — probe ran (pct printed OR `unknown` with samply-missing warning).
#   1 — profile harness failed / parse error / other hard failure.
#
# Dependencies:
#   - `cargo` (rust toolchain).
#   - `samply` (install via `cargo install samply`). If absent, the script
#     warns once and emits TOKIO_SHARE_PCT=unknown with exit 0; the smoke
#     test then panics with an actionable "install samply" message.
#
# Notes:
#   - The harness under `tests/profile_ingest.rs` today uses pprof-rs and
#     emits `/tmp/beava_ingest.top.txt` (a per-symbol self-sample dump).
#     We grep THAT file for `tokio::runtime::task` substrings and sum the
#     self-sample percentages. This keeps the gate functional pre-samply-
#     migration; Wave 4 may re-implement against native `samply record`
#     output when available.
#   - This probe is operator-side / bench-only. It never runs in the
#     production push path. (T-58-00-02 disposition: accept.)

set -uo pipefail

SCRIPT_NAME="samply-probe-tokio-share"
REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TOP_TXT="/tmp/beava_ingest.top.txt"

DURATION_S="${DURATION_S:-8}"   # matches tests/profile_ingest.rs default
SHARDS="${SHARDS:-8}"

# ---- arg parsing ------------------------------------------------------
while [[ $# -gt 0 ]]; do
    case "$1" in
        -h|--help)
            cat <<EOF
$SCRIPT_NAME — Phase 58 TPC-PERF-08 probe helper.

Usage:
  scripts/samply-probe-tokio-share.sh [--duration-s N] [--shards N]
  scripts/samply-probe-tokio-share.sh --help

Invokes the profile_ingest harness, parses tokio::runtime::task self-sample
percentages, and prints a machine-parseable final line:

  TOKIO_SHARE_PCT=<float>     (e.g. TOKIO_SHARE_PCT=12.3)
  TOKIO_SHARE_PCT=unknown     (samply absent or harness inconclusive)

Phase 58 gate (TPC-PERF-08 / D-C4):  TOKIO_SHARE_PCT <= 15.0
Baseline (pre-Wave-1): ~60 % per Phase 54 pprof.

Env:
  DURATION_S   profile duration seconds      (default: 8)
  SHARDS       BEAVA_SHARDS for the harness   (default: 8)
EOF
            exit 0
            ;;
        --duration-s)
            DURATION_S="$2"; shift 2
            ;;
        --shards)
            SHARDS="$2"; shift 2
            ;;
        *)
            echo "$SCRIPT_NAME: unknown argument '$1' (try --help)" >&2
            exit 1
            ;;
    esac
done

cd "$REPO"

# ---- samply presence check -------------------------------------------
# Phase 58 Wave 0: probe script is allowed to emit `unknown` (exit 0) if
# samply is missing. The RED smoke test handles that as an actionable
# panic, NOT a silent pass.
if ! command -v samply >/dev/null 2>&1; then
    echo "[$SCRIPT_NAME] WARN: 'samply' CLI not found in PATH." >&2
    echo "[$SCRIPT_NAME] Install via: cargo install samply" >&2
    echo "[$SCRIPT_NAME] Emitting TOKIO_SHARE_PCT=unknown (harness did not run)." >&2
    echo "TOKIO_SHARE_PCT=unknown"
    exit 0
fi

# ---- run the profile harness ------------------------------------------
# The harness writes /tmp/beava_ingest.top.txt. Delete any stale file so
# a harness failure can't mask a previous run's numbers.
rm -f "$TOP_TXT"

export BEAVA_SHARDS="$SHARDS"
export BEAVA_SHARD_INBOX_SIZE="${BEAVA_SHARD_INBOX_SIZE:-1048576}"

echo "[$SCRIPT_NAME] running profile_ingest harness (shards=$SHARDS, duration~${DURATION_S}s)..." >&2

if ! cargo test --release --test profile_ingest \
        -- --ignored --nocapture profile_ingest_hot_path >/tmp/beava_ingest.probe.log 2>&1; then
    echo "[$SCRIPT_NAME] ERROR: profile_ingest harness failed. See /tmp/beava_ingest.probe.log" >&2
    tail -40 /tmp/beava_ingest.probe.log >&2 || true
    exit 1
fi

if [[ ! -s "$TOP_TXT" ]]; then
    echo "[$SCRIPT_NAME] WARN: $TOP_TXT missing or empty after harness run." >&2
    echo "TOKIO_SHARE_PCT=unknown"
    exit 0
fi

# ---- extract tokio::runtime::task self-sample share -------------------
# top.txt lines look like (whitespace-delimited):
#    <self_pct>  <inclusive_pct>  <symbol-path>
# We sum the self_pct column of all lines whose symbol contains
# `tokio::runtime::task`. Robust to pprof/samply formatting variants:
# we also tolerate a leading rank column.

PCT="$(
    awk '
        BEGIN { sum = 0.0 }
        /tokio::runtime::task/ {
            # Find the first numeric-looking column; treat it as self%.
            for (i = 1; i <= NF; i++) {
                if ($i ~ /^-?[0-9]+(\.[0-9]+)?%?$/) {
                    v = $i; gsub("%", "", v);
                    sum += v + 0.0;
                    break;
                }
            }
        }
        END { printf "%.1f\n", sum }
    ' "$TOP_TXT"
)"

# Defensive: if awk produced an empty or non-numeric result, emit unknown.
if [[ -z "$PCT" || ! "$PCT" =~ ^-?[0-9]+(\.[0-9]+)?$ ]]; then
    echo "[$SCRIPT_NAME] WARN: could not parse self-sample % from $TOP_TXT." >&2
    echo "TOKIO_SHARE_PCT=unknown"
    exit 0
fi

# Machine-parseable final line.
echo "TOKIO_SHARE_PCT=$PCT"
exit 0
