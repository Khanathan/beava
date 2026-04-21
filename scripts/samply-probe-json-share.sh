#!/usr/bin/env bash
# samply-probe-json-share.sh — Phase 59 perf gate helper (D-D3)
#
# Runs the pprof/samply profiling harness (tests/profile_ingest.rs) and
# reports the combined percentage of leaf samples spent in
# `serde_json::*`, `std::str::from_utf8`, and `format_escaped_str` frames.
# Used by tests/binary_push_bytes_passthrough.rs (RED) and Wave 4's
# samply re-run to measure the JSON-share ceiling after binary-passthrough
# lands.
#
# Phase 59 gate (TPC-PERF-09 D-D3):
#   JSON_SHARE_PCT ≤ 3.0
#
# Baseline (pre-Wave-1, per Phase 58 pprof notes): ~8% (to_vec 4.5% +
# from_slice 3.5%, combined with from_utf8).
#
# Usage:
#   scripts/samply-probe-json-share.sh [--duration-s N] [--shards N]
#   scripts/samply-probe-json-share.sh --help
#
# Output (machine-parseable LAST line):
#   JSON_SHARE_PCT=1.7
#   JSON_SHARE_PCT=unknown          # samply CLI absent or harness unable.
#   JSON_SHARE_PCT=SENTINEL_FAILED  # coverage sentinel: top.txt lacks the
#                                   # push-path frames we expect. Exits 2
#                                   # to force Wave 1+ to extend harness
#                                   # so gate is load-bearing (mirrors
#                                   # Phase 58 SC-1 harness-unable case).
#
# Exit codes:
#   0 — probe ran (pct printed OR `unknown` with samply-missing warning).
#   1 — profile harness failed / parse error / other hard failure.
#   2 — coverage sentinel failed (harness ran but did not exercise the
#       TCP push path — top.txt lacks handle_push_batch|handle_push_core|
#       decode_event_binary frames; a naive ≤ 3% gate would false-pass).
#
# Dependencies:
#   - `cargo` (rust toolchain).
#   - `samply` (install via `cargo install samply`). If absent, the script
#     warns once and emits JSON_SHARE_PCT=unknown with exit 0; the smoke
#     test then panics with an actionable "install samply" message.
#
# Notes:
#   - The harness under `tests/profile_ingest.rs` today uses pprof-rs and
#     emits `/tmp/beava_ingest.top.txt`. Wave 0 coverage sentinel floors
#     the read: if top.txt does not contain at least one push-path frame,
#     we emit SENTINEL_FAILED + exit 2 to prevent silent false-pass.
#   - This probe is operator-side / bench-only. Never runs in the
#     production push path.

set -uo pipefail

SCRIPT_NAME="samply-probe-json-share"
REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TOP_TXT="/tmp/beava_ingest.top.txt"

DURATION_S="${DURATION_S:-8}"   # matches tests/profile_ingest.rs default
SHARDS="${SHARDS:-8}"

# ---- arg parsing ------------------------------------------------------
while [[ $# -gt 0 ]]; do
    case "$1" in
        -h|--help)
            cat <<EOF
$SCRIPT_NAME — Phase 59 TPC-PERF-09 probe helper.

Usage:
  scripts/samply-probe-json-share.sh [--duration-s N] [--shards N]
  scripts/samply-probe-json-share.sh --help

Invokes the profile_ingest harness, parses serde_json::* / from_utf8 /
format_escaped_str self-sample percentages, and prints a machine-parseable
final line:

  JSON_SHARE_PCT=<float>            (e.g. JSON_SHARE_PCT=1.7)
  JSON_SHARE_PCT=unknown            (samply absent or harness inconclusive)
  JSON_SHARE_PCT=SENTINEL_FAILED    (coverage sentinel: harness did not
                                     exercise the TCP push path)

Phase 59 gate (TPC-PERF-09 / D-D3):  JSON_SHARE_PCT <= 3.0
Baseline (pre-Wave-1): ~8 % per Phase 58 pprof breakdown.

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
# Phase 59 Wave 0: probe script is allowed to emit `unknown` (exit 0) if
# samply is missing. The RED smoke test handles that as an actionable
# panic, NOT a silent pass.
if ! command -v samply >/dev/null 2>&1; then
    echo "[$SCRIPT_NAME] WARN: 'samply' CLI not found in PATH." >&2
    echo "[$SCRIPT_NAME] Install via: cargo install samply" >&2
    echo "[$SCRIPT_NAME] Emitting JSON_SHARE_PCT=unknown (harness did not run)." >&2
    echo "JSON_SHARE_PCT=unknown"
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
    echo "JSON_SHARE_PCT=unknown"
    exit 0
fi

# ---- coverage sentinel (D-D1, adopted from Phase 58 58-NEXT #1) -------
# If top.txt does not contain a push-path frame, the harness ran but did
# not exercise the TCP push path — a naive ≤ 3% gate would false-pass on
# pct=0.0. Emit SENTINEL_FAILED + exit 2 to force harness extension before
# the ceiling gate activates.
if ! grep -qE 'handle_push_batch|handle_push_core|decode_event_binary' "$TOP_TXT"; then
    echo "[$SCRIPT_NAME] WARN: coverage sentinel FAILED — $TOP_TXT lacks push-path frames." >&2
    echo "[$SCRIPT_NAME] Wave 1+ must extend profile_ingest (or add sibling harness) to drive a real TCP push." >&2
    echo "JSON_SHARE_PCT=SENTINEL_FAILED"
    exit 2
fi

# ---- extract JSON-share (serde_json::* + from_utf8 + format_escaped_str)
# top.txt lines look like (whitespace-delimited):
#    <self_pct>  <inclusive_pct>  <symbol-path>
# We sum the self_pct column of all lines whose symbol contains
# `serde_json::` OR `std::str::from_utf8` OR `format_escaped_str`.

PCT="$(
    awk '
        BEGIN { sum = 0.0; in_leaf_section = 0 }
        # Phase 59 Wave 4 fix: top.txt has TWO sections — leaf + inclusive.
        # Only sum leaf samples (D-D3 compares against the leaf ≤ 3% target);
        # inclusive would double-count callers-of-serde. Also must match
        # ONLY percent columns (value ending in "%"), not the raw-sample
        # column which has no suffix and would false-match the regex.
        /^## Top .* leaf functions/ { in_leaf_section = 1; next }
        /^## Top .* inclusive/ { in_leaf_section = 0; next }
        (in_leaf_section == 1) && /serde_json::|std::str::from_utf8|format_escaped_str/ {
            for (i = 1; i <= NF; i++) {
                # Require a trailing "%" to pin to the percent column.
                if ($i ~ /^-?[0-9]+(\.[0-9]+)?%$/) {
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
    echo "JSON_SHARE_PCT=unknown"
    exit 0
fi

# Machine-parseable final line.
echo "JSON_SHARE_PCT=$PCT"
exit 0
