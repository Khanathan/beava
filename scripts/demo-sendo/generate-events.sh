#!/usr/bin/env bash
# generate-events.sh — produce Sendo Farm-style event JSONL for the demo.
#
# Thin wrapper around generate-events.py. Idempotent: skips regeneration
# if events.jsonl already has at least MIN_EVENTS lines.
#
# Output: scripts/demo-sendo/events.jsonl (~220 MB at the default 2M events)

set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OUT="${HERE}/events.jsonl"
MIN_EVENTS="${MIN_EVENTS:-1500000}"

if [[ -f "${OUT}" ]]; then
    have=$(wc -l < "${OUT}" | tr -d ' ')
    if [[ "${have}" -ge "${MIN_EVENTS}" ]]; then
        echo "events.jsonl already has ${have} lines (>= ${MIN_EVENTS}); skipping generation."
        exit 0
    fi
    echo "events.jsonl exists but only has ${have} lines; regenerating."
fi

python3 "${HERE}/generate-events.py"

lines=$(wc -l < "${OUT}" | tr -d ' ')
size=$(du -h "${OUT}" | cut -f1)
echo "events.jsonl: ${lines} lines (${size})"
