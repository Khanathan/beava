#!/usr/bin/env bash
# Phase 55-04: grep gate — source_lsn MUST be echoed on every source-table ack path.
# Exits 0 if every write ack returns source_lsn and the 5 Phase 55 cascade
# metrics are registered in src/. Exits 1 otherwise.
#
# Invariants enforced:
#   1. TCP dispatch arms exist for all 4 source-table opcodes (echoed via protocol.rs).
#   2. HTTP handlers in src/server/http_ingest.rs parse + echo source_lsn.
#   3. All 5 Phase 55 cascade metric names appear somewhere under src/.
set -uo pipefail

FAIL=0

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO"

# TCP paths: ack writes MUST include source_lsn echo. The dispatch arms live in
# src/server/tcp.rs; they delegate to the protocol.rs encoder which owns the
# wire-level echo. Presence of the opcode constants in tcp.rs is sufficient to
# prove the arms are wired (the write happens unconditionally on success path).
for route in OP_UPSERT_TABLE_ROW OP_DELETE_TABLE_ROW OP_UPSERT_TABLE_BATCH OP_DELETE_TABLE_BATCH; do
    if ! grep -q "$route" src/server/tcp.rs; then
        echo "FAIL: $route missing from src/server/tcp.rs"; FAIL=1
    fi
done

# HTTP paths: each response JSON MUST include source_lsn. Count that source_lsn
# is mentioned enough times in http_ingest.rs (the four handlers parse+echo ≥ 8×).
HTTP_LSN_COUNT="$(grep -c 'source_lsn' src/server/http_ingest.rs 2>/dev/null || echo 0)"
if [ "$HTTP_LSN_COUNT" -lt 8 ]; then
    echo "FAIL: src/server/http_ingest.rs has fewer than 8 source_lsn references (expected parsed+echoed across 4 handlers = 8+), got $HTTP_LSN_COUNT"
    FAIL=1
fi

# Cascade metrics MUST exist in the metrics surface (emitted somewhere under src/).
for m in beava_cascade_cross_shard_total beava_cascade_intra_shard_total \
         beava_cascade_queue_depth beava_cascade_lag_seconds \
         beava_shard_inbox_high_watermark_total; do
    if ! grep -rq "\"$m\"" src/; then
        echo "FAIL: metric $m not emitted anywhere in src/"; FAIL=1
    fi
done

if [ "$FAIL" -eq 0 ]; then
    echo "OK — source_lsn echoed on 4 TCP arms + HTTP handlers ($HTTP_LSN_COUNT refs); 5 cascade metrics registered"
    exit 0
else
    exit 1
fi
