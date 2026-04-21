#!/usr/bin/env bash
set -uo pipefail
# Phase 59 — Grep-ZERO gate for TPC-PERF-09 D-C3.
#
# Asserts: ZERO `serde_json::to_vec(payload)` or `serde_json::to_vec(r.payload)`
# references in src/server/tcp.rs (outside line-comments).
# Additionally verifies that `src/server/http_ingest.rs` STILL CONTAINS at
# least one `serde_json::to_vec` (D-A4: HTTP path stays JSON; the invariant
# is scoped to tcp.rs only, and MUST NOT accidentally be over-applied to
# the HTTP surface).
#
# Wave 0 expectation (HEAD before Wave 1):
#   EXIT 1 — the WASTE lives at tcp.rs:2159 + tcp.rs:2538 (RED contract).
# Wave 1 expectation (after Bytes passthrough lands):
#   EXIT 0 — WASTE deleted; tcp.rs no longer re-serializes.
#
# The invariant script is idempotent and cheap; Wave 4 close MUST still
# see exit 0 (guards against accidental reintroduction).

TCP_FILE="src/server/tcp.rs"
HTTP_FILE="src/server/http_ingest.rs"

if [[ ! -f "$TCP_FILE" ]]; then
    echo "FAIL: $TCP_FILE not found" >&2
    exit 1
fi

# Strip line-comments before grepping so doc-comments referencing the
# pattern (e.g. in the CONTEXT header) do not cause false RED.
TCP_HITS=$(grep -nE 'serde_json::to_vec\(payload\)|serde_json::to_vec\(r\.payload\)' "$TCP_FILE" \
    | grep -v -E '^[^:]+:[0-9]+:[[:space:]]*//' \
    | grep -v -E '^[^:]+:[0-9]+:[[:space:]]*\*' \
    || true)

if [[ -n "$TCP_HITS" ]]; then
    echo "FAIL: TCP JSON re-serialize patterns found in $TCP_FILE (TPC-PERF-09 D-C3):" >&2
    echo "$TCP_HITS" >&2
    total=$(echo "$TCP_HITS" | wc -l | tr -d ' ')
    echo "Total hits: $total (expected 0 after Phase 59 Wave 1)" >&2
    exit 1
fi

# D-A4 safety check: the HTTP ingest path is explicitly OUT of scope for
# Phase 59 (stays JSON). If this grep returns 0, we've accidentally
# over-applied the optimization to the HTTP path — surface as failure.
if [[ ! -f "$HTTP_FILE" ]]; then
    echo "WARN: $HTTP_FILE not found — skipping D-A4 HTTP-stays-JSON guard" >&2
else
    HTTP_JSON_REFS=$(grep -cE 'serde_json::to_vec' "$HTTP_FILE" || true)
    if [[ "$HTTP_JSON_REFS" == "0" ]]; then
        echo "FAIL: D-A4 safety guard: $HTTP_FILE no longer has serde_json::to_vec references." >&2
        echo "  HTTP PUSH is expected to stay JSON (Phase 59 D-A4 explicit scope exclusion)." >&2
        echo "  If this was intentional, update this script and TPC-PERF-09 row." >&2
        exit 1
    fi
fi

echo "OK: zero TCP JSON re-serialize patterns in $TCP_FILE (excluding comments)"
exit 0
