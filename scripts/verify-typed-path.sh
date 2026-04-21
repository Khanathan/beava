#!/usr/bin/env bash
# Phase 59.6 grep-gate — exits 0 when typed-pipeline infra is in place, 1 otherwise.
#
# Wave 0 (this wave): planted; MUST exit 1 (RED gate).
# Wave 1: exit 1 still — schema infra lands but wire codec not yet.
# Wave 2: exit 1 — wire codec lands but operators not yet.
# Wave 3-6: progressive green for each operator group.
# Wave 7 (close): MUST exit 0 — all 5 invariants hold.
#
# Invariants checked:
#   1. RegisteredSchema type exists at `src/engine/schema.rs` or equivalent.
#   2. Row type exists with `#[repr(C)]` and `schema_id` field.
#   3. SchemaRegistry exists on PipelineEngine (accessible via `engine.get_schema(name)`
#      / `engine.is_typed_stream(name)` / `engine.register_typed_schema(...)`).
#   4. OP_PUSH_BATCH decoder branches on schema presence (typed → Row, untyped → Value).
#   5. Per-stream counters `typed_row_path_total` + `value_fallback_path_total` increment.

set -eu
ROOT="${ROOT:-$(git rev-parse --show-toplevel)}"
cd "$ROOT"

fail() {
  echo "GATE FAIL: $1"
  exit 1
}

# 1. RegisteredSchema exists.
if ! grep -rqE 'pub struct RegisteredSchema' src/; then
  fail "RegisteredSchema not yet defined (expected Wave 1+)"
fi

# 2. Row type with #[repr(C)] + schema_id field.
if ! grep -rqE '#\[repr\(C\)\]' src/ 2>/dev/null; then
  fail "Row type repr(C) not yet defined"
fi
if ! grep -rqE 'pub struct Row' src/; then
  fail "Row type not yet defined"
fi

# 3. SchemaRegistry accessible via engine.
if ! grep -rqE 'pub fn get_schema|pub fn is_typed_stream|pub fn register_typed_schema' src/engine/; then
  fail "Engine schema accessors not yet wired (get_schema / is_typed_stream / register_typed_schema)"
fi

# 4. OP_PUSH_BATCH branches on schema presence.
if ! grep -rqE 'schema_id|RegisteredSchema' src/server/protocol.rs src/server/tcp.rs 2>/dev/null; then
  fail "Wire codec has not yet added schema_id prefix (Wave 2)"
fi

# 5. Counters referenced in shard thread (beyond the Wave 0 struct decl).
COUNT=$(grep -rcE 'typed_row_path_total\.fetch_add|typed_row_path_total\.store' src/ 2>/dev/null | grep -v ':0$' | wc -l | tr -d ' ')
if [ "$COUNT" -lt 1 ]; then
  fail "typed_row_path_total never bumped on the hot path (expected Wave 1+ wiring)"
fi

echo "GATE OK: Phase 59.6 typed-path invariants hold"
exit 0
