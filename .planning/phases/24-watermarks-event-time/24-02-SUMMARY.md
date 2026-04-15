---
phase: 24-watermarks-event-time
plan: 02
subsystem: protocol+sdk
tags: [protocol, tcp, sdk, python, table-row]
dependency_graph:
  requires:
    - 24-01   # EntityState.table_rows + upsert/tombstone primitives
  provides:
    - WIRE-TABLE-01   # OP_PUSH_TABLE opcode wired end-to-end
    - WIRE-TABLE-02   # OP_DELETE_TABLE opcode wired end-to-end
    - SDK-TABLE-01    # app.push(table, key, fields) + app.delete(table, key)
    - GET-MERGED-01   # GET returns streams + Live table_rows + static_features
  affects:
    - src/server/protocol.rs       # opcodes + Command variants + parse_command
    - src/server/tcp.rs            # dispatch + unknown-table rejection
    - src/state/store.rs           # collect_merged_features
    - src/engine/pipeline.rs       # has_registered_table + merged GET plumbing
    - python/tally/_protocol.py    # encoder constants + functions
    - python/tally/_app.py         # push overload + delete
    - python/tally/_stream.py      # _tally_kind="stream" marker
    - python/tally/_table.py       # _tally_kind="table" marker
tech-stack:
  added: []
  patterns:
    - decorator-marker-dispatch    # _tally_kind on Stream/Table classes
    - merged-view-read-path        # store-side flatten, engine-side derive
key-files:
  created:
    - tests/test_op_push_table.rs
    - python/tests/test_push_table_e2e.py
  modified:
    - src/server/protocol.rs
    - src/server/tcp.rs
    - src/state/store.rs
    - src/engine/pipeline.rs
    - python/tally/_protocol.py
    - python/tally/_app.py
    - python/tally/_stream.py
    - python/tally/_table.py
decisions:
  - "Opcode assignment locked at OP_PUSH_TABLE=0x0B and OP_DELETE_TABLE=0x0C. 0x09 remains a deliberate gap left after OP_FLUSH in prior phases; keeping the new Table opcodes contiguous after OP_PUSH_BATCH (0x0A) was cleaner than back-filling the gap. The decision is documented with a block comment above the constants in both the Rust and Python protocol modules so future opcodes know the pattern."
  - "App.push dispatch is primarily driven by _tally_kind — a class-level attribute set on the Stream and Table base classes in the Python SDK (inherited by every decorated subclass). Secondary fallback on positional arity (len(args) == 1 vs 2) is enforced in the branch body so callers get a typed TypeError for structurally wrong calls BEFORE any wire I/O. Isinstance checks on Stream/Table were rejected because they would force _app.py to import the concrete classes and invert the current dependency direction."
  - "Table form push is SYNCHRONOUS (OP_PUSH_TABLE round-trips and returns STATUS_OK before control returns). The Stream form remains fire-and-forget (OP_PUSH_ASYNC). Sync-ness for Tables is a deliberate v0 choice so tests and callers can do a race-free app.get(key) immediately after a push; async Table push ships post-v0 when retraction needs it."
  - "PipelineEngine::get_features now reads through StateStore::collect_merged_features instead of get_all_features. This deliberately extends the merged view to the derive/view eval path too — derive expressions can now reference `TableName.field` entries. This is a capability gain, not a breaking change: the previous `StreamName.field` qualifier syntax still works because it's computed in a separate pass inside get_features."
  - "Merged-view collision rule: on a name clash between a flattened Table row and a static_feature, static_features wins (overlay order is streams → table_rows → static_features). In v0 this should not occur because Table rows emit prefixed names (`TableName.col`) while static_features use raw names; the rule is documented inline on collect_merged_features for forward compatibility."
  - "TCP dispatch calls engine.cascade_table_upsert for both OP_PUSH_TABLE and OP_DELETE_TABLE so the Phase 23 TT-join cascade continues to fire — this preserves the existing marker-based TT join behavior unchanged. Plan 24-03 reworks the cascade internals to consume table_rows directly and un-ignore the 7 Phase 23 TT tests; plan 02 only keeps the hook live."
  - "`now` passed into upsert/tombstone is wall-clock. Plan 24-04 will replace the timestamp with a parsed `_event_time` JSON field (with wall-clock fallback) once watermarks land. A `Phase 24-04: replace with _event_time parsing` comment marks the call sites."
metrics:
  duration: ~40min
  completed: 2026-04-14
  tasks: 2
  commits:
    - f539af2   # Task 1: Rust server — opcodes, dispatch, merged GET
    - 6b4a668   # Task 2: Python SDK — push/delete + e2e tests
---

# Phase 24 Plan 02: TCP opcodes + Python SDK push/delete — Summary

**One-liner:** Wired the Plan 01 `table_rows` storage primitive end-to-
end: `OP_PUSH_TABLE` (0x0B) and `OP_DELETE_TABLE` (0x0C) opcodes with
Command variants, TCP dispatch, unknown-table rejection, and a
merged-view GET path that flattens Live Table rows as
`TableName.field`; Python SDK gained `app.push(table, key, fields)` and
`app.delete(table, key)` routed by a `_tally_kind` decorator marker.

## What shipped

### 1. Wire protocol — opcodes + parse_command (commit `f539af2`)

`src/server/protocol.rs`:

```rust
pub const OP_PUSH_TABLE:   u8 = 0x0B; // [u16 name][u16 key][JSON fields]
pub const OP_DELETE_TABLE: u8 = 0x0C; // [u16 name][u16 key]

pub enum Command {
    // ...
    PushTable   { table_name: String, key: String, fields: serde_json::Value },
    DeleteTable { table_name: String, key: String },
}
```

`parse_command` handles both opcodes; `OP_PUSH_TABLE` rejects non-object
JSON payloads as `TallyError::Protocol("...must be a JSON object")`.

Three parse-level unit tests: round-trip for each opcode plus the non-
object rejection.

### 2. TCP dispatch (commit `f539af2`)

`src/server/tcp.rs::handle_push_table` / `handle_delete_table`:

1. Read the engine, validate `engine.has_registered_table(name)`;
   reject with `STATUS_ERROR "unknown table: {name}"` if not. **No
   state mutation before this check** (T-24-02-04).
2. Convert the JSON fields → `AHashMap<String, FeatureValue>` using the
   existing `json_to_feature_value` helper (same path `OP_SET` uses).
3. Call `store.upsert_table_row` / `tombstone_table_row` with wall-
   clock `now` (marked with a `Phase 24-04: replace with _event_time
   parsing` TODO for the coming watermark phase).
4. Mark the key dirty and fire
   `engine.cascade_table_upsert(..., tombstoned, ...)` to keep the
   Phase 23 TT cascade alive.
5. Return `STATUS_OK` with empty payload.

New method on `PipelineEngine`:
```rust
pub fn has_registered_table(&self, name: &str) -> bool {
    // true iff stored REGISTER JSON carries `"kind": "table"`
}
```

### 3. Merged GET view (commit `f539af2`)

New `StateStore::collect_merged_features(key, now) -> FeatureMap`
overlays three sources in order:

1. Live stream operator features (via `op.read(now)`).
2. Live `table_rows` flattened as `format!("{table_name}.{field_name}")`.
   Tombstoned rows are **skipped** (T-24-02-03).
3. `static_features` (last writer wins on any future collision).

`PipelineEngine::get_features` now reads through
`collect_merged_features` so derive expressions can reference
`TableName.field` entries transparently. No existing call site changed.

### 4. Python SDK (commit `6b4a668`)

`python/tally/_protocol.py`:

```python
OP_PUSH_TABLE:   int = 0x0B
OP_DELETE_TABLE: int = 0x0C

def encode_push_table(table_name, key, fields) -> bytes:
    return encode_string(table_name) + encode_string(key) + \
           json.dumps(fields).encode("utf-8")

def encode_delete_table(table_name, key) -> bytes:
    return encode_string(table_name) + encode_string(key)
```

`python/tally/_stream.py` and `_table.py` gained a class-level
`_tally_kind` marker (`"stream"` / `"table"`) inherited by every
decorated subclass.

`python/tally/_app.py::push` refactored into a dispatch:

```python
def push(self, source, *args) -> None:
    if getattr(source, "_tally_kind", "stream") == "table":
        key, fields = args              # push(table, key, fields) — SYNC
        payload = encode_push_table(source._tally_stream_name, key, fields)
        self._send(OP_PUSH_TABLE, payload)
        return
    event = args[0]                     # push(stream_class, event) — ASYNC
    payload = encode_push_binary(source._tally_stream_name, event)
    self._client.send_frame_no_recv(OP_PUSH_ASYNC, payload)
```

`App.delete(table, key)` mirrors the Table form: sync round-trip,
`ProtocolError` on unknown table.

Both overloads raise `TypeError` before any wire I/O when arity is
wrong or `fields` is non-dict.

### 5. Tests

**`tests/test_op_push_table.rs`** — 6 end-to-end Rust tests against a
real TCP listener:

| Test | Covers |
| ---- | ------ |
| `push_table_creates_live_row` | OP_PUSH_TABLE → Live row in store. |
| `push_table_overwrites_prior_live_row` | Whole-row replacement on re-push. |
| `delete_table_flips_to_tombstone` | OP_DELETE_TABLE → TableRowState::Tombstoned. |
| `push_table_unknown_table_returns_error` | Unknown table → STATUS_ERROR, state untouched; symmetric delete. |
| `get_returns_merged_view` | Live stream op + Table row + static_feature all flow through OP_GET. |
| `get_filters_tombstoned_rows` | Tombstoned row invisible to OP_GET (T-24-02-03). |

Plus 3 parse-level tests in `src/server/protocol.rs::tests`:
`op_push_table_roundtrip_via_parse_command`,
`op_push_table_rejects_non_object_fields`,
`op_delete_table_roundtrip_via_parse_command`.

**`python/tests/test_push_table_e2e.py`** — 7 pytest cases:

| Test | Covers |
| ---- | ------ |
| `test_encode_push_table_wire_format_matches_rust` | Byte-exact layout check. |
| `test_encode_delete_table_wire_format` | Byte-exact layout check. |
| `test_push_delete_get_roundtrip` | push → get merged → delete → get filtered. |
| `test_push_stream_vs_push_table_disambiguation` | One App drives a Stream (2-arg) and a Table (3-arg). |
| `test_push_table_unknown_table_raises_protocol_error` | Unknown table → ProtocolError. |
| `test_delete_unknown_table_raises_protocol_error` | Unknown delete → ProtocolError. |
| `test_push_table_bad_arity_type_error` | Wrong arity / non-dict fields → TypeError before wire I/O. |

## Test results

* `cargo build` — clean.
* `cargo test --lib` — **682 / 682** passed (up from 679; added 3
  parse_command tests in `src/server/protocol.rs::tests`).
* `cargo test --test test_op_push_table` — **6 / 6** passed.
* `cargo test --test test_join_table_table` — **5 pass / 7 ignored**
  (unchanged Phase 23 baseline; un-ignoring is Plan 03's job).
* `cargo test` (full suite across 30+ integration binaries) — all
  green.
* `pytest python/tests/test_push_table_e2e.py` — **7 / 7** passed.
* `pytest python/tests/` — **418 passed, 2 skipped** (new tests
  included; no regressions in existing surface).

## Deviations from plan

### [Rule 2 — Missing functionality] Parse-level tests co-located in protocol.rs

**Found during:** Task 1.

**Issue:** The plan enumerated `op_push_table_roundtrip_via_parse_command`
and `op_delete_table_roundtrip_via_parse_command` as needed tests but
placed them in `tests/test_op_push_table.rs`. Those belong next to the
other `test_parse_command_*` tests inside `src/server/protocol.rs` so
they exercise the pure parser without pulling in the full integration
harness.

**Fix:** Added both parse-level tests inline in `protocol.rs::tests`
alongside the existing `test_parse_command_*` cases, plus a third
`op_push_table_rejects_non_object_fields` test guarding the JSON-
object type check. The integration file (`tests/test_op_push_table.rs`)
keeps the 6 end-to-end TCP tests.

### [Rule 2 — Missing functionality] TypeError gates in App.push/App.delete

**Found during:** Task 2.

**Issue:** The plan specified `app.push(table, key, fields)` and
`app.delete(table, key)` but did not gate against `push(stream, k, f)`
(wrong arity for Stream) or `push(table, key, "not a dict")` (non-dict
fields). Without the gate, a wrong-arity call would pack garbage into
the wire frame and the server would reject it with a generic protocol
error — hard to attribute at the call site.

**Fix:** Both branches of `App.push` now raise `TypeError` before any
wire I/O when arity is wrong. The Table branch additionally checks
`isinstance(fields, dict)`. One new pytest case
(`test_push_table_bad_arity_type_error`) guards these paths. Treated
as a Rule 2 correctness fix.

### [Intentional — scope boundary] Registration payloads emitted by SDK unchanged

**Found during:** Task 2.

**Issue:** `@tl.table`-decorated descriptors still serialize through
the existing `compile_to_register_json` path. I verified the REGISTER
payload carries `"kind": "table"` (that is precisely what my new
`engine.has_registered_table` checks for), but I did not touch the
serializer itself — scope says no changes to REGISTER.

**Fix:** None needed. The dispatch marker (`_tally_kind`) lives on the
Python class tree only; the wire payload continues to route through
the unchanged v0 translator.

## Known stubs

None introduced. The merged GET view filters tombstoned rows
correctly, the unknown-table path short-circuits before any state
mutation, and the Phase 23 TT cascade hook is still called on every
push/delete so `test_join_table_table` remains at its 5 pass / 7
ignored baseline.

Plan 03 will:
1. Un-ignore the 7 TT tests by reworking `cascade_table_upsert` to
   consume `table_rows` instead of `static_features` markers.
2. Likely deprecate the "empty-object SET = tombstone" convention
   once full delete semantics live under OP_DELETE_TABLE.

## Threat flags

Plan's threat register (T-24-02-01 … 05) all mitigated:

* **T-24-02-01 (framing tampering)** — mitigated. `parse_frame`
  enforces the 4-byte length prefix; `read_string` enforces u16
  length bounds; both opcodes reuse these helpers unchanged.
* **T-24-02-02 (flood)** — mitigated. New opcodes route through the
  same per-connection command path as every other opcode; the
  existing rate limiter covers them transparently.
* **T-24-02-03 (info disclosure via tombstoned row)** — mitigated.
  `collect_merged_features` filters `TableRowState::Tombstoned`.
  Covered by `get_filters_tombstoned_rows`.
* **T-24-02-04 (unknown-table bypass)** — mitigated.
  `handle_push_table` / `handle_delete_table` call
  `engine.has_registered_table` BEFORE any `upsert_table_row` /
  `tombstone_table_row` call. Covered by
  `push_table_unknown_table_returns_error`.
* **T-24-02-05 (JSON→FeatureValue type confusion)** — mitigated.
  Reuses the existing `json_to_feature_value` helper from the OP_SET
  path; nested objects/arrays flatten to `FeatureValue::Missing`
  (same behaviour as SET).

No new threat surface introduced beyond what Plan 01 already exposed
via the storage primitive.

## Self-Check: PASSED

Verified files exist (absolute paths):

* `/data/home/tally/src/server/protocol.rs` — FOUND (modified)
* `/data/home/tally/src/server/tcp.rs` — FOUND (modified)
* `/data/home/tally/src/state/store.rs` — FOUND (modified)
* `/data/home/tally/src/engine/pipeline.rs` — FOUND (modified)
* `/data/home/tally/python/tally/_protocol.py` — FOUND (modified)
* `/data/home/tally/python/tally/_app.py` — FOUND (modified)
* `/data/home/tally/python/tally/_stream.py` — FOUND (modified)
* `/data/home/tally/python/tally/_table.py` — FOUND (modified)
* `/data/home/tally/tests/test_op_push_table.rs` — FOUND (created)
* `/data/home/tally/python/tests/test_push_table_e2e.py` — FOUND (created)
* `/data/home/tally/.planning/phases/24-watermarks-event-time/24-02-SUMMARY.md` — FOUND (this file)

Verified commits exist on `main`:

* `f539af2` feat(24-02): OP_PUSH_TABLE/OP_DELETE_TABLE opcodes + merged GET view
* `6b4a668` feat(24-02): Python SDK push/delete for Tables + merged GET e2e tests

Verified test gates:

* `cargo test --lib` — 682 / 682
* `cargo test --test test_op_push_table` — 6 / 6
* `cargo test --test test_join_table_table` — 5 / 5 + 7 ignored (unchanged)
* `pytest python/tests/test_push_table_e2e.py` — 7 / 7
* `pytest python/tests/` — 418 passed, 2 skipped (no regressions)

Phase 24 Plan 02 is complete. Plan 03 can now migrate
`cascade_table_upsert` to consume `table_rows` directly and un-ignore
the 7 remaining Phase 23 TT-join tests.
