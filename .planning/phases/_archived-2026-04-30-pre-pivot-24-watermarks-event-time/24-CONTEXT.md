# Phase 24: Watermarks, event-time, & Table storage redesign - Context

**Gathered:** 2026-04-14
**Status:** Ready for planning
**Mode:** Auto-generated from v0 design conversation + Phase 23 handoff

<domain>
## Phase Boundary

This phase has TWO inseparable concerns that must ship together:

### Concern 1 — Proper Table row storage model (carried forward from Phase 23 deferral)

Phase 23 used a marker-based cascade approach (shadow markers in `static_features`) for Table↔Table joins. This works for current scenarios but doesn't satisfy the v0 spec which calls for first-class Table row addressing by `(table_name, key)`. Phase 24 must implement the proper model because retraction semantics need real per-table-row identity to function correctly.

Specifically:
- `EntityState.table_rows: AHashMap<String, TableRow>` where `TableRow { fields: AHashMap<String, FeatureValue>, state: TableRowState }`
- `TableRowState::Live | Tombstoned(SystemTime)` with 7d grace window
- New TCP opcodes: `OP_PUSH_TABLE` (upsert), `OP_DELETE_TABLE` (tombstone)
- Python SDK: `app.push(table, key, fields)`, `app.delete(table, key)`
- StateStore methods: `upsert_table_row`, `tombstone_table_row`, `get_table_row`
- Snapshot codec v6 → v7 with backward-compat migration
- Migrate Phase 23's marker-based TT cascade to use real TableRow lookups
- Unblock the 7 ignored tests in `test_join_table_table.rs`

### Concern 2 — Watermarks + event-time (original Phase 24 scope)

- Events carry `_event_time` JSON field; absent → wall-clock fallback
- Per-stream watermark = max(event_time seen) − 5s
- γ propagation: alignment only at join/agg boundaries (stateless ops pass through)
- Late events (event_time < watermark) dropped + counter `tally_late_events_dropped_total{stream}`
- `now()` builtin → wall-clock; `event_time()` → current event's event-time
- Tests: out-of-order within 5s lands in correct bucket; > 5s late drops

The two concerns share substrate: late-event handling for Tables means the Table row gets updated retroactively, requiring proper TableRow identity. Phase 23's marker model can't represent "this column from input A vs that column from input B at this event-time" cleanly enough for retraction semantics.

**Out of scope:**
- DAG-level retraction propagation through aggregations (still v0.1 — Table aggregation is disabled in v0)
- Per-stream tunable lateness (fixed 5s in v0)
- Side outputs for very-late events
- Phase 25 (query surface), Phase 26 (test migration)

</domain>

<decisions>
## Implementation Decisions (LOCKED — from spec + earlier conversation)

### Storage model

- `EntityState.table_rows: AHashMap<String, TableRow>` per entity key
- `TableRow { fields: AHashMap<String, FeatureValue>, state: TableRowState }`
- `TableRowState::Live | Tombstoned { since: SystemTime }` with 7d grace
- After 7d grace, tombstoned rows GC'd from `table_rows`
- Existing `static_features` map preserved for legacy/static writes (back-compat)
- `app.get(key)` returns merged view: union of live `table_rows` + `static_features` overlay

### TCP opcodes

- `OP_PUSH_TABLE` — payload: `{table_name, key, fields}`. Pick opcode byte that doesn't collide; document in `src/server/protocol.rs`
- `OP_DELETE_TABLE` — payload: `{table_name, key}`
- Existing `OP_SET`/`OP_MSET` continue to work for static_features (backward-compat)
- `OP_PUSH` (event ingestion to Stream sources) unchanged

### Python SDK

- `app.push(table, key, fields)` — for pushing Table-row updates
- `app.delete(table, key)` — for tombstoning a row
- `app.set(...)` / `app.mset(...)` continue to work for static features
- Document the distinction in docstrings: Table sources use `push`/`delete`; static feature writes use `set`

### Snapshot migration v6 → v7

- v7 format adds `table_rows` field per `EntityState`
- v6 snapshots load with empty `table_rows` (no Table data exists yet in v6)
- `tombstoned_at` field on TableRowState requires SystemTime serialization (already supported via postcard + serde)

### Cascade migration

- Phase 23's `cascade_table_upsert` reworked to consume `table_rows[A]` and `table_rows[B]` instead of static_features markers
- TableTableJoin output written to `table_rows[output_name]`
- `tt_inner_tombstone_right_deletes_output`, `tt_left_tombstone_right_nulls_right_fields` etc. — the 7 ignored tests in `test_join_table_table.rs` un-ignored and pass

### Watermarks

- Per-stream watermark = max(event_time observed for events on this stream) − 5s
- Watermark stored in stream's metadata (state store has stream-level metadata, not just per-key)
- γ propagation:
  - Stateless ops (filter/map/select/...) pass watermark through from input to output stream
  - Joins (Stream↔Stream): output watermark = min(left_wm, right_wm)
  - Aggregations: output watermark attached to the resulting Table; defines bucket "sealing" semantics
  - Tables: per-table watermark advanced on `OP_PUSH_TABLE` / `OP_DELETE_TABLE` based on `_event_time` field if present
- Counter: `tally_late_events_dropped_total{stream}` when event arrives with event_time < watermark

### Event-time wire format

- `_event_time` as JSON field on every event payload (per Q1 lock)
- Falls back to wall-clock arrival if absent
- Both `OP_PUSH` (Stream events) and `OP_PUSH_TABLE` (Table upserts) honor `_event_time`

### Builtins

- `now()` → wall-clock (existing)
- `event_time()` → new builtin; returns current event's event_time. Available in derive expressions and filter predicates.

### Late event handling (no DAG retraction in v0)

- Within-5s late events land in the correct bucket (per Phase 24's bucket-routing logic by event_time)
- Beyond-5s late events dropped with counter increment
- For aggregations whose buckets have already aged out: late-but-in-window events still update the live bucket; late-but-bucket-aged events are dropped
- Stream↔Stream joins: late event triggers retroactive match against opposite-side buffer (per Phase 23-02 known limitation; this phase improves correctness within the 5s window)

</decisions>

<code_context>
## Existing Code Insights

- `src/state/store.rs` — current `EntityState` with `static_features` only; Phase 24 adds `table_rows`
- `src/state/snapshot.rs` — current v6 codec; Phase 24 adds v7 with migration
- `src/server/tcp.rs` — REGISTER + SET dispatch from 22-04; add new opcodes
- `src/server/protocol.rs` — opcode constants
- `src/engine/pipeline.rs` — cascade, especially `cascade_table_upsert` from Phase 23
- `src/engine/operators.rs` — operator state; watermark tracking lives at stream level
- `src/engine/window.rs` — bucket logic (event-time routing changes here)
- `python/tally/_app.py` — SDK; add `push(table, key, fields)`, `delete(table, key)`
- `python/tally/_protocol.py` — encode opcodes
- `tests/test_join_table_table.rs` — 7 ignored tests waiting on storage redesign

</code_context>

<specifics>
## Specific Ideas

- **Layer storage first, then watermarks** — storage is foundational; watermarks layer cleanly on top
- **Per-stream watermark observability**: expose in `/debug/key/:key` and `/debug/streams/:name`
- **Test the migration**: spin up a server with v6 snapshot, upgrade binary, verify state loads with empty table_rows and continues to work

</specifics>

<deferred>
## Deferred Ideas

- DAG-level retraction propagation (v0.1)
- Per-stream tunable lateness (v0.1)
- Side outputs for very-late events (post-v0)
- Session windows (post-v0)

</deferred>

---

*Phase: 24-watermarks-event-time*
*Storage scope absorbed from Phase 23-03 deferral; watermark scope from original ROADMAP.md*
