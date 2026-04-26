# Phase 23: Joins - Context

**Gathered:** 2026-04-14
**Status:** Ready for planning
**Mode:** Auto-generated from v0 design conversation + Phase 22 status

<domain>
## Phase Boundary

Implement the three join shapes that Phase 21's SDK stubbed out:

1. **Stream↔Stream** — symmetric interval windowed join, `inner` + `left` only (no outer per `join-outer-needed.md` research)
2. **Stream↔Table** — enrichment at event-time (point-in-time join of a Stream event with Table's current row)
3. **Table↔Table** — same-key join, both inputs keyed on identical key columns; output is a Table with unioned fields

Also in scope: **composite group_by keys** deferred from Phase 22-04 (aggregations can group by list of keys).

Phase 21's `_join.py` already produces the JSON payload for REGISTER; this phase is the Rust engine consumer + execution logic.

**Out of scope:**
- Outer joins (full outer) — deferred to v0.1 per research
- Partial-key joins (joining on a subset of composite keys) — deferred to v0.1
- Non-equi joins — never in scope for v0
- Joins without a key — rejected at registration (Phase 21)

</domain>

<decisions>
## Implementation Decisions (LOCKED)

### Stream↔Stream windowed join

- Symmetric interval: emit match iff `|event_time(left) − event_time(right)| ≤ within`
- State: per-key buffers of recent events on both sides, bounded by `within` (events older than `within` from the most recent event are evicted)
- Types: `inner` and `left` only. `right` can be built via `b.join(a, type="left")` if needed; no dedicated `right` type. Outer rejected at registration.
- On each event arrival: insert into own-side buffer, probe other-side buffer for matching keys within the interval, emit joined events
- Late events (within 5s watermark, once Phase 24 ships) trigger retroactive match against other-side buffer
- Output schema: union of left and right fields, with `_right` suffix on column collision (polars-style)
- Output is a **Stream**

### Stream↔Table enrichment join

- Each event on the Stream side looks up the Table's current row for the joined key(s)
- Point-in-time: Table's state at the moment of the event is used; no historical Table state lookup (Tables are current-state-only in v0)
- If Table has no row for the key: `inner` drops the event; `left` emits with null Table fields
- Output is a **Stream** (same cardinality as left Stream in the `left` case, ≤ left cardinality for `inner`)
- Schema union + `_right` suffix as above

### Table↔Table same-key join

- Both inputs must have identical key declarations (same field names, same types)
- Output Table shares the same key
- For each key, merge fields from both input Tables. Collision: polars `_right` suffix.
- Output Table is updated synchronously whenever either input Table updates for a given key
- Table tombstones (delete on either side) propagate: in `inner`, deletion on either side deletes the output row; in `left`, deletion on right-side nulls the right-side fields but keeps the left-side row
- Output is a **Table**

### Composite keys in group_by

Deferred from 22-04. The SDK (Phase 21) already produces `group_by: [key1, key2, ...]` in the REGISTER JSON. Engine must:
- Compose the group_by composite key from multiple event fields
- Use `(field1_value, field2_value, ...)` tuple as the HashMap key in the aggregation state store
- Composite keys work identically for counts, sums, percentile, top_k, etc.
- No schema or API change needed — just engine support

### Retraction handling (v0 reminder)

Since Table-input aggregation is disabled in v0, the retraction-through-joins cases are:
- Stream↔Stream: 5s-late event triggers retroactive match — emit additional joined events downstream
- Stream↔Table: late event triggers re-lookup of Table's current state; same as an in-order event
- Table↔Table: Table upserts/tombstones propagate through the join to the output Table (no DAG-level retraction propagation since output is Table; downstream Tables joining it see its changes via the same upsert/tombstone mechanism)

No DAG-level retraction propagation complexity because v0 does not allow Table-input aggregation.

### Schema inference

All three join types: union of input schemas with `_right` suffix on collision. Phase 21's `_serialize.py` already produces the expected schema. Engine must validate at REGISTER time that column types match where both sides have the same column name without suffix.

### Performance

- Stream↔Stream with `within="30m"` at 100K eps on each side: buffer state ~O(events × within) = ~3M events on each side, ~100 MB total. Acceptable.
- Stream↔Table: enrichment is O(1) lookup per event, essentially free after Phase 22's state store
- Table↔Table: O(1) per-key merge on Table updates

### Registration-time validation

At registration (Phase 21 SDK surface; this phase adds engine-side validation):
- Reject `type="outer"` with clear message: "full outer join deferred to v0.1; use two inner+left joins unioned as a workaround"
- Reject partial-key joins: "v0 requires full-key match; key fields must exactly match between inputs"
- Reject mismatched composite key orders/types
- Reject Stream↔Stream without `within` parameter

</decisions>

<code_context>
## Existing Code Insights

- `/data/home/tally/python/tally/_join.py` — SDK stub + REGISTER JSON shape (frozen contract)
- `/data/home/tally/src/engine/register.rs` — Phase 22-04 wires REGISTER; join-specific code is not yet present — this phase adds it
- `/data/home/tally/src/engine/pipeline.rs` — cascade; join operators will live here or in a new module
- `/data/home/tally/src/engine/operators.rs` — OperatorState enum; may gain Join variants
- `/data/home/tally/src/state/store.rs` — EntityState; Table↔Table join output needs its own keyed state
- Phase 22's event-time parsing (`parse_event_time()` in operators.rs) is reusable for Stream↔Stream window logic

</code_context>

<specifics>
## Specific Ideas

- **Stream↔Stream buffer eviction**: on event arrival, sort/probe against opposite side; after emission, evict opposite-side events older than `arrival_event_time - within`. Use a per-key deque indexed by event_time.
- **Stream↔Table enrichment**: single HashMap lookup on the Table's state per event. Piggyback on existing `get_features` machinery.
- **Table↔Table state**: treat as a derived Table with its own EntityState; populate on either-side updates; tombstone semantics as described.
- **Composite key support**: introduce a `CompositeKey = Vec<Value>` type (or smallvec-optimized) for aggregation state store HashMap keys. Single-key case stays fast-path.

</specifics>

<deferred>
## Deferred Ideas

- Full outer Stream↔Stream join (v0.1)
- Partial-key joins (v0.1)
- Non-equi joins (never)
- Joins on computed-state fields (Case 3 — requires v0.1 retractions)
- Range joins / band joins (not in scope)

</deferred>

---

*Phase: 23-joins*
*Design decisions sourced from `.planning/research/v0-restructure-spec.md`, `.planning/research/join-outer-needed.md`, Phase 21/22 artifacts*
