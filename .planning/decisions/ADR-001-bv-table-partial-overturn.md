# ADR-001: `@bv.table` aggregation-output revival (partial overturn of v0 events-only scope)

## Status

Accepted
Date: 2026-05-03

## Context

Phase 12.7 (closed 2026-05-01, PASS) stripped the entire `@bv.table` surface from
beava v0 — no `bv.table` decorator in the public namespace, no `OpNode::Table*`
variants, no `app.upsert / app.delete / app.retract` SDK verbs, no `TemporalStore`,
no MVCC, no `temporal_http`. The strip removed approximately 5,500 LOC and locked
the events-only commitment via the memory file `project_v0_events_only_scope`
(originally locked 2026-04-30) and the architectural test
`crates/beava-server/tests/phase12_7_no_table_surface.rs`, which walks the
workspace at test runtime and fails if any forbidden symbol resurfaces.

The 2026-05-03 v0-launch design session re-examined what the public Python API
should look like for the v0 ship. The team locked Polars-style chained syntax
for feature authoring. The natural form for an aggregation declaration is:

```python
@bv.event
class Txn:
    user_id: str
    amount: float

@bv.table(key="user_id")
def UserFeatures(txn) -> bv.Table:
    return (
        txn.group_by("user_id")
           .agg(tx_count_1h=bv.count(window="1h"),
                tx_sum_1h=bv.sum("amount", window="1h"))
    )
```

The `@bv.table` decorator is the natural attachment point for the
`group_by().agg(...)` aggregation output — the user names the result, declares
the partition key, and beava registers it as a derivation node with
`output_kind=table`. There is no upsert, no delete, no retract, no MVCC, no
temporal versioning. It is a **function-form** decorator that wraps an
aggregation chain into a named derivation node and nothing more.

Phase 12.7's strip was over-broad: it killed both the user-mutable surface AND
the aggregation-output decorator. The launch design session restored only the
latter. This is a strictly narrower revival than the pre-12.7 surface. Joins,
table mutation, retraction propagation, and session windows REMAIN killed.

## Decision

Revive `@bv.table` as an aggregation-output decorator in the public `bv`
namespace. Specifically:

- `@bv.table(key=...)` decorates a function that returns
  `events.group_by(...).agg(...)`. The decorator captures the chain, names the
  result, and emits a derivation node with `output_kind=table`.
- `app.upsert(...)`, `app.delete(...)`, `app.retract(...)` SDK verbs REMAIN absent.
- `OpNode::TableUpsert`, `OpNode::TableDelete`, `OpNode::TableRetract` REMAIN
  absent. `OpNode::Table*` is permitted ONLY for derivation `output_kind=table`
  — not as a top-level register node, not as a user-facing mutation surface.
- `TemporalStore`, `MvccVersion`, `temporal_http`, and the `RecordType::Table*`
  WAL variants REMAIN absent.
- The architectural test `crates/beava-server/tests/phase12_7_no_table_surface.rs`
  is amended to permit `OpNode::Table*` when it appears as the output kind of
  a derivation node, and to continue rejecting it everywhere else (top-level
  register nodes, all upsert/delete/retract surface, `TemporalStore`,
  `temporal_http`, `RecordType::Table*` WAL variants). The amendment lands in
  Phase 13.4 alongside the engine-side `output_kind=table` support; **Phase
  13.0 documents the intent here, but does not change any code or test file**.
- The memory file `project_v0_events_only_scope` is updated with a
  partial-overturn pointer to this ADR (Task 5 of Plan 13.0-01).

## Consequences

**Easier:**
- Polars-style aggregation syntax is the canonical Python API (locked from the
  2026-05-03 design session). The decorator syntax matches the user's mental
  model: declare an event source, declare a feature table that aggregates from
  it.
- Feature authors declare aggregations as named, keyed entities — natural
  conceptual model for fraud / ad-tech / behavioral analytics. Priya (target
  user per `project_beava_website_ia`) writes one decorator and one chain;
  beava handles the rest.
- The wire spec (`docs/wire-spec.md`, Plan 13.0-02) carries `kind=table`
  derivations, so SDK porters in Phase 13.6 (TypeScript + Go) implement
  aggregation output without designing a parallel mechanism. The decorator is
  pure sugar on top of the same JSON wire shape Python already produces.

**Harder:**
- The architectural-test allowlist needs careful scoping. Permitting
  `OpNode::Table*` as a derivation `output_kind` while keeping it forbidden as
  a top-level register node + at all other call sites requires a per-AST-context
  check, not a global symbol grep. Phase 13.4 owns the careful test update.
- Future contributors might assume `@bv.table` revives the full Phase 11.5
  surface (joins / MVCC / mutation). Counter-discipline: this ADR is normative;
  the architectural test enforces; CLAUDE.md `§ Events-Only Invariant` block
  gets a footnote pointing here (Plan 13.0-15 closure may add it).

**Follow-on actions:**
- **Phase 13.4 Plan:** implement engine-side `output_kind=table` for derivations
  in `register_validate.rs` + `OpNode` enum + `agg_compile.rs`. Amend
  `phase12_7_no_table_surface.rs` allowlist (per-AST-context check). Add a
  GREEN register-payload round-trip test that confirms the allowlist permits
  derivation `output_kind=table` while still rejecting top-level register
  table-nodes.
- **Phase 13.5 Plan:** implement Python `@bv.table` function-form decorator in
  `python/beava/_table.py` (or merge into `_register.py`). The decorator wraps
  the aggregation chain into a derivation node JSON payload at register time.
- **Phase 13.6 Plans:** TypeScript + Go SDKs each support `bv.table()` as the
  aggregation-output equivalent (TS: builder-pattern wrapper; Go: function
  returning a table-derivation struct). Wire JSON parity with Python is the
  contract; per-language idioms diverge below the wire.
- **CLAUDE.md `§ Events-Only Invariant`** block gets a footnote pointing at
  this ADR (Plan 13.0-15 closure adds it).
- **REQUIREMENTS.md `V0-EVENTS-ONLY-01`** gets a sub-bullet documenting the
  permitted aggregation-output exception (Plan 13.0-15 closure may add it).

### Deferred for Phase 13.4 implementation

ADR-001 documents the intent but lands no code. The following code changes are
DEFERRED to Phase 13.4 and MUST be addressed there:

- Update `crates/beava-server/tests/phase12_7_no_table_surface.rs` allowlist to
  permit `OpNode::Table*` on derivations whose `output_kind` is `table`, while
  keeping rejection for top-level register table-nodes and all other call sites
  (a per-AST-context check, not a global symbol grep).
- Implement `OpNode::Table*` enum variants in the engine's op-node tree (or
  revive prior variants conditionally).
- Implement `output_kind: "table"` field plumbing through register payload
  validation in `crates/beava-core/src/register_validate.rs`.
- Add a register-payload round-trip GREEN test that exercises the per-AST-context
  check end-to-end: a register with a derivation `output_kind=table` succeeds;
  a register with a top-level table node fails with `unsupported_node_kind`.

Phase 13.5 then adds the Python `@bv.table(key=...)` decorator on top.
Phase 13.6 ports the equivalent surface to TS + Go SDKs.
