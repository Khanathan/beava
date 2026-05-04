# ADR-003: First-class global aggregation + public `bv.lit` export

## Status

ACCEPTED 2026-05-03 (Phase 13.0 mid-execution scope addition per user directive
"ship both / do both").

Implementation deferred to:

- **Phase 13.4** — engine sentinel routing (~30 LOC: empty-string `entity_id`
  routes to a regular hashmap key; register-time accepts `key_cols: []`).
- **Phase 13.5** — Python SDK (~110 LOC: `bv.lit` public export, `events.group_by()`
  empty allowance, `events.agg(**aggs)` shorthand, `@bv.table` no-`key=` form,
  `App.get(table_name)` 1-arg overload).
- **Phase 13.6** — TypeScript + Go SDK overloads (~150 LOC across both ports).

ADR-003 is a single combined ADR for two related surface additions — splitting
into ADR-003 + ADR-004 would have added bookkeeping noise without information
value (both decisions arose in the same user directive, both are small mechanical
exposures of existing internal capability, and the global-aggregation workaround
naturally requires `bv.lit` if `bv.lit` were not exposed).

## Context

Phase 13.0 mid-execution (2026-05-03) the user surfaced two API gaps not covered
by any prior decision:

### Gap 1 — No global aggregation

Beava v0 currently rejects `events.group_by()` (empty args) at the SDK layer
(`python/beava/_events.py:170-172`) with
`ValueError: "group_by() requires at least one key"`. `@bv.table(key=...)`
requires a key. There is **no** way for users to express "total events / sec
across all entities", "p95 latency globally", "top 10 hottest pages across the
platform" — every aggregation in v0 is per-entity.

The natural workaround would be to add a literal column with `bv.lit("global")`
and `group_by` it. But `bv.lit` is also not exposed in the public namespace
(see Gap 2). Without a public `bv.lit`, even the workaround is unwriteable in
user code.

Use cases blocked by this gap:

- **Operator dashboards** — global throughput; current entity count; global p95
  latency; "events ingested in the last minute".
- **Monitoring** — anomaly detection on global rates ("is the GLOBAL signup
  rate spiking right now?"); platform health metrics.
- **Top-K-globally features** — "top 10 hottest pages on the site" or "most
  active accounts in the last hour" — `top_k` aggregations across all entities,
  not per-entity.
- **Cross-entity aggregations** — "total spend across all users" or "avg session
  duration across all sessions" — features that summarize the platform, not
  individual entities.

The first-impression criticism risk for the v0 launch is real: "you can't even
count total signups today?" is the kind of comment that lives forever on Hacker
News.

### Gap 2 — `bv.lit(value)` not in public namespace

The `_Literal` AST node already exists at `python/beava/_col.py:195` and is used
internally by operator-overloading coercion (e.g., `bv.col("x") > 100`
constructs an implicit `_Literal(100)` via `__gt__`). Users can construct
literals **implicitly** through operator overloading, but cannot construct them
**explicitly** with a function call.

Use cases blocked:

- **Constant columns:** `events.with_columns(source=bv.lit("web"))` — no way
  to add a literal-valued column to an event derivation.
- **Explicit literal coercion:** `bv.col("count") / bv.lit(10.0)` — force
  float division when both sides could otherwise be integers.
- **Pattern compositions** — the global-aggregation workaround above
  (`events.with_columns(global_key=bv.lit("global")).group_by("global_key")`).
- **Cross-language parity** — TypeScript and Go SDKs need a public literal
  factory regardless (since their type systems don't have Python's flexible
  operator overloading); the Python SDK should match.

Both gaps are small mechanical exposures of existing internal capability. Both
have high user-visible value for monitoring and dashboard use cases. v0 launch
ship-criticism is real and avoidable.

## Decision

**Ship both `bv.lit` and first-class global aggregation in v0.**

### Decision A — Public `bv.lit(value)` export

Expose the existing internal `_Literal` AST node as a public factory function
`bv.lit(value)`:

```python
# Public API (lands in Phase 13.5):
def lit(value: int | float | str | bool | None) -> _Literal:
    """Construct an explicit literal expression for use in bv.col chains and
    with_columns()."""
    return _Literal(value)
```

Mirror surfaces:

- TypeScript SDK: `bv.lit(value: number | string | boolean | null) → Expr`
- Go SDK: `bv.Lit(value any) Expr`

Wire-level: literals were already serialized via the existing
operator-overloading path (`_Literal.to_expr_string()` produces the
JSON expression-string form). **No wire change required.** The schema for
expression-bearing fields already accepts the literal text forms (`null`,
`true`, `'string'`, numeric repr).

### Decision B — First-class global aggregation

Three coordinated SDK + engine + wire surface additions:

**1. SDK — Python (lands Phase 13.5):**

- Allow `events.group_by()` with empty args (no `*keys`) → returns a `GroupBy`
  instance with empty `_keys` tuple.
- Add `events.agg(**aggs)` shorthand on `EventSource` / `EventDerivation` (no
  `group_by` required) — equivalent to `events.group_by().agg(**aggs)`.
- Allow `@bv.table` decorator without `key=` kwarg → declares a global table
  (no per-entity dimension). Equivalent to `@bv.table(key=())`.
- Add `App.get(table_name)` arity overload (no `entity_id` arg) → returns the
  global feature dict; `app.get("GlobalTable") == {}` until first event lands
  for that table; `app.get("GlobalTable") == {"feature": value, ...}` after.
- All 53 ops work with both per-entity and global aggregation — same op
  semantics, different state-keying dimension.

**2. Engine — Rust (lands Phase 13.4):**

- Sentinel `entity_id = ""` (empty string) routes global-table state. Existing
  per-entity hashmap machinery handles it as "just another entity" with key = "".
  No new code path inside `apply_shard.rs::dispatch_*_sync` — just the absence
  of a special-case rejection.
- Register-time validation: `kind="table"` with `key_cols=[]` is the wire-level
  signal for global. Validates that `key_cols` is either non-empty (per-entity)
  or empty (global) — never null. Lives in
  `crates/beava-core/src/register_validate.rs`.
- GET path: `entity_id=""` on the wire returns the global state for that table.
- Cold-start: same as per-entity (empty dict before first event).

**3. Wire spec — JSON (patches Phase 13.0 docs):**

- Register payload:
  `{"kind": "table", "name": "GlobalCounter", "key_cols": [], "spec": {...}}`
  — empty `key_cols` array signals global.
- GET request: `{"table": "GlobalCounter", "entity_id": ""}` — empty string
  `entity_id` retrieves global state.
- Existing JSON Schemas need a one-line description update to clarify that
  `key_cols: []` (already-allowed `array` shape) signals a global table per
  ADR-003. **No schema constraint change.**
- 3 new example fixtures: `register-global-counter.request.json`,
  `get-global.request.json`, `get-global.response.json`.

### User-facing examples

**Per-entity (existing, unchanged):**

```python
@bv.table(key="user_id")
def UserSpend(purchases) -> bv.Table:
    return purchases.group_by("user_id").agg(spend=bv.sum("amount", window="1h"))

app.get("UserSpend", "alice")  # → {"spend": 150.0}
```

**Global (new, per ADR-003):**

```python
@bv.table   # no key= → global
def TotalSpend(purchases) -> bv.Table:
    return purchases.agg(spend=bv.sum("amount", window="1h"))   # no group_by

app.get("TotalSpend")  # → {"spend": 12345.67}, no entity arg
```

**`bv.lit` in expressions (new, per ADR-003):**

```python
events.filter(bv.col("amount") > bv.lit(100))             # explicit literal in filter
events.with_columns(safe_div=bv.col("a") / bv.lit(10.0))  # force float division
events.with_columns(source=bv.lit("web"))                  # constant column
```

## Consequences

### Positive

- **v0 launch ships with monitoring/dashboard use cases unblocked** — total
  throughput, global p95, top-K-globally, current entity count. The first
  question every operator asks ("how many total events did we get today?")
  has a clean answer.
- **Polars-aligned ergonomic** — Polars uses `df.agg(...)` (no `group_by`) for
  whole-frame aggregation; Beava now mirrors. The SDK feels familiar to anyone
  who's used Polars or pandas.
- **`bv.lit` enables clean expression composition** — especially type-coercion
  patterns and cross-language parity with TS/Go SDKs.
- **Cheap implementation cost** — ~30 LOC engine + ~110 LOC across 3 SDKs +
  small wire-spec doc patches. No new opcodes, no new schemas, no new wire-level
  error codes.
- **All 53 existing operators automatically support global mode** via the same
  code paths (state lives at sentinel `entity_id = ""`). No per-op porting work.
- **Failing acceptance tests already exist in Plan 16** — `python/tests/v0/test_global.py`
  (8 tests) and `python/tests/v0/test_lit.py` (5 tests) gate the implementation.
  When 13.4 + 13.5 land the surface, these tests turn GREEN as the contract
  acceptance.

### Negative

- **Two ways to express aggregation** (per-entity OR global) — slight
  onboarding cognitive load. Mitigated by clear `docs/concepts/global-aggregation.md`
  doc + worked examples in `docs/sdk-api/python.md` + `docs/pipeline-dsl/overview.md`.
- **Empty-string `entity_id` sentinel is implementation-leaking through the wire.**
  Alternatives considered:
  - **Dedicated `OP_GET_GLOBAL` opcode** — REJECTED. Adds wire surface (new opcode
    table entry; new schema; new SDK method); doesn't compose with `OP_BATCH_GET`.
  - **Special header bit** (e.g., `entity_id=null` instead of `""`) — REJECTED.
    Adds parsing complexity (null vs missing vs empty distinction) without
    benefit; JSON null values are already a footgun in cross-language ports.
  - **Empty-string `entity_id`** — CHOSEN. Simplest mechanism that works; no new
    wire surface; composes naturally with `OP_BATCH_GET` (a heterogeneous batch
    can mix per-entity and global lookups by entity_id alone); matches the
    "global is just another entity" mental model.
- **`App.get` arity overload** — `app.get(table)` (1 arg) vs
  `app.get(table, entity_id)` (2 args). Must enforce that global tables are
  queried with 1 arg and per-entity tables with 2 args (raise `KeyError` on
  mismatch). SDK ergonomics test in Plan 16 (`test_global.py::test_get_arity_mismatch`).
- **Adds a code path the engine must never break** — global-table state could
  grow unbounded if users put `bv.count(window="forever")` on a global table.
  However, standard memory governance applies:
  - `cold_after=` doesn't make sense for global (sentinel `""` is always-live).
  - Lifetime ops still subject to V0-MEM-GOV-02 lifetime-bound enforcement.
  - `top_k(k=N)` and other `BoundedByConfig` ops have their normal caps.
  - The single state slot per global table is bounded by the per-table state
    size (~hundreds of bytes for `BoundedSketch` ops at most), independent of
    entity count.

### Implementation deferral by phase

**Phase 13.4** (engine, ~30 LOC):

- Sentinel routing in `apply_shard.rs::dispatch_*_sync` — empty `entity_id`
  treated as a regular hashmap key (no special-case branch needed; the
  existing `&str` key path handles `""` natively).
- Register validation in `register_validate.rs` — accept `key_cols: []` as a
  valid global-table declaration.
- Architectural-test allowlist: ADR-001's existing deferred update for
  `phase12_7_no_table_surface.rs` (permitting `OpNode::Table*` on derivation
  `output_kind=table`) covers this — global tables are still aggregation-output
  decorators per ADR-001. **No new allowlist change needed.**

**Phase 13.5** (Python SDK, ~110 LOC):

- Public `bv.lit` export in `python/beava/__init__.py` (~5 LOC).
- `events.group_by()` empty allowance in `python/beava/_events.py:170-172`
  — flip the rejection to acceptance + new GroupBy state with empty
  `_keys` tuple (~10 LOC).
- `events.agg(**aggs)` direct shorthand on EventSource / EventDerivation
  classes (~30 LOC; reuses existing `GroupBy.agg(...)` machinery via the empty
  group_by path).
- `@bv.table` decorator factory accepts no-`key=` form — ~15 LOC change in the
  decorator factory; delegates to the same derivation-node JSON construction
  as the keyed path with `key_cols: []`.
- `App.get(table_name)` 1-arg overload — ~30 LOC in client + transport
  (server-side handles the empty-string entity_id; client just shapes the
  request).
- Tests in Plan 16 `test_global.py` exercise all the above — Phase 13.5 must
  make those tests pass (current `_engine_available()` SKIP gate flips when
  Phase 13.4 + 13.5 land together).

**Phase 13.6** (TS + Go SDKs, ~150 LOC across both):

- TypeScript: `bv.lit(value)` factory; `events.groupBy()` empty allowance;
  `events.agg({...})` direct; `app.get("table")` overload.
- Go: `bv.Lit(value)` factory; `events.GroupBy()` empty allowance;
  `events.Agg(map[string]bv.AggOp{...})` direct; `app.GetGlobal("table")`
  (Go's typing convention favors a separate method over arity overload —
  Go's type system makes "1 arg vs 2 args" awkward without overloading).

## Related

- **ADR-001** (`@bv.table` partial overturn): global tables are still
  aggregation-output decorators — no new write verbs (`upsert`/`delete`/`retract`
  STAY KILLED). Global-agg is a key-shape variation (`key_cols=[]` vs
  `key_cols=[...]`), not a semantic widening of the table contract.
- **ADR-002** (Polars op renames): Polars-aligned naming holds.
  `events.agg(...)` direct shorthand is itself a Polars-aligned ergonomic
  addition — Polars `df.agg(...)` is the canonical whole-frame aggregation form.
- **`project_redis_shaped_no_event_time_ever`**: global-agg uses server
  `now_ms()` for window semantics, same as per-entity (no event-time surface).
- **`project_v0_events_only_scope`** (with ADR-001 partial overturn): global
  tables are aggregation-output; events-only invariant holds.
- **REQUIREMENTS.md anchors:** `V0-GLOBAL-AGG-01` (engine sentinel routing),
  `V0-GLOBAL-AGG-02` (SDK no-key form), `V0-LIT-01` (public expression literal
  export).
- **Plan 13.0-16 acceptance tests:** `python/tests/v0/test_global.py` (8 tests)
  + `python/tests/v0/test_lit.py` (5 tests) form the failing-acceptance gate
  for Phase 13.4 + 13.5 implementation.

---

*ADR-003 — accepted 2026-05-03 mid-execution Phase 13.0 per user directive
("ship both / do both"). Plan 13.0-15 closure documents the design and lands
the doc patches; Phases 13.4 / 13.5 / 13.6 implement the engine + SDK surface.*
