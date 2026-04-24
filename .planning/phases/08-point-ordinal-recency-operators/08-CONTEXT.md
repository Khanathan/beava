# Phase 8: Point / ordinal / recency operators — Context

**Captured:** 2026-04-23 (auto mode, executed by orchestrator agent)
**Branch:** `worktree-agent-a5c71a97` (from v2/greenfield @ 157630f)
**Status:** Ready for planning

## Phase boundary

Ship 15 stateful, deterministic, single-event-update operators on top of the
Phase 5 `AggOp` framework + WindowedOp ring buffer:

| # | Operator | Family |
|---|----------|--------|
| 1 | first | point/ordinal |
| 2 | last  | point/ordinal |
| 3 | first_n | point/ordinal |
| 4 | last_n  | point/ordinal |
| 5 | lag | point/ordinal |
| 6 | first_seen | recency |
| 7 | last_seen  | recency |
| 8 | age | recency |
| 9 | has_seen | recency |
| 10 | time_since | recency |
| 11 | time_since_last_n | recency |
| 12 | streak | recency / streak |
| 13 | max_streak | recency / streak |
| 14 | negative_streak | recency / streak |
| 15 | first_seen_in_window | recency (windowed) |

Plus folded scope:

- **TCP `OP_PUSH` handler** — shipped here so Phase 8+ throughput rows can
  measure the TCP path, not just HTTP.
- **Throughput run** appended to `.planning/phases/08-name/08-throughput-row.md`
  (orchestrator merges to `.planning/throughput-baselines.md`).
- **Per-phase criterion microbench** appended to
  `.planning/phases/08-name/08-perf-row.md`.

Out of scope: SDK Python helpers for the new ops (Phase 9-13 will batch SDK
work). Server-side correctness + JSON wire are sufficient for the brief —
v0 SDK helpers can be folded in a later sweep without breaking anything.

> **2026-04-23 update from execution:** Time pressure ⇒ SDK Python helpers
> *are* shipped here as thin descriptor constructors so the round-trip path
> (Python → JSON → server → query) is exercisable. They follow the v1
> `bv.first(field)`, `bv.lag(field, n)`, etc. signatures from
> `git show main:python/beava/_agg_ops.py`.

## Architecture decisions

### D-01 — Operator dispatch model: extend `AggKind` enum + `AggOp` enum

Phase 5 chose enum-dispatch (D-01 in 05-CONTEXT) for `AggOp` to avoid
`Box<dyn AggOp>` virtual-dispatch overhead on the hot path. Phase 8 stays in
the same enum — adding 15 variants to `AggKind` and 15 variants to `AggOp`,
each with its small `*State` struct in `agg_state.rs`. This keeps the apply
loop's match-arm dispatch monomorphic and means snapshot serialization
(bincode through `serde`) "just works".

### D-02 — All Phase 8 operators are LIFETIME by default

The 15 ops in this phase are point/ordinal/recency/streak — they are
fundamentally about "what was the value the first/last time we saw an
event for this entity" or "how many consecutive matching events". They do
NOT need a tumbling-bucket ring buffer (the only one that does is
`first_seen_in_window`, see D-03).

For consistency with Phase 5, *some* of these ops still accept a `window`
parameter through `WindowedOp` (`first/last` per window, `streak` per
window, etc.) but the **default** behavior is lifetime. Wrapping in
`WindowedOp` would require per-bucket combine logic for each new op —
significant code, and the v1 reference doesn't expose `window=` for these
ops either. **Decision:** Phase 8 ships LIFETIME-only for all 15 ops EXCEPT
`first_seen_in_window` (which is the only windowed one in the spec list).
SDK helpers do not accept a `window=` kwarg.

### D-03 — `first_seen_in_window` semantics

"Was this entity ever seen in the last `window`?" — returns Bool.
Implementation: maintain `last_event_time_ms: i64`; on query
`age = query_time_ms - last_event_time_ms`; return
`Bool(age >= 0 && age < window_ms)`. This is a lifetime-state operator
that *parameterizes* on a window duration but doesn't need bucketing
because the answer is "was the most-recent event within window".

### D-04 — Determinism: event_time_ms only, no SystemTime::now

All Phase 8 ops follow Phase 5 D-06: `event_time_ms` is the only time
source. Recency ops compute `query_time_ms - last_event_time_ms` where
`query_time_ms` comes from the caller (the query path).

For lifetime queries where no events have been observed:
- `first/last/lag/first_n/last_n` → `Value::Null`
- `first_seen/last_seen` → `Value::Null` (Datetime when set)
- `age/time_since/time_since_last_n` → `Value::Null` (I64 ms when set)
- `has_seen` → `Value::Bool(false)`
- `streak/max_streak/negative_streak` → `Value::I64(0)`
- `first_seen_in_window` → `Value::Bool(false)`

### D-05 — `where_expr` semantics for streak ops

The brief lists `streak`, `max_streak`, `negative_streak`. v1 doesn't
implement these. Defining the contract here:

- **streak**: current count of consecutive matching events (i.e.
  `where_matched=true` for every event since the last non-matching). Reset
  to 0 when an event arrives with `where_matched=false`. If no
  `where_expr`, every event matches → equals total event count.
- **max_streak**: high-watermark of `streak` over the entity's lifetime.
- **negative_streak**: current count of consecutive non-matching events.
  Symmetric to `streak`. Reset to 0 when a matching event arrives.

These three operators only make pragmatic sense with `where_expr` — but the
implementation must work without one (degenerate cases noted above).

### D-06 — `lag(field, n)` semantics

Returns the value of `field` from the n-th-most-recent event (1-indexed:
`lag(field, 1)` = previous event's value before the most recent).

Implementation: bounded `VecDeque<Value>` of capacity n+1; on every event,
push the current value, pop front if length > n+1; on query, return
element at index `len - 1 - n` if it exists (or Null).

Snapshot size: O(n) per (entity, feature). OK for small n. The descriptor
must validate `n` at register time (reject n=0 or n > 1024 to keep memory
bounded).

### D-07 — `first_n` / `last_n` semantics

- **first_n(field, n)**: first n distinct event values seen, in arrival
  order. Stop appending after n. Returns `Value::Bytes(serde_json::encode)`?
  No — the v1 reference returns `list`. We return a JSON-encoded array as
  a `Value::Str` to keep wire stable without expanding the `Value` enum.

  > **Re-decision after looking at row.rs**: `Value` is the wire type and
  > already supports `Bytes(Vec<u8>)`. But list-of-values doesn't fit any
  > variant cleanly. **Choice:** add `Value::List(Vec<Value>)` would be
  > the cleanest design, but it ripples through every consumer (display,
  > codec, expr eval, schema). Pragmatic call: return a JSON-array
  > **string** as `Value::Str(serde_json::to_string(&list))`. v1
  > `output_type_for` returned `list` — we document the wire encoding as
  > "JSON-array string" in the docs entry. Phase 12+ can introduce
  > `Value::List` properly when first-class list support is needed (e.g.
  > most_recent_n in Phase 11).

- **last_n(field, n)**: last n event values seen, in arrival order
  (oldest at index 0, newest at end). Bounded `VecDeque` of cap n; same
  JSON-array-string return.

### D-08 — `output_type_for` for new ops

Extends `agg_op::output_type_for`:

| Op | Output type |
|----|-------------|
| first, last, lag | inherit upstream `field` type |
| first_n, last_n | `FieldType::Str` (JSON-array encoded; see D-07) |
| first_seen, last_seen | `FieldType::Datetime` |
| age, time_since, time_since_last_n | `FieldType::I64` (ms) |
| has_seen, first_seen_in_window | `FieldType::Bool` |
| streak, max_streak, negative_streak | `FieldType::I64` |

### D-09 — JSON wire mapping (compile-side)

Extends `agg_compile::parse_agg_kind`. The op-name strings:
`"first", "last", "first_n", "last_n", "lag", "first_seen", "last_seen",
"age", "has_seen", "time_since", "time_since_last_n", "streak",
"max_streak", "negative_streak", "first_seen_in_window"`.

`first_n` / `last_n` / `lag` consume `params.n: u32`.
`first_seen_in_window` consumes `params.window: str`.
All others have no extra params.

To carry `n`, we extend `AggOpDescriptor` with `n: Option<u32>`. To carry
the windowed-recency window, we already have `window_ms: Option<u64>`.

Field requirements:
- `first/last/lag/first_n/last_n` need a field
- `streak/max_streak/negative_streak` typically use `where=` only (no
  `field`); compile validates `where=` is present
- All others (recency markers) need NO field

### D-10 — Apply-loop integration

Extend `apply_event_to_aggregations` is **not** required — the apply path
already calls `AggOp::update_with_row(row, event_time_ms, field,
where_expr)` for every feature. The new `AggOp` variants just add their
own `update_with_row` arms via the `match self` in `agg_op.rs`.

For `streak`-family ops, `update_with_row` MUST evaluate `where_expr`
itself (rather than a single `where_matched: bool` from the outer match)
because the streak counters care about *every* event including
non-matching ones. This already works the same way as Ratio.

### D-11 — Snapshot evolution

Adding new `AggKind` / `AggOp` variants is an additive change. The
`SnapshotBody` codec uses bincode through `serde`. Any snapshot taken
before Phase 8 cannot deserialize a Phase 8 `AggOp`, but it never has to
(snapshots are taken AFTER registration; you can't have a Phase 8 op in a
Phase 7 snapshot). New code reading new snapshots: works. Old code
reading new snapshots: tooling-only concern. No migration needed.

### D-12 — TCP OP_PUSH handler design

The HTTP push handler in `crates/beava-server/src/push.rs` is the
canonical reference. The TCP handler:

- Reads a single MessagePack body (CT=0x02) per frame:
  `{"event": "<event_name>", "body": {<event_payload>}}`
  (We could mirror the URL-path event_name from HTTP, but TCP frames
  don't have a "path" — embedding the event name in the payload is the
  natural fit.)

  > **Trade-off:** JSON (CT=0x01) is also accepted for parity with HTTP.
  > MessagePack support reserved here matches the wire.rs roadmap.
- Reuses `execute_push` extracted from the HTTP handler so logic is
  shared (mirrors `execute_register` from Phase 2.5).
- On success, emits a frame with `op=OP_PUSH`, ct=JSON, payload =
  the same `PushAck { ack_lsn, idempotent_replay, registry_version }`
  the HTTP path returns.
- On error, emits an `op=OP_ERROR_RESPONSE` frame with the standard
  `{error: {code, ...}, registry_version}` shape.

### D-13 — Server `AppState` access on TCP path

Phase 2.5 wired the TCP `accept_loop` with only `Arc<Registry>`, not
`Arc<AppState>`. Phase 8 widens the parameter to `Arc<AppState>` (which
already wraps a `Registry` reference) so the TCP handler can reach
`wal_sink + idem_cache + dev_agg.state_tables` exactly the way the HTTP
push handler does. `register_handler` continues to read only what it
needs from the registry.

### D-14 — Microbench coverage

One criterion bench under `crates/beava-core/benches/agg_op_phase8.rs`
covering `update_with_row` for the 15 new ops. Pattern follows Phase 5's
`agg_op` bench (`benches/agg_op.rs`). Each variant: build the AggOp,
fire 1k events through it, measure ns/event.

### D-15 — Throughput run pipelines

Re-use Phase 7.5's small/medium/large configs UNCHANGED for the baseline
comparison. Add ONE new pipeline `phase8.json` that exercises the new
operators (e.g., `streak + max_streak + last_seen + age + first` on the
same Txn event) so the throughput row reflects "small + Phase 8 ops".
This new shape is appended to the harness's pipeline-config directory.

### D-16 — Plan structure (TDD red→green per task)

Plan splits into 5 atomic tasks (each red→green commit pair):

1. **Plan 08-01** — Extend `AggKind`/`AggOp`/`AggOpDescriptor` with point
   ops (first, last, first_n, last_n, lag) + `output_type_for` + JSON
   wire compile + per-state structs + per-op tests.
2. **Plan 08-02** — Add recency markers (first_seen, last_seen, age,
   has_seen, time_since, time_since_last_n) — they share a small State
   struct of `(first_seen_ms, last_seen_ms, n_seen, last_n_seens_ring)`.
3. **Plan 08-03** — Add streak family (streak, max_streak,
   negative_streak) and `first_seen_in_window`.
4. **Plan 08-04** — TCP `OP_PUSH` handler — extract `execute_push` from
   HTTP, wire MessagePack-or-JSON dispatch on TCP, integration smoke test
   for HTTP+TCP equivalence.
5. **Plan 08-05** — Criterion bench for the 15 ops + `phase8.json`
   pipeline + run the throughput harness + write `08-perf-row.md` and
   `08-throughput-row.md`. Docs entries appended to
   `crates/beava-core/src/agg_op.rs` rustdoc and a sketch
   `docs/operators.md` (created).

## Canonical references

- `.planning/ROADMAP.md` § Phase 8 — goal + 4 success criteria + 15 ops list
- `crates/beava-core/src/agg_op.rs` — Phase 5 enum dispatch + output_type_for
- `crates/beava-core/src/agg_state.rs` — per-op state struct pattern to follow
- `crates/beava-core/src/agg_windowed.rs` — WindowedOp ring buffer pattern
- `crates/beava-core/src/agg_compile.rs` — `parse_agg_kind` + JSON-wire validation
- `crates/beava-server/src/push.rs` — HTTP push handler (canonical TCP reference)
- `crates/beava-server/src/tcp.rs` — TCP accept loop + dispatch + handler pattern
- `crates/beava-bench/` — throughput harness; pipeline configs in `configs/`
- `git show main:python/beava/_agg_ops.py` — v1 SDK signatures (first/last/first_n/last_n/lag)
- `git show main:src/engine/operators.rs` — v1 LastOp/LagOp Rust impls (lines 470, 1058)

## Plan-checker contract

- Phase 6+ requires at least one task with `files_modified` under
  `crates/*/benches/`. **Plan 08-05 satisfies this** with
  `crates/beava-core/benches/agg_op_phase8.rs`.
- Phase 8+ requires at least one "throughput run" task. **Plan 08-05
  satisfies this** with `.planning/phases/08-point-ordinal-recency-operators/08-throughput-row.md`.
