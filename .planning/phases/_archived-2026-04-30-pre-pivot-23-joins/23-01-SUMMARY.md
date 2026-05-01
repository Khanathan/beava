---
phase: 23-joins
plan: 01
subsystem: engine+register+pipeline
tags: [stream-table-join, enrichment, composite-group-by, _right-suffix, cascade]
dependency_graph:
  requires:
    - 22-04  # v0 TCP REGISTER dispatch + v0→v2 translator
    - 21-03  # SDK Stream.join + JoinSpec contract
  provides:
    - JOIN-ST              # Stream↔Table enrichment end-to-end
    - GB-COMPOSITE         # composite group_by keys
    - PIPELINE-EFFECTIVE-EVENTS  # per-stream synthesized event in cascade
  affects:
    - src/engine/pipeline.rs   # FeatureDef::EnrichFromTable + JoinType + cascade
    - src/engine/register.rs   # JoinSpec typed; v0_join_to_stream_def; composite gb
    - src/server/tcp.rs        # REGISTER Join branch wired with left-fields lookup
    - src/server/http.rs       # /pipelines/:name renders new variant
    - src/server/protocol.rs   # StreamDefinition default-fill for v2.0 path
    - src/state/eviction.rs    # constructor backfill
tech-stack:
  added: []
  patterns:
    - per-stream-effective-event-in-cascade  # join derivations synthesize event
    - left-fields-lookup-closure-at-register  # avoids piping schemas through
    - encode_group_by-everywhere              # composite + single-key fast-path
    - sdk-applied-suffix-engine-passthrough   # _right collision rename owned by SDK
key-files:
  created:
    - tests/test_composite_group_by.rs
    - tests/test_join_stream_table.rs
    - python/tests/test_v0_stream_table_join.py
  modified:
    - src/engine/pipeline.rs
    - src/engine/register.rs
    - src/server/tcp.rs
    - src/server/http.rs
    - src/server/protocol.rs
    - src/state/eviction.rs
decisions:
  - "StreamDefinition.group_by_keys is the canonical composite-key carrier; key_field still points at keys[0] for read paths that want a representative scalar field name. encode_group_by drives the actual entity key used in push_internal."
  - "EnrichFromTable executes inside push_with_cascade_internal, not push_internal — it synthesizes an effective event for the rest of the cascade subtree and is otherwise stateless. Inner-miss adds the join derivation to a `dropped` set so its entire downstream subtree is skipped for that push."
  - "Right-side row lookup reads `static_features` only (Tables in v0 are overwrite-mode current-state). Live operators on the right side are out of scope for stream_table — that's stream_stream territory (Plan 23-02)."
  - "_right collision suffix is owned by the SDK (`compute_joined_schema` in `_join.py`). The engine emits emitted_name verbatim from `right_fields` and refuses to clobber a pre-existing left field of the same name (defense in depth for T-23-03)."
  - "Translator partitions the output schema by consulting the LEFT source's previously-stored raw register JSON via a closure injected at TCP REGISTER time. Without left-schema knowledge the translator falls back to a conservative heuristic (only `_right`-suffixed names are right-side)."
  - "Outer joins rejected at the engine layer with the SDK's exact 'deferred to v0.1' message (T-23-04). Tests cover both SDK and engine paths."
  - "stream_stream and table_table shapes return clear 'deferred to 23-02 / 23-03' Protocol errors at the translator. Plans 23-02 and 23-03 replace these stubs."
metrics:
  duration: ~1.5h
  completed: 2026-04-14
  tasks: 3
  commits:
    - a06f4d1  # composite group_by + test harness
    - 6e08a68  # Stream↔Table enrichment + 6 tests
    - 5d7307c  # pytest TCP round-trip
---

# Phase 23 Plan 01: Stream↔Table enrichment + composite group_by — Summary

**One-liner:** Shipped Stream↔Table enrichment joins (inner + left)
with `_right` collision passthrough, lifted the Phase 22-04 composite
group_by rejection (`encode_group_by` is now the canonical key encoder
on the hot path), and stubbed `stream_stream` / `table_table` for
Plans 23-02 / 23-03 — all behind the v0 REGISTER `Join` payload that
Phase 21 already serializes.

## What shipped

### 1. Composite group_by keys (commit `a06f4d1`)

`StreamDefinition` gained `group_by_keys: Option<Vec<String>>`. The
v0 translator (`v0_aggregation_to_stream_def`) drops the 22-04
"composite not yet supported" rejection and populates `group_by_keys`
when `aggregation.keys.len() > 1`. `push_internal` derives the entity
key via `encode_group_by(keys, event)` when `group_by_keys` is `Some`,
preserving the single-key fast path because `encode_group_by` of a
one-element slice returns just the scalar string.

`v0_source_to_stream_def` also honors composite `key_fields` for Table
sources so Stream↔Table joins on composite keys can SET / lookup
under `"u1|US"`-style keys consistently.

`push_with_cascade_internal` was updated to check every group_by key
when deciding whether to skip a downstream that doesn't have its key
fields populated (the "key missing → skip" gate from the original
single-key implementation).

`StreamDefinition` now has an `impl Default` so future fields can be
added with `..Default::default()` instead of patching N callsites. The
108 existing struct-literal constructors were mechanically extended
with `group_by_keys: None,` via a Python pass — pure additive change,
no behavior delta.

**Tests:** `tests/test_composite_group_by.rs` (5/5 passing):

  * `composite_keys_register_accepted` — REGISTER no longer rejects.
  * `composite_keys_bucket_independently_and_merge_on_match` — three
    events under (u1, m1) / (u1, m2) bucket into the right composite
    rows; the third event correctly merges into row #1.
  * `composite_keys_missing_field_errors` — encode_group_by still
    raises a typed error on missing key field.
  * `single_key_encode_fast_path_unchanged` — encode_group_by of a
    one-element slice returns the scalar string.
  * `single_key_engine_dispatch_unchanged` — single-key aggregation
    keys state under the plain key, not a piped composite.

### 2. Stream↔Table enrichment join (commit `6e08a68`)

Added `FeatureDef::EnrichFromTable { right_table, on, join_type,
right_fields }` and `JoinType { Inner, Left }` to `pipeline.rs`. Made
`JoinDescriptor.join` typed as `JoinSpec` (was `serde_json::Value`).
Added `v0_join_to_stream_def(desc, left_fields_lookup)` to
`register.rs`.

**Cascade execution:** `push_with_cascade_internal` now keeps a
per-cascade `effective_events: AHashMap<String, serde_json::Value>`
and `dropped: AHashSet<String>`. When the topo walk reaches a
downstream that has an `EnrichFromTable` feature:

  1. Compose the right-side key via
     `encode_group_by(&on, &effective_event)` (composite-aware).
  2. Snapshot the right table's `static_features` map at that key.
  3. **Inner + miss** → add the join derivation to `dropped`. Any
     downstream whose `depends_on` contains this stream is skipped for
     this push.
  4. **Left + miss** → fill right-side fields with `Value::Null`.
  5. **Hit** → overlay right-side values into a cloned event and
     publish it as the effective event for the rest of the subtree.

Downstream aggregations / derives in the same cascade then see the
enriched event when they push, so an aggregation keyed on a right-side
field (e.g. `country`) materializes correctly only when the
enrichment populated that field.

**TCP REGISTER wiring:** the `Join` branch in `tcp.rs` resolves the
left source's field schema by looking up `engine.get_raw_register_json(left)`
and passes it to the translator as a closure. The translator
partitions output fields:

  * names in `desc.join.on` → join keys (skip; come from left event)
  * names with `_right{N?}` suffix → unambiguously right-side rename
  * names known to be in left's schema → skip (already on event)
  * everything else → right-side passthrough (source_name == emitted)

**HTTP `/pipelines/:name`** renders the new variant as
`{"type":"enrich_from_table", right_table, on, join_type, right_fields}`.

**Tests:** `tests/test_join_stream_table.rs` (6/6 passing):

  * `enrich_inner_hit` — Clicks ⋈ UserProfile inner; downstream agg
    materializes count=1 only when the right row exists.
  * `enrich_inner_miss_drops` — no right row → downstream count=0.
  * `enrich_left_miss_nulls` — type=left + miss → downstream count=1
    (event still cascades).
  * `enrich_collision_suffix` — both sides have `status` field; right
    arrives as `status_right`; aggregation keyed on (user_id,
    status_right) confirms left's `status` is preserved unchanged
    while right's value lands in the suffixed slot.
  * `enrich_composite_key` — composite (user_id, region) join; both
    hit (u1|US) and left-miss (u1|EU) bucket correctly.
  * `enrich_rejects_outer` — translator returns the SDK's exact
    "outer joins deferred to v0.1" Protocol error.

`stream_stream` and `table_table` shapes return clear "Plan 23-02 /
23-03" Protocol stubs at the translator.

### 3. Python TCP round-trip pytest (commit `5d7307c`)

`python/tests/test_v0_stream_table_join.py` (3/3 passing) drives the
live `tally_server` fixture end-to-end:

  * Single-key Clicks ⋈ UserProfile (left) — assert downstream agg
    materializes for both u1 (right-row exists) and u2 (left-miss).
  * Composite-key CompClicks ⋈ CompProfile (left) keyed on
    (user_id, region) — assert hit (u1|US) and left-miss (u1|EU)
    both flow through.
  * Offline: SDK rejects `type="outer"` before any TCP round-trip.

No SDK code changed (Phase 21-03 contract was frozen).

## Test results

  * `cargo test --lib` — **678 passed**, 0 failed (no regression from 22-04).
  * `cargo test --test test_composite_group_by` — **5 passed**.
  * `cargo test --test test_join_stream_table` — **6 passed**.
  * `cargo test --test test_register_json_v0` — **21 passed** (no regression).
  * `cargo test` (all integration binaries) — every binary green.
  * `pytest python/tests/test_v0_stream_table_join.py` — **3 passed**.
  * `pytest python/tests/` — **408 passed, 2 skipped** (unrelated; same
    skip count as pre-23-01).

## Deviations from plan

### [Rule 3 - Blocking issue] `JoinDescriptor.join` was untyped `Value`

**Found during:** Task 2 setup.

**Issue:** The `<interfaces>` block in 23-01-PLAN.md documented a
typed `JoinSpec` already on `JoinDescriptor.join`, but the actual code
declared `pub join: serde_json::Value` (parsed lazily). The plan's
suggested "use `desc.join.right` etc." would not compile.

**Fix:** Added `pub struct JoinSpec { op, left, right, on, within,
type_, shape }` (matching `_join.py::JoinSpec._to_join_json()`) and
changed `JoinDescriptor.join` to be typed. `op` is `#[serde(default)]`
so existing test fixtures in register.rs that omit it (the legacy
`parse_join` test was added in Phase 22-01) still parse. No external
callers — only `register.rs` consumed the field.

### [Rule 2 - Missing critical functionality] `StreamDefinition` had no `Default` impl

**Found during:** Task 1 — adding `group_by_keys` would have required
patching every struct-literal constructor in src + tests (108 sites)
even before any new code logic. Without `Default`, future plans that
add another optional field face the same churn.

**Fix:** Added `impl Default for StreamDefinition { ... }` at
declaration site. Existing call sites still use struct literals (a
mechanical Python pass added `group_by_keys: None,` after each
`key_field:` line); but going forward, new fields can be added with
`..Default::default()` syntax without churn.

### [Rule 1 - Bug fix] cascade key-missing skip was single-key-only

**Found during:** Task 1 — composite group_by aggregation downstream
of a Source. The original cascade gate at `push_with_cascade_internal`
only checked `event.get(key_field)` for the single-key path. With
`group_by_keys: Some`, every key field needs to be checked; otherwise
a composite agg would silently drop events that have all keys
present (because the gate would skip on missing `key_field`-named
field that no longer matched composite semantics).

**Fix:** New `keyed_ready` block checks `group_by_keys` first
(`all(...)`) and falls back to single-key `key_field` matching.

### [Rule 1 - Bug fix] downstream cascade used original event, not enriched

**Found during:** Task 2 — aggregation downstream of an
`EnrichFromTable` derivation. The pre-23-01 cascade always passed the
ORIGINAL event to every downstream. For Stream↔Table enrichment, the
right-side fields must materialize INTO the event so downstreams can
group_by them.

**Fix:** Per-stream `effective_events` map; downstream consults
`depends_on` and uses the enriched event from any upstream that
synthesized one. `dropped` set propagates inner-miss cuts through the
DAG (any stream whose upstream dropped is also skipped).

### Auth gates

None — the plan executed entirely against the in-process engine
fixture (Tasks 1, 2) and the existing pytest server fixture (Task 3).
No new credentials, no external services.

## Known stubs

| File | Location | Resolved by |
|------|----------|-------------|
| `src/engine/register.rs` | `v0_join_to_stream_def` returns Protocol error for `shape="stream_stream"` with explicit "Plan 23-02 ships Stream↔Stream" pointer | Plan 23-02 |
| `src/engine/register.rs` | `v0_join_to_stream_def` returns Protocol error for `shape="table_table"` with explicit "Plan 23-03 ships Table↔Table" pointer | Plan 23-03 |
| `src/engine/register.rs` | translator's left-field-lookup heuristic falls back to "only `_right`-suffixed names are right-side" when the left source's schema is not registered yet. The TCP wire path always supplies the lookup, so production calls are precise. | Acceptable as-is — direct in-process callers (tests) supply the lookup explicitly. |

All stubs surface clear runtime errors pointing at the resolving plan.

## Threat flags

None new. The plan's threat register (T-23-01..T-23-04) is fully
mitigated:

  * T-23-01 (tampering) — translator validates `desc.join.on` is
    non-empty; `encode_group_by` raises typed errors for missing/
    non-scalar key fields.
  * T-23-02 (DoS) — accepted; enrichment is O(1) per event.
  * T-23-03 (info disclosure via `_right`) — engine emits emitted_name
    verbatim from `right_fields`; refuses to clobber a pre-existing
    left field of the same name. Suffix logic owned by the SDK.
  * T-23-04 (outer smuggling) — engine rejects `type="outer"` at the
    translator with the exact SDK message; covered by the
    `enrich_rejects_outer` Rust test AND the
    `test_stream_table_outer_rejected_at_register` SDK test.

## Self-Check: PASSED

Verified files exist (absolute paths):

  * `/data/home/tally/tests/test_composite_group_by.rs` — FOUND (178 lines)
  * `/data/home/tally/tests/test_join_stream_table.rs` — FOUND (348 lines)
  * `/data/home/tally/python/tests/test_v0_stream_table_join.py` — FOUND (113 lines)
  * `/data/home/tally/src/engine/pipeline.rs` — FOUND (modified: +332 net)
  * `/data/home/tally/src/engine/register.rs` — FOUND (modified: +210 net)
  * `/data/home/tally/src/server/tcp.rs` — FOUND (modified: +23 net)
  * `/data/home/tally/src/server/http.rs` — FOUND (modified: +3)
  * `/data/home/tally/src/server/protocol.rs` — FOUND (modified: +1)
  * `/data/home/tally/src/state/eviction.rs` — FOUND (modified: +3)
  * `/data/home/tally/.planning/phases/23-joins/23-01-SUMMARY.md` — FOUND (this file)

Verified commits exist on main:

  * `a06f4d1` feat(23-01): unblock composite group_by keys + test harness
  * `6e08a68` feat(23-01): Stream↔Table enrichment join (inner+left, _right suffix)
  * `5d7307c` test(23-01): Stream↔Table join TCP round-trip pytest cases

Verified test gates (last run):

  * `cargo test --lib` — 678 / 678
  * `cargo test --test test_composite_group_by` — 5 / 5
  * `cargo test --test test_join_stream_table` — 6 / 6
  * `cargo test --test test_register_json_v0` — 21 / 21 (regression guard)
  * `cargo test` (all binaries) — every binary green
  * `pytest python/tests/test_v0_stream_table_join.py` — 3 / 3
  * `pytest python/tests/` — 408 passed, 2 skipped

Phase 23 Plan 01 is complete. Plans 23-02 (Stream↔Stream) and 23-03
(Table↔Table) can build on the typed `JoinSpec`, the cascade
`effective_events` mechanism, and the `EnrichFromTable` translator
scaffolding shipped here.
