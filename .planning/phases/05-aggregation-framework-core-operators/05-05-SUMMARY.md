---
phase: 05-aggregation-framework-core-operators
plan: 05
subsystem: aggregation-apply-loop
tags: [aggregation, apply-loop, state-table, dev-endpoint, determinism, phase5]
dependency_graph:
  requires:
    - 05-04  # compiled_aggregations registry cache (AggregationDescriptor)
    - 05-02  # AggOp::update_with_row + where-predicate threading
    - 05-01  # AggOp enum dispatch
  provides:
    - apply_event_to_aggregations  # single-writer event dispatch hook
    - AggStateTable                # per-aggregation per-entity state store
    - EntityKey                    # canonical group-key tuple (BTreeMap-friendly)
    - POST /dev/apply_events       # dev endpoint exercising apply loop
    - Registry::compiled_aggregations_for_source  # source routing lookup
  affects:
    - 05-06  # GET /dev/query reads AggStateTable populated here
    - 06     # Phase 6 WAL push handler will call apply_event_to_aggregations with real event_id
tech_stack:
  added: []
  patterns:
    - BTreeMap-keyed entity state (D-06 determinism)
    - single-writer apply loop (Mutex on outer map only)
    - _event_id prefix convention for Phase 6 signature contract
key_files:
  created:
    - crates/beava-core/src/agg_state_table.rs
    - crates/beava-core/src/agg_apply.rs
  modified:
    - crates/beava-core/src/registry.rs   (compiled_aggregations_for_source)
    - crates/beava-core/src/lib.rs        (new pub mods)
    - crates/beava-server/src/registry_debug.rs  (DevAggState + /dev/apply_events)
    - crates/beava-server/src/http.rs     (dev_apply_events_router mount)
decisions:
  - "Entity row created before per-feature where-predicate evaluation (entity row IS created even if all features' where-predicates are false; individual feature state stays at default)"
  - "F64 group-key canonicalization uses format!(\"{:?}\", f) (Rust Debug repr) — I64(42) and F64(42.0) produce distinct strings (42 vs 42.0)"
  - "Bytes group-key value drops the event (None from EntityKey::from_row) — not a sane entity key in v0"
  - "_event_id parameter prefixed with _ in body but kept in signature; Phase 6 WAL will populate without caller churn"
  - "DevAggState.state_tables wrapped in parking_lot::Mutex (single outer lock); per-entity AggOp state is lock-free inside the lock window (single-writer invariant)"
metrics:
  duration_minutes: 45
  completed_date: "2026-04-23"
  tasks_completed: 2
  files_created: 2
  files_modified: 4
  tests_added: 23
  tests_total_workspace: 498
---

# Phase 5 Plan 5: Apply-Loop Hook + Per-Entity State Table + /dev/apply_events Summary

**One-liner:** Single-writer apply-loop hook routing events to per-entity BTreeMap state via `apply_event_to_aggregations`, with `DevAggState` + `POST /dev/apply_events` dev endpoint gated by `BEAVA_DEV_ENDPOINTS=1`.

## What Was Built

### `crates/beava-core/src/agg_state_table.rs`

`EntityKey` — canonical entity identifier:
- `from_row(group_keys, row) -> Option<EntityKey>` — extracts group-key values in declaration order; returns `None` for Null/missing/Bytes fields (event dropped)
- Canonicalization: `Str` → as-is, `I64(n)` → `n.to_string()`, `F64(f)` → `format!("{:?}", f)`, `Bool` → `"true"/"false"`, `Datetime(ms)` → decimal
- `I64(42)` and `F64(42.0)` produce distinct canonical strings — no entity key collisions across types

`AggStateTable` — per-aggregation state store:
- `BTreeMap<EntityKey, Vec<AggOp>>` — deterministic iteration (D-06)
- `get_or_init` — lazy-init entity row with one `AggOp::new` per feature
- `query_feature(key, feature_index, query_time_ms) -> Option<Value>`
- `entity_count() -> usize`
- Implements `Default` (delegates to `new()`)

### `crates/beava-core/src/agg_apply.rs`

`apply_event_to_aggregations(source_name, row, event_time_ms, _event_id, registry, state_tables)`:
- Iterates `registry.compiled_aggregations_for_source(source_name)` 
- Per aggregation: extracts `EntityKey`, drops on None, calls `get_or_init`, calls `update_with_row` per feature
- `_event_id: u64` — threaded but ignored in Phase 5; Phase 6 WAL populates via D-08
- Pure function: no wall-clock reads, no random sources, no `SystemTime::now` (D-06 grep guard test)

### `crates/beava-core/src/registry.rs`

`compiled_aggregations_for_source(source_name) -> Vec<Arc<AggregationDescriptor>>`:
- Filters `compiled_aggregations.values()` by `source_node_name == source_name`
- Used by apply loop to find all aggregations watching an incoming event's source

### `crates/beava-server/src/registry_debug.rs` + `http.rs`

`DevAggState` — shared state for dev endpoints:
- `state_tables: Arc<Mutex<BTreeMap<String, AggStateTable>>>` — single outer lock
- `registry: Arc<Registry>` — read-only reference
- `next_event_id: Arc<AtomicU64>` — monotonic counter for apply-loop event_id

`POST /dev/apply_events`:
- Request: `{ source, event_time_ms, row: {field: json_value} }`
- Response 200: `{ applied_to: [node_names] }` — lists aggregations whose source matched
- Response 404: `{ error: "source_not_found" }` — source not in registry
- Gated by `BEAVA_DEV_ENDPOINTS=1` (not mounted in production)
- Pulls monotonic event_id from `DevAggState.next_event_id` before calling `apply_event_to_aggregations`

## TDD Trace

| Commit | Type | Description |
|--------|------|-------------|
| `c114b40` | RED `test(05-05)` | 21 failing tests: EntityKey, AggStateTable, apply_event_to_aggregations stubs |
| `ead1ac8` | GREEN `feat(05-05)` | Full implementation; all 498 workspace tests pass |

## Tests Added (23)

**agg_state_table (11):** `entity_key_from_row_extracts_group_keys_in_order`, `entity_key_from_row_returns_none_on_null_field`, `entity_key_from_row_returns_none_on_missing_field`, `entity_key_normalises_numeric_values_deterministically`, `entity_key_returns_none_for_bytes_value`, `agg_state_table_get_or_init_creates_row_of_correct_arity`, `agg_state_table_get_or_init_returns_existing_on_repeat`, `agg_state_table_entity_count_counts_distinct_keys`, `agg_state_table_query_feature_returns_value`, `agg_state_table_query_feature_returns_none_for_unknown_key`, `agg_state_table_uses_btreemap`

**agg_apply (10):** `apply_routes_event_to_matching_source_only`, `apply_increments_count_feature`, `apply_drops_events_with_null_group_key`, `apply_with_where_false_skips_update`, `apply_replay_determinism`, `apply_multi_feature_aggregation_updates_all`, `apply_accepts_event_id_and_ignores_it_in_phase_5`, `no_systemtime_now_in_agg_apply`

**registry source tests (2):** `compiled_aggregations_for_source_returns_matching`, `compiled_aggregations_for_source_empty_for_unknown`

## Deviations from Plan

None — plan executed exactly as written.

The plan's note on where-predicate semantics ("document the chosen semantics") was resolved: entity row IS created before per-feature where evaluation (D-03 threading — `AggOp::update_with_row` gates per feature). This matches Plan 05-02's existing semantics.

## Known Stubs

None. All features are fully wired. `_event_id` is intentionally unused in Phase 5 (not a stub — it is a reserved parameter that Phase 6 will populate).

## Threat Surface Scan

| Flag | File | Description |
|------|------|-------------|
| threat_flag: dos_unbounded_entity_growth | crates/beava-core/src/agg_state_table.rs | No per-aggregation entity cap; documented accepted risk T-05-05-02 in plan |

The plan's threat model (T-05-05-02) explicitly accepts unbounded entity growth in v0. No new unmitigated threats beyond what the plan's threat register covers.

## Self-Check: PASSED

Files verified:
- `/Users/petrpan26/work/tally/crates/beava-core/src/agg_state_table.rs` — FOUND
- `/Users/petrpan26/work/tally/crates/beava-core/src/agg_apply.rs` — FOUND

Commits verified:
- `c114b40` — RED: test(05-05) — FOUND
- `ead1ac8` — GREEN: feat(05-05) — FOUND

`cargo test --workspace`: 498 passed, 0 failed
`cargo clippy --workspace --all-targets --all-features -- -D warnings`: clean
`cargo fmt --all --check`: clean
