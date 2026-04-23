---
phase: 05-aggregation-framework-core-operators
plan: "06"
subsystem: feature-query
tags: [http, aggregation, feature-index, query-endpoints, tdd]
dependency_graph:
  requires: [05-05]
  provides: [feature-query-endpoints, feature-index, cross-agg-collision-rule]
  affects: [beava-core/registry, beava-server/http, beava-server/feature_query]
tech_stack:
  added: []
  patterns:
    - feature_index reverse-map on RegistryInner for O(1) feature-name lookup
    - DevAggState.max_event_time_ms AtomicU64 for deterministic query time (D-06)
    - axum Router per-module pattern (feature_query_router)
key_files:
  created:
    - crates/beava-server/src/feature_query.rs
  modified:
    - crates/beava-core/src/registry.rs
    - crates/beava-core/src/register_validate.rs
    - crates/beava-core/src/agg_compile.rs
    - crates/beava-server/src/http.rs
    - crates/beava-server/src/register.rs
    - crates/beava-server/src/registry_debug.rs
    - crates/beava-server/src/server.rs
    - crates/beava-server/src/lib.rs
decisions:
  - "query time uses DevAggState.max_event_time_ms (AtomicU64) not wall-clock (D-06)"
  - "feature_query.rs is self-contained with its own value_to_json (no dep on registry_debug)"
  - "cross-agg collision check runs in agg_compile after per-node errors clear (fail-soft)"
  - "http::router gains 4th arg Option<DevAggState> for shared state injection"
metrics:
  duration_minutes: 45
  completed_date: "2026-04-23"
  tasks_completed: 2
  files_changed: 9
requirements: [SDK-AGG-02]
---

# Phase 05 Plan 06: Feature Query Endpoints Summary

GET /get/:feature/:key + POST /get batch query endpoints wired to DevAggState; feature_index rebuilt on every apply_registration for O(1) feature-name resolution; cross-aggregation feature-name collision enforced at register time.

## What Was Built

### feature_index on RegistryInner

`BTreeMap<String, (String, usize)>` added to `RegistryInner`. Rebuilt in `apply_registration` after every new batch of compiled aggregations (additive-only: first-registration wins). `Registry::resolve_feature(&str) -> Option<(String, usize)>` provides O(1) lookup.

### Cross-aggregation Feature-Name Collision Rule (Rule 11 extension)

`AggregationFeatureNameCollisionAcrossAggregations` added to `ErrorCode` enum. `compile_aggregations_from_nodes` in `agg_compile.rs` now checks:
1. Two new aggregations in the same payload both define the same feature name.
2. A new aggregation defines a feature name already registered by a different aggregation (via registry `feature_index`).

Wire string: `aggregation_feature_name_collision_across_aggregations`.

### GET /get/:feature/:key

Returns `{"value": <JSON>}` for present entity keys (D-02 envelope — value only). Returns 404 `feature_not_found` for unknown feature names; 404 `key_not_found` for valid feature with unseen entity key.

### POST /get batch

Accepts `{"keys": [...], "features": [...]}`. Validates all feature names upfront (400 `feature_not_found` with `missing` array). Enforces 10,000-cell cap (400 `batch_too_large`). Missing keys are omitted from result map (not null), per SRV-API-08.

### DevAggState.max_event_time_ms (D-06)

`Arc<AtomicU64>` added to `DevAggState`. Bumped via `fetch_max` in `post_dev_apply_events` on every event apply. `compute_query_time_ms` reads this field — never wall-clock. Grep guard test enforces `SystemTime::now` is absent from `feature_query.rs` production code.

### http::router Signature Extension

`router(readiness, registry, dev_endpoints, dev_agg_state: Option<DevAggState>)` — 4th arg allows caller to inject a shared `DevAggState` so `/dev/apply_events` and `/get` share the same state tables. All existing callers updated to pass `None` (backward-compatible).

## Commits

| Hash | Type | Description |
|------|------|-------------|
| `80d17ad` | test(05-06) | Failing tests: GET/POST /get, feature_index, collision rule (RED) |
| `bee606c` | feat(05-06) | Implementation: endpoints + feature_index + collision rule (GREEN) |

## Tests Added (13 new)

**feature_query module:**
- `get_endpoint_returns_value_for_present_entity`
- `get_endpoint_404_on_unknown_feature`
- `get_endpoint_404_on_unknown_key`
- `get_endpoint_handles_sum_feature_returning_float`
- `get_endpoint_respects_envelope_shape`
- `post_get_batch_returns_map_of_results`
- `post_get_batch_400_on_unknown_feature`
- `post_get_batch_omits_missing_keys`
- `post_get_batch_respects_cap_10k`
- `rule11_rejects_cross_aggregation_feature_name_collision`
- `get_windowed_count_uses_max_event_time`
- `envelope_purity_no_meta_key_in_production_code`
- `d06_no_system_time_now_in_production_code`

**registry module:**
- `resolve_feature_after_register`
- `resolve_feature_missing_returns_none`
- `resolve_feature_index_rebuilt_on_register`

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] Axum route pattern syntax**
- **Found during:** Task 1.b
- **Issue:** Plan specified `"/get/{feature}/{key}"` but axum 0.7 uses `":param"` syntax.
- **Fix:** Changed route to `"/get/:feature/:key"`.
- **Files modified:** `feature_query.rs`
- **Commit:** `bee606c`

**2. [Rule 2 - Missing functionality] max_event_time_ms tracking**
- **Found during:** Task 1.b
- **Issue:** `compute_query_time_ms` returned hardcoded 0, causing windowed counts to appear as 0 since events at time ~1_000_000ms are out-of-window when queried at time 0.
- **Fix:** Added `max_event_time_ms: Arc<AtomicU64>` to `DevAggState`; bumped in `post_dev_apply_events` and `push_events` test helper.
- **Files modified:** `registry_debug.rs`, `feature_query.rs`
- **Commit:** `bee606c`

**3. [Rule 3 - Blocking] Borrow conflict in apply_registration**
- **Found during:** Task 1.a
- **Issue:** `w.feature_index.entry(...).or_insert_with(...)` took a mutable borrow of `w` while `w.compiled_aggregations.iter()` held an immutable borrow.
- **Fix:** Collected new entries into a `Vec` first, then inserted without the conflicting borrow.
- **Files modified:** `registry.rs`
- **Commit:** `80d17ad` (fix in same session before red commit)

**4. [Rule 1 - Bug] http::router signature breaking all callers**
- **Found during:** Task 1.a
- **Issue:** Adding 4th arg broke all existing `router(...)` call sites (registry_debug.rs, register.rs, server.rs, http.rs tests).
- **Fix:** Updated all 20+ call sites to pass `None` as 4th arg.
- **Files modified:** `registry_debug.rs`, `register.rs`, `server.rs`, `http.rs`
- **Commit:** `80d17ad`

## Known Stubs

None — all endpoints are wired end-to-end.

## Threat Flags

None beyond what is already in the plan's threat model (T-05-06-01 through T-05-06-03).

## Self-Check: PASSED

- `crates/beava-server/src/feature_query.rs` exists: YES
- `pub mod feature_query` in `lib.rs`: YES
- `AggregationFeatureNameCollisionAcrossAggregations` in `register_validate.rs`: YES
- `feature_index` in `registry.rs`: YES
- `resolve_feature` in `registry.rs`: YES
- Commits `80d17ad` (red) and `bee606c` (green) exist: YES
- `cargo test --workspace`: 507 tests, 0 failures
- `cargo clippy -- -D warnings`: clean
- `cargo fmt --all --check`: clean
- `grep "SystemTime::now"` in production code: 0 matches
- `grep '"meta"'` in production code: 0 matches
