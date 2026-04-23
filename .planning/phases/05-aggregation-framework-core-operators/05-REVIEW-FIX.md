---
phase: 05-aggregation-framework-core-operators
fixed_at: 2026-04-23T00:00:00Z
review_path: .planning/phases/05-aggregation-framework-core-operators/05-REVIEW.md
iteration: 1
findings_in_scope: 4
fixed: 4
skipped: 0
status: all_fixed
---

# Phase 5: Code Review Fix Report

**Fixed at:** 2026-04-23
**Source review:** .planning/phases/05-aggregation-framework-core-operators/05-REVIEW.md
**Iteration:** 1

**Summary:**
- Findings in scope: 4 (1 Critical, 3 Warnings)
- Fixed: 4
- Skipped: 0

## Fixed Issues

### CR-01: Zero-window string panics at apply time

**Files modified:** `crates/beava-core/src/agg_compile.rs`, `python/beava/_agg.py`
**Commit:** 21ca582
**Applied fix:**
- `parse_duration_to_ms`: added `if n == 0 { return Err(()); }` after parsing the numeric prefix, before the checked multiply. All zero-valued durations (`"0ms"`, `"0s"`, `"0m"`, `"0h"`, `"0d"`) now return `Err(())` and propagate as `AggregationInvalidWindow` errors at register time — the panic path in `WindowedOp::bucket_index` via `div_euclid(0)` is never reached.
- `_agg.py`: tightened `_WINDOW_PATTERN` from `r"^(?:\d+(?:ms|s|m|h|d)|forever)$"` to `r"^(?:[1-9]\d*(?:ms|s|m|h|d)|forever)$"` — leading digit must be 1-9, rejecting `"0ms"` at Python SDK call time.
- Added two new Rust unit tests: `parse_duration_rejects_zero_values` (all five zero suffixes) and `rule11_rejects_zero_window` (compile-path integration). No existing Python tests used `"0ms"` so no test changes were needed there.
- Gate result: 395 beava-core tests pass (was 393 + 2 new).

### WR-01: Duplicate-feature-name check is dead code

**Files modified:** `crates/beava-core/src/agg_compile.rs`, `crates/beava-core/src/register_validate.rs`
**Commit:** c3f2e7b
**Applied fix:**
- Removed the `seen_feature_names: HashSet<String>` declaration and the `if !seen_feature_names.insert(...)` block from `compile_aggregations_from_nodes`. Replaced with an explanatory comment: "JSON duplicate keys are silently dropped by BTreeMap deserialization (last-writer-wins). A per-iteration HashSet duplicate check is therefore unreachable."
- Added `#[allow(dead_code)]` to the `AggregationDuplicateFeatureName` variant in `register_validate.rs` (matching the existing pattern for `UnsupportedOpInPhase4`) with a note that it is reserved for future Vec-based deserialization.
- Gate result: 395 tests pass, clippy clean.

### WR-02: `|` separator in `parse_entity_key` silently corrupts multi-key entity keys

**Files modified:** `crates/beava-server/src/feature_query.rs`
**Commit:** 667c2a1
**Applied fix:**
- Changed `parse_entity_key` return type from `EntityKey` (with sentinel on mismatch) to `Option<EntityKey>` (`None` = part-count mismatch).
- GET `/get/:feature/:key` handler: on `None`, returns 400 `{"error": {"code": "key_parse_failure"}}` — clearly distinguishable from `key_not_found`.
- POST `/get` batch handler: on `None`, silently skips the feature for that key (consistent with the batch "missing keys are omitted" semantics).
- Updated docstring to document the `%7C` workaround for pipe-in-values; full URL-decoding deferred to Phase 12.
- Gate result: 114 beava-server unit tests pass, clippy clean. (Two pre-existing `cli_smoke` integration test failures confirmed pre-existing via `git stash` verification.)

### WR-03: `feature_index` rebuild is O(N_total) per registration

**Files modified:** `crates/beava-core/src/registry.rs`
**Commit:** 3af055d
**Applied fix:**
- Added `newly_inserted_agg_names: Vec<String>` local variable populated when `agg_map.remove(&d.name)` succeeds inside the node loop.
- Replaced the O(N_total) `w.compiled_aggregations.iter()` scan with an O(N_new) scan over `newly_inserted_agg_names`, looking up each node in `compiled_aggregations` directly.
- The `entry().or_insert()` additive semantics are preserved — existing feature_index entries from prior registrations are never overwritten.
- Gate result: 395 beava-core tests pass (including all three `resolve_feature_*` tests), clippy clean.

---

_Fixed: 2026-04-23_
_Fixer: Claude (gsd-code-fixer)_
_Iteration: 1_
