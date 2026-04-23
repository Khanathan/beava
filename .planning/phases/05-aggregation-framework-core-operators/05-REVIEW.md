---
phase: 05-aggregation-framework-core-operators
reviewed: 2026-04-23
depth: standard
files_reviewed: 27
findings:
  critical: 1
  warning: 3
  info: 3
  total: 7
status: issues_found
---

# Phase 5: Code Review Report

**Reviewed:** 2026-04-23
**Depth:** standard
**Status:** issues_found

## Summary

Phase 5 lands the aggregation framework — 9 AggOp variants, 64-bucket ring buffer, Welford variance, three-valued null (D-03), D-02 `{value}` envelope, D-06 no-wall-clock determinism, and the Python SDK GroupBy/AggDescriptor layer. The core correctness story is solid. One critical crash path exists: zero-window strings like `"0ms"` pass validation, reach `WindowedOp::new(kind, 0)`, set `bucket_ms = 0`, and cause a `div_euclid(0)` panic at first apply.

## Critical

### CR-01 — Zero-window string panics at apply time

**Files:** `crates/beava-core/src/agg_compile.rs` (parser), `crates/beava-core/src/agg_windowed.rs` (consumer), `python/beava/_agg.py` (_WINDOW_PATTERN)

`parse_duration_to_ms` accepts `"0ms"`/`"0s"`/`"0m"`/`"0h"`/`"0d"` as `Ok(Some(0))`. `WindowedOp::new(kind, 0)` sets `bucket_ms = 0`. First event triggers `event_time_ms.div_euclid(0)` — panic. Python regex `_WINDOW_PATTERN` has same gap.

**Fix:**
- `agg_compile.rs`: reject ms==0 with `Err("window duration must be positive; got {s:?}")`
- `_agg.py`: tighten regex to `r"^(?:[1-9]\d*(?:ms|s|m|h|d)|forever)$"` (leading digit 1-9 required)
- Add test: register with `window="0ms"` → 400 with `invalid_window` kind

## Warnings

### WR-01 — Duplicate-feature-name check is dead code

**File:** `crates/beava-core/src/agg_compile.rs`

Serde deserializes JSON object into `BTreeMap<String, AggSpec>` — duplicate keys are silently last-writer-wins before the `HashSet` check runs. Check never fires. Either delete the dead check + document last-writer-wins, or deserialize via `Vec<(String, AggSpec)>` and check duplicates before conversion.

### WR-02 — `|` separator in `parse_entity_key` silently corrupts multi-key entity keys

**File:** `crates/beava-server/src/feature_query.rs`

Multi-key group-by URL-encodes entity as `val1|val2`; split on `|`. If any key value contains `|` (URL, pipe-delimited string), split produces wrong part count → wrong EntityKey → silent 404. No error distinguishes "not found" from "key parse failure."

**Fix:** Either URL-pct-encode values (`%7C` for literal pipe) and document the restriction, OR return distinct error kinds on part-count mismatch (`key_parse_failure` vs `key_not_found`).

### WR-03 — `feature_index` rebuild is O(N_total) per registration, not O(N_new)

**File:** `crates/beava-core/src/registry.rs`

`apply_registration` iterates ALL compiled_aggregations to rebuild feature_index entries. With 500 existing nodes, adding 1 triggers 500+ redundant iterations. Should scope index updates to the newly-inserted node only.

**Fix:** After insert, iterate only `inner.compiled_aggregations.get(&new_agg_name).unwrap().features` and update feature_index.

## Info

### IN-01 — Per-entity AggStateTable BTreeMap growth unbounded; undocumented at server layer

`AggStateTable` has no eviction. High-cardinality group keys (e.g., `group_by("session_id")`) silently OOM. Documented intentional for Phase 5 in design doc but no `// TODO(phase-6): eviction` comment in server code.

### IN-02 — D-06 grep-guard tests are TDD smell

`test_no_systemtime_now_in_agg_state` uses `include_str!` + string search. Aliases (`use std::time::SystemTime as ST; ST::now()`) or indirect calls would pass the check silently. Supplement with behavioral determinism tests (run apply twice, assert identical state).

### IN-03 — Python `GroupBy.agg()` auto-generates derivation name without collision detection

Generated name `f"{upstream_name}_by_{'_'.join(self._keys)}"` can collide for different group_by calls with same upstream+keys. Server catches it as duplicate-node error; could surface at SDK build time with a fingerprint suffix.

---
*Reviewed: 2026-04-23 by gsd-code-reviewer (standard depth)*
