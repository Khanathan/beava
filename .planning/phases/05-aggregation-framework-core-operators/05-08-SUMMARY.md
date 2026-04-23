---
phase: "05"
plan: "08"
subsystem: "acceptance-gate"
tags: [acceptance, smoke, integration, sc1, sc2, sc3, sc4, sc5, sc6]
dependency_graph:
  requires: [05-01, 05-02, 05-03, 05-04, 05-05, 05-06, 05-07]
  provides: [phase5-acceptance-green]
  affects: []
tech_stack:
  added: []
  patterns:
    - "TDD red-green: #[ignore] red commit → un-ignore green commit"
    - "SC4 layered coverage: unit gate (windowed_replay_determinism) + observable gate (this test)"
    - "Two-process SC4: subprocess-spawned servers for Python, two TestServer instances for Rust"
key_files:
  created:
    - crates/beava-server/tests/phase5_smoke.rs
    - python/tests/test_phase5_smoke.py
  modified:
    - crates/beava-core/src/agg_compile.rs
    - python/beava/_agg.py
    - python/pyproject.toml
decisions:
  - "SC4 layered coverage: Plan 05-01 windowed_replay_determinism proves internal state equality; this plan proves observable output equality through full wire path"
  - "Bug fixes applied inline under Rule 1 (auto-fix): agg_compile.rs upstream_is_table + _agg.py output schema"
metrics:
  duration: "~120 min (across two sessions)"
  completed: "2026-04-23T18:01:00Z"
  tasks_completed: 2
  files_changed: 5
---

# Phase 5 Plan 08: Phase 5 Acceptance Gate Summary

**One-liner:** Phase 5 acceptance gate — 10 Rust + 9 Python smoke tests proving SC1..SC6 end-to-end with two cross-plan integration bugs found and fixed.

## Objective

Create Rust and Python smoke tests that prove all six ROADMAP success criteria (SC1..SC6) against a live server binary. Tests act as the acceptance gate for the entire Phase 5 aggregation framework.

## Tasks Completed

### Task 1.a (red) — commit `7721dbc`

Created `crates/beava-server/tests/phase5_smoke.rs` with 10 `#[tokio::test]` functions all marked `#[ignore]`. Registered the test binary in `Cargo.toml`. All tests existed but did not run in the red commit (ignore ensures RED without skipping the compile check).

### Task 1.b (green) — commits `7a1991c`, `e0c23f2`, `6437d56`

Un-ignored all 10 Rust tests, fixed two integration bugs discovered during execution (see Deviations), created `python/tests/test_phase5_smoke.py` with 9 tests, added `phase5` marker to `pyproject.toml`.

**Final test counts:**
- Rust `beava-core`: 393 tests, 0 failed
- Rust `beava-server` (all test binaries): 114 tests, 0 failed
- Python: 222 tests, 0 failed

## Verification

All acceptance criteria from the plan confirmed:

| Criterion | Result |
|-----------|--------|
| SC1: group_by/agg registers Table derivation with correct schema | PASS |
| SC2: push → count updates + where predicate filters | PASS |
| SC3: all 8 operators (count/sum/avg/min/max/variance/stddev/ratio) correct | PASS |
| SC4: two-instance replay → byte-identical GET responses | PASS |
| SC5: windowless count=50 over 50 days, ratio=0.3 from 3/10 ok events | PASS |
| SC6: unknown_field → 400, aggregation_on_table → 400 | PASS |
| D-02 envelope shape: GET /get/{f}/{k} returns exactly `{"value": ...}` | PASS |
| `windowed_replay_determinism` (SC4 unit gate, Plan 05-01): | PASS |
| `cargo clippy --workspace --all-targets --all-features -- -D warnings` | PASS |
| `cargo fmt --all --check` | PASS |
| `python -m ruff check . && python -m mypy beava/` | PASS |

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] `agg_compile.rs`: upstream_is_table missed table-output derivations**

- **Found during:** Task 1.b, sc6_aggregation_on_table_rejected test
- **Issue:** `upstream_is_table` in `agg_compile.rs` only checked `registry.tables` (explicit `@bv.table` class-form nodes) and `PayloadNode::Table`, not derivations whose `output_kind == OutputKind::Table`. A derivation like `TxTable` (produced by `group_by().agg()`) was not detected as a Table source, so the aggregation-on-table validation silently passed and returned 200 instead of 400.
- **Fix:** Extended the check to also scan `registry.derivations` for entries with `output_kind == OutputKind::Table`, and `PayloadNode::Derivation` nodes with `output_kind == Table`.
- **Files modified:** `crates/beava-core/src/agg_compile.rs`
- **Commit:** `7a1991c`

**2. [Rule 1 - Bug] `_agg.py`: GroupBy.agg() sent upstream event schema to derivation**

- **Found during:** Task 1.b, test_sc1_groupby_agg_produces_table Python test
- **Issue:** `GroupBy.agg()` in `python/beava/_agg.py` passed `schema=self._upstream._schema` to `TableDerivation` — which is the event source schema (all event fields like `amount`, `event_time`, `status`, `user_id`). The server stored this verbatim, so `GET /registry` showed Transaction event fields in the derivation schema instead of the aggregated output fields (`user_id`, `cnt`).
- **Fix:** Built a proper `output_schema` dict containing: (a) group-by keys with their types from the upstream schema, (b) aggregated feature names with output types (`count` → `int`/`i64`, all others → `float`/`f64`).
- **Files modified:** `python/beava/_agg.py`
- **Commit:** `e0c23f2`

**3. [Rule 3 - Deviation] Expression predicates require single-quoted strings**

- **Found during:** Task 1.b, sc2_push_with_where_filters Rust test
- **Issue:** The expression parser in `beava-core/src/expr.rs` only accepts single-quoted string literals — `(status == 'ok')`. Initial Rust test code used double-quoted strings `"(status == \"ok\")"`.
- **Fix:** Changed all where predicate strings in both Rust and Python tests to use single quotes.
- **Files modified:** `crates/beava-server/tests/phase5_smoke.rs`
- **Commit:** Part of `6437d56`

## SC4 Layered Coverage

SC4 (replay determinism) is proven at two levels:

1. **Unit-level gate** (Plan 05-01, `windowed_replay_determinism`): 1000-event stream applied twice to `WindowedOp` structs → `format!("{:?}", state)` equality. Proves byte-identical internal state at the aggregation data-structure level.

2. **Observable-output gate** (this plan, `sc4_replay_determinism`): Same 100-event stream applied to two independent fresh `TestServer` instances → byte-identical `GET /get/{feature}/{key}` HTTP response bodies. Proves internal state faithfully projects through the full apply-loop + registry + query wire path.

Together these form the complete SC4 proof: internal equality + faithful projection = byte-identical state visible at every layer.

## Commits

| Hash | Message |
|------|---------|
| `7721dbc` | `test(05-08): add phase5_smoke SC1..SC6 acceptance tests (ignored until 1.b)` |
| `7a1991c` | `fix(05-04): extend upstream_is_table to cover table-output derivations` |
| `e0c23f2` | `fix(05-07): GroupBy.agg() builds correct output schema for derivation` |
| `6437d56` | `feat(05-08): Phase 5 acceptance gate green — all SC1..SC6 smoke tests pass` |

## Self-Check: PASSED
