---
phase: 05-aggregation-framework-core-operators
plan: "07"
subsystem: python-sdk
tags: [aggregation, python-sdk, group-by, tdd, window-validation, where-predicate]
dependency_graph:
  requires:
    - "05-03 (AggregationDescriptor JSON shape — Python wire mirrors this)"
    - "Phase 4 _ExprAST.to_expr_string (where= predicate serialization)"
  provides:
    - GroupBy builder (python/beava/_agg.py)
    - AggDescriptor frozen dataclass (python/beava/_agg.py)
    - 8 module-level helpers: count/sum/avg/min/max/variance/stddev/ratio
    - _EventOpsMixin.group_by() (python/beava/_events.py)
    - _TableOpsMixin.group_by() raises TypeError citing SDK-AGG-05 (python/beava/_tables.py)
  affects:
    - Plan 05-08: end-to-end integration test runs this SDK surface against the live server
tech_stack:
  added: []
  patterns:
    - "Frozen dataclass (AggDescriptor) — hashable, immutable, safe to reuse across .agg() calls"
    - "_validate_window: regex \\d+(ms|s|m|h|d)|forever enforced at helper-call time (SDK-AGG-06)"
    - "_serialize_where: duck-typed to_expr_string() call — no hard import of _ExprAST (avoids circular dep)"
    - "GroupBy.agg() deferred import of TableDerivation — avoids circular import at module level"
    - "_EventOpsMixin.group_by() deferred import of GroupBy — same circular-import avoidance pattern"
    - "sum/min/max shadow Python builtins at beava module scope (intentional DSL design, documented)"
key_files:
  created:
    - python/beava/_agg.py
    - python/tests/test_agg.py
  modified:
    - python/beava/_events.py
    - python/beava/_tables.py
    - python/beava/__init__.py
decisions:
  - "Deferred imports inside group_by() and GroupBy.agg() avoid circular dependency between _agg.py, _events.py, and _tables.py without needing TYPE_CHECKING guards on runtime paths"
  - "window= is a keyword-only required parameter for sum/avg/min/max/variance/stddev (Python enforces this via function signature — no extra validation needed for the 'required' check)"
  - "_validate_window raises ValueError (not TypeError) for malformed window strings — consistent with SDK-AGG-06 scope (format validation, not type checking)"
  - "where= uses duck-typing (hasattr to_expr_string) rather than isinstance(_ExprAST) — avoids hard coupling to _col.py internal class; raises TypeError with guidance if not an expression node"
  - "TableDerivation name is synthesized as '{upstream}_by_{keys}' at .agg() time; callers can use .named() to override (consistent with EventDerivation naming pattern)"
metrics:
  duration_seconds: 420
  completed_date: "2026-04-23"
  tasks_completed: 2
  files_created: 2
  files_modified: 3
---

# Phase 5 Plan 07: Python SDK group_by + 8 Aggregation Helpers + REGISTER JSON Serialization Summary

Python SDK surface for Phase 5: `Event.group_by(*keys).agg(**named_features)` + 8 `bv.<op>` helpers with window/where validation and REGISTER JSON serialization matching the Rust-side AggregationDescriptor shape.

## What Was Built

### Task 1.a (red) — Failing test suite: `8145bb6`

**`python/beava/_agg.py`** (stub)

- `AggDescriptor` frozen dataclass with `to_agg_spec()` raising `NotImplementedError`
- 8 helper stubs (`count`, `sum`, `avg`, `min`, `max`, `variance`, `stddev`, `ratio`) all raising `NotImplementedError`
- `GroupBy` stub class with `__init__` and `agg()` raising `NotImplementedError`

**`python/beava/_events.py`** — `_EventOpsMixin.group_by()` stub raising `NotImplementedError`

**`python/beava/_tables.py`** — `_TableOpsMixin.group_by()` raises `TypeError` citing SDK-AGG-05 (fully correct at red — it's a hard reject with no impl needed)

**`python/beava/__init__.py`** — re-exports `AggDescriptor`, `GroupBy`, and all 8 helpers

**`python/tests/test_agg.py`** — 57 test cases; 48 FAIL / 9 PASS confirmed RED.

### Task 1.b (green) — Full implementation: `19831df`

**`python/beava/_agg.py`** (full implementation)

`_validate_window(window, op, requires_window)`:
- Accepts `None` (allowed for count/ratio; raises ValueError for sum/avg/min/max/variance/stddev)
- Rejects malformed strings via `_WINDOW_PATTERN = re.compile(r"^(?:\d+(?:ms|s|m|h|d)|forever)$")`
- Error message: `window={window!r} must match regex \d+(ms|s|m|h|d) or 'forever'`

`_serialize_where(where)`:
- Returns `None` if where is None
- Duck-typed: calls `where.to_expr_string()` if the attribute exists
- Raises `TypeError` with guidance if not an expression node (T-05-07-02 mitigation)

`AggDescriptor.to_agg_spec()`:
- Returns `{"op": <op>, "params": {...}}` omitting None-valued keys
- Shape matches Rust-side `AggSpec { op, params }` in the REGISTER JSON

8 helpers:
| Helper | field required | window required | AGG-CORE ref |
|--------|---------------|-----------------|--------------|
| `count` | No | No (lifetime) | AGG-CORE-01 |
| `sum` | Yes (positional) | Yes | AGG-CORE-02 |
| `avg` | Yes (positional) | Yes | AGG-CORE-03 |
| `min` | Yes (positional) | Yes | AGG-CORE-04 |
| `max` | Yes (positional) | Yes | AGG-CORE-05 |
| `variance` | Yes (positional) | Yes | AGG-CORE-06 |
| `stddev` | Yes (positional) | Yes | AGG-CORE-07 |
| `ratio` | No | No (lifetime) | AGG-CORE-08/09 |

`GroupBy.agg(**named_features)`:
- Validates each value is an `AggDescriptor` (T-05-07-02)
- Builds op-node: `{"op": "group_by", "keys": [...], "agg": {name: to_agg_spec(), ...}}`
- Returns `TableDerivation` with `output_kind="table"`, `table_primary_key=keys`

**`python/beava/_events.py`** — `_EventOpsMixin.group_by()` full implementation:
- Rejects empty key list (`ValueError`)
- Rejects non-string keys (`TypeError`)
- Rejects keys absent from `self._schema` (`ValueError: key {k!r} is not in schema`)
- Returns `GroupBy(self, list(keys))`

## Tests Added

57 test cases covering:
- All 8 helpers: minimal call, field+window, window-required rejection, malformed window rejection
- All valid window unit suffixes: `1ms`, `1s`, `1m`, `1h`, `1d`, `forever`, `100ms`, `24h` (8 parametrized)
- All invalid window strings: `1hour`, `1min`, `5sec`, `2weeks`, `abc`, `5`, `` (7 parametrized)
- `where=` serialization via `to_expr_string()` (SDK-AGG-04)
- `where=` rejection for non-expression values (TypeError)
- `to_agg_spec()` wire shape for count, sum, count-with-where, minimal count
- `GroupBy` creation: single key, multiple keys, non-string key rejection, missing key rejection, empty keys
- `GroupBy.agg()` returns `TableDerivation` with correct `output_kind`, `_table_primary_key`, op-node shape
- `GroupBy.agg()` with `where=` serialised into op-node params
- `GroupBy.agg()` with 3 features preserves insertion order
- `GroupBy.agg()` rejects non-`AggDescriptor` values
- `_TableOpsMixin.group_by()` raises `TypeError` citing `SDK-AGG-05`
- Import smoke: all 8 helpers + `GroupBy` + `AggDescriptor` importable from `beava`

## Deviations from Plan

None — plan executed exactly as written.

The plan's `_events.py` TYPE_CHECKING block shows `from ._agg import GroupBy` — this was added as a type annotation aid, but the runtime import is inside the method body (deferred) to avoid circular imports. Both approaches are consistent with the plan's intent.

## Known Stubs

None. All module-level helpers, `AggDescriptor.to_agg_spec()`, `GroupBy.agg()`, and `_EventOpsMixin.group_by()` are fully implemented. No placeholder values flow to any output.

## Threat Flags

None. This plan introduces no network endpoints, auth paths, or file access patterns. Pure in-process Python DSL construction.

## Self-Check: PASSED

Files exist:
- `python/beava/_agg.py` — FOUND
- `python/tests/test_agg.py` — FOUND
- `python/beava/_events.py` (group_by method) — FOUND
- `python/beava/_tables.py` (group_by raises TypeError) — FOUND
- `python/beava/__init__.py` (re-exports) — FOUND

Commits exist:
- `8145bb6` — test(05-07): add failing pytest suite for Python SDK group_by + 8 aggregation helpers
- `19831df` — feat(05-07): Python SDK group_by + 8 aggregation helpers + REGISTER JSON serialization (SDK-AGG-01..06)

Gates:
- `python -m pytest tests/test_agg.py -q` — 57 passed, 0 failed
- `python -m pytest -q` (unit tests only) — 162 passed, 0 failed (server-dependent tests excluded — pre-existing, not regressions)
- `python -m ruff check .` — All checks passed
- `python -m mypy beava/` — Success: no issues found in 14 source files
- `python -c "from beava import count, sum, avg, min, max, variance, stddev, ratio, GroupBy; print('ok')"` — ok
- `python -c "import beava as bv; d=bv.count(window='5m'); print(d.to_agg_spec())"` — {'op': 'count', 'params': {'window': '5m'}}
- No Rust files modified: `git diff 8145bb6 19831df -- crates/` — empty
