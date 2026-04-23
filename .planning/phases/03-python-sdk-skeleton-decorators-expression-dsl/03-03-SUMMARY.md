---
phase: "03"
plan: "03-03"
subsystem: python-sdk
tags: [python-sdk, decorators, schema-extraction, events, tables, tdd]
completed: "2026-04-23T05:33:46Z"
duration_minutes: 6

dependency_graph:
  requires:
    - python/beava package (from 03-01)
    - beava.Optional, Field, _types.py, py_type_to_field_type (from 03-01)
    - beava._col, Col (from 03-02)
  provides:
    - python/beava/_schema.py (FieldSpec, extract_schema, validate_duration_string, duration_to_ms)
    - python/beava/_events.py (EventSource, EventDerivation, event decorator)
    - python/beava/_tables.py (TableSource, TableDerivation, table decorator)
    - bv.event (class-form + function-form) — SDK-DEC-01/02/03/08/09
    - bv.table (class-form + function-form) — SDK-DEC-04/05
    - _to_register_json() on all four descriptor types matching Phase 2 wire contract
    - _beava_kind / _name / _schema / _upstreams / _ops on all descriptors (Plan 03-05 DAG walker)
  affects:
    - Plans 03-04, 03-05, 03-06 (App client + DAG walker + smoke test import these)
    - Phase 4 (server evaluator will consume derivation ops JSON)

tech_stack:
  added: []
  patterns:
    - TDD red-then-green commit pair (test: 878956c → feat: 32bd6f3)
    - stdlib-only schema extraction via inspect.signature param.annotation (not typing.get_type_hints)
    - Dispatch decorator pattern: event(arg=None, **kwargs) — bare vs parenthesized form
    - Sentinel-based bare-decorator detection for table() using _SENTINEL object()
    - dataclass FieldSpec with MISSING sentinel default_factory
    - Duration string validation via regex + unit lookup table

key_files:
  created:
    - python/beava/_schema.py
    - python/beava/_events.py
    - python/beava/_tables.py
    - python/tests/test_schema.py
    - python/tests/test_events.py
    - python/tests/test_tables.py
  modified:
    - python/beava/__init__.py

decisions:
  - "param.annotation used instead of typing.get_type_hints() for function-form upstream resolution — avoids NameError when annotated type is a local variable (PEP 563 lazy strings break get_type_hints in test scope)"
  - "test_events.py + test_tables.py deliberately omit from __future__ import annotations so parameter annotations are evaluated eagerly at def-time, capturing decorated EventSource/TableSource from local scope"
  - "event_time is OPTIONAL (SDK-DEC-08 devex-first): if declared must be int or datetime.datetime; if omitted server stamps wall-clock on receipt; event_time_field=null in JSON"
  - "Duration strings validated as shape-only in Phase 3 (regex ^\\d+(ms|s|m|h|d)$); duration_to_ms() translates to int at decoration time for wire JSON"
  - "TTL not carried on TableDerivation — derivations are source-agnostic; ttl kwarg accepted for API symmetry but is intentionally unused in function-form"
  - "_SENTINEL object() used to distinguish bare @bv.table from @bv.table() — avoids the None-collision since key=None means 'not provided'"
  - "FieldSpec uses dataclass field(default_factory=lambda: MISSING) to avoid mutable default issue while preserving MISSING sentinel behavior"

metrics:
  tasks_completed: 2
  subtasks: "1.a (red) + 1.b (green)"
  tests_added: 27
  tests_passing: 51
  files_created: 6
  files_modified: 1
---

# Phase 03 Plan 03: `@bv.event` and `@bv.table` Decorators Summary

**One-liner:** `@bv.event` and `@bv.table` decorators (class + function form) with stdlib-only schema extraction, duration validation, and `_to_register_json()` output matching Phase 2 EventDescriptor/TableDescriptor/DerivationDescriptor wire shapes.

## What Was Built

### `python/beava/_schema.py` (145 lines)

- **`FieldSpec` dataclass** — `name`, `py_type`, `optional`, `desc`, `default` (MISSING sentinel via `default_factory`).
- **`extract_schema(cls)`** — iterates `cls.__annotations__` in declaration order, resolves via `typing.get_type_hints(include_extras=False)`, handles `_OptionalSpec` (bv.Optional), rejects `typing.Optional[T]` with a message directing to `bv.Optional`, rejects unsupported types via `py_type_to_field_type()`, merges `_FieldMarker` metadata from `cls.__dict__`.
- **`validate_duration_string(s)`** — regex `^\d+(ms|s|m|h|d)$` or literal `"forever"`, else `TypeError`.
- **`duration_to_ms(s)`** — converts to int milliseconds; `"forever"` raises `ValueError` (no finite ms).

### `python/beava/_events.py` (336 lines)

- **`EventSource`** — source descriptor with `_name`, `_schema`, `_beava_kind="event"`, `_event_time_field`, `_dedupe_key`, `_dedupe_window_ms`, `_keep_events_for_ms`, `_tolerate_delay_ms`, `_upstreams=[]`, `_ops=[]`. `_to_register_json()` emits Phase 2 `EventDescriptor` JSON.
- **`EventDerivation`** — derivation descriptor with `_beava_kind="derivation"`, `_upstreams`, `_ops`, `_output_kind="event"`. `_to_register_json()` emits Phase 2 `DerivationDescriptor` JSON with `table_primary_key: null`.
- **`event()` decorator** — handles bare `@bv.event`, parenthesized `@bv.event(...)`, class-form (returns `EventSource`), function-form (returns `EventDerivation`). event_time optional per SDK-DEC-08 devex-first; if declared must be `int` or `datetime.datetime`.

### `python/beava/_tables.py` (325 lines)

- **`TableSource`** — source descriptor with `_primary_key`, `_ttl_ms`, `_mode="upsert"`. `_to_register_json()` emits Phase 2 `TableDescriptor` JSON.
- **`TableDerivation`** — derivation with `_output_kind="table"`, `_table_primary_key`. `_to_register_json()` emits `DerivationDescriptor` with `table_primary_key` populated.
- **`table()` decorator** — uses `_SENTINEL` to distinguish bare `@bv.table` from `@bv.table()`; both raise `TypeError` requiring `key=`. Validates every key field is in schema. `ttl="forever"` → `ttl_ms=null`.

### `python/beava/__init__.py`

- Replaced `_stub_event` / `_stub_table` stubs with `from ._events import event` and `from ._tables import table`.
- Retained `from ._col import Col, col` from Plan 03-02.

## TDD Commit Trace

| Commit | Type | Message |
|--------|------|---------|
| `878956c` | RED | `test(03-03): failing tests for @bv.event + @bv.table decorators + schema extraction` |
| `32bd6f3` | GREEN | `feat(03-03): @bv.event + @bv.table decorators with schema extraction + duration validation` |

Red commit: `pytest` exits non-zero with `ModuleNotFoundError: No module named 'beava._schema'` — no impl files existed.
Green commit: 27/27 new tests + 24 prior tests = 51 total passing; ruff clean; mypy strict clean.

## Verification Results

```
pytest tests/test_schema.py tests/test_events.py tests/test_tables.py -v
  → 27 passed in 0.03s

pytest tests/ -q
  → 51 passed in 0.04s

ruff check beava/ tests/
  → All checks passed!

mypy beava/
  → Success: no issues found in 7 source files

python -c "@bv.event event smoke"
  → {"dedupe_key": null, "dedupe_window_ms": null, "event_time_field": "event_time",
     "keep_events_for_ms": null, "kind": "event", "name": "T",
     "schema": {"fields": {"amount": "f64", "event_time": "i64", "user_id": "str"},
     "optional_fields": []}, "tolerate_delay_ms": null}

python -c "@bv.table table smoke"
  → {"kind": "table", "mode": "upsert", "name": "U",
     "primary_key": ["user_id"],
     "schema": {"fields": {"name": "str", "user_id": "str"}, "optional_fields": []},
     "ttl_ms": null}
```

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Nested pytest.raises in test_event_dedupe_key_must_be_in_schema**
- **Found during:** Task 1.b (first test run)
- **Issue:** The test had a logically incorrect nested `pytest.raises` structure where the inner context manager never executed the decorator call, causing the outer `pytest.raises(TypeError, match="dedupe_key")` to fail because the inner `with pytest.raises(TypeError, match="missing_field")` block never raised.
- **Fix:** Collapsed to a single `pytest.raises(TypeError, match="missing_field")` wrapping the decorator.
- **Files modified:** `python/tests/test_events.py`
- **Commit:** included in `32bd6f3`

**2. [Rule 1 - Bug] `typing.get_type_hints()` fails for function-form in PEP 563 scope**
- **Found during:** Task 1.b (first test run)
- **Issue:** With `from __future__ import annotations` in test files, function parameter annotations are stored as strings. `typing.get_type_hints(func)` tries to evaluate `'TxSrc'` against `func.__module__`'s globals, but `TxSrc` is a test-local decorated `EventSource` not in module globals → `NameError`.
- **Fix:** Two-part: (a) switched from `typing.get_type_hints(func)` to reading `param.annotation` directly from `inspect.signature(func)` parameters; (b) removed `from __future__ import annotations` from `test_events.py` and `test_tables.py` so annotations are evaluated eagerly at `def`-time in the test function's local scope, correctly capturing the decorated descriptor objects.
- **Files modified:** `python/beava/_events.py`, `python/beava/_tables.py`, `python/tests/test_events.py`, `python/tests/test_tables.py`
- **Commit:** included in `32bd6f3`

**3. [Rule 1 - Bug] Unused `ttl_ms` variable in `_decorate_table_function`**
- **Found during:** Task 1.b ruff gate
- **Issue:** `ttl_ms` was computed in the function-form path but never passed to `TableDerivation` (which doesn't have a TTL field — TTL is a source-table concern, not a derivation concern).
- **Fix:** Removed the dead TTL computation block; added a comment explaining that TTL is intentionally unused in function-form.
- **Files modified:** `python/beava/_tables.py`
- **Commit:** included in `32bd6f3`

**4. [Rule 1 - Bug] Ruff I001 import-sort violations in test files**
- **Found during:** Task 1.b ruff gate
- **Issue:** `ruff --fix` reported 3 I001 violations across `test_events.py`, `test_schema.py`, `test_tables.py`.
- **Fix:** `python -m ruff check --fix tests/` auto-sorted all three files.
- **Files modified:** all three test files
- **Commit:** included in `32bd6f3`

## Known Stubs

The following Phase 3 stub remains in `python/beava/__init__.py`:

| Stub | File | Line | Resolved By |
|------|------|------|-------------|
| `App = _AppStub` | `beava/__init__.py` | ~32 | Plans 03-04 + 03-05 (`bv.App` client) |

`event` and `table` are **no longer stubs** — they are the real implementations from this plan.

## Threat Flags

No new network endpoints, auth paths, or trust-boundary crossings introduced. Schema extraction operates entirely on user-authored Python classes at decoration time. T-03-03-01/02/03 from the plan's threat register: T-03-03-01 (name collision) deferred to Plan 03-05 DAG walker as designed; T-03-03-02 (infinite recursion) handled by Python naturally; T-03-03-03 (schema field names) accepted.

## Self-Check: PASSED

- `python/beava/_schema.py` — FOUND
- `python/beava/_events.py` — FOUND
- `python/beava/_tables.py` — FOUND
- `python/tests/test_schema.py` — FOUND
- `python/tests/test_events.py` — FOUND
- `python/tests/test_tables.py` — FOUND
- `python/beava/__init__.py` has `from ._events import event` — VERIFIED
- `python/beava/__init__.py` has `from ._tables import table` — VERIFIED
- `python/beava/__init__.py` retains `from ._col import Col, col` — VERIFIED
- Commit `878956c` (red) — FOUND
- Commit `32bd6f3` (green) — FOUND
- `pytest tests/` → 51 passed — VERIFIED
- `ruff check beava/ tests/` → clean — VERIFIED
- `mypy beava/` → clean — VERIFIED
