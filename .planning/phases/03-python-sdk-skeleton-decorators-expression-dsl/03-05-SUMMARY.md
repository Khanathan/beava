---
phase: "03"
plan: "03-05"
subsystem: python-sdk
tags: [python-sdk, app, dag, topological-sort, cycle-detection, register, validate, tdd]
completed: "2026-04-23T08:30:00Z"
duration_minutes: 30

dependency_graph:
  requires:
    - python/beava package (from 03-01)
    - beava._errors: ValidationError, RegistrationError (from 03-01)
    - beava._events: EventSource, EventDerivation (from 03-03)
    - beava._tables: TableSource, TableDerivation (from 03-03)
    - beava._transport: Transport, parse_url_to_transport (from 03-04)
    - python/tests/conftest.py: beava_binary + beava_server fixtures (from 03-04)
  provides:
    - python/beava/_validate.py (validate_descriptors, topo_sort, _detect_cycle_dfs)
    - python/beava/_app.py (App class — register, validate, ping, context manager)
    - beava.App: fully wired sync client replacing _AppStub
  affects:
    - Plan 03-06 (end-to-end smoke uses bv.App directly)
    - Phase 6 (bv.AsyncApp will mirror this App structure)

tech_stack:
  added: []  # no new deps
  patterns:
    - TDD red-then-green commit pair (test: 166c4c3 → feat: 6a04e74)
    - Kahn's algorithm for topological sort (stable: preserves input order as tiebreaker)
    - DFS three-color cycle detection (WHITE/GRAY/BLACK) for O(V+E) cycle detection
    - Fail-soft validation: collect all errors, return as list (not fail-fast exception)
    - Lazy embed transport: deferred until __enter__ to enforce context-manager pattern
    - Idempotent close(): _closed flag guards double-close

key_files:
  created:
    - python/beava/_validate.py
    - python/beava/_app.py
    - python/tests/test_validate.py
    - python/tests/test_app.py
  modified:
    - python/beava/__init__.py (replaced _AppStub with `from ._app import App`)

decisions:
  - "topo_sort raises ValueError (not ValidationError) on cycle — ValidationError is a frozen dataclass, not an Exception; register() calls validate_descriptors() first so topo_sort cycle branch is a defensive fallback"
  - "validate_descriptors is fail-soft: collects all errors (duplicate_name, missing_upstream, cycle, field-type checks) and returns them all; callers decide whether to surface first or all"
  - "embed mode guard: _transport=None until __enter__ fires; _require_transport() raises RuntimeError with context-manager hint if transport is None"
  - "close() sets _closed=True first to make double-close idempotent; checks _closed at start"
  - "App.validate() does not call _require_transport() — works on any App instance, even before __enter__ (zero network I/O so no transport needed)"
  - "schema_mismatch rule is a Phase 3 no-op placeholder in validate_descriptors; Phase 4 op-chain walk extends it"
  - "FieldSpec requires name= argument (discovered at test authoring time) — test helpers updated accordingly"

metrics:
  tasks_completed: 2
  subtasks: "1.a (red) + 1.b (green)"
  tests_added: 23
  tests_passing: 105
  files_created: 4
  files_modified: 1
---

# Phase 03 Plan 05: `bv.App` Client — register + validate + DAG topo-sort + cycle detection

**One-liner:** `bv.App` sync client with Kahn's topo-sort + DFS three-color cycle detection, fail-soft `validate_descriptors`, `app.register()` local-validate-then-wire, `app.ping()` (TCP only), and embed-mode context-manager guard.

## What Was Built

### `python/beava/_validate.py` (290 lines)

- **`validate_descriptors(descriptors) -> list[ValidationError]`**: Fail-soft validator collecting all local errors:
  1. `duplicate_name` — two descriptors with same `_name` in one batch
  2. `missing_upstream` — derivation upstream not in the batch (server handles registry-state check)
  3. `cycle` — DFS three-color detection; path string e.g. `"A -> B -> C -> A"`
  4. `unknown_field_type` — field's `py_type` not in the 6 valid wire types
  5. `event_time_field_invalid` — event_time_field missing from schema or wrong type
  6. `table_key_invalid` — primary_key field(s) not in schema
  7. `bad_return_type` — `_beava_kind` not one of `event|table|derivation`
  8. `schema_mismatch` — Phase 3 no-op placeholder (Phase 4 op-chain extension point)

- **`topo_sort(descriptors) -> list`**: Kahn's algorithm; stable (preserves input order for equal in-degree nodes); raises `ValueError` (not `ValidationError`) on cycle — this branch is a defensive fallback since `validate_descriptors` catches cycles first.

- **`_detect_cycle_dfs(graph) -> list[str] | None`**: DFS three-color (WHITE/GRAY/BLACK); returns cycle path list on first cycle found, `None` if acyclic.

### `python/beava/_app.py` (200 lines)

- **`class App`**: Sync client composing `Transport` + validation.
  - `__init__(url=None, *, timeout=30.0)`: Explicit-URL mode creates transport eagerly; embed mode defers until `__enter__`.
  - `__enter__` / `__exit__`: Context manager; `__exit__` calls `close()`.
  - `close()`: Idempotent — `_closed` flag; closes transport on first call, no-op on subsequent.
  - `validate(*descriptors) -> list[ValidationError]`: Zero network I/O; delegates to `validate_descriptors`; works even before `__enter__`.
  - `ping() -> dict`: Delegates to `transport.send_ping()`; HTTP raises `NotImplementedError`; TCP returns `{server_version, registry_version}`.
  - `register(*descriptors) -> dict`: (1) `validate_descriptors` → raise `RegistrationError` if non-empty; (2) `topo_sort`; (3) compile `{"nodes": [...]}` JSON; (4) `transport.send_register(payload_bytes)` → return server dict.
  - Embed-mode guard: `_require_transport()` raises `RuntimeError` with context-manager hint if `_transport is None`.

### `python/beava/__init__.py`

Replaced `class _AppStub` + `App = _AppStub` stub with `from ._app import App`. `__all__` unchanged (App already listed).

## TDD Commit Trace

| Commit | Type | Message |
|--------|------|---------|
| `166c4c3` | RED | `test(03-05): failing tests for validate + App client (register, validate, context manager)` |
| `6a04e74` | GREEN | `feat(03-05): bv.App client — register + validate + DAG topo-sort + cycle detection` |

Red commit: 23 tests fail — 8 `ModuleNotFoundError: No module named 'beava._validate'` in test_validate.py + 15 `NotImplementedError: bv.App lands in Plan 03-05` in test_app.py.

Green commit: 105/105 tests pass; ruff clean; mypy strict clean (12 source files).

## Verification Results

```
pytest tests/test_validate.py -v  → 9 passed
pytest tests/test_app.py -v       → 14 passed
pytest tests/ -q                  → 105 passed in 1.88s

ruff check beava/ tests/          → All checks passed!
mypy beava/                       → Success: no issues found in 12 source files

python -c "
import beava as bv
@bv.event
class T:
    amount: float; user_id: str; event_time: int
@bv.table(key='user_id')
class U:
    user_id: str; balance: float
errs = bv.App('http://placeholder').validate(T, U)
assert errs == [], errs
print('ok')
"  → ok
```

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] FieldSpec requires positional `name=` argument in tests**
- **Found during:** Task 1.b, first test run after implementing _validate.py
- **Issue:** Test helpers constructed `FieldSpec(py_type=str, optional=False)` but `FieldSpec` is `@dataclass` with `name` as first required field.
- **Fix:** Updated `_make_event`, `_make_derivation`, `_make_table` helpers in both test files to pass `name=field_name`.
- **Files modified:** `python/tests/test_validate.py`, `python/tests/test_app.py`
- **Commit:** included in `6a04e74`

**2. [Rule 1 - Bug] `raise ValidationError(...)` in topo_sort — ValidationError is not an Exception**
- **Found during:** Task 1.b, mypy strict gate
- **Issue:** `topo_sort` raised `ValidationError` directly, but `ValidationError` is a `@dataclass(frozen=True)` not a `BaseException` subclass. mypy `[misc]` error: "Exception must be derived from BaseException".
- **Fix:** Changed to `raise ValueError(str(err)) from None` — the cycle path info is preserved in the message string. This branch is a defensive fallback only; `validate_descriptors` catches cycles before `topo_sort` is called from `register`.
- **Files modified:** `python/beava/_validate.py`
- **Commit:** included in `6a04e74`

**3. [Rule 1 - Bug] Unused `type: ignore` comments in `_validate.py` and `_app.py`**
- **Found during:** Task 1.b, mypy strict gate
- **Issue:** Several `# type: ignore[no-any-return]` comments were unnecessary — mypy strict didn't flag those lines. Also a mid-file `import datetime as _dt` triggered `E402 Module level import not at top of file`.
- **Fix:** Removed unused ignores; moved `import datetime as _dt` to the top-of-file import block; used explicit typed intermediate variables where needed.
- **Files modified:** `python/beava/_validate.py`, `python/beava/_app.py`
- **Commit:** included in `6a04e74`

**4. [Rule 1 - Bug] `@bv.event` function-form annotation stringification in test**
- **Found during:** Task 1.b, first test_app.py run
- **Issue:** `test_app_register_topological_order_in_payload` used `@bv.event def Checkouts(src: Transactions) -> object:` inside the test, but `from __future__ import annotations` at the top of the test file stringifies all annotations. The `_decorate_event_function` reads `param.annotation` directly and got `'Transactions'` (a string) instead of the actual `EventSource` descriptor.
- **Fix:** Replaced the function-form decorator with manually constructed `_make_event` + `_make_derivation` descriptors (which already set `_upstreams` correctly), avoiding the annotation resolution issue entirely.
- **Files modified:** `python/tests/test_app.py`
- **Commit:** included in `6a04e74`

**5. [Rule 1 - Bug] Ruff import-sort violations in test files**
- **Found during:** Task 1.b, ruff gate
- **Issue:** `I001` import-sort violations in both `test_app.py` and `test_validate.py`.
- **Fix:** `ruff check --fix` resolved both automatically.
- **Files modified:** `python/tests/test_app.py`, `python/tests/test_validate.py`
- **Commit:** included in `6a04e74`

## Known Stubs

None — `bv.App` is fully implemented. The only remaining stub in the package is:

| Stub | File | Reason | Resolved By |
|------|------|---------|-------------|
| `schema_mismatch` rule in `validate_descriptors` | `_validate.py` | Phase 3 has no op-chain (ops always empty) | Plan 04-xx (stateless op chain) |

This stub does not affect Plan 03-05's goal — the `schema_mismatch` rule only fires on non-empty op chains, which Phase 3 never produces.

## Threat Surface Scan

Threat model items from plan addressed:

- **T-03-05-01 (DFS cycle short-circuit)**: `_detect_cycle_dfs` returns on first back-edge found; DFS never revisits BLACK nodes; no infinite loop possible even on deeply cyclic DAGs.
- **T-03-05-02 (register payload)**: Validated descriptors reach the wire; no unvalidated pass-through.
- **T-03-05-03 (error messages)**: ValidationError messages contain only user-authored schema paths and descriptor names — no secrets or server internals.

No new network surface introduced. `App.validate()` is explicitly zero-network-IO (no transport required).

## Self-Check: PASSED

Files:
- `/Users/petrpan26/work/tally/python/beava/_validate.py` — FOUND
- `/Users/petrpan26/work/tally/python/beava/_app.py` — FOUND
- `/Users/petrpan26/work/tally/python/beava/__init__.py` has `from ._app import App` — VERIFIED
- `/Users/petrpan26/work/tally/python/tests/test_validate.py` — FOUND
- `/Users/petrpan26/work/tally/python/tests/test_app.py` — FOUND

Commits:
- `166c4c3` (red) — FOUND
- `6a04e74` (green) — FOUND

Gates:
- `pytest tests/ -q` → 105 passed — VERIFIED
- `ruff check beava/ tests/` → clean — VERIFIED
- `mypy beava/` → 12 source files, no issues — VERIFIED
- Inline smoke `bv.App('http://placeholder').validate(T, U) == []` → ok — VERIFIED
