---
phase: "03"
plan: "03-01"
subsystem: python-sdk
tags: [python-sdk, packaging, errors, types, tdd]
completed: "2026-04-23T05:19:27Z"
duration_minutes: 15

dependency_graph:
  requires: []
  provides:
    - python/beava package (importable, editable install)
    - beava.ValidationError (frozen dataclass, __str__ = [kind] path: message)
    - beava.RegistrationError (Exception with code/path/message/errors)
    - beava.BinaryNotFoundError (Exception)
    - beava.Optional (nullable marker, nesting-collapse, distinct from typing.Optional)
    - beava.Field (per-field metadata factory, MISSING sentinel)
    - beava._types.py_type_to_field_type (str/int/float/bool/bytes/datetime.datetime)
    - pytest + ruff + mypy harness via pyproject.toml
  affects:
    - Plans 03-02, 03-03, 03-04, 03-05, 03-06 (all import from these modules)

tech_stack:
  added:
    - httpx>=0.27,<1 (runtime dep — used in Plan 03-04)
    - pytest>=8,<9 (dev dep)
    - ruff>=0.5,<1 (dev dep, line-length=100, py310, E+F+I+W+B)
    - mypy>=1.10,<2 (dev dep, strict mode)
  patterns:
    - TDD red-then-green commit pair (test: commit then feat: commit)
    - _private.py modules re-exported via __init__.py
    - __slots__ with explicit class-level annotations for mypy strict compatibility
    - Singleton pattern for MISSING sentinel (_Missing.__new__)
    - Frozen dataclass for ValidationError (immutable, hashable)

key_files:
  created:
    - python/beava/__init__.py
    - python/beava/_errors.py
    - python/beava/_types.py
    - python/tests/__init__.py
    - python/tests/conftest.py
    - python/tests/test_errors.py
    - python/tests/test_types.py
    - python/tests/test_package_init.py
  modified:
    - python/pyproject.toml

decisions:
  - "MISSING sentinel uses __new__ singleton pattern (matches v1 reference shape from main:_types_core.py)"
  - "Optional nesting collapse implemented in _OptionalSpec.__init__ via isinstance check"
  - "_OptionalSpec.__slots__ requires explicit class-level Any annotation for mypy strict (slot inference limitation)"
  - "py_type_to_field_type uses exact identity matching via dict lookup (bool before int is safe — dict keys are exact types, not isinstance)"
  - "RegistrationError stores errors list with default [] (not None) for clean attribute access"
  - "Phase 3 stubs (event/table/col/App) raise NotImplementedError with plan reference message"
  - "Broke pip@24.3.1 (corrupted resolvelib vendor); fixed by reinstalling pip@26.0.1 via get-pip.py"

metrics:
  tasks_completed: 2
  subtasks: "1.a (red) + 1.b (green)"
  tests_added: 10
  tests_passing: 10
  files_created: 8
  files_modified: 1
---

# Phase 03 Plan 01: Python SDK Skeleton — Errors, Types, pyproject Summary

**One-liner:** `beava` package skeleton with `ValidationError` frozen dataclass, `Optional`/`Field`/`MISSING` type primitives, and `py_type_to_field_type` mapping; pytest + ruff + mypy harness wired via pyproject.toml.

## What Was Built

The foundational layer every subsequent Phase 3 plan imports from:

- **`python/beava/_errors.py`** — `ValidationError` frozen dataclass with `[{kind}] {path}: {message}` `__str__`, `RegistrationError(Exception)` carrying `.code/.path/.message/.errors`, `BinaryNotFoundError(Exception)`, and `VALIDATION_ERROR_KINDS` frozenset with all 9 valid kind strings.
- **`python/beava/_types.py`** — `MISSING` singleton sentinel (`__bool__ = False`), `_OptionalSpec` with nested-collapse, `_OptionalMarker` (`bv.Optional[T]`), `_FieldMarker` + `Field()` factory, and `py_type_to_field_type()` mapping 6 Python types to server FieldType strings with a helpful `TypeError` on unsupported types.
- **`python/beava/__init__.py`** — Public re-exports of all error and type primitives; `event/table/col/App` stubs that `raise NotImplementedError` with plan reference messages so `hasattr(bv, "event")` returns True.
- **`python/pyproject.toml`** — Extended with `httpx>=0.27,<1` runtime dep, `dev = [pytest>=8, ruff>=0.5, mypy>=1.10]` optional extras, `[tool.ruff]` (line-length=100, py310, select E+F+I+W+B), `[tool.mypy]` (strict=true), `[tool.pytest.ini_options]` addopts `-ra --strict-markers`.
- **`python/tests/`** — 5 test files (3 functional, `__init__.py`, `conftest.py`); 10 tests all passing.

## TDD Commit Trace

| Commit | Type | Message |
|--------|------|---------|
| `7c59e00` | RED | `test(03-01): failing tests for errors, types, and package exports` |
| `d95d8c0` | GREEN | `feat(03-01): beava package skeleton — errors, types, pyproject, pytest harness` |

Red commit: `pytest` exits non-zero with `ModuleNotFoundError: No module named 'beava'` — no impl files existed.
Green commit: 10/10 tests pass; ruff clean; mypy strict clean.

## Verification Results

```
pytest -q          → 10 passed in 0.01s
ruff check . → All checks passed!
mypy beava/        → Success: no issues found in 3 source files

python3 -c "import beava as bv; assert str(bv.ValidationError(kind='cycle', path='A.b', message='m')) == '[cycle] A.b: m'"  → OK
python3 -c "import beava as bv; assert bv.Optional[str] == bv.Optional[bv.Optional[str]]"  → OK
```

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] Broken pip@24.3.1 (corrupted resolvelib vendor)**
- **Found during:** Task 1.b (install step)
- **Issue:** `python -m pip install -e ".[dev]"` raised `ImportError: cannot import name 'RequirementInformation'` from pip's vendored resolvelib. The conda base environment had a partial pip reinstall that left the vendor in an inconsistent state.
- **Fix:** Bootstrapped pip@26.0.1 via `curl https://bootstrap.pypa.io/get-pip.py | python3`. Installation then succeeded.
- **Files modified:** None (pip is a system tool, not a repo file).
- **Impact:** None on repo state; dev install works on this machine after fix.

**2. [Rule 1 - Bug] Ruff import-sort violations in test files**
- **Found during:** Task 1.b post-implementation lint gate
- **Issue:** 3 ruff `I001` (import sort) violations across `test_errors.py`, `test_types.py`, and `test_package_init.py`; 1 `E501` (line > 100) in `test_package_init.py` docstring.
- **Fix:** Reordered imports to alphabetical within each import group; split long docstring across multiple lines; ran `ruff check --fix` for the remaining auto-fixable violation.
- **Files modified:** `python/tests/test_errors.py`, `python/tests/test_types.py`, `python/tests/test_package_init.py`

**3. [Rule 1 - Bug] mypy strict cannot infer slot type for `_OptionalSpec.inner`**
- **Found during:** Task 1.b mypy gate
- **Issue:** `__slots__ = ("inner",)` with `from __future__ import annotations` causes mypy strict to report `Cannot determine type of "inner"` — slots require explicit class-level variable annotations when annotations are postponed.
- **Fix:** Added `inner: Any` class-level annotation alongside `__slots__`.
- **Files modified:** `python/beava/_types.py`

## Known Stubs

The following Phase 3 stubs in `python/beava/__init__.py` are intentional and documented:

| Stub | File | Line | Resolved By |
|------|------|------|-------------|
| `event = _stub_event` | `beava/__init__.py` | ~37 | Plan 03-03 (`@bv.event` decorator) |
| `table = _stub_table` | `beava/__init__.py` | ~38 | Plan 03-03 (`@bv.table` decorator) |
| `col = _stub_col` | `beava/__init__.py` | ~39 | Plan 03-02 (`bv.col` expression DSL) |
| `App = _AppStub` | `beava/__init__.py` | ~40 | Plans 03-04 + 03-05 (`bv.App` client) |

These stubs satisfy `hasattr(bv, "event")` etc. and raise `NotImplementedError` with a plan-reference message when called. They do NOT block Plan 03-01's goal (error/type primitives). Downstream plans replace them.

## Self-Check: PASSED

- `python/beava/__init__.py` — FOUND
- `python/beava/_errors.py` — FOUND
- `python/beava/_types.py` — FOUND
- `python/tests/test_errors.py`, `test_types.py`, `test_package_init.py` — FOUND
- Commit `7c59e00` (red) — FOUND
- Commit `d95d8c0` (green) — FOUND
- `pytest -q` → 10 passed — VERIFIED
- `ruff check .` → clean — VERIFIED
- `mypy beava/` → clean — VERIFIED
