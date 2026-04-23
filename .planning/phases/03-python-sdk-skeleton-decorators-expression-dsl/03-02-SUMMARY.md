---
phase: "03"
plan: "03-02"
subsystem: python-sdk
tags: [python-sdk, dsl, expression, ast, tdd]
completed: "2026-04-23T00:00:00Z"
duration_minutes: 20

dependency_graph:
  requires:
    - python/beava package (from 03-01)
    - beava.ValidationError, RegistrationError, BinaryNotFoundError (from 03-01)
    - beava.Optional, Field, _types.py (from 03-01)
  provides:
    - python/beava/_col.py (_ExprAST base, _Field, _Literal, _BinOp, _UnaryOp, _Call, _BareIdent)
    - bv.col(name) → _Field leaf (public constructor)
    - Col = _ExprAST (public alias for isinstance checks)
    - infer_output_type(lhs, rhs, op) → str (SDK-COL-08 type inference)
    - Canonical grammar serialization via .to_expr_string() (D-08 LOCKED)
    - .referenced_fields() → set[str] for Phase 4 schema validation
  affects:
    - Plans 03-03, 03-04, 03-05, 03-06 (expression DSL available for filter/derive use)
    - Phase 4 (server-side evaluator parses the grammar emitted by to_expr_string())

tech_stack:
  added: []
  patterns:
    - TDD red-then-green commit pair (test: commit e29b042 then feat: commit d67a34f)
    - Operator-overloading AST pattern (_ExprAST base with all dunders)
    - _BinOp single parenthesization code path enforces D-08 invariant
    - bool-before-int check in _Literal.to_expr_string() (bool subclasses int)
    - _BareIdent marker for cast target type (renders bare, not quoted)
    - __hash__ = id(self) so AST nodes stay hashable after __eq__ override
    - String literal escaping via .replace() chain (T-03-02-01 mitigation)

key_files:
  created:
    - python/beava/_col.py
    - python/tests/test_col.py
  modified:
    - python/beava/__init__.py

decisions:
  - "_stub_col removed; replaced with from ._col import col, Col in __init__.py"
  - "Col = _ExprAST alias exported for downstream isinstance checks in decorators and ops"
  - "infer_output_type uses frozenset constants for op classification (clean, extensible)"
  - "Division always widens to f64 per D-08 discretion — avoids integer truncation surprises"
  - "bool is NOT numeric in infer_output_type — arithmetic on bool raises TypeError"
  - "to_expr_string() on _BinOp is the single code path for parenthesization (grep-provable)"
  - "referenced_fields() delegates to _collect_fields(out) abstract method — _Literal.collect is a no-op"

metrics:
  tasks_completed: 2
  subtasks: "1.a (red) + 1.b (green)"
  tests_added: 14
  tests_passing: 24
  files_created: 2
  files_modified: 1
---

# Phase 03 Plan 02: `bv.col(...)` Expression DSL Summary

**One-liner:** Operator-overloaded expression AST (`_ExprAST` / `_Field` / `_Literal` / `_BinOp` / `_UnaryOp` / `_Call`) with canonical parenthesized serialization via `to_expr_string()` and arithmetic type inference via `infer_output_type()`.

## What Was Built

- **`python/beava/_col.py`** — Clean-room reimplementation of the expression DSL:
  - `_ExprAST` base class with all 16 operator dunders (arithmetic r-forms included), `__hash__ = id(self)`, `.isnull()`, `.cast()`, `.to_expr_string()` (abstract), `.referenced_fields()`, `._collect_fields()` (abstract).
  - `_BareIdent` marker — values that serialize as bare identifiers (used for `cast(x, float)` type arg).
  - `_Field(name)` — leaf that emits the bare field name and populates `referenced_fields`.
  - `_Literal(value)` — leaf with full serialization: `None→null`, `True/False→true/false` (bool-before-int check!), `_BareIdent→bare`, `int/float→repr()`, `str→'...'` with `\\` doubled and `'` escaped (T-03-02-01 mitigation).
  - `_BinOp(op, left, right)` — the **single** parenthesization code path: `f"({left} {op} {right})"`.
  - `_UnaryOp(op, operand)` — emits `f"({op} {operand})"`.
  - `_Call(fn, args)` — emits `f"{fn}(arg1, arg2, ...)"`.
  - `_wrap(value)` — promotes plain Python values to `_Literal`.
  - `col(name: str) -> _ExprAST` — validates non-empty string, returns `_Field`.
  - `Col = _ExprAST` — public alias.
  - `infer_output_type(lhs, rhs, op)` — comparison ops → `"bool"`; boolean ops require `bool+bool`; arithmetic requires `i64/f64` (bool excluded); division always widens to `"f64"`; unknown op raises `ValueError`.

- **`python/beava/__init__.py`** — `_stub_col` / `col = _stub_col` replaced with `from ._col import Col, col`; `Col` added to `__all__`.

- **`python/tests/test_col.py`** — 14 test functions, 35+ individual assertions covering: field rendering, arithmetic parenthesization (including `radd`), all 6 comparison ops, boolean combinators (`& | ~` → `and or not`), `isnull`, `cast` (including TypeError on non-string arg), string literal escaping (apostrophe and backslash), bool/null literals, `referenced_fields` (excludes string literals and cast targets), `col()` argument validation, and all `infer_output_type` branches.

## TDD Commit Trace

| Commit | Type | Message |
|--------|------|---------|
| `e29b042` | RED | `test(03-02): failing tests for bv.col AST grammar + type inference` |
| `d67a34f` | GREEN | `feat(03-02): bv.col expression DSL — AST, canonical serialization, type inference` |

Red commit: `pytest` exits non-zero with `ModuleNotFoundError: No module named 'beava._col'` — `_col.py` did not exist.
Green commit: 14/14 col tests pass; 24/24 total tests pass; ruff clean; mypy strict clean.

## Verification Results

```
pytest tests/test_col.py -v    → 14 passed in 0.02s
pytest tests/ -q               → 24 passed in 0.03s  (10 from 03-01 + 14 new)
ruff check beava/ tests/        → All checks passed!
mypy beava/                     → Success: no issues found in 4 source files

python -c "import beava as bv; print((bv.col('a') + 1).to_expr_string())"
  → (a + 1)

python -c "import beava as bv; print(bv.col('x').isnull().to_expr_string())"
  → (x == null)

python -c "import beava as bv; print(bv.col('x').cast('float').to_expr_string())"
  → cast(x, float)

python -c "from beava._col import infer_output_type; print(infer_output_type('i64', 'f64', '+'))"
  → f64

python -c "import beava as bv; print(((bv.col('amount') > 100) & ~bv.col('blocked')).to_expr_string())"
  → ((amount > 100) and (not blocked))

grep -n 'to_expr_string' python/beava/_col.py | grep 'self.left\|self.op'
  → line 249: return f"({self.left.to_expr_string()} {self.op} {self.right.to_expr_string()})"
  (single parenthesization code path confirmed)
```

## Grammar Invariant Verification (D-08)

The plan requires that **every** binary op is parenthesized and that this is enforced by a **single** code path. `_BinOp.to_expr_string()` at line 249 is the only place that emits binary-op strings; it always wraps in `(...)`. The `grep` above proves there is no alternative path.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Ruff I001 import-sort violation in test_col.py**
- **Found during:** Task 1.b lint gate
- **Issue:** Initial import order had `pytest` in wrong group relative to `beava` (third-party). Ruff's I-section rule required `pytest` in its own group above `beava` imports.
- **Fix:** Moved `import pytest` to its own block above `import beava as bv` + `from beava._col import infer_output_type`.
- **Files modified:** `python/tests/test_col.py`
- **Commit:** included in `d67a34f`

### None architectural — plan executed exactly as written.

## Known Stubs

The following Phase 3 stubs remain in `python/beava/__init__.py`:

| Stub | File | Line | Resolved By |
|------|------|------|-------------|
| `event = _stub_event` | `beava/__init__.py` | ~35 | Plan 03-03 (`@bv.event` decorator) |
| `table = _stub_table` | `beava/__init__.py` | ~36 | Plan 03-03 (`@bv.table` decorator) |
| `App = _AppStub` | `beava/__init__.py` | ~37 | Plans 03-04 + 03-05 (`bv.App` client) |

`col` and `Col` are **no longer stubs** — they are the real implementation from this plan.

## Threat Flags

No new network endpoints, auth paths, or trust-boundary crossings introduced. The string-literal injection threat T-03-02-01 is fully mitigated: `_Literal.to_expr_string()` escapes `\\` and `'` before embedding any user-supplied string in the expression output.

## Self-Check: PASSED

- `python/beava/_col.py` — FOUND
- `python/tests/test_col.py` — FOUND
- `python/beava/__init__.py` (no `_stub_col` line) — VERIFIED
- Commit `e29b042` (red) — FOUND
- Commit `d67a34f` (green) — FOUND
- `pytest tests/test_col.py` → 14 passed — VERIFIED
- `pytest tests/` → 24 passed — VERIFIED
- `ruff check beava/ tests/` → clean — VERIFIED
- `mypy beava/` → clean — VERIFIED
- `grep -c "^def \|^class " python/beava/_col.py` → 10 (≥ 10 required) — VERIFIED
