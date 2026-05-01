---
phase: 21-type-system-sdk-skeleton
plan: 01
subsystem: python-sdk
tags: [sdk, v0, schema, decorators, expression-dsl]
requires: []
provides:
  - "Stream / StreamSource runtime types + @tl.stream class-form decorator"
  - "Table / TableSource runtime types + @tl.table class-form decorator"
  - "tl.Optional marker (distinct from typing.Optional)"
  - "tl.Field(desc=, default=) attribute metadata"
  - "tl.col expression DSL — arithmetic/comparison/boolean/cast/isnull → engine grammar string"
  - "Schema extractor with Levenshtein mismatch suggestions"
  - "Deterministic .describe() introspection"
affects:
  - "tally public import surface (Phase 16 source/dataset API removed)"
tech-stack:
  added: []  # stdlib-only: dataclasses, typing
  patterns:
    - "Class-as-descriptor decorator pattern (inspect annotations, no __init__)"
    - "AST capture via operator overloading for tl.col"
    - "Levenshtein suggest() for surgical schema errors"
key-files:
  created:
    - python/tally/_types_core.py
    - python/tally/_schema_v0.py
    - python/tally/_col.py
    - python/tally/_describe.py
    - python/tally/_stream.py
    - python/tally/_table.py
    - python/tests/test_v0_schema.py
    - python/tests/test_v0_col.py
    - python/tests/test_v0_decorators.py
    - python/tests/test_v0_public_surface.py
  modified:
    - python/tally/__init__.py
    - python/tally/_app.py        # register() docstring only
    - python/tests/test_app.py    # module-level pytest.skip
    - python/tests/test_integration.py  # module-level pytest.skip
  deleted:
    - python/tally/_source.py
    - python/tally/_dataset.py
    - python/tally/_schema.py
    - python/tally/_validate.py
    - python/tests/test_source.py
    - python/tests/test_dataset.py
    - python/tests/test_dataset_behaviors.py
    - python/tests/test_new_api.py
decisions:
  - "tl.Optional uses a dedicated _OptionalSpec wrapper rather than typing.Optional → the schema model explicitly distinguishes nullable from union-with-None"
  - "Single-field table keys emit key_field as a string, composite keys emit key_fields list (matches existing engine contract)"
  - "test_app.py was module-skipped (not deleted) because it tests App protocol wiring; Phase 26 will port it by replacing the @source/@dataset fixtures with @tl.stream/@tl.table"
  - "OperatorBase kept in public __all__ as an internal anchor for Plan 21-03 aggregation-spec descriptors"
metrics:
  duration: "~25 min"
  completed: 2026-04-14
---

# Phase 21 Plan 01: Type system & SDK skeleton Summary

v0 Python SDK foundation landed: @tl.stream + @tl.table class-form decorators,
tl.col expression DSL serialising to the existing Rust engine grammar, and
.describe() introspection — with the Phase 16 @tl.source/@tl.dataset/EventSet/
FeatureSet surface fully deleted.

## What Shipped

- **Schema primitives** (`_types_core.py`): `Optional[T]` marker,
  `Field(desc=, default=)` descriptor, `FieldSpec` dataclass, `MISSING` sentinel.
- **Schema extractor** (`_schema_v0.py`): `extract_schema(cls)` reads class
  annotations + `Field()` markers into an ordered `FieldSpec` map; rejects
  class bodies containing methods with a surgical error pointing the user
  at the function-form decorator (Plan 21-02); `suggest()` pure-Python
  Levenshtein; `schema_mismatch_error()` shared message builder.
- **tl.col DSL** (`_col.py`): `Col` AST supporting `+ - * / > >= < <= == !=`,
  Python `& | ~` → engine `and / or / not`, `.cast(type_name)`, `.isnull()`,
  and `.referenced_fields()` for downstream schema validation. Emits strings
  that re-parse cleanly under `src/engine/expression.rs` (every BinOp is
  parenthesised).
- **Stream decorator** (`_stream.py`): `Stream` runtime type + `StreamSource`
  descriptor; `@tl.stream` bare and `@tl.stream(history_ttl="…")` parameterized
  forms. Function form raises `NotImplementedError` pointing at Plan 21-02.
- **Table decorator** (`_table.py`): `Table` + `TableSource`; enforces
  `key=` (single str or composite list), `mode="append"` (default) or
  `mode="changelog"` → `NotImplementedError("ships in v0.1")`; key field
  membership validated against the declared schema with Levenshtein hints.
- **describe()** (`_describe.py`): deterministic declaration-ordered dict
  (`name`, `kind`, `key`, `mode`, `fields`, optional `ttl`/`history_ttl`).

## Public Export Delta

**Added:** `stream`, `table`, `Optional`, `Field`, `col`, `Stream`, `Table`

**Removed:** `source`, `dataset`, `group_by`, `union`, `EventSet`, `FeatureSet`,
`validate`, `ValidationError`, and the lowercase operator aliases
(`count`, `sum`, `avg`, `min`, `max`, `distinct_count`, `last`, `stddev`,
`percentile`, `derive`, `lookup`, `lag`, `ema`, `last_n`, `first`, `exact_min`,
`exact_max`) — the latter return in Plan 21-03 as aggregation-spec descriptors.

**Retained:** `App`, `FeatureResult`, `TallyError`, `ConnectionError`,
`ProtocolError`, protocol opcodes, `OperatorBase`.

## Deviations from Plan

- **[Rule 3 — blocking]** `test_app.py` is module-skipped, not retained.
  Spot-check during Task 3 showed it imports `source`, `dataset`, `group_by`
  at module top and uses them to build the register-flow fixtures
  (`test_app.py:27,101-118`). Rather than surgically rewrite it (Phase 26
  territory), applied the same `pytest.skip(..., allow_module_level=True)`
  pattern the plan already prescribed for `test_integration.py`. Contract
  validation moved to the new `test_v0_public_surface.py::TestRegisterIntegration`
  which exercises `App.register` against mock clients using real
  `StreamSource` / `TableSource` descriptors.
- **[Rule 3 — blocking]** Added forward-reference resolution to
  `extract_schema`. With `from __future__ import annotations` (used by all
  test modules + SDK files) Python stores annotations as strings. The
  initial implementation only accepted already-resolved types; I layered a
  small eval namespace (primitives + stdlib `datetime`/`date`/`time` +
  `Optional` marker + the class's defining module globals) so
  `ts: datetime` resolves without forcing users to import types at module
  top.

## Engine-Grammar Notes

`Col.to_expr_string()` matches `src/engine/expression.rs` (winnow Pratt
parser) expectations:

- Every `BinOp` is wrapped in parentheses → unambiguous re-parse.
- Keywords `and` / `or` / `not` (not `&&` / `||` / `!`).
- `null` / `true` / `false` lowercase literals.
- Single-quoted string literals with `\'` escape for embedded apostrophes
  and `\\` escape for backslashes.
- `cast(expr, type_name)` uses a bare identifier for the target type (no
  quotes around `float`, `int`, etc.) matching the engine's `FnCall` parser.

Qualified field access (`col("Transactions.amount")`) passes through as a
bare dotted identifier — the Rust parser recognises this as
`FieldRef::Qualified`.

## Test Counts

- **Pre:** 8 test modules in `python/tests/` (test_app, test_client,
  test_dataset, test_dataset_behaviors, test_integration, test_new_api,
  test_operators, test_protocol, test_source, test_types).
- **Post:** 9 test modules — deleted 4 (test_source, test_dataset,
  test_dataset_behaviors, test_new_api); added 4 (test_v0_schema,
  test_v0_col, test_v0_decorators, test_v0_public_surface);
  module-skipped 2 (test_app, test_integration).
- **Active test count:** 250 passed, 1 skipped (run time 0.28s).

## Rust Engine

Zero changes. `grep` of `src/` for `tl.source | tl.dataset | EventSet |
FeatureSet` returns no hits — engine was already decoupled from SDK naming.

## Self-Check: PASSED

- All 10 created files exist on disk (verified).
- All 8 deleted files absent on disk (verified).
- 3 task commits present: 593b522, 607b455, 4fde0eb.
- Full verification suite (250 tests) passes.
