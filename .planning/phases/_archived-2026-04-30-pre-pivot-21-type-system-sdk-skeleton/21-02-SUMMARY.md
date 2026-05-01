---
phase: 21-type-system-sdk-skeleton
plan: 02
subsystem: python-sdk
tags: [sdk, v0, stateless-ops, dag, validation, decorators]
requires: [21-01]
provides:
  - "StatelessOpsMixin — 8 per-row ops (filter/map/select/drop/rename/with_columns/cast/fillna) on Stream and Table"
  - "StreamDerivation / TableDerivation descriptors returned by ops + function-form decorators"
  - "@tl.stream def X(a: A) -> Stream: and @tl.table(key=) def X(...) -> Table: function forms"
  - "build_dag(descriptors): name-indexed adjacency map from parameter-declared upstreams"
  - "DAG.topological_order: Kahn's (deterministic) + DFS cycle trace"
  - "CycleError with cycle_path list[str] + 'A → B → C → A' __str__"
  - "MissingDependency when a derivation's upstream class isn't registered"
  - "tally.validate(*descriptors) -> list[ValidationError] — no TCP"
  - "ValidationError(kind, path, message) re-added to public surface"
  - "App.validate() and App.register() — validates first, raises on error, sends frames in topological order with dedupe by name"
affects:
  - "tally.__init__ public exports (validate/ValidationError/StreamSource/StreamDerivation/TableSource/TableDerivation added)"
  - "test_v0_public_surface: validate/ValidationError removed from 21-01 removed-symbols list"
tech-stack:
  added: []  # stdlib-only (typing, inspect, re)
  patterns:
    - "Mixin-driven stateless op catalog — Stream and Table get identical surface via StatelessOpsMixin"
    - "Function invocation-at-registration: @tl.stream def body runs once, upstream descriptors passed positionally"
    - "Frame-walking forward-ref resolution in _resolve_func_hints so decorators work inside test methods"
    - "Kahn's algorithm with alphabetical tie-break for deterministic topo order; DFS from smallest residual node on cycle"
    - "Reserved 'ops' key in REGISTER JSON payload — Rust engine doesn't yet consume, Phase 22 is first to wire through"
key-files:
  created:
    - python/tally/_stateless_ops.py
    - python/tally/_dag.py
    - python/tally/_validate_v0.py
    - python/tests/test_v0_stateless_ops.py
    - python/tests/test_v0_dag.py
    - python/tests/test_v0_validate.py
  modified:
    - python/tally/_stream.py        # StreamDerivation + function-form decorator + _resolve_func_hints
    - python/tally/_table.py         # TableDerivation + function-form decorator
    - python/tally/_app.py           # App.register validation wiring + App.validate()
    - python/tally/__init__.py       # re-add validate/ValidationError + new Derivation exports
    - python/tests/test_v0_public_surface.py  # unblock validate/ValidationError from 21-01 removed list
decisions:
  - ".map is a thin alias for .with_columns (identical behavior) — DataFrame-parity names without duplicate logic"
  - "v0 cast target types: int/float/str/bool only; datetime/bytes casts deferred"
  - "_reparse_referenced_fields strips single-quoted string literals + cast target idents before tokenizing to avoid false 'missing field' errors on strings like '/checkout'"
  - "DAG cycle path uses alphabetical DFS start for reproducible error messages across runs (mitigates risk noted in plan)"
  - "App.register dedupes REGISTER frames by name after topological walk — a source used by two derivations registers exactly once"
  - "Stream-to-Table conversion (group_by/agg) stays out of 21-02; Table function-form bodies must return a Table — until 21-03 adds aggregation, meaningful Stream→Table derivations error out with a precise TypeError"
metrics:
  duration: "~20 min"
  completed: 2026-04-14
---

# Phase 21 Plan 02: Type system & SDK skeleton — stateless ops, DAG, validation

v0 Python SDK composition surface landed. Function-form decorators build
`StreamDerivation` / `TableDerivation` descriptors, the 8 stateless ops
chain on both Stream and Table with surgical schema propagation, and
`tally.validate(*descriptors)` returns typed `ValidationError`s without
any TCP — safe for unit tests. `App.register` now validates first,
topologically orders frames, and dedupes by name.

## What Shipped

### Stateless operator catalog (`_stateless_ops.py`)

`StatelessOpsMixin` provides the following methods on every Stream /
Table subclass. Each one:

1. Rejects `str` / non-`_ExprAST` inputs where an expression is expected.
2. Validates every referenced field via `_ExprAST.referenced_fields()` /
   explicit field-name args against `self._schema`, reporting **all**
   missing fields in a single `TypeError` with Levenshtein hints.
3. Computes a pure schema-in → schema-out transform.
4. Calls `self._derive(schema=…, op=…)` which returns a new descriptor
   of the same outer runtime type wrapping the upstream + appended op.

| Op | Serialized shape | Schema transform |
|----|------------------|------------------|
| `filter(expr)` | `{op: "filter", expr: "<engine-string>"}` | unchanged |
| `select(*names)` | `{op: "select", fields: [...]}` | only listed fields, in args order |
| `drop(*names)` | `{op: "drop", fields: [...]}` | remaining fields in original order |
| `rename(**map)` | `{op: "rename", mapping: {old: new}}` | renamed fields, types preserved |
| `with_columns(**exprs)` | `{op: "with_columns", exprs: {name: expr_str}}` | add/replace fields with inferred types |
| `map(**exprs)` | identical to `with_columns` | alias — DataFrame parity |
| `cast(**map)` | `{op: "cast", casts: {name: type_name}}` | field py_type updated (int/float/str/bool only) |
| `fillna(**defaults)` | `{op: "fillna", defaults: {...}}` | optional flag cleared on affected fields |

**Table-specific invariants (enforced in the mixin):**
- `.drop(key_field)` raises `TypeError("cannot drop key field 'X' from Table 'Y'")`.
- `.rename(old=new)` where `old` is in `_key` cascades the rename: the
  returned `TableDerivation._key` is rewritten.

**Type inference (`_infer_expr_type`):** Arithmetic BinOps → `float`;
comparison/boolean → `bool`; bare field ref → schema's declared type;
literal → `type(value)`; `cast(x, <t>)` call → the mapped type; else
falls back to `object`. Deliberately minimal for v0 — Phase 22 can
refine.

### Function-form decorators (`_stream.py`, `_table.py`)

```python
@tl.stream
def Checkouts(clicks: Clicks) -> tl.Stream:
    return clicks.filter(tl.col("page") == "/checkout")
```

At decoration time, `@tl.stream` / `@tl.table` detects
`FunctionType` (via `isinstance`) and routes to
`_build_stream_derivation_from_func` / `_build_table_derivation_from_func`:

1. Resolve type hints via `_resolve_func_hints` (detailed below).
2. Require a `-> Stream` or `-> Table` return annotation — else
   `TypeError`.
3. Require ≥1 parameter — else
   `TypeError("derivation function 'X' has no upstreams; annotate
   parameters with your Stream/Table types")`.
4. Invoke the function **once** with the upstream descriptors
   (passed positionally, in parameter order).
5. Check the runtime return type matches the declared return type;
   if a Stream-annotated function returns a Table (or vice-versa),
   raise `TypeError` naming the mismatch.
6. If the returned object is already a `StreamDerivation` /
   `TableDerivation`, rewrite `_name = func.__name__`, set
   `_upstreams = [<parameter-resolved descriptors>]`, stash `_func`
   and `_type_hints`. If it's a source, wrap it in a passthrough
   derivation so `_upstreams` lives on the descriptor.

**For Tables:** if the returned Table's `_key` doesn't match the
decorator's `key=` argument after any renames in the op chain, raise
`TypeError` comparing declared-vs-actual.

**`_resolve_func_hints` (forward-ref fix):** `typing.get_type_hints`
fails when annotations reference names that only exist in an enclosing
function's locals — extremely common under pytest, where decorators
get applied inside test methods. The helper first walks up to 8
enclosing frames via `sys._getframe`, builds a composite `localns`,
and retries `get_type_hints(func, localns=localns)`. On continued
failure it eval's each string annotation individually against
`func.__globals__ + localns`. This was a Rule 3 fix discovered mid-Task 2.

### DAG construction (`_dag.py`)

`build_dag(descriptors: list) -> DAG`:
- Builds two indexes: `_name` → descriptor, and `id(descriptor)` →
  name. The identity index lets us resolve `_upstreams` entries that
  are either the upstream descriptor object itself or a class literal
  that happens to be one of the registered descriptors.
- For each descriptor, walks `_upstreams`, resolves by identity first
  then by name; on miss, raises `MissingDependency(missing, context)`.
- Returns a `DAG(nodes, edges)` with `edges[name] = [upstream_names]`.

`DAG.topological_order()` uses Kahn's algorithm with an
**alphabetical** ready-queue tie-break so the emitted order is
reproducible across runs. On cycle: `_find_cycle` runs DFS from the
alphabetically-smallest residual node, explores upstreams in
alphabetical order, and returns a cycle path `[start, ..., start]`.
`CycleError.__str__` formats it as
`"Circular dependency detected: A → B → C → A. Break the cycle by
removing one edge."`.

### Local validation (`_validate_v0.py`)

`validate(*descriptors) -> list[ValidationError]`:
1. `build_dag(...)` — `MissingDependency` → `ValidationError(kind="missing_dep")`.
2. `topological_order()` — `CycleError` → `ValidationError(kind="cycle", path="A → B")`.
3. Per derivation, re-propagate the schema through `_ops` and append
   `ValidationError(kind="schema_mismatch", path="Name.op[idx]")` for
   any field reference that doesn't resolve. Useful when user code
   mutates `_ops` post-construction.

`ValidationError(kind, path, message)` is exported at
`tally.ValidationError` (re-adding the name that 21-01 removed). Its
`__str__` returns `"[kind] at path: message"`.

`_reparse_referenced_fields(expr_str)` — best-effort bare-identifier
extractor. Strips `'…'` string literals (honouring `\'` and `\\`) and
filters `int`/`float`/`str`/`bool`/`cast` keywords before returning
the remaining tokens. This avoids spurious `schema_mismatch` errors on
expressions like `page == '/checkout'` where the quoted string would
otherwise tokenize as `checkout`.

### `App.register` / `App.validate` (`_app.py`)

```python
app.register(*descriptors)   # validates; raises first error; sends in topo order; dedupes by name
app.validate(*descriptors)   # returns list[ValidationError] — no TCP
```

**Register algorithm:**
1. Call `validate(*descriptors)`. If non-empty, raise
   `ValidationError` with the first error's `kind`/`path`/`message`.
   When there are N>1 errors the message appends
   `"…and {N-1} more validation errors — call tally.validate() to see all"`.
2. Call `build_dag(descriptors)` + `topological_order()`.
3. For each node in topological order, call
   `_collect_registrations()` (source descriptors return their single
   frame; derivations walk upstreams depth-first, then append their
   own frame). Dedupe by `name` using a `seen` set — a source used by
   two derivations registers exactly once.

## Public Export Delta

**Added (re-exported on `tally`):**
`validate`, `ValidationError`, `StreamSource`, `StreamDerivation`,
`TableSource`, `TableDerivation`.

**Unchanged from 21-01:** `stream`, `table`, `Optional`, `Field`,
`col`, `Stream`, `Table`, `App`, `FeatureResult`, `TallyError`,
`ConnectionError`, `ProtocolError`, `OperatorBase`, protocol opcodes.

## REGISTER JSON wire-format extension

Both `StreamDerivation._compile` and `TableDerivation._compile` add
two forward-compat keys:

```json
{
  "name": "Checkouts",
  "key_field": null,
  "features": [],
  "fields": { "...": "..." },
  "ops": [
    {"op": "filter", "expr": "(page == '/checkout')"},
    {"op": "select", "fields": ["user_id"]}
  ],
  "depends_on": ["Clicks"]
}
```

The Rust engine at its current v2.0 state does NOT interpret `ops` —
it's reserved. Phase 22 is the first phase that consumes `ops` through
the aggregation pipeline. Any real `App.register()` call against a
live server in the meantime would fail the engine's JSON parse if the
server rejects unknown keys. Per plan risk mitigation: unit tests mock
the client, so this doesn't block 21-02.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 — blocking] Frame-walking forward-ref resolution in `_resolve_func_hints`**
- **Found during:** Task 2
- **Issue:** `typing.get_type_hints(func)` raised `NameError` when the
  decorator was applied inside a test class method (`Clicks` was a
  local of the test method, not visible to `get_type_hints`).
- **Fix:** Walk up to 8 enclosing frames via `sys._getframe`, build a
  composite `localns`, pass it to `get_type_hints`; fall back to
  per-annotation `eval` against `func.__globals__ + localns`.
- **Files modified:** `python/tally/_stream.py`,
  `python/tally/_table.py` (imports helper from `_stream`).
- **Commit:** b0ebf2e

**2. [Rule 3 — blocking] Unblock `validate` / `ValidationError` from 21-01 removed-symbols list**
- **Found during:** Task 3
- **Issue:** `test_v0_public_surface.py::TestRemovedSurface` asserts
  `validate` and `ValidationError` are NOT on `tally`. Plan 21-01
  removed them; Plan 21-02's `<behavior>` explicitly re-adds them.
- **Fix:** Removed both names from `_REMOVED_PUBLIC_SYMBOLS`, left an
  inline comment pointing at 21-02.
- **Files modified:** `python/tests/test_v0_public_surface.py`.
- **Commit:** 3a327d9

**3. [Rule 1 — bug] `_reparse_referenced_fields` flagged string-literal contents as missing fields**
- **Found during:** Task 3
- **Issue:** The naive `re.findall(r"[A-Za-z_]\w*", expr)` tokenizer
  picked up words inside single-quoted strings, so
  `page == '/checkout'` reported `checkout` as a missing field.
- **Fix:** Strip `'...'` literals (honouring `\'` and `\\` escapes)
  before tokenizing; also filter the four cast-target idents
  (`int`/`float`/`str`/`bool`) as keywords.
- **Files modified:** `python/tally/_validate_v0.py`.
- **Commit:** 3a327d9

## Test Counts

- **Pre:** 250 passed, 1 skipped (after 21-01).
- **Post:** 330 passed, 2 skipped (added
  `test_v0_stateless_ops.py` — 56 tests,
  `test_v0_dag.py` — 14 tests,
  `test_v0_validate.py` — 12 tests). `test_integration.py` remains
  module-skipped per 21-01.

## Rust Engine

Zero changes. `ops` and `depends_on` keys in REGISTER JSON are
forward-compat stubs — Phase 22 is the first phase to wire them
through the Rust side. All unit tests mock the client.

## Commits

| Task | Commit  | Subject |
|------|---------|---------|
| 1    | 4428c06 | feat(21-02): stateless operator catalog on Stream and Table |
| 2    | b0ebf2e | feat(21-02): function-form decorators + DAG discovery |
| 3    | 3a327d9 | feat(21-02): local validate() + App.register/validate wiring |

## Self-Check: PASSED

- All 6 created files present on disk:
  `python/tally/_stateless_ops.py`, `python/tally/_dag.py`,
  `python/tally/_validate_v0.py`, `python/tests/test_v0_stateless_ops.py`,
  `python/tests/test_v0_dag.py`, `python/tests/test_v0_validate.py`.
- All 5 modified files modified on disk:
  `python/tally/_stream.py`, `python/tally/_table.py`,
  `python/tally/_app.py`, `python/tally/__init__.py`,
  `python/tests/test_v0_public_surface.py`.
- All 3 task commits present: 4428c06, b0ebf2e, 3a327d9.
- Full verification suite (330 tests, 2 skipped) passes in 0.29s.
- Manual smoke: `tl.validate(Clicks, Checkouts)` returns `[]` for
  a Stream-source + function-form Stream-derivation pipeline.
