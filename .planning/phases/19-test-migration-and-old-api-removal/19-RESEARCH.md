# Phase 19: Test Migration and Old API Removal - Research

**Researched:** 2026-04-12
**Domain:** Python SDK test migration, API surface removal, benchmark migration
**Confidence:** HIGH

## Summary

Phase 19 migrates all existing tests from two legacy APIs (`@st.stream`/`@st.view` decorator API and `_dataframe.py` DataFrame-style API) to the new v2.0 API (`@tl.source`, `@tl.dataset`, `EventSet`, `FeatureSet`), then removes the old API files and symbols. The benchmark harness (`bench.py`) also uses the old API and must be migrated.

The current test count is approximately 744 total: ~377 Python test functions (331 collectable + ~46 in broken `test_protocol.py`), ~99 Rust integration tests, and ~629 Rust inline tests. The Rust tests do NOT reference Python API names, so only Python tests need migration. Of the Python tests, approximately 132 tests directly test old API constructs (33 in test_stream.py, 12 in test_view.py, 50 in test_dataframe.py, 17 in test_integration.py, 20 in test_app.py). The remaining tests are API-agnostic (operators, types, client, protocol) or already use the new API (59 in test_new_api.py).

**Primary recommendation:** Migrate tests in four waves: (1) port integration tests and app tests to new API, (2) rewrite test_stream.py and test_view.py as test_source.py and test_dataset.py testing the new equivalents, (3) rewrite test_dataframe.py behavioral tests as additional test_dataset.py coverage, (4) delete old API files, remove old symbols from __init__.py, run full suite to verify >= 744 tests, then run benchmark matrix.

<user_constraints>
## User Constraints (from CONTEXT.md)

### Locked Decisions
All implementation choices are at Claude's discretion -- infrastructure phase. Key constraints from STATE.md critical pitfalls:
- C-2: Old API removal breaks 744 tests -- port ALL tests first, verify count >= 744, THEN delete.
- C-4: Two APIs being replaced -- @st.stream AND _dataframe.py. Test migration covers both.
- Old API removed, not deprecated alongside (clean break before launch -- per PROJECT.md decision).
- Order: migrate tests first -> verify count -> delete old API -> verify again -> benchmark.

### Claude's Discretion
All implementation choices are at Claude's discretion -- infrastructure phase.

### Deferred Ideas (OUT OF SCOPE)
None -- infrastructure phase.
</user_constraints>

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|------------------|
| MIG-01 | All existing tests (>= 744) are ported to the new `@tl.source`/`@tl.dataset` API before old API removal | Test inventory below maps every file; migration patterns documented |
| MIG-02 | Old `@st.stream`, `@st.view`, and `_dataframe.py` public API are removed from the SDK | File deletion list and __init__.py cleanup documented |
| MIG-03 | No performance regression -- full benchmark matrix passes within -5% of 1.1M eps baseline after all changes | bench.py migration pattern documented; matrix command identified |
</phase_requirements>

## Standard Stack

No new libraries needed. This phase is purely test rewriting, file deletion, and benchmark migration within the existing codebase.

### Core Tools
| Tool | Version | Purpose | Why Standard |
|------|---------|---------|--------------|
| pytest | (installed) | Python test runner | Already in use |
| cargo test | (installed) | Rust test runner | Already in use |
| bench.py | custom | Throughput benchmark | Already exists at benchmark/tally-throughput/bench.py |

## Architecture Patterns

### Test File Migration Map

Current state -- files that reference old API and what happens to each:

```
python/tests/
├── test_stream.py      (33 tests)  → REWRITE as test_source.py + test_dataset.py
├── test_view.py        (12 tests)  → REWRITE as additional test_dataset.py tests (views → datasets with only derives)
├── test_dataframe.py   (50 tests)  → REWRITE behavioral coverage into test_dataset.py
├── test_integration.py (17 tests)  → MIGRATE: replace @st.stream → @tl.source/@tl.dataset
├── test_app.py         (20 tests)  → MIGRATE: replace stream fixtures with new API
├── test_expr.py        (51 tests)  → PARTIAL: remove _FakeTable, keep expression tests (they test server-side evaluator)
├── test_new_api.py     (59 tests)  → KEEP: remove 2 compat tests that import old stream
├── test_operators.py   (53 tests)  → KEEP AS-IS: tests operator classes directly, API-agnostic
├── test_types.py       (16 tests)  → KEEP AS-IS: tests FeatureResult, API-agnostic
├── test_client.py      (20 tests)  → KEEP AS-IS: tests TCP client, API-agnostic
├── test_protocol.py    (~46 tests) → FIX: broken import (encode_push renamed to encode_push_binary)
└── conftest.py                     → MIGRATE: import tally as tl, use new API for fixtures
```

[VERIFIED: codebase grep + pytest --co]

### Pattern 1: @st.stream(key=...) → @tl.source + @tl.dataset

Old pattern:
```python
import tally as st

@st.stream(key="user_id")
class Transactions:
    tx_count_1h = st.count(window="1h")
    tx_sum_1h = st.sum("amount", window="1h")
    rate = st.derive("tx_count_1h / tx_sum_1h")
```

New equivalent:
```python
import tally as tl
from tally import source, dataset, group_by

@source
class RawTransactions:
    pass

@dataset(depends_on=[RawTransactions])
class Transactions:
    features = group_by("user_id").agg(
        tx_count_1h=tl.count(window="1h"),
        tx_sum_1h=tl.sum("amount", window="1h"),
    )
    rate = tl.derive("tx_count_1h / tx_sum_1h")
```

[VERIFIED: python/tally/_source.py, python/tally/_dataset.py]

### Pattern 2: Keyless @st.stream() → @tl.source

Old:
```python
@st.stream()
class RawEvents:
    pass
```

New:
```python
@source
class RawEvents:
    pass
```

[VERIFIED: python/tally/_source.py]

### Pattern 3: @st.stream(key=..., depends_on=[...]) → @tl.dataset(depends_on=[...])

Old:
```python
@st.stream()
class RawEvents:
    pass

@st.stream(key="user_id", depends_on=[RawEvents])
class UserFeatures:
    count_1h = st.count(window="1h")
```

New:
```python
@source
class RawEvents:
    pass

@dataset(depends_on=[RawEvents])
class UserFeatures:
    features = group_by("user_id").agg(
        count_1h=tl.count(window="1h"),
    )
```

[VERIFIED: python/tally/_dataset.py]

### Pattern 4: @st.view → @tl.dataset (derive-only, no group_by)

Old:
```python
@st.view(key="user_id")
class UserRisk:
    score = st.derive("Transactions.tx_count_1h > 10")
```

New (views become datasets with only derive features and no group_by):
```python
@dataset(depends_on=[Transactions])
class UserRisk:
    features = group_by("user_id").agg()  # empty agg, key only
    score = tl.derive("Transactions.tx_count_1h > 10")
```

Note: Views compile to the same RegisterRequest JSON format -- they just have `"type": "view"` in the old API. The new DatasetDef compiles to a keyed stream with derive features. Need to verify this produces compatible server-side registration. [ASSUMED -- may need the view "type" field or server may accept without it]

### Pattern 5: bench.py Pipeline Definitions

bench.py defines 3 pipeline shapes (small/medium/large) using `@st.stream` and `@st.view`. Each must be rewritten to `@tl.source` + `@tl.dataset`. The pipeline registration call `app.register(*streams)` already accepts DatasetDef objects. [VERIFIED: python/tally/_app.py register() method]

### Pattern 6: app.push() Stream Name Resolution

`app.push()` calls `stream_class._tally_stream_name` to get the stream name. Both old StreamMeta and new SourceDef/DatasetDef expose this property. No change needed in push calls, only in the class definitions. [VERIFIED: _app.py:195, _source.py:52, _dataset.py:162]

### Anti-Patterns to Avoid
- **Migrating Rust tests for Python API changes:** Rust tests don't reference Python API names. They test server-side logic. Leave them untouched.
- **Changing operator constructors:** `tl.count(window="1h")`, `tl.sum(...)` etc. are the same in both APIs. Only the decorator/class structure changes.
- **Removing _expr.py prematurely:** `_expr.py` defines `Column`, `Expr`, `EventProxy` used by `_dataframe.py`. These should be removed together with `_dataframe.py`, but test_expr.py tests the expression *evaluation* logic that may still be useful. Need to check if expression tests cover server-side behavior or only DataFrame API behavior.

## Files to Delete (MIG-02)

After all tests are migrated:

### SDK files to delete:
| File | Reason |
|------|--------|
| `python/tally/_stream.py` | Old `@stream` decorator and `StreamMeta` metaclass |
| `python/tally/_view.py` | Old `@view` decorator |
| `python/tally/_dataframe.py` | DataFrame-style API (Stream, Table, GroupBy, JoinedTable, Dataset) |
| `python/tally/_expr.py` | Expression tree nodes only used by `_dataframe.py` (Column, Expr, EventProxy) |

[VERIFIED: grep shows `_expr.py` only imported by `_dataframe.py` and `__init__.py`]

### Test files to delete:
| File | Reason |
|------|--------|
| `python/tests/test_stream.py` | Replaced by test_source.py/test_dataset.py |
| `python/tests/test_view.py` | Replaced by test_dataset.py view-equivalent tests |
| `python/tests/test_dataframe.py` | Replaced by test_dataset.py behavioral tests |

### __init__.py symbols to remove:
```python
# REMOVE these imports:
from tally._stream import stream
from tally._view import view
from tally._expr import Column, Expr, EventProxy
from tally._dataframe import Stream as DataStream, Table, GroupBy, JoinedTable, Dataset

# REMOVE these from __all__:
"stream", "view",
"Column", "Expr", "EventProxy",
"DataStream", "Table", "GroupBy", "JoinedTable", "Dataset",
```

[VERIFIED: python/tally/__init__.py lines 22, 23, 27-28, __all__ list]

### _app.py cleanup:
- Remove `source()` method that creates DataFrame Stream (lines 100-114) [VERIFIED]
- Remove `serve()` method (lines 116-126) [VERIFIED]
- Remove `register_all()` method (lines 128-148) [VERIFIED]
- Remove DataFrame Dataset import (line 52) [VERIFIED]
- Keep `register()` method -- it supports new API via `_collect_registrations()` and `_to_register_json()` protocols [VERIFIED]

Wait -- `register_all()` and `serve()` are DataFrame-only methods. But `register()` is used by BOTH old decorator API AND new API. Keep `register()`.

Actually, re-reading `_app.py`: the `source()` instance method creates a DataFrame `Stream`, which is different from `@tl.source` decorator. The `@tl.source` decorator returns a `SourceDef` which is a standalone object, not created by `App`. So `App.source()`, `App.serve()`, and `App.register_all()` are DataFrame-specific and should be removed.

[VERIFIED: _app.py lines 100-148]

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| Test counting | Manual grep count | `pytest --co -q 2>&1 \| tail -1` | Accurate collection count including parametrized tests |
| API symbol audit | Manual file reading | `grep -r "st\.stream\|st\.view\|_dataframe" python/` | Catches all references |
| JSON equivalence | Visual comparison | `assert old._to_register_json() == new._compile()` | Regression safety during migration |

## Common Pitfalls

### Pitfall 1: Test Count Drops Below 744
**What goes wrong:** Deleting old test files before writing replacements, or writing fewer replacement tests than the originals.
**Why it happens:** Old test files test old API behaviors (metaclass, decorator validation) that have different but equivalent behaviors in the new API.
**How to avoid:** (1) Write all new tests BEFORE deleting any old tests. (2) Run `pytest --co -q` after migration and before deletion to verify count >= 744. (3) Many test_stream.py tests verify error handling (e.g., view with non-derive operators raises TypeError) -- these error cases still exist in the new API and must have equivalent tests.
**Warning signs:** Test count drops during migration. Run count check after each wave.

### Pitfall 2: RegisterRequest JSON Shape Mismatch
**What goes wrong:** New API produces slightly different JSON than old API, causing server rejection.
**Why it happens:** DatasetDef._compile() may not emit `"type": "view"` for view-equivalent datasets, or may emit `"depends_on"` differently.
**How to avoid:** test_new_api.py already has `TestJsonCompat` tests (2 tests) that verify old and new produce same JSON. Use these as the ground truth. Add more if needed.
**Warning signs:** Integration tests pass in isolation but fail when hitting the server.

### Pitfall 3: bench.py Uses Class Name as Stream Name
**What goes wrong:** `app.push(Transactions, event)` resolves `Transactions._tally_stream_name` which is the class name. If the new `@source` class is named differently (e.g., `RawTransactions` for the source, `Transactions` for the dataset), the push target changes.
**Why it happens:** In old API, `@st.stream(key="user_id") class Transactions` is both the source and the keyed aggregation. In new API, you need a separate source and dataset.
**How to avoid:** Keep the *dataset* class named `Transactions` so `app.push(Transactions, event)` resolves to the same stream name. But wait -- push goes to the SOURCE, not the dataset. Need to verify: does `app.push()` push to a source (keyless) or a keyed stream?
**Warning signs:** "stream not found" errors during benchmark.

### Pitfall 4: _expr.py Column Tests
**What goes wrong:** test_expr.py has 51 tests. Some test Column operator overloading (`col + 1`, `col > 5`) which is a DataFrame-only concept. Others test expression *string evaluation* which the server still uses.
**How to avoid:** Audit test_expr.py: keep tests for expression string evaluation (used by `tl.derive()` expressions). Remove tests for Column/Expr/EventProxy that only exist in the DataFrame API. The expression *parser* on the Rust side still works the same way.
**Warning signs:** Importing deleted modules in test_expr.py.

### Pitfall 5: conftest.py Imports
**What goes wrong:** conftest.py uses `import tally as st` and creates App instances. After old API removal, `st.stream` won't exist.
**Why it happens:** conftest.py was written before the new API.
**How to avoid:** Migrate conftest.py FIRST -- it's imported by all integration tests. Change to `import tally as tl`.
**Warning signs:** All integration tests fail simultaneously.

### Pitfall 6: test_protocol.py Import Error
**What goes wrong:** test_protocol.py currently fails to import `encode_push` from `_protocol.py` (it was renamed to `encode_push_binary`). This means ~46 tests are NOT being collected.
**Why it happens:** Protocol encoding was refactored in Phase 11 (binary push) but test wasn't updated.
**How to avoid:** Fix the import in test_protocol.py as part of this phase. The function name changed, but the tests may need the import name updated.
**Warning signs:** Test count doesn't reach 744 even after migration because protocol tests are broken.

## Test Count Audit

| Source | Count Method | Count |
|--------|-------------|-------|
| Python tests (collectable) | `pytest --co -q` | 331 |
| Python tests (broken import) | `grep -c "def test_" test_protocol.py` | 46 |
| **Python total** | | **377** |
| Rust integration tests | `grep -c "#\[test\]\|#\[tokio::test\]" tests/*.rs` | 99 |
| Rust inline tests | `grep -c "#\[test\]\|#\[tokio::test\]" src/**/*.rs` | 629 |
| **Rust total** | | **728** |
| **Grand total** | | **~1105** |

[VERIFIED: pytest --co and grep counts from codebase]

Note: The >= 744 target in success criteria refers to the total across `cargo test` + `pytest`. With ~728 Rust tests (unchanged) + 377 Python tests = ~1105, the >= 744 target should be easily met as long as Python test count doesn't drop significantly.

The critical constraint is: **Python test count must not drop below 377** (current), and ideally should increase (fixing test_protocol.py adds ~46 tests).

## Benchmark Migration

The benchmark file `benchmark/tally-throughput/bench.py` defines 3 pipeline shapes using old API and must be rewritten:

### Current bench.py structure:
- `define_small()`: 1 keyed `@st.stream`, 5 features
- `define_medium()`: 2 keyed `@st.stream` + 1 `@st.view`, fan-out
- `define_large()`: 3 keyed `@st.stream` + 2 `@st.view`, cascade + fan-out + HLL

### Migration approach:
Each `define_*()` function creates streams inline with `@st.stream`. Replace with `@source` + `@dataset`:

```python
def define_small():
    @source
    class RawTxns:
        pass

    @dataset(depends_on=[RawTxns])
    class Transactions:
        features = group_by('user_id').agg(
            tx_count_1h=tl.count(window='1h'),
            tx_sum_1h=tl.sum('amount', window='1h'),
            avg_amount_1h=tl.avg('amount', window='1h'),
            max_amount_24h=tl.max('amount', window='24h'),
            min_amount_24h=tl.min('amount', window='24h'),
        )
    return [RawTxns, Transactions], RawTxns  # push to source, not dataset
```

**Critical question:** Does `app.push()` push to a source or a keyed stream? Looking at the current bench.py: `app.push(primary, event)` where `primary = Transactions` (keyed stream). The server accepts PUSH to any registered stream by name.

In the new API: events enter through sources (keyless), cascade to datasets. So `app.push(RawTxns, event)` pushes to the source, and the server cascades to Transactions. The primary for push should be the **source**, not the dataset.

BUT: current bench.py pushes to the keyed stream directly. The server accepts this. In the new API with `depends_on`, the source exists as a separate registration. The push target changes from "Transactions" to "RawTxns".

**This could affect benchmark numbers if the cascade adds overhead.** However, the current medium/large pipelines already use cascade (depends_on), so the overhead is already accounted for.

For `define_small()` -- currently has NO source/depends_on. Adding a source + depends_on adds cascade overhead that wasn't there before. To keep benchmarks apples-to-apples, define_small could push directly to the dataset:
```python
return [Transactions], Transactions  # push to dataset directly (no source for simple case)
```

Wait -- but a `@dataset(depends_on=[...])` with no upstream source won't receive events directly? Let me check. Looking at `_dataset.py`: DatasetDef compiles to a keyed stream with `depends_on`. The server needs a source stream to push to. Without a source, the dataset IS the push target (key_field is set, no depends_on needed).

Actually, `@dataset` requires `depends_on` parameter. So for small pipeline, we need EITHER a source OR we use the old pattern where the dataset IS the stream. Checking the decorator signature: `def dataset(*, depends_on: list, ...)` -- depends_on is required.

So every dataset needs at least one upstream. For small pipeline: add a RawTxns source. [VERIFIED: _dataset.py:246]

### Benchmark matrix command:
```bash
python3 benchmark/tally-throughput/bench.py --matrix --clients 1 --events 60000
python3 benchmark/tally-throughput/bench.py --matrix --clients 4 --events 60000
python3 benchmark/tally-throughput/bench.py --matrix --clients 8 --events 60000
```

The success criteria requires: "Full benchmark matrix (small/medium/large x sync/async/batch x 1c/4c/8c) passes within -5% of 1.1M eps baseline". Note: current matrix only runs sync/async (not batch). The batch mode is `--mode async-batch` run separately.

## Code Examples

### Example: Migrating test_integration.py Transactions Stream

Before:
```python
import tally as st

@st.stream(key="user_id")
class Transactions:
    tx_count_1h = st.count(window="1h")
    tx_sum_1h = st.sum("amount", window="1h")
    avg_amount_1h = st.avg("amount", window="1h")
    rate = st.derive("tx_count_1h / tx_sum_1h")
```

After:
```python
import tally as tl
from tally import source, dataset, group_by

@source
class RawTransactions:
    pass

@dataset(depends_on=[RawTransactions])
class Transactions:
    features = group_by("user_id").agg(
        tx_count_1h=tl.count(window="1h"),
        tx_sum_1h=tl.sum("amount", window="1h"),
        avg_amount_1h=tl.avg("amount", window="1h"),
    )
    rate = tl.derive("tx_count_1h / tx_sum_1h")
```

Push call changes:
```python
# Before
features = app.push(Transactions, {"user_id": "u1", "amount": 50.0})

# After -- push to source
features = app.push(RawTransactions, {"user_id": "u1", "amount": 50.0})
```

[VERIFIED: _app.py push/register methods support both old and new types]

### Example: Fixing test_protocol.py

```python
# Before (broken):
from tally._protocol import encode_push

# After:
from tally._protocol import encode_push_binary as encode_push
# OR update all test references to use encode_push_binary
```

[VERIFIED: _protocol.py only has encode_push_binary, not encode_push]

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| `@st.stream(key=...)` | `@tl.source` + `@tl.dataset(depends_on=[...])` | Phase 16 (v2.0) | Explicit source/dataset separation |
| `@st.view(key=...)` | `@tl.dataset(depends_on=[...])` with derive-only | Phase 16 (v2.0) | Views are just datasets |
| `_dataframe.py` (Stream, Table, GroupBy, JoinedTable) | `@tl.dataset` + `group_by().agg()` | Phase 16 (v2.0) | Decorator-based, not fluent builder |
| `app.source()` + `app.serve()` + `app.register_all()` | `@source` + `app.register()` | Phase 16 (v2.0) | Registration through register() only |

## Assumptions Log

| # | Claim | Section | Risk if Wrong |
|---|-------|---------|---------------|
| A1 | View-equivalent datasets (derive-only) produce compatible RegisterRequest JSON that the server accepts | Architecture Patterns / Pattern 4 | HIGH -- server may require `"type": "view"` field. Mitigated by existing TestJsonCompat in test_new_api.py |
| A2 | Pushing to a source (keyless) correctly cascades to downstream datasets in the benchmark | Benchmark Migration | MEDIUM -- if cascade overhead is significant, benchmark may regress. Mitigated by medium/large already using cascade |
| A3 | test_protocol.py broken import is pre-existing and fix should be included in this phase | Common Pitfalls | LOW -- low risk, easy fix, increases test count |

## Open Questions

1. **View type field in RegisterRequest**
   - What we know: Old API emits `"type": "view"` for views. New DatasetDef._compile() does NOT emit a type field.
   - What's unclear: Does the server require `"type": "view"` to treat a registration as a view (derive-only, no state)?
   - Recommendation: Check test_new_api.py TestJsonCompat tests. If they already verify this, it's handled. If not, add a test.

2. **Push target in benchmarks**
   - What we know: Current bench.py pushes to keyed streams directly. New API separates source from dataset.
   - What's unclear: For simple pipelines (small), does adding a source + depends_on add measurable cascade overhead?
   - Recommendation: Benchmark before and after. If small pipeline regresses, push directly to dataset (if server allows pushing to a keyed stream that has depends_on).

3. **_expr.py test coverage after removal**
   - What we know: test_expr.py has 51 tests, many testing Column and Expr which are DataFrame-only constructs.
   - What's unclear: How many tests are worth keeping (expression string evaluation) vs. removing (DataFrame Column API)?
   - Recommendation: Audit test_expr.py line by line. Expression string functions used by `tl.derive()` remain useful; Column/Expr operator overloading tests get removed.

## Validation Architecture

### Test Framework
| Property | Value |
|----------|-------|
| Framework | pytest (Python) + cargo test (Rust) |
| Config file | None (defaults) |
| Quick run command | `PYTHONPATH=python python3 -m pytest python/tests -x -q` |
| Full suite command | `PYTHONPATH=python python3 -m pytest python/tests -q && cargo test` |

### Phase Requirements -> Test Map
| Req ID | Behavior | Test Type | Automated Command | File Exists? |
|--------|----------|-----------|-------------------|-------------|
| MIG-01 | All tests >= 744 pass on new API | smoke | `PYTHONPATH=python python3 -m pytest python/tests --co -q 2>&1 \| tail -1` | Existing tests, rewritten |
| MIG-01 | No old API references in test code | unit | `grep -r "st\.stream\|st\.view\|_dataframe" python/tests/` | Verification script |
| MIG-02 | Old API symbols not importable | unit | `python3 -c "import tally; tally.stream"` should fail | Wave 3 verification |
| MIG-02 | Old API files deleted | smoke | `ls python/tally/_stream.py _view.py _dataframe.py _expr.py` should fail | Deletion verification |
| MIG-03 | Benchmark within -5% of 1.1M eps | perf | `python3 benchmark/tally-throughput/bench.py --matrix` | bench.py exists, needs migration |

### Sampling Rate
- **Per task commit:** `PYTHONPATH=python python3 -m pytest python/tests -x -q`
- **Per wave merge:** `PYTHONPATH=python python3 -m pytest python/tests -q` + test count check
- **Phase gate:** Full suite green + benchmark matrix pass

### Wave 0 Gaps
- [ ] Fix `test_protocol.py` import error (`encode_push` -> `encode_push_binary`) to unlock ~46 tests
- [ ] No new test infrastructure needed -- rewriting within existing framework

## Security Domain

Not applicable -- this phase is pure test/code migration with no new attack surface, no new inputs, no new network endpoints.

## Sources

### Primary (HIGH confidence)
- Codebase inspection: `python/tally/__init__.py`, `_stream.py`, `_view.py`, `_dataframe.py`, `_source.py`, `_dataset.py`, `_schema.py`, `_app.py`, `_expr.py`
- Codebase inspection: All files in `python/tests/`
- Codebase inspection: `benchmark/tally-throughput/bench.py`
- pytest collection: `python3 -m pytest python/tests --co -q`
- grep audit: All `@st.stream`, `@st.view`, `_dataframe` references

### Secondary (MEDIUM confidence)
- Test count estimates from `grep -c "def test_"` (may miss parametrized tests)
- Rust test count from `grep -c "#[test]"` (accurate for non-parametrized)

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH -- no new libraries, pure migration
- Architecture: HIGH -- both old and new API fully inspected in codebase
- Pitfalls: HIGH -- identified from actual code analysis, not speculation
- Benchmark: MEDIUM -- push target change (source vs keyed stream) needs runtime verification

**Research date:** 2026-04-12
**Valid until:** 2026-04-26 (stable -- internal codebase, no external dependencies)
