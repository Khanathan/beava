---
phase: 16-python-sdk-new-types-and-decorators
verified: 2026-04-12T23:00:00Z
status: passed
score: 5/5 must-haves verified
overrides_applied: 0
re_verification: false
---

# Phase 16: Python SDK New Types and Decorators Verification Report

**Phase Goal:** Users can define streaming pipelines using the new function-based API with explicit dependency declaration, typed schemas, and explicit aggregation -- all compiling to the existing RegisterRequest JSON format and testable on the current server without Rust changes
**Verified:** 2026-04-12T23:00:00Z
**Status:** passed
**Re-verification:** No -- initial verification

## Goal Achievement

### Observable Truths (ROADMAP Success Criteria)

| # | Truth | Status | Evidence |
|---|-------|--------|----------|
| SC-1 | User can define a source with `@tl.source` that compiles to a keyless stream RegisterRequest | VERIFIED | `_source.py` SourceDef._compile() returns `{"name":..., "key_field": None, "features": []}`. Spot check confirmed. TestSource 7 tests pass. |
| SC-2 | User can define a `@tl.dataset(depends_on=[...])` with `.group_by().agg()` compiling to a keyed stream RegisterRequest | VERIFIED | `_dataset.py` DatasetDef._compile() returns correct key_field, features list, depends_on. TestDataset + TestGroupByAgg pass. JSON compat test (TestJsonCompat) confirms output matches old API format. |
| SC-3 | User can declare typed schemas with `EventSet`/`FeatureSet` using `Field` descriptors with IDE autocomplete via `dataclass_transform` | VERIFIED | `_schema.py` has `@typing.dataclass_transform(field_specifiers=(Field,))` on both classes. Dynamic `__init__` via exec(). 8 TestSchema tests pass including dtype inference and required-arg validation. |
| SC-4 | User can merge multiple sources with `tl.union(source_a, source_b)` producing multi-parent `depends_on` | VERIFIED | `UnionSource._get_depends_on_names()` flattens to list of string names. Spot check: `tl.union(TxnRaw, LoginRaw)` produces `depends_on: ["TxnRaw", "LoginRaw"]`. TestUnion 3 tests pass. |
| SC-5 | User can call `pipeline.validate()` locally for cycle/missing-dep/type-mismatch errors without server contact | VERIFIED | `_validate.py` uses Kahn's algorithm. No socket/network imports. 8 TestValidate tests pass covering all three error kinds. |

**Score:** 5/5 truths verified

### Deferred Items

None.

### Required Artifacts

| Artifact | Expected | Status | Details |
|----------|----------|--------|---------|
| `python/tally/_schema.py` | EventSet, FeatureSet, Field with dataclass_transform | VERIFIED | 157 lines; contains `class Field:`, `class EventSet:`, `class FeatureSet:`, `@typing.dataclass_transform`, `def _collect_fields(cls)` |
| `python/tally/_source.py` | @tl.source decorator and SourceDef class | VERIFIED | 112 lines; contains `class SourceDef:`, `def source(`, `def _compile(self)`, `def _to_register_json(self)`, `def _collect_registrations(self)`, `_tally_stream_name` property |
| `python/tally/_dataset.py` | @tl.dataset, DatasetDef, GroupedDataset, UnionSource, group_by, union | VERIFIED | 269 lines; contains all 6 required symbols plus `def _compile(self)`, `def _to_register_json(self)`, `def _collect_registrations(self)`, `_tally_stream_name` property |
| `python/tally/_validate.py` | validate() function with DAG validation, no network | VERIFIED | 227 lines; contains `class ValidationError:`, `def validate(`, `def _topological_sort(`, no socket/TallyClient/network imports |
| `python/tally/__init__.py` | New API exports alongside old | VERIFIED | Imports EventSet/FeatureSet/Field, source, dataset/group_by/union, validate/ValidationError from new modules. `__all__` contains all 9 new symbols. Old `stream`, `view`, `App` still exported. |
| `python/tally/_app.py` | register() supports new API objects | VERIFIED | `_collect_registrations()` branch at line 167 handles SourceDef/DatasetDef. v2.0 compat comment added at line 161. |
| `python/tests/test_new_api.py` | Unit tests for all new API types | VERIFIED | 49 tests across 8 classes (TestSchema, TestSource, TestGroupByAgg, TestDataset, TestUnion, TestValidate, TestExports, TestJsonCompat, TestIntegration). All pass. |

### Key Link Verification

| From | To | Via | Status | Details |
|------|----|-----|--------|---------|
| `_source.py` | `_schema.py` | `from tally._schema import EventSet` | WIRED | Line 28 in _source.py |
| `_dataset.py` | `_operators.py` | `from tally._operators import OperatorBase` | WIRED | Line 24 in _dataset.py |
| `_dataset.py` | `_source.py` | lazy import in dataset() decorator body | WIRED | Line 242: `from tally._schema import EventSet` (dataset uses duck typing via hasattr for SourceDef compat -- no isinstance needed; all depends_on handled via `._name` attribute) |
| `_validate.py` | `_dataset.py` | `from tally._dataset import UnionSource` | WIRED | Line 78 (lazy import in _resolve_dep_names) |
| `_validate.py` | `_source.py` | duck typing via `._name` attribute (no isinstance) | WIRED | Plan required `from tally._source import` but implementation uses duck typing -- functionally equivalent. validate(SourceDef, DatasetDef) confirmed working. |
| `__init__.py` | `_source.py` | `from tally._source import source` | WIRED | Line 32 in __init__.py |
| `__init__.py` | `_schema.py` | `from tally._schema import EventSet, FeatureSet, Field` | WIRED | Line 31 in __init__.py |
| `__init__.py` | `_dataset.py` | `from tally._dataset import dataset, group_by, union` | WIRED | Line 33 in __init__.py |
| `__init__.py` | `_validate.py` | `from tally._validate import validate, ValidationError` | WIRED | Line 34 in __init__.py |

### Data-Flow Trace (Level 4)

This phase produces pure Python types and decorators (no UI components, no data rendering). All "data flow" is from Python objects to JSON dicts via `_compile()`. The JSON output was verified in behavioral spot-checks below.

### Behavioral Spot-Checks

| Behavior | Command | Result | Status |
|----------|---------|--------|--------|
| @tl.source compiles to keyless RegisterRequest JSON | `python3 -c "import tally as tl; ..."` | `key_field: None, features: []` | PASS |
| @tl.dataset with group_by().agg() compiles to keyed RegisterRequest | `python3 -c "..."` | `key_field: user_id, features: [count, sum], depends_on: [RawTxns]` | PASS |
| tl.union multi-parent depends_on | `python3 -c "..."` | `depends_on: ["TxnRaw", "LoginRaw"]` | PASS |
| validate() cycle detection | `python3 -c "..."` | `ValidationError(kind='cycle', ...)` returned | PASS |
| validate() missing dep detection | `python3 -c "..."` | `ValidationError(kind='missing_dep', ...)` returned | PASS |
| All 49 new API tests | `pytest tests/test_new_api.py` | `49 passed in 0.03s` | PASS |
| All new symbols importable | `python3 -c "import tally as tl; ..."` | 12/12 symbols OK | PASS |

### Requirements Coverage

| Requirement | Source Plan | Description | Status | Evidence |
|-------------|------------|-------------|--------|----------|
| API-01 | 16-01 | @tl.source compiles to keyless stream RegisterRequest | SATISFIED | SourceDef._compile() returns key_field=None. TestSource 7 tests pass. |
| API-02 | 16-01 | @tl.dataset(depends_on=[...]) compiles to keyed stream RegisterRequest | SATISFIED | DatasetDef._compile() with key_field, depends_on. TestDataset 6 tests pass. |
| API-03 | 16-01 | EventSet/FeatureSet with Field descriptors and dataclass_transform IDE autocomplete | SATISFIED | _schema.py with PEP 681 @dataclass_transform. TestSchema 8 tests pass. |
| API-04 | 16-01 | group_by("key").agg(...) explicit aggregation pattern | SATISFIED | GroupedDataset.agg() pattern in _dataset.py. TestGroupByAgg 3 tests pass. |
| API-05 | 16-01 | tl.union(source_a, source_b) merges multi-parent depends_on | SATISFIED | UnionSource flattens names. TestUnion 3 tests pass. |
| API-06 | 16-02 | pipeline.validate() with cycle/missing-dep/type-mismatch detection, no server | SATISFIED | _validate.py pure Python. No socket imports. TestValidate 8 tests pass. |
| API-07 | 16-02 | Portable JSON format -- same for startup registration, runtime REGISTER, future ephemeral | SATISFIED | TestJsonCompat confirms new API JSON matches old API format for equivalent pipelines. _collect_registrations() ordering test passes. |

All 7 requirements (API-01 through API-07) are SATISFIED. No orphaned requirements.

### Anti-Patterns Found

| File | Pattern | Severity | Assessment |
|------|---------|----------|------------|
| `_validate.py` | `return []` in `validate()` main function | Info | Not a stub -- the function builds and returns the errors list, returning `[]` means no errors found (correct behavior). |
| `_source.py` | `"features": []` in SourceDef._compile() | Info | Not a stub -- a keyless source intentionally has no aggregation features. This is the correct RegisterRequest format for SC-1. |
| `_dataset.py` | `DatasetDef._grouped_dataset` can be None | Info | Not a stub -- the dataset() decorator handles edge case where no `features = group_by(...).agg(...)` is provided (only extra derive features). |

No blockers or warnings. All patterns examined are intentional design choices, not implementation gaps.

### Human Verification Required

None. All success criteria are verifiable programmatically:
- JSON compilation verified via `python3` spot checks
- Test suite (49 tests) verified via pytest
- Export symbols verified via import checks
- Network isolation verified via grep for socket/HTTP imports
- Key wiring verified via grep for import statements

The one SC that has a "testable on the current server" component (SC-1) refers to the RegisterRequest JSON format being server-compatible. This is verified indirectly: TestJsonCompat confirms the new API produces the same JSON as the old `@st.stream` API (which is known to work with the server). No live server test is required for this phase's goal.

### Gaps Summary

No gaps. All 5 ROADMAP success criteria are verified. All 7 requirement IDs (API-01 through API-07) are satisfied. All 7 artifact files exist with substantive, wired implementations. All 49 tests pass. The full test suite excluding pre-existing failures (test_protocol.py import error from Phase 11, integration test failures from Phase 11 -- neither touched by phase 16) passes with 316/321 tests.

---

_Verified: 2026-04-12T23:00:00Z_
_Verifier: Claude (gsd-verifier)_
