# Phase 16: Python SDK -- New Types and Decorators - Research

**Researched:** 2026-04-12
**Domain:** Python SDK API design, type system (PEP 681), decorator patterns, JSON compilation
**Confidence:** HIGH

## Summary

Phase 16 is a pure Python SDK phase -- no Rust changes required. The goal is to introduce a new `@tl.source` / `@tl.dataset(depends_on=[...])` API with `EventSet`/`FeatureSet` typed schemas and explicit `.group_by("key").agg(...)` aggregation, all compiling to the same RegisterRequest JSON format the existing server already accepts.

The codebase already has two API surfaces that do exactly this: (1) the `@st.stream`/`@st.view` decorator API in `_stream.py`/`_view.py`, and (2) the DataFrame API in `_dataframe.py`. Both compile to identical JSON via `_to_register_json()` and reuse the same operator classes from `_operators.py`. The new API is a third surface that must produce the same JSON output while providing better ergonomics (explicit deps, typed schemas, IDE autocomplete).

**Primary recommendation:** Build three new files (`_source.py`, `_dataset.py`, `_schema.py`) that reuse existing `_operators.py` classes and the `_to_register_json()` protocol, add `tl.union()` as a free function, add `pipeline.validate()` for local DAG validation, and export new symbols from `__init__.py` alongside the old API.

<user_constraints>
## User Constraints (from CONTEXT.md)

### Locked Decisions
- New types live in new files: `_source.py`, `_dataset.py`, `_schema.py` -- clean separation from old API
- Public import path: `import tally as tl` (same package, `tl` alias by convention) -- no new package needed
- `@tl.dataset` supersedes `DataStream`/`Table`/`GroupBy` from `_dataframe.py` -- old DataFrame classes deleted in Phase 19
- Naming: `@tl.source` (shorter, matches REQUIREMENTS.md API-01 wording)
- `EventSet`/`FeatureSet` use `dataclass_transform` decorator on a base class -- users write plain class attributes with `Field()` descriptors, IDE autocomplete works via PEP 681
- `.group_by("key").agg(...)` returns a `GroupedDataset` intermediate that has only `.agg()` -- mirrors existing `GroupBy` pattern in `_dataframe.py`
- `tl.union()` is a free function returning a `UnionSource` that compiles to multi-parent `depends_on`
- `pipeline.validate()` returns a list of `ValidationError` objects with `.path`, `.message`, `.kind` (cycle/missing_dep/type_mismatch) -- empty list = valid
- Old and new APIs coexist during Phase 16 -- new API is additive, old API untouched until Phase 19 deletion
- Each dataset has a `._compile()` method returning the same JSON dict that `@st.stream` produces -- tested by asserting JSON equality between old and new definitions
- Reuse existing `_operators.py` classes (`tl.count(window="1h")` is same `Count(window="1h")`) -- zero duplication
- Dedicated test file `test_new_api.py` testing: compile-to-JSON correctness, validate() error cases, EventSet/FeatureSet typing, union, group_by.agg -- all against existing server

### Claude's Discretion
None -- all questions answered explicitly.

### Deferred Ideas (OUT OF SCOPE)
None -- discussion stayed within phase scope.
</user_constraints>

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|------------------|
| API-01 | User can define an event source with `@tl.source` decorator that compiles to a keyless stream RegisterRequest | `_source.py`: decorator wraps class, collects metadata, `_compile()` produces `{"name": ..., "key_field": null, "features": []}` |
| API-02 | User can define a derived dataset with `@tl.dataset(depends_on=[...])` decorator that declares upstream dependencies and compiles to a keyed stream RegisterRequest | `_dataset.py`: decorator with `depends_on` param, `_compile()` produces `{"name": ..., "key_field": ..., "depends_on": [...], "features": [...]}` |
| API-03 | User can declare typed input schemas with `EventSet` and output schemas with `FeatureSet` using `Field` descriptors with IDE autocomplete via `dataclass_transform` | `_schema.py`: `@dataclass_transform(field_specifiers=(Field,))` on base classes, PEP 681 for IDE support |
| API-04 | User can explicitly aggregate events with `.group_by("key").agg(count=tl.count(window="1h"), ...)` | `GroupedDataset` class in `_dataset.py` with `.agg()` method returning the dataset |
| API-05 | User can merge multiple event sources into one dataset with `tl.union(source_a, source_b)` | `union()` free function returning `UnionSource` that compiles with `depends_on: [a, b]` |
| API-06 | User can call `pipeline.validate()` locally to check DAG validity (cycles, missing deps, type mismatches) | Validation module with topological sort, dep resolution, schema type checking |
| API-07 | Pipeline definitions are portable -- same JSON format works for startup registration, runtime REGISTER, and future ephemeral pipelines | `_compile()` produces identical JSON to existing `_to_register_json()` -- verified by assertion tests |
</phase_requirements>

## Standard Stack

### Core
| Library | Version | Purpose | Why Standard |
|---------|---------|---------|--------------|
| typing (stdlib) | Python 3.11+ | `dataclass_transform`, `TYPE_CHECKING`, type annotations | PEP 681 available in Python 3.11 typing module [VERIFIED: Python 3.11.2 on system] |
| dataclasses (stdlib) | Python 3.11+ | Reference pattern for Field descriptors | Standard library, no dependency |
| pytest | latest | Test framework | Already used by project (pyproject.toml `testpaths = ["tests"]`) [VERIFIED: pyproject.toml] |

### Supporting
| Library | Version | Purpose | When to Use |
|---------|---------|---------|-------------|
| _operators.py (internal) | existing | All 16 operator classes with `to_json()` | Reuse directly -- zero duplication [VERIFIED: codebase] |
| _expr.py (internal) | existing | Expression proxies (Column, Expr, EventProxy) | Reuse for derive expression building [VERIFIED: codebase] |
| _protocol.py (internal) | existing | `encode_register()` encodes dict to JSON bytes | Reuse for wire encoding [VERIFIED: codebase] |

### Alternatives Considered
| Instead of | Could Use | Tradeoff |
|------------|-----------|----------|
| `dataclass_transform` on base class | Metaclass approach (like existing `StreamMeta`) | Base class is simpler, PEP 681 works on both; base class preferred per CONTEXT.md decision |
| Custom Field class | `dataclasses.field()` directly | Custom `Field` allows domain-specific params (dtype, description) while PEP 681 handles IDE integration |

**Installation:**
```bash
# No new dependencies -- all stdlib + existing internal modules
pip install -e python/  # existing dev install
```

## Architecture Patterns

### Recommended Project Structure
```
python/tally/
├── __init__.py          # Add new exports alongside old
├── _source.py           # @tl.source decorator + Source class (NEW)
├── _dataset.py          # @tl.dataset decorator + Dataset class + GroupedDataset + union() (NEW)
├── _schema.py           # EventSet, FeatureSet, Field (NEW)
├── _validate.py         # pipeline.validate() logic (NEW)
├── _operators.py        # UNCHANGED -- reused directly
├── _expr.py             # UNCHANGED -- reused directly
├── _stream.py           # UNCHANGED (old API, removed Phase 19)
├── _view.py             # UNCHANGED (old API, removed Phase 19)
├── _dataframe.py        # UNCHANGED (old API, removed Phase 19)
├── _app.py              # Minor addition: support new API objects in register()
├── _protocol.py         # UNCHANGED
├── _client.py           # UNCHANGED
└── _types.py            # UNCHANGED
python/tests/
└── test_new_api.py      # All new API tests (NEW)
```

### Pattern 1: `@tl.source` Decorator
**What:** Decorator that creates a Source object from a class definition, collecting EventSet schema info.
**When to use:** Defining a raw event ingestion point (keyless stream).
**Example:**
```python
# Source: CONTEXT.md decisions + existing _stream.py pattern
import tally as tl
from tally._schema import EventSet, Field

class TxnEvent(EventSet):
    user_id: str = Field()
    amount: float = Field()
    merchant_id: str = Field()

@tl.source
class Transactions:
    event = TxnEvent
    # No operators -- keyless source

# Internal: Transactions._compile() produces:
# {"name": "Transactions", "key_field": null, "features": []}
```

### Pattern 2: `@tl.dataset` with `.group_by().agg()`
**What:** Decorator that creates a derived dataset with explicit upstream deps and aggregation.
**When to use:** Defining keyed feature computation.
**Example:**
```python
# Source: CONTEXT.md decisions + existing _dataframe.py GroupBy pattern
@tl.dataset(depends_on=[Transactions])
class UserFeatures:
    event = TxnEvent
    
    features = (
        tl.group_by("user_id")
        .agg(
            tx_count_1h=tl.count(window="1h"),
            tx_sum_1h=tl.sum("amount", window="1h"),
            avg_amount_1h=tl.avg("amount", window="1h"),
        )
    )

# Internal: UserFeatures._compile() produces:
# {
#   "name": "UserFeatures",
#   "key_field": "user_id",
#   "depends_on": ["Transactions"],
#   "features": [
#     {"name": "tx_count_1h", "type": "count", "window": "1h"},
#     {"name": "tx_sum_1h", "type": "sum", "field": "amount", "window": "1h"},
#     {"name": "avg_amount_1h", "type": "avg", "field": "amount", "window": "1h"}
#   ]
# }
```

### Pattern 3: `dataclass_transform` for IDE Autocomplete
**What:** PEP 681 decorator on base class tells type checkers to synthesize `__init__` and provide autocomplete.
**When to use:** `EventSet` and `FeatureSet` base classes.
**Example:**
```python
# Source: PEP 681 spec (https://peps.python.org/pep-0681/)
from typing import dataclass_transform

class Field:
    """Field descriptor for EventSet/FeatureSet schemas."""
    def __init__(
        self,
        *,
        dtype: type | None = None,
        description: str = "",
        default: object = ...,  # sentinel for "required"
    ) -> None:
        self.dtype = dtype
        self.description = description
        self.default = default

@dataclass_transform(field_specifiers=(Field,))
class EventSet:
    """Base class for typed event schemas. IDE autocomplete via PEP 681."""
    def __init_subclass__(cls, **kwargs: object) -> None:
        super().__init_subclass__(**kwargs)
        # Collect Field descriptors from annotations
        cls._fields: dict[str, Field] = {}
        for name, annotation in getattr(cls, '__annotations__', {}).items():
            val = getattr(cls, name, Field())
            if isinstance(val, Field):
                cls._fields[name] = val

@dataclass_transform(field_specifiers=(Field,))
class FeatureSet:
    """Base class for typed output feature schemas. IDE autocomplete via PEP 681."""
    def __init_subclass__(cls, **kwargs: object) -> None:
        super().__init_subclass__(**kwargs)
        cls._fields: dict[str, Field] = {}
        for name, annotation in getattr(cls, '__annotations__', {}).items():
            val = getattr(cls, name, Field())
            if isinstance(val, Field):
                cls._fields[name] = val
```

### Pattern 4: `tl.union()` for Multi-Source Merge
**What:** Free function that merges multiple sources into a single upstream.
**When to use:** When a dataset depends on events from multiple source streams.
**Example:**
```python
# Source: CONTEXT.md decision
@tl.source
class CardTxns:
    event = TxnEvent

@tl.source  
class WireTxns:
    event = TxnEvent

all_txns = tl.union(CardTxns, WireTxns)

@tl.dataset(depends_on=[all_txns])
class UserFeatures:
    features = tl.group_by("user_id").agg(
        total_count_1h=tl.count(window="1h"),
    )

# all_txns._compile() -> multi-parent depends_on: ["CardTxns", "WireTxns"]
```

### Pattern 5: `pipeline.validate()`
**What:** Local validation of DAG structure without server contact.
**When to use:** Before calling `app.register()`.
**Example:**
```python
# Source: CONTEXT.md decision
errors = tl.validate(Transactions, UserFeatures)
for e in errors:
    print(f"{e.kind}: {e.path} - {e.message}")
# Returns empty list if valid
```

### Anti-Patterns to Avoid
- **Duplicating operator logic:** Never copy operator classes -- always import from `_operators.py`. The 16 operator classes with `to_json()` are the single source of truth.
- **Runtime behavior in `dataclass_transform`:** PEP 681 is a type-checker hint only. Do NOT expect it to generate `__init__` at runtime. The `__init_subclass__` hook handles runtime field collection.
- **Naming collisions between old and new API:** The new `Dataset` class in `_dataset.py` must NOT shadow the existing `Dataset` in `_dataframe.py`. Use different internal names; only export the new one under `tl.dataset` (as a decorator, not a class).

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| JSON compilation | New serialization format | Existing `OperatorBase.to_json(name)` | All 16 operators already have correct `to_json()` implementations |
| Expression building | New expression AST | Existing `_expr.py` (Column, Expr, BinOp) | Full expression tree with `to_expr_string()` already works |
| Wire protocol | New encoding | `encode_register(dict)` in `_protocol.py` | Single function, JSON bytes, already battle-tested |
| Cycle detection | Custom graph algorithm | Standard Kahn's algorithm (topological sort) | Well-known O(V+E) algorithm, simple to implement in ~20 lines |
| Type checking for schemas | Custom type introspection | `typing.get_type_hints()` + `__annotations__` | stdlib handles forward refs, string annotations correctly |

**Key insight:** The entire compilation pipeline from Python DSL to server RegisterRequest already exists in two implementations. The new API is a third frontend to the same backend.

## Common Pitfalls

### Pitfall 1: `from __future__ import annotations` Breaks Runtime Annotation Access
**What goes wrong:** With PEP 563 (deferred annotations), `cls.__annotations__` returns strings instead of types. `typing.get_type_hints()` is needed to resolve them.
**Why it happens:** Every existing file in the SDK uses `from __future__ import annotations`. New files will likely follow suit.
**How to avoid:** Use `typing.get_type_hints(cls)` instead of `cls.__annotations__` when you need resolved types for schema validation. For field collection (just names), `__annotations__` as strings is fine.
**Warning signs:** Type comparison like `annotation is float` fails silently (comparing string "float" to type).

### Pitfall 2: `dataclass_transform` Does Nothing at Runtime
**What goes wrong:** Developer expects `EventSet` subclass to auto-generate `__init__` like a dataclass.
**Why it happens:** PEP 681 is purely a type-checker directive. Runtime behavior must be implemented manually.
**How to avoid:** Explicitly implement `__init_subclass__` to collect fields, and if you want constructors, add a custom `__init__` via `__init_subclass__` or require users to use the class only as a schema declaration (no instantiation needed).
**Warning signs:** `MyEvent()` raises TypeError at runtime despite IDE showing constructor params.

### Pitfall 3: Name Collision Between Old and New `Dataset`
**What goes wrong:** `_dataframe.py` already exports `Dataset` as a base class. `_dataset.py` will define a new `Dataset`-like concept.
**Why it happens:** Both APIs coexist in Phase 16.
**How to avoid:** The `@tl.dataset` decorator returns a wrapped class (not a `Dataset` base class instance). Internally, use a different class name like `DatasetDef` or `_DatasetMeta`. Only expose `dataset` as a decorator function, never as an importable class name that conflicts.
**Warning signs:** `from tally import Dataset` becomes ambiguous.

### Pitfall 4: `depends_on` Takes Classes, JSON Needs Strings
**What goes wrong:** User passes class references in `depends_on=[Transactions]`, but `_compile()` must emit `"depends_on": ["Transactions"]` as strings.
**Why it happens:** The decorator API takes classes for IDE support, but wire format needs strings.
**How to avoid:** In `_compile()`, resolve class references to `._name` strings. The existing `_stream.py` already does this pattern: `dep._tally_stream_name if hasattr(dep, '_tally_stream_name') else str(dep)`. Follow the same approach.
**Warning signs:** JSON contains `<class 'Transactions'>` instead of `"Transactions"`.

### Pitfall 5: Union Source Must Be Passable as `depends_on` Element
**What goes wrong:** `tl.union(a, b)` returns a `UnionSource` but `@tl.dataset(depends_on=[union_result])` needs to handle it differently from a single source.
**Why it happens:** `UnionSource` is not a source class -- it's a composite.
**How to avoid:** `UnionSource` should have a `._compile()` that emits its own keyless stream registration (or just be transparent), AND the dataset compiler must flatten union sources into a multi-element `depends_on` list. Design decision: either `UnionSource` compiles to its own intermediate stream, or it's transparent and just contributes names to `depends_on`. The transparent approach is simpler and matches CONTEXT.md wording.
**Warning signs:** Server rejects registration because union source name doesn't exist as a registered stream.

### Pitfall 6: Validate Must Work Without Server
**What goes wrong:** Validation logic accidentally depends on TCP connection or server state.
**Why it happens:** Existing `App.register()` validates by sending to server and checking error response.
**How to avoid:** `pipeline.validate()` must be a pure function operating only on the Python-side DAG. It takes a list of source/dataset definitions and validates: (1) topological sort succeeds (no cycles), (2) all `depends_on` references resolve to known definitions in the list, (3) schema type compatibility between EventSet fields and operator field references.
**Warning signs:** `validate()` raises `ConnectionError`.

## Code Examples

### RegisterRequest JSON Format (the compilation target)
```python
# Source: Verified from _stream.py StreamMeta._to_register_json() and _dataframe.py Table._to_register_json()
# Keyless source:
{"name": "RawEvents", "key_field": None, "features": []}

# Keyed stream with features:
{
    "name": "UserFeatures",
    "key_field": "user_id",
    "features": [
        {"name": "tx_count_1h", "type": "count", "window": "1h"},
        {"name": "tx_sum_1h", "type": "sum", "field": "amount", "window": "1h"},
    ],
    "depends_on": ["RawEvents"],
}

# With optional fields:
{
    "name": "UserFeatures",
    "key_field": "user_id",
    "features": [...],
    "depends_on": ["RawEvents"],
    "entity_ttl": "5m",        # optional
    "history_ttl": "72h",      # optional
    "filter": "status == 'ok'", # optional
}
```

### How Existing `_to_register_json()` Works (reference for `_compile()`)
```python
# Source: _stream.py line 97-133 [VERIFIED: codebase]
def _to_register_json(cls) -> dict:
    d = {
        "name": cls._tally_stream_name,
        "key_field": cls._tally_key_field,
        "features": [
            op.to_json(feat_name)
            for feat_name, op in cls._tally_features.items()
        ],
    }
    if cls._tally_is_view:
        d["type"] = "view"
    if cls._tally_entity_ttl is not None:
        d["entity_ttl"] = cls._tally_entity_ttl
    if cls._tally_depends_on is not None:
        d["depends_on"] = [
            dep._tally_stream_name if hasattr(dep, '_tally_stream_name') else str(dep)
            for dep in cls._tally_depends_on
        ]
    return d
```

### Kahn's Algorithm for Cycle Detection (validate)
```python
# Source: standard algorithm [ASSUMED]
from collections import deque

def topological_sort(nodes: dict[str, list[str]]) -> list[str] | None:
    """Return topological order, or None if cycle exists.
    
    nodes: {name: [dependency_names]}
    """
    in_degree = {n: 0 for n in nodes}
    for deps in nodes.values():
        for d in deps:
            if d in in_degree:
                in_degree[d] = in_degree.get(d, 0)  # already 0
    # Build reverse adjacency
    dependents: dict[str, list[str]] = {n: [] for n in nodes}
    for n, deps in nodes.items():
        for d in deps:
            if d in dependents:
                dependents[d].append(n)
            in_degree[n] += 1  # fix: count actual deps
    
    # ... standard BFS queue drain
    queue = deque(n for n, deg in in_degree.items() if deg == 0)
    order = []
    while queue:
        n = queue.popleft()
        order.append(n)
        for dep in dependents[n]:
            in_degree[dep] -= 1
            if in_degree[dep] == 0:
                queue.append(dep)
    return order if len(order) == len(nodes) else None
```

### App.register() Integration Point
```python
# Source: _app.py line 154-172 [VERIFIED: codebase]
# Current code already supports anything with _collect_registrations() or _to_register_json()
def register(self, *stream_classes) -> None:
    for cls in stream_classes:
        if hasattr(cls, '_collect_registrations'):
            for reg in cls._collect_registrations():
                payload = encode_register(reg)
                self._send(OP_REGISTER, payload)
        else:
            definition = cls._to_register_json()
            payload = encode_register(definition)
            self._send(OP_REGISTER, payload)

# New API objects need either:
# (a) _to_register_json() returning a dict  -- for single registrations
# (b) _collect_registrations() returning list[dict] -- for transitive deps
# Using _compile() internally, then aliasing to _to_register_json() for App compat
```

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| `@st.stream(key="user_id")` class decorator | `@tl.dataset(depends_on=[...])` function-based | Phase 16 (now) | Explicit dependencies, typed schemas |
| Implicit keying via `key=` param | Explicit `.group_by("key").agg(...)` | Phase 16 (now) | No hidden magic, clear aggregation boundary |
| No input schemas | `EventSet` / `FeatureSet` with `Field()` | Phase 16 (now) | IDE autocomplete, documentation, future server-side validation |
| `app.source("name")` DataFrame API | `@tl.source` decorator | Phase 16 (now) | Cleaner syntax, schema support |

## Assumptions Log

| # | Claim | Section | Risk if Wrong |
|---|-------|---------|---------------|
| A1 | Kahn's algorithm is appropriate for cycle detection in validate() | Code Examples | LOW -- standard well-known algorithm, alternative DFS-based also trivial |
| A2 | `EventSet`/`FeatureSet` do not need runtime `__init__` (schema-only) | Architecture Patterns | MEDIUM -- if users expect to instantiate EventSet for testing, need to add `__init__` in `__init_subclass__` |
| A3 | `UnionSource` is transparent (contributes names to `depends_on`, not its own stream) | Pitfalls | MEDIUM -- if server requires a registered stream for every depends_on entry, union needs its own keyless stream registration |

## Open Questions

1. **Does `UnionSource` need its own stream registration?**
   - What we know: CONTEXT.md says "compiles to multi-parent `depends_on`". The server accepts `depends_on` with multiple stream names.
   - What's unclear: Whether `UnionSource` should register as its own keyless stream (creating an intermediate node) or just inject its children's names into the parent's `depends_on`.
   - Recommendation: Start with transparent approach (inject children's names). If server needs intermediate stream, add it. Test against actual server.

2. **Should `EventSet`/`FeatureSet` support instantiation?**
   - What we know: Primary use is schema declaration (class attributes with `Field()`). IDE autocomplete is the key goal.
   - What's unclear: Whether users will want to instantiate `TxnEvent(user_id="u1", amount=50.0)` for testing.
   - Recommendation: Support instantiation by generating `__init__` in `__init_subclass__`. Low cost, high convenience for tests.

## Validation Architecture

### Test Framework
| Property | Value |
|----------|-------|
| Framework | pytest (version TBD -- needs install) |
| Config file | `python/pyproject.toml` `[tool.pytest.ini_options]` |
| Quick run command | `cd python && python -m pytest tests/test_new_api.py -x` |
| Full suite command | `cd python && python -m pytest tests/ -x` |

### Phase Requirements to Test Map
| Req ID | Behavior | Test Type | Automated Command | File Exists? |
|--------|----------|-----------|-------------------|-------------|
| API-01 | `@tl.source` compiles to keyless stream JSON | unit | `python -m pytest tests/test_new_api.py::TestSource -x` | Wave 0 |
| API-02 | `@tl.dataset(depends_on=[...])` compiles to keyed stream JSON | unit | `python -m pytest tests/test_new_api.py::TestDataset -x` | Wave 0 |
| API-03 | `EventSet`/`FeatureSet` with `Field` and IDE autocomplete | unit | `python -m pytest tests/test_new_api.py::TestSchema -x` | Wave 0 |
| API-04 | `.group_by("key").agg(...)` explicit aggregation | unit | `python -m pytest tests/test_new_api.py::TestGroupByAgg -x` | Wave 0 |
| API-05 | `tl.union()` multi-parent `depends_on` | unit | `python -m pytest tests/test_new_api.py::TestUnion -x` | Wave 0 |
| API-06 | `pipeline.validate()` catches cycles, missing deps, type mismatches | unit | `python -m pytest tests/test_new_api.py::TestValidate -x` | Wave 0 |
| API-07 | JSON portability -- same format as old API | unit | `python -m pytest tests/test_new_api.py::TestJsonCompat -x` | Wave 0 |
| E2E | Full pipeline register + push + get against server | integration | `python -m pytest tests/test_new_api.py::TestIntegration -x` | Wave 0 |

### Sampling Rate
- **Per task commit:** `cd /data/home/tally/python && python -m pytest tests/test_new_api.py -x`
- **Per wave merge:** `cd /data/home/tally/python && python -m pytest tests/ -x`
- **Phase gate:** Full suite green before `/gsd-verify-work`

### Wave 0 Gaps
- [ ] `python/tests/test_new_api.py` -- covers API-01 through API-07 + E2E
- [ ] pytest installation: `pip install pytest` -- not currently installed on system
- [ ] `python/tally/_source.py` -- new module
- [ ] `python/tally/_dataset.py` -- new module
- [ ] `python/tally/_schema.py` -- new module
- [ ] `python/tally/_validate.py` -- new module

## Environment Availability

| Dependency | Required By | Available | Version | Fallback |
|------------|------------|-----------|---------|----------|
| Python 3.11+ | `dataclass_transform` in stdlib `typing` | Yes | 3.11.2 | -- |
| pytest | Test execution | No (not installed) | -- | `pip install pytest` |
| Tally server binary | Integration tests | Yes (cargo build) | -- | -- |
| Rust/Cargo | Building server for integration tests | Yes | -- | -- |

**Missing dependencies with no fallback:**
- None (pytest is installable)

**Missing dependencies with fallback:**
- pytest: `pip install pytest` (trivial install, no system deps)

## Security Domain

Security enforcement not applicable for this phase. This is a client-side SDK API design phase with no authentication, cryptography, access control, or input validation beyond schema types. The compilation target (RegisterRequest JSON) is validated server-side by existing Rust code.

## Sources

### Primary (HIGH confidence)
- Codebase: `_operators.py` -- all 16 operator classes with `to_json()` [VERIFIED: direct read]
- Codebase: `_stream.py` -- `StreamMeta` metaclass, `_to_register_json()` protocol [VERIFIED: direct read]
- Codebase: `_dataframe.py` -- `Stream`, `Table`, `GroupBy`, `_collect_registrations()` DAG walker [VERIFIED: direct read]
- Codebase: `_app.py` -- `App.register()` integration point, `App.source()` [VERIFIED: direct read]
- Codebase: `_expr.py` -- Expression tree, Column proxy [VERIFIED: direct read]
- Codebase: `__init__.py` -- Current export surface [VERIFIED: direct read]
- [PEP 681](https://peps.python.org/pep-0681/) -- `dataclass_transform` spec with base class, metaclass, field_specifiers examples [CITED: peps.python.org/pep-0681/]

### Secondary (MEDIUM confidence)
- Codebase: `pyproject.toml` -- `requires-python = ">=3.10"`, pytest config [VERIFIED: direct read]
- System: Python 3.11.2 installed [VERIFIED: `python3 --version`]

### Tertiary (LOW confidence)
- None

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH -- stdlib only, zero new dependencies, all patterns verified in codebase
- Architecture: HIGH -- three new files following established codebase patterns, compilation target is known JSON format
- Pitfalls: HIGH -- identified from direct codebase analysis (annotation handling, name collisions, compilation protocol)

**Research date:** 2026-04-12
**Valid until:** 2026-05-12 (stable -- stdlib features, no moving targets)
