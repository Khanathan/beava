# Stack Research -- v2.0 New API & Engine

**Domain:** Function-based pipeline API (Python SDK), engine enriched event propagation, feature projection, union node, on-demand compute architecture
**Researched:** 2026-04-12
**Confidence:** HIGH (Python stdlib features verified against docs; Rust changes are internal to existing crates)

**Scope boundary:** This doc covers ONLY the additions/changes needed for the v2.0 milestone. The existing Rust stack (tokio 1.50, serde, postcard, winnow, ahash, dashmap 6.1, parking_lot 0.12, petgraph 0.8, axum 0.8, ordered-float 4, rust-embed 8.11) and Python SDK (hatchling build, requires-python >=3.10) are already locked in and NOT re-evaluated.

---

## TL;DR Recommendations

| Need | Recommendation | Add dependency? |
|------|---------------|-----------------|
| EventSet/FeatureSet type system | Plain Python classes + `__init_subclass__` + `typing.dataclass_transform` (from `typing_extensions` for 3.10 compat) | **Add** `typing_extensions>=4.6` to pyproject.toml |
| Field descriptors for typed schemas | Custom `Field` descriptor class (like Fennel/Pydantic pattern) | No new dep -- hand-roll |
| `@tl.source` / `@tl.dataset` decorators | Pure Python class decorators compiling to existing `RegisterRequest` JSON | No new dep |
| Enriched event propagation (Rust) | ~50 LOC change in `push_with_cascade_internal` -- merge upstream features into event JSON before downstream push | No new dep |
| Feature projection (Rust) | New `projection` field on `StreamDefinition` -- list of field names to include/exclude from downstream propagation | No new dep |
| Union node (Rust) | New `FeatureDef::Union` variant + `depends_on` with multiple parents in existing petgraph DAG | No new dep |
| On-demand compute primitives | `ephemeral: bool` flag on `StreamDefinition` -- skip snapshot, auto-TTL, REGISTER stays runtime | No new dep |
| Expression improvements for v2 API | Existing winnow parser -- add `_upstream.StreamName.feature` prefix syntax | No new dep |

**Net new dependencies: 1** -- `typing_extensions>=4.6` (Python only). Zero new Rust crates.

---

## Python SDK Changes

### Core: typing_extensions

| Technology | Version | Purpose | Why Recommended |
|------------|---------|---------|-----------------|
| `typing_extensions` | >=4.6 | `dataclass_transform` decorator for `@tl.source`/`@tl.dataset` | PEP 681 landed in Python 3.11 but Tally supports >=3.10. `typing_extensions` backports it. This tells mypy/pyright that classes decorated with `@tl.dataset` behave like dataclasses (autocomplete, type checking on fields). Fennel, attrs, and Pydantic all use this pattern. Zero-cost at runtime (just sets `__dataclass_transform__` attribute). |

### EventSet / FeatureSet Type System

**Decision: Plain classes with custom `Field` descriptor, NOT Pydantic, NOT dataclasses.**

Why not Pydantic:
- Pydantic v2 is 5.4MB+ with a Rust core (`pydantic-core`). Adding it as a dependency for an SDK that prides itself on being thin and lightweight contradicts the "zero infrastructure" ethos.
- Runtime validation on every field access adds overhead we do not want on the definition path.
- The SDK does not validate data -- it builds pipeline definitions serialized to JSON. Pydantic's runtime validation is paying for something we do not need.

Why not stdlib `@dataclass`:
- `@dataclass` generates `__init__`, `__repr__`, `__eq__` which collide with our need for lazy proxy objects. EventSet/FeatureSet fields must be descriptor objects that capture operations (like the existing `Column` proxy), not concrete values.
- Cannot customize field collection behavior without fighting the dataclass machinery.

**Recommended pattern:**

```python
from typing_extensions import dataclass_transform

class Field:
    """Typed field descriptor for EventSet/FeatureSet schemas."""
    def __init__(self, dtype, *, key=False, timestamp=False):
        self.dtype = dtype
        self.key = key
        self.timestamp = timestamp

@dataclass_transform(field_specifiers=(Field,))
def source(*, name: str):
    """Decorator for event source definitions."""
    def decorator(cls):
        # Collect Field descriptors from class body
        # Compile to RegisterRequest JSON
        cls._tally_type = "source"
        cls._tally_name = name
        return cls
    return decorator

@dataclass_transform(field_specifiers=(Field,))
def dataset(*, depends_on: list):
    """Decorator for derived dataset definitions."""
    def decorator(cls):
        cls._tally_type = "dataset"
        return cls
    return decorator
```

This gives us:
1. Full mypy/pyright autocomplete on field names (via `dataclass_transform`)
2. Zero runtime validation overhead
3. Custom field collection via `__init_subclass__` or class-level `__set_name__`
4. Fields are descriptor proxies that support `.group_by()`, `.agg()`, operator chaining
5. Compiles to the same `RegisterRequest` JSON the server already understands

### Integration with Existing DataFrame API

The existing `_dataframe.py` module (Stream, Table, GroupBy, JoinedTable, Dataset) already implements the right compilation model. The v2.0 API wraps it in new surface syntax:

- `@tl.source` creates a `Stream` (keyless)
- `@tl.dataset` with `pipeline()` method creates derived nodes
- `.group_by("key").agg(...)` produces a `Table` (keyed)
- `EventSet` = renamed/typed `Stream`
- `FeatureSet` = renamed/typed `Table`

The existing `_expr.py` (Column, Expr, BinOp, FnCall), `_operators.py` (all 17 operator classes), and `_dataframe.py` compilation logic are reused wholesale. The v2.0 API is a new facade, not a rewrite.

---

## Rust Engine Changes (Zero New Crates)

### Enriched Event Propagation (~50 LOC)

**Current behavior:** `push_with_cascade_internal` passes the raw `event: &serde_json::Value` to every downstream stream unchanged. Downstream derives can reference `_event.field` but NOT upstream-computed features.

**Required change:** After pushing to each stream in topo order, merge the freshly computed features for that entity into a "enriched event" JSON object. Downstream streams receive `enriched_event` which contains both original event fields and upstream feature values.

```rust
// Pseudocode for the ~50 LOC change in push_with_cascade_internal:
let mut enriched = event.clone(); // Start with raw event
for stream_in_order in &self.topo_order {
    // ... existing cascade logic ...
    let features = self.push_internal(stream_in_order, &enriched, store, now, true);
    // Merge computed features into enriched event for downstream
    if let Ok(ref feat_map) = features {
        for (k, v) in feat_map {
            enriched[format!("{}.{}", stream_in_order, k)] = v.to_json();
        }
    }
}
```

**No new crate needed.** Uses existing `serde_json::Value` mutation. The clone is necessary but bounded -- enriched event grows by feature count, not by data volume.

**Integration with existing stack:** This works with DashMap concurrency because cascade execution is already sequential within a single push (topo order traversal holds entity-level locks per stream). No cross-thread coordination change needed.

### Feature Projection

**What it is:** Downstream streams should only see a subset of upstream fields/features. Without projection, the enriched event would leak all upstream state, making pipeline definitions fragile.

**Implementation:** Add an optional `projection: Option<Vec<String>>` field to `StreamDefinition`. When set, only listed fields from the enriched event are visible to this stream's operators and expressions.

**No new crate.** Just a `Vec<String>` filter applied before `push_internal`.

### Union Node

**What it is:** A stream that receives events from multiple upstream parents (logical UNION ALL). Example: combine `card_transactions` and `ach_transactions` into a single `all_transactions` stream.

**Implementation:** Already supported by petgraph DAG -- `depends_on` is `Option<Vec<String>>`. The existing cascade logic in `push_with_cascade_internal` does BFS from the pushed stream through `downstream_map`. A union node simply appears as a downstream of MULTIPLE parents, so pushing to ANY parent triggers the union node.

**What needs to change:** Currently, if `depends_on` has multiple parents, cascade works but the union node receives only the event from whichever parent triggered it. We need to add a `node_type: NodeType` enum (`Normal | Union`) to `StreamDefinition` so the engine knows this is a multi-parent merge point. The wire format already supports `depends_on: ["stream_a", "stream_b"]`.

**No new crate.** petgraph 0.8 handles multi-parent DAGs natively.

### On-Demand Compute Primitives

**What it is:** Ephemeral pipelines registered at runtime that auto-expire and skip persistence.

**Implementation:** Add to `StreamDefinition`:
```rust
pub ephemeral: bool,        // Skip snapshots, skip event log
pub ephemeral_ttl: Option<Duration>,  // Auto-deregister after TTL
```

**Integration points:**
- Snapshot serialization: skip streams where `ephemeral == true` (existing `iter()` loop gets a filter)
- Event log: skip append for ephemeral streams (existing `if event_log_enabled` check gets `&& !ephemeral`)
- REGISTER handler: already a runtime operation -- no change needed
- Eviction timer: deregister ephemeral streams past their TTL
- Memory: enforce a max ephemeral stream count / memory budget (configurable)

**No new crate.** All changes are flags and conditionals on existing data structures.

---

## Supporting Libraries (Python)

| Library | Version | Purpose | When to Use |
|---------|---------|---------|-------------|
| `typing_extensions` | >=4.6 | `dataclass_transform` for IDE support on `@tl.source`/`@tl.dataset` | Always -- required for >=3.10 compat with PEP 681 |

No other new Python dependencies. The SDK stays zero-dependency beyond `typing_extensions`.

---

## What NOT to Add

| Technology | Why Not | Use Instead |
|------------|---------|-------------|
| Pydantic | 5.4MB dependency with Rust core; runtime validation overhead for a definition-only SDK; violates "thin client" principle | Custom `Field` descriptor + `dataclass_transform` (0 runtime cost) |
| `attrs` | Smaller than Pydantic but still an unnecessary dependency; our Field descriptors need custom behavior (proxy objects, lazy compilation) that attrs cannot provide without fighting it | Custom `Field` class |
| `marshmallow` / `cattrs` | Serialization libraries solving a problem we do not have (SDK serializes to JSON via `to_json()` methods, not via schema reflection) | Existing `to_json()` pattern on OperatorBase |
| `msgspec` | Fast struct library but optimized for deserialization speed, which is irrelevant for pipeline definition objects created once at registration time | Plain classes |
| New Rust crates for enriched propagation | The change is ~50 LOC of `serde_json::Value` manipulation; adding a crate for this would be over-engineering | Direct `serde_json::Value` mutation |
| `uuid` (Rust) for ephemeral pipeline IDs | Ephemeral pipelines can use the same string-name scheme as persistent ones; collision avoidance is the caller's responsibility (SDK generates `f"_ephemeral_{name}_{timestamp}"`) | String names with SDK-side convention |

---

## Installation

```toml
# Python SDK pyproject.toml additions
[project]
dependencies = [
    "typing_extensions>=4.6",
]
```

```toml
# Rust Cargo.toml -- NO CHANGES
# All engine changes use existing crates:
# - serde_json (enriched event mutation)
# - petgraph 0.8 (union node DAG)
# - existing StreamDefinition struct (projection, ephemeral fields)
```

---

## Version Compatibility

| Package | Compatible With | Notes |
|---------|-----------------|-------|
| `typing_extensions>=4.6` | Python >=3.10 | `dataclass_transform` backported from 3.11; 4.6 is the minimum version with stable PEP 681 support |
| `typing_extensions>=4.6` | mypy >=1.1.1 | mypy supports `dataclass_transform` since 1.1.1 (2023-02) |
| `typing_extensions>=4.6` | pyright >=1.1.290 | pyright supports `dataclass_transform` since 1.1.290 |
| petgraph 0.8 (existing) | Multi-parent DAG | Union nodes with multiple `depends_on` entries -- already supported by DiGraph |
| serde_json 1.0 (existing) | Value mutation | `Value::as_object_mut()` for enriched event -- stable API |

---

## Alternatives Considered

| Category | Recommended | Alternative | Why Not |
|----------|-------------|-------------|---------|
| Python type system | `typing_extensions` + custom Field | Pydantic v2 | Too heavy for definition-only SDK; 5.4MB Rust core dependency |
| Python type system | `typing_extensions` + custom Field | stdlib `@dataclass` | Collides with proxy/descriptor pattern; cannot customize field collection |
| Python type system | `typing_extensions` + custom Field | `attrs` | Unnecessary dependency; custom Field gives us exactly what we need with zero overhead |
| Enriched propagation | serde_json::Value clone + merge | New `EnrichedEvent` struct | Would require changing the entire push pipeline signature; Value mutation is simpler and sufficient |
| Union node | petgraph multi-parent edge | New `UnionStream` type | Unnecessary complexity; existing `depends_on: Vec<String>` + a `node_type` flag is sufficient |
| Ephemeral lifecycle | `ephemeral: bool` flag on StreamDefinition | Separate `EphemeralPipeline` struct | Doubles the type hierarchy; a flag on the existing struct keeps the engine code uniform |

---

## Migration Strategy: Old API Removal

The old API (`@st.stream`, `@st.view`, legacy `_stream.py`, `_view.py`) compiles to the same `RegisterRequest` JSON as the new API. The removal plan:

1. **Phase 1:** Ship new `@tl.source`/`@tl.dataset` API alongside old API (both produce same JSON)
2. **Phase 2:** Mark old API as deprecated with runtime warnings
3. **Phase 3:** Remove `_stream.py`, `_view.py`, update `__init__.py` exports

The server sees identical JSON in all phases. Zero server-side migration needed.

---

## Sources

- [PEP 681 -- Data Class Transforms](https://peps.python.org/pep-0681/) -- dataclass_transform specification
- [typing_extensions changelog](https://github.com/python/typing_extensions/blob/main/CHANGELOG.md) -- version history for PEP 681 support
- [Python typing docs](https://docs.python.org/3/library/typing.html) -- Generic, TypeVar, Protocol
- [Fennel AI dataset concept](https://github.com/fennel-ai/client/blob/main/docs/pages/concepts/dataset.md) -- comparable @dataset decorator pattern
- Existing codebase: `python/tally/_dataframe.py`, `python/tally/_expr.py`, `python/tally/_operators.py` -- compilation model already in place
- Existing codebase: `src/engine/pipeline.rs` lines 852-917 -- cascade push internals for enriched propagation design
- Existing codebase: `Cargo.toml` -- current dependency versions verified

---
*Stack research for: v2.0 New API & Engine*
*Researched: 2026-04-12*
