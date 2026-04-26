# Phase 16: Python SDK -- New Types and Decorators - Context

**Gathered:** 2026-04-12
**Status:** Ready for planning

<domain>
## Phase Boundary

Replace the `@st.stream` decorator API with a function-based `@tl.source` / `@tl.dataset(depends_on=[...])` pipeline pattern using `EventSet`/`FeatureSet` types. All definitions must compile to the existing RegisterRequest JSON format and be testable on the current server without Rust changes. Old API remains untouched until Phase 19.

</domain>

<decisions>
## Implementation Decisions

### Module & Naming Strategy
- New types live in new files: `_source.py`, `_dataset.py`, `_schema.py` — clean separation from old API
- Public import path: `import tally as tl` (same package, `tl` alias by convention) — no new package needed
- `@tl.dataset` supersedes `DataStream`/`Table`/`GroupBy` from `_dataframe.py` — old DataFrame classes deleted in Phase 19
- Naming: `@tl.source` (shorter, matches REQUIREMENTS.md API-01 wording)

### Type System Design
- `EventSet`/`FeatureSet` use `dataclass_transform` decorator on a base class — users write plain class attributes with `Field()` descriptors, IDE autocomplete works via PEP 681
- `.group_by("key").agg(...)` returns a `GroupedDataset` intermediate that has only `.agg()` — mirrors existing `GroupBy` pattern in `_dataframe.py`
- `tl.union()` is a free function returning a `UnionSource` that compiles to multi-parent `depends_on`
- `pipeline.validate()` returns a list of `ValidationError` objects with `.path`, `.message`, `.kind` (cycle/missing_dep/type_mismatch) — empty list = valid

### Compilation & Coexistence
- Old and new APIs coexist during Phase 16 — new API is additive, old API untouched until Phase 19 deletion
- Each dataset has a `._compile()` method returning the same JSON dict that `@st.stream` produces — tested by asserting JSON equality between old and new definitions
- Reuse existing `_operators.py` classes (`tl.count(window="1h")` is same `Count(window="1h")`) — zero duplication
- Dedicated test file `test_new_api.py` testing: compile-to-JSON correctness, validate() error cases, EventSet/FeatureSet typing, union, group_by.agg — all against existing server

### Claude's Discretion
None — all questions answered explicitly.

</decisions>

<code_context>
## Existing Code Insights

### Reusable Assets
- `_operators.py` — All 16 operator classes (Count, Sum, Avg, etc.) with `to_json()` methods — reuse directly
- `_dataframe.py` — `GroupBy` pattern, `_to_register_json()` protocol, `_collect_registrations()` DAG walker — reference for compilation
- `_expr.py` — `Column`, `Expr`, `EventProxy` expression proxies — reuse for derive expressions
- `_protocol.py` — Binary protocol encoding, `OP_REGISTER` — reuse for registration

### Established Patterns
- All pipeline classes implement `_to_register_json() -> dict` for compilation
- `Dataset` base class in `_dataframe.py` provides collection/registration protocol
- `StreamMeta` metaclass in `_stream.py` collects operator descriptors from class body and bases
- `App.register()` and `App.register_all()` handle transitive dependency registration

### Integration Points
- `App._register_one(json_dict)` sends RegisterRequest over TCP — new API must produce identical dicts
- `__init__.py` exports — new symbols added alongside old ones
- `_app.py` — `App.source()` already exists for DataFrame API, needs to work with new `@tl.source` too

</code_context>

<specifics>
## Specific Ideas

No specific requirements — open to standard approaches. The v2.0 decisions in STATE.md (function-based API, EventSet/FeatureSet as honest types, explicit .group_by().agg()) are the spec.

</specifics>

<deferred>
## Deferred Ideas

None — discussion stayed within phase scope.

</deferred>
