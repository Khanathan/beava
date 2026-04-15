# Phase 21: Type system & SDK skeleton - Context

**Gathered:** 2026-04-14
**Status:** Ready for planning
**Mode:** Auto-generated from v0 design conversation + spec (`.planning/research/v0-restructure-spec.md`)

<domain>
## Phase Boundary

Ship the Python SDK foundation for v0: two decorators (`@tl.stream`, `@tl.table`), class-vs-function convention, schema declaration via class attributes + type hints, DAG discovery from function parameter types, output-schema inference, and DataFrame-parity operator catalog stubs. No Rust engine work in this phase — SDK scaffolding only. Engine ops that don't yet exist can raise `NotImplemented` at registration; Phase 22 fills them in.

**This phase is a hard reset** of the Python SDK's public surface. The current `@tl.source` / `@tl.dataset` / `EventSet` / `FeatureSet` API (Phase 16) is deleted. All existing Python tests against the old API are disabled for this phase (Phase 26 rewrites them). The Rust engine remains at its v2.0 state — this phase just builds the new SDK surface that talks to it, with thin bridges.

</domain>

<decisions>
## Implementation Decisions (LOCKED)

### Decorator surface
- `@tl.stream` accepts both classes and functions
  - Class (no function body) = external Stream source; attributes declare schema
  - Function with `-> Stream:` return = Stream derivation; parameters declare upstream dependencies
- `@tl.table(key=str | list[str], ttl="30d")` same pattern, mandatory `key` argument
  - Class = external Table source (CDC-style ingest)
  - Function with `-> Table:` return = Table derivation
- Composite keys declared as a list: `key=["user_id", "merchant_id"]`
- No `@tl.source` or `@tl.dataset` — those names are deleted

### Schema declaration
- Class attributes with type hints (Python 3.10+ PEP 604 style):
  ```python
  @tl.stream
  class Clicks:
      user_id: str
      url: str
      timestamp: datetime
  ```
- `tl.Optional[T]` for nullable fields (Tally-owned, separate from `typing.Optional`)
- `tl.Field(desc="docs string", default=None)` for per-field metadata
- No pydantic dependency required — we own validation

### Output schema
- **Inferred** from operations — no explicit return-type schema required
- `.describe()` method on any Stream/Table returns the inferred schema
- Mismatch errors must include: offending field name, upstream schema dump, closest lexical match using Levenshtein distance (`"'amout' not found in Purchases; did you mean 'amount'?"`)

### DAG discovery
- Dependencies = function parameter type hints
  - `def UserSpend(purchases: Purchases, users: Users) -> Table:` — depends on `Purchases` and `Users`
- Circular dependencies detected at registration with a named-cycle error
- No `depends_on=[...]` parameter on decorators — the function signature is the declaration

### Operator catalog (stubs in this phase)
Stateless per-row operators on both Stream and Table:
- `.filter(predicate)` — expression-language predicate (same grammar as existing `where` / `derive`)
- `.map(fn)` — stateless transform (expression-language or `tl.col(...)` style)
- `.select(*fields)` — keep listed fields
- `.drop(*fields)` — drop listed fields
- `.rename(**mapping)` — rename fields
- `.with_columns(**derived)` — add derived fields (expression-based)
- `.cast(**type_map)` — type coercion
- `.fillna(**defaults)` — null handling

DataFrame-style column references via `tl.col("x")` with arithmetic / comparison operators overloaded:
- `tl.col("amount") > 100`
- `tl.col("x") + tl.col("y")`
- `tl.col("status") == "failed"`

Aggregation stub: `.group_by(*keys).agg(**features)` returns Table; individual aggregation operators (`tl.count`, `tl.sum`, etc.) are Phase 22 scope — in this phase the stubs raise `NotImplementedError("ships in Phase 22")`.

Join stub: `.join(other, on=[...], within=..., type=...)` — Phase 23 scope, stub in this phase.

Union stub: `tl.union(a, b, ...)` — stubbed.

### What's CUT in v0
- `@tl.source`, `@tl.dataset`, `EventSet`, `FeatureSet` — deleted from SDK
- `.lookup()` — removed
- `sort`, `pivot`, `melt`, `explode`, `head/tail/limit`, user-defined reducers — not in v0 surface
- Table `.group_by().agg(...)` aggregation — Table-input aggregation deferred to v0.1 (raises registration error with clear message)

### Validation behavior
- `app.register(*classes_and_functions)` walks the DAG, builds schemas, validates types, catches cycles, produces a `RegisteredPipeline` object
- `app.validate()` runs full validation without sending to server — for tests
- Pipeline serializes to JSON matching the Rust engine's REGISTER opcode payload

### Forward-compat reservations (non-shipped, just reserved)
- Decorator accepts `mode="append"|"changelog"` keyword for Tables, defaulting to `"append"` — `"changelog"` raises `NotImplementedError`
- Wire format carries `_op` field reserved (for v0.1 retraction propagation)

### Scope boundary — what's NOT in this phase
- Rust engine changes (Phase 22 onward)
- Actual aggregation execution (Phase 22)
- Actual join execution (Phase 23)
- Watermark / event-time handling (Phase 24)
- Query surface changes (Phase 25)
- Test migration (Phase 26)

This phase's output: a working Python SDK that registers pipelines, reports useful errors, and can be unit-tested against mock engine responses. End-to-end running requires Phase 22 to land.

</decisions>

<code_context>
## Existing Code Insights

Current SDK lives in `python/tally/` — inspect these files before planning (paths assume project root `/data/home/tally`):
- `python/tally/__init__.py` — current public exports
- `python/tally/_app.py` — client / App class
- `python/tally/_source.py` — existing `@tl.source` decorator to be deleted
- `python/tally/_dataset.py` — existing `@tl.dataset` decorator to be deleted
- `python/tally/_operators.py` — operator catalog (partial — `stddev`, `percentile`, `ema`, `lag`, `last_n`, `first`, `exact_min`, `exact_max` already present per `flink-kafka-gap-analysis.md`)
- `python/tally/_validate.py` — existing validation; will be rewritten for new surface
- `python/tally/_dataframe.py` — old dataframe helpers — to be deleted
- `python/tally/_expr.py` — expression-language compiler (reused; Phase 16 shipped this)

Rust engine's current REGISTER payload shape: see `src/engine/pipeline.rs:666-857` for cascade wiring.

</code_context>

<specifics>
## Specific Ideas

- **Descriptor-style decorator** — `@tl.stream class X:` inspects the class body, confirms no methods present (only attributes), produces a `StreamSource` descriptor. `@tl.stream def X(...) -> Stream:` inspects the function signature, validates return-type annotation matches, produces a `StreamDerivation` descriptor. Dispatch via `isinstance(arg, type)` check inside decorator.
- **Validation errors must be surgical.** The agent-coding brief (a-wins memo) requires every error to name the exact bad thing, show the upstream schema, and suggest a close match. No `ValidationError: check your schema` generic messages.
- **tl.col** expression type should be introspectable — at registration we reconstruct the expression tree via operator overload capture, serialize to the engine's expression grammar, and validate field references against inferred schemas.

</specifics>

<deferred>
## Deferred Ideas

- Table.group_by().agg(...) aggregation — v0.1 (requires retraction propagation)
- Full-outer Stream↔Stream join — v0.1 (per `join-outer-needed.md`)
- Partial-key joins — post-v0
- SUBSCRIBE / SCAN query surface — v0.1+
- Custom user-defined reducers — indefinitely deferred
- On-demand pipeline submission — designed-in but not user-facing yet

</deferred>

---

*Phase: 21-type-system-sdk-skeleton*
*Design decisions sourced from `/data/home/tally/.planning/research/v0-restructure-spec.md` and `/data/home/tally/.planning/ROADMAP.md`*
