# Phase 7: Composable Pipeline - Context

**Gathered:** 2026-04-10
**Status:** Ready for planning

<domain>
## Phase Boundary

Users can define multi-stage streaming pipelines where events automatically cascade through dependent streams in topological order. Delivers: keyless streams (pure ingest + cascade), keyed streams with `depends_on`, DAG topological execution with petgraph, cycle detection at registration, LEFT JOIN semantics for missing upstream values, stream-level filter expressions, and Fennel-inspired pipeline API.

</domain>

<decisions>
## Implementation Decisions

### Keyless Stream Design
- Keyless streams defined via `@st.stream()` with no `key` parameter — `key` becomes optional on the decorator
- Keyless streams have no windowed operators (count, sum, avg, etc.) — they are pure ingest + cascade points
- Keyless streams CAN have derive expressions that compute from `_event.*` fields for enrichment/filtering before cascade
- Keyless streams are invisible to GET — no entity state exists, GET returns features from keyed streams only
- Events pushed to keyless streams are persisted to the event log and cascaded to all downstream dependents

### DAG Cascade Behavior
- Pushing to a keyless stream returns empty feature map `{}` — no features to return
- If a downstream stream's key_field is missing from the cascaded event, that downstream stream is silently skipped (LEFT JOIN spirit)
- Keyed-to-keyed `depends_on` is supported — downstream stream extracts its own key from the event (re-keying)
- Multi-level cascades supported (A->B->C, arbitrary depth) — topological sort handles ordering via petgraph
- Events cascade through the entire DAG in a single push-through cycle (synchronous)

### SDK API Design (Fennel-Inspired)
- `depends_on` expressed as class references: `depends_on=[RawEvents]` — resolved to string names at serialization
- Stream-level `filter="expr"` parameter on `@st.stream()` — applies before all operators, uses existing expression engine
- Multiple `depends_on` sources supported — stream receives events from ALL upstream streams
- Type enforcement at REGISTER time: cycle detection, depends_on streams must exist, key_field presence validated
- Explicit LEFT JOIN semantics via `st.lookup()` — when referencing features from another stream via foreign key, Missing returned if key not found (not error)

### Pipeline Mapping (Fennel -> Tally)
- Fennel's `.filter()` -> Tally's `filter="expr"` on `@st.stream()` (stream-wide) + `where="expr"` on individual operators
- Fennel's `.groupby("key")` -> Tally's `key="field"` on `@st.stream()`
- Fennel's `.aggregate(Count(...))` -> Tally's `st.count(window="1h")` as class attributes
- Fennel's `@inputs(SourceDS)` -> Tally's `depends_on=[SourceDS]`
- Fennel's `.join(..., how="left")` -> Tally's `st.lookup(..., on="foreign_key")` with LEFT JOIN semantics

### TDD Process
- Write failing tests first for each capability (keyless, depends_on, cascade, cycle detection, LEFT JOIN)
- Then implement Rust structures/types
- Then implement logic to make tests pass

### Claude's Discretion
- petgraph graph type selection (DiGraph vs StableGraph)
- Internal cascade event representation (clone event vs reference)
- Registration order handling (does depends_on target need to be registered first, or deferred resolution?)

</decisions>

<code_context>
## Existing Code Insights

### Reusable Assets
- `PipelineEngine` (src/engine/pipeline.rs) — stream/view registration, push_event flow. Needs depends_on + cascade logic
- `StreamDefinition` (src/engine/pipeline.rs) — needs optional key_field, depends_on, filter fields
- `Expression evaluator` (src/engine/expression.rs) — reusable for stream-level filter expressions
- `EventLog` (src/state/event_log.rs) — already handles per-stream event persistence
- `ViewDefinition` with `st.lookup()` (src/engine/view.rs) — existing LEFT JOIN pattern
- Protocol `RegisterRequest` (src/server/protocol.rs) — needs depends_on, filter, optional key_field

### Established Patterns
- AHashMap everywhere (locked decision)
- SystemTime for timestamps
- Postcard for serialization
- winnow for expression parsing
- Cooperative yielding for long operations
- Per-stream isolation in EntityState (Phase 6)

### Integration Points
- `PipelineEngine::push()` — must cascade through DAG after direct stream update
- `StreamDefinition` — add `depends_on: Vec<String>`, `filter: Option<Expr>`, make `key_field: Option<String>`
- `Command::Register` — serialize depends_on + filter from Python SDK
- `convert_register_request()` — parse new fields, validate DAG
- Python SDK `@st.stream()` — optional key, depends_on, filter params
- Snapshot format — may need v5 if StreamDefinition shape changes (or store in pipeline JSON)

</code_context>

<specifics>
## Specific Ideas

- Use petgraph for DAG construction and topological sort (from v1.1 research decision)
- Fennel API as reference model: https://github.com/fennel-ai/client/tree/main/examples
- No full schema declaration required downstream — streams reference upstream fields dynamically
- Type enforcement at REGISTER time, not at event time (registration is the validation gate)
- User instruction: TDD — write tests first, then structures, then code

</specifics>

<deferred>
## Deferred Ideas

- Full Fennel-style `.filter(lambda df: ...)` with Python lambda support (Tally uses string expressions, not Python lambdas — keeps Python out of hot path)
- Complex DAG transformations: map, flatMap on keyless streams (PIPE-F1, deferred to v1.2+)
- Schema migration for running operators (SCHM-F1, deferred to v1.2+)

</deferred>
