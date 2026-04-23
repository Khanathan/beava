# Phase 2: Primitive infra + registration - Context

**Gathered:** 2026-04-22
**Status:** Ready for planning
**Mode:** Auto-generated (decisive synthesis from DESIGN-V2.md + PROJECT.md + Phase 1 handoff)

<domain>
## Phase Boundary

Make `POST /register` real. The server parses JSON declarations for one stream + a list of features, validates them, and stores them in an in-memory registry. Three supporting pieces of shared infrastructure land alongside: the `Operator` trait, the `Windowed<Op>` wrapper with uniform event-time bucketing (cap 64), and the `Where` filter DSL evaluator.

Crucially, this phase ships **NO primitives that actually do aggregation work** beyond a placeholder `PassthroughOp` used in tests. The 9 core aggregates are Phase 3. The operator trait and registry exist to be filled.

Out of scope for Phase 2:
- `POST /push/{stream}` (Phase 3)
- `POST /get` (Phase 3)
- Any real primitive implementations
- WAL / durability (Phase 4)
- Snapshot / recovery (Phase 5)

</domain>

<decisions>
## Implementation Decisions

### Operator trait shape (locked per DESIGN-V2.md discussion)

- `pub trait Operator: Send + 'static`
- Associated type `type State: Default + Serialize + DeserializeOwned + 'static` — v0 uses Serde + bincode (no `fjall`/`rocksdb`; state is in-memory only, serde only needed later for snapshots in Phase 5)
- Methods (v0 minimal set — no timers, no emits, no responses):
  - `fn apply(&self, event: &Event, state: &mut Self::State, ctx: &mut OpCtx)`
  - `fn read(&self, state: &Self::State, ctx: &ReadCtx) -> FeatureValue`
- `OpCtx` carries: `now: EventTime`, `event_time: EventTime` of the current event — nothing else in v0
- No `on_timer`, no `OpOutcome::Emit/Reject/Response` — those land in v1
- `FeatureValue` enum: `{Int(i64), Float(f64), Bool(bool), Str(String), List(Vec<FeatureValue>), Map(BTreeMap<String, FeatureValue>), Null}` — serializable to JSON for `/get` responses

### Windowing infra

- `Windowed<Op: Operator>` wraps any operator to add event-time bucketing
- Uniform bucketing, default `bucket_count = 64`, width = `ceil(window_ms / 64)`
- Per-feature override: `bucket_count` optional at registration time; server logs a warning if the user overrides
- Lazy rollover: on each `apply`, compute current bucket from event_time; evict buckets older than `window_ms`
- "Lifetime" mode when `window_ms` omitted — a single bucket, no rollover, state grows bounded by the operator's own invariants
- `WindowedState<S> { buckets: Vec<(bucket_id, S)>, lifetime_state: Option<S> }`

### Where-filter DSL

- JSON grammar: `{field: {op: value}}` as the leaf predicate; `{and: [P1, P2, ...]}` / `{or: [...]}` for composition
- Ops: `eq`, `ne`, `gt`, `lt`, `gte`, `lte`, `in` (in takes an array)
- Type coercion: integer-vs-float comparisons promote; string ops compare byte-lex; bool ops eq/ne only (gt/lt on bool returns 400 at registration time)
- Compile the where-clause at registration time into a `CompiledPredicate` that references field *indices* into the stream's schema (not string lookups) — hot-path-friendly for Phase 3
- Errors are surfaced at registration, not at push time; the registration response's 400 names the offending `path` (e.g., `features[3].where.and[1].amount.gt`) and `reason`

### Stream declaration + registry

- Stream config: `{name: String, shard_key: String, idempotency_key: Option<String>, idempotency_ttl_ms: Option<u64>, schema: BTreeMap<String, FieldType>}`
- `FieldType` enum: `Str`, `F64`, `I64`, `Bool`
- Mandatory `event_time: i64` in every schema; registration returns 400 if missing
- `idempotency_key` + `idempotency_ttl_ms` accepted and persisted but NOT enforced in Phase 2 (Phase 4 wires the enforcement logic); register must store them so Phase 4 can pick them up without a schema migration
- Feature declaration: `{name, type, field?, window_ms?, where?, ...type_specific_params}`
- Registration is idempotent: same (stream name + features) payload ⇒ 200 no-op. Conflicting redeclaration (same name, different schema or different feature set) ⇒ 409 with a structured `{diff: {streams: {...}, features: {added, removed, changed}}}` body
- Registry is an `Arc<RwLock<Registry>>` for Phase 2; swap to single-writer invariant in Phase 3 when push path lands

### `POST /register` endpoint contract

- `Content-Type: application/json` required; 415 otherwise
- Request body: `{stream: {...}, features: [{...}, ...]}`
- Response 200: `{status: "ok", stream: "<name>", feature_count: N, registered_at: <unix_ms>}`
- Response 400 (validation): `{error: {code: "invalid_registration", path: "<json-pointer-ish>", reason: "<human-readable>"}}`
- Response 409 (conflict): `{error: {code: "registration_conflict", diff: {...}}}`
- Response 415 (content-type): `{error: {code: "unsupported_media_type"}}`

### Placeholder operator for test coverage

- `PassthroughOp` — increments a `u64` counter on apply, returns `FeatureValue::Int(count)` on read
- Used to exercise `Windowed<PassthroughOp>`, registry round-trips, where-filter pre-filtering
- NOT exposed via the JSON DSL as a registrable primitive type; it's test-only (`#[cfg(test)]`)

### Error-handling stance

- `anyhow::Error` at the HTTP layer (error → ErrorResponse conversion)
- `thiserror`-derived errors at the library layer: `RegistrationError`, `WhereParseError`, `WindowError` with specific variants
- All 400-path errors include enough context for a curl user to find the issue without reading server logs

### Where compilation strategy (v0 cut)

- At registration: walk the JSON where-clause, resolve each `field` against the stream schema's field index; produce a `CompiledPredicate` tree of `Node::{And, Or, Leaf{field_idx, op, value}}`
- At evaluate time (Phase 3+): take a typed `Event` row, evaluate in-place with zero allocations
- Do NOT attempt SIMD / vectorized predicates in Phase 2; a naive interpretation is fine at 3M EPS

</decisions>

<code_context>
## Existing Code Insights

Phase 1 established:

- `crates/beava-core/` (lib) — add new modules: `operator.rs`, `window.rs`, `where_filter.rs`, `registry.rs`, `schema.rs` — all no-unsafe, minimal deps
- `crates/beava-server/src/http.rs` — use `.merge(registration_router())` to attach the new `POST /register` route
- `crates/beava-server/` `Config` — additive: no new fields in Phase 2 (registry lives in-memory, no config needed)
- `TestServer::spawn()` harness — reuse in integration tests; add helper methods like `TestServer::post_json(path, body)` if the existing surface is too raw
- Error handling pattern from Phase 1: `anyhow` at `main.rs` / `http.rs`, `thiserror` at core

New deps needed (add in Phase 2):
- `serde_json` (already likely a transitive dep) — JSON DSL parsing
- `indexmap` (optional) — ordered feature list preservation in registry responses
- `proptest` (dev-dep) — quickcheck-style tests for where-filter parser

</code_context>

<specifics>
## Specific Ideas

- Registration response omits the full feature list on success — just count + stream name, to avoid large response bodies when users register 50+ features at once. `GET /registry` (future) can return the full dump.
- The `/register` handler must short-circuit on content-type mismatch BEFORE deserialization, to give a clean 415 instead of a Serde error.
- Where-filter error paths should use a pseudo-JSON-pointer (e.g., `features[2].where.and[0].amount`) — users grep their registration JSON for it.
- Consider exposing a `#[cfg(feature = "debug-dump")]` endpoint `GET /registry/_debug` that dumps the registry as JSON. Useful during Phase 3+ development; gate behind a feature flag, not shipped in release builds.
- Where-clause on events that don't have the referenced field is an error at registration time, never at push time. Schema is the contract.
- Idempotency fields accepted but unused in Phase 2 — tested separately in Phase 4. Doc the expected behavior inline.

</specifics>

<deferred>
## Deferred Ideas

- **`POST /push`** — Phase 3. Parse events, route to registered features, run through apply loop.
- **Push validation against schema** — Phase 3. Register stores the schema; push consumes it.
- **Per-entity state backing** — Phase 3. `WindowedState<S>` only makes sense once apply runs.
- **Concrete primitives (count/sum/etc.)** — Phase 3. `PassthroughOp` is the only operator in Phase 2.
- **Persistent registration** — Phase 5. Registry serialized into snapshots; WAL records each `POST /register` so the registry rebuilds on restart.
- **Registration mutation** — always deferred. v0 registrations are append-only; changing a feature = new name. (Enforced via the 409 conflict response.)

</deferred>
