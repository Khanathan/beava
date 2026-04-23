# Phase 2: Sources + registry + version bumps - Context

**Gathered:** 2026-04-22
**Status:** Ready for planning
**Mode:** Auto-generated (decisive synthesis from PROJECT.md + REQUIREMENTS.md + Phase 1 handoff + v1 API research)

<domain>
## Phase Boundary

Make `POST /register` real. The server accepts a JSON DAG of event/table/derivation nodes, validates it, and stores it in an in-memory `Registry` keyed by descriptor name. Registration is additive-only: submitting a DAG that adds nodes succeeds with monotonic `registry_version` bump; any removal, type change, or in-place mutation returns 409 with a structured diff naming each offending descriptor.

No apply loop runs yet. No push endpoint. No aggregation infra. This phase is "the registry is real; nothing else computes." Phase 3 (Python SDK) generates the JSON DAG from decorators; Phases 4–5 wire the apply loop to read from the registry.

Out of scope for Phase 2:
- Expression DSL evaluation (Phase 4)
- Stateless ops execution (Phase 4)
- Aggregation operators (Phase 5)
- Python SDK (Phase 3)
- WAL persistence of registry changes (Phase 6 picks this up; Phase 2 only persists in-memory)
- Snapshot (Phase 7)

</domain>

<decisions>
## Implementation Decisions

### JSON DAG payload shape (locked — informs the entire phase)

`POST /register` body:

```json
{
  "nodes": [
    {
      "kind": "event",
      "name": "Transaction",
      "schema": {
        "card_id": "str",
        "amount": "f64",
        "merchant_id": "str",
        "event_time": "i64"
      },
      "optional_fields": [],
      "event_time_field": "event_time",
      "idempotency_key": "request_id",
      "idempotency_ttl_ms": 86400000,
      "history_ttl_ms": 604800000,
      "watermark_lateness_ms": 5000
    },
    {
      "kind": "table",
      "name": "Merchant",
      "primary_key": ["merchant_id"],
      "schema": {
        "merchant_id": "str",
        "name": "str",
        "category": "str"
      },
      "optional_fields": ["category"],
      "ttl_ms": 2592000000,
      "mode": "append"
    },
    {
      "kind": "derivation",
      "name": "BigTx",
      "output_kind": "event",
      "upstreams": ["Transaction"],
      "ops": [
        {"op": "filter", "expr": "(amount > 500)"}
      ],
      "schema": {
        "card_id": "str",
        "amount": "f64",
        "merchant_id": "str",
        "event_time": "i64"
      }
    }
  ]
}
```

Response body on success:
```json
{
  "status": "ok",
  "registry_version": 3,
  "registered_descriptors": ["Transaction", "Merchant", "BigTx"],
  "added": ["BigTx"],
  "already_present": ["Transaction", "Merchant"]
}
```

Response on 409 (additive-only violation):
```json
{
  "error": {
    "code": "registration_conflict",
    "message": "Registration would change or remove existing descriptors",
    "diff": {
      "added": ["NewEvent"],
      "removed": ["OldEvent"],
      "changed": [
        {"name": "Transaction", "reason": "schema_mismatch", "details": "field 'amount' type changed from f64 to i64"}
      ]
    }
  },
  "registry_version": 3
}
```

Response on 400 (validation):
```json
{
  "error": {
    "code": "invalid_registration",
    "path": "nodes[2].upstreams[0]",
    "reason": "upstream 'Missing' not declared in this payload or in registry"
  },
  "registry_version": 3
}
```

### Registry data model (Rust)

```rust
pub struct Registry {
    inner: parking_lot::RwLock<RegistryInner>,
}

pub struct RegistryInner {
    pub version: u64,
    pub events:       std::collections::BTreeMap<String, EventDescriptor>,
    pub tables:       std::collections::BTreeMap<String, TableDescriptor>,
    pub derivations:  std::collections::BTreeMap<String, DerivationDescriptor>,
}

pub struct EventDescriptor {
    pub name:                 String,
    pub schema:               EventSchema,  // BTreeMap<String, FieldType> + optional_fields
    pub event_time_field:     String,
    pub idempotency_key:      Option<String>,
    pub idempotency_ttl_ms:   Option<u64>,
    pub history_ttl_ms:       Option<u64>,
    pub watermark_lateness_ms: Option<u64>,
    pub registered_at_version: u64,
}

pub struct TableDescriptor {
    pub name:          String,
    pub primary_key:   Vec<String>,
    pub schema:        TableSchema,
    pub ttl_ms:        Option<u64>,
    pub mode:          TableMode,  // v0: only `Append`
    pub registered_at_version: u64,
}

pub struct DerivationDescriptor {
    pub name:        String,
    pub output_kind: OutputKind,  // Event | Table
    pub upstreams:   Vec<String>,  // names referencing events/tables/other derivations
    pub ops:         Vec<OpNode>,  // filter/select/with_columns/... + agg spec + join spec + union spec
    pub schema:      DerivedSchema,
    pub table_primary_key: Option<Vec<String>>,  // present if output_kind = Table from group_by
    pub registered_at_version: u64,
}

pub enum FieldType { Str, F64, I64, Bool, Bytes, Datetime }
pub enum OutputKind { Event, Table }
pub enum TableMode { Append }  // v0 only
```

### OpNode — what to carry, what to defer

Phase 2 stores op JSON faithfully but does NOT execute anything. Representation in Rust:

```rust
pub enum OpNode {
    Filter    { expr: String },                   // Phase 4 evaluates
    Select    { fields: Vec<String> },
    Drop      { fields: Vec<String> },
    Rename    { mapping: BTreeMap<String, String> },
    WithColumns { exprs: BTreeMap<String, String> },
    Map       { exprs: BTreeMap<String, String> },  // alias for WithColumns
    Cast      { type_map: BTreeMap<String, String> },
    Fillna    { defaults: BTreeMap<String, serde_json::Value> },
    GroupBy   { keys: Vec<String>, agg: BTreeMap<String, AggSpec> }, // Phase 5
    Join      { other: String, on: Vec<String>, within_ms: Option<u64>, join_type: JoinType },
    Union     { others: Vec<String> },
}

pub struct AggSpec {
    pub op:     String,             // "count", "sum", ..., "ewma", "geo_velocity", ...
    pub params: serde_json::Value,  // operator-specific params
}

pub enum JoinType { Inner, Left }
```

Parsing AggSpec's `op` against the 40+ operator catalogue does NOT happen in Phase 2. Phase 2 validates that `op` is a string; Phase 5 validates it's a known operator. Rationale: keeps Phase 2 narrow and avoids re-adjudicating operator names before they're actually implemented.

### Validation pass (phase-2-visible portion)

At registration time, the server validates:

1. **Node uniqueness**: names within payload are unique; conflict → 400
2. **Reserved names**: descriptor names match `[A-Za-z_][A-Za-z0-9_]*`, length 1–128; reserved prefix `_beava_` rejected
3. **Event schema**: `event_time_field` exists; field type is `i64`; schema has ≥ 1 non-event_time field
4. **Table schema**: all fields in `primary_key` exist in schema; primary key length 1–4 fields
5. **Derivation upstreams**: every name in `upstreams` exists in the payload or in the existing registry
6. **Derivation output schema**: required; Phase 2 does NOT recompute it from `ops` (that's Phase 4's evaluator — Phase 2 trusts the client-supplied schema)
7. **DAG acyclicity**: build a dependency graph across all nodes (payload + existing registry); cycle → 400 with path
8. **Topological order**: nodes must appear in dependency-valid order (upstreams before dependents); if not, 400 with specific offending pair
9. **Idempotency fields**: if `idempotency_key` present, it must be a field name in schema; `idempotency_ttl_ms` must be positive

Validations **deferred** to later phases:
- Expression parsing (`expr: "(amount > 500)"`) — Phase 4
- Operator-name resolution (`op: "count"`) — Phase 5
- Schema propagation through ops (server-side) — Phase 4 (stateless) + 5 (agg)

### Additive-only diff engine

On `POST /register`:
1. Compute `target = current_registry ∪ payload_nodes` (payload supersedes current on name match)
2. For each `name` in `target`:
   - If `name ∉ current` — **added**
   - If `name ∈ current` and the submitted node equals the stored node (field-by-field struct equality on the descriptor, but ignoring `registered_at_version`) — **already_present** (a no-op)
   - If `name ∈ current` and the submitted node differs — **changed** (conflict)
3. For each `name ∈ current` that's NOT in `target` — this case can't happen because we never remove; but if the payload omitted it, that's fine (it stays). A client CANNOT cause a "removed" diff because we ignore names not in the payload. Still, the diff engine explicitly reports it as "stays" so the response shape is stable.
4. If ANY `changed` entries: respond 409 with full diff; do NOT mutate the registry; do NOT bump version
5. If only `added` entries (or mixed added + already_present): atomically install into registry, bump `version` by 1, respond 200

Explicitly: there is **no** `DELETE /register/{name}` endpoint in v0. Descriptors persist for the server's lifetime or until a future v0.1 deprecation endpoint ships.

### Concurrency + atomicity

- `Registry` wraps `RwLock<RegistryInner>` — in v0, reads are hot-path (apply loop needs them) but registrations are rare, so RwLock is fine. Phase 5 can optimize to single-writer-view if needed.
- Registration takes the write lock, computes diff, either commits atomically or rolls back (simple: drop the write without mutating). Version bump + inserts are atomic from the reader's perspective.
- If two clients register simultaneously: order is serialized by the write lock; second registration sees first's additions as "already present" if identical, "changed" if conflicting — this is correct.

### HTTP endpoint (Phase 2)

**`POST /register`**
- Content-Type: `application/json` required; 415 otherwise
- Body: JSON DAG described above
- Success: 200 with the response shape above
- Validation failure: 400 with `{error: {code: "invalid_registration", path, reason}}`
- Conflict: 409 with `{error: {code: "registration_conflict", message, diff}}`
- Empty `nodes` array: 200 with no-op (no version bump)

**`GET /registry` (optional, Phase 2 ships if cheap)**
- Returns full registry JSON at current version
- Useful for Phase 3 Python SDK tests and operator debugging
- Gate behind `--dev` or `BEAVA_DEV_ENDPOINTS=1` env var

### Error messages

Every 400 / 409 error body follows the structured shape. No string-only errors. `path` uses a pseudo-JSON-pointer (`nodes[2].upstreams[0]` or `nodes[3].ops[1].expr`). `reason` is a short sentence. Users can grep their JSON for the `path` to find the issue.

</decisions>

<code_context>
## Existing Code Insights

Phase 1 established (see `.planning/phases/01-foundation/01-SUMMARY.md`):

- **Crates**: `beava-core` (lib) + `beava-server` (bin + lib, `testing` feature)
- **HTTP**: `crates/beava-server/src/http.rs` exposes a `router() -> axum::Router` function; merge new route via `.merge(register_router())`
- **Config**: `crates/beava-core/src/config.rs` — no new fields needed in Phase 2 (registry is in-memory, no config surface)
- **Test harness**: `beava-server::testing::TestServer` with `spawn()` / `wait_ready()` / `shutdown()` + OS-allocated port. Add `TestServer::post_json(path, body)` helper in Phase 2.
- **Error pattern**: `anyhow` at HTTP/main layers; `thiserror` at library layer
- **Observability**: `tracing` JSON logs already wired; emit a log line per successful register + per validation failure (INFO + WARN level respectively)

New crates / deps to add in Phase 2:
- `serde_json` (may already be transitive) — JSON DAG parsing
- `parking_lot` — RwLock for Registry
- `proptest` (dev-dep) — random DAG generation for diff-engine tests

No existing Rust code to integrate with for registry semantics — this is greenfield on top of Phase 1's skeleton.

</code_context>

<specifics>
## Specific Ideas

- Registry reads are hot-path once the apply loop lands (Phase 5). Use `arc_swap::ArcSwap<RegistryInner>` to make reads lock-free. Writes swap the Arc atomically. For Phase 2 alone, RwLock is simpler and adequate; swap to `ArcSwap` in Phase 5 if benchmarks show lock contention.
- `/register` response's `registered_descriptors` field must preserve the order of the input `nodes` array so SDK tests can assert order. BTreeMap iteration is lex-sorted, so compute this from the input payload, not from the registry.
- Validation that's O(n²) (e.g., cycle detection across the full registry + payload) is fine in Phase 2 because registrations are low-volume (admin-path). Don't optimize prematurely.
- Error path coverage is just as important as happy-path coverage. Every 400 case has a specific test.
- The `GET /registry` debug endpoint in v0 is useful but not a REQ. Ship it if it fits in the phase budget; skip if it slips.

</specifics>

<deferred>
## Deferred Ideas

- **WAL persistence of registration events** — Phase 6. Phase 2's registry is in-memory only; restart = empty registry. That's fine for Phase 2's tests; Phase 6 wires WAL around this.
- **Snapshot persistence of registry** — Phase 7.
- **Expression parsing** — Phase 4. Phase 2 stores `expr` strings verbatim.
- **Operator-name resolution** — Phase 5. Phase 2 trusts `op: "count"` is a string.
- **Schema recomputation from ops** — Phase 4 (stateless) + Phase 5 (agg). Phase 2 trusts client-supplied schemas.
- **Deprecation / archival of registered descriptors** — v0.x. v0 is strict append-only.
- **Registry pagination / `_debug` gated endpoint** — ship `GET /registry` only if cheap; mark dev-only.

</deferred>
