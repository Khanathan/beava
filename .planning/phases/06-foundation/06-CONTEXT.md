# Phase 6: Foundation - Context

**Gathered:** 2026-04-09
**Status:** Ready for planning

<domain>
## Phase Boundary

Restructure entity state for per-stream isolation and establish the SSD event log as the persistence foundation for all subsequent v1.1 features. Delivers: per-stream EntityState grouping, append-only event log with configurable history TTL, background compaction, MGET command, and per-stream entity TTL.

</domain>

<decisions>
## Implementation Decisions

### Event Log Storage Design
- Per-stream log files (one file per registered stream) for independent compaction and TTL management
- Length-prefixed postcard serialization for log entries (consistent with snapshot format, compact binary)
- Timer-based background compaction every 60 seconds with cooperative yielding (like MSET chunking)
- Default history_ttl of 72 hours (3 days) when not configured per stream

### MGET Protocol & Response
- Opcode 0x06 for MGET (next sequential after REGISTER=0x05)
- Response format: nested JSON map `{ "key1": { "feat": val }, "key2": { "feat": val } }`
- Missing keys return empty map `{}` in response (consistent with GET behavior)
- No hard limit on keys per MGET request (server trusts client, like MSET)

### EntityState Restructure & Per-Stream TTL
- EntityState uses `HashMap<StreamName, StreamEntityState>` for per-stream isolation (StreamEntityState holds operators + last_event_at)
- Snapshot format bumped to v4 (clean break from v3, no migration — backward compatibility is not a concern)
- GET response merges all streams' features into flat map (simple API, no nesting)
- Per-stream entity TTL via `entity_ttl` field on StreamDefinition (set at registration time via Python SDK)

### Claude's Discretion
- Event log directory structure and naming conventions
- Compaction implementation details (rewrite vs truncate)
- BufWriter buffer sizing and fdatasync interval
- MGET wire format details (string encoding for multiple keys)

</decisions>

<code_context>
## Existing Code Insights

### Reusable Assets
- `StateStore` (src/state/store.rs) — AHashMap<EntityKey, EntityState>, needs restructure for per-stream grouping
- `OperatorState` enum (src/state/snapshot.rs) — serializable operator wrapper, reusable in new StreamEntityState
- `StaticFeature` (src/state/store.rs) — direct-write features, stays alongside per-stream live state
- Protocol frame encoding (src/server/protocol.rs) — `encode_frame`/`parse_frame` for adding MGET opcode
- Eviction logic (src/state/eviction.rs) — needs update from global TTL to per-stream TTL

### Established Patterns
- AHashMap everywhere (locked decision from v1.0)
- SystemTime for all timestamps (not Instant — client timestamps must be comparable)
- Postcard for binary serialization (not bincode — RUSTSEC advisory)
- Cooperative yielding for long operations (MSET chunking pattern)
- spawn_blocking for snapshot writes (clone-then-serialize)

### Integration Points
- `PipelineEngine::push_event()` — must write to event log after operator updates
- `StreamDefinition` — needs new fields: `history_ttl`, `entity_ttl`
- `Command` enum (protocol.rs) — add `Mget { keys: Vec<String> }` variant
- TCP handler (server/tcp.rs) — route MGET command
- HTTP debug API — expose event log stats
- Python SDK — add `entity_ttl` and `history_ttl` to stream definition, add `app.mget()` method

</code_context>

<specifics>
## Specific Ideas

- Event log uses BufWriter + periodic fdatasync (never sync on hot path) — from v1.1 research
- EntityState refactor (per-stream grouping) must precede all other v1.1 work — from v1.1 research
- No backward compatibility constraints — user explicitly stated clean breaks are fine

</specifics>

<deferred>
## Deferred Ideas

None — discussion stayed within phase scope.

</deferred>
