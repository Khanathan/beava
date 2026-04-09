# Phase 4: Persistence and Operational Readiness - Context

**Gathered:** 2026-04-09
**Status:** Ready for planning

<domain>
## Phase Boundary

Tally survives restarts (snapshot persistence + crash recovery), reclaims memory for idle keys (TTL eviction), and exposes enough observability for production use (HTTP management API with pipeline CRUD, metrics, and debug endpoints).

</domain>

<decisions>
## Implementation Decisions

### Snapshot Persistence Strategy
- Enum wrapper around operator state (CountState, SumState, etc.) serialized with postcard — per locked decision to use postcard (not bincode, RUSTSEC-2025-0141)
- Periodic timer (default 30s) via tokio::time::interval — matches CLAUDE.md spec
- Clone state then spawn_blocking for serialization — simple, proven pattern (Redis RDB model)
- Single file with atomic rename (write temp, rename) — crash-safe, simple

### TTL Eviction
- Periodic sweep via tokio timer (every 60s) — low overhead, predictable
- Default TTL: 2x largest window per entity — matches CLAUDE.md spec
- Evicted keys re-initialize fresh on next event — matches CLAUDE.md: "state is re-initialized fresh"
- Server-level config flag (--ttl-multiplier, default 2) — simple, single knob

### HTTP Management API
- Prometheus text format (text/plain) for /metrics — industry standard
- Core operational metrics: keys_total, events_total, push_latency_seconds, snapshot_duration_seconds, memory_bytes
- Full operator internals via /debug/key/:key (ring buffer state, HLL sketch) — matches CLAUDE.md spec
- Pipeline CRUD: GET/POST/DELETE /pipelines + GET /pipelines/:name — matches CLAUDE.md spec

### Claude's Discretion
- Snapshot format versioning strategy (version byte per key or header version)
- Cooperative yielding granularity for MSET during snapshot
- Exact Prometheus metric naming conventions

</decisions>

<code_context>
## Existing Code Insights

### Reusable Assets
- src/state/store.rs: StateStore with AHashMap, EntityState with live_operators and static_features
- src/server/http.rs: Existing axum HTTP server with /health endpoint, SharedState pattern
- src/main.rs: Dual server startup (TCP + HTTP), tokio current_thread runtime, env var port config
- Existing Serialize/Deserialize derives on StaticFeature, FeatureValue, window types

### Established Patterns
- Arc<Mutex<AppState>> shared state between TCP and HTTP servers
- Pre-bound listener pattern for test isolation (run_http_server_with_listener)
- axum Router with get/post routes
- postcard for serialization (locked decision)

### Integration Points
- EntityState.live_operators: Vec<(String, Box<dyn Operator>)> — needs enum wrapper for serialization
- EntityState.last_event_at: Option<SystemTime> — already exists for TTL
- HTTP server already accepts SharedState — add routes for pipelines, metrics, debug
- store.rs comment on line 25: "Not serializable via serde (trait objects) -- Phase 4 will use enum wrapper"

</code_context>

<specifics>
## Specific Ideas

- STATE.md blocker note: "Snapshot memory approach — clone-then-spawn_blocking creates up to 2x peak memory. Decide between clone approach and chunked cooperative yielding before Phase 4." — Decision: clone+spawn_blocking chosen (simpler, acceptable for v1)
- Snapshot format must be forward-compatible — version byte allows migration on read per CLAUDE.md
- MSET chunked yielding: process 1024 keys, yield to event loop, continue — per CLAUDE.md spec

</specifics>

<deferred>
## Deferred Ideas

None — discussion stayed within phase scope.

</deferred>
