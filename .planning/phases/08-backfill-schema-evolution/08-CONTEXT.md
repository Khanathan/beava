# Phase 8: Backfill & Schema Evolution - Context

**Gathered:** 2026-04-09
**Status:** Ready for planning

<domain>
## Phase Boundary

Users can evolve stream definitions over time -- adding and removing features without state reset -- and backfill new features from the event log for deterministic results. Delivers: schema diff on re-registration, lazy GC of removed features, per-feature backfill flag, cooperative backfill replay with epoch boundary semantics, and re-registration response with diff summary.

</domain>

<decisions>
## Implementation Decisions

### Schema Diff & Migration Semantics
- Diff old vs new FeatureDef lists by name — compare registered stream's features against incoming definition, classify as added/removed/unchanged
- Removed features cleaned up lazily on next snapshot — mark removed, stop computing, GC during snapshot serialization (no hot-path cost)
- Reject type changes — return error on re-register if existing feature name has different operator type (user must remove+add with new name)
- Atomic swap on re-register — build new definition, swap in single assignment; in-flight event completes with old definition (single-threaded, no race)

### Backfill Execution Model
- Epoch boundary for live+backfill coexistence — backfill replays events using historical timestamps; live events update operators normally for existing features; backfill only initializes the NEW feature's operator; no conflict because they operate on disjoint feature sets
- 64 events per yield cycle — same cooperative pattern as MSET chunking
- Automatic on re-register — when new feature has `backfill=True`, server starts background backfill task after registration returns OK; expose backfill status via `GET /debug/backfill` HTTP endpoint
- Idempotent restart on crash — detect incomplete backfill (feature exists but no "backfill complete" marker), re-read event log from start; operators are deterministic so replay produces same result

### SDK API & Deterministic Replay
- Per-feature backfill flag — `st.count(window="1h", backfill=True)` on individual features; serialized in FeatureDef JSON, server reads during schema diff
- Event timestamps for bucketing during replay — `operator.push(event, event_timestamp)` instead of wall clock; window expiry relative to event time for deterministic results
- Derives auto-resolve after backfill — computed on read, no special handling; once backfilling operator has state, derives return computed values
- Re-registration returns schema diff summary — `{"status": "ok", "added": ["feat"], "removed": ["feat"], "backfilling": ["feat"]}`

### Claude's Discretion
- Backfill task internal data structures (tracking progress, completion markers)
- Event log seek/iteration strategy for backfill replay
- HTTP backfill status endpoint response format details
- Snapshot v4 compatibility handling for lazy GC markers

</decisions>

<code_context>
## Existing Code Insights

### Reusable Assets
- `PipelineEngine::register()` (src/engine/pipeline.rs) — current registration flow, needs schema diff logic added
- `EventLog::read_entries(stream_name)` (src/state/event_log.rs) — reads entire log file, reusable for backfill replay source
- `create_operator(def)` (src/engine/pipeline.rs) — operator factory, reusable for initializing new features during schema evolution
- Cooperative yielding pattern from MSET chunking — established pattern for backfill rate limiting
- `OperatorState` enum (src/state/snapshot.rs) — push/read trait, operators already support timestamp parameter

### Established Patterns
- AHashMap everywhere (locked v1.0 decision)
- SystemTime for timestamps (event timestamps from log compatible)
- Postcard for serialization (event log entries, snapshot format)
- Single-threaded event loop — backfill must yield cooperatively, not block
- Per-stream isolation in EntityState (Phase 6) — backfill targets specific stream's operators

### Integration Points
- `PipelineEngine::register()` — must add schema diff before storing new definition
- `StateStore::get_or_create_stream()` — must preserve existing operators for unchanged features
- `EventLog::read_entries()` — backfill reads from here, may need streaming iterator for large logs
- `handle_register()` in TCP handler — must return diff summary JSON instead of plain OK
- HTTP management API — add `GET /debug/backfill` endpoint

</code_context>

<specifics>
## Specific Ideas

No specific requirements — open to standard approaches guided by established codebase patterns.

</specifics>

<deferred>
## Deferred Ideas

None — discussion stayed within phase scope.

</deferred>
