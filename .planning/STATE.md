# State: Beava v2 — v0 OSS Launch

**Project reference:** `.planning/PROJECT.md`
**Roadmap:** `.planning/ROADMAP.md`
**Requirements:** `.planning/REQUIREMENTS.md` (100 v1 REQ-IDs, 100% mapped)
**Milestone:** v0 (first public OSS cut on beava.dev)
**Created:** 2026-04-22

## Core Value

Declare a feature, push events, query it — in under 10 minutes, with curl alone.

## Current Focus

**Phase 1: Foundation** — Rust workspace, HTTP server scaffolding, config, structured logging, integration test harness. No primitive-shipping requirements in this phase; this is the skeleton everything else attaches to.

## Current Position

- **Milestone:** v0
- **Phase:** 1 of 10 (Foundation)
- **Plan:** not yet planned
- **Status:** roadmap approved, awaiting `/gsd-plan-phase 1`
- **Progress:** ▱▱▱▱▱▱▱▱▱▱ 0/10 phases

## Performance Metrics

Measured at v0 ship (Phase 9 is the gate):

- **Apply loop throughput target:** ≥3M EPS/core single-thread (32-byte events × 5 primitives, server-truth)
- **Batch-get latency target:** P50 <2ms, P99 <10ms (100 features × 1 key, warm cache)
- **WAL group-commit overhead target:** P50 <2ms, P99 <10ms added to push ACK
- **Recovery RTO target:** <30s for 10GB state on NVMe
- **Binary size budget:** ≤200MB stripped
- **Primitive catalogue size:** 40 primitives in v0

Not yet measured. Baseline established in Phase 5, hit-gate in Phase 9.

## Accumulated Context

### Architectural Decisions (locked from DESIGN-V2.md §17 and PROJECT.md)

- Single Rust process, single apply-loop thread (auxiliary threads for WAL fsync, HTTP accept, snapshot writer only)
- In-memory state only; no RocksDB, no fjall, no SSD tiering
- WAL file with 1-5ms group-commit fsync; periodic snapshot (default 30s) of in-memory state
- HTTP/1.1 + JSON; 4 endpoints (`/register`, `/push/{stream}`, `/get`, `/get/{feature}/{key}`)
- 40 built-in primitives declared via JSON DSL with where-filter grammar (ops: eq/ne/gt/lt/gte/lte/in + and/or)
- Uniform event-time bucketing, cap 64 buckets per windowed primitive (DGIM rejected for replay determinism)
- Schema evolution: `schema_version: u8` in row header; last 8 schemas retained; on-read migration
- Python SDK: thin HTTP wrapper, sync + fire-and-forget only, no callbacks, no persistent connections
- Commercial tier (HA, replicas, cross-region) explicitly out of v0 OSS scope

### Deferred / Out of Scope (v1+)

- Cross-entity / cross-shard features
- Event emission / timers / CEP / state machines
- Backfill + branching
- Custom user-defined operators (plugin ABI)
- SQL query language
- Multi-tenant isolation
- TCP binary wire protocol in OSS
- Multi-process / multi-instance coordination

### Active Todos

- [x] Roadmap drafted and approved (auto-approved under yolo mode)
- [ ] Plan Phase 1 (`/gsd-plan-phase 1`)
- [ ] Execute Phase 1 through Phase 10

### Blockers

None.

### Open Questions / Follow-ups

- DESIGN-V2.md contains older content (RocksDB, thread-per-core, replicas) that predates the v2 architectural pivot in PROJECT.md. The roadmap follows the PROJECT.md + explicit instruction set (single-process, single-thread, in-memory, no RocksDB). DESIGN-V2.md may want a refresh pass or a clear "superseded by PROJECT.md" header at some later point, but the roadmap does not depend on it being updated.
- `TEST-06` benchmark harness is introduced in Phase 5 as a regression check and driven to the `PERF-01` target in Phase 9; if hardware for the 3M EPS/core bench is not the developer's box, Phase 9 may need to defer the full-target verification to a dedicated benchmark machine.

## Session Continuity

Next session should:

1. Read `.planning/PROJECT.md`, `.planning/ROADMAP.md`, this file.
2. Confirm Phase 1 is the current focus.
3. Run `/gsd-plan-phase 1` to decompose Phase 1 into plans.

---
*State initialized: 2026-04-22 after roadmap creation.*
