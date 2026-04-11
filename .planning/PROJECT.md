# Tally

## What This Is

A lightweight, single-binary real-time feature server in Rust. Push events over a custom TCP protocol, get updated streaming features back synchronously. Designed for fraud detection, ML feature serving, and real-time context for AI agents — zero infrastructure, zero ops.

## Core Value

Events go in, features come out — synchronously, in one request-response cycle, with sub-millisecond latency and zero external dependencies.

## Current State

Shipped v1.0, v1.1 (composable pipeline + event log + debug UI), and v1.2 (fire-and-forget PUSH + binary wire protocol hitting 128–142k eps single-client on medium pipelines).

**Tech stack:** Rust (tokio, serde, postcard, winnow, ahash), Python SDK
**Architecture:** Single-threaded tokio event loop, custom binary TCP protocol (port 6400), HTTP management API (port 6401)
**Operators:** count, sum, avg, min, max, last, distinct_count (HLL), derive, lookup
**Persistence:** Periodic postcard snapshots to disk, crash recovery on startup
**Testing:** 110+ Rust unit tests, 12 Python E2E integration tests

## Requirements

### Validated

- ✓ In-memory state store (HashMap<EntityKey, FeatureMap>) — v1.0
- ✓ Sliding window implementation (bucketed ring buffer) — v1.0
- ✓ Operators: count, sum, avg, min, max, distinct_count (HLL), last — v1.0
- ✓ Expression parser and evaluator (arithmetic, comparison, boolean, field access) — v1.0
- ✓ TCP server with persistent connections and binary protocol — v1.0
- ✓ PUSH, GET, SET, MSET, REGISTER commands — v1.0
- ✓ Pipeline registration from JSON definitions — v1.0
- ✓ Synchronous push-through (event -> operators -> derives -> response) — v1.0
- ✓ Cross-stream views with derive expressions — v1.0
- ✓ Cross-key lookups (st.lookup) — v1.0
- ✓ Event fan-out to multiple streams — v1.0
- ✓ Where-clause filtering — v1.0
- ✓ Python SDK: @st.stream, @st.view decorators, operator classes — v1.0
- ✓ Python SDK: TCP client with connection pooling and auto-reconnect — v1.0
- ✓ Python SDK: typed feature results — v1.0
- ✓ Snapshot persistence (periodic postcard to disk) — v1.0
- ✓ Snapshot recovery on startup — v1.0
- ✓ TTL-based key eviction — v1.0
- ✓ MSET chunked yielding (cooperative, non-blocking) — v1.0
- ✓ HTTP management API (health, metrics, debug, pipeline CRUD) — v1.0
- ✓ EntityState per-stream isolation + SSD event log (append-only, history_ttl, compaction) — v1.1
- ✓ Keyless streams for raw event ingestion — v1.1
- ✓ Composable pipeline DAG (keyed depends_on, topological cascade, LEFT JOIN, cycle detection) — v1.1
- ✓ Backfill & schema evolution (add/remove features without state reset, replay from event log) — v1.1
- ✓ MGET batch reads — v1.1
- ✓ Incremental snapshots (dirty-key tracking, base + delta files) — v1.1
- ✓ Debug UI (interactive topology DAG, memory/throughput/state drill-ins, latency histograms) — v1.1
- ✓ Fire-and-forget PUSH (`OP_PUSH_ASYNC` 0x07, `OP_FLUSH` 0x08, `app.push_sync`, `app.flush`) — v1.2
- ✓ Binary wire event format on PUSH paths (replaces JSON serialize on hot path) — v1.2
- ✓ 128–142k eps single-client on medium pipelines (5.7× v1.1 baseline) — v1.2

### Active

#### Current Milestone: v1.3 Concurrency & Client Batching

**Goal:** Break past the single-core ceiling v1.2 hit at ~140k eps. Target 500k–1M events/sec on a single node by parallelizing the engine, amortizing per-event fixed costs, and unblocking the main thread during snapshots — buying the headroom future milestones will need (disk/S3 spill, heavier operators, richer pipelines).

**Target features:**
- Server-side async push coalescing (buffer per-connection, batch under one state lock)
- SDK `push_many()` batch API + `OP_PUSH_BATCH` wire opcode (0x0A)
- Key-partitioned multi-threaded engine (shard `EntityState` across `num_cpus` workers, no cross-thread locks on hot path)
- Snapshot I/O off main thread (eliminate 15–25% duty-cycle loss during writes)

**Performance targets:**
- Phase 12 multi-client async: ≥ 200k eps @ 4 clients on medium pipeline
- Phase 13 single-client via `push_many`: ≥ 300k eps on medium pipeline
- Phase 14 aggregate multi-threaded: ≥ 1,000,000 eps @ 16 clients × 16 shards
- Phase 15 snapshot stall regression: ≤ 5% async throughput loss during write (was 15–25%)
- 532 existing tests remain green across all phases

### Out of Scope

- Cluster mode / distributed operation — single-node by design
- Client-side sharding / hash-ring routing across instances — document, don't build
- Multi-tenancy / namespace isolation
- Disk/S3 state spill — future milestone (v1.3 provides the headroom, not the implementation)
- Bundled binary distribution (pip install tally) — requires v1 validated first
- Session windows — only sliding/tumbling
- WAL / full durability — violates <100µs p99 latency target
- Point-in-time historical replay — changes system from serving to storage

## Constraints

- **Language**: Rust — memory safety, single binary distribution, performance
- **Threading**: Single-threaded core through v1.2; v1.3 introduces key-partitioned multi-threading (shard-per-worker, no cross-thread locks on hot path) — simplicity preserved per shard
- **Protocol**: Custom binary TCP — HTTP too heavy for hot-path latency targets
- **Persistence**: Periodic snapshots only — no WAL, no embedded KV, losing ~30s on crash is acceptable
- **Performance**: <100us p99 PUSH latency, <50us p99 GET latency, >100K events/sec throughput

## Key Decisions

| Decision | Rationale | Outcome |
|----------|-----------|---------|
| Rust for implementation | Memory safety + single binary + performance for "zero ops" promise | ✓ Good |
| Single-threaded v1 | Keys are independent; key-partitioned sharding is drop-in upgrade later | ✓ Good |
| In-memory HashMap (AHashMap) | Fastest possible; SipHash has 20-25% CPU overhead at 100K+ events/sec | ✓ Good |
| Custom binary TCP protocol | HTTP too heavy for hot path; persistent connections, minimal framing | ✓ Good |
| String-based expression language (winnow Pratt) | Keeps Python out of hot path; tiny parser covers 95% of use cases | ✓ Good |
| HyperLogLog for distinct_count | Bounded memory per key (~12KB); well-understood error bounds (~1.6%) | ✓ Good |
| Postcard for snapshots (not bincode) | bincode has RUSTSEC-2025-0141 advisory, unmaintained | ✓ Good |
| SystemTime (not Instant) for windows | Client-supplied Unix timestamps must be comparable | ✓ Good |
| OperatorState enum (not Box<dyn Operator>) | Required for postcard serialization; eliminates dynamic dispatch | ✓ Good |
| Clone-then-spawn_blocking for snapshots | Up to 2x peak memory but non-blocking; acceptable for v1 | ⚠️ Revisit |
| Rename from Streamlet to Tally | Shorter, punchier, approved during design review | ✓ Good |
| Fire-and-forget PUSH (v1.2) | Decouple PUSH from feature response so clients stop round-tripping per event; unlocked 5.7× throughput on medium pipelines | ✓ Good |
| Binary wire event format on PUSH paths (v1.2) | JSON serialize was ~30% of single-event cost; replacing it on hot path was prerequisite for >100k eps single-client | ✓ Good |
| Key-partitioned multi-threading (v1.3 plan) | Single-core ceiling was hit in v1.2 (66% CPU at 142k eps); 47 idle cores is the highest-ROI lever. Shard-per-worker preserves lock-free hot path per shard | ⏳ Planned |

## Evolution

This document evolves at phase transitions and milestone boundaries.

**After each phase transition** (via `/gsd-transition`):
1. Requirements invalidated? -> Move to Out of Scope with reason
2. Requirements validated? -> Move to Validated with phase reference
3. New requirements emerged? -> Add to Active
4. Decisions to log? -> Add to Key Decisions
5. "What This Is" still accurate? -> Update if drifted

**After each milestone** (via `/gsd-complete-milestone`):
1. Full review of all sections
2. Core Value check — still the right priority?
3. Audit Out of Scope — reasons still valid?
4. Update Context with current state

---
*Last updated: 2026-04-11 — v1.3 Concurrency & Client Batching milestone started*
