# Tally

## What This Is

A lightweight, single-binary real-time feature server in Rust. Push events over a custom TCP protocol, get updated streaming features back synchronously. Designed for fraud detection, ML feature serving, and real-time context for AI agents — zero infrastructure, zero ops.

## Core Value

Events go in, features come out — synchronously, in one request-response cycle, with sub-millisecond latency and zero external dependencies.

## Current State

Shipped v1.0 with 9,904 lines of Rust + 2,915 lines of Python (~12,800 total).

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

### Active

(None — planning next milestone)

### Out of Scope

- Key-partitioned multi-threading — v1 is single-threaded like Redis; sharding is a future vertical scaling upgrade
- Cluster mode / distributed operation — single-node by design
- Bundled binary distribution (pip install tally) — requires v1 validated first
- Schema evolution (add/remove features without reset) — post-v1 (TODOS.md P1)
- Incremental snapshot serialization — post-v1 optimization (TODOS.md P1)
- Batch GET (MGET) — post-v1 (TODOS.md P1)
- Multi-tenancy / namespace isolation — post-v1 (TODOS.md P2)
- Session windows — only sliding/tumbling in v1
- WAL / full durability — violates <100us p99 latency target
- Point-in-time historical replay — changes system from serving to storage

## Constraints

- **Language**: Rust — memory safety, single binary distribution, performance
- **Threading**: Single-threaded core (v1) — simplicity, no locks, no contention
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
*Last updated: 2026-04-09 after v1.0 milestone completion*
