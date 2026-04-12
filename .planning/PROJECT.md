# Tally

## What This Is

A lightweight, single-binary real-time feature server in Rust. Push events over a custom TCP protocol, get updated streaming features back synchronously. Designed for fraud detection, ML feature serving, and real-time context for AI agents — zero infrastructure, zero ops.

## Core Value

Events go in, features come out — synchronously, in one request-response cycle, with sub-millisecond latency and zero external dependencies.

## Current State

Shipped v1.0 through v1.3. Branch `v1.3-concurrency` carries all work.

**Tech stack:** Rust (tokio, serde, postcard, winnow, ahash, dashmap, parking_lot), Python SDK
**Architecture:** Multi-threaded tokio runtime, DashMap per-stream entity concurrency, custom binary TCP protocol (port 6400), HTTP management API (port 6401)
**Operators:** 16 total — count, sum, avg, min, max, last, distinct_count (HLL), derive, lookup, stddev, percentile, lag, ema, last_n, first, exact_min, exact_max
**Expressions:** 18 builtins — if/coalesce/is_missing, lower/upper/len/contains/starts_with/concat, sqrt/log/pow/ceil/floor/round/clamp
**Persistence:** Periodic postcard snapshots + SSD event log (both disable-able via flags), SlateDB state backend (feature-gated)
**Performance:** 1.1M eps (8 proc), 359k batch (1c), 139k async (1c)
**Testing:** 744 tests (622 lib + 122 integration), all green
**SDK:** DataFrame API (Column proxy + Table + Stream + GroupBy + JoinedTable) alongside legacy @st.stream API

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
- ✓ SDK `push_many()` batch API + `OP_PUSH_BATCH` wire opcode (0x0A) — v1.3
- ✓ DashMap per-stream entity concurrency + parking_lot RwLock PipelineEngine — v1.3
- ✓ Multi-threaded tokio runtime with per-stream locks — v1.3
- ✓ 1.1M eps aggregate (8 proc), 359k batch (1c), 139k async (1c) — v1.3
- ✓ 8 additional operators: stddev, percentile, lag, ema, last_n, first, exact_min, exact_max — v1.3/v1.4
- ✓ 18 expression builtins (conditionals, string ops, math) — v1.3/v1.4
- ✓ DataFrame SDK (Column proxy, Table, Stream, GroupBy, JoinedTable) — v1.3/v1.4
- ✓ SlateDB state backend (write-through cache, feature-gated) — v1.3/v1.4
- ✓ Event log + snapshot disable flags — v1.3/v1.4
- ✓ Snapshot cycle guard + manual trigger endpoint — v1.3/v1.4
- ✓ New Python SDK API: @tl.source, @tl.dataset(depends_on=[...]), EventSet/FeatureSet types, .group_by("key").agg(...), tl.union(), pipeline.validate() — v2.0
- ✓ Local DAG validation: cycle detection, missing dep checks, type mismatch detection — v2.0

### Active

#### Current Milestone: v2.0 New API & Engine

**Goal:** Replace the `@st.stream` decorator API with a function-based `@tl.dataset(depends_on=[...])` pipeline pattern using `EventSet`/`FeatureSet` types. Fill engine gaps (enriched event propagation, feature projection, union node). Remove old API. Architect for on-demand compute.

**Target features:**
- New Python SDK API: `@tl.source`, `@tl.dataset(depends_on=[...])`, `EventSet`/`FeatureSet` types, explicit `.group_by("key").agg(...)`
- Engine: enriched event propagation (~50 LOC Rust), feature projection, union node
- Full test plans per phase (upfront test design, not afterthought)
- Remove old `@st.stream`/`@st.view` API entirely
- Architect for on-demand compute: keep REGISTER as runtime operation, ephemeral pipeline flag, portable definitions (same format for pre-registered and on-demand)

### Out of Scope

- Cluster mode / distributed operation — single-node by design
- Client-side sharding / hash-ring routing across instances — document, don't build
- Multi-tenancy / namespace isolation
- Disk/S3 state spill — future milestone post-launch (S3 replay log is month 1 post-launch)
- On-demand compute product layer — architect for it in v2.0, build post-launch
- Cross-key queries ("count across all users where...") — fundamentally at odds with per-key state model
- Session windows — only sliding/tumbling
- WAL / full durability — violates <100µs p99 latency target
- DataFrame simulation (tl.DF) — users expect Pandas behavior; rejected in favor of honest EventSet/FeatureSet types
- Server-side async push coalescing (PERF-03) — deferred from v1.3, low ROI vs batch API
- Off-thread snapshot I/O (OPS-05) — deferred from v1.3, revisit post-launch

## Constraints

- **Language**: Rust — memory safety, single binary distribution, performance
- **Threading**: Multi-threaded tokio runtime with DashMap per-stream entity concurrency (since v1.3)
- **Protocol**: Custom binary TCP — HTTP too heavy for hot-path latency targets
- **Persistence**: Periodic snapshots + optional SSD event log + optional SlateDB state backend
- **Performance**: <100us p99 PUSH latency, <50us p99 GET latency, 1M+ eps aggregate throughput
- **API compatibility**: v2.0 is a breaking change — old @st.stream API removed, not deprecated alongside

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
| DashMap + per-stream locks (v1.3) | Incremental concurrency over full key-partitioned sharding; DashMap entity-level concurrency + parking_lot RwLock PipelineEngine; 1.1M eps @ 8 proc | ✓ Good |
| Function-based API over decorators (v2.0) | @st.stream hides aggregation key; function pattern is testable, composable, explicit about dependencies. Informed by Fennel experience | — Pending |
| EventSet/FeatureSet over DataFrame (v2.0) | DataFrame simulation is a trap — users expect Pandas behavior. Honest types communicate what the system actually does | — Pending |
| Remove old API, not deprecate (v2.0) | Maintaining two APIs doubles surface area; clean break before launch is the right time | — Pending |
| REGISTER stays runtime operation (v2.0) | Enables sub-second pipeline creation — the primitive that unlocks on-demand compute post-launch | — Pending |

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
*Last updated: 2026-04-12 — v2.0 New API & Engine milestone started*
