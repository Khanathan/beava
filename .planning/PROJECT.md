# Tally

## What This Is

A lightweight, single-binary real-time feature server in Rust. Push events over a custom TCP protocol, get updated streaming features back synchronously. Designed for fraud detection, ML feature serving, and real-time context for AI agents — zero infrastructure, zero ops.

## Core Value

Events go in, features come out — synchronously, in one request-response cycle, with sub-millisecond latency and zero external dependencies.

## Current Milestone: v1.0 Core Feature Server

**Goal:** Ship a complete, single-binary real-time feature server with Python SDK — from event ingestion to feature serving.

**Target features:**
- In-memory state store with windowed aggregation engine
- Custom binary TCP protocol (PUSH, GET, SET, MSET, REGISTER)
- Expression evaluator for derive and where clauses
- Cross-stream views and cross-key lookups
- Python SDK with decorators and TCP client
- Snapshot persistence with crash recovery
- TTL-based key eviction
- HTTP management API

## Requirements

### Validated

<!-- Shipped and confirmed valuable. -->

(None yet — ship to validate)

### Active

<!-- Current scope. Building toward these. -->

- [ ] In-memory state store (HashMap<EntityKey, FeatureMap>)
- [ ] Sliding window implementation (bucketed ring buffer)
- [ ] Operators: count, sum, avg, min, max, distinct_count (HLL), last
- [ ] Expression parser and evaluator (arithmetic, comparison, boolean, field access)
- [ ] TCP server with persistent connections and binary protocol
- [ ] PUSH, GET, SET, MSET, REGISTER commands
- [ ] Pipeline registration from JSON definitions
- [ ] Synchronous push-through (event -> operators -> derives -> response)
- [ ] Cross-stream views with derive expressions
- [ ] Cross-key lookups (st.lookup)
- [ ] Event fan-out to multiple streams
- [ ] Python SDK: @st.stream, @st.view decorators, operator classes
- [ ] Python SDK: TCP client with connection pooling
- [ ] Python SDK: typed feature results
- [ ] Snapshot persistence (periodic serde+bincode to disk)
- [ ] Snapshot recovery on startup
- [ ] TTL-based key eviction
- [ ] MSET chunked yielding (cooperative, non-blocking)
- [ ] HTTP management API (health, metrics, debug, pipeline CRUD)

### Out of Scope

<!-- Explicit boundaries. Includes reasoning to prevent re-adding. -->

- Key-partitioned multi-threading — v1 is single-threaded like Redis; sharding is a future vertical scaling upgrade
- Cluster mode / distributed operation — single-node by design
- Bundled binary distribution (pip install tally) — requires v1 validated first
- Schema evolution (add/remove features without reset) — post-v1 (TODOS.md P1)
- Incremental snapshot serialization — post-v1 optimization (TODOS.md P1)
- Batch GET (MGET) — post-v1 (TODOS.md P1)
- Multi-tenancy / namespace isolation — post-v1 (TODOS.md P2)
- Session windows — only sliding/tumbling in v1

## Context

- Greenfield Rust project, no existing code
- Formerly named "Streamlet" — renaming to "Tally" (per TODOS.md)
- CLAUDE.md contains the full design spec: architecture, protocol wire format, state structures, operator details, benchmark targets
- Single-threaded event loop (tokio), Redis-inspired architecture
- Custom binary TCP protocol for hot path; HTTP only for management
- All state in-memory with periodic snapshot to disk for crash recovery

## Constraints

- **Language**: Rust — memory safety, single binary distribution, performance
- **Threading**: Single-threaded core (v1) — simplicity, no locks, no contention
- **Protocol**: Custom binary TCP — HTTP too heavy for hot-path latency targets
- **Persistence**: Periodic snapshots only — no WAL, no embedded KV, losing ~30s on crash is acceptable
- **Performance**: <100us p99 PUSH latency, <50us p99 GET latency, >100K events/sec throughput

## Key Decisions

| Decision | Rationale | Outcome |
|----------|-----------|---------|
| Rust for implementation | Memory safety + single binary + performance for "zero ops" promise | — Pending |
| Single-threaded v1 | Keys are independent; key-partitioned sharding is drop-in upgrade later | — Pending |
| In-memory HashMap for state | Fastest possible; embedded KV adds latency for durability we don't need | — Pending |
| Custom binary TCP protocol | HTTP too heavy for hot path; persistent connections, minimal framing | — Pending |
| String-based expression language | Keeps Python out of hot path; tiny parser covers 95% of use cases | — Pending |
| HyperLogLog for distinct_count | Bounded memory per key (~12KB); well-understood error bounds | — Pending |
| Rename to Tally | Approved during design review | — Pending |

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
*Last updated: 2026-04-09 after milestone v1.0 initialization*
