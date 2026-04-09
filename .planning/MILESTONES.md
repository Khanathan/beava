# Milestones

## v1.0 Core Feature Server (Shipped: 2026-04-09)

**Phases:** 5 | **Plans:** 19 | **Tasks:** 36
**Lines of Code:** 9,904 Rust + 2,915 Python (~12,800 total)
**Commits:** 132

**Key Accomplishments:**

1. In-memory state store with AHashMap, time-bucketed ring buffer aggregation engine (count, sum, avg, min, max, distinct_count, last), and winnow Pratt expression evaluator with Missing propagation
2. Tokio TCP server with custom binary protocol (PUSH, GET, SET, MSET, REGISTER), synchronous push-through, and MSET cooperative yielding
3. Python SDK with @st.stream/@st.view decorators, 9 operator descriptor classes, TCP client with auto-reconnect, and typed FeatureResult
4. Postcard snapshot persistence with crash recovery, TTL eviction, and HTTP management API (pipeline CRUD, Prometheus metrics, debug endpoints)
5. HyperLogLog distinct_count with epoch-based windowed rotation, where-clause filtering, cross-stream views, cross-key lookups, and event fan-out

**Delivered:** A complete, single-binary real-time feature server — push events over TCP, get updated streaming features back synchronously with sub-millisecond latency and zero external dependencies.

**Archive:** [v1.0-ROADMAP.md](milestones/v1.0-ROADMAP.md) | [v1.0-REQUIREMENTS.md](milestones/v1.0-REQUIREMENTS.md) | [v1.0-MILESTONE-AUDIT.md](milestones/v1.0-MILESTONE-AUDIT.md)
