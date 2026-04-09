# Project Retrospective

*A living document updated after each milestone. Lessons feed forward into future planning.*

## Milestone: v1.0 — Core Feature Server

**Shipped:** 2026-04-09
**Phases:** 5 | **Plans:** 19 | **Tasks:** 36

### What Was Built
- Complete real-time feature server in Rust with 8 streaming operators (count, sum, avg, min, max, last, distinct_count, derive) plus lookups
- Custom binary TCP protocol with synchronous push-through — push event, get features in the same response
- Python SDK with declarative @st.stream/@st.view decorators and typed feature results
- Postcard snapshot persistence with crash recovery, TTL eviction, and HTTP management API
- HyperLogLog distinct_count with epoch-based windowed rotation, where-clause filtering, cross-stream views, cross-key lookups, event fan-out

### What Worked
- Contract-first approach with TDD — defining types and tests before implementation kept each phase clean
- Strict phase dependency ordering (engine → server → SDK → persistence → advanced) prevented rework
- Operator trait abstraction allowed easy addition of min/max/last/distinct_count in Phase 5 without engine changes
- Gap closure plans (Phase 2) caught edge cases that would have been bugs in production
- Single-day execution: all 5 phases completed in one session with ~81 minutes total execution time

### What Was Inefficient
- ROADMAP.md progress table never updated during execution (still showed "0/4 plans" after completion)
- Phase 1 research was heavier than needed for a greenfield project with a detailed CLAUDE.md spec
- Some decisions were logged per-plan in STATE.md that could have been phase-level summaries

### Patterns Established
- RingBuffer<T> with head pointer for all windowed operators — cache-friendly, fixed-size, reusable
- OperatorState enum (not trait objects) for serialization compatibility
- Pre-bound listener pattern for test isolation with random ports
- Session-scoped server fixtures with unique entity keys for Python integration tests
- guard_float() defense-in-depth: all f64 results checked for NaN/infinity → Missing

### Key Lessons
1. Postcard over bincode for serialization — bincode has active RUSTSEC advisory, postcard is better maintained
2. AHashMap from day one — SipHash overhead measurable at target throughput
3. SystemTime (not Instant) for window buckets when clients supply timestamps
4. Clone bound (not Copy) on RingBuffer generic — needed for complex bucket types like MinBucket/MaxBucket
5. Raw register JSON must be stored and re-stored after snapshot restore for pipeline persistence across restarts

### Cost Observations
- 19 plans across 5 phases, ~81 minutes total execution
- Average ~4.3 minutes per plan
- Phase 5 (advanced operators) took longest (~22min) due to HLL complexity and cross-stream wiring
- Phase 2 (TCP server) had most plans (5) due to gap closure additions

---

## Cross-Milestone Trends

| Metric | v1.0 |
|--------|------|
| Phases | 5 |
| Plans | 19 |
| Tasks | 36 |
| Avg min/plan | ~4.3 |
| LOC (Rust) | 9,904 |
| LOC (Python) | 2,915 |
| Commits | 132 |
| Gap closures | 2 (Phase 2) |
