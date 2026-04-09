# Phase 1: Core Engine - Context

**Gathered:** 2026-04-09
**Status:** Ready for planning

<domain>
## Phase Boundary

Build the foundational engine: in-memory state store (HashMap<EntityKey, EntityState>), windowed aggregation via bucketed ring buffers, core operators (count, sum, avg), and expression evaluator — fully functional and unit-tested without any networking. This is the compute core that all later phases build on.

</domain>

<decisions>
## Implementation Decisions

### Window & Bucket Defaults
- Uniform 1-minute bucket granularity as default for all windows (30m = 30 buckets, 24h = 1440 buckets)
- Bucket granularity is configurable per-operator, with global default fallback
- Non-divisible window durations: round up bucket count
- No minimum window duration enforced — any duration >= 1 bucket size is valid
- Multi-tier buckets deferred to v2

### Value Types & Missing Semantics (Redis-Strict)
- FeatureValue variants: Float(f64), Int(i64), String(String), Missing
- Redis-strict type enforcement: errors on type violations (e.g. string field in sum → push error), not silent Missing
- Fields used by operators are implicitly typed by the operator: sum("amount") means "amount" must be numeric when present
- optional=True flag on operators: absent field produces Missing without error. Without optional=True, absent field → error on push
- count(window=...) needs no field — always succeeds regardless of event shape
- last("field") accepts any type — no numeric requirement
- No implicit type coercion beyond Int+Float→Float in arithmetic expressions
- Division-by-zero → Missing (value-level concern, not type error)
- Zero events in window → Missing (no events means no value, not 0)

### Project Skeleton & Module Organization
- Single crate for v1 — one binary, one test suite. Extract to workspace when Python FFI needs it
- Integration tests in tests/ dir, unit tests inline with #[cfg(test)] mod tests
- Single TallyError enum with thiserror — variants for Parse, Type, Window, Expression, Protocol
- "Tally" naming everywhere from day one (not "Streamlet") per approved rename decision

### Claude's Discretion
- Exact Rust struct layouts and field naming within the patterns established by CLAUDE.md
- Ring buffer implementation details (VecDeque vs fixed array vs custom)
- Expression AST node structure
- Test fixture design and helper utilities

</decisions>

<code_context>
## Existing Code Insights

### Reusable Assets
- Greenfield project — no existing code to reuse

### Established Patterns
- CLAUDE.md specifies: AHashMap (not std HashMap), SystemTime (not Instant), postcard for serialization, winnow for expression parsing
- STATE.md logs these as init decisions with rationale

### Integration Points
- Phase 2 will wrap this engine behind a TCP server — engine API must be clean and synchronous
- Pipeline registration (JSON → engine types) will be needed in Phase 2's REGISTER command

</code_context>

<specifics>
## Specific Ideas

- Redis-inspired strict type system: errors on type violations, explicit optional fields
- Memory target: <5KB average per key with 10 mixed operators across 30m/1h/24h windows
- Performance: pure engine operations (no I/O) should be sub-microsecond for single-event processing

</specifics>

<deferred>
## Deferred Ideas

- Multi-tier buckets (fine-grained recent + coarse older) — v2 optimization
- Schema evolution (add/remove features without reset) — post-v1

</deferred>
