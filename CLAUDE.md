<!-- GSD:project-start source:PROJECT.md -->
## Project

**Beava v2**

Beava is a single-binary real-time feature server for fraud, ad-tech, and behavioral analytics. Push events in over HTTP, beava tracks per-entity features (counters, velocities, distances, rates, distributions) updated atomically on every event, and your application queries them via HTTP to power live scoring rules. Think "Redis for stateful streaming features," with 40+ purpose-built aggregation primitives instead of do-it-yourself Lua scripts.

**Core Value:** **Declare a feature, push events, query it — in under 10 minutes, with curl alone.** Every architectural and product choice serves this: HTTP-first API, JSON-declarative feature registration, zero SDK requirement, single binary, in-memory state, batch lookup for sub-millisecond fraud/feature-serving decisions.

### Constraints

- **Tech stack**: Rust server (ownership + perf), HTTP API (axum or actix), Python SDK (sync + fire-and-forget only, HTTP-backed). No external storage dependencies (RocksDB, fjall removed).
- **Architecture**: Single process, single thread for event processing. In-memory state. WAL + periodic snapshot for durability. No cross-process coordination.
- **Performance**: ≥3M events/sec/core sustained on typical fraud-shape workloads; P99 batch-get < 10ms.
- **Memory**: No SSD overflow. Users must size their box. Budget: ~7KB per entity for a rich 30-feature pack → ~700GB for 100M entities.
- **Compatibility**: HTTP/1.1 minimum; JSON request/response only in v0. No Protobuf, no TCP binary in OSS.
- **Licensing**: Apache 2.0 OSS for v0. Commercial-tier (HA, replicas, cross-region) is explicitly out of v0 scope.
- **Timeline**: v0 target is weeks, not months — aiming for engineering-complete in ~6-10 weeks from Phase 1 kickoff.
<!-- GSD:project-end -->

<!-- GSD:stack-start source:STACK.md -->
## Technology Stack

Technology stack not yet documented. Will populate after codebase mapping or first phase.
<!-- GSD:stack-end -->

<!-- GSD:conventions-start source:CONVENTIONS.md -->
## Conventions

Conventions not yet established. Will populate as patterns emerge during development.
<!-- GSD:conventions-end -->

<!-- GSD:architecture-start source:ARCHITECTURE.md -->
## Architecture

Architecture not yet mapped. Follow existing patterns found in the codebase.
<!-- GSD:architecture-end -->

<!-- GSD:skills-start source:skills/ -->
## Project Skills

| Skill | Description | Path |
|-------|-------------|------|
| beava | \| Guided setup and pipeline builder for Beava real-time feature server. Walks through setup, pipeline design, feature writing, test data, benchmarking, live debugging, memory planning, and capacity estimation. Type /beava to start. Proactively invoke when user asks about getting started, building pipelines, adding features, testing, memory usage, scaling, debugging a running Beava, or capacity planning. | `.agents/skills/beava/SKILL.md` |
<!-- GSD:skills-end -->

<!-- GSD:workflow-start source:GSD defaults -->
## GSD Workflow Enforcement

Before using Edit, Write, or other file-changing tools, start work through a GSD command so planning artifacts and execution context stay in sync.

Use these entry points:
- `/gsd-quick` for small fixes, doc updates, and ad-hoc tasks
- `/gsd-debug` for investigation and bug fixing
- `/gsd-execute-phase` for planned phase work

Do not make direct repo edits outside a GSD workflow unless the user explicitly asks to bypass it.
<!-- GSD:workflow-end -->



<!-- GSD:profile-start -->
## Developer Profile

> Profile not yet configured. Run `/gsd-profile-user` to generate your developer profile.
> This section is managed by `generate-claude-profile` -- do not edit manually.
<!-- GSD:profile-end -->
