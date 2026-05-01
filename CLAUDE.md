<!-- GSD:project-start source:PROJECT.md -->
## Project

**Beava v2**

Beava is a single-binary real-time feature server for fraud, ad-tech, and behavioral analytics. Push events in over HTTP, beava tracks per-entity features (counters, velocities, distances, rates, distributions) updated atomically on every event, and your application queries them via HTTP to power live scoring rules. Think "Redis for stateful streaming features," with 40+ purpose-built aggregation primitives instead of do-it-yourself Lua scripts.

**Core Value:** **Declare a feature, push events, query it — in under 10 minutes, with curl alone.** Every architectural and product choice serves this: HTTP-first API, JSON-declarative feature registration, zero SDK requirement, single binary, in-memory state, batch lookup for sub-millisecond fraud/feature-serving decisions.

### Constraints

- **Tech stack**: Rust server (ownership + perf), HTTP API (axum) + custom-framed TCP fast-path, Python SDK (sync + fire-and-forget) over either transport. No external storage dependencies (RocksDB, fjall removed).
- **Architecture**: Single process, single thread for event processing. In-memory state. WAL + periodic snapshot for durability. No cross-process coordination.
- **Performance**: ≥3M events/sec/core sustained on typical fraud-shape workloads; P99 batch-get < 10ms.
- **Memory**: No SSD overflow. Users must size their box. Budget: ~7KB per entity for a rich 30-feature pack → ~700GB for 100M entities.
- **Compatibility**: HTTP/1.1 + JSON for curl/LB/WAF reach. Custom framed TCP `[u32 length][u16 op][u8 content_type][payload]` for low-latency fast-path — Redis-style strict-FIFO correlation on a connection (no request_id). No Protobuf.
- **Licensing**: Apache 2.0 OSS for v0. Commercial-tier (HA, replicas, cross-region) is explicitly out of v0 scope.
- **Timeline**: v0 target is weeks, not months — aiming for engineering-complete in ~6-10 weeks from Phase 1 kickoff.
<!-- GSD:project-end -->

<!-- GSD:stack-start source:STACK.md -->
## Technology Stack

Technology stack not yet documented. Will populate after codebase mapping or first phase.
<!-- GSD:stack-end -->

<!-- GSD:conventions-start source:CONVENTIONS.md -->
## Conventions

### TDD Discipline (strict red-green-refactor, enforced Phase 3 onward)

**Rule:** Every plan task produces at least two atomic commits — the red commit (failing test) lands FIRST, then the green commit (implementation) makes it pass.

**Why:** Writing the test first forces you to encode the contract as executable before writing code that could cheat toward its own shape. Catches impl-first drift that makes tests rubber-stamps of whatever got written. Phases 1–2.5 predate this rule and are grandfathered; every phase from 3 onward follows it.

**How to apply:**

1. **Plan documents** (`NN-MM-PLAN.md`) — every task decomposes into two subtasks:
   - `Task N.a (red)` — write the failing test(s). Run the test suite to confirm RED. Commit with `test(<phase>-<plan>): <subject>` message.
   - `Task N.b (green)` — implement until the test passes. Refactor freely inside this subtask as long as the test stays green. Commit with `feat(<phase>-<plan>): <subject>` or `chore(...)` / `refactor(...)` as appropriate.

2. **Commit messages** follow conventional-commits `type(scope): subject` — `test:`, `feat:`, `fix:`, `refactor:`, `chore:`, `docs:`. `test:` commits are expected to precede `feat:` commits that implement the same thing.

3. **Proptests count as the red test** — if a plan's assertion is property-based (e.g. "frame codec round-trips"), the proptest itself is the red-commit contract; the impl commit makes the proptest green.

4. **Smoke / acceptance tests** can be written after all sub-features land, but each sub-feature must have its own unit-test red-green inside the plan. The phase-level smoke test is additive assurance, not a replacement for per-task tests.

5. **Gates that block merge** (enforced via `cargo test --workspace` + `cargo clippy --workspace --all-targets --all-features -- -D warnings` + `cargo fmt --all --check`) run on every commit. A `test:` commit where the test passes without the impl is a bug in the test (the test isn't actually red) — reject it and rewrite the test so it fails without the impl.

6. **Executor agents** are prompted with this rule and must structure their work red-then-green per task.

**Grandfathered exceptions:**
- Phase 1 (Foundation), Phase 2 (Registry), Phase 2.5 (TCP wire) — written and partially executed under the prior test-coupled convention. Still ship with `cargo test` green; just lack the red-then-green commit trace.

**Validation recipe (reviewer sanity check):**
```bash
# For the most recent phase's commits, every feat: should have a preceding test:
# touching the same plan/scope.
git log --format='%s' <base>..<head> | grep -E '^(test|feat|fix|refactor):'
```

### Performance Discipline (regression gates, enforced Phase 6 onward)

**Rule:** Every phase from Phase 6 onward ships at least one `criterion` microbench covering its hot path. Baselines live in `.planning/perf-baselines.md` keyed by hw-class. Verification compares current numbers to the same hw-class's prior baseline:

- **10% slower** than prior baseline → WARNING in the phase's VERIFICATION.md (must be investigated before Phase 13)
- **25% slower** than prior baseline → BLOCKER (phase verification fails; must fix or explicit override)

**Scope:** measurement, not optimization. Phase 13 is the final ship gate (≥3M EPS/core, P99 < 10ms batch-get). Phase 5.5's benches are a regression tripwire, not a target.

**Plan-checker contract:** plans for Phase 6+ must include at least one task whose `files_modified` contains a path under `crates/*/benches/` (or language-equivalent). Plans that don't are rejected.

**Do NOT bench:** cold-path code (register compile, schema propagation at register-time), I/O paths that will change (WAL pre-Phase 6, snapshot pre-Phase 7), end-to-end network roundtrips (that's Phase 13).

**Retroactive coverage landed in Phase 5.5:** Phase 2.5 frame codec, Phase 3 SDK REGISTER compile (Python), Phase 4 expr parse/eval/op-chain, Phase 5 AggOp::update / WindowedOp fold / apply_event_to_aggregations.

**End-to-end throughput regression contract (added Phase 7.5, enforced Phase 8 onward):** Every phase from Phase 8 onward MUST include a **throughput run** task that re-runs `crates/beava-bench` against the small/medium/large pipelines (plus any phase-specific operator-family variant) and appends a row per `(pipeline-shape, transport)` tuple to `.planning/throughput-baselines.md`. Same 10% warn / 25% block thresholds apply, measured against the **simple-fraud (small) shape** of the most recent prior baseline in the same hw-class. Plan-checker contract: a Phase 8+ plan without a "throughput run" task whose `files_modified` includes `.planning/throughput-baselines.md` is rejected. The harness lives at `crates/beava-bench/`; baselines are NOT a substitute for the per-phase criterion microbench (`perf-baselines.md`) — both gates apply.

### mio-only Hot-Path Invariant (locked Phase 12.6)

**Rule:** The mio event loop is the only data-plane runtime. All push/get/upsert/delete/retract dispatches go through `crates/beava-server/src/apply_shard.rs::dispatch_*_sync` (specifically `dispatch_one` → `dispatch_push_sync`). The only legitimate callers of `apply_event_to_aggregations` are `apply_shard.rs` (mio data plane) and `recovery.rs` (cold-path WAL replay on boot). axum is restricted to the ServerV18 admin sidecar in `crates/beava-server/src/http_admin.rs::BoundAdminServer`, bound on a separate port (`cfg.admin_addr`) for `/health`, `/ready`, `/metrics`, `/registry` only.

**Why:** Locked architectural commitments — `project_phase18_no_dual_runtime` (single hot-path entry; admin endpoints stay on tokio) + `project_redis_shaped_no_event_time_ever` (Redis-shaped, processing-time only, no dual stack). New code MUST NOT introduce a parallel data-plane runtime, a third caller of `apply_event_to_aggregations`, or `axum::*` symbols (Router/Json/Extension/extract/routing/http/body/response/middleware) outside `http_admin.rs`.

**How enforced:** `crates/beava-server/tests/phase12_6_mio_only_dataplane.rs` walks the workspace at test runtime and fails if either invariant is violated. CI runs the test on every PR via `cargo test --workspace`. Companion: `crates/beava-server/tests/phase12_6_legacy_axum_killed.rs` (Plan 12.6-07) asserts the legacy axum files and symbols stay deleted.

**Reviving a second data-plane runtime requires:** explicit user override + new ADR overturning `project_phase18_no_dual_runtime`.

### Events-Only Invariant (locked Phase 12.7)

**Rule:** Beava v0 ships events-only. The public `bv` namespace exposes `@bv.event` and the operator catalogue; `@bv.table`, `app.upsert`, `app.delete`, `app.retract`, `OpNode::Table*`, `RecordType::TableUpsert/TableDelete/Retract`, `TemporalStore`, `MvccVersion`, `temporal_http`, `push_table`, `delete_table` are all gone. Register payloads with `{"kind": "table", ...}` are rejected at the JSON-prelude layer with structured error code `unsupported_node_kind`. Wire-format reset to version=1 (WAL `FORMAT_VERSION = 1`, snapshot `SNAPSHOT_FORMAT_VERSION = 1`, snapshot body `SNAPSHOT_BODY_FORMAT_VERSION = 1`); pre-12.7 dev WALs / snapshots fail at the version-byte check and require operators to clear `.beava/wal` + `.beava/snapshots` before boot.

**Why:** Locked architectural commitments — `project_v0_events_only_scope` (locked 2026-04-30): v0 strips tables / table-aggregation / session windows / `bv.fork` / playground. Tables/joins/aggregation return together in v0.1+ if/when justified by demand. Combined with `project_redis_shaped_no_event_time_ever`, v0's surface is push events / register events+derivations / get features.

**How enforced:** `crates/beava-server/tests/phase12_7_no_table_surface.rs` walks the workspace at test runtime and fails if any forbidden symbol resurfaces (`OpNode::Table*`, `TemporalStore`, `MvccVersion`, `RecordType::TableUpsert/TableDelete/Retract`, `WireRequest::HttpUpsert/HttpDelete/HttpRetract/HttpTableGet`, `Route::Upsert/Delete/Retract/TableGet`, `temporal_http`, `push_table`, `delete_table`, `fn retract(`). CI runs the test on every PR via `cargo test --workspace`. Companion: `crates/beava-server/tests/phase12_7_legacy_table_handlers_killed.rs` asserts the deleted files (`temporal_http.rs`, `temporal.rs`, `python/beava/_tables.py`) stay deleted.

**Reviving `@bv.table` requires:** explicit user override + new ADR overturning `project_v0_events_only_scope`.
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

**TDD discipline is MANDATORY from Phase 3 onward.** See §Conventions → TDD Discipline above. Every plan task splits into a red commit (failing test) followed by a green commit (impl). Executors must follow this; plan documents must structure tasks this way; code reviewers should reject commit sequences that show impl-first.
<!-- GSD:workflow-end -->



<!-- GSD:profile-start -->
## Developer Profile

> Profile not yet configured. Run `/gsd-profile-user` to generate your developer profile.
> This section is managed by `generate-claude-profile` -- do not edit manually.
<!-- GSD:profile-end -->
