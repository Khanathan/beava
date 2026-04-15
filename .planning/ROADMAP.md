# Tally Roadmap

## Milestones

- [x] **v1.0 -- Foundation** (Phases 1-5) -- Complete 2026-04-09 -- archived
- [x] **v1.1–v1.3 -- Event Log, Pipelines, Concurrency** (Phases 6-15) -- Complete 2026-04-12
- [x] **v2.0 -- API & Engine** (Phases 16-19) -- Complete 2026-04-13 -- `.planning/milestones/v2.0-ROADMAP.md`
- [x] **v2.1 -- Launch** (Phase 20) -- Engineering complete 2026-04-14 (live-run ops pending, calendar-gated) -- `.planning/milestones/v2.1-ROADMAP.md`
- [ ] **v0 -- Restructure + Local Replica** (Phases 21-34) -- Active. Phases 21-26 shipped 2026-04-14 (restructure); Phases 27-34 up next (scope-aware local replica). Restructure archive: `.planning/milestones/v0-ROADMAP.md`

## Phases

### v0 Restructure (Phases 21-26 — Complete 2026-04-14)

**Outcome:** Two-type (Stream + Table) API, DataFrame-parity operators, hybrid sketches (UDDSketch / CMS+heap / HLL), 5-second fixed event-time watermarks with γ propagation, per-Table row storage with 7d tombstone grace, unified `/debug/warnings` + `tally suggest-config`, zero-old-API codebase, 9-cell benchmark matrix within −5% of v2.0 BASELINE (worst cell −4.84%), launch blog rewritten honestly. **All 11 sign-off criteria green** — see `.planning/phases/26-test-migration-bench-docs-demo/26-SIGNOFF.md`. Full phase list + plan history archived in `.planning/milestones/v0-ROADMAP.md`.

### v0 Local Replica (Phases 27-34 — Active)

**Goal:** Ship a read-only mini-Tally client that dependency-scoped-pulls a slice of the cluster's state (only the streams/keys the user's pipeline declares), catches up through the event log, and then either stops (historical mode) or keeps subscribing (streaming mode). Same engine code, no write path. Delivers the "fork prod to laptop" DX — `tally clone prod-cluster:6400 --streams Transactions,Logins` produces a queryable local replica sized to what the user actually needs.

**Load-bearing design choice: scope-driven, NOT whole-cluster.** A 100M-key cluster would never fit on a laptop; but "transactions for 10K specific users over the last 30 days" does. Client declares its scope (streams, optional key set/prefix), cluster filters everything (snapshot, log, subscribe) to that scope. Out-of-scope queries return a clear error, never a silent null. Predicate-level scoping (`balance > 1000`) deferred later.

Full design: `.planning/research/local-replica-design.md`. User-facing API is the `tally.Pipeline(remote=..., streams=..., keys?=..., mode="historical"|"streaming", since=...)` Python class + `tally clone` / `tally sync` CLI.

Sequencing: 27 → 31 is core, 32-34 is stretch.

- [ ] **Phase 27: Server-side replica endpoints (scope-aware)** — `OP_SNAPSHOT_FETCH{scope}` + `OP_LOG_FETCH{from, scope}` + full `OP_SUBSCRIBE{scope}` (reserved in v0 25-01); per-connection subscription cursor + scope; admin-token auth; scope-filter hot path uses per-stream log files (Phase 6) for cheap stream filtering.
- [ ] **Phase 28: Client engine embedding** — extract `tally-core` crate (or feature-flag the engine crate); client-side `StateStore` + `PipelineEngine::apply_event` with no listeners; stub `tally` CLI with `clone`/`sync` subcommands.
- [ ] **Phase 29: Session manager + log consumer + dependency analyzer (historical mode end-to-end)** — walks client pipeline DAG to derive scope automatically; persistent TCP session with reconnect re-sending scope; log consumer calling `apply_event` per entry; mode state machine (`bootstrap → catchup → done`); out-of-scope queries raise `OutOfScopeError`. Historical mode ships here: `tally clone <remote>` produces a frozen local replica scoped to declared dependencies.
- [ ] **Phase 30: Python Pipeline API + local query surface** — `tally.Pipeline(...)` class via PyO3 or subprocess bridge; `.run() / .get() / .inspect()` methods; CLI `tally query` / `tally inspect`.
- [ ] **Phase 31: Streaming mode + watch** — upgrade catchup to SUBSCRIBE on the same connection; `pipe.watch(key)` diff-emits on every apply; downgrade (drop sub, keep state).
- [ ] **Phase 32: Mode switching + resume (stretch)** — persist client's last-applied seq; reconnect-and-resume; on-the-fly historical → streaming upgrade.
- [ ] **Phase 33: Upstream backfill sources (stretch)** — `source="s3://..."` / `source="snowflake://..."` instead of `remote=`; reuses `BackfillSource` trait reserved in v0 Phase 22.
- [ ] **Phase 34: Write-back / promote flow (stretch)** — register a feature on the client, compute locally against the stream, `.promote()` to the cluster. "Shadow replica + promote" DX loop.

Open questions (from design doc §Open design questions): engine crate split depth, PyO3-vs-subprocess, snapshot chunking shape, read-only vs admin token for SUBSCRIBE, write-back conflict resolution, SUBSCRIBE backpressure policy.

## Phase Details

### Phase 20: Traction Demo
**Goal**: Public-facing, read-only web demo showcasing Tally running live for 5 days post-launch, with a historical-replay benchmark that ingests the last 30 days of data on startup and records wall-clock throughput — surfaced as a headline number in the launch blog post.
**Depends on**: Phase 19 (v2.0 complete)
**Status**: Engineering complete 2026-04-14. All 3 plans shipped; ported to v0 SDK under Phase 26-03. Deploy artifacts (`tally.service`, `Caddyfile`, `provision.sh`, `smoke.sh`, `deploy/README.md`) clean-diff and API-agnostic. Remaining work is human ops only: Hetzner VM provision (~15 min) + 5-day live observation (calendar-gated). Runbook: `.planning/phases/26-test-migration-bench-docs-demo/26-04-SUMMARY.md § Resuming v2.1 Launch`.
**Plans:** 3/3 plans complete
  - [x] 20-01-PLAN.md — Deterministic 30-day replay CLI + multi-process push_many driver + integration test at 100k-event scale
  - [x] 20-02-PLAN.md — `/public/*` read-only HTTP surface + loopback-or-token admin middleware + extended `/metrics` + vanilla-JS demo.html
  - [x] 20-03-PLAN.md — Hetzner CX22 deploy (systemd + Caddyfile + provision.sh) + smoke script + blog headline + LIVE_SIGNOFF framework (artifacts shipped; VM provision + 5-day live run still pending as ops gate)

### Phase 27: Server-side replica endpoints (scope-aware)
**Goal**: Land `OP_SNAPSHOT_FETCH{scope}`, `OP_LOG_FETCH{from, scope}`, and full `OP_SUBSCRIBE{scope}` on the server side. Per-connection subscription cursor + scope; admin-token auth; scope-filter hot path uses per-stream log files (Phase 6) for cheap stream filtering.
**Depends on**: Phase 25 (reserved SUBSCRIBE opcode), Phase 6 (per-stream log files)
**Plans:** 3 plans

Plans:
- [ ] 27-01-PLAN.md — OP_SNAPSHOT_FETCH (0x12) + Scope struct + wire codec + scope validator + streaming filter-iterator on SnapshotReader
- [ ] 27-02-PLAN.md — OP_LOG_FETCH (0x13) + per-stream log filter-iterator + Python asyncio end-to-end clone-then-catchup test
- [ ] 27-03-PLAN.md — OP_SUBSCRIBE (0x11) + SubscriberRegistry (DashMap) + ingest-path notify hook + 10k backpressure drop + metrics + signals

### Phase 28: Client engine embedding
**Goal**: Extract `tally-core` crate (or feature-flag the engine crate); client-side `StateStore` + `PipelineEngine::apply_event` with no listeners; stub `tally` CLI with `clone`/`sync` subcommands.
**Depends on**: Phase 27
**Plans**: 0/? — not planned yet

### Phase 29: Session manager + log consumer + dependency analyzer (historical mode end-to-end)
**Goal**: Walk the client pipeline DAG to derive scope automatically; persistent TCP session with reconnect re-sending scope; log consumer calling `apply_event` per entry; mode state machine (`bootstrap → catchup → done`); out-of-scope queries raise `OutOfScopeError`. Historical mode ships here: `tally clone <remote>` produces a frozen local replica scoped to declared dependencies.
**Depends on**: Phase 27, Phase 28
**Plans**: 0/? — not planned yet

### Phase 30: Python Pipeline API + local query surface
**Goal**: Ship `tally.Pipeline(remote=..., streams=..., keys?=..., mode="historical").run(); .get(key, stream=...); .inspect()` via PyO3/maturin (Linux x86_64 wheel, Python >=3.10), plus `tally query` / `tally inspect` CLI subcommands wrapping the same Rust Session + StateStore primitives. Typed error hierarchy (TallyError + OutOfScopeError / ClientConnectError / HandshakeError / ReplicaStateError), hand-written .pyi stubs, E2E pytest against a real server, CI wheel-build job.
**Depends on**: Phase 29
**Plans:** 2 plans

Plans:
- [ ] 30-01-PLAN.md — New `python-native/` cdylib crate + maturin pyproject; PyO3 `Pipeline` class with `__init__`/`.run()` (releasing GIL)/`.get()`/`.inspect()`; typed exception hierarchy; `.pyi` stubs; unit tests for construction validation + error mapping; CI job building the wheel and running unit tests.
- [ ] 30-02-PLAN.md — `tally query` / `tally inspect` CLI subcommands in `src/bin/tally_cli.rs` (pure Rust, no Python dep); E2E pytest spinning up a real server, pushing fixture events with the existing Python SDK, asserting `Pipeline.run/get/inspect` behavior + `OutOfScopeError` + CLI subprocess coverage; extend CI to run E2E suite.

### Phase 31: Streaming mode + watch
**Goal**: Upgrade catchup to SUBSCRIBE on the same connection; `pipe.watch(key)` diff-emits on every apply; downgrade path (drop sub, keep state). Wire `tally sync` CLI to emit NDJSON until Ctrl-C.
**Depends on**: Phase 27 (OP_SUBSCRIBE + SubscriberRegistry), Phase 28 (client feature), Phase 29 (Session state machine), Phase 30 (PyO3 Pipeline)
**Plans:** 2 plans

Plans:
- [ ] 31-01-PLAN.md — Client SUBSCRIBE upgrade: Session Streaming/Stopped states, OP_SUBSCRIBE on same socket after LOG_FETCH tail, bg apply thread, parking_lot::RwLock on client StateStore (streaming only), idempotent .stop(), transition-race + server-drop integration tests
- [ ] 31-02-PLAN.md — PyO3 .watch() generator (GIL-released recv) + streaming-mode .run() + .stop() + __del__ + SubscriberDroppedError; `tally sync` CLI (NDJSON stdout, Ctrl-C → exit 130); Python E2E pytest with mid-stream push

### Phase 32: Mode switching + resume (stretch)
**Goal**: Persist client's last-applied seq; reconnect-and-resume; on-the-fly historical → streaming upgrade.
**Depends on**: Phase 31
**Plans**: 0/? — not planned yet

### Phase 33: Upstream backfill sources (stretch)
**Goal**: `source="s3://..."` / `source="snowflake://..."` instead of `remote=`; reuses `BackfillSource` trait reserved in v0 Phase 22.
**Depends on**: Phase 29
**Plans**: 0/? — not planned yet

### Phase 34: Write-back / promote flow (stretch)
**Goal**: Register a feature on the client, compute locally against the stream, `.promote()` to the cluster. "Shadow replica + promote" DX loop.
**Depends on**: Phase 30, Phase 31
**Plans**: 0/? — not planned yet

## Progress

**Execution Order:** v0 Restructure (Phases 21-26) complete; v2.1 Launch (Phase 20) engineering complete, deploy ops pending async; v0 Local Replica (Phases 27-34) up next — start with Phase 27.

| Phase | Milestone | Plans Complete | Status | Completed |
|-------|-----------|----------------|--------|-----------|
| 20. Traction Demo | v2.1 | 3/3 | Engineering complete; live-run ops pending | 2026-04-14 (eng) |
| 21. Type system & SDK skeleton | v0 | 3/3 | Complete | 2026-04-14 |
| 22. Stream aggregation engine | v0 | 4/4 | Complete | 2026-04-14 |
| 23. Joins | v0 | 3/3 | Complete | 2026-04-14 |
| 24. Table storage + Watermarks & event-time | v0 | 5/5 | Complete | 2026-04-14 |
| 25. Query surface, TTL, warnings | v0 | 3/3 | Complete | 2026-04-14 |
| 26. Test migration, bench, docs, demo | v0 | 4/4 | Complete | 2026-04-14 |
| 27. Server-side replica endpoints | v0 | 0/? | Not planned | — |
| 28. Client engine embedding | v0 | 0/? | Not planned | — |
| 29. Session manager + log consumer (historical) | v0 | 0/? | Not planned | — |
| 30. Python Pipeline API + local query surface | v0 | 0/2 | Planned | — |
| 31. Streaming mode + watch | v0 | 0/? | Not planned | — |
| 32. Mode switching + resume (stretch) | v0 | 0/? | Not planned | — |
| 33. Upstream backfill sources (stretch) | v0 | 0/? | Not planned | — |
| 34. Write-back / promote flow (stretch) | v0 | 0/? | Not planned | — |
