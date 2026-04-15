# Tally Roadmap

## Milestones

- [x] **v1.0 -- Foundation** (Phases 1-5) -- Complete 2026-04-09 -- archived
- [x] **v1.1–v1.3 -- Event Log, Pipelines, Concurrency** (Phases 6-15) -- Complete 2026-04-12
- [x] **v2.0 -- API & Engine** (Phases 16-19) -- Complete 2026-04-13 -- `.planning/milestones/v2.0-ROADMAP.md`
- [x] **v2.1 -- Launch** (Phase 20) -- Engineering complete 2026-04-14 (live-run ops pending, calendar-gated) -- `.planning/milestones/v2.1-ROADMAP.md`
- [ ] **v0 -- Restructure + Data-Scientist Fork** (Phases 21-38) -- Active. Phases 21-26 shipped 2026-04-14 (restructure); Phase 27 shipped 2026-04-15 (server replica opcodes); Option K (phases 28/30/31) SUPERSEDED 2026-04-15 by Option M (phases 35-38) — local server in replica mode. Restructure archive: `.planning/milestones/v0-ROADMAP.md`.

## Phases

### v0 Restructure (Phases 21-26 — Complete 2026-04-14)

**Outcome:** Two-type (Stream + Table) API, DataFrame-parity operators, hybrid sketches (UDDSketch / CMS+heap / HLL), 5-second fixed event-time watermarks with γ propagation, per-Table row storage with 7d tombstone grace, unified `/debug/warnings` + `tally suggest-config`, zero-old-API codebase, 9-cell benchmark matrix within −5% of v2.0 BASELINE (worst cell −4.84%), launch blog rewritten honestly. **All 11 sign-off criteria green** — see `.planning/phases/26-test-migration-bench-docs-demo/26-SIGNOFF.md`. Full phase list + plan history archived in `.planning/milestones/v0-ROADMAP.md`.

### v0 Data-Scientist Fork — Option M (Phases 35-38 — Active)

**Adopted 2026-04-15** after user clarified the real product: data scientists fork a scoped CDC stream from prod to their laptop, register their **own** pipelines (different from prod's), run historical replay + live tail. Option K (embedded-engine client, phases 28/30/31) SUPERSEDED because it didn't deliver live-updated aggregates under scientist-defined pipelines.

**Architecture:** the replica IS a full Tally server process running in "replica mode". It pulls events from remote via `OP_LOG_FETCH` (historical) + `OP_SUBSCRIBE` (live), routes them through its own ingest path, persists to local per-stream log, runs scientist's registered pipelines, serves queries via normal HTTP/TCP. Scientist connects with `tl.Client(remote="localhost:7400")`.

**No snapshot seed in MVP** (user directive 2026-04-15): replay CDC from scratch is simpler and sufficient for demo.

**Load-bearing primitives:**
1. Scoped `OP_LOG_FETCH{from_ts_millis, scope}` — ships Phase 35.
2. Server `--replica-from` boot mode — ships Phase 36.
3. `tally fork` CLI + E2E demo — ships Phase 37.
4. Mothball Option K surfaces — Phase 38.

SUPERSEDED Option K phases preserved as historical record (SUMMARY files stay; CONTEXT files banner-tagged):
- [x] **Phase 27: Server-side replica endpoints (2 opcodes)** — `OP_SNAPSHOT_FETCH{scope}` (0x12) + `OP_SUBSCRIBE{scope}` (0x11). **STILL ACTIVE** — SUBSCRIBE is reused by Phase 36's replica client; SNAPSHOT_FETCH kept but unused by Option M MVP. (completed 2026-04-15)
- [SUPERSEDED] **Phase 28: Client engine embedding + historical clone** (28-01 crate features still active; 28-02/03/04 obsoleted). Mothballed by Phase 38.
- [SUPERSEDED] **Phase 30: Python Pipeline API** — PyO3 Pipeline obsoleted. Scientists use `tl.Client` HTTP SDK against `localhost:7400`. Mothballed by Phase 38.
- [SUPERSEDED] **Phase 31: Streaming mode + watch** — 31-01 committed but unused; 31-02 cancelled mid-flight. Mothballed by Phase 38.

**Option M phases:**
- [ ] **Phase 35: `OP_LOG_FETCH{from_ts, scope}`** — Server ships the historical-CDC opcode. Timestamp cursor, at-least-once on boundary, per-stream ordering. 1 plan.
- [x] **Phase 36: Replica-mode server boot** — `tally serve --replica-from HOST --replica-since T --replica-streams S --replica-token T [--replica-pipeline-file F]`. LOG_FETCH catchup → SUBSCRIBE live-tail, all feeding local ingest. Listener gate until catchup-done. 1 plan, 4 tasks. (completed 2026-04-15)
- [ ] **Phase 37: `tally fork` CLI + E2E demo** — `tally fork --remote ... --since ... --streams ... --pipeline-file ...` wrapper + load-bearing pytest that proves the scientist workflow end-to-end. 1 plan.
- [ ] **Phase 38: Mothball Option K surfaces** — Delete obsolete embedded-client code, `tally_cli clone/query/inspect/sync`, `python-native/` crate. Housekeeping. 1 plan.

**Stretch (unchanged):**
- [ ] **Phase 32: Resume across restarts (stretch)** — persist `last_applied_timestamp` to disk; replica re-runs from there.
- [ ] **Phase 33: Upstream backfill sources (stretch)** — `source="s3://..."` instead of `--replica-from`; reuses Phase 22 `BackfillSource` trait.
- [ ] **Phase 34: Write-back / promote (stretch)** — scientist's compute → promote to cluster.

~~Phase 29~~ (removed 2026-04-15, Option K era).

## Phase Details

### Phase 20: Traction Demo
**Goal**: Public-facing, read-only web demo showcasing Tally running live for 5 days post-launch, with a historical-replay benchmark that ingests the last 30 days of data on startup and records wall-clock throughput — surfaced as a headline number in the launch blog post.
**Depends on**: Phase 19 (v2.0 complete)
**Status**: Engineering complete 2026-04-14. All 3 plans shipped; ported to v0 SDK under Phase 26-03. Remaining work is human ops only: Hetzner VM provision + 5-day live observation (calendar-gated). Runbook: `.planning/phases/26-test-migration-bench-docs-demo/26-04-SUMMARY.md § Resuming v2.1 Launch`.
**Plans:** 3/3 plans complete
  - [x] 20-01-PLAN.md — Deterministic 30-day replay CLI + multi-process push_many driver + integration test at 100k-event scale
  - [x] 20-02-PLAN.md — `/public/*` read-only HTTP surface + loopback-or-token admin middleware + extended `/metrics` + vanilla-JS demo.html
  - [x] 20-03-PLAN.md — Hetzner CX22 deploy (systemd + Caddyfile + provision.sh) + smoke script + blog headline + LIVE_SIGNOFF framework

### Phase 27: Server-side replica endpoints (Option K — 2 opcodes)
**Goal**: Land `OP_SNAPSHOT_FETCH{scope}` (0x12) and `OP_SUBSCRIBE{scope}` (0x11) on the server. In-memory filter of `BaseSnapshotState` for SNAPSHOT_FETCH (no new reader API); `SubscriberRegistry` (`DashMap<conn_id, ReplicaSession>`) + ingest-path `notify_subscribers` hook for SUBSCRIBE with 10k bounded queue (drop on overflow). Admin-token auth reusing Phase 22 middleware. Response header carries `snapshot_taken_at: SystemTime` so the client can close the snapshot/subscribe gap in Phase 31.
**Depends on**: Phase 25 (reserved SUBSCRIBE opcode), Phase 22 (admin token)
**Plans**: 2 plans (previous 3-plan version obsoleted 2026-04-15 — primitive mismatch)
- [x] 27-01-PLAN.md — OP_SNAPSHOT_FETCH (0x12) + shared `Scope` struct/codec/validator + in-memory filter of entities + `snapshot_taken_at` response header + Rust + Python asyncio tests
- [x] 27-02-PLAN.md — OP_SUBSCRIBE (0x11) + SubscriberRegistry + ingest notify hook + 10k backpressure drop + metrics + signals + Rust + Python asyncio tests

### Phase 28: Client engine embedding + historical clone
**Goal**: Feature-flag the main crate into `client`/`server` flavors; ship `tally_cli` bin with real `tally clone` historical-mode clone backed by `OP_SNAPSHOT_FETCH`; `OutOfScopeError` raised at `client.get(key)` time. `tally sync` stays a stub until Phase 31.
**Depends on**: Phase 27 (Scope codec + OP_SNAPSHOT_FETCH); in-flight phase 28-01/02/03 plans already compile against a stub Session, so they're independent.
**Plans**: 4 plans (Phase 29 folded in as 28-04)
- [x] 28-01-PLAN.md — Add `client`/`server` Cargo features; gate server modules + main.rs; smoke test both feature builds
- [x] 28-02-PLAN.md — `src/bin/tally_cli.rs` with hand-rolled arg parsing; `clone`/`sync` stubs; `--mode streaming` rejected with Phase 31 pointer
- [x] 28-03-PLAN.md — `src/client/mod.rs` (stub Session + OutOfScopeError + scope types); engine side-effect audit; client-features `apply`/push round-trip integration test
- [x] 28-04-PLAN.md — Replace 28-02's `tally clone` stub with real TCP session + OP_SNAPSHOT_FETCH handshake + postcard deserialize into client StateStore; exponential-jitter reconnect; OutOfScopeError at `.get()` time; E2E `tests/integration/test_tally_clone.py` against a real server with fixture events

### Phase 30: Python Pipeline API + local query surface
**Goal**: Ship `tally.Pipeline(remote=..., streams=..., keys?=..., mode="historical").run(); .get(key, stream=...); .inspect()` via PyO3/maturin (Linux x86_64 wheel, Python >=3.10), plus `tally query` / `tally inspect` CLI subcommands. Typed error hierarchy (TallyError + OutOfScopeError / ClientConnectError / HandshakeError / ReplicaStateError), `.pyi` stubs, E2E pytest, CI wheel-build job.
**Depends on**: Phase 28-04 (real client `Session` + `StateStore`)
**Plans:** 2/2 plans complete
- [x] 30-01-PLAN.md — `python-native/` cdylib crate + maturin pyproject; PyO3 `Pipeline` class with `__init__`/`.run()` (GIL-releasing)/`.get()`/`.inspect()`; typed exceptions; `.pyi` stubs; unit tests + CI wheel job
- [x] 30-02-PLAN.md — `tally query` / `tally inspect` CLI subcommands (pure Rust); E2E pytest spinning up a real server, seeding events via existing Python SDK, asserting `Pipeline.run/get/inspect` + `OutOfScopeError` + CLI coverage

### Phase 31: Streaming mode + watch
**Goal**: Subscribe-first buffered-replay client dance (open SUBSCRIBE socket, buffer, fetch snapshot via SNAPSHOT_FETCH, apply snapshot, drop events with `timestamp ≤ snapshot_taken_at`, apply rest, continue live). PyO3 `.watch(key, stream)` generator yielding `{timestamp, event, value}`. `tally sync` CLI emitting NDJSON until Ctrl-C.
**Depends on**: Phase 27 (OP_SUBSCRIBE), Phase 28 (client feature + historical session), Phase 30 (PyO3 Pipeline class)
**Plans:** 2 plans (31-01 rewritten 2026-04-15 for Option K)
- [ ] 31-01-PLAN.md — Rust client streaming-mode session: two-socket subscribe-first dance; bg apply thread; `parking_lot::RwLock<StateStore>`; idempotent `.stop()` with thread join; server-drop detection (socket EOF → `StopReason::ServerDropped{at_ts}`); integration tests against Phase 27 server
- [ ] 31-02-PLAN.md — PyO3 `.watch()` generator (GIL-released recv) + streaming-mode `.run()` + `.stop()` + `__del__` + `SubscriberDroppedError`; `tally sync` CLI (NDJSON stdout, Ctrl-C → exit 130); Python E2E pytest with mid-stream push

### Phase 32: Resume + reconnect (stretch)
**Goal**: Persist last-applied timestamp per scope; on `SubscriberDroppedError`, reconnect and resume transparently.
**Depends on**: Phase 31
**Plans**: 0/? — not planned yet

### Phase 33: Upstream backfill sources (stretch)
**Goal**: `source="s3://..."` / `source="snowflake://..."` instead of `remote=`; reuses `BackfillSource` trait reserved in v0 Phase 22.
**Depends on**: Phase 28
**Plans**: 0/? — not planned yet

### Phase 34: Write-back / promote flow (stretch)
**Goal**: Register a feature on the client, compute locally against the stream, `.promote()` to the cluster. "Shadow replica + promote" DX loop.
**Depends on**: Phase 30, Phase 31
**Plans**: 0/? — not planned yet

### Phase 35: OP_LOG_FETCH (Option M)
**Goal**: Add scoped time-ranged historical CDC pull opcode `OP_LOG_FETCH{from_ts_millis, scope}` (0x13). Timestamp-based cursor (at-least-once on boundary), per-stream order, reuses Phase 6 per-stream log readers + Phase 27 Scope codec + admin-token auth pattern. Terminal `REPLICA_FRAME_TAG_END` (0x04) signals caught-up-to-tail.
**Depends on**: Phase 27 (Scope codec, entity_matches_scope), Phase 6 (per-stream log files)
**Plans:** 1 plan
- [ ] 35-01-PLAN.md — Opcode + Command variant + handler + per-stream log iteration + timestamp gate + Rust/Python tests

### Phase 36: Replica-mode server boot (Option M)
**Goal**: `tally serve --replica-from HOST:PORT --replica-since T --replica-streams S --replica-token T [--replica-pipeline-file F]` boot mode. Server connects to remote, runs LOG_FETCH catchup, transitions to SUBSCRIBE live-tail, routes all events through its own local ingest path (persisted to Phase 6 logs; routed through any registered scientist pipelines). Listener binding gated until catchup-done. Rejects local PUSH in replica mode. Auto-reconnects SUBSCRIBE on drop with timestamp-resume.
**Depends on**: Phase 27 (OP_SUBSCRIBE), Phase 35 (OP_LOG_FETCH), Phase 28-01 (feature flags — active), Phase 31-01 (streaming consumer code — reused)
**Plans:** 1/1 plans complete
- [x] 36-01-PLAN.md — CLI flag parsing + ReplicaClient loop + ingest routing (replica_ingest) + listener gate + integration test

### Phase 37: `tally fork` CLI + E2E demo (Option M)
**Goal**: Ship `tally fork --remote HOST --since T --streams S --keys K --token T [--local-port 7400] [--pipeline-file P]` as a scientist-ergonomic wrapper for `tally serve --replica-from ...`, plus the load-bearing E2E pytest that proves the whole Option M workflow (prod → fork → register pipeline → query → live update).
**Depends on**: Phase 35, Phase 36
**Plans:** 1 plan
- [ ] 37-01-PLAN.md — `tally fork` subcommand + `/debug/ready` endpoint + test_fork_demo.py end-to-end

### Phase 38: Mothball Option K surfaces (Option M housekeeping)
**Goal**: Delete superseded embedded-client code (`src/client/clone.rs`, `streaming.rs`, `state.rs`; `tally_cli clone/query/inspect/sync`; `python-native/` crate). Banner-tag Option K CONTEXTs as SUPERSEDED. Prevents future readers from confusing dead paths with live ones.
**Depends on**: Phase 35, Phase 36, Phase 37 all green
**Plans:** 1 plan
- [ ] 38-01-PLAN.md — Delete Rust modules + tally_cli subcommands + python-native crate + doc sweep

## Progress

**Execution Order:** v0 Restructure (Phases 21-26) complete; v2.1 Launch (Phase 20) engineering complete, deploy ops pending async; Phase 27 server opcodes shipped. **Option M active**: Phase 35 (OP_LOG_FETCH) → Phase 36 (replica-mode boot) → Phase 37 (tally fork + E2E demo) → Phase 38 (mothball Option K).

| Phase | Milestone | Plans Complete | Status | Completed |
|-------|-----------|----------------|--------|-----------|
| 20. Traction Demo | v2.1 | 3/3 | Engineering complete; live-run ops pending | 2026-04-14 (eng) |
| 21. Type system & SDK skeleton | v0 | 3/3 | Complete | 2026-04-14 |
| 22. Stream aggregation engine | v0 | 4/4 | Complete | 2026-04-14 |
| 23. Joins | v0 | 3/3 | Complete | 2026-04-14 |
| 24. Table storage + Watermarks & event-time | v0 | 5/5 | Complete | 2026-04-14 |
| 25. Query surface, TTL, warnings | v0 | 3/3 | Complete | 2026-04-14 |
| 26. Test migration, bench, docs, demo | v0 | 4/4 | Complete | 2026-04-14 |
| 27. Server-side replica endpoints | v0 | 2/2 | Complete (SUBSCRIBE reused by Option M) | 2026-04-15 |
| 28. Client engine embedding + historical clone | v0 | 4/4 | SUPERSEDED by Option M (mothballed in Phase 38) | 2026-04-15 |
| 30. Python Pipeline API + local query surface | v0 | 2/2 | SUPERSEDED by Option M (mothballed in Phase 38) | 2026-04-15 |
| 31. Streaming mode + watch (Option K) | v0 | 1/2 | SUPERSEDED mid-flight (31-02 cancelled); 31-01 shipped but unused | 2026-04-15 (partial) |
| 32. Resume across restarts (stretch) | v0 | 0/? | Not planned | — |
| 33. Upstream backfill sources (stretch) | v0 | 0/? | Not planned | — |
| 34. Write-back / promote (stretch) | v0 | 0/? | Not planned | — |
| 35. OP_LOG_FETCH | v0 | 0/1 | **Planned (Option M)** | — |
| 36. Replica-mode server boot | v0 | 1/1 | Complete   | 2026-04-15 |
| 37. `tally fork` CLI + E2E demo | v0 | 0/1 | **Planned (Option M)** | — |
| 38. Mothball Option K surfaces | v0 | 0/1 | **Planned (Option M)** | — |
