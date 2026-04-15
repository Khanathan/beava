# Tally Roadmap

## Milestones

- [x] **v1.0 -- Foundation** (Phases 1-5) -- Complete 2026-04-09 -- archived
- [x] **v1.1–v1.3 -- Event Log, Pipelines, Concurrency** (Phases 6-15) -- Complete 2026-04-12
- [x] **v2.0 -- API & Engine** (Phases 16-19) -- Complete 2026-04-13 -- `.planning/milestones/v2.0-ROADMAP.md`
- [x] **v2.1 -- Launch** (Phase 20) -- Engineering complete 2026-04-14 (live-run ops pending, calendar-gated) -- `.planning/milestones/v2.1-ROADMAP.md`
- [ ] **v0 -- Restructure + Local Replica** (Phases 21-34) -- Active. Phases 21-26 shipped 2026-04-14 (restructure); Phases 27, 28, 30, 31 are the v0 replica leg. Restructure archive: `.planning/milestones/v0-ROADMAP.md`.

## Phases

### v0 Restructure (Phases 21-26 — Complete 2026-04-14)

**Outcome:** Two-type (Stream + Table) API, DataFrame-parity operators, hybrid sketches (UDDSketch / CMS+heap / HLL), 5-second fixed event-time watermarks with γ propagation, per-Table row storage with 7d tombstone grace, unified `/debug/warnings` + `tally suggest-config`, zero-old-API codebase, 9-cell benchmark matrix within −5% of v2.0 BASELINE (worst cell −4.84%), launch blog rewritten honestly. **All 11 sign-off criteria green** — see `.planning/phases/26-test-migration-bench-docs-demo/26-SIGNOFF.md`. Full phase list + plan history archived in `.planning/milestones/v0-ROADMAP.md`.

### v0 Local Replica — Option K (Phases 27, 28, 30, 31 — Active)

**Revised 2026-04-15** after executor stopped at a primitive mismatch: the prior plan assumed a global event-log seq counter and streamable snapshot frames, neither of which exist. Under user directive "easiest for v0 and demo" the design reduces to **Option K: snapshot + subscribe only** (no LOG_FETCH, no catchup loop, no seq).

**What Option K delivers:**
- `tally clone` (historical mode): one-shot OP_SNAPSHOT_FETCH → deserialize → queryable local replica at a point in time. Re-run to get a different point in time.
- `tally sync` (streaming mode): subscribe-first buffered-replay dance (open subscribe socket, buffer, fetch snapshot, apply, drop pre-snapshot events, apply rest, continue live). Feeds `pipe.watch(key)`.

**Cut from v0** (moved to v0.2 or later):
- `OP_LOG_FETCH` + arbitrary-range event replay — requires event-log seq (schema change).
- Global seq-monotonic SUBSCRIBE ordering — requires seq. Replaced with per-subscriber accept order.
- Streamable snapshot format — requires snapshot v8.
- DAG-derived automatic scope.
- Phase 29 (session manager + catchup) — folded into Phase 28 as plan 28-04.
- Phase 32/33/34 — stretch, unchanged.

Full design discussion: `.planning/phases/27-server-replica-endpoints/27-CONTEXT.md` (Option K revision).

- [x] **Phase 27: Server-side replica endpoints (2 opcodes)** — `OP_SNAPSHOT_FETCH{scope}` (0x12) + `OP_SUBSCRIBE{scope}` (0x11). In-memory filter of `BaseSnapshotState`; `SubscriberRegistry` with DashMap + 10k bounded queue; ingest-path notify hook; admin-token auth; response-header `snapshot_taken_at`. No LOG_FETCH. Per-subscriber order only. (completed 2026-04-15)
- [x] **Phase 28: Client engine embedding + historical clone** — feature-flag `client`/`server` on main crate; `tally_cli` bin with `clone`/`sync` subcommands; `src/client/` module with real `Session`, `OutOfScopeError`, and historical-mode snapshot fetch. `tally clone` ships in this phase (plan 28-04). `tally sync` stubs. (completed 2026-04-15)
- [ ] **Phase 30: Python Pipeline API + local query surface** — PyO3/maturin `tally.Pipeline(...).run()/.get()/.inspect()`; typed error hierarchy; `tally query` / `tally inspect` CLI.
- [ ] **Phase 31: Streaming mode + watch** — subscribe-first buffered-replay client dance; bg apply thread + RwLock on client StateStore; PyO3 `.watch()` generator; `tally sync` NDJSON CLI.
- [ ] **Phase 32: Resume + reconnect (stretch)** — persist last-applied timestamp per scope; on `SubscriberDroppedError`, reconnect and resume.
- [ ] **Phase 33: Upstream backfill sources (stretch)** — `source="s3://..."` / `source="snowflake://..."` instead of `remote=`; reuses Phase 22 `BackfillSource` trait.
- [ ] **Phase 34: Write-back / promote (stretch)** — register a feature on the client, compute locally, `.promote()` to cluster.

~~Phase 29~~ (Session manager + historical E2E) — **removed 2026-04-15**. Its remaining work (~30 lines of client-side snapshot fetch + deserialize) folded into Phase 28 as plan 28-04. Original 29-CONTEXT preserved at `.planning/phases/29-session-manager-historical-OBSOLETE/`.

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
**Plans:** 4/4 plans complete
- [ ] 30-01-PLAN.md — `python-native/` cdylib crate + maturin pyproject; PyO3 `Pipeline` class with `__init__`/`.run()` (GIL-releasing)/`.get()`/`.inspect()`; typed exceptions; `.pyi` stubs; unit tests + CI wheel job
- [ ] 30-02-PLAN.md — `tally query` / `tally inspect` CLI subcommands (pure Rust); E2E pytest spinning up a real server, seeding events via existing Python SDK, asserting `Pipeline.run/get/inspect` + `OutOfScopeError` + CLI coverage

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

## Progress

**Execution Order:** v0 Restructure (Phases 21-26) complete; v2.1 Launch (Phase 20) engineering complete, deploy ops pending async; v0 Local Replica under Option K runs 27 → 28 → 30 → 31 (Phase 29 folded into 28-04).

| Phase | Milestone | Plans Complete | Status | Completed |
|-------|-----------|----------------|--------|-----------|
| 20. Traction Demo | v2.1 | 3/3 | Engineering complete; live-run ops pending | 2026-04-14 (eng) |
| 21. Type system & SDK skeleton | v0 | 3/3 | Complete | 2026-04-14 |
| 22. Stream aggregation engine | v0 | 4/4 | Complete | 2026-04-14 |
| 23. Joins | v0 | 3/3 | Complete | 2026-04-14 |
| 24. Table storage + Watermarks & event-time | v0 | 5/5 | Complete | 2026-04-14 |
| 25. Query surface, TTL, warnings | v0 | 3/3 | Complete | 2026-04-14 |
| 26. Test migration, bench, docs, demo | v0 | 4/4 | Complete | 2026-04-14 |
| 27. Server-side replica endpoints (Option K) | v0 | 2/2 | Complete   | 2026-04-15 |
| 28. Client engine embedding + historical clone | v0 | 4/4 | Complete   | 2026-04-15 |
| 30. Python Pipeline API + local query surface | v0 | 0/2 | Planned | — |
| 31. Streaming mode + watch (Option K) | v0 | 1/2 | 31-02 planned; 31-01 needs re-plan | — |
| 32. Resume + reconnect (stretch) | v0 | 0/? | Not planned | — |
| 33. Upstream backfill sources (stretch) | v0 | 0/? | Not planned | — |
| 34. Write-back / promote flow (stretch) | v0 | 0/? | Not planned | — |
