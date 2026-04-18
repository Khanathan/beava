# Tally Roadmap

## Milestones

- [x] **v1.0 -- Foundation** (Phases 1-5) -- Complete 2026-04-09 -- archived
- [x] **v1.1–v1.3 -- Event Log, Pipelines, Concurrency** (Phases 6-15) -- Complete 2026-04-12
- [x] **v2.0 -- API & Engine** (Phases 16-19) -- Complete 2026-04-13 -- `.planning/milestones/v2.0-ROADMAP.md`
- [x] **v2.1 -- Launch** (Phase 20) -- Engineering complete 2026-04-14 (live-run ops pending, calendar-gated) -- `.planning/milestones/v2.1-ROADMAP.md`
- [ ] **v0 -- Restructure + Data-Scientist Fork** (Phases 21-38) -- Active. Phases 21-26 shipped 2026-04-14 (restructure); Phase 27 shipped 2026-04-15 (server replica opcodes); Option K (phases 28/30/31) SUPERSEDED 2026-04-15 by Option M (phases 35-38) — local server in replica mode. Restructure archive: `.planning/milestones/v0-ROADMAP.md`.
- [x] **v1.0-launch — Public Launch Readiness** (Phases 45-47) — Engineering complete 2026-04-17 (launch-day human-run pending) — `.planning/milestones/v1.0-launch-ROADMAP.md`

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
- [x] **Phase 37: `tally fork` CLI + E2E demo** — `tally fork --remote ... --since ... --streams ... --pipeline-file ...` wrapper + load-bearing pytest that proves the scientist workflow end-to-end. 1 plan. (completed 2026-04-15)
- [ ] **Phase 38: Mothball Option K surfaces** — Delete obsolete embedded-client code, `tally_cli clone/query/inspect/sync`, `python-native/` crate. Housekeeping. 1 plan.

**Stretch (unchanged):**
- [ ] **Phase 32: Resume across restarts (stretch)** — persist `last_applied_timestamp` to disk; replica re-runs from there.
- [ ] **Phase 33: Upstream backfill sources (stretch)** — `source="s3://..."` instead of `--replica-from`; reuses Phase 22 `BackfillSource` trait.
- [ ] **Phase 34: Write-back / promote (stretch)** — scientist's compute → promote to cluster.

~~Phase 29~~ (removed 2026-04-15, Option K era).

### v1.0-launch — Public Launch Readiness (Phases 45-47 — Active 2026-04-17)

**Goal:** Ship Beava as a public Apache 2.0 project. A skeptical engineer landing on the GitHub repo goes from cold to correct, live feature values in under 60 seconds — from any language.

**Structure:** Three additive workstreams derived from LAUNCH.md + research/SUMMARY.md. Phases 45 and 46 touch disjoint code paths (HTTP router vs engine internals) and run **fully in parallel from day one**. Phase 47 has item-level dependencies on both — Docker/CI/clippy/community-files/directory-READMEs start immediately; README rewrite + `docs/http-api.md` + `examples/curl-ingest/` block on Phase 45; `docs/event-time.md` blocks on Phase 46; ship-gate items SHIP-02..SHIP-05 land at end of Phase 47 since they require all three phases done.

**Phase numbering:** Continues from previous milestones (highest existing phase dir is 41 per LAUNCH.md Key Decision); no reset. Stretch phases 32/33/34 reserved for v0 but unstarted.

**Phase summary:**
- [x] **Phase 45: HTTP Ingest & Read API** — 6 HTTP endpoints (3 push, 3 read) + curl/Go/Node examples + >100 K EPS load-test harness. Covers HTTP-01..HTTP-10 (10 requirements). (completed 2026-04-17)
- [x] **Phase 46: Correctness Audit, Fixes & Ship-Gate Integration Test** — 2a batch-`now` fix, 2b ring-buffer-drops metric, 2c per-stream watermark lateness, 2d.i–vii audit closures (some code, some docs), `docs/event-time.md`, single backfill→crash→recover→verify integration test. Covers CORR-01..CORR-10 + OBS-01..OBS-03 + SHIP-01 (14 requirements). (completed 2026-04-18)
- [ ] **Phase 47: Repo Polish, Docker, CI, Docs, Examples** — multi-stage distroless Docker image + Docker Hub publish, GitHub Actions CI (fmt/clippy/test <5 min), <60-line HTTP-first README rewrite, 8 flat-markdown docs pages, 3 example projects, community files audit, directory READMEs, TODO/FIXME sweep, social preview + topics, fresh-VM smoke test, outreach re-audit, benchmarks re-verified, quickstart GIF. Covers INFRA-01..INFRA-10 + CONTENT-01..CONTENT-11 + SHIP-02..SHIP-05 (25 requirements).

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
**Plans:** 1/1 plans complete
- [x] 37-01-PLAN.md — `tally fork` subcommand + `/debug/ready` endpoint + test_fork_demo.py end-to-end

### Phase 38: Mothball Option K surfaces (Option M housekeeping)
**Goal**: Delete superseded embedded-client code (`src/client/clone.rs`, `streaming.rs`, `state.rs`; `tally_cli clone/query/inspect/sync`; `python-native/` crate). Banner-tag Option K CONTEXTs as SUPERSEDED. Prevents future readers from confusing dead paths with live ones.
**Depends on**: Phase 35, Phase 36, Phase 37 all green
**Plans:** 1 plan
- [ ] 38-01-PLAN.md — Delete Rust modules + tally_cli subcommands + python-native crate + doc sweep

### Phase 45: HTTP Ingest & Read API
**Goal**: Ship the 6 HTTP endpoints (3 push + 3 read) that turn Beava into a curl-able, language-agnostic server. Every non-Python engineer evaluating Beava — Go, Node, Java, Ruby, log-shippers, browsers — gets first-class ingest and read without an SDK. Delivers the core-value "any language" half of the 60-second onboarding promise.
**Depends on**: Existing v2.1 axum router (`src/server/http.rs:1529`), v2.0 engine (`handle_push_core_ex`, `handle_push_batch`), v2.1 `require_loopback_or_token` middleware. Runs fully in parallel with Phase 46 (disjoint code paths).
**Requirements**: HTTP-01, HTTP-02, HTTP-03, HTTP-04, HTTP-05, HTTP-06, HTTP-07, HTTP-08, HTTP-09, HTTP-10
**Success Criteria** (what must be TRUE):
  1. A user can `curl -X POST http://localhost:6900/push/<stream> -d '{"user":"alice"}'` and immediately see the event via `curl http://localhost:6900/features/alice` — from any language, no SDK required.
  2. A user pushing a 1000-event JSON array to `/push-batch/{stream}` gets a single structured response summarizing per-event accept/reject, and each event is bucketed by its own payload `_event_time` (client-side validation of the 2a fix).
  3. A user can drive sustained **>100 K EPS** against `/push-batch/{stream}` from a single `oha` client against a reference box, with the number committed in `benchmark/README.md`.
  4. A user's unauthenticated `POST /push/*` request returns 401 from a non-loopback source and 200 from loopback — inheriting `require_loopback_or_token` unchanged, verified by a per-endpoint auth integration test.
  5. A developer opens `docs/http-api.md` and copy-pastes working curl, Go (`net/http`), and Node (`fetch`) examples for each of the 6 endpoints.
**Plans:** 5/5 plans complete
- [x] 45-01-PLAN.md — Wave 0 scaffolding: deps (axum-extra/tower-http/tower), auth 403→401, body-limit+timeout layer, http_ingest.rs skeleton, 7 test scaffolds (TDD RED)
- [x] 45-02-PLAN.md — Wave 1 read endpoints: GET /features/{key} with ?table filter, GET /streams, GET /streams/{name}, public-mode routing (HTTP-04/05/07)
- [x] 45-03-PLAN.md — Wave 1 write endpoints: POST /push, /push-batch, /push/ndjson via handle_push_core_ex/handle_push_batch + schema-parity round-trip (HTTP-01/02/03)
- [x] 45-04-PLAN.md — Wave 2 exhaustive per-route auth sweep + beava_events_total{proto} dual-emit metric transition (HTTP-06 / A5)
- [x] 45-05-PLAN.md — Wave 2 docs/http-api.md rewrite + examples/curl-ingest/ + benchmark/http_load.sh (>100K EPS reference-box checkpoint) + docs/http-api-examples.sh (HTTP-08/09/10)

### Phase 46: Correctness Audit, Fixes & Ship-Gate Integration Test
**Goal**: Close every correctness item flagged by the release audit so a publicly-launched Beava can be trusted with backfill, crash-recovery, TTL, and fork workloads from minute one. Land the single backfill→crash→recover→verify integration test that simultaneously exercises the 2a fix, the 2d.i closure, and the 2d.ii fix.
**Depends on**: v2.0 engine internals (`push_batch_with_cascade_no_features`, `WatermarkTracker`, `run_backfill`, `replica_ingest_batch`, `evict_expired_stream_entries`, `take_dirty_and_advance_gen`), ring-buffer operators. Runs fully in parallel with Phase 45 (disjoint code paths). No external dependencies; adds zero new runtime crates.
**Requirements**: CORR-01, CORR-02, CORR-03, CORR-04, CORR-05, CORR-06, CORR-07, CORR-08, CORR-09, CORR-10, OBS-01, OBS-02, OBS-03, SHIP-01
**Success Criteria** (what must be TRUE):
  1. A user pushing a batch where `event_time[0]` is 2 hours in the past and `event_time[1..]` are `now` sees event 0 in the 2-hours-ago bucket and the rest in the now bucket — never collapsed (validates CORR-01).
  2. A user ingesting events then running `kill -9 && restart` sees feature values bit-identical to live-ingest baseline on the same event sequence — validated by the single ship-gate integration test (SHIP-01, covering CORR-01/05/06 simultaneously).
  3. A user running `tally fork` against a remote and pushing events sees fork-replica watermarks advance and downstream cascades fire (validates CORR-08; closes 2d.iv).
  4. A user ingesting 30-day-old historical events does NOT see `entity_ttl` evict the entities immediately — eviction clock sources from observed event-time, not wall-clock (validates CORR-07; closes 2d.iii).
  5. A user scrapes `/metrics` under load and sees exactly one of `beava_late_events_dropped_total` or `beava_ring_buffer_drops_total{reason}` fire per dropped event (mutually exclusive, bounded label cardinality — validates OBS-01/OBS-02).
  6. A user defines `@bv.stream(watermark_lateness="10m")` and sees that value honored; streams without the field keep defaulting to 5 s with no snapshot-migration churn (validates CORR-03/CORR-04).
  7. A user or maintainer opens `docs/event-time.md` and understands bucket assignment, watermark lateness, crash-replay determinism, TTL semantics, join idle-input behavior, and fork watermark propagation in one page (validates OBS-03; closes 2d.v + 2d.i as docs-only).
  8. A maintainer runs the full 9-cell benchmark matrix after all Phase 46 merges and every cell is within −5% of the committed v2.0 BASELINE (validates CORR-02 — hard merge gate for the 2a fix).
**Plans:** 8/8 plans complete
- [x] 46-01-PLAN.md — Wave 0 deps + 9-cell bench shims + 10 test scaffolds + CORR-05 verification test
- [x] 46-02-PLAN.md — Wave 1 docs/event-time.md stub (CORR-09 2d.i + 2d.v one-line closures)
- [x] 46-03-PLAN.md — Wave 2 2a batch-path fix (D-01/D-02/D-26) + CORR-01 proptest + 9-cell merge gate (CORR-02)
- [x] 46-04-PLAN.md — Wave 3 per-stream watermark_lateness (CORR-03/04) + Python SDK plumbing (no humantime_serde)
- [x] 46-05-PLAN.md — Wave 3 (parallel) 2d.ii backfill event-time + 2d.iii TTL clock + 2d.iv replica observe (CORR-06/07/08)
- [x] 46-06-PLAN.md — Wave 4 ring-buffer drops metric with bounded cardinality + mutual-exclusivity (OBS-01/02)
- [x] 46-07-PLAN.md — Wave 4 (parallel) ArcSwap<DashSet> atomic dirty-set swap + busy-racer + <2% bench gate (CORR-10)
- [x] 46-08-PLAN.md — Wave 5 full docs/event-time.md (OBS-03) + fsync hook scaffold (D-27) + ship-gate test (SHIP-01)

### Phase 47: Repo Polish, Docker, CI, Docs, Examples
**Goal**: Turn the GitHub repo into a surface that earns the "let me try the quickstart" click in the first 10 seconds and delivers correct, live feature values in the next 50. `docker run beavadb/beava:latest` + a copy-pasteable `curl` from a <60-line README is the single published onboarding path. Close the ship-gate (fresh-VM smoke, outreach re-audit, benchmarks re-verified, quickstart GIF).
**Depends on**: Most items start day one (Docker, CI, clippy/fmt, community files, directory READMEs, `docs/{concepts,operations,architecture,faq,python-sdk,getting-started}.md`, social preview, topics). **Blocks on Phase 45** for: README rewrite (CONTENT-01), `docs/http-api.md` (CONTENT-10 reference material), `examples/curl-ingest/` (CONTENT-10), `examples/fraud-scoring/` HTTP variant (CONTENT-08), `examples/session-features/` (CONTENT-09). **Blocks on Phase 46** for: `docs/event-time.md` deep-reference content (OBS-03 authored in 43; CONTENT pages cross-link). **Blocks on Phases 45 + 46** for: SHIP-02 fresh-VM E2E smoke, SHIP-03 benchmarks re-verified, SHIP-04 outreach re-audit, SHIP-05 quickstart GIF.
**Requirements**: INFRA-01, INFRA-02, INFRA-03, INFRA-04, INFRA-05, INFRA-06, INFRA-07, INFRA-08, INFRA-09, INFRA-10, CONTENT-01, CONTENT-02, CONTENT-03, CONTENT-04, CONTENT-05, CONTENT-06, CONTENT-07, CONTENT-08, CONTENT-09, CONTENT-10, CONTENT-11, SHIP-02, SHIP-03, SHIP-04, SHIP-05
**Success Criteria** (what must be TRUE):
  1. A visitor lands on the GitHub repo, sees a green CI badge and accurate repo description + topics, copy-pastes `docker run -p 6900:6900 beavadb/beava:latest` from a <60-line README followed by a curl — both succeed on a fresh machine without edits (validates INFRA-01/04, CONTENT-01).
  2. A visitor opens `docs/getting-started.md` on a fresh machine and reaches a live feature read in under 60 seconds (validates SHIP-02 and CONTENT-02; recorded as the quickstart GIF/video per SHIP-05).
  3. A maintainer pushes a commit and sees `cargo fmt --check`, `cargo clippy --all-targets -- -D warnings`, and `cargo test --lib` all green on `.github/workflows/ci.yml` in under 5 minutes (validates INFRA-03).
  4. A user pulls `beavadb/beava:latest` and confirms the container runs as non-root on multi-stage distroless/cc Debian 12, with `docker compose up` against `examples/docker-compose.yml` exposing port 6900 + a mounted data volume (validates INFRA-01/02/05).
  5. A user runs `examples/fraud-scoring/`, `examples/session-features/`, and `bash examples/curl-ingest/run.sh` — each against a freshly-started Beava Docker container, without edits (validates CONTENT-08/09/10).
  6. A maintainer can re-run `benchmark/` ingest / recovery / fork-replay scripts on the current tree and reproduce the committed v2.0 BASELINE within −5%, with the machine spec in the committed JSON (validates SHIP-03).
  7. A maintainer has re-audited `.planning/outreach/LAUNCH-PACKAGE-V8.md` against `AUDIT-V11.md` — every headline claim in README + outreach maps to a committed benchmark file or citation, with no fabricated "N× faster" claims (validates SHIP-04).
  8. A visitor browsing `src/server/`, `src/engine/`, `src/state/`, `benchmark/`, `deploy/` sees a 1–2 paragraph `README.md` in each explaining the module's role; every `pub fn` / `pub struct` in `src/lib.rs` exports has at least a one-line doc comment; zero stray `println!` / `dbg!` / `eprintln!` outside intentional startup logging (validates INFRA-06/07/08, CONTENT-11).
**Plans:** 9/10 plans executed
- [x] 47-01-PLAN.md — Wave 0 Dockerfile (cargo-chef → distroless/cc-debian12:nonroot) + examples/docker-compose.yml + Docker Hub publish runbook (INFRA-01/02/05)
- [x] 47-02-PLAN.md — Wave 0 GitHub Actions CI workflow (fmt / clippy / nextest / Python SDK 3.10-3.12 matrix; <5 min) (INFRA-03/04)
- [ ] 47-03-PLAN.md — Wave 0 code hygiene: TODO/FIXME audit + println/dbg sweep + missing_docs on lib.rs + clippy + fmt + dead-code (INFRA-06/07/08)
- [x] 47-04-PLAN.md — Wave 0 community files audit + CHANGELOG v0.1.0 + social preview PNG + github repo surface runbook (INFRA-09/10)
- [x] 47-05-PLAN.md — Wave 0 directory READMEs: src/server, src/engine, src/state (new) + benchmark (extend) + deploy (new) (CONTENT-11)
- [x] 47-06-PLAN.md — Wave 1 README rewrite <60 lines HTTP-first + docs/legacy-readme.md preservation (CONTENT-01)
- [x] 47-07-PLAN.md — Wave 1 core docs: getting-started.md (60-sec Docker) + concepts.md + operations.md (CONTENT-02/03/05)
- [x] 47-08-PLAN.md — Wave 1 reference docs: architecture + faq + comparison + python-sdk re-verify + http-api.md polish (CONTENT-04/06/07, CONTENT-10 docs polish)
- [x] 47-09-PLAN.md — Wave 1 examples: fraud-scoring/ (HTTP variant) + session-features/ + curl-ingest/README + examples/README index (CONTENT-08/09/10)
- [x] 47-10-PLAN.md — Wave 2 ship gate: SHIP-VM-SMOKE runbook + LAUNCH-VERIFY benchmarks + OUTREACH audit checklist + QUICKSTART recording runbook + PHASE-47-CLOSURE audit (SHIP-02/03/04/05)

## Progress

**Execution Order:** v0 Restructure (Phases 21-26) complete; v2.1 Launch (Phase 20) engineering complete, deploy ops pending async; Phase 27 server opcodes shipped; Option M Phases 36-37 complete, Phases 35 + 38 planned. **v1.0-launch active 2026-04-17**: Phase 45 (HTTP) ∥ Phase 46 (correctness) running in parallel; Phase 47 (polish + Docker + CI + docs + examples) starts day one on non-blocked items, finalizes once 45 + 46 land.

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
| 37. `tally fork` CLI + E2E demo | v0 | 1/1 | Complete   | 2026-04-15 |
| 38. Mothball Option K surfaces | v0 | 0/1 | **Planned (Option M)** | — |
| 45. HTTP Ingest & Read API | v1.0-launch | 5/5 | Complete   | 2026-04-17 |
| 46. Correctness Audit, Fixes & Ship-Gate Integration Test | v1.0-launch | 8/8 | Complete   | 2026-04-18 |
| 47. Repo Polish, Docker, CI, Docs, Examples | v1.0-launch | 9/10 | In Progress|  |
