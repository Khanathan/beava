# Beava v2 — v0 OSS Launch Roadmap

**Milestone:** v0 (first public OSS cut on `beava.dev`)
**Granularity:** fine (10 phases; 5-10 plans per phase)
**Mode:** yolo (auto-approved; written to hold up unrevised)
**Parallelization:** enabled where indicated
**Created:** 2026-04-22
**Source:** `.planning/PROJECT.md`, `.planning/REQUIREMENTS.md` (100 REQ-IDs), `DESIGN-V2.md`

## North Star

Declare a feature, push events, query it — in under 10 minutes, with curl alone. v0 is "the smallest thing that delivers that promise for 40 built-in primitives on a single box, durably."

## Architecture (locked, do not revisit in phases)

- Single Rust process, single OS thread for the apply loop (plus auxiliary threads for WAL fsync, HTTP accept, snapshot writer)
- In-memory state only (no RocksDB, no fjall, no tiered storage)
- WAL file per instance with 1-5ms group-commit fsync; periodic snapshots (default 30s) of in-memory state
- Recovery = load latest snapshot + replay WAL from snapshot LSN
- HTTP/1.1 + JSON only; endpoints: `POST /register`, `POST /push/{stream}`, `POST /get`, `GET /get/{feature}/{key}`, `/metrics`, `/health`, `/ready`
- 40 built-in primitives declared via JSON DSL with where-filter grammar
- Python SDK = thin HTTP wrapper; sync + fire-and-forget only; no callbacks, no persistent connections

## Phase Overview

| # | Phase | Goal | Reqs | Success criteria |
|---|-------|------|------|------------------|
| 1 | Foundation | Rust workspace, HTTP scaffolding, config, logging, test harness — one can boot an empty `beava` binary and curl `/health` | 0 scope-shipping + supports all | 4 |
| 2 | Primitive infra + registration | `POST /register` parses streams, features, where-filter DSL; operator trait + windowed bucket infra + registry exist end-to-end | 15 | 5 |
| 3 | Core aggregates + push/get | Full HTTP surface works for `count`/`sum`/`avg`/`min`/`max`/`stddev`/`variance`/`z_score`/`ratio`; windowing semantics correct; batch + single get | 18 | 5 |
| 4 | WAL + idempotency | Push is crash-safe: ACK only after fsync; duplicate idempotency keys replay byte-identical response | 6 | 5 |
| 5 | Snapshot + recovery | Server restarts from snapshot+WAL; schema evolution lives across restarts | 6 | 5 |
| 6 | Recency, decay, velocity primitives | 19 temporal/rate/trend primitives land on the existing apply loop | 19 | 4 |
| 7 | Bounded buffers + geo primitives | 14 histogram/buffer/geo primitives land with their (often structured) return shapes | 14 | 4 |
| 8 | Sketch primitives | HLL, Bloom, DDSketch, SpaceSaving, entropy land; crash-recovery-tested across sketches | 5 | 4 |
| 9 | Observability + performance | `/metrics`, `/health`, `/ready`, structured logs; perf harness proves ≥3M EPS/core and <10ms P99 batch-get | 10 | 5 |
| 10 | Python SDK, docs, packaging (ship) | `pip install beava` works; docs site live; Linux/Mac binaries + Docker image published; `README.md` 3-command smoke | 16 | 5 |

**Total:** 10 phases, 100/100 requirements mapped, 46 success criteria.

## Parallelization

- **Phases 1 → 2 → 3 → 4 → 5** are strictly sequential (each builds on the previous).
- **Phase 5 → Phases 6, 7, 8** can run in parallel: each primitive family shares the apply loop and state cache from Phase 3 but touches independent operator modules. A single implementer can batch-sequence them; if Claude runs concurrent worktrees (one per family), conflicts are limited to `operator_registry.rs` and `docs/primitives.md` (append-only). **Recommended:** sequence 6 → 7 → 8 unless explicitly parallelizing.
- **Phase 9 (observability + perf)** depends on all primitive phases being complete for representative benchmarking.
- **Phase 10 (SDK + docs + packaging)** has three independent sub-tracks (SDK, docs, packaging) that the `plan-phase` step can parallelize.

## Dependency Graph

```
  Phase 1 (Foundation)
       │
       ▼
  Phase 2 (Primitive infra + registration)
       │
       ▼
  Phase 3 (Core aggregates + push/get)
       │
       ▼
  Phase 4 (WAL + idempotency)
       │
       ▼
  Phase 5 (Snapshot + recovery)
       │
       ├──────────────┬──────────────┐
       ▼              ▼              ▼
  Phase 6         Phase 7        Phase 8
  (Recency/       (Buffers       (Sketches)
   decay/         + geo)
   velocity)
       └──────────────┴──────────────┘
                      │
                      ▼
              Phase 9 (Obs + perf)
                      │
                      ▼
              Phase 10 (SDK + docs + pkg — ship)
```

## Phase Details

### Phase 1: Foundation

**Goal:** A `beava` binary that boots from config, exposes an HTTP server with health and ready stubs, writes structured JSON logs, and runs under an integration test harness. Nothing domain-shaped; the skeleton every later phase attaches to.

**Depends on:** Nothing (first phase).

**Requirements:** None directly scope-shipping — this phase is pure enablement. (No REQ-IDs consumed here; every requirement is shipped in phases 2–10.)

**Success Criteria** (what must be TRUE):
  1. `cargo build --release` produces a single stripped binary; `./beava --config ./beava.yaml` starts a server that binds an HTTP port and logs JSON.
  2. `curl localhost:$PORT/health` returns HTTP 200 with `{"status":"ok"}` within 1s of startup; `/ready` returns 503 until the (stubbed) recovery-complete flag is set.
  3. The HTTP framework is picked, wired to `axum` (or equivalent), and graceful shutdown on SIGTERM is implemented and tested.
  4. An integration-test harness exists that can spawn the binary in-process, wait for readiness, issue HTTP calls, and tear down cleanly — used by every subsequent phase.

**Plans:** TBD

---

### Phase 2: Primitive infra + registration

**Goal:** Users can `POST /register` a stream with a typed schema and a list of feature declarations; the server parses the JSON DSL, validates it, persists the registration in memory, and exposes a feature registry that every downstream operator will attach to. The operator trait, windowed bucket infrastructure, and where-filter evaluator exist and are exercised by at least one placeholder operator.

**Depends on:** Phase 1.

**Requirements:**
- `API-01`, `API-02`, `API-09`
- `STREAM-01`
- `REG-01`, `REG-02`, `REG-03`, `REG-04`
- `WIN-01`, `WIN-02`, `WIN-03`, `WIN-04`
- `TEST-02` (partial — HTTP integration test infra stood up against `/register`)
- Also lays the foundation for all `PRIM-*` requirements (shared infra only; individual primitives ship in phases 3, 6, 7, 8)

**Note:** we intentionally assign `TEST-02` to the first phase that introduces HTTP endpoints; subsequent phases extend that harness but do not re-own the requirement. `WIN-*` requirements ship here because windowing infra is what the operator trait is built around.

**Success Criteria** (what must be TRUE):
  1. `POST /register` with a valid payload (stream + feature list) returns 200 and the registration is retrievable; re-posting the same payload is idempotent (no-op); conflicting redeclaration returns 409 with a structured diff (`REG-04`, `API-02`).
  2. `POST /register` with an unknown feature type, unknown field, or malformed `where` clause returns 400 with a message naming the offending location (`REG-04`).
  3. The where-filter DSL accepts `{field: {op: value}}` with ops `eq/ne/gt/lt/gte/lte/in`, composed via `{and:[...]}`/`{or:[...]}`; unit tests cover nesting, type coercion, and the error paths (`REG-02`, `REG-03`).
  4. A `Windowed<Operator>` wrapper with event-time bucketing (default cap 64, `ceil(window_ms/64)` width, lazy rollover on apply) exists, is covered by unit tests for edge-of-window and out-of-order event_time within bucket (`WIN-01`, `WIN-02`, `WIN-04`).
  5. "Lifetime" windowless mode is supported when `window_ms` is omitted and is the default for operators that declare themselves non-windowed (`WIN-03`).

**Plans:** TBD

---

### Phase 3: Core aggregates + push/get API surface

**Goal:** The full v0 HTTP surface — register, push, batch get, single get — is wired end-to-end for the 9 core numeric aggregates (count, sum, avg, min, max, stddev, variance, z_score, ratio) over a fraud-shape demo stream. A user can run the curl quickstart from start to finish against an in-memory, non-durable server. This phase proves the apply-loop architecture on realistic operators before durability lands.

**Depends on:** Phase 2.

**Requirements:**
- `API-03`, `API-04`, `API-05`, `API-06`, `API-07`, `API-08`
- `PRIM-CORE-01`, `PRIM-CORE-02`, `PRIM-CORE-03`, `PRIM-CORE-04`, `PRIM-CORE-05`
- `TEST-01` (table-driven primitive tests established here; extended in later primitive phases)
- `TEST-05` (windowing determinism test lives with the first real windowed operators)

**Note:** `API-03` here asserts the *shape* of the push path (accepts event, validates against schema, returns `{ack_lsn, idempotent_replay}`). The *durability* half of API-03 ("ACK only after WAL fsync") is finalized in Phase 4 when the WAL lands; in Phase 3 the apply loop is in-memory-only and `ack_lsn` is a monotonic in-memory counter.

**Success Criteria** (what must be TRUE):
  1. `POST /push/{stream}` accepts a JSON event, validates it against the registered schema (400 on mismatch naming the failing field), dispatches to all registered operators atomically on the apply-loop thread, and returns `{ack_lsn, idempotent_replay:false}` (`API-03`, `API-04`).
  2. `POST /get` with `{keys, features}` returns a `{key:{feature:value}}` map; over-cap requests (`keys × features > 10000`) return 413 with the cap in the body; `GET /get/{feature}/{key}` returns `{value}` or `{value, meta}` for structured features (`API-05`, `API-06`, `API-07`).
  3. Unknown feature name on either get endpoint returns 400; unknown entity key returns the feature's documented zero/null (e.g. `count` → 0, `min` → null) — documented in the per-primitive test table (`API-08`).
  4. The 9 core aggregates produce mathematically correct values under a table-driven test matrix (happy path, empty window, single event, all-filtered, numeric-edge cases); `stddev`/`variance` use Welford's running algorithm and match a numpy oracle within 1e-9 relative error (`PRIM-CORE-01..05`, `TEST-01`).
  5. A replay test pushes the same event stream twice (fresh server each run) and asserts byte-identical feature values across 9 primitives over a 4-window range, establishing windowing determinism (`TEST-05`).

**Plans:** TBD

---

### Phase 4: WAL + idempotency

**Goal:** `POST /push` is crash-safe. Every event is appended to an on-disk WAL, fsynced via group-commit, and only then does the client receive an ACK containing the fsynced `ack_lsn`. Stream-level idempotency (`idempotency_key` + TTL) returns the cached, byte-identical response on duplicate request_id within TTL without re-applying state.

**Depends on:** Phase 3.

**Requirements:**
- `STREAM-02`, `STREAM-03`
- `DUR-01`, `DUR-02`, `DUR-03`, `DUR-04`
- `TEST-04` (idempotency test lives with the idempotency feature)

**Success Criteria** (what must be TRUE):
  1. A per-instance append-only WAL file is written; group-commit fsync fires every 1-5ms or 1MB (whichever first); push ACK latency waits for the fsync past the event's LSN (`DUR-01`, `DUR-02`).
  2. The WAL record format carries `schema_version`, `stream_id`, `event_time`, `entity_key`, and the typed event body; a decoder reads any prior WAL byte-for-byte and reproduces the event (`DUR-03`).
  3. WAL segment rotation works: after a snapshot covers LSNs up to X, segments entirely below X are deleted on the next rotation cycle (`DUR-04`).
  4. A stream declared with `idempotency_key: <field>` + `idempotency_ttl_ms` returns the byte-identical PushResponse with `idempotent_replay: true` on duplicate key within TTL; state is not mutated; tests verify both the response equality and that downstream feature values are unchanged (`STREAM-02`, `STREAM-03`, `TEST-04`).
  5. A WAL-only crash test (restart process without a snapshot, full WAL replay) reproduces pre-crash feature values exactly for the 9 core aggregates — this is the "WAL works" gate before snapshotting (`DUR-01..04`).

**Plans:** TBD

---

### Phase 5: Snapshot + recovery + schema evolution

**Goal:** Server state survives restart within the documented RTO. A periodic snapshot writes the full in-memory state to disk; recovery loads the latest snapshot and replays the WAL from its covered LSN to present. Schema evolution (versioned row headers, last-8 schema retention, on-read migration) is implemented so older WAL records remain replayable after schema bumps.

**Depends on:** Phase 4.

**Requirements:**
- `STREAM-04`, `STREAM-05`
- `RECOV-01`, `RECOV-02`, `RECOV-03`, `RECOV-04`
- `TEST-03` (crash-recovery test)
- `TEST-06` (throughput bench harness — lives here because snapshot+WAL is now the true server shape; used as a regression harness, full target met in Phase 9)

**Success Criteria** (what must be TRUE):
  1. A periodic snapshot (default 30s; configurable) serializes the full in-memory state (streams, features, per-entity state, idempotency cache) to disk with a checksum; corrupt snapshots or WAL segments fail cleanly with operator-readable errors and can be rolled back to the previous snapshot (`RECOV-01`, `RECOV-04`).
  2. Recovery on boot loads the latest snapshot, replays the WAL from the snapshot's covered LSN to the WAL tail, and marks `/ready` 200; a 10GB-state target on NVMe completes recovery in under 30s (benchmarked in a harness; full verification in Phase 9 if hardware constrained) (`RECOV-02`, `RECOV-03`).
  3. Row headers carry `schema_version: u8`; the server retains the last 8 schemas; a WAL written under schema v1 is replayable by a binary running schema v3 via on-read migration; additive field changes migrate silently, breaking changes require an explicit `schema_version` bump at register time (`STREAM-04`, `STREAM-05`).
  4. A kill-9-mid-push crash-recovery test (process killed between WAL fsync boundaries and between snapshots) restarts, recovers, and produces feature values byte-identical to a control run where the process was not killed (`TEST-03`).
  5. A throughput benchmark harness exists that drives synthetic events at the server and reports server-truth EPS (counted at the apply loop, not client submissions); this harness runs in CI as a smoke benchmark and will be driven to target in Phase 9 (`TEST-06`).

**Plans:** TBD

---

### Phase 6: Recency, decay, and velocity primitives

**Goal:** 19 temporal-shaped primitives (recency/identity, decay, velocity/trend) land on the existing apply loop, durability stack, and snapshot+recovery path. Each primitive has a table-driven test fixture, a docs entry, and verified replay determinism under WAL-only and snapshot+WAL recovery.

**Depends on:** Phase 5. (Can run in parallel with Phases 7 and 8 — see Parallelization section.)

**Requirements:**
- Recency (8): `PRIM-RECENCY-01`, `PRIM-RECENCY-02`, `PRIM-RECENCY-03`, `PRIM-RECENCY-04`, `PRIM-RECENCY-05`, `PRIM-RECENCY-06`, `PRIM-RECENCY-07`, `PRIM-RECENCY-08`
- Decay (4): `PRIM-DECAY-01`, `PRIM-DECAY-02`, `PRIM-DECAY-03`, `PRIM-DECAY-04`
- Velocity (7): `PRIM-VEL-01`, `PRIM-VEL-02`, `PRIM-VEL-03`, `PRIM-VEL-04`, `PRIM-VEL-05`, `PRIM-VEL-06`, `PRIM-VEL-07`

**Success Criteria** (what must be TRUE):
  1. All 19 primitives compute correct values under table-driven test fixtures: for each primitive, at least one happy-path, one empty-input, and one edge-case scenario pass against a hand-computed oracle (or numpy/statsmodels for stats shapes like trend/regression).
  2. Decay primitives (`ewma`, `ewvar`, `ew_zscore`, `decayed_sum`, `decayed_count`, `twa`) produce values matching the closed-form formulas within 1e-9 relative error at matching `half_life_ms`; replay of the same event stream on a fresh server produces byte-identical values.
  3. `first_seen_in_window` (`PRIM-RECENCY-08`) — bloom + timestamp combo — is exercised for both true positives and bounded false-positive rate (documented configurable FPR); tests assert FPR under the configured ceiling across a 1M-event fixture.
  4. Every primitive in this phase survives a snapshot+WAL crash-recovery cycle with byte-identical post-recovery values (piggybacks on the Phase 5 harness; adds 19 primitive-specific scenarios).

**Plans:** TBD

---

### Phase 7: Bounded-buffer and geo primitives

**Goal:** 14 primitives with structured / bounded-state backing (histograms, deques, reservoirs, geo) land. Special attention to: (a) bounded memory under adversarial input, (b) structured return shapes through `GET /get/{feature}/{key}` → `{value, meta}`, (c) geo-math correctness against a reference implementation.

**Depends on:** Phase 5. (Can run in parallel with Phases 6 and 8.)

**Requirements:**
- Bounded buffers (8): `PRIM-BUF-01`, `PRIM-BUF-02`, `PRIM-BUF-03`, `PRIM-BUF-04`, `PRIM-BUF-05`, `PRIM-BUF-06`, `PRIM-BUF-07`, `PRIM-BUF-08`
- Geo (6): `PRIM-GEO-01`, `PRIM-GEO-02`, `PRIM-GEO-03`, `PRIM-GEO-04`, `PRIM-GEO-05`, `PRIM-GEO-06`

**Success Criteria** (what must be TRUE):
  1. All 14 primitives compute correct values under table-driven test fixtures including structured-return cases: `most_recent_n` returns a JSON array of length ≤ N; `dow_hour_histogram` returns a 168-bin map; `geo_velocity` returns `{value: km_h, meta: {from, to}}` or similar documented shape.
  2. Bounded-buffer primitives (`most_recent_n`, `reservoir_sample`, `histogram`, `hour_of_day_histogram`, `dow_hour_histogram`, `time_since_last_n`) stay within their declared memory bounds under a 10M-event adversarial fixture — memory does not grow unbounded (CI assertion).
  3. Geo primitives match a reference Haversine / geohash implementation within documented tolerance; `geo_velocity`, `geo_distance`, `geo_spread`, `unique_cells`, `geo_entropy`, `distance_from_home` each have their own oracle table fixture.
  4. All 14 primitives survive the Phase 5 crash-recovery harness with byte-identical post-recovery values (serialization of structured state — deques, reservoirs, geohash sets — is covered).

**Plans:** TBD

---

### Phase 8: Sketch primitives

**Goal:** The 5 sketch primitives (`distinct` HLL, `bloom_member`, `quantile` DDSketch, `top_k` SpaceSaving, `entropy`) land with configurable accuracy/memory trade-offs, documented error bounds, and crash-recovery correctness. Sketches are the highest-risk serialization target — they go last so the snapshot format is exercised on every prior primitive before sketch bytes join it.

**Depends on:** Phase 5. (Can run in parallel with Phases 6 and 7 but best-sequenced last because of serialization scrutiny.)

**Requirements:**
- Sketches (5): `PRIM-SKETCH-01`, `PRIM-SKETCH-02`, `PRIM-SKETCH-03`, `PRIM-SKETCH-04`, `PRIM-SKETCH-05`

**Success Criteria** (what must be TRUE):
  1. `distinct` (HLL) estimates cardinality within documented error bound (default 2% at p=14) across a 1M-entry test; merge correctness over bucket rollover produces results equivalent to a fresh compute within error bound.
  2. `quantile` (DDSketch) produces p50/p95/p99 within 1% of ground truth on a 100K-sample fixture; configurable `q` levels are respected; windowed merge works correctly.
  3. `top_k` (SpaceSaving) — top-K matches ground truth on the stationary portion of a Zipf distribution within documented approximation guarantees; `bloom_member` FPR stays under the configured rate on a 10M-insert fixture; `entropy` matches closed-form Shannon entropy.
  4. All 5 sketches survive the Phase 5 crash-recovery harness with estimates that are equal (to the bit) to the pre-crash sketch bytes — the serialization format is exercised byte-for-byte.

**Plans:** TBD

---

### Phase 9: Observability + performance hardening

**Goal:** The server is operationally monitorable (Prometheus metrics, health/ready, structured logs with trace_id) and provably meets the three PERF requirements: ≥3M EPS/core apply loop, P50 <2ms / P99 <10ms batch-get on warm cache, and <2ms P50 WAL group-commit latency overhead on push ACK.

**Depends on:** Phases 6, 7, 8 (requires the full primitive catalogue in the loop for representative benchmarking).

**Requirements:**
- Observability (4): `OBS-01`, `OBS-02`, `OBS-03`, `OBS-04`
- Performance (3): `PERF-01`, `PERF-02`, `PERF-03`

**Note:** `API-03`'s durability half (ACK only after WAL fsync) was delivered in Phase 4; here we prove the *latency* overhead meets the documented target.

**Success Criteria** (what must be TRUE):
  1. `/metrics` exposes Prometheus text format with: per-primitive counters, push throughput, batch-get + single-get QPS, p50/p95/p99 latency histograms per endpoint, WAL group-commit latency histogram, snapshot latency, recovery time; a local Prometheus scrape against a running beava produces all labels (`OBS-01`).
  2. `/health` returns 200 whenever the process is up; `/ready` returns 200 only after recovery is complete; structured JSON logs emit INFO/WARN/ERROR with `trace_id` propagated from `X-Trace-Id` on every request (`OBS-02`, `OBS-03`, `OBS-04`).
  3. The perf harness (from Phase 5) sustains ≥3M EPS/core on a 32-byte event × 5-primitive workload for 60s+ measured via `server_processed_events` at the apply loop, on modern server-class NVMe hardware; report includes hardware spec and flamegraph (`PERF-01`).
  4. Batch get of 100 features × 1 entity against a warm cache returns P50 <2ms, P99 <10ms across a 10-minute soak; single-get P99 <5ms across the same soak (`PERF-02`).
  5. WAL group-commit adds P50 <2ms, P99 <10ms to push ACK latency at the default 1-5ms commit window under 3M EPS sustained write load (`PERF-03`).

**Plans:** TBD

**UI hint:** no (observability is backend-only; Prometheus is the consumer; no beava-owned dashboards in v0)

---

### Phase 10: Python SDK, docs, and packaging — ship

**Goal:** The OSS launch: `pip install beava` works on PyPI, docs site has the full quickstart and primitive catalogue, prebuilt binaries for linux/amd64, linux/arm64, darwin/arm64 on GitHub Releases, Docker image on ghcr.io, README drives a 3-command smoke, LAUNCH.md is ready to post.

**Depends on:** Phase 9.

**Requirements:**
- Python SDK (5): `SDK-01`, `SDK-02`, `SDK-03`, `SDK-04`, `SDK-05`
- Docs (5): `DOC-01`, `DOC-02`, `DOC-03`, `DOC-04`, `DOC-05`
- Packaging (4): `PKG-01`, `PKG-02`, `PKG-03`, `PKG-04`
- `API-01` (re-verified end-to-end through SDK surface), `API-02` (re-verified)

**Note:** `API-01` / `API-02` are shipped in Phase 2 at the server level; they are re-listed here as covered-through-SDK to close the loop but are not double-counted in traceability (primary phase = 2; SDK test coverage is secondary). Traceability table reflects primary mapping only.

**Success Criteria** (what must be TRUE):
  1. `pip install beava` installs the Python SDK; it exposes `push`, `push_batch`, `get`, `get_batch`, `register` sync methods plus a fire-and-forget enqueuer that flushes on timer/buffer threshold with no persistent connection and no callbacks; no required deps beyond stdlib + `requests` (`SDK-01..04`). Integration tests hit a real running beava over HTTP (`SDK-05`).
  2. `docs/quickstart.md` walks through a fraud-scoring demo in ≤10 curl commands with working copy-paste output in <5 minutes on a clean machine (automated doctest in CI); `docs/primitives.md` lists all 40 primitives with JSON example, example return, and a one-line use case; `docs/http-api.md` documents every endpoint with request/response and error codes; `docs/architecture.md` describes the apply loop, WAL group-commit, snapshot recovery, memory sizing (`DOC-01..04`).
  3. `README.md` at repo root links to the docs site (`beava.dev`) and contains a 3-command smoke demo (`docker run ... && curl register && curl push && curl get`) that runs cleanly (`DOC-05`).
  4. GitHub Releases carries prebuilt binaries for linux/amd64, linux/arm64, darwin/arm64; each binary is ≤200MB stripped and has no non-libc runtime dependencies; Docker image `ghcr.io/petrpan26/beava:v0` is published with a zero-config entrypoint that boots on `docker run ghcr.io/petrpan26/beava:v0` alone (`PKG-01`, `PKG-02`, `PKG-04`).
  5. Configuration works via env vars (`BEAVA_DATA_DIR`, `BEAVA_PORT`, `BEAVA_WAL_COMMIT_MS`, etc.) and an optional single YAML file; no external config store is required; all config knobs are documented in `docs/architecture.md` (`PKG-03`).

**Plans:** TBD

**UI hint:** no (docs site is markdown served via GitHub Pages or similar, not a beava-owned UI; the repo does not own a dashboard)

---

## Progress

| Phase | Plans Complete | Status | Completed |
|-------|----------------|--------|-----------|
| 1. Foundation | 0/? | Not started | - |
| 2. Primitive infra + registration | 0/? | Not started | - |
| 3. Core aggregates + push/get | 0/? | Not started | - |
| 4. WAL + idempotency | 0/? | Not started | - |
| 5. Snapshot + recovery | 0/? | Not started | - |
| 6. Recency/decay/velocity primitives | 0/? | Not started | - |
| 7. Bounded buffers + geo primitives | 0/? | Not started | - |
| 8. Sketch primitives | 0/? | Not started | - |
| 9. Observability + performance | 0/? | Not started | - |
| 10. Python SDK + docs + packaging | 0/? | Not started | - |

Plan counts populated by `/gsd-plan-phase` as each phase is planned.

---
*Roadmap created: 2026-04-22 from `.planning/PROJECT.md` + `.planning/REQUIREMENTS.md` (100 v1 REQ-IDs) + `DESIGN-V2.md`.*
