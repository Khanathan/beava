# Requirements: Beava — milestone v1.0-launch

**Defined:** 2026-04-17
**Core Value:** A skeptical engineer evaluating Beava on github.com can go from landing on the repo to correct, live feature values in under 60 seconds — from any language.

## v1 Requirements

Requirements for the **v1.0-launch** milestone (Public Launch Readiness). Each maps to roadmap phases 45–44 derived from the three LAUNCH.md blocks plus a ship gate. REQ-IDs use `[CATEGORY]-[NN]`.

### HTTP — HTTP Ingest & Read API (Phase 45, Block 1)

- [ ] **HTTP-01**: A user can create a single event via `POST /push/{stream}` with a JSON body and receive a 2xx on accept, a structured 4xx on schema mismatch, and a 413 when the body exceeds `BEAVA_HTTP_MAX_BODY`.
- [ ] **HTTP-02**: A user can push a JSON array of events via `POST /push-batch/{stream}`, receive one response summarizing per-event accept/reject, and observe each event appear under its correct event-time bucket (validates the 2a fix from the client side).
- [ ] **HTTP-03**: A user can stream events via `POST /push/{stream}/ndjson` using chunked transfer, parsed line-by-line via `axum-extra::JsonLines`, without the 2 MiB default body limit truncating the upload.
- [x] **HTTP-04**: A user can query features for an entity via `GET /features/{key}` and receive the current values across all tables, with optional `?table=X` to filter to a single table.
- [x] **HTTP-05**: A user can list registered streams via `GET /streams` and inspect one via `GET /streams/{name}`, returning the stream's schema and current watermark.
- [x] **HTTP-06**: A user's write requests to `/push*` are rejected with 401 when the admin token is missing and accepted from loopback without a token — inheriting the existing `require_loopback_or_token` middleware unchanged.
- [x] **HTTP-07**: A user can serve `/features/*` and `/streams/*` read endpoints from the public router when the server is started with `--public`, while writes remain admin-only.
- [x] **HTTP-08**: A user can reproduce the `docker run ... → curl POST /push → curl GET /features` flow from the repo's `examples/curl-ingest/` directory without editing configuration.
- [x] **HTTP-09**: A user can drive sustained **>100 K EPS** on `/push-batch/{stream}` from a single client, measured by `oha` against the reference box, and record the number in `benchmark/README.md`.
- [x] **HTTP-10**: A developer can reference `docs/http-api.md` and find working `curl`, Go (`net/http`), and Node (`fetch`) examples for each of the 6 endpoints above.

### CORR — Correctness Fixes (Phase 46, Block 2 — 2a, 2c, 2d.*)

- [x] **CORR-01**: A developer running `push_batch_with_cascade_no_features` can no longer pass a shared `now` across a batch — the function signature accepts `&[(&Value, SystemTime)]` and internally groups events by event-time bucket (one lock per group). Validated by a property test: N events through single-event path vs batch path produce identical per-bucket aggregates.
- [x] **CORR-02**: A user running the 9-cell benchmark matrix after the 2a fix sees results within **−5%** of the committed v2.0 BASELINE; if not, the fix is not merged. *(Spot check complex-c8-x8 +10.48%; full matrix deferred pending run_matrix.sh OUTPUT_DIR fix)*
- [x] **CORR-03**: A user defining a stream can set a per-stream `watermark_lateness` (`@bv.stream(watermark_lateness="10m")`), stored in `StreamDefinition`, propagating through the engine; absent field defaults to 5 s.
- [x] **CORR-04**: A stream-definition snapshot from before this change loads cleanly with the default 5 s lateness (schema-migration tolerance).
- [ ] **CORR-05**: A maintainer can close audit item **2d.i** with a verification test proving `run_backfill` uses `push_for_backfill` (single-event path), **not** `handle_push_batch` — closes as "not a bug, verified."
- [x] **CORR-06**: A user running backfill via the event log sees each event bucketed by its payload `_event_time`, not by the log entry's wall-clock timestamp — closes audit item **2d.ii**. Validated by property test: push → crash → recover → feature values identical to a live-ingest baseline.
- [x] **CORR-07**: A user ingesting 30-day-old historical events does not see `entity_ttl` immediately evict the entity — eviction clock sources from `WatermarkTracker::observed_max(stream)`, not wall-clock — closes audit item **2d.iii**.
- [x] **CORR-08**: A user running a fork replica that cascades into downstream tables sees watermarks advance on the downstream — `replica_ingest_batch` calls `watermarks.observe()` per event — closes audit item **2d.iv**.
- [x] **CORR-09**: A maintainer can close audit item **2d.v** with a one-paragraph note in `docs/event-time.md` documenting that joins require both sides producing events in v1; per-stream idle markers are explicitly deferred to v1.1.
- [x] **CORR-10**: A developer can no longer observe a race where a writer calls `mark_dirty(k)` under the old gen while `clear_dirty()` has already advanced the gen — fixed via atomic swap of the dirty set in `take_dirty_and_advance_gen()` — closes audit items **2d.vi** and **2d.vii** together. Regression is within 2% on the 9-cell bench.

### OBS — Observability & Correctness Documentation (Phase 46, Block 2 — 2b + 2e docs)

- [x] **OBS-01**: A user can observe a new Prometheus counter `beava_ring_buffer_drops_total{stream, operator_kind, reason}` with bounded labels (`reason ∈ {too_old, too_new, pre_epoch}`; `operator_kind` not per-instance UUID); counters are cached at operator registration to keep hot-path overhead below 100 ns.
- [x] **OBS-02**: A user can tell from metrics alone whether an event was dropped by the watermark (existing `beava_late_events_dropped_total`) or the ring buffer (new `beava_ring_buffer_drops_total`) — the two counters are mutually exclusive; an integration test asserts exclusivity.
- [x] **OBS-03**: A user can open `docs/event-time.md` and understand event-time semantics (bucket assignment, watermark lateness, crash-replay determinism, TTL vs event-time, join idle-input behavior, fork watermark propagation) in one page.

### INFRA — Docker, CI, Repo Hygiene (Phase 47, Block 3 — 3a/3c/3e/3g)

- [ ] **INFRA-01**: A user can run `docker run -p 6900:6900 beavadb/beava:latest` and see a working Beava server — image is published to Docker Hub with `:latest` + `:0.1.0` tags.
- [ ] **INFRA-02**: A user can `docker compose up` against `examples/docker-compose.yml` to get a Beava container with a mounted data volume and exposed port.
- [ ] **INFRA-03**: A maintainer sees `cargo fmt --check`, `cargo clippy --all-targets -- -D warnings`, and `cargo test --lib` all green on every push and PR via `.github/workflows/ci.yml` (<5 min run time).
- [ ] **INFRA-04**: A user sees a green CI badge linked to GitHub Actions at the top of README.md.
- [ ] **INFRA-05**: A maintainer builds the image from the repo root using a multi-stage Dockerfile (cargo-chef planner/cooker/builder → `gcr.io/distroless/cc-debian12:nonroot`); image runs as non-root.
- [ ] **INFRA-06**: A maintainer has resolved every `TODO` / `FIXME` / `XXX` in `src/` — each is either fixed, converted to a GitHub issue, or marked load-bearing with a tracking-issue link.
- [ ] **INFRA-07**: A maintainer has no stray `println!` / `dbg!` / `eprintln!` calls in `src/` except explicitly-intentional startup logging or profile instrumentation.
- [ ] **INFRA-08**: A maintainer sees `#![warn(missing_docs)]` enabled on the crate root and every `pub fn` / `pub struct` in `src/lib.rs` exports has at least a one-line doc comment.
- [ ] **INFRA-09**: A visitor browsing the GitHub repo sees an accurate repo description, 5–8 topics (`feature-server`, `real-time`, `rust`, `streaming`, `ml`, `apache-2-0`, …), and a 1280×640 social preview PNG.
- [ ] **INFRA-10**: A maintainer has audited `LICENSE`, `CODE_OF_CONDUCT.md`, `SECURITY.md`, `CONTRIBUTING.md`, `GOVERNANCE.md`, `MAINTAINERS.md` — each is present, current, and accurate (especially the "bus factor 1" disclosure).

### CONTENT — Docs, README, Examples (Phase 47, Block 3 — 3a/3b/3d/3f)

- [ ] **CONTENT-01**: A visitor opens `README.md` and sees a <60-line document leading with a 5-line HTTP copy-paste demo (`curl -X POST http://localhost:6900/push/...`), followed by the fork demo, then links to docs + architecture.
- [ ] **CONTENT-02**: A user can open `docs/getting-started.md` and go from `docker run` → pushing an event → reading a feature value in under 60 seconds on a fresh machine.
- [ ] **CONTENT-03**: A user can read `docs/concepts.md` and understand streams, tables, operators, and the fork model in one sitting.
- [ ] **CONTENT-04**: A user can reference `docs/python-sdk.md` and find the Python API reference (decorators, client, types) for the existing SDK.
- [ ] **CONTENT-05**: A user can read `docs/operations.md` and plan sizing, durability guarantees, crash-recovery, and tuning before deploying Beava.
- [ ] **CONTENT-06**: A user can read `docs/architecture.md` and understand Beava's single-node design, why it's single-binary, and the scaling posture (thread-per-core / multi-node as v1.2+).
- [ ] **CONTENT-07**: A user can read `docs/faq.md` and find honest answers to "will it scale?", "what about Flink?", and "is this production-ready?"
- [ ] **CONTENT-08**: A user can run `examples/fraud-scoring/` — a working fraud-pipeline project defining streams + tables, pushing synthetic traffic, querying features — against a freshly-started Beava Docker container.
- [ ] **CONTENT-09**: A user can run `examples/session-features/` — a simpler keyed-stream example (last-N-click features, count + sum aggregations).
- [ ] **CONTENT-10**: A user can run `bash examples/curl-ingest/run.sh` and see the HTTP API exercised end-to-end.
- [ ] **CONTENT-11**: A visitor can find directory READMEs at `src/server/`, `src/engine/`, `src/state/`, `benchmark/`, `deploy/` — each explaining the module's role in 1–2 paragraphs.

### SHIP — Launch Ship Gate (Phase 46 close + cross-phase)

- [x] **SHIP-01**: A maintainer can run a single integration test that exercises `HTTP push → crash → recover → read features` and confirms feature values match a live-ingest baseline (validates CORR-01, CORR-05, CORR-06 simultaneously).
- [ ] **SHIP-02**: A maintainer can reproduce an end-to-end smoke test on a fresh AWS/Fly.io VM: install from public source → run one example → push events via HTTP → read features → kill process → recover → confirm data survived. Time-to-first-success recorded; target <60 seconds.
- [ ] **SHIP-03**: A maintainer has re-verified `benchmark/` numbers (ingest, recovery, fork-replay) on the current tree and they reproduce the committed v2.0 BASELINE within −5%.
- [ ] **SHIP-04**: A maintainer has re-audited `.planning/outreach/LAUNCH-PACKAGE-V8.md` against `AUDIT-V11.md` for fabricated claims — no unverifiable benchmark, no overpromised scaling story.
- [ ] **SHIP-05**: A maintainer has recorded a ~3-minute video or GIF of the 60-second quickstart and linked it from README.

## v2 Requirements

Acknowledged and tracked post-launch; not in this milestone's roadmap.

### Scale
- **SCALE-01**: Thread-per-core runtime unlocking >350 K EPS per node (v1.2 roadmap)
- **SCALE-02**: Multi-node via Kafka horizontal scaling (v1.3+ roadmap)

### Extensibility
- **EXT-01**: UDF / stateful scripting via Rhai or WASM operator plugins (v1.2 roadmap)
- **EXT-02**: Stateless derive expressions (`FeatureDef::Derive`) fully exposed through Python SDK

### Developer Experience (post-launch DX polish)
- **DX-01**: CLI subcommands (`beava push`, `beava get`, `beava tail`)
- **DX-02**: OpenAPI / Swagger UI for the HTTP surface
- **DX-03**: Web UI for `/debug/*` endpoints
- **DX-04**: Deploy buttons (Fly.io, Railway, Render)
- **DX-05**: Dedicated docs site (mkdocs / docusaurus / vitepress) if content outgrows flat markdown
- **DX-06**: Per-stream idle markers to fix join watermark stalls (v1.1 fix for 2d.v)

### Advanced correctness
- **CORR-FUT-01**: Loom-based property tests for snapshot rollover races
- **CORR-FUT-02**: Outer joins (right / full)
- **CORR-FUT-03**: Session windows
- **CORR-FUT-04**: CEP / `match_recognize` patterns
- **CORR-FUT-05**: Table-input aggregation + full retraction propagation through DAG

## Out of Scope

Explicitly excluded for v1.0-launch. Anti-features from research are logged with reasoning to prevent re-adding.

| Feature | Reason |
|---------|--------|
| OpenAPI / Swagger UI at launch | Clean `docs/http-api.md` with curl + Go + Node examples is enough; generators add API-commitment before schema stabilizes. |
| Dedicated docs site (mkdocs / docusaurus) at launch | 3–5 days of setup + migration; Option A (flat `docs/` markdown) is sufficient for v1. Upgrade post-launch if docs outgrow it. |
| gRPC / Tonic | Adds schema-commitment and scope before the HTTP surface has shipped; HTTP is enough for launch audience. |
| WebSocket / SSE subscribe | Adds protocol surface; `OP_SUBSCRIBE` via TCP is already present for internal clients. |
| Built-in TLS | Users terminate TLS at Caddy / nginx / Fly.io edge; bundling TLS in the server adds cert-rotation complexity without launch benefit. |
| Vector / embedding search | Not the feature-server scope; Beava is numeric + categorical features. |
| `axum-prometheus` auto-metrics crate | Conflicts with the existing hand-rolled `/metrics` surface; auto-labels cause cardinality blowups (Pitfall 5). |
| Alpine / MUSL Docker base | MUSL allocator regresses push-batch throughput ~5–15% (Pitfall 14); distroless/cc glibc is strictly better. |
| `tracing` / `tracing-subscriber` overhaul | Cross-cutting refactor with no launch user benefit; existing `log` + `/metrics` + `/debug/warnings` suffice. |
| Release-binary GitHub workflow | Nice-to-have; `docker run` is the public install path for launch. Add post-launch when Linux/macOS binaries matter. |
| `cargo-deny` license/advisory scanning | Defer unless ship-window slack allows in week 3. |
| Deploy buttons (Fly.io / Railway / Render) | Pure polish; `docker run` + `deploy/provision.sh` (Hetzner) cover the v1 need. |
| Exactly-once semantics claim | At-least-once is the honest story; claiming exactly-once triggers audit scrutiny we'd lose. |
| `tally` → `beava` binary rename at launch | Decided: keep `tally` binary name for v1.0-launch to avoid doc churn; rename in v1.1. |
| Silent "100× faster than X" marketing claims | Anti-feature for credibility; reproducible benchmarks only. |
| Public roadmap with specific dates | Anti-feature: dates slip, credibility erodes; roadmap is phase-milestone only. |

## Traceability

Each v1 requirement maps to exactly one phase. Roadmap populated 2026-04-17.

| Requirement | Phase | Status |
|-------------|-------|--------|
| HTTP-01 | Phase 45 | Pending |
| HTTP-02 | Phase 45 | Pending |
| HTTP-03 | Phase 45 | Pending |
| HTTP-04 | Phase 45 | Complete |
| HTTP-05 | Phase 45 | Complete |
| HTTP-06 | Phase 45 | Complete |
| HTTP-07 | Phase 45 | Complete |
| HTTP-08 | Phase 45 | Complete |
| HTTP-09 | Phase 45 | Complete |
| HTTP-10 | Phase 45 | Complete |
| CORR-01 | Phase 46 | Complete |
| CORR-02 | Phase 46 | Complete (spot check; full matrix deferred) |
| CORR-03 | Phase 46 | Complete |
| CORR-04 | Phase 46 | Complete |
| CORR-05 | Phase 46 | Pending |
| CORR-06 | Phase 46 | Complete |
| CORR-07 | Phase 46 | Complete |
| CORR-08 | Phase 46 | Complete |
| CORR-09 | Phase 46 | Complete |
| CORR-10 | Phase 46 | Complete |
| OBS-01 | Phase 46 | Complete |
| OBS-02 | Phase 46 | Complete |
| OBS-03 | Phase 46 | Complete |
| SHIP-01 | Phase 46 | Complete |
| INFRA-01 | Phase 47 | Pending |
| INFRA-02 | Phase 47 | Pending |
| INFRA-03 | Phase 47 | Pending |
| INFRA-04 | Phase 47 | Pending |
| INFRA-05 | Phase 47 | Pending |
| INFRA-06 | Phase 47 | Pending |
| INFRA-07 | Phase 47 | Pending |
| INFRA-08 | Phase 47 | Pending |
| INFRA-09 | Phase 47 | Pending |
| INFRA-10 | Phase 47 | Pending |
| CONTENT-01 | Phase 47 | Pending |
| CONTENT-02 | Phase 47 | Pending |
| CONTENT-03 | Phase 47 | Pending |
| CONTENT-04 | Phase 47 | Pending |
| CONTENT-05 | Phase 47 | Pending |
| CONTENT-06 | Phase 47 | Pending |
| CONTENT-07 | Phase 47 | Pending |
| CONTENT-08 | Phase 47 | Pending |
| CONTENT-09 | Phase 47 | Pending |
| CONTENT-10 | Phase 47 | Pending |
| CONTENT-11 | Phase 47 | Pending |
| SHIP-02 | Phase 47 | Pending |
| SHIP-03 | Phase 47 | Pending |
| SHIP-04 | Phase 47 | Pending |
| SHIP-05 | Phase 47 | Pending |

**Coverage:**
- v1 requirements: 49 total (10 HTTP + 10 CORR + 3 OBS + 10 INFRA + 11 CONTENT + 5 SHIP)
- Mapped to phases: 49
- Unmapped: 0 ✓

---
*Requirements defined: 2026-04-17*
*Last updated: 2026-04-17 after roadmap creation — v1.0-launch traceability populated (Phases 45-47)*
