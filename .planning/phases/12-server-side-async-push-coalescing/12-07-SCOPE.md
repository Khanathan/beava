---
phase: 12-server-side-async-push-coalescing
plan: 07
type: scope-not-yet-planned
captured: 2026-04-29
status: BLOCKER-FOR-READ-BENCH
discovered_via: Phase 19.4 read-bench harness `python/benches/read_bench.py` (commit `a116d87`)
---

# Plan 12-07 (proposed) — Wire `/get` into the mio HTTP fast path

## Diagnosis

**Bug:** `POST /get` and `GET /get/:feature/:key` return 404 on the production `beava` binary.

**Root cause:**
- Handler logic exists at `crates/beava-server/src/feature_query.rs:99-289`
  - `BatchGetRequest { keys: Vec<String>, features: Vec<String> }` request shape
  - `post_get_batch_handler` + `get_feature_handler` implementations
- `feature_query_router` builder exists at line 81-86 — defines the two routes
- BUT: the only callsite of `feature_query_router` is in `crates/beava-server/src/http.rs:133` via `.merge(feature_query_router(...))` inside `router_with_push`
- The `beava` binary uses a **mio-based hand-rolled HTTP parser** (Phase 18 hot path) at `crates/beava-server/src/server.rs:949+` — `EventLoop::tick` orchestrates mio Poll → IoPool parse → ApplyShard dispatch
- The hand-rolled parser ONLY routes:
  - `POST /push/{event}` → apply path
  - Admin paths via tokio runtime side-car: `/health`, `/ready`, `/metrics`, `/registry`
- `/get` falls through to a generic 404

**Discovered via:** Phase 19.4 read-bench harness ran 3× successfully but every `POST /get` returned 404 with empty body. ~21k requests/sec was achieved — that's how fast the server can REJECT 404 requests, not the actual read throughput.

**This is Phase 12 PARTIAL leftover work.** Per ROADMAP, "Plans 12-01, 12-03, 12-04, 12-05, 12-06 pending on `.claude/worktrees/phase-12-followup`" — but the /get wire-up isn't explicitly called out in any of those plans. Surfacing now as Plan 12-07.

## Proposed fix paths (ranked by ROI)

### Path B1 — Wire `/get` into mio HTTP fast path (RECOMMENDED, ~1-2 days)

Mirror the `POST /push/{event}` routing in the mio HTTP parser to also recognize:
- `POST /get` — body parse to `BatchGetRequest`, dispatch to read path on the apply thread (which has the AggStateTable read access), serialize response
- `GET /get/:feature/:key` — parse path params, dispatch to read path

Files to modify:
- `crates/beava-server/src/server.rs:949+` (mio EventLoop tick) — add route dispatch for /get
- `crates/beava-server/src/iopool/parse.rs` (or wherever `parse_http_envelope` lives) — recognize /get in the HTTP method+path parser
- `crates/beava-server/src/apply_shard.rs` — add a read-dispatch helper alongside the existing push-dispatch (apply thread already holds the AggStateTable read lock; reads should be ~free)
- `crates/beava-core/src/feature_query.rs` — possibly factor `post_get_batch_handler`'s logic into a non-axum function (`fn batch_get_pure(state: &..., body: BatchGetRequest) -> BatchGetResponse`) so both axum and mio paths can call it
- Tests in `crates/beava-server/tests/`

Acceptance criteria:
- `python/benches/read_bench.py --pipeline crates/beava-bench/configs/fraud-team.json --total-reads 50000 --warmup-events 100000` produces **non-zero `ok` count** (was 0/50000 in Phase 19.4 read-bench attempt)
- `httpx.get("/get/{feature}/{key}")` returns 200 + JSON body
- `httpx.post("/get", json={"keys": [...], "features": [...]})` returns 200 + JSON map
- Integration test `tests/phase12_07_get_via_mio_test.rs` covers both endpoints
- No regression on push EPS — apply thread cost for read should be measurable but bounded (target: ≤ 5% push regression)
- Read latency p99 ≤ 10ms per CLAUDE.md target

### Path B2 — Spawn axum sidecar on tokio runtime for `/get` (~4-8 hours, simpler but hacky)

Reuse the existing tokio admin runtime; mount `feature_query_router` on a sidecar port:
- `crates/beava-server/src/server.rs` admin runtime block — `.merge(feature_query_router(...))`
- Bind to `--http-port + 1` (e.g., 8080 main, 8081 reads)

Cons:
- Dual-port deployment (config complexity)
- Cross-thread state sharing — feature reads need read-only access to AggStateTable; lock contention with apply thread
- Doesn't match production routing intent

Pros:
- No mio HTTP parser modification
- Reuses existing axum router code 1:1

### Path B3 — Hybrid: route in mio, dispatch via channel (~3-5 days)

Mio parser sees /get, posts a read-request item to apply thread's queue, apply thread responds out-of-band. Most flexible but most complex. Not recommended.

## Recommendation

**Path B1.** Phase 12 is in the v0 critical path (joins + push/get API). The mio fast path is the production hot path; /get must live there for sub-ms latency. Path B2's axum sidecar adds operational complexity and lock contention; Path B3 is over-engineered.

## Effort estimate

- Path B1: 1-2 days focused work — mio parser extension + factor pure handler + apply-thread dispatch + integration tests + read bench validation
- Bundles cleanly with Phase 12 follow-up work already on `.claude/worktrees/phase-12-followup`

## Why this matters for Phase 19.4 / read benchmarks

Phase 19.4 closed the **write path** at PASS (102.8k EPS on fraud-team). The **read path is currently UNMEASURABLE** because /get returns 404. The "Beava vs Redis" comparison story (Beava being 4-7× faster on multi-feature events) is incomplete without read benchmarks.

Once Plan 12-07 lands, Phase 19.4's `python/benches/read_bench.py` (committed `a116d87`) can run unmodified to produce the read baseline.

## Cross-references

- Discovery context: this session, after Phase 19.4 closure
- Existing infra: `feature_query.rs` handler logic (lives), `feature_query_router` (built but unmounted in production binary), `read_bench.py` (committed but currently 404s)
- Memory `project_phase18_no_dual_runtime`: hand-rolled mio is the only data-plane runtime — Path B1 is the architecturally-correct fix
- v0 critical path queue: Phase 14 → 15 → 14.1 → 12-followup → 12.5 → 16 → 13-followup → ship; this scope doc seeds the 12-followup work

## Status

- **NOT YET PLANNED** — needs `/gsd-plan-phase 12` (or scoped `/gsd-discuss-phase` + plan) to break into red-green tasks
- **NOT YET EXECUTED** — fresh session needed; this session ran out of context budget
- **Currently blocking:** Phase 19.4 read-bench measurement runs (cannot complete without server-side fix)
