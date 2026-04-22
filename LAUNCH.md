# Launch Readiness — Three Blocks Before Public Launch

**Status**: internal planning doc. Not user-facing.

**Goal**: ship Beava as a public Apache 2.0 project that a skeptical engineer
can land on, trust, and try in 30 seconds. Three things stand between "works
internally" and "I'd put this in my eval list."

---

## Block 1 — HTTP ingest (DX unlock)

**Why it's a launch blocker**: today's only write path is the Python SDK over
binary TCP. Every non-Python engineer evaluating Beava hits the SDK wall in
the first 60 seconds of their evaluation. HTTP ingest + read is the single
highest-leverage DX investment we can make — turns Beava into a curl-able,
language-agnostic server that Go / Node / Java / Ruby / Vector / Lambda /
browsers can all talk to.

**Design doc**: `.planning/phases/HTTP-INGEST-DESIGN.md` (written).

**Scope** (v1.1):

- `POST /push/{stream}` — single event (JSON)
- `POST /push-batch/{stream}` — JSON array
- `POST /push/{stream}/ndjson` — newline-delimited streaming body
- `GET /features/{key}` — read all tables' features for a key
- `GET /features/{key}?table=X` — filter to one table
- `GET /streams`, `GET /streams/{name}` — list + schema

**Auth**: reuse existing `BEAVA_ADMIN_TOKEN` + `require_loopback_or_token`
middleware. Write on admin router, read on public router if `--public` set.

**Implementation effort**: ~2 weeks for one engineer. All infrastructure
already in place (axum, `handle_push_core_ex`, `public_features` pattern).
Reuse existing push path — no duplicated ingest logic.

**Ship criteria**:
- [ ] All 6 endpoints implemented
- [ ] Unit tests (10+ cases) green
- [ ] Integration test (curl-driven) passes E2E
- [ ] Load test shows >100 K EPS on a single HTTP batch stream
- [ ] Homepage curl demo copy-paste works from fresh Docker image
- [ ] HTTP API reference docs + Go/Node/curl examples

---

## Block 2 — Correctness audit and fixes

**Why it's a launch blocker**: the release audit this week surfaced one real
correctness bug and several silent-failure modes. A feature server that
silently misbuckets historical events is not something you publicly launch.
Fix the known issues, add metrics for the silent ones, and run a narrow
audit to make sure nothing else is hiding.

### 2a. Known bug: batch-path shared `now` for event-time bucketing

**Bug**: `handle_push_batch` (`src/server/tcp.rs:1731`) uses
`min_event_time.unwrap_or(batch[0].now)` as a single `now` passed to every
event in the batch. The inner `push_batch_with_cascade_no_features` loops
events with the same `now`, so operators assign every event to the bucket
containing the minimum event_time of the batch.

**Impact**:
- **Masked in production** because live batches are typically sub-millisecond
  wall-clock wide; events land in the same bucket anyway.
- **Real bug for backfill**: historical events spanning multiple buckets all
  get collapsed to the earliest bucket.
- **Real bug for sparse-batch clients**: clients that accumulate events for
  seconds+ before flushing would misbucket.

**Fix**: change `push_batch_with_cascade_no_features` to pass per-event
event_time into its inner loop. Two approaches in the design doc:
(a) take `&[(event, now)]` and loop per-event (simpler), or
(b) group events by bucket and issue one lock per group (composable with
the batch-coalescing work we already explored).

**Effort**: ~2 days including a correctness test ("event with `_event_time`
2 hours ago in a batch should land in the 2-hours-ago bucket, not the now
bucket"). Verify via property test: run N events through single-event path
vs batch path, assert identical per-bucket aggregates.

### 2b. Silent ring-buffer drops (metric needed)

**Issue**: `RingBuffer::update_at_event_time` / `add_at_event_time` silently
drop events that fall outside the current window — either because they're
older than `head - window_duration` or newer (out-of-order future). These
drops happen AFTER the watermark late-drop check passes, so `beava_late_events_dropped_total`
does NOT capture them. Users have no way to know they're losing data.

**Fix**: bump a new `beava_ring_buffer_drops_total{stream, operator, reason}`
counter when the ring rejects an event. Reasons: `too_old`, `too_new`
(pre-epoch or beyond head).

**Effort**: ~1 day. Mostly plumbing through the ring buffer callers.

### 2c. `WATERMARK_LATENESS` hardcoded at 5 s

**Issue**: `src/engine/event_time.rs:50` hardcodes 5 s. Different workloads
need different values — IoT (minutes of out-of-order tolerance), fraud
(sub-second tight watermarks). No way to configure today.

**Fix**: add `watermark_lateness: Option<Duration>` to `StreamDefinition`.
Default remains 5 s. Propagate through `@bv.stream(watermark_lateness="10m")`
in the Python SDK.

**Effort**: ~2 days including schema migration for existing snapshots
(tolerate absent field = 5 s default).

### 2d. Broader correctness audit items (verify before launch)

Items identified but not yet deeply verified — each is a ~1-day audit,
need confirmed or fixed:

**2d.i. Backfill path correctness.** Does `run_backfill` in main.rs feed
historical events through `handle_push_batch` (buggy) or via single-event
`handle_push_core_ex` (correct)? If it uses the batch path with historical
data, the 2a bug bites immediately. Verify and fix if needed.

**2d.ii. Crash-recovery determinism.** The event log stores
`LogEntry.timestamp: SystemTime`. On replay after crash, does the push path
use `LogEntry.timestamp` or re-parse `_event_time` from the payload body?
If wall-clock-at-append is used, replay produces different bucket assignments
than the original ingest. Verify replay uses payload's `_event_time` as the
source of truth. Add a property test: push → crash → recover → features
match original.

**2d.iii. TTL expiration timing.** `entity_ttl` and `history_ttl` — are they
evaluated against event_time or wall-clock? If wall-clock, ingesting 30-day-old
historical events could immediately trigger `entity_ttl < wall_clock - event_ts`
and evict entities that should still be live. Verify event-time semantics.

**2d.iv. Fork watermark propagation.** `replica_ingest_batch` does NOT call
`engine.watermarks.observe()` per event. That means a fork replica ingesting
from upstream doesn't track watermarks. If the fork then cascades to its own
tables, those downstream watermarks stall forever. Check whether fork pipelines
actually cascade; if yes, add watermark observation to the replica ingest path.

**2d.v. Join watermark idle-input problem.** `propagate_join = min(left, right)`
is correct when both sides produce events. What happens when one side is silent
for long periods? The join's downstream watermark stalls at the silent side's
last-observed event_time. Standard problem; Flink fixes it with per-stream
idle markers. For Beava v1, document the behavior (joins need both sides
producing) and defer a fix to v1.2.

**2d.vi. Snapshot consistency during live ingest.** Snapshotter iterates
DashMap and clones entities. DashMap shard-level locks give per-entity
atomicity, but what about cross-entity consistency? If the snapshot clones
entity A, then a push lands for entity B that would also update entity A
via cascade, the snapshot reflects A as of the clone point but misses the
B-driven cascade effect on A. This is the "skew between related entities"
problem. Check: does our cascade update propagate into the delta snapshot
correctly, or do we get skewed snapshots?

**2d.vii. Dirty-gen race during snapshot cycle rollover.** Our recent
`dirty_gen` + `DashSet<String>` change has a narrow race: between
`clear_dirty()` (which empties the set and bumps `snapshot_gen`) and a
concurrent writer's `mark_dirty(key)` under the old gen, the writer could
observe the old gen, skip the insert (thinking "already dirty this cycle"),
but the key has actually been cleared. Narrow window, likely tolerable,
but worth a property-test-with-shrinker to confirm.

**Total audit effort**: ~7 days to verify/fix these (some are "just audit,
no change," others need a fix + test).

### 2e. Deliverables for Block 2

- [ ] 2a batch-path bug fix + test
- [ ] 2b ring-buffer-drops metric + docs on how to monitor
- [ ] 2c per-stream `watermark_lateness` config
- [ ] 2d audit items: each either (i) verified correct and documented, or
      (ii) bug filed + fix shipped
- [ ] Single integration test exercising backfill → crash → recover → verify
      feature values match live-ingest baseline
- [ ] Documentation: one-page "Event-time and watermark semantics" in docs

**Total Block 2 effort**: ~2 weeks for one engineer. Could parallelize with
Block 1 (different code areas).

---

## Block 3 — Cleanup codebase, looks legit

**Why it's a launch blocker**: first impressions from a github.com visit
decide whether someone tries the product. A repo that looks abandoned /
rough / confused gets dismissed in 10 seconds. A repo that looks polished
earns the "let me try the quickstart" click. This is presentation work —
low complexity, high marginal impact.

### 3a. Repo surface (the first 30 seconds)

- [ ] **`README.md` rewrite** — lead with a 5-line code example (now that
      HTTP ingest exists, start with `curl -X POST http://localhost:6900/push/...`
      instead of Python). Follow with the fork demo. End with links to docs
      and architecture. Target: <60 lines total.
- [ ] **`CONTRIBUTING.md`** — exists but likely needs a pass. Build commands
      (`cargo build --release --bin beava`), test commands
      (`cargo test --lib`, `cargo test --tests`), code-style guidance, PR
      flow. Confirm it's accurate.
- [ ] **`CHANGELOG.md`** — bring current. We've been updating through this
      week's perf work; verify.
- [ ] **`LICENSE`** — Apache 2.0 present. Spot-check the file is intact.
- [ ] **`CODE_OF_CONDUCT.md`** — present. Verify wording.
- [ ] **`SECURITY.md`** — present. Confirm it has a disclosure email.
- [ ] **`GOVERNANCE.md`** / **`MAINTAINERS.md`** — present. Verify the
      "bus factor 1, disclosed up front" language is current and honest.
- [ ] **Repo description + topics** on GitHub itself — short one-liner, 5-8
      topics (`feature-server`, `real-time`, `rust`, `streaming`, `ml`,
      `apache-2-0`).
- [ ] **Social preview image** — 1280×640 PNG with logo + tagline.

### 3b. Directory structure clarity

- [ ] Walk the top-level directory. Anything that doesn't have an obvious
      purpose to a new visitor gets either (a) a README at that level
      explaining it, or (b) moved / renamed. Candidates that need clarity:
      `src/server/`, `src/engine/`, `src/state/` — each deserves a tiny
      `README.md` explaining its role.
- [ ] `benchmark/` — add a top-level `benchmark/README.md` explaining how
      to run each, what the numbers mean, where baselines live.
- [ ] `deploy/` — confirm `beava.service` is current. Add a
      `deploy/README.md` with one-page deploy-on-Linux instructions.
- [ ] Remove anything that's clearly demo / exploration cruft from the
      top level. Move to `.planning/` (already gitignored) if keeping.

### 3c. Code hygiene

- [ ] **Grep for `TODO` / `FIXME` / `XXX`** in `src/`. For each, either
      (a) fix it, (b) convert to a GitHub issue and remove the comment, or
      (c) confirm the comment is load-bearing and leave with a link to the
      tracking issue.
- [ ] **Dead code audit**: `cargo +nightly rustc -- -W dead_code` or equivalent.
      Remove unused modules, stale constants, commented-out blocks.
- [ ] **Rustdoc on all public APIs**: every `pub fn` / `pub struct` in
      `src/lib.rs` exports should have at least a one-line doc comment.
      Enable `#![warn(missing_docs)]` on the crate.
- [ ] **Clippy pass**: `cargo clippy --all-targets -- -D warnings`. Zero
      warnings. Address or explicitly `#[allow(...)]` with a comment.
- [ ] **Rustfmt pass**: `cargo fmt --check` is green. Commit any remaining
      formatting.
- [ ] **No stray `println!` / `dbg!` / `eprintln!`** in `src/` except where
      intentional (startup logging, profile instrumentation).

### 3d. Docs site or polished README-docs

Decide one of:

**Option A — Polished README only**: rely on `README.md` + docs in markdown
files in `docs/`. No separate site. Lower effort. Works for smaller
projects.

**Option B — Dedicated docs site** (mkdocs, docusaurus, vitepress): looks
more professional, easier to navigate long docs, has search. Higher effort
(~3-5 days to set up + migrate).

**Recommendation**: **Option A for launch**. Upgrade to Option B post-launch
if / when docs get big enough to warrant it. A clean README + a `docs/`
directory with 5-8 well-organized markdown files is plenty for v1.

Docs to write / verify exist:
- [ ] `docs/getting-started.md` — Docker / cargo install, first pipeline
- [ ] `docs/concepts.md` — streams, tables, operators, fork
- [ ] `docs/http-api.md` — HTTP endpoint reference (from Block 1)
- [ ] `docs/python-sdk.md` — Python API reference
- [ ] `docs/event-time.md` — event-time + watermark semantics (from Block 2d)
- [ ] `docs/operations.md` — sizing, durability, recovery, tuning
- [ ] `docs/architecture.md` — single-node design, scaling posture
- [ ] `docs/faq.md` — "will it scale," "what about Flink," etc.

### 3e. Docker + one-command install

- [ ] **Official Docker image** published to Docker Hub (`beavadb/beava:latest`
      + `beavadb/beava:0.1.0`). `Dockerfile` at repo root.
- [ ] **`docker compose up` example** in `examples/docker-compose.yml`.
- [ ] **Test the homepage "try it in 30 seconds" flow** from a clean machine
      (fresh VM or a colleague's laptop): `docker run -p 6900:6900 beavadb/beava`
      → `curl -X POST ...` → see response → `curl /features/...` → see value.
      Record time-to-first-success. Target: <60 seconds.

### 3f. Example projects

- [ ] **`examples/fraud-scoring/`** — a working fraud-pipeline project.
      Define streams + tables, push synthetic traffic, query features.
      Include a `README.md` with what to run and why.
- [ ] **`examples/session-features/`** — simpler example. Keyed streams,
      last-N-click features, count + sum aggregations.
- [ ] **`examples/curl-ingest/`** — shell scripts demonstrating the HTTP
      API end-to-end. Run via `bash examples/curl-ingest/run.sh`.
- [ ] Each example runs against a freshly-started Beava Docker container.
      Reproducible on any laptop.

### 3g. CI polish

- [ ] **GitHub Actions**: `ci.yml` with `cargo test --lib --release`,
      `cargo clippy -D warnings`, `cargo fmt --check` on push + PR. Fast
      (<5 min).
- [ ] **Release workflow**: `release.yml` that builds release binaries for
      Linux/macOS on tag push, uploads to GitHub Releases. Nice-to-have —
      doesn't block launch if not done.
- [ ] **CI badge** on README (`[![CI](...)](...)`). Visible signal that
      tests pass.
- [ ] **CodeCov badge** if we set up coverage. Lower priority.

**Total Block 3 effort**: ~1-2 weeks for one engineer, parallel with
Blocks 1 and 2.

---

## Ship gate

Launch when ALL of the following are true:

- [ ] Block 1 ship criteria met (HTTP ingest complete, docs, demos)
- [ ] Block 2 ship criteria met (batch-path bug fixed, ring-drops metric,
      per-stream lateness, audit items resolved)
- [ ] Block 3 ship criteria met (README rewrite, clippy green, Docker
      image, at least 2 example projects, docs present)
- [ ] **End-to-end smoke test** on a fresh AWS/Fly.io box: install from
      public source, run one example, push events via HTTP, read features,
      kill and recover, confirm data survived. Record a ~3-minute video
      or GIF.
- [ ] **Outreach rewrite** (`.planning/outreach/LAUNCH-PACKAGE-V8.md`) —
      reviewed against `AUDIT-V11.md` for fabricated claims one more time
      before posting.
- [ ] **Benchmarks re-verified** on current tree: ingest, recovery,
      fork-replay all reproduce the committed baseline numbers.

## Total effort and ordering

| Block | Effort | Who | Parallel? |
|---|---:|---|---|
| Block 1 HTTP ingest | ~2 weeks | Eng A | yes |
| Block 2 correctness | ~2 weeks | Eng B | yes (with A) |
| Block 3 cleanup | ~1-2 weeks | Eng C (or part-time on A/B) | yes |
| **Integration + smoke test** | ~3 days | All | sequential |
| **Total ship window** | **3 weeks** | 1-3 engineers | |

With one engineer doing everything: ~5-6 weeks sequential. With two
engineers working in parallel: ~3 weeks. With three (HTTP + correctness +
cleanup all in parallel): 2-3 weeks gated by integration testing.

## What we're deferring to post-launch

Explicitly NOT in the launch scope. These get public roadmap treatment
so evaluators see the growth path:

- **Thread-per-core mode** (v1.2) — unlock scaling past ~350 K EPS/node.
  Plan it, don't build it yet.
- **Multi-node via Kafka** (v1.3 or later) — horizontal scaling. Plan, not
  build.
- **UDF / stateful scripting** (v1.2) — Rhai or WASM operator plugins.
  Plan, scope, estimate.
- **Stateless derive expressions** (partly built) — finish `FeatureDef::Derive`
  exposure through the Python SDK if not already.
- **Web UI** for `/debug/*` endpoints — nice, not critical.
- **CLI subcommands** (`beava push`, `beava get`, `beava tail`) — good DX
  polish, not launch-gating.
- **OpenAPI / Swagger UI** — nice but not critical.
- **Deploy buttons** (Fly.io / Railway / Render) — nice, not blocking.

Each of these should have a short note in the public roadmap so evaluators
know they're planned. That's credibility without shipping commitment.

## Open questions

1. **Demo vs launch** — do we want a quiet demo to a handful of friendly
   users first (1-2 weeks of live feedback), then full public launch? Or
   go straight to public once the three blocks land? Recommend: quiet demo
   first, gather real-user friction, fix what we hear, then public.

2. **Launch channel** — HN? r/rust? r/dataengineering? ML Twitter?
   Probably all of them with staggered timing. Tie-in to the
   `.planning/outreach/LAUNCH-PACKAGE-V8.md` rewrite.

3. **Success metric** — what does "launch went well" look like? GitHub
   stars are vanity. Better: (a) 10+ unique people who followed the quickstart
   to end; (b) 3+ evaluators running Beava in a real non-demo context; (c)
   first inbound issue / PR that isn't from us. Aim for clear signal within
   30 days.

## Status

- Block 1: design doc written (`.planning/phases/HTTP-INGEST-DESIGN.md`).
  Not started.
- Block 2: audit started this session. 2a bug identified with fix proposal.
  2b, 2c, 2d.i-2d.vii flagged. Not started.
- Block 3: not started. Most items are in good shape (license, CODE_OF_CONDUCT,
  SECURITY all present); execution is "pass through and polish."

Pick up from here in the next working session.
