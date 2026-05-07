# Docs Deep Audit — 2026-05-04

> Scope: Beava OSS docs (`beava-website/project/docs/` + `docs/`). Priya-as-evaluator standard. Visual + sidebar-IA basics out of scope (covered by `2026-05-04-design-review.md` + `2026-05-04-devex-review.md`).
>
> **v0 SCOPE CORRECTION (2026-05-04, user direction):** Beava v0 is **single-node only**. Treat as a single-node database. Multi-instance scale-out (Redis-cluster pattern) is a future direction, NOT a v0 deployment story — do NOT write docs as if multi-node is ready. The audit has been re-prioritized below; the original "build a scaling-out page" P0 is removed and replaced with an audit-and-correct task to make the single-node v0 stance explicit.

## Executive summary

- **Reference is the new junk drawer.** 19 flat entries: Operators / Pipeline DSL / SDK / Wire+API mashed. Add 4 visual subgroups inside Reference (no extra sidebar levels).
- **Three pages are content-thin and dead-end.** `/docs/concepts/streams/` (~25 lines), `/docs/concepts/get-and-mget/`, `/docs/vision/non-goals/` are skeletons next to wire-spec / pipeline-DSL pages.
- **v0 is single-node — but the docs hint at multi-instance scale-out in 4 places** (single-thread-apply, memory-budget, non-goals, global-aggregation). Audit those references and either delete or rephrase to "future direction"; do NOT add a scaling-out page in v0.
- **No "embedded vs server" decision page.** `embed-mode.md` covers embed only; the choice between `bv.App()` / `bv.App("http://...")` / `beava serve` is implicit. (Note: third option "multi-instance" deferred to v0.x+.)
- **Wire-spec + http-api + error-codes + schema-evolution are gold-standard.** Third-party SDK author has everything needed. Lean on this strength.

---

## Phase 1 — Walk findings

### 1. `/docs/index.html` (Introduction launchpad)
- **Works:** 5 sections (Loop / Why / Commitment / Where to start / Get in touch); 3 audience-routed CTAs.
- **Breaks:** Markdown source vs HTML drift — `/get-started/define-a-pipeline/` and all `concepts/*` (streams/tables/windows/get-and-mget/freshness) ship as committed HTML with no `.md` source. Future contributors will diverge.
- **Wants next:** A `bv.demo("fraud")` one-liner inline (already documented in `quickstart.md:46-50`) — gives the lurker a zero-commitment third path.

### 2. `/docs/install/` (`docs/install.md`)
- **Works:** 4 install paths (pip / docker / brew / static), each one verb + Verify footer.
- **Breaks:** No "which one?" decision matrix. pip subprocess-spawns; docker is `:6400` long-running; brew + static are user-managed. Treated as interchangeable; they're not.
- **Wants next:** 3-row matrix `(install path → use case → state lifecycle)`.

### 3. `/docs/get-started/quickstart/` (`docs/quickstart.md`)
- **Works:** 14-line sample runs honestly. `bv.demo()` block covers "I have no data."
- **Breaks:** Never shows `app.batch_get(...)` — the API the homepage's PipelineShowcase implies, the actual fraud read path.
- **Wants next:** Second 5-line block with `app.batch_get([("UserClicks","u_42"), ...])`.

### 4. `/docs/get-started/define-a-pipeline/` (HTML only)
- **Works:** "Events come in, tables stay fresh, applications query by key" frame (`define-a-pipeline/index.html:58`). BidEvent + UserCampaignFeatures example.
- **Breaks:** The Pipeline DSL chain methods (`.filter()`, `.with_columns()`, `.group_by()`) live at `/docs/pipeline-dsl/overview/` and aren't linked here.
- **Wants next:** "Test your pipeline" subsection — `bv.test.fixture` is buried at `python.md:275-284`. The "feature owners own logic" thesis demands a visible testing surface.

### 5. `/docs/concepts/streams/` → `/tables/` → `/windows/`
- **Streams:** One snippet for the whole page. No derivation example, no `bv.Optional[T]`, no `keep_events_for=`. Full event-source surface at `python.md:131-162` not cross-linked.
- **Tables:** Only `count` appears as the agg op example. No mention of global tables (`@bv.table` no key) — separate page on `/docs/concepts/global-aggregation/`.
- **Windows:** Doesn't differentiate sliding/tumbling/lifetime; no `window="forever"`; no link to processing-time-only semantics.
- **All three:** Zero outbound docs links beyond the sidebar.

### 6. `/docs/concepts/get-and-mget/` → `/freshness/`
- **get/mget:** Skips `app.batch_get(...)` Python signature, the 10000-batch cap, the no-partial-success contract — all in `python.md:104-116` and `wire-spec.md:206-237`. Two short paragraphs that should be five with one example each.
- **Freshness:** Distinguishes read-latency from write-to-read; never names a number. Benchmarks page has "600k EPS / 100k EPS"; freshness page has zero quantitative anchors. Priya wants "median write-to-read on the fraud bench is X ms."

### 7. `/embed-mode/`, `/processing-time-only/`, `/global-aggregation/` (rescued)
- **Embed-mode:** Substantive — 5-step subprocess explainer, binary discovery, when-to-use/not-to-use, persist_dir tip. Best concept page in the section. Wants next: a back-reference to `bv.App("http://...")` for the "now I want a real server" pivot.
- **Processing-time-only:** Names the locked decision; rejected surface (no event-time, no joins, no watermarks); the historical-replay trade-off (line 19) deserves its own H3.
- **Global aggregation:** Best-documented of the three — decision matrix, three equivalent forms, performance math. Possibly over-fitted: 115 lines for one concept, more than streams+tables+windows combined.

### 8. `/docs/vision/why-beava/`, `/non-goals/`, `/benchmarks/`
- **why-beava:** 4-section narrative + beaver YouTube embed (on-brand). "Stack is powerful but heavy" lists 5 bullets without naming Kafka/Flink/Redis — Priya needs the names.
- **non-goals:** 5 honest bullets. Lacks the 6th/7th: *no event-time*, *no joins*, *no SSD spill* — the locked decisions documented elsewhere. The "600k EPS / 100k EPS" numbers appear here AND on benchmarks — single-source.
- **benchmarks:** "What we plan to report" 17-row reproducibility checklist is the right shape. No actual report linked. Either link `crates/beava-bench/` README or note "first public report at v0.1."

### 9. `/docs/operators/` + family pages (`velocity/`, `sketch/`, `core/`)
- **Index:** 7-family table with one-liner + "start in Core and Recency" tip. Op count discrepancy: index says 47, `python.md:246` says 53, `python.md:248-256` table sums to 54.
- **Velocity / Sketch / Core:** Real reference pages. Each op: runnable example + prose tradeoffs + signature. Tier classification only at top of family page, not per-op signature. Strongest reference asset on the site.

### 10. `/docs/pipeline-dsl/overview/`
- **Works:** End-to-end 25-line example, then `@bv.event` + retention/dedupe/cold_after kwargs, `@bv.table`, chain methods, register/push/query, Limits section. Comprehensive.
- **Breaks:** "Three forms compile to the same wire payload" claim (line 99) without showing the JSON inline.
- **Wants next:** Forward-promise to compilation-rules ("if you want to know what `with_columns` actually emits...").

### 11. `/docs/sdk-api/python/`
- **Works:** Best reference page on the site. Walks install → App lifecycle → register → push → get → batch_get → reset/ping → decorators → expressions → operator catalog → exceptions → fixtures → versioning. Cold-start `{}`, force=, embed-vs-explicit, idempotent dedupe — all named.
- **Breaks:** No async surface — sync-only is fine but should be explicit.
- **Wants next:** None material. This page is a flagship.

### 12. `/docs/wire-spec/`, `/http-api/`, `/error-codes/`, `/schema-evolution/`
- **wire-spec:** Frame format diagram + opcode table + per-opcode body schemas + error envelope + 4-language validation harness pointers + stable-contract guarantees. Third-party SDK author has everything. **Strongest page on the site.**
- **http-api:** Mirrors wire-spec; admin sidecar (4 GET endpoints) documented with metric families enumerated; auth + CORS + 415 explicit. One curl per route.
- **error-codes:** Comprehensive alphabetical list with code/path/when/HTTP status. Production-grade.
- **schema-evolution:** Additive matrix + destructive matrix + dry_run + diff envelope + 6-step validation order. The answer to "rollout without downtime" — but the title doesn't say so. Rename to "Schema evolution & rollouts."

### 13. `/docs/architecture/single-thread-apply/`, `/wal-snapshot/`, `/memory-budget/`, `/observability/`
- **single-thread-apply:** Honest "we walked past the thread pool" framing. Redis-cluster horizontal-scale story named in passing (lines 13, 43-45). **This is where multi-process scale-out lives today** — but as a passing reference, not a runbook.
- **wal-snapshot:** Diagram → boot recovery 3-step → sync modes (periodic vs per-event) → 4 watermarks. Sets up "what happens after a crash" with confidence.
- **memory-budget:** Per-entity math + 7KB ceiling + 3 guardrails + 3 failure modes. Best-in-class memory page.
- **observability:** Why-separate-port framing → 4-endpoint table → metric families per concern. Solid.

---

## Phase 2 — Coverage gaps

| Topic | State | Blueprint | IA placement | Priority |
|---|---|---|---|---|
| **Single-node v0 stance is implicit** | NEEDS AUDIT — passing scale-out hints in 4 pages contradict v0 single-node scope | Audit `single-thread-apply` (lines 13, 43-45), `memory-budget`, `non-goals`, `global-aggregation`. Either delete the multi-instance / Redis-cluster references, OR rephrase as "future direction beyond v0". Add an explicit "v0 is single-node" line to `non-goals.md` and `single-thread-apply.md`. **DO NOT add a scaling-out page in v0** | Multiple pages | **P0** |
| **Embedded vs server mode** | INCOMPLETE — embed-mode covers embed only | Section in `embed-mode.md` ("Decision tree") OR new `concepts/deployment-modes/`. 2 rows for v0: laptop / dev → embed (`bv.App()`); prod → `beava serve` + `bv.App("http://...")`. Multi-instance/HA explicitly out-of-scope for v0 | Concepts | **P0** |
| **Backpressure / overload** | NO PAGE | New `architecture/backpressure/`: frame_too_large (4 MiB), max_batch_size=10000, WAL fsync stalls per sync mode, RAM exhaustion → cold-TTL/lifetime-ceiling. Tie to error codes | Architecture | **P1** |
| **Schema evolution end-to-end** | INCOMPLETE — excellent reference, no narrative walkthrough | "Worked example: adding `device_id` to a live pipeline" at top of `schema-evolution.md`: 5-step rollout (dry_run → diff → SDK deploy → register → confirm via /registry) | Reference | **P1** |
| **Operator catalog completeness** | COMPLETE on family pages | Per-op cost-class tier chip in signatures; reconcile op count (47/53/54) | Reference | **P2** |
| **Wire format / 3rd-party SDK feasibility** | COMPLETE | None — gold standard. Maybe a "Build your own SDK" stub stitching the 4 refs | Reference | **P2** |
| **HTTP API endpoint inventory** | COMPLETE — 6 POST + 4 GET routes | Top-of-page route inventory card | Reference | **P2** |
| **Migration / pipeline rollout** | BURIED in schema-evolution | Cross-link from single-thread-apply + wal-snapshot; add `/docs/operations/rollouts/` if Operations section is added | Operations / Reference | **P1** |
| **Local testing pattern** | BURIED at `python.md:275-284` only | New `/docs/concepts/testing/` OR "Test your pipeline" subsection on define-a-pipeline. Required by the "feature owners own logic" thesis | Concepts | **P0** |
| **Comparison vs Materialize / Redis Streams / Tinybird / Flink / DIY-Kafka-Lua** | NO PAGE — why-beava names "the existing stack" without naming systems | New `vision/comparisons/`: 5-column matrix × 8 rows (model, durability, latency, scale, ops, learning curve, cost shape, license). Honest about no-joins / no-event-time / single-instance ceiling | Vision | **P0** |
| **Cloud roadmap stance** | INCOMPLETE — landing promises "same binary you self-host is what we run in our cloud" (`docs/index.html:92`); no docs page elaborates | Add "On managed beava" section to `vision/open-source/` (or new `vision/cloud/`): what's in/out, what stays Apache 2.0, timeline | Vision | **P1** |

Sub-gaps:
- Op count discrepancy (47/53/54) across `operators/index.md:3`, `python.md:246`, `python.md:248-256`. **P2.**
- "600k EPS / 100k EPS" numbers on both `non-goals` and `benchmarks` — anchor to benchmarks. **P2.**
- Markdown vs HTML authoring drift — concept + get-started pages ship HTML-only with no `.md` source. **P1 docs-infra decision.**
- `define-a-pipeline:112` claims TS/Go SDKs are "communicate-only" — strong design statement worth elevating to `vision/why-beava/`. **P2.**

---

## Phase 3 — What other OSS docs teach us

### bun.sh — "the docs are 4 tools, not 4 sections"
- Top-level IA is product-shaped: 4 cards (Runtime / Package Manager / Test Runner / Bundler), then 2 cards (Install / Quickstart). That's the whole IA above the fold.
- Bun's own copy: each section "structured with overview, quick examples, reference, and best practices for fast scanning and deep dives." Beava's operator family pages should mirror this 4-shape.
- **`llms.txt` flat machine-readable doc index** at `bun.com/docs/llms.txt` — cheap, signals "we know AI agents read these now." Beava should ship one; Priya's eval will partly happen via Cursor / Claude.
- **Steal:** "What's inside" 4-card grid above the launchpad — Push events / Define pipelines / Query features / Operate.

### docs.pola.rs — "user-guide vs API-reference is the right split"
- Top-level IA is User Guide / API Reference / Development. User Guide is *sequential*: Getting started → Concepts → Expressions → Transformations → Time series → I/O → Migration. It's a *book*, not a list.
- Concepts and how-to interleave inside topic guides — no separate "How-to" section.
- **Migration guides are first-class** ("Migrate from Pandas / from Spark"). For Beava: comparison-as-migration ("Migrating from Materialize / Redis Streams / DIY Kafka+Lua") is more useful than a feature matrix.
- **Steal:** "User Guide as a book" pattern. Beava's Getting started + Concepts already lean this way; Vision should fold in as Chapter 1.

### redis.io — "deployment paths are top-level"
- Top-level IA is verb-shaped: Develop / Libraries+tools / Redis products / Commands / APIs.
- Deployment paths are surfaced top-level: Open Source / Software / Kubernetes / Cloud. The "same APIs across deployments" promise is repeated. Beava's analog (embed / `beava serve` / multi-instance Redis-cluster) is currently scattered across 3 pages and 4 passing references.
- AI use-cases get top-billing as a section. Beava's analog is fraud / personalization / live dashboards — currently absent from `/docs/` (lives only in the Guide, which ships Chapter 1).
- **Steal:** "Operate" subtree as a peer of Develop. Beava has no Operations section — capacity, sizing, sync-mode choice, observability dashboards, rollouts, backpressure all scatter inside Architecture.

---

## Phase 4 — Prioritized

### P0 (this week)
1. **Make single-node v0 explicit; audit and correct multi-instance hints.** Walk `single-thread-apply` (lines 13, 43-45 currently namedrop Redis-cluster horizontal scale), `memory-budget`, `non-goals`, `global-aggregation`. Either delete the references or rephrase as "future direction beyond v0." Add an explicit "v0 is single-node — outgrow it? talk to us" line to `non-goals.md`. **DO NOT** create a scaling-out page. ~½ day.
2. **Build `/docs/concepts/deployment-modes/`** (or major addition to embed-mode.md) — 2-row v0 decision tree: laptop/dev → embed (`bv.App()`); prod → `beava serve` + `bv.App("http://...")`. Multi-instance explicitly deferred. ½ day.
3. **Build `/docs/vision/comparisons/`** — 5-column × 8-row matrix vs Materialize / Redis Streams / Tinybird / Flink. Honest about losses including the single-instance ceiling. ~1 day.
4. **Promote testing into Get-started** — New `/docs/concepts/testing/` OR "Test your pipeline" subsection on `/docs/get-started/define-a-pipeline/`. Currently `bv.test.fixture` is buried at `python.md:275-284`. ½ day.

### P1 (next 2 weeks)
5. **Backfill thin pages**: `streams/`, `tables/`, `windows/`, `get-and-mget/`, `non-goals/`. Each gets one operator example, one SDK cross-link, one wire-spec cross-link. Target ~70 lines. ~2 days total.
6. **Add `/docs/architecture/backpressure/`** — Frame caps, batch caps, fsync stalls, RAM exhaustion. Tie to error codes. ~1 day.
7. **Add "Worked example: live pipeline migration" to `schema-evolution.md`** — 5-step rollout. Title shift to "Schema evolution & rollouts." ½ day.
8. **Add cloud roadmap stance** — 3 paragraphs on `vision/open-source/`. What cloud will/won't include. What stays Apache 2.0. The landing promises this; docs must back it. ½ day.
9. **Decide markdown vs HTML authoring source** — Backport HTML→MD with a render step, OR document "edit `.html` directly." ½ day.

### P2 (backlog)
10. Per-op cost-class Tier chip on every operator signature.
11. Reconcile op count (47 / 53 / 54).
12. Single-source the "600k EPS / 100k EPS" numbers to `benchmarks`.
13. Ship `llms.txt` flat docs index.
14. "What's inside" 4-card grid on `/docs/` above the launchpad.
15. Promote Operations as a 7th sidebar peer to Architecture (capacity, sizing, sync modes, observability dashboards, rollouts, backpressure).
16. Migration-not-comparison framing — once `/docs/vision/comparisons/` lands, follow with "Migrating from X" pages (Polars-style).

## Out-of-scope notes
- Visual / typography / spacing — see `2026-05-04-design-review.md`.
- Sidebar IA basics — see `2026-05-04-devex-review.md` (resolved in current 6-section sidebar).
- Cloud product copy — out of scope unless part of the Apache-2.0 commitment doc.
- Guide / Chapter 1 audit — covered by design + devex reviews; this audit is `/docs/` only.
- RFC content quality — this audit checks only that RFCs are sidebar-reachable.
