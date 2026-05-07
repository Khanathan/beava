# Session handoff — 2026-05-04 late

**Status:** Homepage redesign shipped. Docs and vision content from this session is at risk of being overwritten by parallel work — read § "Critical issues" first before continuing.

---

## TL;DR

This session delivered three things:

1. **Homepage redesign** — full 5-section rebuild of `project/index.html` per a Claude Design handoff. 14 locked copy + structure decisions. Live now.
2. **Docs + RFCs build-out** — 17 docs pages (intro, get-started, concepts, vision, community), 6 RFC stub pages, sidebar restructure, Pagefind indexed.
3. **Tone + voice memory updates** — locked dev-empathy register for hero copy, retired the FeedBeaver brand moment, locked "lowercase beava" everywhere.

But: while we were working, **another agent (or another tool, or the design handoff itself) generated ~80+ additional docs pages** under `/docs/operators/`, `/docs/sdk-api/`, `/docs/architecture/`, `/docs/wire-spec/`, `/docs/http-api/`, etc. AND **overwrote `project/docs/index.html`** (title is now "Beava Docs — beava docs" instead of our "Introduction · beava docs"). Our vision/ pages and community/rfcs/ pages **survived** intact.

---

## Critical issues to address next session

### 1. Header / shell mismatch (user-flagged: "headers are wrong")

The site has two parallel header systems:

- **Homepage** uses `Nav` from `project/js/Shared.jsx` (defined inline, expects `active="home"`)
- **Docs pages** use `SiteHeader` from `project/js/_shared/SiteHeader.jsx` (different file, different styling)

After the homepage redesign, the visual gap is now obvious. The new homepage Nav doesn't visually match the docs SiteHeader. Pick one and propagate. Recommendation: make `Nav` (used by homepage) and `SiteHeader` (used by docs) use the same canonical implementation — probably promote one to `project/js/_shared/Nav.jsx` and rewire both surfaces.

### 2. Docs pages "ugly as hell" (user-flagged)

The docs pages still use `project/styles/docs-kit.css` and the older inline-styled DocsShared/DocsSidebar/DocsTOC components. They never got the visual refresh that the homepage just got (warm cards, soft shadows, refined typography). They look stale next to the new homepage.

What's needed: a docs-page visual refresh in the same aesthetic as the new homepage. Concretely:
- Card / surface style (white bg, soft border, subtle shadow `var(--shadow-sm)`)
- Typography rhythm (larger H1 with tight `letter-spacing: -0.02em`, italic-serif lede)
- Sidebar treatment matching the new homepage Nav voice
- TOC styling refresh

### 3. Verify nothing of ours got overwritten

`/docs/index.html` definitely got overwritten (title changed from "Introduction · beava docs" → "Beava Docs — beava docs"). Our 5-section short intro page is gone. Need to either:
- Restore the version we wrote (preserved inline below in § "Locked content snapshots")
- OR accept the new content and reconcile

Worth diff-checking these specific files we wrote this session against current state:
- `project/docs/index.html` — KNOWN OVERWRITTEN
- `project/docs/get-started/quickstart/index.html` — verify
- `project/docs/get-started/define-a-pipeline/index.html` — verify
- `project/docs/get-started/push-events/index.html` — verify (had the durability-fsync section)
- `project/docs/get-started/query-features/index.html` — verify
- `project/docs/concepts/streams/index.html` — verify
- `project/docs/concepts/tables/index.html` — verify
- `project/docs/concepts/windows/index.html` — verify
- `project/docs/concepts/get-and-mget/index.html` — verify
- `project/docs/concepts/freshness/index.html` — verify
- `project/docs/vision/why-beava/index.html` — confirmed surviving
- `project/docs/vision/open-source/index.html` — confirmed surviving
- `project/docs/vision/non-goals/index.html` — confirmed surviving
- `project/docs/vision/benchmarks/index.html` — confirmed surviving
- `project/docs/community/rfcs/index.html` + 6 RFC stubs — confirmed surviving

### 4. Mass-generated docs pages need triage

The parallel-generated content includes:

```
docs/operators/{decay,cost-class,core,point-ordinal,buffer-geo,velocity,recency,sketch}/
docs/sdk-api/{go,python,typescript,shared}/
docs/architecture/{mio-data-plane,memory-budget,observability,single-thread-apply,wal-snapshot}/
docs/concepts/{events-vs-tables,global-aggregation,lifetime-aggregation}/
docs/wire-spec/
docs/http-api/
docs/quickstart/                ← collides with our /docs/get-started/quickstart/
docs/error-codes/
docs/schema-evolution/
docs/pipeline-dsl/
```

Three things to do:
1. **Read** these pages to see what voice/style they use (probably auto-generated and rough)
2. **Reconcile** with our locked sidebar IA — either fold these into our sections or expand the sidebar
3. **Choose** which pages survive — some look genuinely useful (operators catalog, sdk-api reference); others (e.g. `/docs/quickstart/`) duplicate what we already have

---

## What got built this session — file inventory

### Homepage redesign (complete)

- `project/index.html` — full rewrite. 5 sections: Hero · Pillars · PipelineShowcase · Recipes · FinalCTA
- `project/js/FeedBeaver.jsx` — DELETED
- Source design handoff: was at `/tmp/beava-design-handoff/` (will be cleared on reboot — re-fetch from `https://api.anthropic.com/v1/design/h/ebjZn4mhb0XN-c5pelZHaA` if needed)

### Docs site (built, partially at risk)

17 docs pages we authored — see § "Critical issues" #3 for the verify list. Plus:
- `project/js/docs/DocsSidebar.jsx` — 4-section sidebar (Getting started · Vision · Concepts · Community + RFCs as own group)
- `project/js/docs/DocsShared.jsx` — added `<Code lang="...">` component with hljs lazy-load
- `project/styles/docs-kit.css` — added hljs token-class palette mapping
- `build-search.mjs` — Pagefind index generator with 23 records

### RFCs (6 stubs)

All under `project/docs/community/rfcs/`:
- `rfc-001-tiered-storage/`
- `rfc-002-table-ingestion/` (was -table-upsert-delete, renamed)
- `rfc-003-stream-to-table-join/` (was -stream-to-table-lookup, renamed)
- `rfc-004-event-log-retention/`
- `rfc-005-event-log-query-replay/`
- `rfc-006-online-pipeline-migration/`

### Planning docs

- `beava-website/.planning/HERO-REDESIGN-2026-05-04.md` — 14 locked decisions, full context
- `beava-website/.planning/HERO-COPY-DECK-2026-05-04.md` — per-element copy + layout spec
- `beava-website/.planning/HANDOFF-2026-05-04-late.md` — this file

### Memory updates

- `feedback_beava_doc_tone.md` — long-form doc tone (lowercase beava, bold soundbites, TiDB-style trust-building, 30-60s first pages)
- `feedback_beaver_overflow_keep.md` — RETIRED, FeedBeaver removed from homepage
- `project_redis_shaped_no_event_time_ever.md` — softened from "permanent commitment" to "v0 default" per user correction; stream-table joins now in scope for RFC

---

## Locked content snapshots (preserve these even if files are overwritten)

### The 14 locked homepage decisions

| # | Decision | Choice |
|---|---|---|
| 1 | Headline copy | "Real-time features without heavy infrastructure." |
| 2 | "Dam good at streams" placement | Gaegu-font eyebrow above H1 |
| 3 | LiveMetrics visual style | Clean dashboard cards (label + big number + sparkline) |
| 4 | Three metrics | Avg time on /docs/ (1h) · Pages viewed today · Top page this hour |
| 5 | FeedBeaver | Delete entirely |
| 6 | Cloud waitlist banner | Drop entirely |
| 7 | Backend source | Real beava on beava.dev (no inflation; metrics dignified at any scale) |
| 8 | PipelineShowcase swap | Show SiteMetrics pipeline that produces the hero numbers |
| 9 | Empathy line | "If your last 'simple' feature ate a sprint to deploy, you know why we built this." |
| 10 | Section count | 9 → 5 sections |
| 11 | Section reorder | Pillars moves to slot 2 (before PipelineShowcase) |
| 12 | Install-tabs eyebrow | "Run it locally." |
| 13 | 3-step plan | Middot strip beneath install tabs: "Push events · Maintain tables · Query by key." |
| 14 | Philosophical line | "Stream processing shouldn't require a platform team." (Pillars section eyebrow) |

### Hero copy (left column, top to bottom)

```
Pill:     v0.9.4 · Apache 2.0 · single binary
Eyebrow:  Dam good at streams.                        (Gaegu, -2° rotation)
H1:       Real-time features without heavy infrastructure.
Empathy:  If your last 'simple' feature ate a sprint to deploy,
          you know why we built this.
Lede:     Personalization, fraud rules, live dashboards —
          in hours, not quarters.

Kicker:   Run it locally.
Tabs:     [brew] [curl] [docker]
          $ brew install beava
Caption:  ~14 MB · macOS, Linux, Windows · runs on 1 GB RAM · scales to one big box
3-step:   Push events · Maintain tables · Query by key.

CTA:      Read the guide →
```

### Hero right column — LiveMetrics

Three cards stacked, each: small uppercase label / big number / tiny sparkline.

```
CARD 1
Label:    AVG TIME ON /docs/ · LAST HOUR
Number:   2m 14s (sample)
Sparkline: 60 points, last hour

CARD 2
Label:    PAGES VIEWED · TODAY
Number:   1,247 (sample)
Sparkline: 24 points, last 24h

CARD 3
Label:    TOP PAGE · LAST HOUR
Number:   /docs/get-started/quickstart/ (real path string)
Subline:  382 views (smaller, fg3)
Sparkline: omit

Caption:  Three real beava queries. The pipeline is below ↓
```

### SiteMetrics pipeline (the 13-line one shown in PipelineShowcase)

```python
import beava as bv

@bv.stream
class PageView:
    session_id: str
    path: str
    dwell_ms: int   # set when the visitor leaves the page

@bv.table(key="__global__")
def SiteMetrics(e: PageView):
    return e.agg(
        avg_dwell_docs_1h = bv.avg(e.dwell_ms, window="1h",
                                   where="_event.path.startswith('/docs/')"),
        page_views_today  = bv.count(window="24h"),
        top_page_1h       = bv.top_k(e.path, k=1, window="1h"),
    )

bv.App("0.0.0.0:6400").register(PageView, SiteMetrics).serve()
```

### 4 Pillar cards

```
01  REPLACES REDIS + LUA
    No more 200-line Lua scripts maintaining counters. beava is the table.
    foot: redis-cli → bv.get(...)

02  NO STREAMING STACK
    No Kafka, no Flink, no Schema Registry. One Go binary, one HTTP port.
    foot: ~14 MB · single binary

03  CRASH-SAFE BY DEFAULT
    WAL on every write. Restart mid-flight; tables come back exactly where they were.
    foot: fsync configurable, default sane

04  APACHE 2.0, FOREVER
    No source-available rug-pull. No usage limits. The same binary in the cloud you self-host.
    foot: github.com/beava-dev/beava
```

### 5 Recipe cards

```
personalization · 22 lines  "Seen but didn't click" recency score
                            Demote items a user has impressed and skipped, without batching features overnight.

fraud · 18 lines           Card-testing detection
                            Two counters and a threshold — flag the attacker before the 11th decline.

leaderboard · 14 lines     Always-fresh top-N
                            A leaderboard that updates incrementally and never stalls behind a batch job.

rate limit · 9 lines       Per-key sliding window
                            The cheapest, most correct rate limit you can ship — one counter, one window.

usage metering · 16 lines  Per-customer monthly counters
                            Bill on what they actually used, with counters that survive restarts and reset on schedule.
```

### Sidebar IA (4 sections + RFCs as own group)

```
Getting started:
  Introduction              /docs/
  Quickstart                /docs/get-started/quickstart/
  Define a pipeline         /docs/get-started/define-a-pipeline/
  Push events               /docs/get-started/push-events/
  Query features            /docs/get-started/query-features/

Vision:
  Why beava                 /docs/vision/why-beava/
  Open source commitment    /docs/vision/open-source/
  Non-goals and tradeoffs   /docs/vision/non-goals/
  Benchmarks                /docs/vision/benchmarks/

Concepts:
  Streams                   /docs/concepts/streams/
  Tables                    /docs/concepts/tables/
  Windows                   /docs/concepts/windows/
  get and mget              /docs/concepts/get-and-mget/
  Freshness                 /docs/concepts/freshness/

RFCs (own group):
  About RFCs                /docs/community/rfcs/
  RFC-001 Tiered storage
  RFC-002 Table ingestion
  RFC-003 Stream-to-table join
  RFC-004 Event log retention
  RFC-005 Event-log query and replay
  RFC-006 Online pipeline migration

Community:
  Weekly dev calls          /docs/community/dev-calls/
  Contributing              /docs/community/contributing/
  Discussions (external)    https://github.com/beava-dev/beava/discussions
  GitHub (external)         https://github.com/beava-dev/beava
```

The mass-generated /docs/operators, /docs/sdk-api, /docs/architecture, etc. need to be folded into this IA OR explicit decisions made about which ones survive.

### StoryBrand voice — established this session

Dev-empathy register: terse, knowing, "we've been there", no marketing fluff. Lowercase "beava" everywhere. Avoid "lock", "commitment", "permanent" framing in RFCs (use "v0 default" or "today's design" instead). Bold soundbites in long-form prose. Bold-lead bullets.

---

## Suggested next-session priority order

1. **Audit overwrites** (15 min). Run `git status` + `git diff` on `project/docs/` to see exactly what got changed by the parallel work. Restore anything of ours that's gone using the snapshots in this doc.

2. **Reconcile docs IA** (30 min). The mass-generated pages cover real concepts (operators catalog, SDK refs, architecture). Decide:
   - Which to keep as-is
   - Which to rewrite in our locked tone
   - Which to merge into existing sections vs. add as new sidebar groups
   - Update `DocsSidebar.jsx` and `build-search.mjs` accordingly

3. **Unify headers** (30 min). Pick a single canonical Nav/SiteHeader implementation, propagate to both homepage and docs. Easiest path: take the new homepage's `Nav` from `Shared.jsx`, factor into `_shared/`, replace docs `SiteHeader`.

4. **Docs page visual refresh** (1-2h). Apply the homepage aesthetic to docs pages: card surfaces, refined typography, consistent shadows, tighter sidebar. Same warm palette stays. New `docs-kit.css` tokens may be needed.

5. **LiveMetrics backend** (separate session). Wire the homepage's three metric cards to a real beava instance. Currently uses synthesized seed numbers (intentional per locked decision #7). The pipeline source is in PipelineShowcase. Need: deploy beava + push `PageView` events from every page + poll `/features/SiteMetrics/__global__`.

6. **Pre-existing 404s** (10 min). Homepage features grid still has dead links to `/docs/operations/` and `/docs/compare/vs-redis-lua/`. Either redirect or delete the references.

---

## Key external references

- **Design handoff URL** (re-fetch if /tmp/ is gone): `https://api.anthropic.com/v1/design/h/ebjZn4mhb0XN-c5pelZHaA` — "Beava Design System-handoff.tar.gz", 11 MB compressed
- **Design source files** were at: `/tmp/beava-design-handoff/beava-design-system/project/ui_kits/marketing/` with separate JSX files (Hero, Pillars, PipelineShowcase, Recipes, FinalCTA, Shared, Nav, Footer). Implementation inlined them into `project/index.html`.
- **Design chats** were at: `/tmp/beava-design-handoff/beava-design-system/chats/chat[1-4].md`. Chat 4 has the full reasoning behind the marketing-kit redesign.

---

## Things deliberately NOT changed this session

- The `beava-website/project/styles/colors_and_type.css` design token file
- The `beava-website/project/community/index.html` standalone community page (only updated the RFC list)
- The `beava-website/project/guide/` pages (only updated one /docs/performance/ link)
- Any `beava-website/beava-design-system/` files (the older design system; the new handoff was a separate bundle)
- The CLAUDE.md project file
- Any code in `crates/` (Rust server)

---

## Definitions of done for next session

- `localhost:8002/` and `localhost:8002/docs/` use visually consistent header/nav.
- Docs pages no longer feel "ugly as hell" relative to the homepage.
- The 17 pages we authored this session are intact (or consciously merged with newer content, with a record of what was kept).
- The sidebar reflects the actual /docs/ contents, not just our 17 pages.
- `git status` after the cleanup shows a coherent set of changes ready to commit.
