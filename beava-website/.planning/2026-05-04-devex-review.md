# DevEx & IA Review — 2026-05-04

Audit performed against the static source under
`/Users/petrpan26/work/tally/beava-website/project/` (Chrome MCP was offline; review is source-tree based, which is fine because every page is a self-contained `index.html`). Sidebar IA is in `js/docs/DocsSidebar.jsx`.

## Executive summary

- **Docs sidebar is the #1 problem.** It has 10 sections / 47 entries and the `Architecture` section is a junk drawer mixing 5 concept pages (`embed-mode`, `processing-time-only`, `global-aggregation`, plus 2 phantom links `events-vs-tables` and `lifetime-aggregation` that 404) with 5 actual architecture pages. Two of the 47 sidebar links are dead. Recommend collapsing to **6 sections / 33 entries**.
- **The `/docs/` Introduction page is a stub.** ~30 lines of body content (lede, 13-line code block, 3 "Start here" bullets, 1 "Read on" paragraph). It is missing the four launchpad surfaces the user asked for: quick intro, quick vision, quick commitment, quick contact. A concrete blueprint is below.
- **Pager (prev/next) is missing on 31 of ~47 docs pages.** It exists in Getting started, main 5 Concepts, Vision, Community, Introduction. Operators, SDK, Wire & API, Pipeline DSL, Architecture, and the 3 misfiled concept pages all dead-end. Adding Pager is a one-week win.
- **Cross-link density is dangerously low on the foundational pages.** `/docs/concepts/streams/` has 0 outbound docs links. `/docs/vision/why-beava/` has 0 outbound docs links. These are the pages a Priya bounces from when she wants to "go deeper."
- **Landing flow is solid; guide is honest.** The "Read the docs" + "Join Discord" CTA pair plus the "Three ways in" final card grid + Calendly band gives evaluators a clean route. Guide ships only Chapter 1 + a small "more chapters coming soon" tag — no broken recipe links — which is the right call.

---

## Landing — minute-1 flow

### Strengths
- Hero ships an InstallTabs widget (`brew` / `curl` / `docker`) front-and-center: that **is** "minute 1" for an OSS evaluator. Competing with DuckDB-style installs, this lands.
- LiveMetrics panel + the "homepage runs beava" PipelineShowcase are rare proof artifacts. Most OSS landing pages prove nothing; this one shows the product working on this page.
- Trust signals all present and not buried: v0 + Apache 2.0 + single-binary chip top-left, GitHub link in pillar 04, Calendly band at the bottom, "no sales pitch" copy.
- The "Three ways in" grid (Chapter 1 / Star on GitHub / GitHub Discussions) does what most landing pages forget: gives the visitor a non-binary CTA so the lurkers and the doers both have a path.

### P0 issues
1. **Hero example uses `@bv.table` with no `key=`** (line 630 of `index.html`: `# no key= → one row, site-wide`). This is a contract violation — `project_v0_events_only_scope` killed `@bv.table` for everything except aggregation-output (after the 2026-05-03 partial overturn). The keyless / site-wide form on the homepage will confuse Priya when she hits the Quickstart and finds `@bv.event` instead. Recommendation: align the homepage code to the `@bv.event` + `events.agg(key=, window=)` form locked in the docs (or update docs to the homepage form, but pick one).
2. **The Discord CTA is the secondary on the hero, but Discord isn't even in the FinalCTA grid** (Chapter 1 / GitHub / Discussions) — and Discussions is the third card. So a visitor who clicked Discord at the top can't tell whether to lurk on Discord or Discussions. Pick one community lurker channel for the hero secondary. Discussions is already the better "anyone can read async" surface — promote that to the hero.

### P1 issues
3. **No `/docs/` route on the hero.** The CTA labelled "Read the docs" goes to `/docs/`, which is good, but the hero has no second-line that says "or read Chapter 1 first" — that route exists three sections down in FinalCTA. A first-time visitor is going to scroll past LiveMetrics + Pillars + PipelineShowcase before being told "Chapter 1 is the gentler starting point." Consider one extra small chip under the InstallTabs: "New to streams? Read Chapter 1 →".
4. **`PipelineShowcase` precedes `Pillars`** in `App` composition (lines 845-852: Hero → PipelineShowcase → Pillars → FinalCTA). The 13-line pipeline is great, but 13 lines of Python in the second viewport before any "what is beava" framing risks losing the evaluator who hasn't decided yet why they should read code. Order should be Hero → **Pillars** → PipelineShowcase → FinalCTA. (Pillars is "what's different"; that's what we want to convince Priya of *before* she reads code.)
5. **`Recipes` section is in the source comment** ("5 sections: Hero · Pillars · PipelineShowcase · Recipes · FinalCTA") but **not rendered** in `App`. Either delete the comment or render the section. Right now the comment is misleading anyone editing the file.

### P2 issues
6. The hero handwritten tagline "Dam good at streams." is fine but appears once in 24px italic and never recurs as a cross-page anchor. If it's a brand line, FinalCTA should echo it.

### Concrete recommendations
- Reorder `App` to `Hero → Pillars → PipelineShowcase → FinalCTA`.
- Replace the hero "Join Discord →" secondary with "Read Chapter 1 →" pointing at `/guide/chapter-1/`.
- Fix the `@bv.table` example to match locked docs surface.
- Delete the dead `Recipes`/`ComingSoonBanner` references in `index.html` (lines 144-160 of `/guide/index.html` define a `RecipeIndex` that isn't rendered either — see Guide section).

---

## Guide — quality & honesty

### Strengths
- **`/guide/` is honest.** Source has a `RecipeIndex` (5 cards with "soon" badges) and `ComingSoonBanner`, but **neither is rendered in `App`**. What ships is `Hero → ChapterCard → Footer` plus a small "more chapters coming soon ~" handwritten tag. That's the right call — no broken `/guide/recipes/fraud/` dead-ends, no fake roadmap inflation. Don't undo it.
- Chapter 1 itself has strong pacing: hero with breadcrumbs, `Skip the browser — run it for real` callout (covers prerequisites without a sterile "Prerequisites" header), "Same shape as pandas" framing, then interactive Tutorial. The "Eyebrow / Part 1 / Part 2 / etc." progressive disclosure works.
- Chapter 1 uses real beava code shapes — `@bv.event` and `events.groupby(...).agg(...)` — that match the docs surface, so the user is actually learning the API.

### P0 issues
1. **Chapter 1's exit dead-ends.** The closing CTA reads "Pick a recipe and build for real" with one button labelled "Back to the guidebook" — but the guidebook doesn't have any recipes yet. Better exit: "Try it on your own data" → `/docs/get-started/quickstart/`, plus "Browse all 53 operators" → `/docs/operators/`, plus a softer "Star on GitHub" tertiary. Right now Chapter 1 graduates the user back to the page that already told them there's nothing more.
2. **The `<RecipeIndex/>` source code is present but unused.** If it ever gets uncommented during a refactor, all 5 recipe cards will 404. Either delete the dead component or guard with a `RECIPES_LIVE` flag. Same for `ComingSoonBanner`.

### P1 issues
3. **No Pager on Chapter 1.** Inner docs pages have `<Pager next=...>`; the guide has nothing. Adding a "Next: read /docs/concepts/streams/" Pager-equivalent at the bottom of Chapter 1 would close the loop.

### P2 issues
4. The "more chapters coming soon" tag is in handwritten font at 26px which reads as charming but in an SEO-conscious YC-CTO eye also reads as "this product is unfinished." Consider a more concrete "Chapter 2: fraud rules — drafting" rather than open-ended "soon."

### Recipe-placeholder strategy proposal
**Don't render placeholders. Do redirect intent.** Where a recipe would have lived, link to the closest existing real surface:

| Recipe | Send the user to |
|---|---|
| Personalization | `/docs/operators/recency/` + `/docs/operators/sketch/` (top_k) |
| Fraud detection | `/docs/operators/velocity/` + `/docs/concepts/windows/` |
| Leaderboard | `/docs/operators/sketch/` (top_k) + `/docs/concepts/get-and-mget/` |
| Rate limiting | `/docs/operators/velocity/` (rate ops) |
| Usage metering | `/docs/operators/core/` (count, sum) |

This way Chapter 1's exit can read: "**Building fraud rules?** Start at /docs/operators/velocity/. **Ranking?** /docs/operators/sketch/." Without a single placeholder.

---

## Docs — IA restructure (the main event)

### Current state inventory

**10 sections, 47 entries** (from `js/docs/DocsSidebar.jsx`):

| # | Section | Open by default | Items |
|---|---|---|---|
| 1 | Getting started | yes | 5 (Introduction, Quickstart, Define a pipeline, Push events, Query features) |
| 2 | Vision | yes | 4 (Why beava, Open source commitment, Non-goals and tradeoffs, Benchmarks) |
| 3 | Concepts | yes | 5 (Streams, Tables, Windows, get and mget, Freshness) |
| 4 | RFCs | no | 7 (About RFCs + 6 numbered RFCs) |
| 5 | Community | no | 4 (Weekly dev calls, Contributing, Discussions ↗, GitHub ↗) |
| 6 | Operators | no | 9 (Catalog + 8 family pages) |
| 7 | SDK reference | no | 4 (Cross-language parity, Python, TypeScript, Go) |
| 8 | Wire & API | no | 4 (Wire spec, HTTP API, Schema evolution, Error codes) |
| 9 | Pipeline DSL | no | 3 (Overview, Expressions, Compilation rules) |
| 10 | Architecture | no | 10 (5 real architecture + 5 misfiled concepts incl. 2 phantom links) |

**Defects:**
- `/docs/concepts/events-vs-tables/` — sidebar entry, **file missing → 404**.
- `/docs/concepts/lifetime-aggregation/` — sidebar entry, **file missing → 404**.
- `/docs/install/` and `/docs/quickstart/` — files exist but **NOT in sidebar** (orphans, search-only). Both are pre-13.7 legacy paths.
- `/docs/architecture/memory-governance/` — file exists, **NOT in sidebar**.
- `Architecture` section mixes 5 actual architecture pages (single-thread-apply, mio-data-plane, wal-snapshot, memory-budget, observability) with 5 concept pages (events-vs-tables [missing], embed-mode, lifetime-aggregation [missing], processing-time-only, global-aggregation). Concept pages should live under Concepts.
- 4 sections (Vision, RFCs, Community, Wire & API) are reasonably small but RFCs with 7 entries open-by-collapse is invisible to first-time visitors. Community at the docs level duplicates the top-nav `/community/` link.
- `RFC-002 — Table ingestion` and `RFC-003 — Stream-to-table join` directly contradict locked v0 commitments. They're proposals, so technically fine, but a YC CTO opening these will see "v0 doesn't do this, but here's the RFC for when it might" — keep them but make sure each RFC page leads with `Status: future / not in v0`.

### Where the Introduction page falls short of the user's spec

The user asked for: **"quick intro, quick vision, quick commitment, quick way to contact, etc. And all the pages should be link and order reasonably."**

Current `/docs/index.html` (114 lines, ~30 lines of body content) has:
- ✅ One-line lede ("beava is a single-binary feature server...")
- ✅ One Python code block + curl block (the loop)
- ✅ "Start here" with 3 bullets (Quickstart, Why beava, Concepts)
- ✅ "Read on" with 1 paragraph (define-a-pipeline → operators → SDK → wire spec → RFCs)
- ✅ "Stuck on this one?" Discussions/Discord callout
- ❌ **No quick vision section** — the closest is the "Read on" paragraph, which is too far down and doesn't lift the soundbites from `/docs/vision/why-beava/`.
- ❌ **No quick commitment section** — no Apache 2.0 / single-binary / no-telemetry promise on the launchpad.
- ❌ **No quick contact** — only Discussions/Discord buttons buried in the help callout. No GitHub repo link, no Calendly, no dev calls.
- ❌ **No "path forward" segmentation** — the page assumes one persona ("read top to bottom"). A YC CTO evaluating wants Why beava; a builder wants Quickstart; a browser wants Operators. Three different first clicks.

### Proposed Introduction page blueprint

Replace the current single-block Introduction with a launchpad of 5 short sections, each ~3-6 lines + a tile/CTA:

```
Hero
    H1: Introduction
    Lede: "beava is a single-binary feature server. Push events in over HTTP,
    declare aggregations in Python, query features by entity key. One Apache 2.0
    binary. No Kafka, no Flink, no Redis-with-Lua."
    [Code: the 13-line pipeline + the two-curl loop, same as today]

§ The whole loop in 60 seconds (current "That's the whole loop" para → keep)

§ Quick vision  ← NEW
    3-4 bullets, lifted from /docs/vision/why-beava/:
      • Real-time features without a streaming team
      • Same shape as pandas — groupby + agg, but live forever
      • One binary you pip install, brew install, or docker run
      • Fits on one big box (100M entities at ~7 KB each is in budget)
    Tile: "Why beava — the longer story →" → /docs/vision/why-beava/

§ Quick commitment  ← NEW
    3 bullets:
      • **Apache 2.0, forever.** No source-available rug-pull. The same binary
        you self-host is what we run in our cloud.
      • **Single binary, single thread, single port.** No external services.
        No telemetry without you opting in.
      • **WAL + snapshot durability.** Crash mid-flight; tables come back
        exactly where they were.
    Tile: "Open-source commitment in detail →" → /docs/vision/open-source/

§ Three paths forward  ← NEW (replaces "Start here" + "Read on")
    3-card grid:
      [Builder]    Quickstart                 → /docs/get-started/quickstart/
                   "pip install beava, ship a feature in 60s."
      [Evaluator]  Why beava                  → /docs/vision/why-beava/
                   "The gap we felt and the bet we're making."
      [Browser]    Operator catalog           → /docs/operators/
                   "All 53 aggregation primitives, with cost classes."

§ Quick contact  ← NEW
    4 channels in a horizontal row:
      • GitHub repo               → github.com/beava-dev/beava
      • Discussions               → github.com/beava-dev/beava/discussions
      • Discord                   → discord.gg/Jnx89PN9
      • Talk to the founders      → calendly.com/hoang-beava/30min
    Sub-line: "30 min, no sales pitch."

[Pager: next = Quickstart → /docs/get-started/quickstart/]
```

Estimated render: ~1 vertical screen on desktop; nothing more than 6 lines per section. This is the "launchpad" pattern Linear/Bun/DuckDB use. The current page is closer to a PEP-style intro paragraph; the user is asking for an index page.

### Proposed sidebar restructure

**Before: 10 sections / 47 entries.**

**After: 6 sections / 33 entries.**

```
1. GETTING STARTED          (open by default)
   - Introduction              /docs/
   - Install                   /docs/install/      ← orphan rescued
   - Quickstart                /docs/get-started/quickstart/
   - Define a pipeline         /docs/get-started/define-a-pipeline/
   - Push events               /docs/get-started/push-events/
   - Query features            /docs/get-started/query-features/

2. CONCEPTS                  (open by default)
   - Streams                   /docs/concepts/streams/
   - Tables                    /docs/concepts/tables/
   - Windows                   /docs/concepts/windows/
   - get and mget              /docs/concepts/get-and-mget/
   - Freshness                 /docs/concepts/freshness/
   - Embed mode                /docs/concepts/embed-mode/         ← moved from Architecture
   - Processing-time only      /docs/concepts/processing-time-only/← moved from Architecture
   - Global aggregation        /docs/concepts/global-aggregation/  ← moved from Architecture

3. VISION                    (collapsed by default)
   - Why beava                 /docs/vision/why-beava/
   - Open-source commitment    /docs/vision/open-source/
   - Non-goals and tradeoffs   /docs/vision/non-goals/
   - Benchmarks                /docs/vision/benchmarks/

4. REFERENCE                 (collapsed; auto-expand on inner page)
   - Operator catalog          /docs/operators/
       (and 8 family pages, indented one level — same hrefs)
   - Pipeline DSL overview     /docs/pipeline-dsl/overview/
       (Expressions, Compilation rules — indented)
   - Python SDK                /docs/sdk-api/python/
   - TypeScript SDK            /docs/sdk-api/typescript/
   - Go SDK                    /docs/sdk-api/go/
   - Cross-language parity     /docs/sdk-api/shared/
   - HTTP API                  /docs/http-api/
   - Wire spec (TCP framing)   /docs/wire-spec/
   - Schema evolution          /docs/schema-evolution/
   - Error codes               /docs/error-codes/

5. ARCHITECTURE              (collapsed by default)
   - Single-thread apply       /docs/architecture/single-thread-apply/
   - mio data plane            /docs/architecture/mio-data-plane/
   - WAL + snapshot            /docs/architecture/wal-snapshot/
   - Memory budget             /docs/architecture/memory-budget/
   - Memory governance         /docs/architecture/memory-governance/  ← orphan rescued
   - Observability             /docs/architecture/observability/

6. COMMUNITY                 (collapsed by default)
   - About RFCs                /docs/community/rfcs/
   - RFC-001 … RFC-006         (indented one level)
   - Weekly dev calls          /docs/community/dev-calls/
   - Contributing              /docs/community/contributing/
   - Discussions ↗
   - GitHub ↗
```

**Deleted:** the dead `/docs/concepts/events-vs-tables/` and `/docs/concepts/lifetime-aggregation/` sidebar links — both 404 today.

**Justification for Priya (YC fintech CTO):**
- Top 3 sections are what she actually reads in order: how do I run it (Getting started) → what are the primitives (Concepts) → why should I trust the bet (Vision). Today Vision sits at #2 ahead of Concepts; for a builder Concepts comes first; for an evaluator Vision is one level deeper but linked from Introduction. Both personas served.
- Collapsing Operators / SDK / Wire / Pipeline DSL into a single `Reference` supersection (using indented sub-items) cuts the sidebar from 10 collapsibles to 6 — half the cognitive load. Catalog-style references belong together; nobody navigates "Wire & API" as a peer of "Operators."
- `Community` absorbs `RFCs` because RFCs are a community artifact — the community section now also serves as "what's coming next." `Wire & API` as a top-level was always two technical docs and two reference appendices (Error codes, Schema evolution).
- Architecture stays as a top-level because for a sysadmin / SRE persona, "where does the WAL live and what's the memory governance" is a different mental model than concepts or reference. Five clean pages instead of ten mixed pages.

### Cross-link & pager fixes

**Add `<Pager>` (31 pages currently missing):**
- All 8 Operators family pages + Operators landing + Cost classes (10 pages)
- All 4 SDK pages (Python, TS, Go, Cross-language parity)
- All 3 Pipeline DSL pages
- All 6 Architecture pages (incl. memory-governance once added to sidebar)
- HTTP API, Wire spec, Schema evolution, Error codes (4 Reference appendices)
- 3 misfiled concept pages (embed-mode, processing-time-only, global-aggregation)
- `/docs/install/` once added to sidebar

**Add inline cross-links (P0 specifics):**
- `/docs/concepts/streams/` — currently 0 outbound docs links. Should link to `windows`, `tables`, `operators/`, `get-and-mget`, `freshness`. This is THE foundational page; users land here from Introduction's "Concepts" bullet.
- `/docs/vision/why-beava/` — currently 0 outbound docs links. Should link to `quickstart`, `concepts/streams`, `vision/non-goals`, `vision/benchmarks`. Today an evaluator finishes the "why" page and has no next click.
- `/docs/operators/` — 59 outbound links is great; verify each family page links back to the cost-class doc + at least one Concepts page.
- Quickstart already has 4 outbound links — fine, but should add a one-line link to `/docs/sdk-api/typescript/` + `/docs/sdk-api/go/` for the "I push events from a non-Python service" reader.

**Naming-collision fix:**
- Top-nav `/community/` and docs-sidebar `Community` use the same word for different surfaces. Either rename the docs-sidebar group to `Community & RFCs` or fold `/community/` into `/docs/community/dev-calls/` (current top-level `/community/` is a single page).

---

## Quick wins (top 5 things to ship this week)

1. **Rebuild `/docs/index.html` per the launchpad blueprint above.** The user's #1 ask, single-file change, ~3 hours. Pulls anchor copy from `/docs/vision/why-beava/` + `/docs/vision/open-source/` so no new content needed.
2. **Restructure `DocsSidebar.jsx` to 6 sections / 33 entries.** Delete the 2 phantom links, rescue 3 orphans, move 3 misfiled concept pages back to Concepts. Single-file change. Zero new pages required.
3. **Add `<Pager>` to the 31 missing pages.** Mechanical change — Pager is already a registered component (used in the 16 pages that have it). Each page only needs `<Pager prev={…} next={…}/>` before the closing `</main>`. ~2 hours scripted.
4. **Fix the `@bv.table` example on the homepage.** Either match the locked `@bv.event` + `events.groupby(...).agg(...)` form from Chapter 1 / Quickstart, or update Quickstart to match. The two surfaces should not contradict on the visitor's first two pages.
5. **Add inline cross-links to `/docs/concepts/streams/` and `/docs/vision/why-beava/`.** Currently both have 0 outbound docs links. Each page needs ~5 inline `<a>` tags in its closing paragraph. ~30 min.

---

## Bigger restructure (week 2+)

**PR-1 (week 2): Sidebar restructure + Introduction launchpad.**
- Land #1 + #2 above. Includes the page moves (events vs tables / lifetime aggregation deletions if those concepts are confirmed dead under v0 events-only; otherwise create the missing pages).
- ~0.5 days work, big visual + IA win.

**PR-2 (week 2): Pager everywhere + cross-link audit.**
- Land #3 + #5. Plus: walk every Concepts and Vision page and add a "Related" inline footer (3 cross-links) before Pager. Gives Priya a chain to keep reading.

**PR-3 (week 3): Recipe redirects in Chapter 1 exit.**
- Replace the Chapter 1 "Pick a recipe" CTA with the redirect-intent table in the Guide section above. Removes the dead-end without spinning up any recipe pages. Land #4-Guide-P0 from the Guide section.

**PR-4 (week 3): Homepage code-shape consistency.**
- Land #4 above. Audit every code block on `/`, `/guide/chapter-1/`, `/docs/`, `/docs/get-started/quickstart/` to use one canonical form. The voice-locked memo `project_v0_events_only_scope` (PARTIAL OVERTURN 2026-05-03) should be the source of truth.

**PR-5 (week 3+): RFC status banners.**
- Add a `Status: not in v0 (RFC)` banner to RFC-001 through RFC-006. Especially RFC-002/003 which directly contradict the events-only commitment. Set expectation upfront.

**PR-6 (week 4+): Decide on `/community/` top-level vs docs-sidebar `Community`.**
- Either fold the top-nav `/community/` page into the docs sidebar, or rename the docs-sidebar group. One-word fix, but resolves the ambiguity for a first-time visitor.

---

## Surfaces I did NOT audit

- Visual hierarchy / typography / color — design agent owns this in parallel.
- Search experience (`pagefind`) — out of IA scope, would need live testing.
- Mobile responsiveness — not in this prompt.
- Accessibility (axe / a11y tree) — not in this prompt; would need a separate pass.
