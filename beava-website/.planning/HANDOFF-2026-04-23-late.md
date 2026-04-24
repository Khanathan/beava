# Handoff — beava-website, 2026-04-23 late session

Generated: 2026-04-23 after the OSS-first redo + live animation + first recipe shipped
Branch: v2/greenfield
Repo: beava (tally codename)
Workspace: `/Users/petrpan26/work/tally/beava-website/project/`
Status: Week 1 + part of Week 2 shipped; Chapter 1 interactive tutorial is the next priority artifact.

## TL;DR for next session

The homepage is done. The /guide/ landing and the first recipe (fraud) are done. The biggest remaining artifact is `/guide/chapter-1/` — a two-half interactive tutorial that teaches beava by building a per-customer analytics dashboard. Everything else is either optional polish or Week 3 scope (/docs/, /community/, /cloud).

Pick up at Chapter 1. See **Immediate next task** below for the full spec.

## How to resume in one minute

```bash
# 1) Restart the local server if it's not running
cd /Users/petrpan26/work/tally/beava-website/project && python3 -m http.server 8765 &

# 2) Open the existing pages to see where things stand
open http://localhost:8765/                              # home (OSS-first, animated dataframes)
open http://localhost:8765/guide/                        # guidebook landing
open http://localhost:8765/guide/recipes/fraud/          # first recipe (Priya's unlock)

# 3) Read this doc, the design doc, and the chapter 1 spec section below
cat /Users/petrpan26/work/tally/beava-website/.planning/HANDOFF-2026-04-23-late.md
cat /Users/petrpan26/work/tally/beava-website/.planning/DESIGN-2026-04-23-oss-first-redo.md

# 4) Review commits from this session to see the shape of changes
git log --oneline --grep='website' | head -25
```

Then read the **Locked decisions** and **Locked positioning** sections below so you don't re-litigate anything already decided. Those decisions are also persisted in user-memory (auto-loads), but skim them once for context.

## What shipped today

Commit sequence on `v2/greenfield` (top = most recent):

```
7b163f0 feat(website): SEO-by-outcome titles + Chapter 1 two-half framing
d4eced0 feat(website): beava is groupby().agg() — matches pandas chain shape
5b3c4e1 feat(website): /guide/ landing + first recipe /guide/recipes/fraud/
80a0451 refactor(website): absolute hrefs for nav, CTAs, pillar + trust links
246370a feat(website): kill-the-stream-SQL punchline + banner absolute href
434d265 feat(website): pandas ↔ beava mapping — add time window to both sides
98f0599 feat(website): pandas ↔ beava mapping — same shape, both have key on agg
ec2a0c8 feat(website): pandas ↔ beava mapping above the live dataframes
c42d436 feat(website): dataframe panels styled with bordered cells + accent header
02bfd6b feat(website): render live animation as pandas-style dataframes
ae81913 feat(website): live pipeline animation below "turtles all the way down"
f22ae8a revert(website): keep FeedBeaver DOM-eating finale on home page
9119d44 feat(website): panel review round 1 — 5 fixes from Priya/Jamie/Sam/Raj
d2dfbc7 feat(website): clickspeed rename + fraud detection + punchier pillars
e71a65c feat(website): chapter 1 teaser + trust band + final install CTA
75ddcad feat(website): recipe grid (5 cards) + responsive collapse rules
c15b8aa feat(website): replace WhatIsThis with 4-pillars grid
1f4422a feat(website): merge pipeline sections into one code block
0b99c97 feat(website): hero subhead swap + drop 3rd line + CTA re-aim
c1cb5fc feat(website): nav redo + dismissable cloud waitlist banner
6b7beaf docs(website): initialize beava-website + OSS-first redo design doc
```

## Current state of each page

| Page | Path | Status | Notes |
|---|---|---|---|
| Home | `/` | **DONE** | 7 sections + banner + footer. OSS-dev voice, 4 pillars, pandas↔beava teaching, live dataframe animation, recipe grid, trust band, final install CTA. |
| Guide landing | `/guide/` | **DONE (stub)** | Hero ("How to build the live features your product actually needs"), Chapter 1 teaser card, 5 recipe cards. Only fraud is `ready: true`; other 4 show "SOON" chip. |
| Recipe: Fraud | `/guide/recipes/fraud/` | **DONE** | Full recipe using the design-doc template: problem → pipeline (24 lines) → run-it (3 curl stanzas) → what-you-get (4 stat blocks) → next steps + "Or just ask Claude". SEO title: "How to detect fraud in real time". |
| Chapter 1 | `/guide/chapter-1/` | **NOT STARTED** | This is the next priority artifact. See **Immediate next task** below. |
| Recipe: Personalization | `/guide/recipes/personalization/` | **NOT STARTED** | Use fraud page as template. SEO title: "How to build personalization for a marketplace". |
| Recipe: Leaderboard | `/guide/recipes/leaderboard/` | **NOT STARTED** | SEO title: "How to build a live leaderboard". |
| Recipe: Rate limiting | `/guide/recipes/rate-limiting/` | **NOT STARTED** | SEO title: "How to build rate limiting with auto-ban". |
| Recipe: Usage metering | `/guide/recipes/usage-metering/` | **NOT STARTED** | SEO title: "How to build usage-based billing meters". |
| Docs | `/docs/` | **NOT STARTED** | Week 3 scope. Chalk-style left-nav + pagefind cmd-K search. |
| Docs sub-pages | `/docs/install/`, `/docs/operations/`, `/docs/performance/`, `/docs/api/`, `/docs/compare/vs-redis-lua/` | **NOT STARTED** | Pillar links and trust-band deep-links point here but currently 404. |
| Community | `/community/` | **NOT STARTED** | Get Involved (GH Discussions + Discord-later) + Follow Along + Contribute + blog-latest-3. |
| Cloud | `/cloud` | **NOT STARTED** | Banner links here. Should be a minimal waitlist form (HTML → serverless endpoint → list). Phase-1 scope. |
| Blog | `/blog/` | **NOT STARTED** | Posts + release notes. 3 seed posts planned ("Why beava", "Benchmarks", "Architecture"). |

---

## Immediate next task: `/guide/chapter-1/` interactive tutorial

### Scope per user direction

User said *(2026-04-23)*: "Chapter 1 should teach users to build an analytics dashboard for each of the user customers with our pipeline. User can click to register pipeline, send event (random event or fill in), see it update in the table and see downstream table update like in our landing page. At the end show the analytics table that they built. It should be some window stuff."

And *(same session, follow-up)*: "hmm first is also a quick intro about beava and show how similar is it to df and how to write simple pipeline with visual example. Then we dive into build an analytics dashboard."

So Chapter 1 has **two halves**:

### Part 1: Quick intro (the pedagogy)

1. **One-paragraph intro**: what beava is (one binary, Python, dataframe-shaped). Pitch Priya's vibe but in learning mode.
2. **The dataframe similarity**: reuse the pandas↔beava comparison pattern from home page (`events.groupby("session_id").agg(window="1h", ...)`) but go slightly deeper. Maybe show 2-3 pandas idioms side-by-side with beava.
3. **Simple pipeline visual**: a **3-line** toy pipeline a reader can mentally execute. Suggested:
   ```python
   @bv.stream
   class PageView:
       user_id: str
       path: str

   @bv.table(key="user_id")
   def UserStats(e: PageView):
       return e.agg(views_total=bv.count())
   ```
   Render it as a visual with a single event in → a single row updated in UserStats. Teaches event=row, stream=dataframe, table=keyed aggregate in three lines.

### Part 2: Build the analytics dashboard (the outcome)

This is the core build. Should feel like a playable tutorial.

**Target artifact**: a per-customer analytics dashboard with metrics like:
- `views_total` (lifetime, unwindowed)
- `views_24h` (windowed count)
- `distinct_categories_7d` (windowed distinct)
- `last_seen` (latest timestamp)
- `avg_session_length_7d` (windowed avg — if supported; else distinct counts / rolling max)

Pick 4–5 metrics that show a range of operator types (count, distinct, latest, avg) and mix windowed + unwindowed.

**Interactive steps**:

1. **Register the pipeline** — button the reader clicks. Shows a `register pipeline →` CTA. Click triggers an animated "registering..." → "✓ registered" state. Visually shows the pipeline definition getting parsed.
2. **Send an event** — two paths:
   - **"Send random event"** button — generates a realistic PageView with random user_id from a pool, random path, timestamp=now
   - **Fill-in form** — three inputs (user_id, path) plus a send button. Lets the reader craft a specific event.
3. **Watch tables update live** — three panels like the home-page LivePipelineAnimation, but specialized for this tutorial:
   - **Events panel**: last 3 events sent
   - **UserStats table** (intermediate): shows the user just affected by the last event, with their metrics updating
   - **Dashboard table**: the final per-customer analytics dashboard with all 4-5 metrics, one row per user seen so far
4. **Final state**: after 5-10 events, the reader sees a full dashboard table with real-looking per-user metrics. That's the "you built this" moment.

**Interaction notes**:
- Use the same dataframe-table styling as the home page's DfPanel (`border-collapse`, accent header row, row-index column, full cell borders in warm palette). Consistency reinforces "these are dataframes" for the whole site.
- Flash animation (`anim-row td` keyframe in `site.css`) should be reused on row updates.
- The "send random event" button can trigger a small burst (3 events) on first click to populate the tables faster, then single events per subsequent click.
- Ideally the pipeline can be **edited inline** — the reader sees the 3-line pipeline, then a slightly expanded 6-line version for the analytics dashboard. Could be a "toggle: basic / analytics" switcher, or just static progression down the page.

### Page structure proposal

```
Breadcrumb: Guide / Chapter 1

Hero
  Eyebrow: Chapter 1
  H1: How to build a per-customer analytics dashboard
  Lead: 10-minute interactive build. Meet beava, write your first pipeline,
        extend it into a real analytics dashboard. Zero installs.

Part 1 — Meet beava
  Subheading: "Same shape as pandas."
  Intro paragraph (1-2 sentences)
  Pandas ↔ beava side-by-side (reuse from home, maybe add a second example)
  "Now let's write a pipeline."

Part 2 — Your first pipeline
  Subheading: "Three lines."
  [Code block: 3-line PageView + UserStats pipeline]
  [Button: Register this pipeline]
  Visual: pipeline state changes to "registered"
  [Button: Send a random event]
  Visual: event appears in events panel, row updates in UserStats table

Part 3 — Build the dashboard
  Subheading: "Now the real thing."
  [Code block: expanded 6-8 line pipeline with all 4-5 metrics]
  [Register this version] — replaces the first pipeline
  [Send random event | or fill in your own]
  Live tables (events + UserStats intermediate + Dashboard final)
  After N events: "You built this."

Footer
  [Next: pick a recipe →] links to /guide/
  [Star on GitHub]
```

### Technical notes for the build

- Use React-via-Babel pattern (same as all existing pages).
- Load `/js/Shared.jsx` for Banner / Nav / Footer / Button / Icon / Eyebrow / CopyBtn.
- New widgets to write in the page's inline script:
  - `<RegisterButton pipeline={...}/>` — animated "registering" state machine
  - `<EventComposer onSend={...}/>` — random + fill-in
  - `<LiveTables events sessions dashboard/>` — three DfPanel instances wired to shared state
- Reuse DfPanel component shape from home (copy inline; DfPanel isn't in Shared.jsx yet — consider promoting it).
- All simulated client-side — no backend. Same as home-page LivePipelineAnimation.
- Path is `/guide/chapter-1/index.html`. Use absolute hrefs (`/assets/`, `/styles/`, `/js/Shared.jsx`).
- Responsive: collapse the 3 live tables to 1-column on mobile via CSS media query.

### Why this chapter matters

Two jobs in one artifact:

1. **Primary SEO / search target**: "how to build per-customer analytics in real time" is a high-intent query. Per the positioning memory, every tutorial owns an outcome in its page title.
2. **Activation funnel**: Chapter 1 is where a visitor turns into a user. By the end of the tutorial they've *operated* a beava pipeline (even simulated), which is the single strongest signal that beava is real and easy. The FeedBeaver widget on the home already does this viscerally; Chapter 1 does it pedagogically with explicit pipeline code the reader can copy.

---

## Locked decisions (do NOT re-litigate)

Captured during this session. Any future session that sees evidence of these being re-proposed should flag them.

### Voice and positioning

- **Voice is OSS-dev (DuckDB/bun/Linear), NOT SaaS-product (Tinybird/Chalk/Materialize).** No "Book a demo", no customer logos, no enterprise framing, no pricing tables on home. See `feedback_beava_website_voice` memory.
- **/guide/ is a real-time use-case guidebook.** Pages target outcomes ("How to detect fraud"), not the tool ("beava for fraud"). See `project_guide_seo_positioning` memory.
- **Target user for conversion is Priya** — YC fintech CTO, 10-person team, already left Redis+Lua. See `project_beava_website_ia` memory.

### Home page IA

- **7-section home** (~6.6vh total): Banner → Nav → Hero → PipelineShowcase (with live dataframes + pandas mapping) → Pillars (4 cards) → RecipeGrid (5 cards) → GuideTeaser → TrustBand → FinalCTA → Footer.
- **4 pillars** (not 6, not 3): SINGLE BINARY / 40+ OPERATORS / WAL-DURABLE / APACHE 2.0. Body copy is benefit-led (REPLACES REDIS + LUA / NO STREAMING STACK / CRASH-SAFE BY DEFAULT / APACHE 2.0, FOREVER).
- **Nav is `Logo | Guide | Docs | Community | GitHub★`** — no Blog (it lives under /community + footer), no Cloud (banner only).

### Recipes

- **5 recipes locked**: Personalization / Fraud detection / Leaderboard / Rate limiting / Usage metering. User picked rate-limiting + usage-metering over my recommendation of activity-feed; respect the B2B-substance bet.

### FeedBeaver

- **DOM-eating finale stays live on home.** Panel review flagged it as hostile to eval-mode; user weighed whimsy > eval politeness and chose to keep it. See `feedback_beaver_overflow_keep` memory. The `noOverflow` prop exists for future /fun or gated contexts; don't pass it on the home Hero.

### Cloud offering

- **Banner + /cloud waitlist page.** Never a top-nav Cloud tab. Matches DuckDB/MotherDuck pattern, not ClickHouse. Self-host is always free (noted in banner copy).

### /docs/ search

- **pagefind** for cmd-K. Not Algolia (phone-home clashes with voice). Not Lunr/FlexSearch (pagefind is the docs-specific purpose-built tool).

### Pandas ↔ beava mapping

- Simplified `events.groupby("session_id").agg(window="1h", ...)` form on home is pedagogy-only, doesn't match real beava decorator API. User flagged for future revision. See `project_pandas_mapping_revisit` memory.

---

## Open questions (decide during execution)

1. **Chapter 1 exact metric list.** My proposal: views_total, views_24h, distinct_categories_7d, last_seen, avg_session_length_7d. User should confirm or swap. Principle: show 4-5 metrics with a range of operator types (count, distinct, latest, avg) and mix windowed + unwindowed.
2. **Register-pipeline interaction UX.** Button that animates? Static "registered" badge? Show the curl equivalent?
3. **Event composer shape.** Random-only, fill-in-only, or both? Recommend both (random for impatient readers, fill-in for curious ones).
4. **Table styling for Chapter 1.** Reuse home's DfPanel exactly, or slightly adapt? Recommend reuse — consistency beats novelty.
5. **Promote DfPanel to Shared.jsx?** Currently inlined in home's script tag. If Chapter 1 reuses it, promoting to Shared.jsx makes sense. Low risk.

## Reference files

### Core pages
- `beava-website/.planning/DESIGN-2026-04-23-oss-first-redo.md` — the master design doc (643 lines)
- `beava-website/.planning/HANDOFF-2026-04-23-late.md` — this file
- `beava-website/project/index.html` — home (large, ~700 lines)
- `beava-website/project/guide/index.html` — guide landing
- `beava-website/project/guide/recipes/fraud/index.html` — fraud recipe (template for other 4)
- `beava-website/project/js/Shared.jsx` — Nav, Banner, Footer, Button, Icon, Eyebrow, Callout, CopyBtn
- `beava-website/project/js/FeedBeaver.jsx` — hero interactive widget (large, ~750 lines)
- `beava-website/project/styles/colors_and_type.css` — design tokens (colors, typography, spacing)
- `beava-website/project/styles/site.css` — page layout + animation keyframes

### Memory files (auto-load in future sessions)
- `~/.claude/projects/-Users-petrpan26-work-tally/memory/feedback_beava_website_voice.md`
- `~/.claude/projects/-Users-petrpan26-work-tally/memory/project_beava_website_ia.md`
- `~/.claude/projects/-Users-petrpan26-work-tally/memory/feedback_beaver_overflow_keep.md`
- `~/.claude/projects/-Users-petrpan26-work-tally/memory/project_pandas_mapping_revisit.md`
- `~/.claude/projects/-Users-petrpan26-work-tally/memory/project_guide_seo_positioning.md`

## Commit hygiene

All commits use `feat(website):`, `refactor(website):`, `docs(website):`, or `revert(website):` prefixes. No red-commit discipline enforced for website work (CLAUDE.md §TDD applies to Phase 3+ Beava server code, not HTML/JSX). Copy-only changes commit as-is. Logic changes can ship without tests (there are none in this directory).

## If the next session is NOT me (Claude Opus)

- Read CLAUDE.md at the repo root first for project-wide conventions.
- Memory auto-loads from `~/.claude/projects/-Users-petrpan26-work-tally/memory/MEMORY.md`.
- The user's writing preferences: no em dashes in new copy (use commas/periods/"..."), avoid AI-vocabulary ("delve", "robust", "nuanced", etc). Be direct.
- The server runs at port 8765 serving `beava-website/project/`. Check with `lsof -nP -iTCP:8765 -sTCP:LISTEN` before starting.
- The user moves fast and picks terse "yes" / "next" approvals. Build atomic commits so a reversal is cheap.
