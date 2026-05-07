# Hero redesign — beava.dev landing page

**Date:** 2026-05-04
**Status:** Decisions locked 2026-05-04. Awaiting design pass before implementation.
**Goal:** Reduce above-the-fold cognitive load. Lead with the value prop, not the pun. Show product, not whimsy. Replace FeedBeaver with a live metrics demo that's representative of what beava actually does.

## Decisions locked 2026-05-04

| # | Decision | Choice |
|---|---|---|
| 1 | Headline copy | **"Real-time features without heavy infrastructure."** |
| 2 | "Dam good at streams" placement | Gaegu-font eyebrow above H1 |
| 3 | LiveMetrics visual style | Clean dashboard cards (label + big number + sparkline) |
| 4 | Which three metrics | **Avg time on /docs/ (1h)** · **Pages viewed today** · **Top page this hour** |
| 5 | FeedBeaver disposition | Delete entirely (overrides `feedback_beaver_overflow_keep.md`; memory to be updated after redesign ships) |
| 6 | Cloud waitlist banner | Drop entirely |
| 7 | Backend source | Real beava on beava.dev — paired with dignified-at-any-scale metrics so no inflation needed |
| 8 | PipelineShowcase swap | Yes — show the SiteMetrics pipeline that produces the hero numbers (replaces the orphaned FeedClick pipeline) |
| 9 | Empathy + stakes line (between H1 and lede) | **"If your last 'simple' feature ate a sprint to deploy, you know why we built this."** — dev-empathy register |
| 10 | Homepage section count | Prune from 9 → 5 sections (table below) |
| 11 | Section reorder | Pillars moves to slot 2 (before PipelineShowcase) — name the value prop before showing the demo |
| 12 | Install-tabs eyebrow | **"Run it locally."** — labels the install snippet as a CTA without marketing-speak |
| 13 | 3-step plan presentation | Middot strip below install tabs: **"Push events · Maintain tables · Query by key."** Lede trimmed accordingly |
| 14 | Philosophical line | **"Stream processing shouldn't require a platform team."** Placed as the Pillars section eyebrow (replaces today's "What's different") |

## Homepage structure — 5 sections

Locked 2026-05-04. Down from 9 sections + banner. Keeps the warm aesthetic; matches slatedb-style information density.

| # | Section | What it does |
|---|---|---|
| 1 | **Hero** | Eyebrow + H1 + empathy line + lede + install tabs + CTA + LiveMetrics panel |
| 2 | **Pillars** *(moved up from slot 5)* | 4 cards: REPLACES REDIS+LUA / NO STREAMING STACK / CRASH-SAFE / APACHE 2.0 |
| 3 | **PipelineShowcase** *(absorbs TrustBand)* | 13-line SiteMetrics code + animated dataframes + 3 trust stats inline ("the homepage runs beava" + receipts) |
| 4 | **Recipes** | 5 use-case cards: Personalization / Fraud / Leaderboard / Rate-limiting / Usage metering |
| 5 | **FinalCTA** *(absorbs GuideTeaser)* | Install (repeat) + GitHub star + Chapter 1 link + Discussions |

**Cut or merged:**
- Cloud waitlist banner — dropped
- FeedBeaver — dropped
- GuideTeaser (Chapter 1 callout) → absorbed into FinalCTA
- TrustBand stats → absorbed into PipelineShowcase ("here's the code, here's how fast it runs")

**Tradeoff acknowledged:** absorbing TrustBand into PipelineShowcase removes the perf numbers' dedicated spotlight. Acceptable because the target reader is "is this simple enough?" first, "is this fast enough?" second. Revisit if telemetry shows visitors bouncing before the perf message lands.

---

## Why we're changing this

Current hero leads with a 132 px hand-drawn pun ("Dam good at streams.") and a clickable mascot widget. A first-time reader has to scroll past whimsy to learn what beava is. StoryBrand-style critique: the hero answers "we have humor", not "what we do for you."

Specific problems:
- **Headline tells the reader nothing.** Pun, no value prop.
- **FeedBeaver demo is whimsical, not representative.** Clicking a mascot doesn't show beava doing aggregation, ranking, or anything a fraud / personalization / analytics team would care about.
- **Hero CTA count = 4** (install / Read the guide / Star on GitHub / "click the beaver"). Pick one primary, one transitional.
- **Hand-drawn font dominates.** It's the most striking thing on the page. Brand-positive but obscures what beava is.

The whimsy isn't the enemy — its **size and position** are. We keep the wordmark playfulness; we just don't lead with it.

---

## Proposed new hero

### Layout
Same 1.15fr / 1fr two-column grid (no structural change). Left column = words. Right column = live metrics panel (replaces FeedBeaver).

### Left column

1. **Status pill** (unchanged) — `v0.9.4 · Apache 2.0 · single binary`

2. **NEW H1** — value prop in plain serif (or sans, designer's call). Drop the rotated hand-drawn font here.

   Candidate copy:
   - *"Real-time features without the streaming stack."* ← lead candidate
   - *"Real-time features as application code."* (echoes the Vision page line)
   - *"Streaming features in hours, not quarters."* (recycles the existing tagline phrase)

3. **Hand-drawn accent line** — keep "Dam good at streams." but as a small, secondary tagline beneath the H1 (or above it as a Gaegu-font eyebrow). Demoted, not deleted.

4. **Lede paragraph** — repurpose the existing italic serif line:
   *"Personalization, fraud rules, live dashboards — in hours, not quarters. Push events. Maintain tables. Query by key."*

5. **InstallTabs** (unchanged — brew default)

6. **ONE primary CTA** — `Read the guide` (transitional CTA in StoryBrand language). Star on GitHub moves to FinalCTA only.

### Right column — `LiveMetrics` panel (replaces FeedBeaver)

Clean dashboard cards — three vertical cards, each with a small label + big number + tiny sparkline. Refreshes every few seconds.

- **Avg time on /docs/ (1h)** — average dwell time on docs pages over the last hour
- **Pages viewed today** — total page views over the last 24 h
- **Top page this hour** — most-viewed path over the last hour

Caption underneath: *"Three real beava queries. The pipeline is below."* (pairs with the existing PipelineShowcase section.)

These metrics were chosen so the panel reads dignified at any traffic scale (no "Visitors now: 3" embarrassment for a brand-new site).

---

## Above the fold should now answer in <5 seconds

| StoryBrand slot | Answered by |
|---|---|
| What is this? | H1 value prop |
| Who is it for? | Lede ("personalization, fraud rules, live dashboards") |
| Does it work? | LiveMetrics panel — three live numbers from beava itself |
| How do I start? | InstallTabs |
| What's next? | "Read the guide" |
| Brand voice | "Dam good at streams." accent line + warm palette |

---

## What gets removed from above the fold

- **FeedBeaver widget** — see "Where does FeedBeaver go?" below
- **"psst → click the beaver" hint** — gone (no beaver to click)
- **"Star on GitHub" button in hero** — kept only in FinalCTA
- **Cloud waitlist banner** — drop entirely (already conflicts with the "no product CTAs on home" voice memory `feedback_beava_website_voice.md`)

---

## FeedBeaver — deleted

Per Q5 decision: the FeedBeaver demo is removed entirely. The DOM-eating finale memory (`feedback_beaver_overflow_keep.md`) will be updated to reflect that the brand moment retired with the redesign.

Implementation note: delete `project/js/FeedBeaver.jsx`, the `<FeedBeaver>` render in the hero, the `useBeavaClient` hook reference inside it, the "psst → click the beaver" hint, and any `<script src="...FeedBeaver.jsx">` tag in `index.html`.

---

## Pipeline that backs the LiveMetrics panel

The `PipelineShowcase` section (further down the page) swaps from the orphaned `FeedClick` pipeline to the `SiteMetrics` pipeline that produces the three hero numbers. Hero ↔ code now refer to the same thing — strongest possible proof the homepage actually runs beava.

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

13 lines. Same shape as today's FeedClick pipeline; different data; matches the hero numbers exactly.

---

## Backend — real beava on beava.dev

Per Q7 decision: the backend is a real beava instance reachable from beava.dev. No mocking, no inflation. The three chosen metrics (Q4) are dignified at any traffic scale, so a brand-new site won't look embarrassing — `Avg time on /docs/` is meaningful even with one visitor; `Pages viewed today` is cumulative; `Top page this hour` is a path string.

Wiring needed:
- Deploy a beava instance (any reachable host) running the `SiteMetrics` pipeline above
- Add a tiny client snippet to every page that pushes a `PageView` event on load and on `beforeunload` (with computed `dwell_ms`)
- Add a poll on the homepage that fetches `/features/SiteMetrics/__global__` every ~5 s and updates the three cards

Infra prerequisites this redesign depends on:
- A beava instance reachable from beava.dev (DNS + TLS)
- The `SiteMetrics` pipeline deployed and registered
- CORS or same-origin path so the homepage can query it

If the infra isn't ready when the design lands, the cards can render with placeholder zeros (with a small "warming up" indicator) until the backend is live — better than mocking with fake numbers.

---

## Open questions — all resolved 2026-05-04

See "Decisions locked" table at the top of this document. The 8 design questions raised in this plan have all been answered.

---

## Files that will need to change (implementation phase)

- `project/index.html`
  - `Hero` component — restructure left column, swap right column from FeedBeaver to LiveMetrics
  - `App` composition — drop `<Banner>`, possibly drop `<FeedBeaver>` instance
  - `PipelineShowcase` — swap pipeline source if we go with #8 above
  - `Pillars` / `RecipeGrid` / `TrustBand` / `FinalCTA` — unchanged in this pass (separate review)
- New: `LiveMetrics` component (in `index.html` or a separate `.jsx` if it grows)
- `project/js/FeedBeaver.jsx` — keep as-is if we move to `/playground/`; delete if option (C)
- New: `project/playground/index.html` if we go with option (B) for FeedBeaver
- `build-search.mjs` — add `/playground/` to PAGES if we add the page

---

## Out of scope for this redesign

- Pillars copy / order
- Recipe grid contents
- Trust band numbers (separate "are these the right numbers" decision; current page says ~3M EPS but Vision says 600k/100k — that conflict is an open item but not this redesign's job)
- Final CTA shape
- Footer

---

## Acceptance criteria (when implementing)

- Above-the-fold renders the value prop H1, lede, install tab, one CTA, and a live metrics panel — no FeedBeaver, no banner, no GitHub button.
- "Dam good at streams." is still present somewhere in the hero, smaller.
- LiveMetrics panel updates at least once per 5 s on first paint, doesn't block initial render, doesn't error if the backend is down.
- No layout shift > 50 px after first paint.
- Mobile: panel stacks below text without dwarfing it.
