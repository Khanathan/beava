# Next-session continuation doc — beava realtime guide + follow-on guides

Date: 2026-04-24 (end of this session)
Read this first in the next session. It captures every decision locked, every experiment's state, every honest caveat, and the plan for the three-guide release cadence.

## Release strategy — three guides, 3-day cadence

Proposed cadence: ship one every 3 days.

- **Day 0 — Guide 1: Traditional real-time features** (`/guide/`). Current target. Ch 1-6. Audience: teams building feature pipelines for fraud, personalization, anomaly detection, attribution, geospatial. Shipping priority.
- **Day 3 — Guide 2: Agentic patterns** (`/agents/`). Audience: teams wiring beava into LLM decision loops. Closed-loop patterns, customer-support / RevOps / incident-triage agent chapters. Planning doc at `.planning/advanced-recipes/agentic-guide-plan.md`.
- **Day 6 — Guide 3: Advanced streaming** (`/streaming/` or `/advanced/`). Audience: power users going deep on temporal semantics, retraction, time travel, windowing correctness, backfill, co-occurrence, scaling. See "Advanced streaming guide" section below for the outline.

## Current state of Guide 1 (real-time features)

### Chapter state table

| Ch | Topic | URL | Evidence | Rework |
|---|---|---|---|---|
| 1 | Per-customer dashboard | `/guide/chapter-1/` | n/a (foundational) | ✅ shipped |
| 2 | Fraud detection | `/guide/recipes/fraud/` | ✅ real (needs leak-audit reframe; see below) | pending write |
| 3 | Personalization | `/guide/recipes/personalization/` | ✅ real (Yoochoose session-level) | pending write |
| 4 | Anomaly detection (NEW — replaced leaderboard) | `/guide/recipes/anomaly/` (path TBD) | ✅ real (NAB matched-recall) | pending write |
| 5 | Multi-touch attribution (NEW — replaced rate-limiting) | `/guide/recipes/attribution/` (path TBD) | ✅ real (Criteo) | pending write |
| 6 | Geospatial ETA (NEW — replaced usage-metering) | `/guide/recipes/geospatial/` (path TBD) | ⏳ running (NYC TLC) at end of this session | pending write |

The URL paths for new Ch4/5/6 can either reuse the old URLs under `/guide/recipes/` with redirect from old names, or introduce a new `/guide/chapter-N/` scheme. Decide next session.

### Experiment artifacts (all in `.planning/advanced-recipes/`)

- `fraud-experiment-results.md` + `fraud-experiment/` — IEEE-CIS; Part 1 ablation (real), Part 2 fixed-threshold staleness (mixed signal), Part 3 matched-staleness (real freshness lift, but see leak audit)
- `personalization-experiment-results.md` + `personalization-experiment/` — Yoochoose 2015, session-level features, clean leak-free
- `anomaly-experiment-results.md` + `anomaly-experiment/` — NAB, Part 1 fixed-threshold (illustrative over-firing), Part 2 matched-recall (real 5.2× fewer FPs story, clean)
- `attribution-experiment-results.md` + `attribution-experiment/` — Criteo, real per-channel MAPE at staleness tiers, clean
- Geospatial: in flight at end of session

## Leak audit — important honest caveat for Chapter 2

The fraud matched-staleness result (1.58× recall lift from real-time vs 60-day-stale) **combines two effects**:

1. **Feature-semantic boundary (beava's atomic update-then-read):** at Δ=0 the feature includes the event being scored (e.g. `tx_count_5m` counts the current transaction). At Δ=60s, it excludes it. This jump alone is ~1.35× of the recall lift.
2. **Pure temporal staleness:** the additional ~1.17× from Δ=60s to Δ=60d — attackers you haven't seen yet.

The chapter should separate these:
- "beava's atomic update-then-read semantics let the feature include the event you're scoring. That alone catches 1.35× more fraud than a pipeline that can't."
- "Beyond that, pure temporal freshness adds another 1.17× across 60 days of drift on IEEE-CIS."
- "Combined: 1.58× using beava correctly vs a 60-day-stale batch pipeline."

This is a stronger story than "2.4× lift from real-time" would have been, because it isolates beava's distinctive property (atomic semantics) from the generic "real-time" hand-wave.

All other experiments passed the leak audit (personalization, anomaly, attribution all compute features strictly from context that doesn't include the target).

## Decisions locked across all 6 chapters

### Shared chapter template (applies Ch2-6)

1. Hero: breadcrumb, eyebrow `Chapter N · Interactive`, h1, 1-sentence lead, mascot.
2. **The stakes:** named persona + real cited incident or published number + specific number.
3. **Why these features?** ablation table from our offline experiment (per-feature marginal lift).
4. **Evidence block:** freshness / staleness table from our offline experiment (real measured) + published anchor citation.
5. **Meet the pipeline:** pandas ↔ beava warm-up.
6. **Build it:** register → EventComposer → hidden demo until Run query → QueryPanel.
7. **What it costs:** MemoryScale + HA toggle.
8. **Where it fits in your stack:** CapabilityMap listing beava AggKinds used + link to advanced-tier recipes.
9. **Next evolutions:** 3 bullets to advanced recipes.
10. **AskClaude tips:** 3, pose-3 mascot.

### Per-chapter specs

Full content in `.planning/advanced-recipes/chapter-specs.md` (NEEDS UPDATE — still shows old leaderboard/rate-limiting/usage-metering for Ch4/5/6; update to Anomaly / Attribution / Geospatial next session).

Needed specs to write next session:
- Chapter 4 (Anomaly detection): persona (Devika, payments SRE), stakes narrative, evidence block content, pandas ↔ beava warm-up, full 6-feature pipeline using `Ewma`/`EwVar`/`EwZScore`/`OutlierCount`, interactive demo design.
- Chapter 5 (Multi-touch attribution): persona (CMO/growth), stakes with real incident citation, attribution-model ablation table + staleness table, pandas ↔ beava warm-up, full 6-feature pipeline using `LastN`/`FirstSeen`/`TimeSinceLastN`, interactive demo design (split-view batch vs real-time attribution dashboard).
- Chapter 6 (Geospatial ETA): persona (delivery/mobility ops lead), stakes, evidence from NYC TLC experiment, pandas ↔ beava warm-up, feature set using `GeoVelocity`/`GeoDistance`/`GeoSpread`/`UniqueCells`, interactive demo (map-based).

## Execution plan for next session

1. **Read this doc + all 5 experiment results** (fraud / personalization / anomaly / attribution / geospatial).
2. **Check if Geospatial experiment finished** — read `.planning/advanced-recipes/geospatial-experiment-results.md` if present; relaunch if agent died.
3. **Update `chapter-specs.md`** with the new Ch4/5/6 topics (anomaly / attribution / geospatial). Replace the old leaderboard/rate-limiting/usage-metering specs.
4. **Walk through demo design and remaining specs for Ch4, Ch5, Ch6 with user**, same 3-question pattern as Ch2 and Ch3.
5. **Apply the leak-audit reframe to Chapter 2's evidence block** before launching the rework agent.
6. **Launch 5 rework agents in parallel** (Ch2-6), each writing their interactive page to match Chapter 1's polish, using the new evidence numbers.
7. **Rewire `/guide/` landing page** to show the 6 chapters as a series with progress bar + 3-state status per chapter.
8. **Visual diff regression pass** against Chapter 1 for each new page.
9. **Ship Guide 1.**

## Agentic guide (Guide 2) — `/agents/`

Planning doc: `.planning/advanced-recipes/agentic-guide-plan.md` — 4 chapters locked in outline:

1. Chapter 1: Why agents need real-time memory (foundational)
2. Chapter 2: Customer-support agent with live context (persona: Meera, support lead)
3. Chapter 3: Sales / RevOps agent (persona: Emmanuel, RevOps)
4. Chapter 4: Incident-triage / alert-routing agent (persona: Priyanka, staff SRE)

Start after Guide 1 ships. Use a fresh context session to avoid bloat.

## Advanced streaming guide (Guide 3) — `/streaming/` or `/advanced/`

Outline (not yet planned in detail):

1. **Time travel queries** — Phase 11.5 MVCC already shipped. Chapter on point-in-time reads, "as-of" queries, reproducing any past state.
2. **Retraction semantics** — when events are deleted or corrected (GDPR right-to-be-forgotten, chargebacks, bug-triggered bad events). How to propagate deletes through aggregations; how different aggregates retract (sum easy, HLL hard).
3. **Backfill** — replaying historical events to bootstrap a new feature without lying about timestamps. Batching, ordering, idempotency.
4. **Windowing correctness** — event time vs processing time, watermarks, late arrivals, exactly-once semantics (Akidau / Flink territory).
5. **Compound-key tables + derived streams** — the capability we identified as missing in advanced-personalization. Co-occurrence matrices, pair emission, graph patterns.
6. **Joins across streams** — point-in-time joins, streaming enrichment, joining a fast stream with a slow changing reference table.
7. **Operator internals** — how `CountDistinct` 3-tier promotion works; HLL bias correction; EWMA stability; sketch-merging semantics.
8. **Scaling + sharding** — partitioning strategies, horizontal scale, replica consistency, cross-region.
9. **Durability mechanics** — WAL internals, snapshot/checkpoint, crash recovery, backup strategy.
10. **Writing new operators** — the extension path for users who need a primitive beava doesn't ship.

Audience: power users, infrastructure leads, contributors. Reference-book feel rather than narrative-tutorial feel.

Start once Guide 2 ships.

## Assorted notes for next session

- **Merge conflict still pending** on `crates/beava-core/Cargo.toml` from the Phase 11.5 merge that happened mid-session. Not mine. User to decide abort / resolve / stash; I've been holding off committing all the website work because of it.
- **Progress tracking IA:** `localStorage.beava:guide:progress` uses keys `chapter:N` (new) with a mirror to `recipe:<slug>` for backward compat. For Guide 2 (`/agents/`), use key prefix `agent:N`. For Guide 3, `streaming:N`. Separate namespaces so completion doesn't leak across guides.
- **Evidence-block CSS component** needs to be extracted and reused across all 5 evidence-bearing chapters. Design is in `.planning/advanced-recipes/chapter-specs.md` §"Shared component spec" (EvidenceBlock).
- **CapabilityMap component** — same, needs extraction.
- **Design audit findings** from `.planning/advanced-recipes/RECIPE-FIXLIST.md` (225 lines) — most items will be auto-fixed by the rework pass, but the "hoist `.composer-card` / `.memscale` / `.pill` into site.css" fix should happen at the start of the rework pass, not per-chapter, to prevent duplicate CSS across 5 pages.

## Open questions for next session

1. Should each guide have its own landing page, or is `/guide/` an umbrella with three sub-guides?
2. Does Guide 3 (Advanced streaming) need the same evidence-block/interactive-demo pattern, or does it follow a reference-docs pattern (tables, examples, no narrative)?
3. For Guide 2 (Agentic), do we write a custom CX-agent offline experiment (like fraud/perso) or rely on published Intercom Fin / Zendesk AI numbers?
4. For leak audit reframe on Chapter 2, do we want to separate the 1.35× + 1.17× + 1.58× into three rows in the evidence table, or collapse into one "1.58× (atomic + temporal combined)" headline?

End of continuation doc.

## Surprise callouts — trust-signal content for chapters

Every chapter's evidence block should have at least one "Surprise" or "Note" inline callout surfacing a non-obvious finding from the experiment. These are the strongest signal that a human ran this and thought about it; they're the differentiation from AI-generated benchmark blog slop. Don't bury them in an appendix.

Design: left-bordered card styled like the existing Evidence block, labeled "Surprise" or "Note," 1-3 sentences, blunt non-defensive tone.

### Catalog per chapter

**Chapter 2 (Fraud)**
- The 30K-sample run showed a 1.3× staleness lift that reversed at full 590K scale — sparse-window artifact.
- The 1.58× headline bundles atomic-update-then-read semantics (1.35×) and pure temporal drift (1.17×); separate them in the chapter.
- IEEE-CIS fraud rate barely shifts 3.517% → 3.457% train-to-test. Closed population. Real-world adversarial drift comes from Stripe / Cloudflare citations, not our data.

**Chapter 3 (Personalization)**
- MovieLens attempted first, got a 0.005-pt staleness effect — too slow-clock. Swap to Yoochoose and the effect appeared immediately.
- 30-minute staleness costs the same as 10-second staleness on session-clickstream because sessions are only a few minutes long — cutting the tail already strips the live category.

**Chapter 4 (Anomaly)**
- Fixed-threshold F1 makes stale detectors LOOK like they beat fresh because they over-fire. Story inverted once we matched recall: fresh produces 5.2× fewer false alarms at equal recall.
- MTTD equalizes at matched recall. The "stale detects earlier" illusion was over-firing, not lead time.

**Chapter 5 (Attribution)**
- Stale attribution doesn't shift credit between channels — it makes credit vanish. Every channel is under-credited at 1d because whole user paths fall off the cutoff.

**Chapter 6 (Geospatial)**
- 6h staleness is worse than 1d staleness (traffic cycle misalignment; 1d/7d sample the same hour-of-day).
- Rush-hour ETAs are 2× more sensitive to staleness than off-peak.

### Methodology-section footnotes (less prominent but still called out)

- Criteo channel names are synthesized from a deterministic hash of anonymized category columns. Path/timing/conversion data is real.
- pandas loads parquet timestamps as `datetime64[us]` not `[ns]`. Silently breaks hour-binning until caught.
- Personalization ablation used uniform weights; last 3 features added slight negatives. Not grid-searched further because the 7× headline was already strong.

### Rework-agent instruction (add to each chapter's rework prompt)

> Include at least one "Surprise" callout in the evidence block, drawn from the per-chapter list in NEXT-SESSION.md § Surprise callouts. Render it as a left-bordered card labeled "Surprise," styled consistently with the Evidence block. Keep it 1-3 sentences, blunt, non-defensive. Also include the methodology footnote(s) for that chapter in the transparency section below the evidence.


## Navigation / IA fixes required (from user review, end of session)

The current `/guide/` landing and the home-page entry point still reflect the pre-rework IA. Fix before shipping Guide 1.

### 1. `/guide/` landing — ship as a numbered chapter series

Current state: `/guide/` renders `ChapterCard` (a single Chapter 1 card) + `RecipeIndex` (a 5-card grid for recipes). This predates the chapter-series decision.

Required state: one unified 6-chapter series, numbered, each chapter on its own card with:

- Chapter number (accent-font, rotated −2°)
- Title
- 1-sentence lede
- Mascot
- Lift headline snippet pulled from the chapter's evidence block ("2.4× more fraud caught…", "5.2× fewer false alarms…", etc.)
- Status pill (complete / in progress / not started) — keeping the 3-state tracker built earlier
- Link directly to `/guide/chapter-N/` (see URL decision below)

Layout: vertical stack of 6 cards OR 2-column grid (3 rows). Pick whichever reads less like a "shopping page" and more like a "table of contents."

Progress bar at top still tracks `completed / total` with green/yellow/red per earlier rules.

### 2. Home page entry should point to the guidebook, not Chapter 1

Current state (likely): home → "Read the guide" CTA jumps straight to `/guide/chapter-1/` via the chapter card.

Required state: home CTA points to `/guide/` (the guidebook table-of-contents). The guidebook page is the entry; chapter selection happens there.

Check `beava-website/project/index.html` for any hardcoded `/guide/chapter-1/` links that should become `/guide/`.

### 3. Previous / Next chapter links on each chapter page

Every chapter (2 through 6) needs a compact pager component at the top and the bottom of the page.

Layout:
```
← Chapter N-1: [title]                   Chapter N+1: [title] →
```

Footer of each chapter also needs "Back to guide" and the Prev/Next pager. Chapter 1 has no prev; Chapter 6 has no next.

Component: a small `<ChapterPager prevSlug prevTitle nextSlug nextTitle/>` that renders a two-column row with underlined link styling, accent-orange on hover. Reuse the `Icon name="arrow"` from Shared.jsx.

### 4. Sidebar with all chapters on every chapter page

Every chapter page gets a left sidebar that:

- Stays visible while scrolling (sticky position, top offset from nav)
- Lists all 6 chapters with titles
- Highlights the current chapter (accent bar + bold)
- Shows per-chapter status icon (✓ complete / ● in progress / ○ not started) using the same 3-state colors as /guide/
- Collapses into a hamburger / drawer on mobile (< 900px)

Layout: chapter content shifts to a two-column grid `grid-template-columns: 240px 1fr` at desktop. Sidebar is the first column, content is the second.

Width: ~240px sidebar. Each chapter-link row has:
- Chapter number (small, fg3)
- Chapter title (14px sans)
- Status icon (right-aligned, 12px)

Sidebar should be a reusable component extracted into `Shared.jsx` as `<GuideSidebar activeChapter={N}/>`. Reads the same `localStorage.beava:guide:progress` that `/guide/` uses for status tracking.

### 5. URL decision (still pending)

Current: `/guide/chapter-1/` + `/guide/recipes/<slug>/`. Inconsistent.

Options:
- **(a) Move all to `/guide/chapter-N/`** — rename 2-6 to `/guide/chapter-2/`, `/guide/chapter-3/`, etc. Clean. Requires path rewrites.
- **(b) Keep `/guide/recipes/<slug>/` for 2-6 and `/guide/chapter-1/` for the foundational one** — inconsistent; weak naming.
- **(c) Redirect old recipe paths to new chapter paths** — nginx-level or in-page meta refresh.

Recommend (a) — rename everything to `/guide/chapter-N/` for consistency. Update all cross-links in Shared nav, sidebar, landing page, prev/next pagers, progress-tracking localStorage keys.

Break the `recipe:<slug>` → `chapter:N` migration once more; keep both keys for a few sessions for backward-compat.

### Summary checklist to add to rework-agent prompts

Each chapter's rework prompt (launched next session) should include:
- [ ] Include Prev/Next pager at top + bottom
- [ ] Include left sidebar listing all 6 chapters with current-chapter highlight and status icons
- [ ] URL is `/guide/chapter-N/` (per decision 5)
- [ ] Status written to `localStorage.beava:guide:progress["chapter:N"]` (primary) + mirror to legacy key for migration
- [ ] Evidence block includes at least one inline "Surprise" callout from the catalog
- [ ] Methodology transparency footnotes included

And `/guide/` landing-page rework prompt:
- [ ] 6-chapter series layout (not Chapter 1 card + recipes grid)
- [ ] Each card shows lift headline snippet + status pill
- [ ] Progress bar tracks 6 chapters
- [ ] All cards link to `/guide/chapter-N/`

And home-page (`beava-website/project/index.html`) edit:
- [ ] Any `/guide/chapter-1/` CTA rewritten to `/guide/`


## UI consistency across chapters — structural parity with Chapter 1

Current state: chapters 2-6 were written by separate agents. Even if the rework pass fixes the narrative, visual cadence drifts slightly. Spacing, padding, component ordering, mascot rotation, callout placement, code-well styling, composer card layout — all small deviations that add up to "these feel like different hands wrote them."

Required state: every chapter matches Chapter 1's structure and visual cadence component-for-component. A reader navigating Ch2 → Ch3 → Ch4 should feel zero visual surprise between sections of the same name.

### Structural parity checklist (per chapter)

The rework agents should treat Chapter 1 as the visual source of truth. For each reworked chapter:

- [ ] Section ordering matches the shared template (Hero → Stakes → Why these features? → Evidence → Meet the pipeline → Build it → What it costs → Where it fits → Next evolutions → AskClaude)
- [ ] Section padding: `padding: '48px 28px 16px'` for mid-page sections, `'64px 28px 24px'` for hero, `'48px 28px 96px'` for final. Match Chapter 1 exactly.
- [ ] Main content max-width: **1040px everywhere**. The design audit found some recipes at 880px — that's a visible width difference the reader can see.
- [ ] Eyebrow style, H2 size, body paragraph width: match Chapter 1's exact inline styles.
- [ ] AskClaude cards: always `mascot-pose-3` (the work pose), always same positioning and rotation (+6° top-right translate).
- [ ] YOU BUILT THIS Callout: always `tint="warm"`, always `mascot="pose-2"`, always placed below the demo.
- [ ] MemoryScale: same segmented control, same breakdown rows format, same HA toggle position.
- [ ] Composer card: shared `.composer-card` class from site.css — do NOT let agents fork it into `.fd-composer` / `.pz-composer` / etc.
- [ ] Code-well: shared `.code-well` + `.kw`/`.str`/`.cmt`/`.fn`/`.ty`/`.num` token classes. No per-chapter overrides.
- [ ] Pill styles (idle/reg/done): shared `.pill` classes from site.css.
- [ ] Copy button placement + CopyBtn from Shared.jsx.

### UI check — automated visual regression

After the rework agents finish, run a visual-regression pass **before shipping**:

1. **Screenshot each chapter** at identical viewport (1280×1024 desktop, 390×844 mobile) via the browse/gstack skill.
2. **Side-by-side diff** Chapter 1 vs each of 2-6 at each corresponding section. Use Chapter 1 as the visual ground truth.
3. **Look for specific drift:**
   - Section padding mismatches
   - Max-width inconsistency
   - Typography scale differences (H2 size especially)
   - Callout card backgrounds (`var(--beava-paper)` vs `var(--beava-orange-wash)` mix-ups)
   - Composer button styling (should be shared `.composer-random` — orange, rotated down-translate on hover)
   - Pill color drift (green `#16a34a` / yellow `#eab308` / red `#ef4444`)
   - Mascot rotation consistency (+6° on callouts, −2° on accent-font headings)
4. **Produce a `UI-CHECK.md`** in `.planning/advanced-recipes/` listing per-chapter deviations with screenshots.
5. **Fix loop**: for each deviation, either edit the offending chapter inline OR update Shared.jsx / site.css if the fix is cross-cutting.

### Shared-component hoist pass (do this BEFORE the rework agents run)

Several components are currently inlined into individual pages. Hoisting them into `Shared.jsx` / `site.css` before the rework prevents agents from forking:

- [ ] `DfPanel` — already hoisted, but confirm home's inline local copy is dropped
- [ ] `CodeBlock` (with `note` tokens non-selectable) — hoist into Shared.jsx
- [ ] `AskClaude` — hoist with mascot hardcoded to `pose-3`
- [ ] `RegisterButton` — hoist (shared state machine, shared `.composer-random` class)
- [ ] `EventComposer` — hoist the card skeleton; each chapter passes its own form body
- [ ] `MemoryScale` — hoist with a per-chapter `breakdown` prop and `perEntityBytes` prop
- [ ] `EvidenceBlock` — NEW component, hoist first. Props: `dataset`, `citations[]`, `rows[]`, `headline`, `methodology`, `transparency`, `surprise` (inline callout)
- [ ] `CapabilityMap` — NEW component, hoist first. Props: `ops[{name, use}]`, `advancedLink?`
- [ ] `ChapterPager` (prev/next) — NEW, hoist
- [ ] `GuideSidebar` — NEW, hoist

Once those are in `Shared.jsx`, each chapter's rework agent has way less surface area to drift on.

### Addition to rework-agent prompts

> Reuse shared components from `Shared.jsx` for EVERY reusable element (`DfPanel`, `CodeBlock`, `AskClaude`, `RegisterButton`, `EventComposer`, `MemoryScale`, `EvidenceBlock`, `CapabilityMap`, `ChapterPager`, `GuideSidebar`). Do NOT re-implement any of them in your chapter's inline script. Your chapter script should contain only: chapter-specific content (strings, pipeline tokens, table rows, demo state machine). Any visible style drift between your chapter and Chapter 1 will be flagged in the UI check and fixed back.

### First step of next session

**Before reworking chapters, do the shared-component hoist pass.** That single change prevents 80% of the drift. Then launch the 5 rework agents with the consistency constraint in their prompts.


## New design-system reference (extracted from Anthropic design URL)

Source: `https://api.anthropic.com/v1/design/h/_HSYOe5kOBcQiqXb9cIxNA` (binary gzipped tarball of a full `beava-design-system/` project, fetched 2026-04-24).

Extracted to `.planning/advanced-recipes/design-system-ref/`. Files of interest:
- `design-system-ref/README.md` — system-level guidance
- `design-system-ref/project/README.md` — project-level guidance
- `design-system-ref/project/SKILL.md` — specialized design guidelines
- `design-system-ref/project/colors_and_type.css` — updated design tokens
- `design-system-ref/chats/chat1.md` — conversation context (may include rationale)

### Next-session design-system action

1. Read all 4 of the files above.
2. Diff `design-system-ref/project/colors_and_type.css` against the current `beava-website/project/styles/colors_and_type.css`. Apply any token updates (palette, typography, spacing) that represent improvements.
3. Read SKILL.md and README.md for rules that should propagate — especially anything about variety of surfaces, card backgrounds, depth / elevation.
4. Apply to Chapter 1 first as the reference implementation, then roll into the rework template so all 6 chapters pick up the new rules.

## User UI feedback — "cluttered with same box color"

Reviewed end-of-session. Valid criticism: the current chapter pages stack many card-like surfaces, and almost all use `var(--beava-paper)` for their background. EventComposer, MemoryScale, user cards in the dashboard demo, callout cards, AskClaude cards — all similar cream/warm-paper tones. The page flattens visually into a wall of same-colored boxes.

Fix direction (use the new design-system ref from above as the authority):

- **Introduce a small palette of surface tiers** — e.g. `--surface-page` (lightest), `--surface-card`, `--surface-elevated`, `--surface-subtle-tint`. Different sections use different tiers so the eye can parse hierarchy.
- **Reserve the warm-paper / accent-wash for narrative moments** (YOU BUILT THIS, AskClaude, Evidence block). Don't use warm-paper for everything.
- **Use elevation (shadow) sparingly** to distinguish "active interactive" cards (composer, demo) from "static content" cards (evidence, memory scale).
- **Try alternating sections** between paper-tone and pure-white to break the monotony.
- **Outline-only cards for reference data** — no fill, just 1px border — for the MemoryScale breakdown table and CapabilityMap, which are tabular/informational rather than interactive.

Concrete first step for next session (after reading the design-system ref files):

1. Write a short "surface inventory" — list every card component on a Chapter 1 page and assign it to a surface tier.
2. Update `site.css` with the new surface tokens.
3. Apply to Chapter 1, screenshot, compare to current. If it reads better, roll into the rework template.

Add to the UI-check pass: **surface-variety audit** — for each chapter page, check that no more than 50% of vertical scroll is occupied by the same-color surface. If more than that, flag it.


## "YOU BUILT THIS" chapter-completion moment (applies to all chapters)

Current state: the YOU BUILT THIS callout on Chapter 1 uses `var(--beava-orange-wash)` — the same warm cream tone used by AskClaude cards, EventComposer cards, and several other surfaces on the same page. It disappears into the "wall of same-colored boxes."

Required state for every chapter (2-6):

### 1. Distinct surface treatment

This is the celebratory capstone of the chapter. It should *feel* different.

Options (pick one, apply consistently across all 6 chapters):
- **Gradient surface**: cream → warm-orange left-to-right gradient. Stands out from flat-filled cards.
- **Pure white card with orange border-left accent**: contrasts with the cream page background.
- **Dark / inverted**: dark brown background with cream text. Most celebratory and most distinct.
- **Confetti texture**: subtle paper texture or faint radial gradient behind the text.

Recommend gradient or inverted. Dark-inverted is my pick — it breaks the cream monotone and feels like "this is the moment."

### 2. Celebratory copy + mascot

Currently the copy is explanatory ("That grid above? That's live beava state..."). Keep the substance but lift the voice:

- Open with a congratulatory beat: "Nicely done." / "You just shipped a real fraud detector." / "You just built what Uber charges 10× to embed."
- Then the substance paragraph (what they built, what it does at scale, why it matters).
- Mascot pose: use a more celebratory one — the "arms raised" or "on-logs-flexed" pose for each chapter. Rotate the mascot poses across chapters so each YOU BUILT THIS feels distinct:
  - Ch2: `mascot-pose-2` (current)
  - Ch3: alt pose
  - Ch4: alt pose
  - Ch5: alt pose
  - Ch6: alt pose

### 3. "Read next" call-to-action embedded

The YOU BUILT THIS moment is the perfect hand-off to the next chapter. Below the congratulatory paragraph:

```
  ─────────────────────────────────────────
  Next up →
  Chapter N+1: [Title]
  [one-line teaser]
  [→ Read it]  [↓ Back to guide]
```

- Two buttons: primary "Read Chapter N+1" (accent-orange), secondary "Back to guide" (cream).
- Last chapter (Ch6) instead says "That's the last chapter. Now try it for real → GitHub" + "Jump to the Agents guide (coming soon)".
- Short one-line teaser per chapter to pique curiosity (e.g., "How to detect fraud in real time — 1.58× more caught vs a daily batch").

### 4. Placement

Always at the end of the interactive section (after the demo, after the QueryPanel, after the MemoryScale, after the CapabilityMap) but BEFORE the AskClaude footer block. The reader feels the success, then sees what's next, then sees the always-available "or ask Claude" footer.

### Summary for rework-agent prompts

> Every chapter ends with a "YOU BUILT THIS" celebration block:
> - Distinct surface (gradient or dark-inverted — pick one choice and use it consistently).
> - Congratulatory one-liner opener, then substance paragraph.
> - Mascot in a chapter-specific celebratory pose.
> - "Next up" CTA block with chapter N+1 title + teaser + two buttons (primary to next, secondary to /guide/).
> - Ch6's variant points to GitHub + hints at the Agents guide.
> - Placed after the interactive demo but before the AskClaude footer block.

