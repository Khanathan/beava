# Design Review — 2026-05-04

> Note on methodology: the Chrome browser MCP extension was not connected
> in this environment, so this review was conducted by reading the rendered
> HTML, inline-styled JSX components, and the canonical CSS tokens
> (`styles/colors_and_type.css` + `styles/docs-kit.css`). All px sizes cited
> below come directly from the source. A second pass with live Chrome
> measurement is still recommended for final hover/animation polish.

## Executive summary

- Brand voice is on. Cream + burnt-orange + Alegreya serif lands the OSS-developer-cozy feel without enterprise weight. Hand-drawn Gaegu accent is being used as designed: rare, marker-style, on top of orange — strong and unmistakable.
- The landing **Hero is fighting itself for attention**. The Gaegu marker-tagline ("Dam good at streams." 24px orange italic-ish), the H1 (clamp 34–56px serif), and the italic serif lede (22px) all live in the same vertical 80px gutter — three different "headline-feeling" objects stacked. Eye doesn't get a hard signal where to land.
- **Section ordering on landing contradicts the locked plan.** Code comment on `index.html:25-26` says "Hero · Pillars · PipelineShowcase · Recipes · FinalCTA" but the actual `App` component renders `Hero → PipelineShowcase → Pillars → FinalCTA` (no Recipes section at all). PipelineShowcase comes before Pillars, which makes the page open with the curiosity hook ("13 lines, that's the whole pipeline") *before* the differentiation pillars — actually a defensible flip, but the comment should be updated and the Pillars/Pipeline rhythm reviewed.
- Docs Introduction matches Quickstart visually: same `bv-docs-shell`, same `bv-content` h1/h2 type ramp, same `docs-help-callout` mascot moment, same `Pager`. Cohesion goal achieved. One small drift: Introduction has zero h3 but two h2 with very short bodies — feels skinny vs. Quickstart's 7-section spine.
- Guide landing is the weakest of the five — beneath the H1 + orange tagline, the entire page is **one ChapterCard + a "more chapters coming soon" hand-drawn note**. There is no second piece of content. On a 13"+ screen this looks like an unfinished page, not an intentional minimal index. The `RecipeIndex` and `ComingSoonBanner` components are defined in the source but never rendered (`App` only mounts `Nav + GuideHero + ChapterCard + Footer`).

---

## Per-page findings

### Landing (/)

**Strengths**
- The InstallTabs widget is the best piece of conversion design on the page: three tabs (brew/curl/docker), terminal aesthetic, copy button with green-wash success state, plus a real meta-line ("~14 MB · macOS, Linux, Windows · runs on 1 GB RAM"). This is exactly what Priya wants in the first 5 seconds.
- LiveMetrics column on the right is a very strong "the homepage runs beava" device. The three MetricCards with sparklines + mascot watermarks (84/100/80px, rotated −4/+4/−2°) feel earned because they tie directly to PipelineShowcase below.
- PipelineShowcase has the macOS traffic-light dots + filename + green "registered on beava.dev" pill. Code is hand-tokenized (not a `<pre>` dump) — code highlights map 1:1 to the metric cards via colored dot legend below. This is a *systems-thinking* piece of design.
- Color discipline is tight — orange shows up in: GitHub pill, Gaegu marker-tagline, primary buttons, `accent` text in MetricCard sparklines, code keyword color, and the "registered" pill. None of these feel scattershot.

**P0 (block-the-launch)**
- **Three-headline pile-up in the Hero.** Above the H1 sits the Gaegu marker-tagline `"Dam good at streams."` (24px, weight 700, color `var(--accent)`, rotated −2°). 12px below is the H1 in serif (clamp 34–56px). 38px below is the italic serif lede (22px). All three are visually high-weight; none has a clear vertical-rhythm relationship. **Fix:** demote the Gaegu marker to ~14–16px or move it to the right of the H1 as a margin-note. Or shrink the lede to 17–18px (matching the Pillars/PipelineShowcase body lede). Right now the eye lands on the orange marker first because it's the only color, then has to recover to find the H1 — the H1 should be the hero.
- **CTA hierarchy is inverted.** "Read the docs" (primary, lg, orange) and "Join Discord →" (secondary, lg) sit *below* the InstallTabs block. The InstallTabs block has its own CTAs (the copy button, the implicit "run this") and a meta-line ending "Push events · Define pipelines · Query by key." So you get: install → meta → tagline-recap → docs/discord. By the time the eye reaches the buttons it's already absorbed three calls-to-action. Either fold InstallTabs *under* the buttons (buttons-first), or drop the secondary tagline-recap inside InstallTabs (`marginTop: 14`, line 332-339) since it duplicates the H1.

**P1 (worth fixing)**
- The italic serif lede (line 397-404) at 22px serif italic, color `--fg1`, margin 0 0 36px, comes just 38px below an H1 that is *also* serif. Two serifs of the same weight in the same color, only differentiated by italic + size. To Priya's eye these blur into one paragraph. **Fix:** make the lede `--fg2` (warm secondary brown #3a2a1f) or drop italic → switch to `--font-sans` 18px, which is what Quickstart uses for `bv-lede`. Cohesion bonus: the docs lede already lives in sans.
- The "v0 · Apache 2.0 · single binary ↗" pill links to GitHub but uses `whiteSpace: nowrap; flexWrap: nowrap;` — at 1024px viewport the pill is 10–14px shy of the lede width and looks slightly cramped against `--beava-orange-wash`. Consider `padding: 6px 14px 6px 12px` (was 5px/12px/5px/10px) for breathing room.
- PipelineShowcase signature line (`built it once, ships every page →`, line 689) is 22px Gaegu rotated −2°, color orange. It's the *third* Gaegu marker on the page (Hero, MetricCard subline link, this) — borderline noise. Recommend swap to a smaller, sans 13px italic OR keep marker but lose rotation; rotation-as-a-second-time loses charm.

**P2 (nice-to-have)**
- Pillars cards: kicker (`01`, `02`, `03`, `04`) is mono 12px orange, then the title is sans uppercase 13px with 0.06em tracking — i.e. the eyebrow-style title is the same visual weight as the kicker number. The hierarchy `01 / REPLACES REDIS + LUA / body / footer` flattens. Consider title sans 16px sentence-case OR keep uppercase but bump to 14px and tighten kicker to 11px.
- The geometric mascot watermark in Pillars (top-right, opacity 0.06, rotate 8°) is so faint as to read as a printing artifact on cream. Either bump to opacity 0.10 or drop entirely.
- FinalCTA "Three ways in" cards: the GitHub card uses `accent: var(--fg1)` (brown-ink) for the kicker + CTA, while Chapter 1 + Discussions use orange + dusty-blue. Visually the GitHub card looks dimmer. If GitHub is the most important conversion (per Priya persona), it should be at least equal — give it `--accent` like the Chapter 1 card, or put GitHub first in the array.

---

### Guide landing (/guide/)

**Strengths**
- The H1 ("The streaming guidebook") + orange-accent tagline ("Recipes for real-time features in modern applications.") in 18px sans/medium-weight orange is genuinely lovely — clean, focused, centered, bookish.
- ChapterCard is a strong individual component: 2-col grid (1fr / 260px), `var(--shadow-pop)` chunky-sticker shadow, Gaegu "Chapter 1 →" rotated, 32px serif title, mascot work-pose 220×220 to the right. This is the kind of single-card flagship a Linear/bun-style site can pull off.

**P0 (block-the-launch)**
- **The page has only one piece of content.** App renders `Nav + GuideHero + ChapterCard + Footer`. RecipeIndex (5 recipes, `/guide/recipes/<slug>/`) is fully built in the file (line 82-142) but never mounted. ComingSoonBanner same story. Result: on a 1440×900 desktop viewport, after the user scrolls past the ChapterCard there is ~600px of cream-colored void before the footer. This *reads* as broken/under-construction, not minimal-by-design. **Fix:** mount `RecipeIndex` (5 cards with "soon" badges) below ChapterCard, OR shorten the page (compress section padding, attach a "What you'll learn" preview to the ChapterCard, attach a "/docs/get-started/quickstart/ →" CTA at the bottom). Right now the page is 100vh of headline + one card.

**P1 (worth fixing)**
- The "more chapters coming soon ~" Gaegu note (line 69-77) sits centered below the ChapterCard at 26px Gaegu, color `--fg3` (muted brown). At that size + that color it reads as the visual peer of the ChapterCard's H3 title (32px serif), so the eye reads the page as "Chapter 1 — and more chapters" of equal weight. Demote to ~16–18px and color `--fg3` italic, OR remove the rotation, OR move it inside a footer-y "in the works" rail.

**P2 (nice-to-have)**
- The 18px orange tagline under H1 is centered and balanced ("Recipes for real-time features in modern applications.") but uses `var(--accent)` *and* `fontWeight: 500` — slightly heavy. Try 18px `--accent-soft` (#d97a3e) or 17px italic `--font-serif` to lighten without losing the orange.
- The page padding is asymmetric: `64px 28px 24px` for hero, `20px 28px 40px` for ChapterCard. The 20px top means the ChapterCard sits very tight under the tagline — give it 32–40px breathing room.

---

### Guide Chapter 1 (/guide/chapter-1/)

**Strengths**
- Pedagogy is the clear winner here. `Hero → MeetBeava (pandas↔beava `≈` block) → Tutorial Part 2 → Tutorial Part 3 → Next` is paced perfectly: meet, compare, build small, build real, send-them-onward.
- The "Skip the browser — run it for real" callout in the Hero (orange-accent eyebrow, paper-bg card, dark-mode `pre` block with green ✓ marks) is a fantastic dual-track design choice — tells the reader the in-browser sim is the same shape as `brew install`.
- Live UI in Part 3 is genuinely impressive: ch1-stats (4 stat-tiles, 28px serif numbers), ch1-dash (event ticker + user-card grid with avatars/24h bar/category pills/sparkline). The `card-flash` keyframe (orange-wash → transparent over 900ms) when an event lands is exactly the "delight earned" mascot moment the project wants.
- composer-card pattern (3px orange left border, paper→white gradient, accent eyebrow tag, primary "Send a random event" CTA, dashed-divider into "or craft your own" form). Reusable, distinctive, and immediately reads as "input zone."

**P0 (block-the-launch)**
- **Hero H1 + lede vs. Hero spinup-card competing for first impression.** The H1 ("How to build a per-customer analytics dashboard") is `clamp(36px, 5vw, 56px)` serif. 14px below it the lede sits at 17px sans `--fg2`. Then 36px below the lede is the spinup callout *with its own orange-eyebrow + 14px body + dark code well*. So the user is asked to absorb (a) the H1 (b) the lede (c) a parallel "skip the sim, run it for real" pitch — all before reaching Part 1. The H1 + lede needs one beat of breathing room before the pivot. **Fix:** push spinup-card to *after* MeetBeava, OR collapse it to a one-line link ("Want to run beava locally instead? `brew install beava` →") in the right column of the Hero.

**P1 (worth fixing)**
- The Hero column is `1fr 160px` with a 140×140 mascot — at 1280px viewport the mascot column is ~12% of the width and looks like a stock photo on the side of a Medium article rather than the page's avatar. Either 200×200 in a 240px column OR drop entirely (let Hero be a clean centered 1040px text block; mascots already appear in MeetBeava `≈` and the dashboard).
- Part 2 / Part 3 H2 ("Three lines. One table. Live." / "Now the real thing: four metrics, rendered live.") are `clamp(26px, 3.4vw, 36px)` serif, sentence-case, balanced. Good. But they sit only 16px below the Eyebrow, which *also* uses orange and small caps. The Eyebrow + H2 visually merge into a 50px orange-and-serif block. Add a `marginBottom: 8px` between Eyebrow and H2, or use `--fg1` on the H2 with weight 500 (currently 600) so the H2 reads quieter than the brand-orange eyebrow.
- The pandas/beava `≈` separator (28px Gaegu orange, line 422) is a Gaegu use that earns its keep — but the column code-wells next to it have label tags ("pandas (batch)" / "beava (live)") in 11px mono. The "beava (live)" label is bold + accent-colored; "pandas (batch)" is `--fg3` muted. That asymmetry is intentional but reads as if pandas is "deprecated" rather than "the thing you already know." Consider matching weights and letting the ≈ do the brand work.
- Spark-bars in the user-card use `opacity: 0.28` for cold and 0.9 for hot. The contrast difference is 3x — at 28px tall that's enough variance that the spark looks visually noisy when 8+ users are on screen. Consider 0.4 / 0.85, or color-shift hot bars to `--beava-orange-soft` (#d97a3e).

**P2 (nice-to-have)**
- composer-card pattern is reused twice (Part 2 + Part 3) — could it pick up a subtle differentiator? E.g., Part 2 = green-success accent (because UserStats is the introductory table), Part 3 = orange (because Dashboard is the Real Thing). Right now both look identical, which is a missed pedagogical signal.
- "YOU BUILT THIS" Callout (line 1205-1207, tint="warm") sits at the very bottom of Part 3 with a 168×112 geometric mascot top-right, transformed `translate(50%, -50%) rotate(6deg)`. The half-off-canvas mascot positioning is bold and works, but the "warm" orange-wash callout color makes the 19px serif body text contrast borderline. WCAG it; if `--fg1` on `--beava-orange-wash` clears AA at 19px it's fine, else nudge to 20px.

---

### Docs Introduction (/docs/)

**Strengths**
- Layout matches Quickstart 1:1 — `bv-docs-shell` 3-col (252px sidebar, 1fr content, 220px TOC), `bv-crumbs` breadcrumb, `bv-content` h1 (44px serif/700, line 148-152 of docs-kit.css), `bv-lede` (16px sans `--fg2`, max-width 60ch). This is the cohesion the doc rewrite was supposed to deliver, and it delivers.
- The two `<Code>` blocks at the top (Python pipeline + curl test) sit directly under the lede. Quickstart pattern is the same. A reader can copy-paste from this page and have something running. That's the right shape.
- `docs-help-callout` mascot moment at the bottom (work-pose 72×72, rotating Gaegu kicker "Stuck on this one?", orange + ghost CTAs) is the only mascot on the page, used at the right narrative moment (after content, before pager). This is correct mascot economy.

**P0 (block-the-launch)**
- None. The page works.

**P1 (worth fixing)**
- Page is *too short*. Two h2s ("Start here" with 3 bullets, "Read on" with two paragraphs of inline links) totalling ~80 words after the code blocks. Compared to Quickstart's 7-h2 spine, the Introduction looks underweight in the right-side TOC (just two items) and as a destination it doesn't quite earn the "Introduction" title. **Fix idea:** Add a "What's beava?" mini-card row (3 cards: "events / tables / queries") between the lede and the code block, OR add a "What you'll find in these docs" section that shows the docs-tree (gettings-started / concepts / operators / sdk-api / wire-spec / community).
- The `<p>` after the second `<Code>` block ("That's the whole loop: declare a feature, push events, query by key. No Kafka, no Flink, no Redis-with-Lua. One binary, one HTTP port, one Python file.") is 15.5px sans (per `.bv-content p` rules). It's the punchline of the page. Consider promoting to `<p class="bv-lede">` 16px `--fg2` so the visual weight matches its narrative role.

**P2 (nice-to-have)**
- The first `<Code>` block uses a 7-line Python sample; the second is a 3-line bash sample. Together that's 10 lines of code on screen with no syntax highlighting (assuming `<Code lang="python">` doesn't tokenize like the homepage's hand-tokenized PipelineShowcase). If `Code` *is* tokenized via `js/docs/CodeBlock.jsx`, this is fine; if not, parity with the marketing page would help.

---

### Docs Quickstart (/docs/get-started/quickstart/)

**Strengths**
- Reference docs page. 7-section h2 spine with TOC anchors, lede + paragraphs + `<strong>` punchlines (`**Define events. Maintain tables. Query fresh state.**`, `**Real-time features should feel like application code.**`) — reads like a TiDB/Cockroach/Bun tour. Voice is right.
- Curl + JSON example is full and accurate — `POST /push/UserEvent` with full headers + multi-field body, response schema below it.

**P0 / P1**
- None significant for a reference page. The h2 first-of-type rule (`bv-content h2:first-of-type { border-top: 0; padding-top: 0; margin-top: 36px; }`) cleanly hides the rule above the first h2 — small, important detail.

**P2**
- The 3 bold-lead one-liners (`**Start small...**`, `**beava turns events into fresh, queryable state.**`, `**Real-time features should feel like application code.**`) are visually identical to body bold (per `.bv-content strong { color: var(--fg1); font-weight: 600; }`). Consider promoting them to a small "callout-quote" treatment — left orange bar, italic serif — once per section. Right now they sit in body and a fast scroller might miss them.

---

## Cross-cutting design recommendations

1. **Three serifs in the same column on the landing Hero.** Pill (sans), marker (Gaegu), H1 (serif), italic-serif lede, sans-monospace InstallTabs. The italic-serif lede directly under a regular-serif H1 is the bug. Either make the lede sans (matching docs `bv-lede`) or remove italic. Either gets the page back to "two type registers stacked, three at most."

2. **Gaegu accent is being over-used as decorative chrome rather than as marginalia.** Original brief: "marker, sparingly, margin-notes-only." Current count on landing alone: marker-tagline above H1, MetricCard sparkline link, "Re-run now" sub-language in PipelineShowcase callout. That's three. The chapter-card "Chapter 1 →" is the *correct* Gaegu use (single short phrase as a margin-note).  Pull the count down to 1 per page and watch each remaining one carry more weight.

3. **Eyebrow + H2 visual coupling needs more space.** Throughout the site, `<Eyebrow>` (12px orange, all-caps, 0.08em tracking) sits 12–16px above an h2. At that distance + matching color (orange + fg1) the eye reads them as one unit. A 24px gap or a left-aligned hanging-indent eyebrow (margin-left: -84px on >1100px breakpoints) would give the eyebrow its own moment.

4. **Mascot economy is mostly correct, with one anomaly.** Hero's LiveMetrics has *three* mascots (work-pose 84, logo-mark 100, pose-3 80) in a single column. They're rotating and at different opacities so it works — but Priya's reaction at first scroll might be "is this site... too cute?" Consider one mascot per MetricCard *or* one large one anchoring the column.

5. **CTA color hierarchy** — `var(--accent)` (#b85c20 burnt orange) is used for: pill text, marker, primary buttons, code keywords, "registered" pill in PipelineShowcase, the orange dot under active nav links, and the underline-on-hover. Roughly 7 distinct semantic uses. Consider: code-keyword orange should be `--code-keyword` *only* (already is), CTAs should be the only places `--accent` shows up as a fill. Inline-link orange + active-nav orange + button orange is correct; pill-tag and "registered" pill arguably should drop to `--accent-soft` so primary CTAs stand alone.

---

## Mobile findings

(Source-only review — the responsive media queries were inspected but not visually rendered. A pass with Chrome devtools at 390×844 is still recommended.)

- `index.html:428-437` — `@media (max-width: 960px)` collapses Hero to 1col and hides `.beava-hero-mascot`, but **there is no `.beava-hero-mascot` class anywhere in the rendered Hero**. The selector is dead. Either drop the rule, or add `className="beava-hero-mascot"` to the LiveMetrics column wrapper if hiding metrics on mobile is the intent. As written, on a 390px viewport the LiveMetrics column stays visible and stacks below the hero text — which is fine, but verify.
- The 84/100/80px mascot watermarks inside MetricCard absolute-position to `top: 4–6px, right: 4–10px` with `pointerEvents: none`. On a 358px-wide MetricCard (390 viewport − 32px padding) the 100px logo-mark eats almost a third of the card width. The `paddingRight: mascot ? 100 : 0` on the label saves the label from collision, but the `value` (44px serif) is unconstrained — a long value like the Top Page url (`/docs/community/rfcs/` at 17px mono) will run *under* the 80px mascot. Verify in DOM at 390 width.
- `PipelineShowcase` `pre` block has `overflowX: 'auto'` which is correct, but the long line (`bv.App("0.0.0.0:6400").register(PageView, SiteMetrics).serve()`) on a 358px-wide pre at 14px mono (avg ~7.5px/glyph) is 64 chars × 7.5 = 480px → horizontal scroll required. A reader on phone won't see the `.serve()` call without scrolling. Acceptable, but the `<style>` block for ch1 already uses `font-size: 12px` for mobile pre blocks — the marketing PipelineShowcase doesn't. Consider matching.
- `FinalCTA` mascot (`logo-mark.png`, 120×120, `top: -28px right: -8px rotate(6deg)`) is hidden via `.beava-finalcta-mascot { display: none; }` at 760px breakpoint. Good. The 3-card FinalCTA grid collapses to 1col at 760px. The "Talk to founders" Calendly band switches to column at 760px. All correct.
- Guide ChapterCard: `gridTemplateColumns: '1fr 260px'` with no media query in the inline style. The `.two-col` class is used but never *defined* in the inline `<style>` of `/guide/index.html`. So at 760px the card stays 2-col with a 260px mascot column eating ~70% of the screen. **Fix needed.**

---

## Quick wins (top 5 things to ship this week)

1. **Demote the Hero Gaegu marker-tagline.** Drop "Dam good at streams." from 24px Gaegu/orange/rotated to either 14px Gaegu margin-note OR fold into the GitHub pill copy ("Dam good at streams · v0 · Apache 2.0"). Rationale: H1 should land first, not the marker. Three-headline pile-up is the single biggest landing-page miss.

2. **Mount `RecipeIndex` on /guide/.** The component exists, the data is there, the styling is solid. Just add `<RecipeIndex/>` to the `App` render between `<ChapterCard/>` and `<Footer/>`. Result: page goes from "one card and air" to "Chapter 1 + 5 recipes (4 marked 'soon')" — a real index, not a stub.

3. **Switch the landing Hero lede from italic-serif 22px to sans 18px `--fg2`.** Eliminates the same-typeface-fight with the H1 directly above and aligns lede typography with `bv-lede` in docs. Cohesion + clarity in one move.

4. **Fix the dead `.beava-hero-mascot` selector and add `.two-col` mobile rule for guide ChapterCard.** Both are 5-line CSS edits. The ChapterCard one will visibly break at 600–760px. (`.two-col { grid-template-columns: 1fr !important; } .two-col img { display: none; }` at 760px breakpoint, mirroring chapter-1's existing `.ch1-two` rule.)

5. **Update the comment header on `index.html:25-28`** to match the actual section order and inventory (`Hero · PipelineShowcase · Pillars · FinalCTA`). The "5 sections" comment claims a Recipes section that's not rendered, and inverts Pillars/PipelineShowcase. Keeps future maintainers from chasing a phantom.
