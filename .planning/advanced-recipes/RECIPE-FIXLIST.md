# Recipe Design Audit

Date: 2026-04-24
Reference: `/guide/chapter-1/index.html`
Scope: five recipe pages under `/guide/recipes/`

---

## Overall patterns (common across all recipes)

These issues show up in EVERY recipe — fix them in one pass, not per-recipe.

1. **No pandas↔beava warm-up before the pipeline block.** Chapter 1 has a dedicated Part 1 ("Meet beava") with a side-by-side `events.groupby("user_id").agg(...)` pandas block next to the beava equivalent (Ch1 lines 455-499). Every recipe jumps from `<Problem/>` straight to `<ThePipeline/>`. A reader who lands on fraud or rate-limiting via SEO never sees the pedagogy. Suggested pattern: one 6-10 line block matching the `ch1-two` grid, using each recipe's key column (fingerprint/customer_id/game_id/ip/user_id) so the analogy is domain-specific.

2. **`.composer-card` redefined in every recipe's `<style>` block (identical copy).** Ch1 defines it at line 194; fraud line 209, personalization line 37, rate-limiting line 45, usage-metering line 167. Leaderboard is worse — it renames the class `.lb-composer` and ships an entirely separate copy (lines 39-84). Move `.composer-card`, `.composer-hd`, `.composer-title`, `.composer-title .tag`, `.composer-random`, `.composer-divider` into `/styles/site.css` and delete from every page. Leaderboard's `.lb-composer` block needs renaming back to `.composer-card`.

3. **`.memscale` block redefined in every recipe (identical copy, ~70 lines each).** Same root cause — hoist into site.css.

4. **`.pill` / `.pill.idle` / `.pill.reg` / `.pill.done` redefined in every recipe.** Same fix — hoist to site.css.

5. **`AskClaude` re-implemented inline in every page.** Ch1's definition (lines 589-630) hardcodes `pose-3` regardless of the `mascot` prop (see line 592: `const conf = ASKCLAUDE_MASCOTS['pose-3'];`). Recipes HONOR the `mascot` prop, so they render different mascots. Per the spec ("AskClaude tips should always use `mascot-pose-3.svg`"), the recipes are wrong and Chapter 1 is right. Either (a) hoist `AskClaude` into `Shared.jsx` and hardcode pose-3 there, or (b) patch each recipe's local AskClaude to ignore the mascot prop. Specific offending calls: fraud line 1352 (`mascot="geometric"`), personalization line 1013 (`mascot="work-pose"`) + line 1181 (`mascot="geometric"`), rate-limiting line 1010 (`mascot="geometric"`), usage-metering line 1068 (`mascot="geometric"`).

6. **`CodeBlock` re-implemented inline in every page** with the same `user-select: none` + `aria-hidden="true"` tokens. Hoist to `Shared.jsx`.

7. **`RegisterButton` re-implemented inline in every page.** Hoist to `Shared.jsx`.

8. **`QueryPanel` re-implemented inline in every page.** Ch1's QueryPanel (lines 690-845) is the reference; each recipe has a slightly-adapted copy. Consider hoisting a base `QueryPanel` into `Shared.jsx` that takes a `buildResponse` callback and an optional `renderSecondRequest` slot for two-table recipes (fraud, rate-limiting).

9. **No "Skip the browser — run it for real" spin-up box.** Ch1 has a hero-level block (lines 413-448) showing `brew install` / `docker run` / `curl /health`. Recipes skip this. Arguably intentional (Ch1 is the tutorial entry point) but if a reader enters via SEO on `/guide/recipes/fraud/`, they never see the "one binary" promise. Consider either a shared slim `RunItForReal` component in `Shared.jsx` or a cross-link from each recipe hero back to Ch1.

10. **Em-dashes in prose.** Per the style rule ("no em dashes in new copy"), the recipes violate this in several places. Chapter 1 also uses em-dashes in placeholder text ("— click Run query →", "— live, {latency}ms") and a few prose lines — those patterns are grandfathered since they're in Ch1 and re-used verbatim. New recipe-specific prose violations are listed per-recipe below.

11. **Mascot rotation `+6deg` vs `-6deg` consistency.** Ch1 uses `rotate(6deg)` for the AskClaude mascot (line 606) and `rotate(6deg)` for the Callout mascot (Shared.jsx line 84). All recipes match (+6deg). No fix needed here — just noting it's correct.

12. **Progress tracking (MarkVisited + completion useEffect) correctness.** All 5 recipes have both. Verified:
   - personalization: visited (lines 319-332), completed (lines 1122-1133). OK.
   - fraud: visited (lines 1148-1162), completed (lines 1276-1288). OK.
   - leaderboard: visited (lines 317-330), completed (lines 1078-1089). OK.
   - rate-limiting: visited (lines 301-315), completed (lines 1104-1116). OK.
   - usage-metering: visited (lines 1142-1156), completed (lines 1173-1186). OK.

---

## Per-recipe fix-list

### Personalization (`/guide/recipes/personalization/index.html`)

**Grade:** 6/10 vs Chapter 1 polish.

Biggest problem: the whole recipe is rendered at `maxWidth: 880`, not Chapter 1's 1040 — the page feels 15% narrower than the reference the moment you click through.

**Fix items (priority order):**

1. **[Visual cadence] Hero+page width mismatch.** Line 337 hero uses `maxWidth: 1040` but the pipeline (line 463), register+composer (line 1147), and NextSteps (line 980) sections use `maxWidth: 880`. Chapter 1 uses 1040 throughout. Change all three `maxWidth: 880` → `maxWidth: 1040` to match.

2. **[Pedagogy] No pandas↔beava warm-up.** Add between `<Problem/>` (ends at line 376) and `<ThePipeline/>` (starts at line 461). Suggested content: a 6-line `events.groupby("user_id").agg(top_categories_7d=("category", ...), recent_products=("product_id", "last"))` side-by-side with the beava version.

3. **[Metric binding] Line 395 annotation `← engagement badge` is too abstract.** The badge only says "For you" (line 596), and `views_24h` drives the text "{row.views_24h} views / 24h" in the meta row (line 594). Rewrite to `← "N views / 24h" in the card meta row` to bind to the actual UI element.

4. **[Metric binding] Line 393 `← recently viewed strip` is correct but weakly bound.** The strip renders the tiles at lines 630-636. Consider `← the product tile strip` or `← the emoji tile row` — the word "strip" is internal-jargon a first-time reader won't map.

5. **[Metric binding] Line 394 `← price tier` is correct — the tier segments render at lines 643-650. No change needed.**

6. **[Metric binding] Line 392 `← category rails` — good.** Consider tightening to `← the horizontal chip row` since "rails" is a JS-land term; "chip row" matches what the reader sees. Optional.

7. **[Component reuse] `composer-card` redefined at lines 37-82.** Remove; inherit from site.css once hoisted (see overall fix #2).

8. **[Component reuse] `memscale` redefined at lines 207-261.** Remove; inherit from site.css.

9. **[Component reuse] `pill` redefined at lines 12-23.** Remove.

10. **[Mascot pose] AskClaude at line 1013 uses `mascot="work-pose"` and line 1181 uses `mascot="geometric"`.** Per spec, all AskClaude tips use pose-3. Change both to `mascot="pose-3"` OR patch the local AskClaude (line 423) to hardcode pose-3 like Ch1 does (line 592).

11. **[YOU BUILT THIS copy] Line 1175 is too abstract.** Current: "Those per-shopper cards are live beava state…". Rewrite to name the specific UI: "That grid of For You cards above — the category chips, the emoji tile row, the price-tier selector — is live beava state rendered from one `/batch/UserFeed` call." Matches Ch1's specificity ("That grid above? That's live beava state rendered as a real dashboard.").

---

### Fraud (`/guide/recipes/fraud/index.html`)

**Grade:** 7/10 vs Chapter 1 polish.

Biggest problem: the demo is a two-column triage+hotlist grid that violates Chapter 1's "one integrated surface" pattern and the IP hot-list feels bolted on — Chapter 1 would have woven it into a single dashboard strip above the user cards.

**Fix items (priority order):**

1. **[Demo cohesion] IP hot-list (lines 883-903) is a separate right column grid-child of `.fd-triage`.** Chapter 1 has ONE integrated dashboard (ticker left, cards right). Fraud has KPIs-strip → triage-cards+IP-hotlist-column. Consider folding the IP hot list into the KPI strip (line 856) as a 4th tile: "top IP · 203.0.113.42 · 8 fails [Deny]" — keeps the ops feel without a competing column. Alternative: move the IP hot-list above the user-card grid as a full-width strip.

2. **[Pedagogy] No pandas↔beava warm-up.** Add between `<Problem/>` (ends line 416) and `<ThePipeline/>` (starts line 533). Suggested content: two side-by-side groupbys — one keyed by `ip`, one keyed by `user_id` — reinforcing the "same stream, two views" claim at line 541.

3. **[Metric binding] Line 434 `← velocity` is too abstract.** `attempts_5m` drives the IP hot-list (line 894: `{r.failures_5m} fails`). Rewrite as `← IP hot list count` to bind to the UI widget.

4. **[Metric binding] Line 436 `← device diversity` is abstract; doesn't appear in any visible widget.** `distinct_users_5m` is computed but never rendered in the triage UI. Either add a chip on the IP row for it, or change the annotation to `← fanout signal` and stop overpromising.

5. **[Metric binding] Line 442 `← fail streak` is correct but generic.** The streak drives the risk score and the big colored action pill (ALLOW/CHALLENGE/BLOCK at line 792). Rewrite as `← drives the action pill (block/challenge/allow)` or `← the streak chip on each card` since there's also a visible chip at line 808.

6. **[Metric binding] Line 444 `← buy intent` is correct but weak.** It contributes +20 to the risk score above $500. Rewrite as `← adds 20 pts when > $500`.

7. **[Component reuse] `composer-card` redefined at lines 209-254, `memscale` at lines 257-314, `pill` at lines 37-48.** Remove per overall fix.

8. **[Mascot pose] AskClaude at line 1352 uses `mascot="geometric"`.** Change to `mascot="pose-3"`.

9. **[YOU BUILT THIS] Line 1346 is close to right but names "live beava state" generically.** Chapter 1 names the specific grid ("That grid above"). Rewrite: "That triage queue of user risk cards, the KPI strip with 'blocked/alerts/clean', and the IP hot list — all live beava state rendered from two POSTs." Or cut to: "Those risk cards with the red-yellow-green action pills are live beava state."

10. **[Copy voice] Line 1327 `Run the query below to stream the fraud queue into this panel.` uses "stream" colloquially.** Ch1 avoids verbing "stream" for the query panel (Ch1 line 841 uses "one POST, one JSON blob back. No streaming, no pub/sub, no SSE."). Rewrite: "Run the query below to fill this panel."

---

### Leaderboard (`/guide/recipes/leaderboard/index.html`)

**Grade:** 7/10 vs Chapter 1 polish.

Biggest problem: the recipe invented a parallel `.lb-composer` class instead of reusing Chapter 1's `.composer-card` pattern, which means the composer looks visibly different (no shared left-accent border, different internal spacing) even though the `.lb-composer` copy tried to mimic it.

**Fix items (priority order):**

1. **[CSS scoping] `.lb-composer`/`.lb-composer-hd`/`.lb-composer-title`/`.lb-composer-divider` at lines 39-84 fork Chapter 1's `.composer-card`.** Rename every `.lb-composer*` to `.composer-card*` (throughout JSX too — line 511 `className="lb-composer"` → `className="composer-card"`, etc.) AND remove the local CSS (inherit from site.css once hoisted). This is a double-fix.

2. **[Pedagogy] No pandas↔beava warm-up.** Add between `<Problem/>` (ends line 376) and `<ThePipeline/>` (starts line 466). Suggested content: `events.groupby("user_id").agg(total_score=("score_delta", "sum"))` next to the beava equivalent, emphasizing the second `@bv.table(key="game_id")` can't be expressed as a single groupby — good pedagogy.

3. **[Metric binding] Line 393 `← rank input` is abstract.** `total_score` is the big number on each leaderboard row (line 659 `{row.total.toLocaleString()}`). Rewrite: `← the big number on each row`.

4. **[Metric binding] Line 394 `← today's best run` is good — binds to the orange "+{row.best} today" chip at line 658. Keep.

5. **[Metric binding] Line 395 `← engagement badge` is wrong — `games_played` is NEVER rendered in the UI.** Either add a small "N games played" line under the handle (line 653) or change the annotation to `← not rendered, used for anti-cheat thresholds`.

6. **[Metric binding] Line 396 `← online indicator` is good — drives the green pulse dot at line 654.** Keep.

7. **[Metric binding] Line 402 `← the leaderboard itself` is good.** Keep.

8. **[Component reuse] `memscale` at lines 203-259, `pill` at lines 16-27.** Remove per overall fix.

9. **[Mascot pose] AskClaude at lines 443 (hardcoded pose-3) already correct.** Both AskClaude calls use the hardcoded pose-3 (line 443 sets `mascot-pose-3.svg` directly, no prop). No fix needed.

10. **[YOU BUILT THIS] Line 1128 is fine but could be tighter.** Current: "That Top-10 reshuffled atomically on every score event you pushed…" — matches the "That grid above?" Chapter 1 voice well. Consider adding one more specific detail: "The medal emojis, the climb-flash animation, the honorable-mentions strip — all rendered from one `POST /batch/Leaderboard`." Optional polish.

11. **[Missing feature] No `WhatYouGet` section.** Fraud has a dedicated "Real numbers on a real box" 4-stat block (lines 1080-1098). Leaderboard jumps from YOU-BUILT-THIS → memscale → NextSteps. Adding a parallel block would align the recipes. Optional; if not done, at least call out in recipe-rework.md that fraud is the outlier.

---

### Rate limiting (`/guide/recipes/rate-limiting/index.html`)

**Grade:** 7/10 vs Chapter 1 polish.

Biggest problem: the AskClaude at line 1009 is NESTED INSIDE the `MemoryScale` component (lines 1008-1013), which makes the RAM widget feel heavy and violates Chapter 1's pattern (AskClaude sits AFTER MemoryScale as a sibling, see Ch1 line 1532).

**Fix items (priority order):**

1. **[Composition] Move AskClaude out of `MemoryScale`.** Lines 1008-1013 embed AskClaude inside the memscale component. Ch1 puts it as a peer after `<MemoryScale/>` (Ch1 line 1532). Extract to the Tutorial component after `<MemoryScale/>` at line 1319.

2. **[Pedagogy] No pandas↔beava warm-up.** Add between `<Problem/>` (ends line 361) and `<ThePipeline/>` (starts line 475).

3. **[Metric binding] Line 380 `← short-burst throttle` is correct — the gauge labeled "1m" at line 621 binds to this.** Consider `← the 1m gauge bar` for tighter binding.

4. **[Metric binding] Line 381 `← hourly cap` correct — binds to "1h" gauge (line 622).** `← the 1h gauge bar`.

5. **[Metric binding] Line 382 `← abuse signal` is vague.** `errors_1h` drives the err-rate gauge (line 623) AND the auto-ban decision (lines 611-613). Rewrite: `← drives err-rate gauge + auto-ban`.

6. **[Metric binding] Line 383 `← bandwidth throttle` is abstract — `bytes_5m` is NEVER rendered in the UI.** Either add a fourth gauge or change the annotation: `← not rendered, used for 429 response headers`. As-is it over-promises a widget that doesn't exist.

7. **[Metric binding] Line 390 `← per-route ceiling` is correct — binds to the endpoint heat strip at line 635-658. Good.

8. **[Component reuse] `composer-card` at lines 45-90, `memscale` at lines 214-271, `pill` at lines 22-33.** Remove.

9. **[Mascot pose] AskClaude at line 1010 uses `mascot="geometric"`.** Change to `mascot="pose-3"` (or hardcode in local def at line 426).

10. **[YOU BUILT THIS] Line 1316 is specific enough — names the dashboard and the "hot path" placement.** Consider naming the individual widgets: "The 1m/1h/err-rate gauges, the BANNED/THROTTLED/OK pills, the endpoint heat strip — all fed from two POSTs and re-rendered every 1.5 seconds." Optional polish.

11. **[Structural] `Tutorial` wraps Register+Composer+Dashboard+Query+Callout+MemoryScale in one component (lines 1083-1322).** Chapter 1 inlines these directly in the `Tutorial` return (Ch1 lines 1318+). This is fine for componentization but means the section padding is inherited from the wrapping `<section style={{ padding: '24px 28px 40px' }}>` on line 1336. The resulting visual spacing is OK.

---

### Usage metering (`/guide/recipes/usage-metering/index.html`)

**Grade:** 7/10 vs Chapter 1 polish.

Biggest problem: the hero mascot is the brand logo (line 365 `logo-mark.png`), not a mascot pose. This breaks the "mascot-per-page" pattern (work-pose for Ch1 + personalization, pose-2 for fraud, pose-3 for leaderboard, geometric for rate-limiting, and then... logo for usage-metering). Feels like a placeholder that was never filled.

**Fix items (priority order):**

1. **[Hero mascot] Line 365 uses `/assets/logo-mark.png`.** Chapter 1 + all other recipes use a mascot pose. Pick an unused variant — `mascot-work-pose.svg` is used by Ch1 and personalization, `pose-2` by fraud, `pose-3` by leaderboard, `geometric` by rate-limiting. Options: reuse `mascot-pose-2.svg` (fits the "Finance trust" vibe — beaver on a log with a pencil) OR commission a new variant. Simplest fix: `<img src="/assets/mascot-pose-2.svg" width={140} height={140}/>` (fraud already uses it, but cross-recipe reuse isn't a constraint since they're different pages).

2. **[Pedagogy] No pandas↔beava warm-up.** Add between `<Problem/>` (ends line 386) and `<ThePipeline/>` (starts line 474).

3. **[Metric binding] Line 404 `← billable: per-call` is OK but abstract.** `api_calls_mtd` drives the "api calls" meter bar on every card (line 675) AND the "est. bill · mtd" big number (line 671). Rewrite: `← drives the "api calls" meter bar`.

4. **[Metric binding] Line 405 `← billable: snapshot` — `storage_gb` drives the "storage" meter (line 676).** Rewrite: `← the "storage" meter bar (GB snapshot)`.

5. **[Metric binding] Line 406 `← billable: per-hour` — `compute_hours_mtd` drives the "compute hours" meter (line 677).** Rewrite: `← the "compute hours" meter bar`.

6. **[Metric binding] Line 407 `← tier lookup` — `plan` drives the tier pill on each card (line 668).** Rewrite: `← the tier pill (free/pro/enterprise)`.

7. **[Metric binding] Line 399 `← "calls" | "gb_stored" | "compute_seconds"` is an inline doc-comment on the stream field, not a pipeline-metric binding.** This is OK as a schema-documentation annotation but it's the ONLY recipe that annotates a stream field rather than a metric. Either remove for consistency with the other recipes OR add similar stream annotations to the other recipes for consistency.

8. **[Component reuse] `composer-card` at lines 167-221, `memscale` at lines 224-288, `pill` at lines 153-164.** Remove.

9. **[Mascot pose] AskClaude at line 1068 uses `mascot="geometric"`.** Change to `mascot="pose-3"`.

10. **[YOU BUILT THIS] Line 1298 is strong ("Finance, alerts, in-app banners, support tools — every system reads the same numbers").** But it doesn't name the specific UI widgets. Add one sentence: "The bill cards with quota meters and over badges, the MTD revenue sparkline, the highest-spender tile — all pull from the same `/batch/UsageMeter` call Finance runs at month-close."

---

## Cross-cutting fixes (Shared.jsx / site.css)

If prioritizing impact, these three changes clean up ~60% of the per-recipe duplication with a single pass:

1. **Hoist `.composer-card`, `.composer-hd`, `.composer-title`, `.composer-title .tag`, `.composer-random`, `.composer-random:hover`, `.composer-random:disabled`, `.composer-divider` from Chapter 1's `<style>` (lines 194-239) into `/styles/site.css`.** Delete identical copies from personalization (lines 37-82), fraud (lines 209-254), rate-limiting (lines 45-90), usage-metering (lines 167-221). For leaderboard: rename `.lb-composer*` → `.composer-card*` throughout JSX AND delete lines 39-84.

2. **Hoist `.memscale`, `.memscale-hd`, `.memscale-title`, `.memscale-seg`, `.memscale-grid`, `.memscale-card`, `.memscale-fine` into `/styles/site.css`.** Delete identical copies from all 5 recipes + Chapter 1 (Ch1 lines 284-341).

3. **Hoist `.pill`, `.pill.idle`, `.pill.reg`, `.pill.done`, `.pill .dot`, `@keyframes pulse` into `/styles/site.css`.** Delete from all 5 recipes + Chapter 1 (Ch1 lines 25-36). Note that leaderboard's `.lb-pill` also forks — same rename-and-delete treatment as `.lb-composer`.

4. **Hoist `AskClaude`, `CodeBlock`, `RegisterButton` into `/js/Shared.jsx`.** The spec for `AskClaude`: always render `mascot-pose-3.svg` at 104x104 with `rotate(6deg)` translate pattern; ignore any `mascot` prop (matches Ch1 line 592 behavior). Currently recipes each define their own and several pass `mascot="geometric"` or `mascot="work-pose"` — all of those should silently render pose-3 once hoisted. Delete all inline definitions once the shared export ships.

5. **Hoist `MemoryScale` IF the per-recipe tier tables (`MEMSCALE_TIERS`, `AWS_INSTANCES`, `PER_ENTITY_BYTES`) can be passed as props.** This is the biggest duplication (~200 lines per recipe) but requires the most care — each recipe has a different `PER_ENTITY_BYTES` and a slightly different "fine print" paragraph. Suggested shape: `<MemoryScale perEntityBytes={700} entityLabel="shoppers" finePrint={...} breakdown={[{k, desc, bytes}, ...]}/>`. Ch1 and fraud have extra content (replica-HA checkbox, per-IP addendum in fraud); those can be rendered via optional `extraRows` slot.

6. **`QueryPanel` is a good candidate for Shared.jsx but harder.** It takes `buildResponse` callbacks that differ per recipe, and fraud+rate-limiting need two-table variants. Leave for a second pass.

7. **Every recipe should have `maxWidth: 1040` on its main content column.** Personalization currently uses 880 in 3 places (lines 463, 980, 1147) — see personalization fix #1. Once personalization matches, all 5 recipes are 1040-aligned with Ch1.

8. **Standardize section padding.** Ch1 sections use `padding: '48px 28px 16px'` (body) and `padding: '64px 28px 24px'` (hero). Spot-checked values across recipes show drift:
   - personalization's `ThePipeline` uses `'36px 28px 16px'` (line 462) — should be `'48px 28px 16px'`.
   - personalization's register+composer section uses `'8px 28px 0'` (line 1146) — unusual padding, suggests this section was originally meant to nest inside ThePipeline.
   - fraud register section uses `'12px 28px 8px'` (line 1302) — tight top padding. Consider `'24px 28px 8px'`.
   - usage-metering tutorial section uses `'20px 28px 16px'` (line 1272) — off-pattern. Use `'24px 28px 40px'` or similar standard.
   Not all drift is wrong (tighter spacing after a register button can read better), but a single consistent padding palette for sections would improve vertical rhythm across the whole guide.
