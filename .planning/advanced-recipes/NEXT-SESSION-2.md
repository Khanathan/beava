# Continuation doc — beava website rebuild against design-system v2

Date: 2026-04-25
Read this first next session.

## What's done

### Pages shipped (v2 pattern)
- `/` home — `index.html` 325 lines · keeps FeedBeaver hero · v2 primitives elsewhere
- `/guide/` guidebook landing — 6 chapter cards in vertical stack, all ready, all interactive · "6 chapters · read in any order" · no separate cookbook section · orange-wash foot CTA
- `/guide/chapter-1/` — pedagogy chapter, 449 lines · 4 features + 2 dashboards (Part 2 simple, Part 3 full)
- `/guide/recipes/fraud/` — 385 lines · IEEE-CIS, 2.8× recall lift
- `/guide/recipes/personalization/` — 356 lines · Yoochoose, 2.9-pt hit@10
- `/guide/recipes/anomaly/` — 357 lines · NAB, 5.2× fewer alarms
- `/guide/recipes/attribution/` — 355 lines · Criteo, 21% misallocation
- `/guide/recipes/geospatial/` — 356 lines · NYC TLC, 12.8s MAE

### Shared infrastructure
- `beava-website/project/styles/site.css` — surface-tier classes, `.live` gated entrance animations, evidence/capability/done blocks, dark-banner, install/section/code/compare/ask-side/pusher/rail/rrow/kpis/ent-grid/strip/cost/done/next-posts primitives
- `beava-website/project/js/Shared.jsx` — 14 v2 components: `PostHeader`, `InstallCallout`, `SectionMarker`, `CodeCompare`, `AskClaudeSidebar`, `AskClaudeChip`, `ActionPill`, `Pusher`, `LiveFeed`, `EntityDashboard`, `StatusStrip`, `RequestResponsePair`, `CostEstimator` (with extended AWS tiers up to x2idn.32xlarge + shard fallback), `NextPosts`, plus rewritten green `YouBuiltThis`, dark-banner `Banner`
- `.planning/advanced-recipes/RECIPE-PATTERN.md` — full conventions doc (width system, color contract, page sequence, send-request gate, animation rules, sketch sizing, mascot mapping)
- `.planning/advanced-recipes/design-system-v2/` + `design-guide-ref/` — extracted design bundles for reference

### Cache busting
All v2 pages reference `Shared.jsx?v=1777089047` so future edits propagate. **Bump the timestamp** when editing Shared.jsx — search-replace across all 8 pages with the same `v=` value.

### Backups
v1 pages saved as `index.v1.html.bak` next to each rewritten file (chapter-1, fraud, personalization). Anomaly/attribution/geospatial were new directories.

## What's good

- Color contract holds: orange = action, green = celebration. No mid-post green, no end-post orange.
- 6 chapters share identical structure; reader builds muscle memory.
- Animations gated on `.live` class so initial reveal paints clean.
- Cost estimator uses honest AWS tier math (full RAM, +50% headroom on data side).
- Each chapter has cited offline-experiment numbers, not aspirational claims.
- Send-request gate forces the user to commit; dashboard reveals only after click.

## What's shallow (the "tutorial doesn't get me in the zone" problem)

User's verdict 2026-04-25: **chapters are passive, not interactive enough**. Specific shallowness:

1. **Register button doesn't do anything different from registering nothing.** Both states allow pushes; difference is just a pill color.
2. **Push events have no stakes.** "Push some events" is vague. No goal, no measurable outcome. Reader doesn't know when they've "got it."
3. **Reading >> doing ratio.** Long prose, then one click, then long prose again. Should be opposite.
4. **No challenge.** Reader is never asked a question they have to answer. Ch1 makes a per-user dashboard but the reader doesn't feel "I made this work" — it just runs.
5. **Code is asserted, not constructed.** The pipeline is pre-written. Reader watches it; never touches it.
6. **All 5 recipes use the same pattern.** By Ch3 the reader can predict every section. Repetition without escalation feels like padding.
7. **No "aha" trigger.** The cited numbers (1.58×, 5.2×, 12.8s) are facts on the page; the reader doesn't *experience* the lift.

## Pending small fixes

- v1 backups (`*.v1.html.bak`) clutter the recipes dir — decide keep-or-delete.
- `/guide/recipes/{leaderboard,rate-limiting,usage-metering}/` v1 pages are not in the new chapter list. Remove or keep as deprecated? They still work standalone.
- `/docs/` linked from nav but doesn't exist as a page.
- `field-guide-ch1.html` and `field-guide-ch2.html` at project root are pre-v2 prototypes — likely safe to delete.

## Next session: interactivity rework

User asked to brainstorm interactive tutorial structure. See "Brainstorm directions" below — ranked by impact / cost. Not yet decided which direction; that's next session's first conversation.
