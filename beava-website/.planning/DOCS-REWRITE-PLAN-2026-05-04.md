# Docs rewrite plan — 2026-05-04

**One-line goal:** condense 86 ai-shaped reference pages into ~30 example-led pages that read like Linear / DuckDB / Stripe, not like internal planning docs.

**Status:** plan only — needs user sign-off before mass edits.

---

## What's wrong now

| | |
|---|---|
| **Tone** | Source markdown leaks 631 references to internal planning artifacts: `Phase 12.9`, `Plan 13.0-14`, `ADR-001-bv-table-partial-overturn`, `V0-MEM-GOV-02`, `project_no_sharded_apply`. None of that belongs in user-facing docs. |
| **Shape** | Pages average 180 lines and lead with prose ("This document specifies…"). Operator pages have 5+ levels of headings before showing a code example. |
| **IA** | 86 pages, deep nesting (`/docs/operators/recency/streak/`), fine-grained per-operator pages. Most users want 5 things; we make them browse 86. |
| **Scope** | A new visitor opening `/docs/` saw a 96-line link directory. (Now fixed — landing rewritten.) |

---

## What good project docs look like

Pulled from sites in beava's reference set (per `feedback_beava_website_voice` — DuckDB / bun / Linear pattern, not Stripe / Materialize):

1. **Code first, prose second.** Every concept page opens with a runnable example in the first 100 px. Prose explains *what just happened*, not what's coming.
2. **Use-case IA, not feature IA.** Top-level sections are jobs ("Build a fraud detector", "Rate-limit an API") — not internal taxonomy ("buffer-geo family operators").
3. **Quickstart is a tutorial.** It's the only page where someone follows step-by-step. Every other page is reference / lookup.
4. **One page = one idea.** Operator catalog is *one* page with a table — not 50 pages each describing one operator. Sub-pages exist only when a thing has its own non-trivial story.
5. **Reference is collapsed by default.** Most readers never click into Wire spec or HTTP API. Don't make them scroll past it.
6. **No internal jargon.** External docs say "we run a single thread for predictable latency"; they don't say "per `project_no_sharded_apply`".

---

## Target IA (proposed)

Sidebar groups, top → bottom. *italic* = collapsed by default. Counts are target page count.

```
Get started        (open)        2 pages
  Quickstart                     pip install + first feature in 60s
  Install                        per-platform binaries, docker, brew

Recipes            (open)        6 pages    ← NEW top-level, example-led
  Fraud detection                card-testing in 18 lines
  Personalization                "seen but didn't click" recency
  Rate limiting                  per-key sliding window
  Leaderboards                   always-fresh top-N
  Usage metering                 per-customer monthly counters
  Live dashboards                site-metrics global table

Concepts           (open)        5 pages
  Streams and tables             @bv.event, @bv.table — combined
  Pipelines                      register, push, query loop
  Windows                        time windows, rollups, freshness
  Querying                       get, mget, batch, latency
  Storage and durability         WAL + snapshot, restart behavior

Vision             (open)        4 pages    (already exists, light edit)
  Why beava
  Open source commitment
  Non-goals
  Benchmarks

Reference          (collapsed)   ~10 pages
  Operator catalog               ONE page, table of all 54 ops, links to families
  Operator families              7 pages: core, sketch, recency, decay, velocity, point/ordinal, buffer/geo
  Pipeline DSL                   1 page, was 3
  Configuration                  1 page (config schema, env vars)
  HTTP API                       1 page, was 1 — keep but trim
  Wire spec                      1 page, was 1 — keep but trim
  Schema evolution               1 page (no change, slop scrub)
  Error codes                    1 page (no change, slop scrub)
  Architecture                   1 combined page, was 5

RFCs               (collapsed)   7 pages    (already exists, no change)
Community          (collapsed)   4 items    (already exists, no change)
```

**Final count:** ~38 pages (was 86). Cut by collapsing per-operator pages into family overviews (–46), per-architecture-aspect pages into one combined page (–4), 3 pipeline-dsl into 1 (–2).

---

## Per-page template

```markdown
# Title (verb-led: "Define a pipeline", "Detect card-testing")

> One-line description. What this page is about, in one sentence.

```python
# Smallest runnable example for this concept.
# 5–15 lines max. Real code, not pseudo-code.
import beava as bv
...
```

Two paragraphs explaining what just happened. Not "this document covers X" —
just "the @bv.table decorator above…".

## Subhead (when needed)

More targeted code, then prose. Lead with code, not theory.

## Reference

Link table or short bullet list. No long prose at the bottom.

## See also

- [Related concept](...)
- [Operator catalog](...)
```

**Rules:**
- Page intro: ≤3 sentences before the first code block.
- Body: 200–400 words. If it's longer, split it.
- No "Note that…" / "It is important to…". Use callouts (`Callout` in `docs-kit.css`) sparingly.
- Sentence-case headings.
- Lowercase `beava` everywhere except sentence-start.

---

## AI-slop scrub list

Strip these from every markdown source file:

| Pattern | Replace with |
|---|---|
| `verified Phase NN.NN YYYY-MM-DD` | drop entirely, or "(measured on a 4-core box)" if the number matters |
| `(per <code>project_*</code>)` | drop, or rephrase: "we run single-threaded" not "(per `project_no_sharded_apply`)" |
| `(per ADR-NNN)` | drop |
| `Plan NN.N-NN` | drop |
| `V0-MEM-GOV-NN` | drop, or rephrase: "lifetime aggregation is bound at register time" |
| `(authored by Plan ...)` | drop |
| `Active phase: ...` | drop entire paragraph |
| `Phase 13.X (engine prep / Python SDK / docs site / packaging)` | drop, or "currently in beta" |
| `CI tripwire enforced by crates/...` | drop |

I will:
1. Run a regex sweep over all 86 markdown files generating a draft scrubbed version.
2. Manually review each file and rewrite the surrounding sentence so it still parses without the dropped reference.

---

## Order of operations

1. **Sign-off on this plan.** ← we are here. Confirm IA + page count + tone target.
2. **Wave 1 — slop scrub** (quick, mechanical): regex sweep on all 86 files, manual fixup of broken sentences. Re-render. About 1 hour.
3. **Wave 2 — recipes** (new content): write 6 recipe pages (`docs/recipes/{fraud,personalization,rate-limiting,leaderboards,usage-metering,live-dashboards}.md`) sourced from existing guide-recipes content + the FinalCTA section of the homepage. Each ≤300 words, code-first. About 3 hours.
4. **Wave 3 — concept consolidation**: merge `concepts/events-vs-tables.md` + `concepts/global-aggregation.md` + `concepts/lifetime-aggregation.md` + `concepts/processing-time-only.md` into 5 short concept pages. Update `render-docs-config.json` IA. About 2 hours.
5. **Wave 4 — operator catalog**: replace 50 per-operator pages with 7 family pages, each containing a small table (operator · signature · 1-line description · link to source). About 2 hours.
6. **Wave 5 — DSL + Architecture consolidation**: 3 DSL → 1, 5 Architecture → 1. About 1 hour.
7. **Wave 6 — re-render + slop re-check + Pagefind reindex.** 30 minutes.

Total: ~10 working hours. Spread over 1–2 sessions.

---

## Open questions for sign-off

1. **Recipe content source.** The website already has `/guide/recipes/{fraud,personalization,leaderboard,rate-limiting,usage-metering,attribution,anomaly,geospatial}/index.html` (8 recipes, React-based). Do you want the docs `/recipes/` to be **the same content as `/guide/recipes/`** (duplicates, but searchable), or **shorter / different / mostly link to /guide/recipes/**? Recommendation: short markdown stubs in `/docs/recipes/` that lead with the canonical 18-line code example and link out to the longer `/guide/recipes/` version for narrative.
2. **Per-operator page survival.** Should I keep `docs/operators/recency/streak.md` etc. as deep-link targets (just slop-scrubbed, not rewritten), or merge them into the family page as anchors? Recommendation: merge into family page as `<h3>` anchors, since per-op pages average <100 lines of mostly boilerplate.
3. **Page count tolerance.** I'm aiming for ~38. If you want fewer (~25), I can be more aggressive — e.g., merge all 7 operator families into one giant catalog page. Recommendation: 38 is the right shape; one page per family lets people deep-link.
4. **Tone calibration.** Should benchmark numbers stay (3M EPS/core, 10ms p99) or get scrubbed too? Recommendation: keep — they're the differentiator. Just drop the "verified Phase 12.9" verification stamp.

Reply with "go", or amend any of the above. After sign-off I'll execute Wave 1 first and check back before Wave 2.
