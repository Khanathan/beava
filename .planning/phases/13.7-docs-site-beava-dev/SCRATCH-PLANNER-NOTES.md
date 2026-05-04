# Phase 13.7 Planner — autonomous-decision notes (for user review post-launch)

These are decisions the planner made autonomously per `feedback_logistics_autonomy` (autonomous on logistics; only surface product-shaping or destructive choices to user). All comply with CONTEXT.md's locked decisions D-01..D-04 and the 2 scope reductions; flagging here so user can review post-execute if any feel wrong.

## Logistics-autonomous picks

1. **Plan/wave shape (per CONTEXT.md "Claude's Discretion"):**
   - 4 plans across 3 waves (CONTEXT estimate was "~3-4 plans across 2 waves"; landed at 4 plans / 3 waves because Plan 02 (link audit + Pagefind) requires Plan 01's HTML output → Wave 2; Plan 03 (quickstart polish + /guide/ stubs) is independent of Plan 02 → also Wave 2; Plan 04 (deploy + closure) requires all → Wave 3)
   - Wave 1: Plan 01 (renderer + 82 pages + sidebar) — single big plan because the converter + nav refresh are tightly coupled
   - Wave 2: Plan 02 (Pagefind + link audit) ∥ Plan 03 (quickstart polish + /guide/ stubs) — disjoint files, runs parallel
   - Wave 3: Plan 04 (Cloudflare deploy + closure)

2. **Markdown→HTML converter language: Node (markdown-it) — NOT Python.**
   - CONTEXT D-01 said "Python or Node; planner picks based on existing tooling in beava-website/"
   - Existing beava-website tooling is 100% Node (build-search.mjs, package.json, npm scripts, pagefind devDep)
   - Adding Python would add a second language toolchain to a previously single-language directory — devex friction for contributors
   - markdown-it is the de-facto JS markdown parser, well-maintained, widely-used (>40K stars)

3. **Animated GIF tooling: asciinema + agg with PNG fallback (per CONTEXT D-03 explicit option list).**
   - Plan 03 documents both Option A (asciinema+agg) and Option B (static screenshot) so the executor picks based on tooling availability
   - Phase 13.5 may not have shipped `bv.demo('adtech')` Python helper by the time Plan 03 runs (parallel sibling); fallback is `examples/python/adtech.py` which IS already shipped from Phase 13.0
   - Hard floor: ImageMagick text-art placeholder (always available on macOS+Linux) — better than nothing

4. **Build-time link checker: ADD ONE.** (CONTEXT.md D-01 leaves this open: "uses existing build-time link checker if any, else add one")
   - No existing link checker found in beava-website/
   - Plan 02 adds `scripts/check-links.mjs` (~150-250 LOC Node script)
   - NOT in `npm run build` (deploys shouldn't fail on warnings); IS in `npm run check:links` for developer pre-deploy

5. **/guide/ vertical pages: "coming soon" stubs (vs leave-as-is).** (CONTEXT D-03: planner picks)
   - Picked stubs because CONTEXT says vertical guides will be USER-AUTHORED interactive follow-up — visitors landing on /guide/recipes/fraud/ today might bounce when content doesn't match the new spec docs
   - Banner is additive prepend — no existing content removed

6. **Cloudflare Pages config: CLI-checkable wrangler.toml + dashboard auth.**
   - CONTEXT D-04 left this open: "Cloudflare Pages config exact mechanics (CLI vs dashboard; default CLI for reproducibility)"
   - Picked wrangler.toml (CLI-checkable, repo-tracked) for build settings + manual dashboard step for git-org authorization (unavoidably manual one-time)
   - `_headers` + `_redirects` are Cloudflare Pages conventions — repo-tracked

7. **DocsSidebar IA: 9 sections (added 4 new groups — Wire & API, SDK references, Pipeline DSL, Operators, Architecture; kept 4 existing — Getting started, Concepts, Vision, Community).**
   - Did NOT list 53 individual op pages in sidebar (would balloon to 70+ entries; users browse via family-index pages)
   - Kept legacy /docs/get-started/quickstart/ entries even though Plan 01 also creates a new /docs/quickstart/ — both URLs valid, no destructive removal

8. **Sticky duplicate path: /docs/quickstart/ (new, Plan 01) vs /docs/get-started/quickstart/ (legacy).**
   - Both kept. The new one comes from Phase 13.0 source `docs/quickstart.md`. The legacy one is hand-authored React-rendered prose.
   - Likely follow-up: deduplicate by either redirecting old → new (in `_redirects`) or by deleting the old page in a follow-up phase. Did NOT do either here because (a) destructive removal isn't in scope per CONTEXT, (b) the redirect would break any existing inbound link that pre-dates 13.7.
   - **Flagged for user review:** decide which is canonical, then add a `_redirects` rule in a follow-up plan.

## Departures from CONTEXT.md (none — all 4 D-XX decisions honored verbatim)

If user spot-checks the plans and finds any decision wasn't honored, that's a planner bug — please flag.

## Plans NOT included (deferred / out-of-scope per CONTEXT)

- Vertical guides (adtech / fraud / ecommerce) — DEFERRED per D-03
- TS/Go scope-down patches in `docs/sdk-api/{typescript,go}.md` — Phase 13.6's job
- Versioned docs / Algolia DocSearch / Embedded REPL / i18n — out of v0 scope per CONTEXT `<deferred>` block
- Home-page hero changes (combined latency/throughput/memory headlines from ROADMAP §13.7) — NOT in CONTEXT in-scope items 1-8; CONTEXT supersedes ROADMAP. If user wants the hero changes, that's a separate plan or Plan 13.7-05 amendment.

## Files modified outside `beava-website/`

- `docs/quickstart.md` (Plan 03) — adds image embed reference. This IS Phase 13.0's canonical source-of-truth markdown; the converter reads from here, so the embed lives in source-of-truth. Discussed extensively in Plan 03 Task 2 read_first.
- `.planning/STATE.md` and `.planning/ROADMAP.md` — Plan 04 explicitly does NOT touch these per parent orchestrator constraint. Plan 04 Task 4 writes a `READY-FOR-PARENT-ADVANCE.md` marker file with the proposed edits for the parent orchestrator to apply.
