---
phase: 10
overall_score: 21/24
pillar_scores:
  copywriting: 4/4
  visuals: 3/4
  color: 4/4
  typography: 4/4
  spacing: 4/4
  experience: 2/4
audited_against: UI-SPEC.md
status: advisory
generated: 2026-04-10
screenshots: not captured (no chromium bundle; dev server on :6501 has empty state only)
---

# Phase 10 Debug UI — Visual Audit

## Overall: 21/24

Phase 10 is an unusually disciplined contract implementation. The locked UI-SPEC §13.1 tokens are lifted verbatim into `app.css` (the first 66 lines are a literal copy of the spec block, comment and all), every user-facing string in `§8 Copywriting` appears byte-for-byte in the shipped markup and renderers, and the accent-warm orange usage is exhaustively restricted to the three reserved sites the spec enumerates. The only pillar that genuinely slips is Experience Design, where two keyboard contracts from `§10 Accessibility` (tab bar `←`/`→` navigation and per-node `Tab`/`Enter` focus) and the enumerated `§7.6` node-detail panel are all unimplemented — a direct consequence of the user's decision to route the interactive DAG drill-in to Phase 10.2, but still a measurable gap against the spec the phase was scored against. Visuals lose one point because the DAG card is the declared focal point of the Topology tab (`§7.1`) but has no empty-state icon wired beneath a 640px empty canvas and no skeleton shimmer on first paint.

---

## Pillar Scores

### 1. Copywriting — 4/4

**Strengths:**

- Every `§8.4` H1/subtitle pair appears verbatim in `index.html:41-42, 53-54, 68-69, 97-98`. No "Dashboard", no "Welcome", no generic filler.
- Empty-state copy matches `§8.6` exactly:
  - `app.js:216-217` — `No pipelines registered` + `Use the Python SDK or REGISTER command to push a pipeline, then reload this page.`
  - `app.js:275-276` — `No streams yet` + `Register a pipeline via the SDK to see streams appear here.`
  - `app.js:411-413` — `No memory data yet` + `Tally will report usage once a stream is registered.`
  - `app.js:317-318, 334-335` — `No features for "{key}"` + `This key has not received any events recently, or has been evicted by TTL.`
- Error-state copy matches `§8.7` exactly — `app.js:232` renders `Could not load topology` with the server error body verbatim via `textContent`, and binds the `Retry` label from the `§8.5` button inventory.
- Wordmark anatomy matches `§8.1` character-for-character — `index.html:16` emits `tally<span class="wordmark-tick">'</span>` with the hairline apostrophe receiving `--accent-warm` at `app.css:134-139`.
- Polling label matches `§8.10` verbatim: `Live · 1 Hz` when live (`index.html:31`) and `Paused · last update {HH:MM:SS}` when paused (`app.js:99-100`). The aria-live region announces "Polling paused" / "Polling resumed" (`app.js:105-106`).
- Footer format matches `§8.11`: `tally v{version} · connected to {host}:{port} · last update {HH:MM:SS}` (`index.html:113`, populated by `app.js:61-65` and `114-125`).
- Input placeholder (`e.g. u_12345`, `index.html:82`) and help text with inline `<code>u_demo</code>` (`index.html:86-87`) are byte-matched to `§8.9`.
- Tone is correctly terse/developer-facing throughout — no second-person exclamation marks, no emoji, no "Welcome back" copy, no celebratory microcopy. Monospace-friendly "computed_features", "Transactions", "ewma_5s" language is used without apology.

**Opportunities:**

- None worth calling out. This is a textbook spec-driven copywriting implementation.

---

### 2. Visuals — 3/4

**Strengths:**

- Dagre-d3 DAG is the load-bearing focal point of the Topology tab and the rendering path is clean: `app.js:167-206` uses default `labelType: 'text'` (documented inline at lines 172-176 as the XSS-relevant choice), applies `rx: 12, ry: 12` on every node per `§7.1`, and fits-to-container via a computed `scale`/`translate` transform rather than baking fixed dimensions.
- Node stroke colors are differentiated by kind: stream nodes stroke `--chart-stream` (blue), view nodes stroke `--chart-view` (purple) — `app.css:381-382`. Hover/selected state adds a 6px drop-shadow glow in the matching accent color (`app.css:388-399`), which correctly uses `filter: drop-shadow(...)` rather than CSS box-shadow (SVG-appropriate).
- Iconography is a single `src/server/ui/icons.svg` symbol library with nine symbols referenced via `<use href=...>`. The `<symbol>` elements use `fill="none"` + `stroke="currentColor"` + `stroke-linecap="round"` uniformly, so every icon inherits its color from the containing context — empty-state icons render in `--text-tertiary`, error-state icons in `--status-error`, all via a single CSS rule at `app.css:645-652`.
- Favicon (`src/server/ui/favicon.svg`) is a hand-drawn 32×32 tally-mark in warm orange (`#ff8a4a`) with four verticals and one crossing stroke, matching `§8.1` exactly. No rasterized PNG, no network fetch.
- Visual hierarchy is correct: tab H1 at 24px, card titles at 20px, body at 14px, captions at 12px (tokens declared at `app.css:11-15`, applied at `app.css:216-228` for tab-header and `app.css:246-258` for card primitives).

**Opportunities:**

- **DAG empty-state is a hole in the focal point.** `§7.1` mandates a `§9.2` empty state inside the topology card with the `tally-mark` icon at 48px and the `No pipelines registered` heading. `app.js:208-220` implements this, but the `.topology-canvas` container is hidden via `style.display = 'none'` instead of rendered side-by-side or overlaid. On first paint with zero pipelines the entire 640px card collapses to a small centered icon block, which is a very different gestalt than the spec's "empty state inside the canvas". Practically the empty variant works, but visually the 640px focal canvas disappears on the exact screen the user lands on. (A skeleton shimmer on the first paint, as `§7.1 States: Loading` specifies with a centered spinner and `Computing topology…` caption, would also help — currently `renderTopology()` renders into an empty SVG until the fetch resolves, with no placeholder content.)
- **No skeleton shimmer at all**, despite the `§9.1` spec locking a shimmer gradient animation and `app.css:621-631` defining the `.skeleton` class + `@keyframes shimmer`. The class is defined but never applied — Streams / Memory / Entity all clear their containers to empty on load and fill on first htmx response, so there is always a ≤1s flash of empty content instead of the 6-row skeleton the spec mandates. The CSS class is dead code until a renderer uses it.
- **`§7.1` optional metadata line (`{key_field} · {feature_count} features`) is not rendered under DAG nodes.** Nodes are labeled with just the stream name — correct and XSS-safe, but one of the spec's concrete "under the label" instructions is silently dropped. Low-impact because the dagre-d3 default text label doesn't support multi-line content trivially, but worth noting as a spec-to-code gap.
- **Status chips are hardcoded to `OK` for every stream row** (`app.js:286`). The spec declares the `WARN` and `ERROR` variants in `§8.8` and `app.css:480-482` implements them, but there is no logic deciding which chip to render. Scoring `OK` unconditionally is more misleading than hiding the chip. Not blocking for Phase 10 (there's no per-stream health signal yet), but the chip is a visual component that currently carries zero information.

---

### 3. Color — 4/4

**Strengths:**

- `app.css:5-66` is a literal copy of `§13.1`. Every token — spacing, typography, surfaces, accents, status, text, borders, charts, motion, fonts — is identical to the spec block, including the comment `/* NOTE: the 12px slot is intentionally omitted from the spacing scale */`. Cross-checking all 28 color tokens via grep: zero deviation from the spec values.
- **`--accent-warm` (`#ff8a4a`) is exhaustively restricted** to the three sites `§4.2` enumerates:
  1. `app.css:135` — `.wordmark-tick` color (site 1: "Tally wordmark tally-mark")
  2. `app.css:189-190` — `.poll-control[data-paused="true"] .poll-dot` and `.poll-label` (site 3: "pause indicator dot when polling is paused")
  3. `favicon.svg:3` — stroke color `#ff8a4a` (site 2: "favicon")
  - Zero appearances on buttons, links, or data. No decorative use anywhere.
- **`--accent-primary` (`#4a9eff`) is used only on** the reserved affordances in `§4.2`: active tab underline + label + background tint (`app.css:164-169`), primary button background (`app.css:277-282`), focused input border + glow (`app.css:315-319`), DAG node stroke (`app.css:381` via `--chart-stream` alias), DAG node selected glow (`app.css:391`), and focus ring (`app.css:105`). Every site has a spec justification.
- **`--chart-view` (`#a371f7`, muted purple)** is used on view nodes in the DAG (`app.css:382`), view node glow (`app.css:397`), and memory-bar fill for view rows (`app.css:613`). Three sites, all in the `§4.6 Chart / DAG palette` table.
- **Status chips** (`app.css:480-482`) use the exact `rgba(63, 185, 80, 0.15)` / `rgba(210, 153, 34, 0.15)` / `rgba(248, 81, 73, 0.15)` tints from `§7.2`. Text color pulls from the three `--status-*` tokens.
- **Hardcoded hex outside tokens:** only `#242b3a` (shimmer mid-stop, `app.css:622`, spec-declared in `§9.1`) and `#252b3a` (`.btn-secondary:hover` bg, `app.css:289`, spec-declared in `§7.8` table). Both are the literal values the spec specifies inline. Zero other raw hex in the file.
- **Contrast** is inherited directly from the spec's pre-audited ratios at `§4.4`: `--text-primary` on `--bg-panel` ≈ 12:1 (body copy, headings), `--text-secondary` on `--bg-panel` ≈ 5.4:1 (labels, subtitles, inactive tabs). The `--text-tertiary` at 3.8:1 is used only on captions / timestamps / table headers (all 12px captions are WCAG AA non-body per spec), and it is used exactly in those contexts (`app.css:197-201` footer, `app.css:434-437` streams header, `app.css:508-509` entity timestamp, `app.css:536-539` entity source line).

**Opportunities:**

- None. This is the kind of palette discipline that justifies writing a spec in the first place.

---

### 4. Typography — 4/4

**Strengths:**

- **Exactly 4 declared font-size tokens** (`--text-xs: 12px`, `--text-sm: 14px`, `--text-xl: 20px`, `--text-2xl: 24px`) at `app.css:12-15`. No `--text-base`, no `--text-md`, no 16px slot.
- **Zero 16px `font-size` declarations** anywhere in the file — grep for `font-size:\s*16px` returns empty. The spec is clear that mono and sans share the 14px body slot, and the implementation honors that.
- **Exactly 2 font-weights in use** per grep audit — `400` (the browser default for `<body>` at `app.css:83-84`) and `600` at nine sites: wordmark, wordmark-tick, active tab, card-title, tab-header h1, btn-primary label, chip, empty-state h3, and error-state h3 (all `§3.3`-approved "title / active / button" slots). No 500, no 700, no `bold` keyword, no `italic`.
- **Font stacks** at `app.css:62-66` are the exact system stacks from `§3.1`. No web fonts, no `@font-face`, no network fetches.
- **Monospace is used for exactly the right slots** per `§3.4` — wordmark (`app.css:125`), DAG node labels (`app.css:383-387`), stream name / metrics cells (`app.css:452-463`), entity key chip (`app.css:498-504`), entity feature values (`app.css:528-534`), memory row name / count / size (`app.css:578-595`), input text (`app.css:305`). Every sans/mono split matches the spec table.
- **Table-header uppercase tracking** at `app.css:434-437` applies `text-transform: uppercase; letter-spacing: 0.05em;` at 12px `--text-tertiary` — exactly the `§3.4` Table-header row.
- **11px usage is spec-allowed**, not deviation. Grep surfaces exactly two 11px declarations: `app.css:474` (`.chip` — `§7.2` explicitly specifies "11px uppercase") and `app.css:537` (`.entity-feature-cell .source` — `§7.3` explicitly specifies "11px `--text-tertiary`"). Both are one-off deviations the spec itself bakes in.

**Opportunities:**

- None that don't require editing the spec itself. Worth noting for future-phase awareness: the 11px exceptions are technically the tip of a fifth type size, so if Phase 10.2 adds another 11px site the type scale quietly grows from "4 sizes + 2 exceptions" to "5 sizes".

---

### 5. Spacing — 4/4

**Strengths:**

- **Exactly 6 spacing tokens** defined at `app.css:7-8` — `--space-1: 4px`, `--space-2: 8px`, `--space-4: 16px`, `--space-5: 24px`, `--space-6: 32px`, `--space-8: 48px`. The `--space-3: 12px` slot is explicitly absent, matching `§2.1`. Zero 12px spacing declarations in the file; the only `12px` in `app.css` are `--text-xs` (type, not spacing) and `--radius-lg` (border-radius, which has its own `§5.1` scale).
- **Spacing usage is token-only**, with a short whitelist of justified raw px values:
  - `1px` borders (spec-allowed for `§5.2` border widths)
  - `2px` (`app.css:136` wordmark-tick margin, `app.css:168` active-tab bottom border, `app.css:390, 396` DAG node hover stroke-width) — all spec-declared raw values, not spacing-scale violations
  - `8px`/`32px`/`40px`/`48px`/`36px` height declarations match `§6 Layout` and `§7` component dimensions exactly
  - `11px` chip font-size (not spacing)
  - `1.5px` stroke-width on DAG nodes and edges (spec `§7.1`)
- **Header / footer heights match `§6.3` and `§6.4`:** 48px sticky header (`app.css:115`), 32px footer (`app.css:193`).
- **Card padding** is uniformly `var(--space-5)` (24px) at `app.css:241` — the `§6.6` card primitive exactly.
- **Tab content padding** is `var(--space-5)` (24px) at `app.css:207` — matches `§6.2` "24px gutters".
- **Tab button padding** is `var(--space-2) var(--space-4)` (8px × 16px) at `app.css:148` — matches `§2.2` tab-button row.
- **Stream row padding** is `var(--space-2) var(--space-4)` with `gap: var(--space-4)` and `min-height: 40px` (`app.css:427-431`) — matches `§2.2` + `§7.2` exactly, satisfying the 36px touch-target floor.
- **Memory row** uses `padding: var(--space-2) 0` with `gap: var(--space-4)` and `min-height: 32px` (`app.css:572-576`) — matches `§7.4`.
- **Card-to-card spacing:** `.tab-body` uses `display: grid; gap: var(--space-5);` (`app.css:230`) and an additional `.card + .card { margin-top: var(--space-5); }` fallback at `app.css:244`. Both equal 24px, which matches `§6.5` "display: grid; gap: var(--space-5);".
- **No arbitrary `[12px]` / `[36.5px]` / etc.** spacing values, no magic numbers in margin/padding. 100% of spacing flows through the six declared tokens.

**Opportunities:**

- Extremely minor nit: `app.css:244` declares `.card + .card { margin-top: var(--space-5); }` which is redundant with the `.tab-body` grid gap and only fires if a caller forgets to wrap cards in `.tab-body`. In practice all four tab panels use `.tab-body` so this rule is dead code. Not a spec violation; just a maintenance opportunity.

---

### 6. Experience Design — 2/4

**Strengths:**

- **Pause toggle semantics are correct.** `app.js:108-111` sets `data-hx-disable="true"` on every `[hx-trigger*="every"]` container (streams list and memory bars) when paused, and removes it on resume. `app.js:451-457` wires a global `Space` keyboard shortcut that toggles pause when no input is focused — matches `§8.10` "Space toggles pause/resume ... and also when pressed globally while no input is focused". The aria-live region (`#poll-status`) is updated on every toggle with the spec's exact strings ("Polling paused" / "Polling resumed").
- **Focus ring is visible globally.** `app.css:105` declares `:focus-visible { outline: 2px solid var(--accent-primary); outline-offset: 2px; }`. No `outline: none` override anywhere.
- **Reduced-motion is respected.** `app.css:68-71` sets `--motion-fast` and `--motion-med` to `0ms` under `@media (prefers-reduced-motion: reduce)` AND applies `* { animation: none !important; transition-duration: 0ms !important; }`. Shimmer, hover transitions, and pause-dot fade all collapse to instant.
- **XSS defense is spotless.** Every DOM write for server/user strings flows through `textContent` via the `el({text: ...})` helper (`app.js:20-33`). `d3.text()` is used via dagre-d3's default `labelType: 'text'`. The `app_js_has_no_innerhtml_or_eval_sinks` regression test locks this (`10-VERIFICATION.md` line 153).
- **1 Hz polling cadence** is declared on the Streams tab (`index.html:61` — `hx-trigger="load, every 1s"`). Memory tab polls at 2 Hz (`index.html:104` — `every 2s`), a justifiable relaxation since memory numbers change much more slowly than throughput EWMAs. Topology polls manually on tab activation (`app.js:86`) — also reasonable since topology is a structural view.
- **Error recovery with retry button** is wired on the topology error path (`app.js:228-237`) — the button is bound to `renderTopology` directly and re-fires the fetch. The error-state `§9.4` layout and copy (heading + verbatim server body + Retry button) match the spec.
- **tab-list ARIA is wired**: `role="tablist"` on the nav (`index.html:18`), `role="tab"` + `aria-selected` + `aria-controls` on each tab link, and `role="tabpanel"` + `aria-labelledby` on each section. `activateTab()` in `app.js:75-88` keeps `aria-selected` in sync when tabs switch.

**Opportunities:**

- **Tab bar `←` / `→` keyboard navigation is missing.** `§8.2` mandates "Keyboard: `←` / `→` moves focus along the tabs, `Enter` activates" and `§10 Accessibility` re-lists this as part of the keyboard contract. The implementation has a click handler (`app.js:441-446`) that calls `activateTab` on mouseclick, but no `keydown` handler for arrow keys on the tabs. A screen-reader user or keyboard-only user arriving on `#tab-topology` cannot move focus along the tab bar without `Tab`-ing through every interactive element inside the panel first. This is the most directly-spec-mandated accessibility gap in the phase.
- **Node keyboard focus / Enter / Space / Esc is missing.** `§7.1` mandates "Keyboard: `Tab` focuses nodes in declaration order; `Enter`/`Space` toggles selection; `Esc` deselects" and `§10` reinforces it. The DAG nodes as rendered by dagre-d3 are SVG `<g>` elements with no `tabindex`, no `role="button"`, and no keydown handlers — they are effectively invisible to keyboard users and screen readers. A screen reader will announce "graphic" when landing on the SVG and have no way to interact with individual nodes. (This is the route-to-Phase-10.2 consequence: node click and detail panel are both scoped to 10.2, so the keyboard contract for nodes goes with them, but the spec-as-written still counts the gap.)
- **Node-detail panel `§7.6` is not implemented at all.** The DOM skeleton at `§7.1` shows an `<aside id="topology-detail" class="topology-detail" hidden>` inside the topology card, but `index.html:44-49` only has `.topology-canvas` with the SVG — no detail aside. `.topology-detail` CSS exists at `app.css:367-373` but the element is never created. A click on a node in the current implementation does nothing visual; `app.js` has no click handler on node elements. This is the single largest spec-to-code gap in the phase and is the primary driver of the score drop.
- **Entity tab form wiring (WR-02) is broken end-to-end.** `index.html:75-84` declares `hx-get="/debug/key/"` + `hx-include="#entity-key"`, which htmx serializes to `GET /debug/key/?key=u_demo` — but the router is `/debug/key/{key}`. Every form submission returns 404 and lands in the `renderEntity` "Not found" branch (`app.js:312-321`). A user typing `u_demo` and hitting `Look up` sees `No features for "u_demo"`, which is indistinguishable from a real "key doesn't exist" result. (This is already tracked as WR-02 and explicitly routed to Phase 10.2; see Deferred section below. Scored here as evidence of the pillar gap, not double-counted as a new finding.)
- **Streams row `role="button" tabindex="0"` but no `keydown` handler.** `app.js:287` adds a button role and tabindex to each stream row, so a keyboard user can tab into the row and see a focus ring. But pressing `Enter` or `Space` on a focused row does nothing (the spec `§7.2` says "clicking a row navigates to the Entity tab... v1 MAY simply log the click and do nothing beyond hover" — so this is spec-permitted, but still a quiet interaction mismatch for keyboard users who now have focusable buttons that don't respond to keys).
- **Entity search `Esc`-clears behavior** per `§10` ("Entity search: `Enter` submits the form; `Esc` clears it if it has text") is not implemented. `Enter` submits via the form's default behavior; `Esc` does nothing. Low-impact, but listed in the accessibility baseline.
- **Loading state is a ≤1s blank flash** instead of the `§9.1` shimmer skeleton. See Visuals §Opportunities — the dead `.skeleton` class is technical debt that also counts against Experience here because the spec intentionally chose skeletons over spinners for "polling feel" reasons and the implementation delivers neither.

**Why 2/4 and not 3/4:** The baseline keyboard contract from `§10` (tab bar arrows, node Tab/Enter/Esc, entity Esc-clear) is missing across three enumerated widgets, the node-detail panel (`§7.6`) is an entire unshipped component, and the primary Entity form submission path is a 404. The pillar passes the reduced-motion and focus-ring checks cleanly, but the spec's accessibility commitments are not met in full.

---

## Top Fixes (Prioritized)

1. **[Experience Design] Wire tab-bar `←`/`→` keyboard navigation** — `§8.2` + `§10` commitment. Add a `keydown` listener on `.tabs` that moves focus between `.tab` anchors and calls `activateTab(target.dataset.tab)` on `Enter`. ~15 lines in `app.js` near the existing tab click handler at `app.js:441-446`. Biggest accessibility impact per line of code, unblocks keyboard-only users on the primary navigation surface.

2. **[Visuals] Ship the `§9.1` skeleton shimmer for at least Streams and Memory on first paint** — The `.skeleton` CSS and `@keyframes shimmer` are already defined (`app.css:621-631`) but never used. Inject 6 shimmer rows into `#streams-list` and 4 rows into `#memory-bars` during `DOMContentLoaded` so the first htmx response has something to swap in over. Turns the current ≤1s blank flash into the "feels instant" polling feel the spec reaches for. ~20 lines added in `app.js`.

3. **[Visuals] Fix the DAG empty-state layout so the focal 640px canvas stays visible** — `app.js:212` currently sets `canvas.style.display = 'none'` when the topology is empty, collapsing the tab's focal point. Render the `empty-state` block as an overlay (absolute position centered) or inside the SVG as `<foreignObject>` so the card keeps its declared 70vh/640px footprint. Alternatively, render a single greyed "nothing to graph" placeholder node inside dagre-d3 as the spec-authorized "Computing topology…" loading variant in `§7.1 States: Loading`. Either keeps the visual weight the spec assumes.

**Also worth doing (not scored into the top 3):**

- Hook a real status signal into the per-stream `OK` / `WARN` / `ERROR` chip at `app.js:286` so the chip carries information instead of always rendering `OK`. Until there's a health signal, consider hiding the chip entirely rather than lying.
- Add `app.css` cleanup: remove the dead `.card + .card { margin-top: var(--space-5); }` rule since `.tab-body` grid gap already handles it.
- Add `Esc` clear handler on `#entity-key` per `§10` ("Entity search: `Enter` submits the form; `Esc` clears it if it has text") — 5 lines.

---

## Acknowledgments

Patterns this implementation got right and should be cited in future phases:

- **Token discipline is load-bearing.** `app.css:5-66` being a literal copy of `§13.1` — comment and all — is the mechanism that made Color, Typography, and Spacing score 4/4/4 without any judgment calls. Future UI phases should do the same: declare tokens in UI-SPEC §13.1, copy them verbatim into `app.css`, audit via grep.
- **textContent-only DOM writes everywhere.** `app.js:20-33` defines an `el({tag, attrs, children})` helper whose `text` attr always flows through `.textContent`. Every server/user string (stream names, entity keys, error messages, feature values) goes through this helper or d3's `.text()`. Combined with the `app_js_has_no_innerhtml_or_eval_sinks` regression grep, this makes XSS unreachable at the implementation layer rather than relying on downstream escaping discipline.
- **Exhaustive reserved-for-list enforcement on accent colors.** `§4.2` declares `--accent-warm` with a numbered three-site exhaustive list; the implementation uses it at exactly those three sites and nowhere else. This is the single cheapest discipline trick in the spec and the one most likely to decay in Phase 10.2, so call it out now.
- **Motion is functional, not decorative.** Two motion tokens (`--motion-fast`, `--motion-med`), both short, both used on hover/focus/state-change transitions only. Zero parallax, zero shimmer except for the declared loading state, zero page-load entrance animations. `prefers-reduced-motion` collapses both tokens to `0ms` AND globally nukes animations/transitions via `* { animation: none !important; transition-duration: 0ms !important; }`. This is the right shape for a developer tool and matches the "data MUST feel instant" stance in `§5.4`.
- **Layered defense-in-depth.** Path-traversal rejection at `src/server/ui.rs` on top of rust-embed's compile-time scoping; SHA256 drift tests on vendored JS on top of commit-time vendoring; `textContent`-only DOM writes on top of dagre-d3's default text label; `dt <= 0.0` guard on top of `HashSet<&str>` dedup. Every load-bearing invariant has at least two independent mechanisms enforcing it.
- **Spec-to-code traceability is cheap.** Every finding in this audit points to a concrete file:line and a specific `§N.M` subsection of UI-SPEC.md. That's only possible because both the spec and the implementation file the tokens and copy in the same shape (numbered sections, declarative tables, verbatim strings). Do the same for Phase 10.2's UI-SPEC.

---

## Deferred to Phase 10.2

The following gaps are tracked here but intentionally not patched in Phase 10 per the user's routing decision (see `10-04-SUMMARY.md` Decisions Made and `10-VERIFICATION.md` Known Gaps):

- **WR-02: Entity tab form `hx-get="/debug/key/"` + `hx-include` serializes to `/debug/key/?key=u_demo`** → 404 vs. router path `/debug/key/{key}`. Backend endpoint is verified correct and exercised by `tests/test_debug_ui.rs::entity_lookup_reuses_existing_endpoint`. Phase 10.2 replaces the flat Entity tab with a node-click drill-in from the Topology DAG, so the form is discarded rather than patched.
- **`§7.6` Node detail panel** (`<aside id="topology-detail">` inside topology card, 320px fixed width, feature list, Connections section, close button, Esc key). Entirely unshipped. Phase 10.2 redesigns the DAG drill-in from scratch and owns this component.
- **DAG node keyboard interaction** (`Tab` focus, `Enter`/`Space` select, `Esc` deselect) per `§7.1` and `§10`. Tied to the node-detail panel — Phase 10.2 owns the full interaction surface.
- **`§8.2` tab bar arrow-key navigation** and **`§10` Entity `Esc` clear** are NOT in the Phase 10.2 scope as currently described, so they should be addressed separately — either as follow-up fixes in Phase 10 if the user reverses the routing, or as explicit tasks queued before Phase 10.2's own UI-SPEC lands. Flagged here so they don't silently orphan.

---

## Files Audited

**Source of truth:**
- `/Users/petrpan26/work/tally/.planning/phases/10-debug-ui/10-UI-SPEC.md` (990 lines, §13.1 tokens as verbatim baseline)
- `/Users/petrpan26/work/tally/.planning/phases/10-debug-ui/10-CONTEXT.md` (locked decisions)

**Implementation (Phase 10, Plan 04):**
- `/Users/petrpan26/work/tally/src/server/ui/index.html` (116 lines)
- `/Users/petrpan26/work/tally/src/server/ui/app.css` (668 lines)
- `/Users/petrpan26/work/tally/src/server/ui/app.js` (474 lines)
- `/Users/petrpan26/work/tally/src/server/ui/icons.svg` (50 lines)
- `/Users/petrpan26/work/tally/src/server/ui/favicon.svg` (10 lines)

**Supporting Phase 10 artifacts (context only, not audited for pillar scoring):**
- `10-01-SUMMARY.md` through `10-05-SUMMARY.md` (plan-execution records)
- `10-REVIEW.md` (code review — WR-01 EWMA calibration fixed, WR-02 entity form deferred to 10.2, WR-03 XSS regression test added)
- `10-VERIFICATION.md` (4/5 SC verified, SC-3 partial pending user routing confirmation, manual smoke test passed)

**Screenshots:** not captured. Dev server on `:6501` is alive and responsive but has an empty state (no registered pipelines, no pushed events), so screenshots would only evidence the empty-state rendering — low signal relative to the deep spec-driven code audit above. `playwright` CLI is available via `npx` but chromium headless is not installed and provisioning it would be disproportionate to the audit's value. The previously-completed manual browser smoke test (`10-VERIFICATION.md` §Behavioral Spot-Checks) covered the visual rendering with real data.
