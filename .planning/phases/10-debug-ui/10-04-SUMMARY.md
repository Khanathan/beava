---
phase: 10-debug-ui
plan: 04
subsystem: ui
tags: [htmx, d3, dagre-d3, dark-mode, xss-safe, rust-embed]

requires:
  - phase: 10
    provides: Plan 10-01 vendored htmx/d3/dagre-d3 + Plan 10-03 UiAssets + /debug/topology + /debug/throughput + /debug/memory handlers
provides:
  - Dark-mode debug UI page shell (index.html) with four-tab layout
  - Handwritten app.css matching UI-SPEC §13.1 verbatim (6 spacing values, 4 type sizes, restricted dark palette)
  - app.js with tab activation, pause toggle, dagre-d3 DAG render, streams/entity/memory renderers, XSS-safe textContent-only DOM writes
  - icons.svg symbol library (tally-mark, alert-triangle, search, chevron-right, close, pause, play, copy, link)
  - favicon.svg warm-orange tally-mark
affects: [10-05]

tech-stack:
  added: []
  patterns:
    - "textContent-only DOM writes for all user-supplied strings (XSS defense)"
    - "data-hx-disable toggle as the global polling pause switch"
    - "htmx afterRequest hook reading event.detail.xhr.responseText so JSON never touches innerHTML"
    - "d3 .text() for every label node (never .html())"

key-files:
  created:
    - src/server/ui/index.html
    - src/server/ui/app.css
    - src/server/ui/app.js
    - src/server/ui/icons.svg
    - src/server/ui/favicon.svg
  modified: []

key-decisions:
  - "Four-tab flat layout shipped as-is per UI-SPEC §13.1 contract; interactive DAG drill-in + edge throughput labels routed to Phase 10.2 as a separate insertion (same pattern as Phase 10.1 latency debugger)"
  - "data-hx-disable attribute toggled on every [hx-trigger*=every] container is the pause mechanism; Space key toggles pause globally when no input is focused"
  - "Entity renderer accepts both computed_features and features keys on the /debug/key/:key response for forward-compat"
  - "Footer version shows v— when /health does not expose a version field; degrades gracefully instead of showing undefined"

patterns-established:
  - "Vendor script order in index.html: d3 → dagre-d3 → htmx → app.js. dagre-d3 inlines graphlib so no separate include is needed"
  - "Streams tab uses htmx hx-trigger=every 1s for polling cadence; Memory tab polls /debug/memory at 2 Hz; Topology re-renders on pause/resume edge"
  - "The paste-XSS smoke test (paste <script>alert(1)</script> as the entity key) is the enforcement mechanism for the no-innerHTML rule — not a lint"

requirements-completed:
  - DBUI-01
  - DBUI-02
  - DBUI-03
  - DBUI-04

duration: ~11min
completed: 2026-04-10
---

# Phase 10 Plan 04: Debug UI Frontend Assets

**Five static files (index.html, app.css, app.js, icons.svg, favicon.svg) ship a dark-mode debug UI with four tabs, 1 Hz htmx polling, XSS-safe textContent rendering, and a dagre-d3 DAG — all embedded in the single tally binary via rust-embed.**

## Performance

- **Duration:** ~11 min (Task 1-3 automated; Task 4 human-verify routed through Option A scope-addition decision)
- **Started:** 2026-04-10T12:55:00Z
- **Completed:** 2026-04-10T13:06:00Z (plus Option A scope routing)
- **Tasks:** 4 (3 auto + 1 human-verify routed)
- **Files created:** 5
- **Files modified:** 0

## Accomplishments

- **Page shell (index.html, 116 lines)** with sticky 48px header containing the `tally'` wordmark, four-tab bar (Topology / Streams / Entity / Memory, Topology active by default), "Live · 1 Hz" status dot, and Pause button; footer with version + connection + last-update line.
- **Handwritten CSS (app.css, 668 lines)** matching UI-SPEC §13.1 verbatim: exactly 6 spacing tokens (`--space-1` through `--space-6`, no 12px), exactly 4 type sizes (12 / 14 / 20 / 24, no 16px body blend), dark-mode-only palette with restricted accent colors (`--accent-blue` for streams, `--accent-purple` for views, `--accent-warm` for orange apostrophe + paused state).
- **Vanilla JS (app.js, 474 lines)** with tab activation, pause/resume toggle, dagre-d3 DAG render shim over `/debug/topology`, streams list renderer polling `/debug/throughput`, entity result renderer, memory bar renderer sorting by `estimated_bytes` descending.
- **Zero `.innerHTML` writes for user strings.** Every DOM write for entity keys, feature values, stream names, and error messages uses `.textContent` or d3 `.text()`. Grep audit confirmed by the executor: zero `innerHTML`, `outerHTML`, `document.write`, `eval(`, or `labelType.*html` in app.js.
- **Token audit passed.** `--space-3: 12px` absent, `font-size: 16px` absent. Spacing values are exactly 4 / 8 / 16 / 24 / 32 / 48.
- **Icon symbol library (icons.svg, 50 lines)** with the nine symbols the UI references (tally-mark, alert-triangle, search, chevron-right, close, pause, play, copy, link).
- **Favicon (favicon.svg, 10 lines)** with the warm-orange tally-mark pattern.

## Task Commits

1. **Task 1: Create index.html, icons.svg, and favicon.svg** — `856e353` (feat)
2. **Task 2: Create app.css with every design token and component style** — `172be5e` (feat)
3. **Task 3: Create app.js with tab activation, pause toggle, DAG render, streams/entity/memory renderers** — `8bdcf4d` (feat)
4. **Task 4: Browser smoke test** — routed via Option A (see below); no code changes required; SUMMARY.md commit closes the plan.

## Files Created

- `src/server/ui/index.html` — Page shell (116 lines)
- `src/server/ui/app.css` — Tokens + components (668 lines)
- `src/server/ui/app.js` — Renderers + pause toggle + DAG shim (474 lines)
- `src/server/ui/icons.svg` — Symbol library (50 lines)
- `src/server/ui/favicon.svg` — Warm-orange favicon (10 lines)

Total: 1318 lines across 5 files.

## Automated Verification (executed during Tasks 1-3)

- `curl http://localhost:6501/` → 200, `text/html`, 4764 bytes
- `curl http://localhost:6501/static/app.css` → 200, `text/css`, 15122 bytes
- `curl http://localhost:6501/static/app.js` → 200, `text/javascript`, 17049 bytes
- `curl http://localhost:6501/static/icons.svg` → 200, `image/svg+xml`, 2459 bytes
- `curl http://localhost:6501/static/favicon.svg` → 200, `image/svg+xml`, 404 bytes
- `curl http://localhost:6501/static/vendor/htmx.min.js` → 200, 48101 bytes (matches VENDOR.md SHA256)
- `curl http://localhost:6501/static/vendor/d3.min.js` → 200, 279633 bytes (matches VENDOR.md SHA256)
- `curl http://localhost:6501/static/vendor/dagre-d3.min.js` → 200, 725181 bytes (matches VENDOR.md SHA256)
- `curl http://localhost:6501/debug/topology` → `{"edges":[],"nodes":[],"topo_order":[]}`
- `curl http://localhost:6501/debug/throughput` → `{"streams":[]}`
- `curl http://localhost:6501/debug/memory` → `{"entity_count":0,"estimated_bytes":0,"per_stream":[],"stream_count":0}`
- `curl http://localhost:6501/debug/key/nothere` → 404 `{"error":"key 'nothere' not found"}`
- Grep audit on app.js: zero `innerHTML`, `outerHTML`, `document.write`, `eval(`, `labelType.*html`
- Grep audit on app.css: zero `--space-3`, zero `font-size: 16px`, zero bare `12px` spacing
- `cargo check --lib --bin tally` → clean, zero warnings
- `cargo test --lib` → 461/461 passing, zero regressions

## Decisions Made

- **Four-tab flat layout shipped as-is.** The UI-SPEC §13.1 contract locks the flat tab layout, tokens, and polling cadence. The plan-checker PASSED the 5-plan phase with this layout, and all hard must-haves are verified (textContent only, token counts correct, 1 Hz htmx polling, pause toggle, XSS defense at the DOM-write layer).
- **Interactive DAG drill-in + edge throughput labels routed to Phase 10.2.** After the automated verification steps completed and the smoke-test server was live on 6501, user proposed a redesigned UI where the topology is primary, nodes are clickable to drill into per-stream state + memory + entity queries, and edges carry live throughput numbers. Rather than scope-creep Plan 10-04 mid-execution (which would invalidate the plan-checker PASS), the scope addition routes to a new decimal phase 10.2 — same pattern as Phase 10.1 (latency debugger). Phase 10.2 will get its own CONTEXT / UI-SPEC / research / plans so drill-in semantics are specified before code lands.
- **Port 6501 used for smoke-test server.** Port 6401 has a pre-existing 15-hour-old release build (PID 31352) that was not touched. The smoke-test build ran on 6501 via `TALLY_HTTP_PORT=6501` env override; both ports now coexist.

## Deviations from Plan

**Task 4 routing deviation.** The plan's Task 4 is a human-verify smoke test in a real browser. The user opted to route the interactive-UI redesign to Phase 10.2 (Option A) after reviewing the automated verification output. The code under this plan ships as-is with automated verification covering every technical must-have (200 responses on all asset paths, zero innerHTML writes, exact token counts, cargo check clean, 461/461 lib tests green). Visual-layer verification (UI-REVIEW) runs advisory after this plan per the autonomous workflow.

## Issues Encountered

None — the executor flagged the port conflict with PID 31352 proactively and used an alternate port instead of requesting a destructive kill.

## User Setup Required

None for this plan. Phase 10.2 will redefine the frontend layout; users will install both the flat-tabs (this plan) and the interactive version (10.2) on the same binary without additional setup.

## Next Phase Readiness

- Plan 10-05 (integration tests) has full raw-TCP coverage of every asset path and every debug endpoint this plan consumes.
- Phase 10.1 (latency debugger) and Phase 10.2 (interactive DAG drill-in) are both queued as separate insertions before the v1.1 milestone lifecycle runs.
- UI-SPEC §13.1 tokens are still the source of truth for any frontend work in 10.1 / 10.2 unless those phases' own UI-SPECs override them.

---
*Phase: 10-debug-ui*
*Completed: 2026-04-10*
