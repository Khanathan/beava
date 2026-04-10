---
phase: 10
name: Debug UI
gathered: 2026-04-10
status: ready_for_planning
mode: smart_discuss
---

# Phase 10: Debug UI - Context

**Gathered:** 2026-04-10
**Status:** Ready for planning

<domain>
## Phase Boundary

Deliver an embedded web UI, served from the existing HTTP management port (6401), that lets a developer observe and debug a running Tally instance. The UI surfaces four views built on data the engine already holds: stream topology DAG (from petgraph), live per-stream throughput, entity key inspection (current feature values across all streams), and memory usage breakdown. All UI assets ship inside the single Tally binary with no separate process, no npm build, and no external files at runtime.

Out of scope for this phase: authentication/authz on the UI (the HTTP management port is assumed bound to localhost or a trusted network, matching existing /metrics and /debug/* conventions), historical time-series storage, alerting, writing to state (UI is strictly read-only), Prometheus dashboard embedding, and any design-system extraction for reuse outside the debug UI.

</domain>

<decisions>
## Implementation Decisions

### Frontend Tech Stack
- **Framework:** htmx + vanilla JavaScript. Zero build step, ~14KB for htmx, plays well with `rust-embed`. All reactivity via `hx-get` + polling.
- **DAG rendering:** Vendored `dagre-d3` (layout) rendered into inline SVG. Proven, stable, single JS file, no build.
- **Styling:** Single handwritten CSS file (`app.css`), dark-by-default. No Tailwind, no CSS framework, no preprocessor.
- **Build pipeline:** None. Commit pre-vendored `htmx.min.js`, `dagre-d3.min.js` (and any required transitive files), `app.css`, and `index.html` into `src/server/ui/` and embed via `rust-embed` at compile time. No runtime network fetches, no post-install hooks.

### Backend API Surface
- **Topology endpoint:** New `GET /debug/topology` returning `{nodes: [{name, key_field, features, depends_on}], edges: [{from, to}]}` derived directly from the engine's pipeline registry / petgraph DAG.
- **Throughput endpoint:** New `GET /debug/throughput` returning per-stream EWMA rates over 5s, 1m, 5m windows. Lightweight counters maintained in memory; no persistence. Counter updated on every successful push; read side is O(1) per stream.
- **Real-time updates:** Client-side polling with `hx-get` + `hx-trigger="every 1s"` (configurable via header pause button). No SSE, no WebSocket — keeps server surface identical to existing HTTP endpoints.
- **Entity inspection:** Reuse the existing `GET /debug/key/{key}` endpoint. No new contract.

### UI Layout & Navigation
- **Structure:** Single HTML page (`index.html`) with JavaScript-driven tab switching. No client-side router, no hash routes.
- **Tabs:** Four tabs in order:
  1. **Topology** — DAG visualization, click a node to jump to its stream detail
  2. **Streams** — list view showing per-stream throughput (msgs/sec 5s/1m/5m) with simple sparklines, plus stream metadata
  3. **Entity** — key lookup input box, displays all streams' current feature values for the entered key
  4. **Memory** — per-stream memory breakdown bar chart + total memory, + key counts
- **Entry route:** `GET /` on port 6401 serves `index.html`. Static assets under `/static/*`. Existing `/debug/*` JSON endpoints are untouched and remain the API surface.
- **Refresh UX:** Auto-refresh every 1000ms. Header pause button halts all polling. "Last updated" timestamp visible. Per-panel manual refresh button.

### Visual Style
- **Theme:** Dark-by-default developer tool theme. Background `#0e1116` (near-black), panel `#161a22`, borders `#2a313c`, text `#e6edf3` primary / `#8b949e` secondary. No light mode in v1.
- **Accent palette:** Primary electric blue `#4a9eff` (nodes, links, active tab), accent orange `#ff8a4a` (edges, highlights, emphasis), matching the Tally mascot logo. Status ramp: green `#3fb950` (healthy), yellow `#d29922` (warning), red `#f85149` (hot/error).
- **Typography:** System font stack for UI chrome (`-apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif`). Monospace font stack for metrics, keys, and counters (`ui-monospace, SFMono-Regular, "SF Mono", Consolas, "Liberation Mono", Menlo, monospace`). No web fonts.
- **Branding:** Minimal "tally" wordmark in header (left-aligned, monospace, lowercase). Simple tally-mark SVG favicon. No mascot imagery anywhere in the UI body.

### Claude's Discretion
- Exact CSS selector naming, HTML semantic structure, error-state copy, empty-state illustrations (if any — text is fine), sparkline rendering technique (inline SVG vs canvas), icon choices (none vs minimal SVG), dagre-d3 node styling specifics, responsive breakpoints for narrow viewports, keyboard shortcuts (if any), and exact API payload field names for `/debug/topology` and `/debug/throughput` — all at Claude's discretion provided they satisfy the decisions above and the success criteria.

</decisions>

<code_context>
## Existing Code Insights

### Reusable Assets
- **HTTP router (`src/server/http.rs`)** — axum Router with existing `/debug/key/{key}`, `/debug/memory`, `/debug/backfill`, `/metrics`, `/pipelines` endpoints. New routes slot into `build_router()`.
- **Engine pipeline registry (`src/engine/pipeline.rs`)** — `Engine::list_streams()`, `Engine::get_stream(name)`, petgraph DAG from Phase 7 — provides topology data directly.
- **Per-stream memory tracking (Phase 6)** — already exposed via `/debug/memory`. `/debug/memory` returns structured per-stream data that the UI's Memory tab can consume without extension.
- **Key lookup endpoint (`/debug/key/{key}`)** — Phase 6 restructured state for per-stream isolation; existing endpoint already returns features grouped by stream. UI Entity tab calls this directly.

### Established Patterns
- **HTTP responses** — `Json(serde_json::Value)` pattern throughout `http.rs`. New debug endpoints follow the same shape.
- **Shared state** — `SharedState = Arc<Mutex<AppState>>`. Handlers lock briefly, read, unlock. Keep the pattern — no long-held locks for UI requests.
- **Single binary** — `rust-embed` is already the decided approach from Phase 9 research notes. Add `rust-embed` as a new crate dependency; feature-gate is not required.
- **Testing** — Integration tests use `run_http_server_with_listener` against a pre-bound TCP listener; follow the same pattern for topology/throughput tests.

### Integration Points
- New static asset serving: add a handler `GET /` that returns `index.html` and `GET /static/{file}` that serves from the embedded asset directory.
- New JSON endpoints (`/debug/topology`, `/debug/throughput`) wired into `build_router()`.
- Throughput counters: small struct inside `AppState` or `Engine`, updated on successful PUSH. Atomic counters per stream, read lock-free where possible.
- `Cargo.toml` — add `rust-embed` dependency.
- `src/server/ui/` — new directory holding `index.html`, `app.css`, `app.js`, `htmx.min.js`, `dagre-d3.min.js`, `favicon.svg`. Embedded at compile time.

</code_context>

<specifics>
## Specific Ideas

- The dark theme aligns with developer tool conventions (Grafana, Datadog dashboards, Chrome DevTools).
- The blue+orange palette ties the UI to the existing Tally logo work (LOGO_PROMPT.md variants).
- Polling at 1Hz is plenty for a debug UI on a single-node server and avoids any streaming transport complexity.
- Vendoring JS rather than CDN links is required by DBUI-05 (no runtime fetch of external files) and the "zero ops" promise.
- The UI must degrade gracefully when an endpoint returns empty/missing (e.g., no streams registered yet, no events pushed yet) — show empty-state copy, not an error.

</specifics>

<deferred>
## Deferred Ideas

- Authentication/authorization on the HTTP management port (v1.2+ operational hardening)
- Historical time-series storage and replay
- Alerting / thresholds
- Light mode / theme switching
- Grafana or Prometheus dashboard embedding
- Writable controls (pause pipelines, reset operators, drop key state) — UI is strictly read-only in v1.1
- Multi-node cluster view (Tally is single-node in v1.1)
- SSE or WebSocket transport (revisit if 1Hz polling proves insufficient)
- Mobile / narrow-viewport optimization beyond basic responsiveness

</deferred>
