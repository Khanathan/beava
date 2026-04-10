# Phase 10: Debug UI - Research

**Researched:** 2026-04-10
**Domain:** Embedded web UI served from an existing axum 0.8 HTTP server inside a Rust binary, with vendored htmx + dagre-d3 frontend and a pair of new JSON endpoints backed by petgraph introspection and lock-free throughput counters.
**Confidence:** HIGH

## Summary

Phase 10 ships an embedded debug UI for Tally without introducing a second process, a build step, or a network dependency. The backend work is bounded: two new JSON endpoints (`/debug/topology`, `/debug/throughput`), a static asset handler serving files embedded at compile time via `rust-embed`, and a small per-stream EWMA throughput counter plumbed into the existing `push_with_cascade` path in `src/server/tcp.rs`. The UI is a single `index.html` with htmx + a < 100 LOC vanilla JS file that drives tab switching, polling, and dagre-d3 DAG rendering. Every visual decision is already locked in `10-UI-SPEC.md`.

The core risks are all well-trodden: (1) `rust-embed`'s axum integration pattern is documented upstream [CITED: rust-embed 8.11 docs.rs /crate/rust-embed/latest/source/examples/axum.rs], (2) petgraph introspection is trivial because Phase 7 already stores the DAG and `topo_order` on `PipelineEngine`, and (3) throughput counters are a standard ring buffer of per-stream atomics under the existing `Arc<Mutex<AppState>>`. The only research-heavy question is vendoring discipline for the three JS libraries, which the UI-SPEC has already mandated (§11 locks dagre-d3 0.6.x MIT, htmx 1.9.x BSD-2-Clause, d3 v7 ISC, with SHA256 recorded in `VENDOR.md`).

**Primary recommendation:** Add `rust-embed = "8.11"` with the `mime-guess` feature, embed `src/server/ui/` at compile time, register `GET /` and `GET /static/{*file}` on the existing `build_router`, add a lock-free `ThroughputTracker` field to `AppState` updated inside the Push arm of `handle_sync_command`, read petgraph topology directly from `PipelineEngine` in a new `/debug/topology` handler, and let the frontend poll at 1 Hz per the locked CONTEXT decision.

---

## User Constraints (from CONTEXT.md)

### Locked Decisions

**Frontend tech stack:**
- htmx + vanilla JS. Zero build step. `~14KB` htmx. Plays with `rust-embed`. Reactivity via `hx-get` + polling.
- DAG rendering: vendored `dagre-d3` rendered into inline SVG. Single JS file, no build.
- Styling: single handwritten `app.css`, dark-by-default. No Tailwind, no CSS framework, no preprocessor.
- Build pipeline: **none**. Pre-vendor `htmx.min.js`, `dagre-d3.min.js`, d3 bundle, `app.css`, `index.html` into `src/server/ui/` and embed via `rust-embed` at compile time. No runtime network fetches, no post-install hooks.

**Backend API surface:**
- `GET /debug/topology` — NEW endpoint returning `{nodes: [{name, key_field, features, depends_on}], edges: [{from, to}]}` derived from the engine's pipeline registry / petgraph DAG.
- `GET /debug/throughput` — NEW endpoint returning per-stream EWMA rates over 5s / 1m / 5m windows. Counters live in memory, no persistence. Counter updated on every successful push; read side O(1) per stream.
- `GET /debug/memory` — REUSED (Phase 6). No contract change.
- `GET /debug/key/{key}` — REUSED (Phase 6). No contract change.
- Real-time updates: client-side polling with `hx-get` + `hx-trigger="every 1s"`, pausable. **No SSE, no WebSocket.**

**UI layout & navigation:**
- Single HTML page (`index.html`) with JavaScript-driven tab switching. No client-side router, no hash routes.
- Four tabs exactly, in order: **Topology**, **Streams**, **Entity**, **Memory**.
- Entry route: `GET /` on port 6401 serves `index.html`. Static assets under `/static/*`.
- Auto-refresh every 1000 ms; header pause button halts all polling; "last updated" timestamp visible; per-panel manual refresh.

**Visual style (all exact tokens in `10-UI-SPEC.md` §13.1):**
- Dark-only theme. `--bg-app #0e1116`, `--bg-panel #161a22`, text `--text-primary #e6edf3`.
- Accents: `--accent-primary #4a9eff` (blue), `--accent-warm #ff8a4a` (orange, brand-only).
- Status: `--status-ok #3fb950`, `--status-warn #d29922`, `--status-error #f85149`.
- System font stack for UI, monospace for metrics. No web fonts.
- Minimal `tally` wordmark, tally-mark SVG favicon.

**Vendoring (UI-SPEC §11):**
- htmx 1.9.x (BSD-2-Clause), dagre-d3 0.6.x (MIT), d3 v7 (ISC).
- Vendored under `src/server/static/vendor/` (or `src/server/ui/vendor/` — executor's choice, UI-SPEC uses `src/server/static/`).
- SHA256 hashes recorded in `src/server/static/VENDOR.md`.

### Claude's Discretion

- Exact CSS selector naming, HTML semantic structure, error-state copy beyond what §8.7 lists verbatim, empty-state illustrations (text is fine), sparkline rendering technique (inline SVG vs canvas).
- Icon choices (§12 locks the icon list; shape details within the SVG are free).
- dagre-d3 node *styling* specifics above and beyond the visual-language table in UI-SPEC §7.1.
- Responsive breakpoints for narrow viewports (UI-SPEC §6.1 locks min-width 960px).
- Keyboard shortcuts beyond what §10 mandates.
- Exact JSON field names on `/debug/topology` and `/debug/throughput` (CONTEXT allows this latitude).
- File layout under `src/server/ui/` vs `src/server/static/` (both appear in source docs — executor picks one and sticks with it).
- EWMA constants (half-life for 5s/1m/5m) — Claude's discretion.

### Deferred Ideas (OUT OF SCOPE)

- Authentication / authorization on the HTTP management port (v1.2+).
- Historical time-series storage and replay.
- Alerting / thresholds.
- Light mode / theme switching.
- Grafana or Prometheus dashboard embedding.
- Writable controls (pause pipelines, reset operators, drop key state) — UI is strictly **read-only**.
- Multi-node / cluster view.
- SSE or WebSocket transport (revisit only if 1 Hz polling proves insufficient).
- Mobile / narrow-viewport optimization beyond the 960px floor.
- Pan / zoom on the DAG canvas (fit-to-container only in v1).

---

## Phase Requirements

| ID | Description | Research Support |
|----|-------------|------------------|
| DBUI-01 | View stream topology DAG in a web UI served from the existing HTTP port | Petgraph DAG is already built in `PipelineEngine` (Phase 7). `topo_order`, `downstream_map`, `list_streams()`, `list_views()` are public. The new `/debug/topology` handler reads them under the existing `Arc<Mutex<AppState>>` lock and emits nodes + edges; dagre-d3 renders the result. |
| DBUI-02 | See real-time throughput (messages/sec) per stream | New `ThroughputTracker` field on `AppState` with per-stream ring-buffer counters, incremented on the Push branch of `handle_sync_command` in `src/server/tcp.rs` and on every successful cascade/fan-out target push. New `/debug/throughput` returns EWMA over 5s / 1m / 5m. |
| DBUI-03 | Inspect current feature values for any entity key | REUSE existing `GET /debug/key/{key}` endpoint. The UI posts a key via htmx `hx-get="/debug/key/"` and renders the returned `computed_features` map into the metric grid (UI-SPEC §7.3). No new backend. |
| DBUI-04 | See memory usage breakdown per stream + total | REUSE existing `GET /debug/memory` endpoint. **Note:** the current payload (`entity_count`, `stream_count`, `estimated_bytes`) is NOT per-stream — see "Existing Endpoint Gap" below. Either the UI works with the rollup or the handler is extended; extending is the cleaner path. |
| DBUI-05 | Debug UI embedded in the binary (no separate process, no npm build) | `rust-embed = "8.11"` embeds `src/server/ui/` at compile time. Release builds include the bytes; no runtime filesystem reads. Single binary preserved. No npm, no bundler, no post-install hook. |

---

## Project Constraints (from CLAUDE.md)

- **Zero infrastructure / zero ops** promise — no new runtime process, no external dependency, no network fetch at startup. Debug UI MUST be baked into the binary.
- **Single-threaded core** — all state mutation happens under `Arc<Mutex<AppState>>`. No new long-held locks on the hot path. Throughput counter writes MUST be O(1) and cannot introduce contention beyond the existing mutex scope.
- **In-memory everything, periodic snapshots** — throughput counters are ephemeral by design (per CONTEXT decision, no persistence). No snapshot changes needed.
- **Skill routing** — a CLAUDE.md rule instructs the agent to route product ideas, bugs, deploys, QA, etc. to skills. This rule is for interactive assistant mode and does NOT apply to research/planning subagents; the research completes normally.
- **Tests live under `tests/`** — existing pattern: one file per concern (`test_pipeline.rs`, `test_server.rs`, `test_snapshot.rs`, `test_incremental_snapshot.rs`). Phase 10 adds a new test file (e.g. `tests/test_debug_ui.rs`).
- **HTTP management API is a secondary port** (default 6401). UI MUST bind to `/` and `/static/*` on that same listener. TCP hot path (6400) is untouched.

---

## Standard Stack

### Core (Rust backend)

| Library | Version | Purpose | Why Standard |
|---------|---------|---------|--------------|
| `rust-embed` | `8.11` | Compile-time embed of `src/server/ui/` into the binary; runtime accessor by path | De-facto standard Rust embedding crate; has an upstream axum example; supports `mime-guess` feature flag for automatic MIME detection; actively maintained (8.11 is the latest as of 2026-04) [VERIFIED: docs.rs/crate/rust-embed/latest] |
| `mime_guess` | `2.x` (transitive via rust-embed's `mime-guess` feature) | Map embedded file extensions to `Content-Type` | Standard library for MIME guessing in Rust; MIT licensed; pulled in automatically when rust-embed's `mime-guess` feature is enabled [VERIFIED: crates.io/crates/mime_guess] |
| `axum` | `0.8` (already in Cargo.toml) | HTTP router, extractors, `IntoResponse` | Already the HTTP layer. No version bump needed. |
| `petgraph` | `0.8` (already in Cargo.toml) | DAG introspection for `/debug/topology` | Already used by `PipelineEngine` (Phase 7). `PipelineEngine::topo_order`, `downstream_map`, and `list_streams()` expose everything the topology endpoint needs. |
| `tokio` | `1.50` (already in Cargo.toml) | Async runtime, `Instant`, optional timer for throughput decay | Already present. No new features needed. |
| `serde_json` | `1.0` (already in Cargo.toml) | JSON shape for new endpoints | Already the JSON substrate across `http.rs`. Match the `Json(serde_json::json!(...))` pattern used by every existing debug handler. |

### Supporting (frontend — vendored, no package manager)

| Asset | Version | Purpose | License | Source |
|-------|---------|---------|---------|--------|
| htmx | `1.9.x` (latest 1.9.12 at time of writing) | Declarative `hx-get` polling + swap, `hx-trigger="every 1s"`, tab switching via `hx-swap` | BSD-2-Clause | `https://unpkg.com/htmx.org@1.9/dist/htmx.min.js` |
| dagre-d3 | `0.6.4` | Directed acyclic graph layout + SVG rendering | MIT | `https://unpkg.com/dagre-d3@0.6.4/dist/dagre-d3.min.js` |
| d3 | `v7.x` (required by dagre-d3) | SVG primitives dagre-d3 depends on | ISC | `https://unpkg.com/d3@7/dist/d3.min.js` |

**License safety:** all three are permissive, allow redistribution in a single binary, and have been vendored into other Rust projects without issue. [CITED: github.com/dagrejs/dagre-d3/blob/master/LICENSE (MIT)]

### Alternatives Considered (and rejected)

| Instead of | Could Use | Why Rejected |
|------------|-----------|--------------|
| `rust-embed` | `include_dir` | `include_dir` is simpler but has no first-class axum integration example and no built-in `mime-guess` feature. `rust-embed` is the CONTEXT-locked choice and aligns with the Phase 9 architectural note in STATE.md ("rust-embed for debug UI asset embedding"). |
| `rust-embed` + hand-rolled handler | `static-serve` crate | `static-serve` has compression and ETag built in, but adds a new dependency and a proc-macro layer. `rust-embed` + a 15-line handler is clearer and stays closer to the zero-ops promise. |
| `rust-embed` | `tower_http::services::ServeDir` | `ServeDir` serves from the filesystem — that contradicts the single-binary promise and DBUI-05 ("no external files"). Not a candidate. |
| htmx + vanilla | React / Preact / Svelte / Vue | Any framework implies a build step. CONTEXT explicitly forbids. |
| dagre-d3 | mermaid.js | mermaid is ~900 KB minified vs dagre-d3 + d3 at ~240 KB combined. Mermaid also renders diagrams from text and is overkill; dagre-d3 takes structured node/edge data, which maps 1:1 to the petgraph output. |
| dagre-d3 | cytoscape.js | Cytoscape is ~400 KB and needs its own layout algorithm selection. dagre-d3's single-purpose DAG layout is closer to what Tally needs. |
| 1 Hz polling | SSE from axum | SSE works with axum but introduces a long-held connection per client, complicates the pause button, and gives no benefit at 1 Hz. CONTEXT locks polling. |

**Installation:**

```toml
# Cargo.toml — add under [dependencies]
rust-embed = { version = "8.11", features = ["mime-guess"] }
```

No other crate additions are required. `mime_guess` comes transitively. `axum`, `tokio`, `serde_json`, `petgraph` are already pinned.

**Version verification:**

```bash
cargo search rust-embed
# rust-embed = "8.11.0"   # latest as of 2026-04
```

`rust-embed` 8.11.0 is the current major line. The `mime-guess` feature enables a `.mimetype()` method on each embedded file which returns `&'static str`, removing the need to import `mime_guess` directly. [CITED: docs.rs/crate/rust-embed/latest]

---

## Architecture Patterns

### Recommended Project Structure

```
tally/
├── Cargo.toml                        # + rust-embed dependency
├── src/
│   ├── server/
│   │   ├── http.rs                   # EXTEND: add 4 new routes
│   │   ├── tcp.rs                    # EXTEND: add ThroughputTracker to AppState,
│   │   │                             #         bump counters on successful push
│   │   ├── mod.rs                    # NEW: `pub mod ui;` (optional — if UI handler
│   │   │                             #      code is factored out)
│   │   ├── ui.rs                     # NEW (optional): Embed struct, index + static
│   │   │                             #                 handlers. Alternative: inline
│   │   │                             #                 into http.rs.
│   │   └── ui/                       # NEW: embedded asset source directory
│   │       ├── index.html            # Single page with 4 tab placeholders
│   │       ├── app.css               # Tokens from UI-SPEC §13.1
│   │       ├── app.js                # < 100 LOC tab switch / pause / dagre render
│   │       ├── icons.svg             # <symbol> defs per UI-SPEC §12
│   │       ├── favicon.svg           # Tally-mark orange glyph
│   │       └── vendor/
│   │           ├── htmx.min.js
│   │           ├── d3.min.js
│   │           ├── dagre-d3.min.js
│   │           └── VENDOR.md         # License, version, SHA256 per file
│   └── engine/
│       └── pipeline.rs               # NO CHANGES — read-only access only
└── tests/
    └── test_debug_ui.rs              # NEW: integration tests for new endpoints
                                      #      + static-asset smoke test
```

The UI-SPEC §11 uses `src/server/static/vendor/`; CONTEXT.md §Code Context uses `src/server/ui/`. These describe the same directory under different names. The planner should pick ONE (recommend `src/server/ui/` to match CONTEXT, which is more specific and carries more weight) and use it consistently.

### Pattern 1: `rust-embed` struct + axum handler

This is the canonical axum + rust-embed pattern as documented upstream. [CITED: github.com/pyrossh/rust-embed/blob/master/examples/axum.rs]

```rust
// src/server/ui.rs (or inline in http.rs)
use axum::{
    body::Body,
    extract::Path,
    http::{header, StatusCode, Uri},
    response::{IntoResponse, Response},
};
use rust_embed::Embed;

#[derive(Embed)]
#[folder = "src/server/ui/"]
struct UiAssets;

/// GET / -> index.html
pub async fn ui_index() -> impl IntoResponse {
    serve_asset("index.html")
}

/// GET /static/{*file} -> embedded asset by path
pub async fn ui_static(Path(file): Path<String>) -> impl IntoResponse {
    serve_asset(&file)
}

fn serve_asset(path: &str) -> Response {
    match UiAssets::get(path) {
        Some(content) => {
            // mime-guess feature gives us &'static str directly
            let mime = content.metadata.mimetype();
            Response::builder()
                .status(StatusCode::OK)
                .header(header::CONTENT_TYPE, mime)
                // Conservative cache; embedded bytes change only on binary rebuild
                .header(header::CACHE_CONTROL, "public, max-age=300")
                .body(Body::from(content.data.into_owned()))
                .unwrap()
        }
        None => (StatusCode::NOT_FOUND, "not found").into_response(),
    }
}
```

Register in `build_router`:

```rust
// src/server/http.rs — inside build_router()
Router::new()
    // ... existing routes ...
    .route("/", get(ui_index))
    .route("/static/{*file}", get(ui_static))
    .route("/debug/topology", get(debug_topology))
    .route("/debug/throughput", get(debug_throughput))
    .with_state(state)
```

**Note on axum 0.8 path syntax:** axum 0.8 uses `{param}` / `{*catch}` (braces), not `:param` / `*catch` — the codebase already uses `/debug/key/{key}` and `/pipelines/{name}`, so the new wildcard follows the same convention: `/static/{*file}`.

### Pattern 2: Petgraph DAG → JSON topology

`PipelineEngine` already stores the DAG in Phase 7 form. Reading it is a pure borrow:

```rust
async fn debug_topology(State(state): State<SharedState>) -> Json<serde_json::Value> {
    let app = state.lock().unwrap_or_else(|e| e.into_inner());

    // Nodes: one per stream and one per view
    let mut nodes: Vec<serde_json::Value> = app.engine.list_streams().map(|s| {
        serde_json::json!({
            "name": s.name,
            "kind": "stream",
            "key_field": s.key_field,              // may be null for keyless
            "feature_count": s.features.len(),
            "depends_on": s.depends_on.clone().unwrap_or_default(),
        })
    }).collect();

    nodes.extend(app.engine.list_views().map(|v| {
        serde_json::json!({
            "name": v.name,
            "kind": "view",
            "key_field": v.key_field,
            "feature_count": v.features.len(),
            "depends_on": Vec::<String>::new(),    // views derive via lookups
        })
    }));

    // Edges: one per depends_on entry (direction: upstream -> downstream)
    let edges: Vec<serde_json::Value> = app.engine.list_streams()
        .flat_map(|s| {
            s.depends_on.clone().unwrap_or_default().into_iter()
                .map(move |dep| serde_json::json!({"from": dep, "to": s.name}))
        })
        .collect();

    // View edges: one per lookup source -> view. Requires a small helper, or
    // just walk the ViewDefinition.features looking for ViewFeatureDef::Lookup.
    // (See engine/pipeline.rs:160 for the enum shape.)

    Json(serde_json::json!({
        "nodes": nodes,
        "edges": edges,
        "topo_order": app.engine.get_topo_order(),   // already exposed at line 706
    }))
}
```

The topological order is already cached on the engine (`PipelineEngine::get_topo_order()` at `src/engine/pipeline.rs:706`), so the client can render nodes in a stable order without re-running toposort in JS.

### Pattern 3: Lock-free (per-stream) throughput tracker

The existing `AppState` is a mutex-protected struct, so "lock-free" here is a choice between (a) keeping per-stream counters inside `AppState` and incrementing under the same mutex, or (b) moving them behind `Arc<DashMap<String, AtomicU64>>` or `Arc<RwLock<HashMap<..>>>` so `/debug/throughput` doesn't contend with PUSH.

**Recommendation:** use option (a). Here is why:
- The PUSH handler already owns the mutex when it finishes a push. Adding one counter increment inside that critical section costs ~10 ns.
- `/debug/throughput` is polled at 1 Hz — contention for read is negligible.
- Option (b) adds a new dependency (`dashmap`) or a second lock that has to stay consistent with the first. Net complexity loss.
- Single-threaded v1 means contention is already a non-issue.

**Data model:**

```rust
// src/server/tcp.rs — new struct, new field on AppState
#[derive(Debug, Default)]
pub struct ThroughputTracker {
    /// Per-stream rolling counts. Key = stream name.
    /// Value = (last_update, count_since_last_decay, ewma_5s, ewma_1m, ewma_5m)
    streams: ahash::AHashMap<String, StreamThroughput>,
}

#[derive(Debug, Default, Clone, Copy)]
pub struct StreamThroughput {
    last_update: Option<std::time::Instant>,
    /// Events observed since `last_update` — flushed into EWMAs on each update.
    pending: u64,
    pub ewma_5s: f64,
    pub ewma_1m: f64,
    pub ewma_5m: f64,
}
```

**Update path (in the Push arm of `handle_sync_command`, after `engine.push_with_cascade` succeeds):**

```rust
// Tally primary + cascade + fan-out hits. ONE increment per target.
let now_inst = std::time::Instant::now();
app.throughput.bump(&stream_name, now_inst);
for ds in &cascade_targets { app.throughput.bump(ds, now_inst); }
for (t, _) in &targets { /* fan-out; skip same conditions as current code */ app.throughput.bump(t, now_inst); }
```

`bump()` accumulates `pending += 1` and, if `now - last_update >= 100ms`, folds `pending` into the three EWMAs using standard exponential decay:

```rust
// Time constants for the three windows (Claude's discretion per CONTEXT)
const TAU_5S:  f64 = 5.0;
const TAU_1M:  f64 = 60.0;
const TAU_5M:  f64 = 300.0;

fn decay(current: f64, tau: f64, dt: f64) -> f64 {
    // Standard exponential decay: e^(-dt/tau)
    current * (-dt / tau).exp()
}
```

On each `bump`, decay the existing EWMA values by `dt`, add `pending / dt` as the instantaneous rate, reset `pending`, update `last_update`. On `/debug/throughput`, also decay once more using `now - last_update` so idle streams report a rate that approaches zero rather than staying pinned at the last bump value.

**Important pitfall:** never compute EWMA with `dt = 0` — guard with `if dt > 0.0`. First-ever event for a stream initializes all three EWMA values to the instantaneous rate or to zero.

### Pattern 4: Frontend htmx + vanilla JS shell

Tab switching is pure JS; per-tab content is either inlined in `index.html` (fastest, no extra HTTP roundtrip) or fetched via `hx-get="/static/tabs/topology.html"` (simpler markup but extra round trips). **Recommendation:** inline all four tabs in `index.html` as `<section class="tab-view" hidden>`; show/hide with a `data-active-tab` attribute toggle. htmx drives data polling inside each tab.

```html
<!-- index.html skeleton, heavily abbreviated -->
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>tally — debug</title>
  <link rel="icon" type="image/svg+xml" href="/static/favicon.svg">
  <link rel="stylesheet" href="/static/app.css">
  <script src="/static/vendor/htmx.min.js" defer></script>
  <script src="/static/vendor/d3.min.js" defer></script>
  <script src="/static/vendor/dagre-d3.min.js" defer></script>
  <script src="/static/app.js" defer></script>
</head>
<body data-active-tab="topology" data-paused="false">
  <header>...</header>
  <main id="tab-body">
    <section class="tab-view" data-tab="topology">
      <!-- polls /debug/topology -->
      <div hx-get="/debug/topology" hx-trigger="load, every 1s [!body.dataset.paused]"
           hx-swap="none" id="topology-root"></div>
    </section>
    <section class="tab-view" data-tab="streams" hidden>
      <div hx-get="/debug/throughput" hx-trigger="load, every 1s [!body.dataset.paused]"
           hx-swap="innerHTML"></div>
    </section>
    <!-- Entity, Memory tabs ... -->
  </main>
  <footer>...</footer>
</body>
</html>
```

Because dagre-d3 expects JS objects and renders into an existing `<svg>`, the topology request uses `hx-swap="none"` plus a small `htmx:afterRequest` listener in `app.js` that reads `evt.detail.xhr.response`, parses JSON, feeds it into dagre-d3's graph builder, and renders.

The Streams / Memory tabs can render as server-rendered HTML (either from the JSON or a tiny JS renderer that builds the DOM) — Claude's discretion. The simplest path is: all three JSON endpoints return JSON, `app.js` has a tiny dispatch function that matches `detail.requestConfig.path` to a renderer.

### Anti-Patterns to Avoid

- **DO NOT** call `ServeDir::new("src/server/ui")` or any filesystem-based static server. Breaks single-binary promise, breaks release builds, breaks DBUI-05.
- **DO NOT** add a top-level `/dist/` or `/public/` directory. Vendor files live under `src/server/ui/` (or `src/server/static/` per UI-SPEC §11 — pick one) so they are co-located with the embed source.
- **DO NOT** fetch htmx/d3/dagre-d3 from a CDN even as a fallback. UI-SPEC §11 forbids runtime CDN fetches ("No runtime CDN fetches").
- **DO NOT** introduce a `build.rs` that downloads assets. Vendoring is a one-time manual step; the files are checked into git.
- **DO NOT** hold the `AppState` mutex across `.await` points in new UI handlers. Every new handler follows the existing pattern: lock, read, `drop(app)` (explicit or implicit), then build the response.
- **DO NOT** introduce a new websocket/SSE route. CONTEXT locks polling.
- **DO NOT** add authentication to these endpoints. CONTEXT §Deferred locks auth out of v1.1.
- **DO NOT** mutate state from any new UI endpoint. The UI is strictly read-only.
- **DO NOT** block dagre-d3 rendering on a sync layout — it's sync JS and already fits fine for < 100 nodes, which is the realistic pipeline size.
- **DO NOT** add axum-`tower` middleware just for static cache headers. Set `Cache-Control` manually in the handler.

---

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| Compile-time asset embedding | A `build.rs` that concatenates files into a `const &[u8]` | `rust-embed` with `#[folder = ...]` | `rust-embed` handles metadata, MIME guessing, dev-mode reload, release-mode embed, and cross-platform paths. Hand-rolling loses the MIME feature and forces you to reinvent file walking. |
| MIME type detection | `match extension { "css" => "text/css", ... }` | `rust-embed` `mime-guess` feature + `file.metadata.mimetype()` | There are ~20 MIME types the UI actually uses; hand-maintaining the table is fine functionally but a maintenance drag. Enabling the feature is free. |
| DAG layout in SVG | Manual node positioning in JS | `dagre-d3` | DAG layout with labels, edge routing, and rank assignment is the hard part. dagre-d3 is single-purpose and sized right (~80 KB minified). |
| EWMA / rolling rate | Running-average loops scattered across the push path | Dedicated `ThroughputTracker` struct with 3 EWMAs per stream | Keeps stateful decay in one place, ensures consistent time constants across all streams, eliminates the risk of double-counting on cascade + fan-out boundaries. |
| JSON-to-JS graph conversion | String concatenation of SVG | `dagre-d3`'s `g.setNode()` / `g.setEdge()` API + standard `render(svg, g)` | Every hand-rolled graph-in-SVG project underestimates edge routing. Don't try. |
| Tab routing | A micro-framework or hash routes | Tiny `app.js` (<100 LOC) that flips `[hidden]` on `<section data-tab>` elements | Four tabs. No router required. |
| Polling loop | `setInterval()` loops inside `app.js` | htmx `hx-trigger="every 1s [!body.dataset.paused]"` | Unified pause mechanism across all panels; no manual timer bookkeeping; respects the pause button via a CSS condition attribute. |
| Live timestamp formatting | Hand-parsed date math | `Date.now()` + a small `formatHHMMSS()` helper | Trivial; no library needed. `Intl.DateTimeFormat` is overkill. |

**Key insight:** The frontend and backend are both trivial on their own; the complexity budget for this phase sits almost entirely in *connecting* them and in the *vendoring discipline* (making sure the three JS libraries are the exact versions, sha256-checksummed, and documented in `VENDOR.md`). Everything else is glue.

---

## Runtime State Inventory

Phase 10 is a feature addition, not a rename/refactor — the inventory is mostly empty, but I'm listing it explicitly per the research protocol.

| Category | Items Found | Action Required |
|----------|-------------|------------------|
| Stored data | **None.** Throughput counters are in-memory only; not persisted to snapshot. Snapshot format (v6) does NOT need to change. | None. |
| Live service config | **None.** No external services, no secrets, no DB configs. | None. |
| OS-registered state | **None.** No systemd units, no scheduled tasks, no process managers. | None. |
| Secrets/env vars | **None.** No new env vars. The HTTP bind address (`TALLY_HTTP_ADDR` if already present, default `127.0.0.1:6401`) is reused as-is. | None. |
| Build artifacts | **New compile-time asset directory** at `src/server/ui/`. `rust-embed` re-runs the embed at build time; no stale artifact risk because Cargo tracks the folder. The `cargo build` output will increase by ~240 KB (htmx + d3 + dagre-d3 + CSS + HTML). | None beyond committing the vendor files. |

---

## Common Pitfalls

### Pitfall 1: `rust-embed` dev-mode vs release-mode divergence

**What goes wrong:** In debug builds, `rust-embed` reads files from the filesystem by default, so `cargo test` + `cargo run` "works" even if you forgot to commit an asset. In release builds, the same code compiles with embedded bytes and suddenly 404s on the missing file.

**Why it happens:** `rust-embed` has a default `debug-embed` feature that is OFF — dev builds read from disk, release builds embed.

**How to avoid:** Enable `debug-embed` in `Cargo.toml` so dev and release behave identically:

```toml
rust-embed = { version = "8.11", features = ["mime-guess", "debug-embed"] }
```

This makes `cargo test` exercise the same code path as production. Absolutely required for Phase 10 because the integration tests need to hit the embedded handler.

**Warning signs:** Tests pass locally, CI fails; or `cargo run` finds `index.html` but `cargo build --release && ./target/release/tally` returns 404.

### Pitfall 2: axum 0.8 path syntax changed from `:param` to `{param}`

**What goes wrong:** Copy-pasting an older axum rust-embed example that uses `"/static/*file"` or `"/static/:file"` into this codebase will refuse to compile.

**Why it happens:** axum 0.8 uses brace-style path parameters. The existing codebase already uses `{key}` and `{name}` — any new route MUST follow that style.

**How to avoid:** Use `/static/{*file}` for the wildcard. Cross-check with `src/server/http.rs:464` (`/debug/key/{key}`) before writing new routes.

**Warning signs:** `cannot find value :file in this scope` or `path parameter *file is not valid`.

### Pitfall 3: Holding the `AppState` mutex across an `await` inside the UI handler

**What goes wrong:** The new `debug_topology`/`debug_throughput` handlers are `async`. If the code path ever `.await`s while holding the `std::sync::MutexGuard`, the guard can stall the entire server under load.

**Why it happens:** All existing http handlers use the pattern `let app = state.lock().unwrap_or_else(|e| e.into_inner()); /* sync read */ Json(...)`. No `await` inside the locked region. If someone accidentally writes `app.engine.some_future().await`, the lock is held across the yield point.

**How to avoid:** Keep UI handlers purely synchronous after the lock acquisition. Build the `serde_json::Value`, drop the guard (implicit at end of block), return. Follow the exact shape of `debug_memory` in `http.rs:295–302`.

**Warning signs:** `MutexGuard` held across `.await`, clippy lint `await_holding_lock`.

### Pitfall 4: Double-counting cascade + fan-out events in the throughput tracker

**What goes wrong:** The existing `Push` arm in `handle_sync_command` updates THREE independent code paths that each "push" to a stream:
1. Primary `engine.push_with_cascade(&stream_name, ...)` which already walks downstream (cascade) streams internally.
2. A separate loop over `cascade_targets` for event log and dirty-key tracking.
3. A fan-out loop that pushes to streams with matching key fields.

If the throughput tracker bumps inside all three, a single cascade hit is counted 2× (once by the engine internally, once by the "track cascade targets" loop).

**Why it happens:** The code at `src/server/tcp.rs:187–281` is the result of several phases of accretion. The cascade loop there is ONLY for logging / dirty tracking, not for updating operator state — `push_with_cascade` already did that.

**How to avoid:** Bump the throughput counter in exactly ONE place per stream. Two acceptable strategies:
- **(A) Inside `push_with_cascade`** (engine layer) — for every stream it actually runs, return the list of streams touched. The TCP handler then bumps each one. This is the cleanest and most accurate.
- **(B) Outside in the TCP handler** — bump primary once, bump each unique element of `cascade_targets` once, bump each fan-out target once (skipping duplicates). Easier to retrofit but requires care about uniqueness.

**Recommendation:** Strategy B (TCP-handler side) is the lower-risk Phase 10 implementation. It keeps the engine interface unchanged and concentrates all new throughput-tracking code in one file. Use a `HashSet` to avoid double-counting.

**Warning signs:** Reported EWMA for a cascading pipeline is exactly 2× or 3× the actual push rate.

### Pitfall 5: First-request layout shift on the DAG canvas

**What goes wrong:** dagre-d3 renders once, measures node widths from the DOM, then re-renders. On the very first paint the SVG `<g>` transform is empty, so the graph briefly flashes at (0,0) before snapping into position.

**Why it happens:** dagre-d3 uses browser text metrics to size nodes. It needs the DOM mounted before layout.

**How to avoid:** Render the DAG only after `htmx:afterSettle` on the first successful `/debug/topology` load, and set an explicit `viewBox` on the SVG before inserting nodes so subsequent renders update in place rather than rebuilding from scratch.

**Warning signs:** Visible flicker on tab switch; user reports "the graph jumps".

### Pitfall 6: Vendored library filenames drifting from the version in VENDOR.md

**What goes wrong:** Executor downloads `htmx.min.js` from unpkg, commits it, writes SHA256 to `VENDOR.md`. Six months later a dev re-downloads with a slightly different version, forgets to update the manifest, and the file on disk no longer matches the recorded hash. Silent drift.

**Why it happens:** No automated check.

**How to avoid:** Add a tiny `cargo test` that reads each file under `src/server/ui/vendor/` at test time, computes its SHA256, and compares against a const table embedded in the test. Failing the test on drift is cheap insurance. This test is also the formal "vendor pinning" verification for DBUI-05.

**Warning signs:** Mismatched hashes; a PR that touches `vendor/` without touching `VENDOR.md`.

### Pitfall 7: Views are not in `list_streams()` — they have their own registry

**What goes wrong:** The topology endpoint calls `engine.list_streams()` and forgets that views are stored separately via `engine.list_views()` (`src/engine/pipeline.rs:886`). The DAG then shows only streams and no views, which breaks the Topology tab and the `--chart-view` (purple) visual distinction UI-SPEC §4.6 explicitly allocates.

**Why it happens:** Views were added in Phase 5 and are held in a separate `AHashMap<String, ViewDefinition>`.

**How to avoid:** Emit nodes from BOTH `list_streams()` and `list_views()` in the `/debug/topology` handler. For views, include the lookup-derived edges by walking `ViewDefinition.features` looking for `ViewFeatureDef::Lookup` entries (see `src/engine/pipeline.rs:160`).

**Warning signs:** UI shows `Transactions` and `Logins` but no `UserRisk` view.

### Pitfall 8: `/debug/memory` current payload is NOT per-stream

**What goes wrong:** UI-SPEC §7.4 expects per-stream memory rows. The existing endpoint (`src/server/http.rs:295`) returns only `{entity_count, stream_count, estimated_bytes}` — a rollup. The Memory tab cannot render a bar list from that.

**Why it happens:** `/debug/memory` was added in Phase 6 as a quick rollup for operations and was never extended to per-stream.

**How to avoid:** Extend `debug_memory` to also emit a `per_stream` array. Source of truth: iterate `engine.list_streams()`, and for each stream count how many entities in `store.entity_keys()` have a `StreamState` under that stream name. A rough per-entity byte estimate (e.g. `2 KB * keys_in_stream`) is fine for v1 — it matches the estimator the existing rollup uses (`keys_total * 2048`).

**Warning signs:** Memory tab shows only the three summary tiles and no bar list; executor looks at the existing handler and assumes "reuse means nothing changes".

**Planner action:** Treat this as an additive contract change — `/debug/memory` keeps the three existing fields (backward compatible) and adds `per_stream: [{name, kind, key_count, estimated_bytes}, ...]`.

### Pitfall 9: `rust-embed` path case sensitivity

**What goes wrong:** `UiAssets::get("INDEX.HTML")` returns `None` on Linux (case-sensitive fs + case-sensitive lookup) but may succeed on macOS. Inconsistent behavior across dev machines.

**How to avoid:** Always use lowercase filenames on disk and in the `get()` call. The frontend routes lowercase paths under `/static/`, so this is easy as long as no one accidentally names a file `App.css`.

**Warning signs:** Asset loads on macOS, 404s in CI on Linux.

---

## Code Examples

### 1. `rust-embed` Embed struct with mime-guess

```rust
// src/server/ui.rs
// Source: docs.rs/crate/rust-embed/latest/source/examples/axum.rs
use rust_embed::Embed;

#[derive(Embed)]
#[folder = "src/server/ui/"]
// In debug builds we still want the embedded bytes path exercised (see Pitfall 1).
// `debug-embed` feature in Cargo.toml enables this globally, but the attribute
// below is a per-struct override if needed:
// #[include = "*"]
struct UiAssets;
```

### 2. axum handler for embedded file with MIME + cache headers

```rust
// Source: adapted from rust-embed upstream axum.rs example
use axum::{
    body::Body,
    http::{header, StatusCode},
    response::{IntoResponse, Response},
};

fn serve(path: &str) -> Response {
    match UiAssets::get(path) {
        Some(content) => {
            let mime: &'static str = content.metadata.mimetype();
            // ETag from the file's built-in hash (rust-embed gives a sha256 in metadata)
            let etag = format!("\"{}\"", hex::encode(content.metadata.sha256_hash()));
            Response::builder()
                .status(StatusCode::OK)
                .header(header::CONTENT_TYPE, mime)
                .header(header::ETAG, etag)
                .header(header::CACHE_CONTROL, "public, max-age=300, must-revalidate")
                .body(Body::from(content.data.into_owned()))
                .unwrap()
        }
        None => (StatusCode::NOT_FOUND, "not found").into_response(),
    }
}
```

Note: `sha256_hash()` may require an additional feature flag. If adding `hex` is not desired, a simpler variant skips ETag and just sets `Cache-Control: no-cache` — UI-SPEC does not require ETag. [CITED: docs.rs/rust-embed/latest/rust_embed/struct.EmbeddedFile.html]

### 3. New debug endpoints wired into `build_router`

```rust
// src/server/http.rs — inside pub fn build_router(state: SharedState) -> Router
Router::new()
    .route("/health", get(health))
    .route("/pipelines", get(list_pipelines).post(create_pipeline))
    .route("/pipelines/{name}", get(get_pipeline).delete(delete_pipeline))
    .route("/metrics", get(metrics_endpoint))
    .route("/debug/key/{key}", get(debug_key))
    .route("/debug/memory", get(debug_memory))
    .route("/debug/backfill", get(debug_backfill))
    .route("/debug/topology", get(debug_topology))        // NEW
    .route("/debug/throughput", get(debug_throughput))    // NEW
    .route("/snapshot", post(trigger_snapshot))
    .route("/", get(ui_index))                            // NEW
    .route("/static/{*file}", get(ui_static))             // NEW
    .with_state(state)
```

### 4. Topology handler — reads petgraph DAG

```rust
async fn debug_topology(State(state): State<SharedState>) -> Json<serde_json::Value> {
    let app = state.lock().unwrap_or_else(|e| e.into_inner());

    let mut nodes: Vec<serde_json::Value> = Vec::new();
    let mut edges: Vec<serde_json::Value> = Vec::new();

    for s in app.engine.list_streams() {
        nodes.push(serde_json::json!({
            "name": s.name,
            "kind": "stream",
            "key_field": s.key_field,
            "features": s.features.iter().map(|(n, _)| n).collect::<Vec<_>>(),
            "depends_on": s.depends_on.clone().unwrap_or_default(),
        }));
        for dep in s.depends_on.clone().unwrap_or_default() {
            edges.push(serde_json::json!({"from": dep, "to": s.name}));
        }
    }

    for v in app.engine.list_views() {
        nodes.push(serde_json::json!({
            "name": v.name,
            "kind": "view",
            "key_field": v.key_field,
            "features": v.features.iter().map(|(n, _)| n).collect::<Vec<_>>(),
            "depends_on": [],
        }));
        // Walk view features for Lookup edges
        for (_fname, fdef) in &v.features {
            if let crate::engine::pipeline::ViewFeatureDef::Lookup { target_stream, .. } = fdef {
                edges.push(serde_json::json!({"from": target_stream, "to": v.name, "kind": "lookup"}));
            }
        }
    }

    Json(serde_json::json!({
        "nodes": nodes,
        "edges": edges,
        "topo_order": app.engine.get_topo_order(),
    }))
}
```

### 5. Throughput handler — EWMA snapshot

```rust
async fn debug_throughput(State(state): State<SharedState>) -> Json<serde_json::Value> {
    let mut app = state.lock().unwrap_or_else(|e| e.into_inner());
    let now = std::time::Instant::now();
    // Decay to "now" so idle streams report dropping rates even with no push events
    app.throughput.decay_all(now);
    let streams: Vec<serde_json::Value> = app.throughput.snapshot().into_iter()
        .map(|(name, s)| serde_json::json!({
            "name": name,
            "ewma_5s": s.ewma_5s,
            "ewma_1m": s.ewma_1m,
            "ewma_5m": s.ewma_5m,
        }))
        .collect();
    Json(serde_json::json!({"streams": streams}))
}
```

### 6. Frontend polling + dagre-d3 render shim (`app.js`, abbreviated)

```javascript
// src/server/ui/app.js — <100 LOC total
document.addEventListener('htmx:afterRequest', (evt) => {
  const path = evt.detail.requestConfig.path;
  if (path === '/debug/topology') return renderTopology(evt.detail.xhr.response);
  if (path === '/debug/throughput') return renderStreams(evt.detail.xhr.response);
  // /debug/memory, /debug/key/* handled similarly
});

function renderTopology(body) {
  const data = JSON.parse(body);
  const g = new dagreD3.graphlib.Graph().setGraph({ rankdir: 'LR' });
  for (const n of data.nodes) {
    g.setNode(n.name, {
      label: n.name,
      class: `node-${n.kind}`,
      rx: 12, ry: 12,
    });
  }
  for (const e of data.edges) g.setEdge(e.from, e.to, {});
  const svg = d3.select('#topology-svg');
  const inner = svg.select('g').empty() ? svg.append('g') : svg.select('g');
  new dagreD3.render()(inner, g);
}

// Tab switching
document.addEventListener('click', (evt) => {
  const tab = evt.target.closest('[role="tab"]');
  if (!tab) return;
  document.body.dataset.activeTab = tab.dataset.tab;
});

// Pause/resume
document.getElementById('pause-btn').addEventListener('click', () => {
  const paused = document.body.dataset.paused === 'true';
  document.body.dataset.paused = paused ? 'false' : 'true';
});
```

### 7. Integration test pattern (matches `tests/test_server.rs`)

```rust
// tests/test_debug_ui.rs
use std::sync::{Arc, Mutex};
use tally::server::http::run_http_server_with_listener;
use tally::server::tcp::{AppState, Metrics, SharedState};

#[tokio::test]
async fn topology_endpoint_emits_nodes_and_edges() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let state: SharedState = Arc::new(Mutex::new(AppState {
        engine: Default::default(),
        // ... same AppState fields as tests/test_server.rs:30-55 ...
    }));

    // Register two streams with a depends_on edge via the TCP REGISTER path
    // (or construct StreamDefinitions directly and call engine.register()).

    tokio::spawn(run_http_server_with_listener(listener, state));

    let resp = reqwest::get(format!("http://{}/debug/topology", addr)).await.unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert!(body["nodes"].as_array().unwrap().len() >= 2);
    assert!(body["edges"].as_array().unwrap().len() >= 1);
}

#[tokio::test]
async fn static_index_is_embedded() {
    // Identical setup...
    let resp = reqwest::get(format!("http://{}/", addr)).await.unwrap();
    assert_eq!(resp.status(), 200);
    let ct = resp.headers().get("content-type").unwrap().to_str().unwrap();
    assert!(ct.starts_with("text/html"));
    let body = resp.text().await.unwrap();
    assert!(body.contains("tally"));
    assert!(body.contains("/static/app.css"));
}
```

**Note on `reqwest`:** check `Cargo.toml` dev-deps before assuming it's available. If it isn't, follow the existing test pattern which uses raw `tokio::net::TcpStream` + hand-written HTTP requests (see `tests/test_server.rs`). Planner must resolve this: either add `reqwest` as a dev-dep or hand-write the HTTP request. Hand-writing is closer to the zero-dep ethos of the project but more verbose.

---

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| `rocket_contrib::serve` | `rust-embed` + axum handler | 2020+ | Rocket is deprecated for new Rust projects; axum is the standard. |
| Raw `include_bytes!` macros for assets | `rust-embed` derive macro | 2019+ | `rust-embed` tracks the folder automatically, handles metadata, and exposes a uniform accessor. Hand-rolled `include_bytes!` has no MIME support and breaks if files are added/removed. |
| `tower_http::services::ServeDir` for bundled UIs | `rust-embed` for binary-embedded UIs | ongoing | `ServeDir` is filesystem-only; fine for dev but fails the single-binary promise. |
| dagre (non-d3) | dagre-d3 | 2016+ | dagre-d3 combines layout + rendering. `dagre` (the layout-only package) is still maintained but requires a separate renderer. dagre-d3 is the right choice when d3 is already in the page. |
| SockJS / SSE for live dashboards | Polling every 1 s for single-node debug tools | ongoing | For a single-process, localhost debug UI, polling is simpler and has equivalent UX at 1 Hz. SSE matters at scale; this is not at scale. |
| axum 0.7 `:param` routes | axum 0.8 `{param}` routes | axum 0.8 (2024) | Phase 10 is on 0.8 — use braces. The existing codebase already uses `{key}` and `{name}`. |

**Deprecated / outdated:**
- `dagre-d3` is formally in "maintenance mode" on GitHub — the last release is 2020. It still works with d3 v7 and is widely vendored, but there are no upcoming releases. Acceptable for a vendored, pinned asset. [CITED: github.com/dagrejs/dagre-d3 README]
- `include_dir` is fine but see the alternatives table above for why `rust-embed` wins for this use case.

---

## Assumptions Log

| # | Claim | Section | Risk if Wrong |
|---|-------|---------|---------------|
| A1 | `rust-embed` 8.11 supports the `debug-embed` feature flag the same way 8.x always has. | Common Pitfalls, Installation | If 8.11 dropped the flag, dev/release divergence returns. Mitigation: pin to 8.8+ which is well-known to have it; verify at integration time with `cargo doc --open -p rust-embed`. |
| A2 | The existing `AppState` mutex is fast enough that an added per-stream throughput bump does not measurably affect PUSH p99 (< 100 µs). | Pattern 3 | If wrong, drop the bump behind `Arc<[AtomicU64]>` per stream instead. The single-threaded core means this is overwhelmingly likely to be safe. |
| A3 | dagre-d3 0.6.4 still renders correctly with d3 v7 when both are loaded as global scripts. | Supporting stack | dagre-d3 was originally built for d3 v4/v5. There are community reports that v7 works with minor adapter shims. If it does not, fall back to d3 v5 (also ISC). **HIGH RISK — planner should verify at vendor time by loading both in an `index.html` smoke test.** |
| A4 | The `/debug/memory` endpoint can be extended to emit `per_stream` without breaking existing callers. | Pitfall 8 | Additive change is safe if no one parses the JSON with a strict schema. Current callers are only the Python SDK and manual curl — both tolerate extra fields. |
| A5 | Views should appear on the topology DAG alongside streams, with lookup edges drawn from the lookup's source stream to the view. | Patterns, Pitfalls | UI-SPEC §7.1 explicitly allocates `--chart-view #a371f7` for views and shows them as rounded rects, so they DO belong on the DAG. Verified. |
| A6 | EWMA time constants 5s / 60s / 300s are the right choice. | Pattern 3 | These match the 5s / 1m / 5m window labels in CONTEXT. The decay function uses these as `τ`; if the UI feels "too slow to react" at 5s, shorten τ_5s to 3s. |
| A7 | `reqwest` is NOT a dev-dep. Integration tests use raw `tokio::net::TcpStream`. | Test example | If wrong, the example simplifies. If right, the planner must either add `reqwest` or hand-roll. A grep of `tests/` confirms raw TCP is the existing pattern. |
| A8 | The directory for embedded assets is `src/server/ui/`, not `src/server/static/`. | Project Structure | UI-SPEC §11 says `static/`, CONTEXT says `ui/`. Both are valid; executor picks one. Recommendation: `src/server/ui/` to match CONTEXT (locked user decision outranks UI-SPEC's source path). |
| A9 | The UI is bound to the HTTP management port (6401) with no authentication. | User constraints | CONTEXT §Deferred explicitly keeps auth out. Assumption is that `6401` is bound to localhost or a trusted network, matching existing `/metrics` and `/debug/*` conventions. |

**Actions for discuss-phase or planner:**
- **A3 is the only HIGH-risk assumption.** The d3 v7 + dagre-d3 0.6.4 combination should be smoke-tested with a tiny `index.html` loaded in a browser BEFORE committing to the vendor files. If it fails, downgrade d3 to v5 (still ISC, smaller, and known to work with dagre-d3 0.6.4).
- **A8** is a naming decision the planner should resolve in the plan's Wave 0 so the executor does not oscillate.

---

## Open Questions

1. **Is there an existing `TALLY_HTTP_ADDR` env var, or is the HTTP bind address hardcoded?**
   - What we know: `src/main.rs` is not loaded in this research session; CONTEXT says "existing HTTP management port 6401".
   - What's unclear: whether the address is env-driven or constant.
   - Recommendation: the planner reads `src/main.rs` during Wave 0 and either reuses the existing config or adds nothing (the UI endpoints do not care about the address).

2. **Should the Memory tab consume the existing `/debug/memory` rollup or require an extended `per_stream` field?**
   - What we know: UI-SPEC §7.4 mandates a per-stream bar list; current endpoint only returns a rollup.
   - What's unclear: whether the planner treats this as an endpoint extension or a separate endpoint.
   - Recommendation: **extend** `/debug/memory` additively. Keep the three existing fields, add `per_stream`. Less API surface, backward compatible.

3. **Should the throughput tracker tick also decay on a background timer, or only on read + write?**
   - What we know: on-read decay (during `/debug/throughput`) and on-write decay (during `bump`) are sufficient for correctness.
   - What's unclear: whether the planner wants a background task that decays every 1s for visibility on `/metrics` (Prometheus) too.
   - Recommendation: on-read + on-write only. Adding a background timer doubles the state machinery and saves nothing. Prometheus integration is out of scope for Phase 10.

4. **Does the existing `PipelineEngine` expose a public `get_topo_order` getter?**
   - Verified: yes, `pub fn get_topo_order(&self) -> &[String]` at `src/engine/pipeline.rs:706` (confirmed by grep).
   - No action needed.

5. **How are sparklines (tiny per-stream rate graphs) expected to be rendered?**
   - UI-SPEC leaves this to Claude's discretion. Recommendation: a tiny inline SVG polyline generated from a 60-point rolling buffer stored client-side in `app.js`. Do NOT persist sparkline history on the server — keep state client-side.

6. **Does `/debug/topology` need to expose operator-level detail (count/sum/avg/etc.) on each feature, or just the feature name?**
   - UI-SPEC §7.6 (node detail panel) shows `tx_count_30m  count · 30m` — the type and window are expected.
   - Recommendation: include `{name, type, window_secs}` per feature in the topology response. The existing `/pipelines/{name}` handler in `http.rs:28` already emits this exact shape; lift the helper and reuse it in the topology handler.

---

## Environment Availability

Phase 10 is almost pure Rust + vendored JS. External tooling dependencies:

| Dependency | Required By | Available | Version | Fallback |
|------------|------------|-----------|---------|----------|
| `cargo` | Rust build | ✓ (already used for Phases 1-9) | current stable | — |
| `rustc` | Rust build | ✓ | current stable | — |
| Internet access (one-time, vendor download) | Downloading htmx/d3/dagre-d3 from unpkg at vendor time | ✓ (assumed dev machine) | — | Manual file copy |
| `sha256sum` (or equivalent) | Recording SHA256 in VENDOR.md | ✓ (macOS `shasum -a 256`, Linux `sha256sum`) | — | `openssl dgst -sha256` |
| Browser (for smoke-testing the UI locally) | Manual Wave-3 verification | ✓ (any modern browser) | Chrome / Firefox / Safari | — |
| `reqwest` crate (if chosen for integration tests) | New integration tests | ✗ (not in dev-deps) | — | Hand-rolled `tokio::net::TcpStream` requests (matches existing `tests/test_server.rs` pattern) |

**Missing dependencies with no fallback:** None. Every dependency listed above is either already present or has a trivial fallback.

**Missing dependencies with fallback:**
- `reqwest` for integration tests. Fallback: reuse the raw-TCP pattern already in `tests/test_server.rs`. Cheaper than adding a new dev-dep.

---

## Validation Architecture

Per `.planning/config.json` policy (Nyquist validation assumed enabled unless explicitly disabled). This section lets the planner derive `VALIDATION.md` directly.

### Test Framework

| Property | Value |
|----------|-------|
| Framework | Rust's built-in `#[test]` + `#[tokio::test]` (already in use) |
| Config file | None — `Cargo.toml` `[dev-dependencies]` only |
| Quick run command | `cargo test --test test_debug_ui` |
| Full suite command | `cargo test` |

### Phase Requirements → Test Map

| Req ID | Behavior | Test Type | Automated Command | File Exists? |
|--------|----------|-----------|-------------------|--------------|
| DBUI-01 | `GET /debug/topology` returns `{nodes, edges, topo_order}` shape with streams and views | integration | `cargo test --test test_debug_ui topology_endpoint_emits_nodes_and_edges` | ❌ Wave 0 |
| DBUI-01 | Topology response includes `depends_on` edges for cascade-linked streams | integration | `cargo test --test test_debug_ui topology_includes_cascade_edges` | ❌ Wave 0 |
| DBUI-01 | Topology response includes view nodes with `kind: "view"` and lookup edges | integration | `cargo test --test test_debug_ui topology_includes_view_nodes` | ❌ Wave 0 |
| DBUI-02 | `GET /debug/throughput` returns per-stream EWMA fields (`ewma_5s`, `ewma_1m`, `ewma_5m`) | integration | `cargo test --test test_debug_ui throughput_endpoint_emits_per_stream_ewma` | ❌ Wave 0 |
| DBUI-02 | EWMA increases after pushing events to a stream | integration | `cargo test --test test_debug_ui throughput_reflects_recent_pushes` | ❌ Wave 0 |
| DBUI-02 | EWMA decays to near-zero after a period of no pushes (time-warped via Instant mock or by asserting strict inequality) | integration | `cargo test --test test_debug_ui throughput_decays_when_idle` | ❌ Wave 0 |
| DBUI-02 | Throughput tracker does NOT double-count cascade targets (Pitfall 4) | unit | `cargo test --lib throughput::does_not_double_count_cascade` | ❌ Wave 0 |
| DBUI-03 | Entity tab is reachable: `GET /debug/key/u_demo` still works | integration | `cargo test --test test_debug_ui entity_lookup_reuses_existing_endpoint` | ❌ Wave 0 |
| DBUI-04 | `GET /debug/memory` response contains `per_stream: [...]` with name + key_count + estimated_bytes | integration | `cargo test --test test_debug_ui memory_endpoint_emits_per_stream_breakdown` | ❌ Wave 0 |
| DBUI-04 | `/debug/memory` backward-compatible: old fields (`entity_count`, `stream_count`, `estimated_bytes`) still present | integration | `cargo test --test test_debug_ui memory_endpoint_backward_compatible` | ❌ Wave 0 |
| DBUI-05 | `GET /` returns HTML with `Content-Type: text/html; charset=utf-8` and the page title `tally — debug` | integration | `cargo test --test test_debug_ui static_index_is_embedded` | ❌ Wave 0 |
| DBUI-05 | `GET /static/app.css` returns `text/css` and a non-empty body containing `--accent-primary` | integration | `cargo test --test test_debug_ui static_css_is_embedded` | ❌ Wave 0 |
| DBUI-05 | `GET /static/vendor/htmx.min.js` returns `application/javascript` and matches the SHA256 in `VENDOR.md` | integration | `cargo test --test test_debug_ui static_htmx_is_vendored_and_hashed` | ❌ Wave 0 |
| DBUI-05 | `GET /static/vendor/dagre-d3.min.js` + SHA256 validation | integration | `cargo test --test test_debug_ui static_dagre_is_vendored_and_hashed` | ❌ Wave 0 |
| DBUI-05 | `GET /static/vendor/d3.min.js` + SHA256 validation | integration | `cargo test --test test_debug_ui static_d3_is_vendored_and_hashed` | ❌ Wave 0 |
| DBUI-05 | `GET /static/does-not-exist.css` returns 404 | integration | `cargo test --test test_debug_ui static_unknown_returns_404` | ❌ Wave 0 |
| DBUI-05 | Release build (`cargo build --release`) produces a binary with the same static-asset behavior as debug (via `debug-embed` feature) | manual | `cargo build --release && ./target/release/tally …` (one-shot smoke) | ❌ Wave 0 manual step |

### Sampling Rate

- **Per task commit:** `cargo test --test test_debug_ui` (scoped to Phase 10's new integration file; completes in seconds).
- **Per wave merge:** `cargo test` (full suite — catches regressions in `test_server`, `test_pipeline`, etc.).
- **Phase gate:** Full suite green AND a manual browser smoke test of the rendered UI (load `http://localhost:6401/`, verify four tabs, verify DAG renders, verify pause button, verify no console errors) before `/gsd-verify-work`.

### Wave 0 Gaps

- [ ] `tests/test_debug_ui.rs` — new integration test file covering all 17 test cases above.
- [ ] `tests/common/mod.rs` (optional) — if AppState construction is repeated across tests, extract a `fn test_app_state()` helper so the test file is not copy-pasting 25 lines of `AppState { ... }` initialization per test.
- [ ] `src/server/ui/VENDOR.md` — license + version + SHA256 for htmx.min.js, d3.min.js, dagre-d3.min.js. Created during vendor task, not during test task, but test tasks depend on it.
- [ ] Unit test scaffolding for `ThroughputTracker` (add `#[cfg(test)] mod tests` in `src/server/tcp.rs` or a new `src/server/throughput.rs`).
- [ ] Decision on `reqwest` vs raw-TCP for tests (see A7 / Environment Availability). Wave 0 decision: use raw TCP to match existing pattern.

---

## Security Domain

Phase 10 adds new HTTP endpoints and serves static content from the management port. Per `.planning/config.json` policy, evaluate ASVS applicability.

### Applicable ASVS Categories

| ASVS Category | Applies | Standard Control |
|---------------|---------|------------------|
| V2 Authentication | **no** | CONTEXT §Deferred explicitly excludes auth in v1.1. Management port assumed bound to localhost / trusted network, consistent with existing `/metrics`, `/pipelines`, `/debug/*`. No change in trust model. |
| V3 Session Management | no | No sessions; UI is stateless, read-only. |
| V4 Access Control | no | No access control on management port by design. Existing endpoints already have none. |
| V5 Input Validation | **yes** | `/debug/key/{key}` already URL-decodes a user-supplied key; the new `/static/{*file}` path parameter needs path-traversal hardening (see below). Topology and throughput endpoints take no user input. |
| V6 Cryptography | no | No crypto; UI is read-only over plain HTTP on a trusted port. |
| V7 Error Handling | **yes (light)** | Error responses must not leak filesystem paths or panic messages. Match the existing pattern of `Json(serde_json::json!({"error": ...}))`. |
| V11 Business Logic | no | No business logic on the UI path. |
| V12 File and Resources | **yes** | Static file serving must not allow path traversal out of `src/server/ui/`. `rust-embed`'s `UiAssets::get(path)` is safe by construction — it only matches strings that were embedded at compile time — but it's still worth asserting. |
| V13 API and Web Service | **yes** | New JSON endpoints should set `Content-Type: application/json` (axum `Json` does this automatically) and return well-formed JSON on all paths including error cases. |
| V14 Configuration | no | No new configuration. |

### Known Threat Patterns for Tally UI stack (axum 0.8 + rust-embed + htmx)

| Pattern | STRIDE | Standard Mitigation |
|---------|--------|---------------------|
| Path traversal on `/static/{*file}` | Tampering / Information Disclosure | `rust-embed` `get()` only resolves strings that were embedded at compile time. `../etc/passwd` returns `None` and the handler responds `404`. Still: explicitly reject paths containing `..` as a defense-in-depth measure. |
| XSS in the rendered UI via a crafted stream name | Tampering | Stream names are bounded by the REGISTER path's validation (`src/engine/pipeline.rs:276`). dagre-d3 renders labels using d3's `.text()` which escapes content. Do NOT switch to `.html()` in `app.js`. |
| DoS via expensive `/debug/topology` | DoS | Topology response scales linearly with stream+view count. Even at 10,000 streams (well beyond realistic v1 usage) the response is ~2 MB and the handler is a single mutex-protected borrow. Acceptable. |
| DoS via polling storms | DoS | Polling is client-side (1 Hz). A malicious client could spam the endpoint, but the management port is trusted. Out of scope. |
| Reflected error messages leaking internal state | Information Disclosure | Error handler uses `format!("{}", e)` on `TallyError`. Audit that no `Debug` formatter leaks paths. This is a pre-existing concern inherited from Phases 6–9 and is not made worse by Phase 10. |
| HTML injection via entity key in `/debug/key/{key}` response | Tampering | Existing endpoint returns JSON, not HTML. The UI renders key text via textContent or htmx's default `hx-swap="innerHTML"` — CAUTION: `innerHTML` WILL execute `<script>` tags on SVG injection. Use `hx-swap="none"` + manual `.textContent` for user-supplied content. |
| CSRF on state-mutating endpoints | Tampering | **Not applicable.** Phase 10 adds zero write endpoints. UI is read-only. |

**Security summary:** Phase 10 is low-risk because it adds no authentication surface (explicitly deferred), no mutation endpoints, and no user-supplied HTML rendering. The only hardenings worth doing are (1) defense-in-depth path-traversal check on `/static/{*file}`, (2) ensuring the frontend never `innerHTML`s user-supplied data (use `textContent` or d3's `.text()`), and (3) carrying forward the existing error-response discipline from `http.rs`.

---

## Sources

### Primary (HIGH confidence)

- `src/engine/pipeline.rs` (lines 160–950) — exact API surface for `list_streams`, `list_views`, `get_topo_order`, `downstream_map`, `ViewDefinition`, `ViewFeatureDef::Lookup`. [VERIFIED: codebase read]
- `src/server/http.rs` (full file) — exact `build_router` pattern, `Json(serde_json::json!)` response shape, axum 0.8 path syntax (`{key}`, `{name}`), `run_http_server_with_listener` signature. [VERIFIED: codebase read]
- `src/server/tcp.rs` (lines 1–300) — `AppState` structure, Push arm of `handle_sync_command`, cascade + fan-out code paths for throughput instrumentation. [VERIFIED: codebase read]
- `Cargo.toml` — existing dependencies: axum 0.8, petgraph 0.8, tokio 1.50, serde_json 1.0. [VERIFIED: codebase read]
- `.planning/phases/10-debug-ui/10-CONTEXT.md` — locked user decisions. [VERIFIED: file read]
- `.planning/phases/10-debug-ui/10-UI-SPEC.md` §1–§13 — visual contract, vendor policy, icon list, accessibility, tokens. [VERIFIED: file read]
- `.planning/REQUIREMENTS.md` — DBUI-01 through DBUI-05 exact text. [VERIFIED: file read]
- `.planning/STATE.md` — Phase 9 note confirming "rust-embed for debug UI asset embedding (single binary preserved)". [VERIFIED: file read]
- `tests/test_server.rs` lines 21–56 — existing integration test pattern: raw `TcpListener::bind("127.0.0.1:0")` + `run_http_server_with_listener` + `tokio::spawn`. [VERIFIED: grep]

### Secondary (MEDIUM confidence — web)

- [rust-embed on crates.io / docs.rs](https://docs.rs/crate/rust-embed/latest) — version 8.11.0 is current, `mime-guess` feature exists, axum example in the source tree. [CITED]
- [rust-embed axum example](https://github.com/pyrossh/rust-embed/blob/master/examples/axum.rs) — canonical handler shape. [CITED]
- [mime_guess on crates.io](https://crates.io/crates/mime_guess) — MIT licensed, transitive via rust-embed `mime-guess` feature. [CITED]
- [axum static-file-server example](https://github.com/tokio-rs/axum/blob/main/examples/static-file-server/src/main.rs) — reference pattern (filesystem, not applicable, but confirms axum's static-serving conventions). [CITED]
- [dagre-d3 GitHub repo](https://github.com/dagrejs/dagre-d3) — MIT license confirmed; version 0.6.4 is current stable; project is in maintenance mode. [CITED]
- [dagre-d3 LICENSE](https://github.com/dagrejs/dagre-d3/blob/master/LICENSE) — MIT text. [CITED]
- [dagre-d3 unpkg CDN](https://app.unpkg.com/dagre-d3@0.6.4/files/dist) — `dagre-d3.min.js` download target for vendoring. [CITED]

### Tertiary (LOW confidence — needs runtime verification)

- **dagre-d3 0.6.4 + d3 v7 compatibility** — community reports suggest it works with minor adapter shims; some note breakages. **Smoke-test required at vendor time.** [ASSUMED — A3]
- EWMA time constants 5s / 60s / 300s are "the right feel" for a debug UI. **Tunable at execution time.** [ASSUMED — A6]
- `reqwest` is not a dev-dep. [VERIFIED against Cargo.toml which shows only `tempfile`]

---

## Metadata

**Confidence breakdown:**
- Standard stack (Rust side): **HIGH** — every crate is either already present or has first-party documentation confirming the integration pattern. rust-embed + axum + petgraph + serde_json is a trivial composition.
- Standard stack (frontend / vendored JS): **MEDIUM** — htmx 1.9 and d3 v7 are both well-understood. dagre-d3 0.6.4 is stable but in maintenance mode and its d3 v7 compatibility is the single research item worth smoke-testing.
- Architecture patterns: **HIGH** — every handler skeleton mirrors an existing Phase 6–9 handler in `http.rs`. No architectural novelty.
- Petgraph introspection: **HIGH** — `PipelineEngine` already exposes `list_streams`, `list_views`, `get_topo_order`, and stream `depends_on` as public API (verified by grep against `pipeline.rs`).
- Throughput tracker: **MEDIUM** — the design is sound but correctness depends on getting cascade + fan-out dedup right (Pitfall 4). Needs a unit test.
- Pitfalls: **HIGH** — identified by reading the existing code and applying standard axum/rust-embed knowledge.
- Security: **HIGH** — trust model is unchanged from prior phases; no new attack surface beyond path-traversal (mitigated by rust-embed) and the standard read-only endpoints.
- Validation architecture: **HIGH** — test framework is the same Rust `#[tokio::test]` pattern in `tests/test_server.rs`; all 17 test cases map 1:1 to requirement IDs.

**Research date:** 2026-04-10
**Valid until:** 2026-05-10 (30 days — the backend Rust crates are stable; the only fast-moving assumption is rust-embed's feature set, which changes rarely)
