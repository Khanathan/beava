---
phase: 10-debug-ui
plan: 03
subsystem: infra
tags: [axum, rust-embed, debug-ui, http, topology, throughput, memory, dag, ewma]

requires:
  - phase: 10
    provides: rust-embed dependency wired and src/server/ui/ embed root (Plan 10-01)
  - phase: 10
    provides: ThroughputTracker::decay_all + snapshot API (Plan 10-02)
  - phase: 07
    provides: petgraph DAG + cached topological order (PipelineEngine::get_topo_order)
provides:
  - src/server/ui.rs UiAssets rust-embed struct and ui_index / ui_static handlers
  - GET /debug/topology endpoint (nodes for streams + views, cascade + lookup edges, topo_order)
  - GET /debug/throughput endpoint (EWMA 5s/1m/5m per stream, decayed to now)
  - Additively-extended GET /debug/memory with per_stream breakdown
  - GET / (embedded index.html) and GET /static/{*file} (embedded asset catch-all)
affects: [10-04, 10-05]

tech-stack:
  added: []
  patterns:
    - Lock-once-then-build-JSON for debug endpoints (RESEARCH Pattern 3 / Pitfall 3) — every new handler acquires the AppState mutex, reads/clones what it needs into local Vecs, and returns Json(...) without any .await inside the guard scope
    - Additive contract extension for debug endpoints — /debug/memory keeps its Phase 6 fields (entity_count, stream_count, estimated_bytes) AND adds per_stream, so pre-existing curl scripts still parse
    - Defense-in-depth path-traversal rejection on top of rust-embed's compile-time scoping (T-10-01)

key-files:
  created:
    - src/server/ui.rs
  modified:
    - src/server/mod.rs
    - src/server/http.rs

key-decisions:
  - "MIME type cloned to owned String (not &'static str) in serve_asset — rust_embed::EmbeddedFile::metadata::mimetype() returns a borrow tied to the metadata binding, and content.data.into_owned() consumes content before the header is finalized. Cloning the MIME was the single compile-blocker surfaced by Task 1's cargo check and the only auto-fix on this plan."
  - "entity_keys().collect() into a Vec<String> in debug_memory rather than iterating while also calling get_entity — sidesteps any lifetime conflict on &self and costs one Vec<String> allocation per call (debug endpoint, not hot path)."
  - "Topology emits views as kind='view' nodes with depends_on: [] (views do not have a depends_on field in ViewDefinition, so views only participate in the DAG via their Lookup edges)."
  - "Route ordering inside build_router groups the new /debug/* routes next to the existing /debug/* routes, then the UI routes (/, /static/{*file}) after /snapshot at the bottom to keep the file's visual cascade stable."
  - "Cascade edges tagged kind='cascade' and view-lookup edges tagged kind='lookup' — gives the Plan 10-04 frontend a discriminator to style the two edge kinds differently (solid vs dashed, or primary-color vs accent-orange per UI-SPEC §4.6)."

patterns-established:
  - "Pattern: Debug endpoint handler shape. Every read-only /debug/* handler follows let (mut)? app = state.lock().unwrap_or_else(|e| e.into_inner()); read; Json(...); no .await in the lock scope. Plan 10.1's latency debugger and any future debug endpoint must follow the same shape."
  - "Pattern: Additive debug-endpoint extension. When a debug endpoint needs more fields, add keys to the existing JSON object — never introduce a parallel endpoint (/debug/memory_v2 etc.)."
  - "Pattern: axum 0.8 brace wildcards. Catch-all routes MUST use /prefix/{*param} syntax; the legacy /prefix/*param form is silently broken on 0.8. Cross-reference /pipelines/{name} and /debug/key/{key} for the single-segment form."

requirements-completed:
  - DBUI-01
  - DBUI-02
  - DBUI-04
  - DBUI-05

duration: ~3min
completed: 2026-04-10
---

# Phase 10 Plan 03: Debug Endpoints + Embedded UI Routes Summary

**Four new axum 0.8 routes — `GET /`, `GET /static/{*file}`, `GET /debug/topology`, `GET /debug/throughput` — plus an additive `per_stream` extension on `/debug/memory`, all hung off the existing `Arc<Mutex<AppState>>` lock-once-then-build-JSON pattern with zero `.await` inside any mutex scope.**

## Performance

- **Duration:** ~3 minutes of wall time (2 tasks, no deviations, 1 single-line compile fix)
- **Started:** 2026-04-10T12:48:00Z
- **Completed:** 2026-04-10T12:51:21Z
- **Tasks:** 2 (both `type="auto"`)
- **Files created:** 1 (`src/server/ui.rs`)
- **Files modified:** 2 (`src/server/mod.rs`, `src/server/http.rs`)

## Accomplishments

- **New `src/server/ui.rs` module** — `#[derive(Embed)] #[folder = "src/server/ui/"] pub struct UiAssets;` plus two axum handlers (`ui_index` → `index.html`, `ui_static` → any embedded asset) backed by a private `serve_asset(path)` helper that sets `Content-Type` from `rust-embed`'s `mime-guess` feature and `Cache-Control: public, max-age=300`. Path-traversal defense-in-depth rejects `..`, absolute paths, and NUL bytes before delegating to `UiAssets::get`.
- **New `GET /debug/topology`** — Walks both `engine.list_streams()` AND `engine.list_views()` (RESEARCH §Pitfall 7: forgetting views would break the topology tab's purple nodes). Emits cascade edges from `StreamDefinition.depends_on` and lookup edges from `ViewFeatureDef::Lookup` features, plus the cached `engine.get_topo_order()` so the frontend can render nodes in stable execution order without re-running toposort in JavaScript.
- **New `GET /debug/throughput`** — Locks `AppState` with `&mut`, calls `app.throughput.decay_all(Instant::now())` BEFORE `snapshot()` (per Plan 10-02 contract — idle streams report declining rates even with no recent push), and returns `{streams: [{name, ewma_5s, ewma_1m, ewma_5m}, ...]}`.
- **Extended `GET /debug/memory` additively** — Keeps the Phase 6 top-level rollup fields (`entity_count`, `stream_count`, `estimated_bytes`) for backward compatibility AND adds a `per_stream` array. Per-stream key counts are computed by iterating every entity via `store.entity_keys()` and tallying `StreamEntityState` presence per stream name. Each per-stream entry has `{name, kind, key_count, estimated_bytes}` with `kind: "stream"` for streams (estimated at 2 KB × key_count, same estimator the rollup uses) and `kind: "view"` for views (always `key_count: 0`, `estimated_bytes: 0` — views hold no operator state).
- **Registered four new routes in `build_router`** — `/debug/topology`, `/debug/throughput`, `/`, and `/static/{*file}` (axum 0.8 brace wildcard syntax — RESEARCH §Pitfall 6 verified against existing `/pipelines/{name}` and `/debug/key/{key}` routes).

## Task Commits

1. **Task 1: Create `src/server/ui.rs` with rust-embed static asset handlers** — `4914847` (feat)
2. **Task 2: Add debug_topology + debug_throughput; extend debug_memory; register four new routes** — `ec43ace` (feat)

**Plan metadata:** (pending — added via final commit after this SUMMARY)

## JSON Response Shapes

### GET /debug/topology

```json
{
  "nodes": [
    {
      "name": "Transactions",
      "kind": "stream",
      "key_field": "user_id",
      "features": ["tx_count_30m", "tx_sum_1h"],
      "depends_on": []
    },
    {
      "name": "UserRisk",
      "kind": "view",
      "key_field": "user_id",
      "features": ["tx_to_login_ratio"],
      "depends_on": []
    }
  ],
  "edges": [
    { "from": "RawEvents", "to": "Transactions", "kind": "cascade" },
    { "from": "MerchantActivity", "to": "FraudSignals", "kind": "lookup" }
  ],
  "topo_order": ["RawEvents", "Transactions", "Logins", "UserRisk"]
}
```

### GET /debug/throughput

```json
{
  "streams": [
    { "name": "Transactions", "ewma_5s": 123.4, "ewma_1m": 98.7, "ewma_5m": 55.2 },
    { "name": "Logins",       "ewma_5s":   2.1, "ewma_1m":  1.4, "ewma_5m":  0.9 }
  ]
}
```

### GET /debug/memory (before vs after)

**Before (Phase 6 shape, still supported):**

```json
{
  "entity_count": 12000,
  "stream_count": 3,
  "estimated_bytes": 24576000
}
```

**After (Plan 10-03, additive):**

```json
{
  "entity_count": 12000,
  "stream_count": 3,
  "estimated_bytes": 24576000,
  "per_stream": [
    { "name": "Transactions", "kind": "stream", "key_count": 8000, "estimated_bytes": 16384000 },
    { "name": "Logins",       "kind": "stream", "key_count": 4000, "estimated_bytes":  8192000 },
    { "name": "UserRisk",     "kind": "view",   "key_count":    0, "estimated_bytes":        0 }
  ]
}
```

The three original top-level fields are preserved byte-for-byte; Phase 6's `/metrics` and the Prometheus memory gauge keep their existing estimator.

## Files Created/Modified

- **`src/server/ui.rs`** (created, 85 lines) — `UiAssets` rust-embed struct; `ui_index` and `ui_static` handlers; private `serve_asset(path)` helper with MIME + cache headers; path-traversal defense (`..`, absolute paths, NUL bytes).
- **`src/server/mod.rs`** (modified, +1 line) — added `pub mod ui;` after `pub mod throughput;`.
- **`src/server/http.rs`** (modified, +165 lines) — added `use crate::server::ui::{ui_index, ui_static};`; inserted `debug_topology`, `debug_throughput`, and extended `debug_memory` before `trigger_snapshot`; added four routes to `build_router`.

## Decisions Made

- **MIME cloned into owned `String` in `serve_asset`** — `rust_embed::EmbeddedFile::metadata::mimetype()` returns `&str` borrowed from the metadata binding, so assigning `&'static str` (as the RESEARCH code example used) failed `cargo check` with `E0597: content.metadata does not live long enough`. Cloning to `String` before `content.data.into_owned()` consumes the binding is the minimal fix; the performance cost is negligible for a debug-only asset endpoint. (Task 1 compile-error fix — counted as a single-line correction, not a deviation.)
- **`entity_keys().collect()` into `Vec<String>` in `debug_memory`** — The iterator is `impl Iterator<Item = String> + '_` tied to `&self`. Iterating AND calling `get_entity(key)` on the same borrow would work, but collecting up-front into a Vec keeps the code unambiguous and lets the per-stream count loop finish before the later `app.engine.list_streams()` borrow. Debug endpoint → one allocation per call is acceptable.
- **View nodes emit `depends_on: []`** — `ViewDefinition` has no `depends_on` field. Views participate in the DAG only via `ViewFeatureDef::Lookup` features, which Task 2's handler emits as `lookup` edges. This matches RESEARCH §Pitfall 7's guidance and keeps the frontend DAG rendering consistent.
- **Edge `kind` discriminator (`cascade` vs `lookup`)** — Gives the Plan 10-04 frontend a stable field to style cascade edges (solid, primary blue) differently from lookup edges (dashed, accent orange), matching UI-SPEC §4.6 without committing to CSS selectors from backend code.
- **Route grouping in `build_router`** — All new `/debug/*` routes inserted next to the existing `/debug/*` routes; the two UI routes (`/`, `/static/{*file}`) land after `/snapshot` at the end. Keeps the visual cascade of the file stable and groups routes by purpose rather than by plan.

## Deviations from Plan

None — plan executed exactly as written.

The one wrinkle was a `cargo check` compile error on Task 1 (`E0597: content.metadata does not live long enough` from the `&'static str` annotation copied from RESEARCH §Code Example 2). I changed the single line to `let mime: String = content.metadata.mimetype().to_string();` and re-ran `cargo check` — clean. This is a single-token mechanical fix of a RESEARCH example bug, not a scope change, so it is logged here under Decisions Made rather than as a Rule 1 deviation.

## Issues Encountered

- **`cargo` not on `$PATH` in fresh bash sessions.** Every Bash tool invocation on this executor's agent thread starts from a reset `cwd`, and `cargo` lives at `~/.cargo/bin/cargo` — not on the default `$PATH`. Workaround: every `cargo` call was prefixed with `source ~/.cargo/env &&`. No impact on plan correctness, but worth documenting for future Rust-plan executors on this machine. (Noted here; not an auto-fix deviation.)

## User Setup Required

None — no environment variables, no external services, no config changes.

## Next Phase Readiness

- **Plan 10-04 (frontend HTML/CSS/JS under `src/server/ui/`)** can now author `index.html`, `app.css`, `app.js`, and any additional vendor files, and they will flow through `UiAssets::get` and the `ui_index`/`ui_static` handlers automatically. All four backend endpoints the frontend polls (`/debug/topology`, `/debug/throughput`, `/debug/memory`, `/debug/key/{key}`) are live on the HTTP management port.
- **Plan 10-05 (integration tests)** can exercise:
  - `GET /` — asserts 200 + `Content-Type: text/html` once Plan 10-04 lands `index.html`
  - `GET /static/vendor/htmx.min.js` — asserts 200 + `Content-Type: application/javascript` + body bytes match the SHA256 from `VENDOR.md`
  - `GET /debug/topology` — asserts response shape matches `{nodes, edges, topo_order}` and that a registered view renders as `kind: "view"` with its lookup edges
  - `GET /debug/throughput` — asserts `decay_all` ran by registering a stream, pushing one event, sleeping 10 s, and asserting `ewma_5s < ewma_5m`
  - `GET /debug/memory` — asserts both the original three fields AND the new `per_stream` array are present (backward compat + forward extension)
  - `GET /static/..%2fetc%2fpasswd` — asserts 404 (path-traversal defense-in-depth)
- **No new blockers.** Plan 10-05 still has to fix the stale `AppState { .. }` literals in `tests/test_server.rs` and `tests/test_pipeline.rs` (missing the `throughput` field added by Plan 10-02), which is that plan's explicit scope.
- **461 lib tests** still pass after both Task 1 and Task 2 — no regressions.

## Threat Flags

None — this plan adds no new trust boundaries beyond the four surfaces the threat model already covers:

- `/static/{*file}` path traversal: mitigated via rust-embed's compile-time scoping + explicit `..`/`/`/`\0` rejection (T-10-01).
- Stream name XSS in topology JSON: mitigated — stream names are already bounded by engine registration validation; `serde_json` escapes them on serialization (T-10-02 — frontend Plan 10-04 must still use `d3.text()` not `d3.html()`).
- Error responses: all handlers return static strings or `serde_json::json!(...)` literals; no `format!("{:?}", ...)` on error paths (T-10-03).
- Long-held mutex: every new handler verified by hand + grep to have zero `.await` in its lock scope (T-10-04).

## Self-Check: PASSED

- `src/server/ui.rs` — FOUND (`#[derive(Embed)]`, `#[folder = "src/server/ui/"]`, `pub async fn ui_index`, `pub async fn ui_static`, path-traversal `if file.contains("..")`, `content.metadata.mimetype()` all present).
- `src/server/mod.rs` — FOUND (`pub mod ui;` present alongside `pub mod throughput;`).
- `src/server/http.rs` — FOUND (`use crate::server::ui::{ui_index, ui_static};`, `async fn debug_topology`, `async fn debug_throughput`, `"per_stream": per_stream`, `ViewFeatureDef::Lookup`, `app.throughput.decay_all`, `/debug/topology`, `/debug/throughput`, `/static/{*file}`, `get(ui_index)`, `get(ui_static)` all present).
- Commit `4914847` — FOUND in `git log --oneline`.
- Commit `ec43ace` — FOUND in `git log --oneline`.
- `cargo check --lib --bin tally` — exits 0 with zero errors and zero warnings.
- `cargo test --lib` — 461 of 461 tests pass, no regressions from Plan 10-02's baseline.
- No `.await` between `state.lock(...)` and handler return in `debug_topology`, `debug_throughput`, or `debug_memory` (verified by awk+grep scan of each handler body).

---

*Phase: 10-debug-ui*
*Plan: 03*
*Completed: 2026-04-10*
