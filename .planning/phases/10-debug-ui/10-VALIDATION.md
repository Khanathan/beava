---
phase: 10
slug: debug-ui
status: draft
nyquist_compliant: false
wave_0_complete: false
created: 2026-04-10
---

# Phase 10 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution. Derived from `10-RESEARCH.md` §Validation Architecture.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | Rust's built-in `#[test]` + `#[tokio::test]` (already in use) |
| **Config file** | None — `Cargo.toml` `[dev-dependencies]` only |
| **Quick run command** | `cargo test --test test_debug_ui` |
| **Full suite command** | `cargo test` |
| **Estimated runtime** | ~30 seconds (phase-scoped); ~90 seconds (full suite) |

---

## Sampling Rate

- **After every task commit:** Run `cargo test --test test_debug_ui`
- **After every plan wave:** Run `cargo test`
- **Before `/gsd-verify-work`:** Full suite must be green AND a manual browser smoke test (load `http://localhost:6401/`, verify four tabs, verify DAG renders, verify pause button, verify no console errors)
- **Max feedback latency:** 30 seconds

---

## Per-Task Verification Map

| Req ID | Behavior | Test Type | Automated Command | File Exists | Status |
|--------|----------|-----------|-------------------|-------------|--------|
| DBUI-01 | `GET /debug/topology` returns `{nodes, edges, topo_order}` shape with streams and views | integration | `cargo test --test test_debug_ui topology_endpoint_emits_nodes_and_edges` | ❌ W0 | ⬜ pending |
| DBUI-01 | Topology response includes `depends_on` edges for cascade-linked streams | integration | `cargo test --test test_debug_ui topology_includes_cascade_edges` | ❌ W0 | ⬜ pending |
| DBUI-01 | Topology response includes view nodes with `kind: "view"` and lookup edges | integration | `cargo test --test test_debug_ui topology_includes_view_nodes` | ❌ W0 | ⬜ pending |
| DBUI-02 | `GET /debug/throughput` returns per-stream EWMA fields (`ewma_5s`, `ewma_1m`, `ewma_5m`) | integration | `cargo test --test test_debug_ui throughput_endpoint_emits_per_stream_ewma` | ❌ W0 | ⬜ pending |
| DBUI-02 | EWMA increases after pushing events to a stream | integration | `cargo test --test test_debug_ui throughput_reflects_recent_pushes` | ❌ W0 | ⬜ pending |
| DBUI-02 | EWMA decays to near-zero after a period of no pushes | integration | `cargo test --test test_debug_ui throughput_decays_when_idle` | ❌ W0 | ⬜ pending |
| DBUI-02 | Throughput tracker does NOT double-count cascade targets (Pitfall 4) | unit | `cargo test --lib throughput::does_not_double_count_cascade` | ❌ W0 | ⬜ pending |
| DBUI-03 | Entity tab is reachable: `GET /debug/key/u_demo` still works | integration | `cargo test --test test_debug_ui entity_lookup_reuses_existing_endpoint` | ❌ W0 | ⬜ pending |
| DBUI-04 | `GET /debug/memory` response contains `per_stream: [...]` with name + key_count + estimated_bytes | integration | `cargo test --test test_debug_ui memory_endpoint_emits_per_stream_breakdown` | ❌ W0 | ⬜ pending |
| DBUI-04 | `/debug/memory` backward-compatible: old fields (`entity_count`, `stream_count`, `estimated_bytes`) still present | integration | `cargo test --test test_debug_ui memory_endpoint_backward_compatible` | ❌ W0 | ⬜ pending |
| DBUI-05 | `GET /` returns HTML with `Content-Type: text/html; charset=utf-8` and the page title `tally — debug` | integration | `cargo test --test test_debug_ui static_index_is_embedded` | ❌ W0 | ⬜ pending |
| DBUI-05 | `GET /static/app.css` returns `text/css` and a non-empty body containing `--accent-primary` | integration | `cargo test --test test_debug_ui static_css_is_embedded` | ❌ W0 | ⬜ pending |
| DBUI-05 | `GET /static/vendor/htmx.min.js` returns `application/javascript` and matches the SHA256 in `VENDOR.md` | integration | `cargo test --test test_debug_ui static_htmx_is_vendored_and_hashed` | ❌ W0 | ⬜ pending |
| DBUI-05 | `GET /static/vendor/dagre-d3.min.js` + SHA256 validation | integration | `cargo test --test test_debug_ui static_dagre_is_vendored_and_hashed` | ❌ W0 | ⬜ pending |
| DBUI-05 | `GET /static/vendor/d3.min.js` + SHA256 validation | integration | `cargo test --test test_debug_ui static_d3_is_vendored_and_hashed` | ❌ W0 | ⬜ pending |
| DBUI-05 | `GET /static/does-not-exist.css` returns 404 | integration | `cargo test --test test_debug_ui static_unknown_returns_404` | ❌ W0 | ⬜ pending |
| DBUI-05 | Release build produces a binary with the same static-asset behavior as debug (via `debug-embed` feature) | manual | `cargo build --release && ./target/release/tally …` (one-shot smoke) | ❌ W0 manual | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

---

## Wave 0 Requirements

- [ ] `tests/test_debug_ui.rs` — new integration test file covering all 16 automated test cases above (raw TCP pattern matching `tests/test_server.rs`, NOT reqwest)
- [ ] `tests/common/mod.rs` (optional) — extract `fn test_app_state()` helper if AppState construction is repeated across tests
- [ ] `src/server/ui/VENDOR.md` — license + version + SHA256 for `htmx.min.js`, `d3.min.js`, `dagre-d3.min.js` (created during vendor task; tests depend on it)
- [ ] Unit test scaffolding for `ThroughputTracker` — add `#[cfg(test)] mod tests` in `src/server/tcp.rs` or a new `src/server/throughput.rs`
- [ ] Confirm Wave 0 decision: raw TCP tests (matches existing pattern), not reqwest

---

## Manual-Only Verifications

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| Rendered debug UI loads in a real browser with DAG visualization, live throughput, entity lookup, and memory breakdown all working | DBUI-01..05 | DOM rendering + dagre-d3 + htmx + d3 v7 compatibility cannot be fully verified in Rust integration tests (no browser in CI) | (1) `cargo run` to start server. (2) Open `http://localhost:6401/` in Chrome/Firefox. (3) Verify four tabs render (Topology, Throughput, Entity, Memory). (4) Verify DAG edges and nodes appear, with blue/purple/gray colors. (5) Verify throughput numbers update live (after pushing sample events). (6) Click pause button; confirm polling stops. (7) Search for an entity key; confirm features display. (8) Verify memory tab shows per-stream bars. (9) DevTools console must have zero errors. |
| Release binary preserves single-binary property | DBUI-05 | Ensures `rust-embed` with `debug-embed` feature bundles assets into `--release` output; cannot be asserted by debug-mode tests alone | `cargo build --release && ./target/release/tally` — then `curl -sI http://localhost:6401/static/app.css \| grep 'text/css'` |
| dagre-d3 0.6.4 + d3 v7 compatibility (RESEARCH §A3 flagged risk) | DBUI-01 | The vendored library pair has maintenance-mode status; community reports mixed compatibility results | During Wave 1 vendor task: vendor the files, run a one-off local HTML smoke test rendering a 3-node DAG before committing vendored assets to the repo. Fall back to d3 v5 if rendering breaks. |

---

## Validation Sign-Off

- [ ] All tasks have `<automated>` verify or Wave 0 dependencies
- [ ] Sampling continuity: no 3 consecutive tasks without automated verify
- [ ] Wave 0 covers all MISSING references (`tests/test_debug_ui.rs`, `VENDOR.md`, throughput unit test module)
- [ ] No watch-mode flags
- [ ] Feedback latency < 30s
- [ ] `nyquist_compliant: true` set in frontmatter

**Approval:** pending
