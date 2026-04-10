---
phase: 10
depth: standard
status: findings
files_reviewed: 15
findings_count: 7
blocking_count: 0
warning_count: 3
nit_count: 4
generated: 2026-04-10
---

# Phase 10 Code Review

## Summary

Phase 10 is a high-quality implementation. The load-bearing correctness properties — textContent-only DOM writes, axum 0.8 brace wildcards, additive `/debug/memory` extension, no `.await` across mutex locks, cascade-dedup in `bump_unique`, `dt <= 0.0` guard — all land exactly as planned. The two most notable issues are a mathematically incorrect EWMA formula in `throughput.rs` that will cause `/debug/throughput` to report values substantially larger than real events/sec (~5x to ~500x depending on rate), and a broken htmx wiring in the Entity tab form that returns 404 regardless of input. Neither is blocking for verification sign-off (tests pass and backend endpoints are correct in isolation), but both deserve fast follow-ups.

## Findings by Severity

### Blocking (must fix before verification)

None.

### Warning (should fix)

#### WR-01: EWMA formula does not converge to actual events/sec

**File:** `src/server/throughput.rs:107-110`

`fold_event` uses `ewma = ewma * exp(-dt/tau) + (1/dt)` where `1/dt` is the instantaneous rate (full weight, no alpha mixing). This is not a time-based EWMA. At steady state with rate `r`, `dt = 1/r` and the recurrence becomes `ewma_ss = r / (1 - exp(-1/(r·tau)))`. For `r=100 ev/s, tau=5s`, that is ≈50,000 — roughly 500× the real rate. For `r=1 ev/s, tau=5s`, ≈5.5 — 5× the real rate. The reported `ewma_5s` / `ewma_1m` / `ewma_5m` in the Streams tab will be wildly inflated and scale non-linearly. Unit tests only assert `> 0` and decay direction, so they do not catch calibration.

**Fix:** Use standard time-variable EWMA with alpha mixing:

```rust
let instantaneous = 1.0 / dt;
let alpha_5s = 1.0 - (-dt / TAU_5S).exp();
let alpha_1m = 1.0 - (-dt / TAU_1M).exp();
let alpha_5m = 1.0 - (-dt / TAU_5M).exp();
entry.ewma_5s += alpha_5s * (instantaneous - entry.ewma_5s);
entry.ewma_1m += alpha_1m * (instantaneous - entry.ewma_1m);
entry.ewma_5m += alpha_5m * (instantaneous - entry.ewma_5m);
```

Add calibration test: 100 events spaced 10ms apart, assert `ewma_5s` within 20% of 100.0.

---

#### WR-02: Entity tab form queries `/debug/key/?key=...` instead of `/debug/key/{key}`

**File:** `src/server/ui/index.html:74-84`

Form declares `hx-get="/debug/key/"` + `hx-include="#entity-key"`. htmx serializes to query string: `GET /debug/key/?key=u_demo`. Router is `/debug/key/{key}` expecting a path segment — returns 404 for every key. Entity tab is non-functional as shipped.

**Note on routing:** User routed the interactive UI redesign to Phase 10.2 (Option A). Phase 10.2 will redesign the Entity drill-in from scratch (accessed via node click on topology, not as a separate tab). Fixing this form wiring now is throwaway work that Phase 10.2 will discard. **Defer to Phase 10.2.**

The integration test `entity_lookup_reuses_existing_endpoint` exercises `/debug/key/u_demo` directly via raw HTTP, bypassing the form — which is why tests pass.

---

#### WR-03: Missing paste-XSS regression test for entity key input

**File:** `tests/test_debug_ui.rs` (no test exists)

Code audit of app.js confirms every entity-key DOM write uses `.textContent` (lines 317, 334, 346), and dagre-d3 uses default text labelType. However, no automated test asserts a payload-shaped key is HTML-escaped in the rendered response. A future refactor that switches a `.textContent` write to `.innerHTML` would pass `cargo test` silently.

**Fix:** Add a test that GETs `/` (index.html) and asserts the response body does NOT contain a raw `<script>alert` substring — a source-level regression check. Alternatively, add a build-time or test-time grep sink check for `innerHTML|outerHTML|insertAdjacentHTML|document\.write|eval\(|labelType.*html` in app.js.

---

### Nit (nice to have)

#### IN-01: `Response::builder()...body(...).unwrap()` in ui_static

**File:** `src/server/ui.rs:74-82`

`.unwrap()` after `.body()` can panic on header failure. MIME comes from mime_guess (ASCII only) so unreachable in practice. Prefer `.expect("valid MIME from mime_guess")` or fallback-to-INTERNAL_SERVER_ERROR.

#### IN-02: Path-traversal check reliant on axum Path decoding

**File:** `src/server/ui.rs:57`

`file.contains("..")` runs after axum's `Path<String>` has decoded percent-encoding, so `..%2f` is caught. Defense-in-depth suggestion: add a `debug_assert!` or comment documenting this invariant so future readers don't weaken the check.

#### IN-03: Test-only `pending_total` field in StreamThroughput

**File:** `src/server/throughput.rs:33-36`

`#[cfg(test)] pending_total: u64` changes struct size in test builds only. Not a bug, but document that `pending_total_for_test` is not part of the public snapshot API.

#### IN-04: `debug_memory` recomputes `entity_count()` twice

**File:** `src/server/http.rs:458-462`

```rust
"entity_count": app.store.entity_count(),
"estimated_bytes": app.store.entity_count() * 2048,
```

Bind once: `let entity_count = app.store.entity_count();`. Trivial cost; matches the pattern at line 424 where `keys` is bound once.

---

## Files Reviewed

- `src/server/throughput.rs`
- `src/server/ui.rs`
- `src/server/ui/index.html`
- `src/server/ui/app.css`
- `src/server/ui/app.js`
- `src/server/ui/vendor/VENDOR.md`
- `src/server/mod.rs`
- `src/server/tcp.rs` (Push arm instrumentation + AppState field)
- `src/server/http.rs` (new routes + handlers + additive memory extension)
- `src/main.rs` (AppState field init)
- `Cargo.toml`
- `tests/test_debug_ui.rs`
- `tests/test_server.rs` (stale literal fix)
- `tests/test_pipeline.rs` (stale literal fix)
- `tests/test_snapshot.rs` (stale literal fix)

Vendored JS files content-skipped per scope; SHA256 drift tests cover them at test time.

---

## Acknowledgments (patterns done right)

1. **XSS sink audit in app.js is spotless.** Zero `innerHTML`, `outerHTML`, `insertAdjacentHTML`, `document.write`, `eval(`, `Function(`, or d3 `.html()` calls. Every user/server string goes through `.textContent` or the default dagre-d3 text labelType.
2. **Axum 0.8 brace wildcards correct.** The only catch-all is `/static/{*file}`. No legacy `*file` form exists.
3. **Additive `/debug/memory` contract preserved byte-for-byte.** Three Phase 6 fields keep names and derivation; `per_stream` purely additive. Covered by `memory_endpoint_backward_compatible` test.
4. **Zero `.await` across any new-handler mutex lock.** All five new handlers traced by hand.
5. **Cascade-dedup in `bump_unique` correct.** HashSet-based, uses the same skip logic as the fan-out loop. Load-bearing regression test passes.
6. **`dt <= 0.0` guard in `fold_event`.** Two bumps at the same `Instant` leave EWMAs untouched but advance `last_update`.
7. **Path-traversal defense in `ui_static`.** Explicit `..`, leading `/`, and NUL byte rejection layered on rust-embed's compile-time scoping.
8. **UI-SPEC §13.1 token audit passes verbatim.** Exactly 6 spacing values, 4 type sizes. Whitelisted 11px chip context explicit in plan line 391.
9. **All tests use `127.0.0.1:0` random ports.** Zero hard-coded ports anywhere.
10. **SHA256 drift tests re-hash on-disk bytes** and parse VENDOR.md with a tolerant pipe-cell scanner.
11. **Stale struct-literal fixes use sensible defaults** across three test files.

---

_Review generated: 2026-04-10_
_Reviewer: gsd-code-reviewer (standard depth)_
