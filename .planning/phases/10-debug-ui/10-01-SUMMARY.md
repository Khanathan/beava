---
phase: 10-debug-ui
plan: 01
subsystem: infra
tags: [rust-embed, htmx, d3, dagre-d3, vendor, sha256]

requires:
  - phase: 06
    provides: HTTP management port state wiring foundation
provides:
  - rust-embed dependency with mime-guess and debug-embed features
  - src/server/ui/ embed source tree
  - vendored htmx 1.9.12, d3 7.8.5, dagre-d3 0.6.4 JavaScript libraries
  - VENDOR.md manifest with version/license/source URL/SHA256 per file
  - verified d3 v7 + dagre-d3 0.6.4 renderer compatibility
affects: [10-02, 10-03, 10-04, 10-05]

tech-stack:
  added:
    - rust-embed 8.11 (features: mime-guess, debug-embed)
    - htmx 1.9.12 (vendored, BSD-2-Clause)
    - d3 7.8.5 (vendored, ISC)
    - dagre-d3 0.6.4 (vendored, MIT)
  patterns:
    - Vendor-with-manifest pattern (SHA256 manifest + automated drift-detection tests)
    - Compile-time asset embedding via rust-embed rather than runtime CDN fetch

key-files:
  created:
    - src/server/ui/.gitkeep
    - src/server/ui/vendor/htmx.min.js
    - src/server/ui/vendor/d3.min.js
    - src/server/ui/vendor/dagre-d3.min.js
    - src/server/ui/vendor/VENDOR.md
  modified:
    - Cargo.toml
    - Cargo.lock

key-decisions:
  - "rust-embed 8.11 over alternatives (include_dir, axum-embed) — only option with debug-embed + mime-guess features needed (RESEARCH Pitfall 1)"
  - "debug-embed feature enabled — forces embed in debug builds so `cargo run` serves the UI identically to release"
  - "htmx 1.9.12 pinned — last 1.9.x release; 2.x is breaking and CONTEXT.md locks polling semantics"
  - "d3 v7.8.5 + dagre-d3 0.6.4 pair confirmed by browser smoke test; no fallback to d3 v5 needed"
  - "dagre-d3 bundle size discrepancy accepted — actual 725181 bytes vs plan's ~85KB estimate because unpkg bundle inlines graphlib; this is the authoritative byte-for-byte file"

patterns-established:
  - "Vendored-assets manifest: every new vendored asset MUST be listed in VENDOR.md with version/license/source URL/SHA256; Plan 10-05 drift tests re-hash at test time"
  - "No runtime CDN fetch for debug UI assets (DBUI-05) — all JS/CSS/HTML embedded at compile time"

requirements-completed:
  - DBUI-05

duration: ~15min
completed: 2026-04-10
---

# Phase 10 Plan 01: Single-Binary Foundation for Debug UI

**rust-embed 8.11 wired + htmx/d3/dagre-d3 vendored with SHA256 manifest and browser-verified d3 v7 + dagre-d3 0.6.4 compatibility**

## Performance

- **Duration:** ~15 min (spread across two sessions due to pause before Task 3 checkpoint)
- **Started:** 2026-04-10T03:00:00Z
- **Completed:** 2026-04-10T07:30:00Z (Task 3 user-approval timestamp)
- **Tasks:** 3 (2 auto + 1 human-verify)
- **Files modified:** 6 created, 2 modified

## Accomplishments

- Added `rust-embed = { version = "8.11", features = ["mime-guess", "debug-embed"] }` to Cargo.toml — the single-binary foundation every later Phase 10 plan depends on.
- Created the `src/server/ui/` and `src/server/ui/vendor/` embed source tree that CONTEXT.md locked as the rust-embed root path.
- Vendored three JavaScript libraries (htmx 1.9.12, d3 7.8.5, dagre-d3 0.6.4) as pinned `.min.js` files byte-for-byte from unpkg, with SHA256s recorded in VENDOR.md.
- **Validated** d3 v7 + dagre-d3 0.6.4 compatibility with a local browser smoke test before committing — the only high-risk assumption RESEARCH §A3 called out for the phase.

## Task Commits

1. **Task 1: Add rust-embed dependency and create embed source directory** — `7a5a5d1` (feat)
2. **Task 2: Vendor htmx, d3, and dagre-d3 with SHA256 manifest** — `185df22` (feat) + `a9df815` (chore: drop vendor/.gitkeep now that real files replace it)
3. **Task 3: Local browser smoke test of d3 + dagre-d3 compatibility** — checkpoint:human-verify, approved by user (no commit; smoke-test HTML discarded per plan)

## Files Created/Modified

- `Cargo.toml` — added rust-embed dependency (one line)
- `Cargo.lock` — new transitive crates: rust-embed 8.11, mime_guess 2.0.5, sha2, digest, walkdir, and their dependencies
- `src/server/ui/.gitkeep` — preserves empty embed root directory in git
- `src/server/ui/vendor/htmx.min.js` — 48101 bytes, SHA256 `449317ade7881e949510db614991e195c3a099c4c791c24dacec55f9f4a2a452` (htmx 1.9.12, BSD-2-Clause)
- `src/server/ui/vendor/d3.min.js` — 279633 bytes, SHA256 `d6b03aefc9f6c44c7bc78713679c78c295028fa914319119e5cc4b4954855b1c` (d3 7.8.5, ISC)
- `src/server/ui/vendor/dagre-d3.min.js` — 725181 bytes, SHA256 `74f9b84c0f18f4f639ab99a6b563244463823072432b2df866bc5d6c1180f5cb` (dagre-d3 0.6.4, MIT)
- `src/server/ui/vendor/VENDOR.md` — version/license/source URL/SHA256 manifest for all three vendored files

## Decisions Made

- **debug-embed feature enabled on rust-embed.** Without it, `cargo run` serves files from disk instead of from the binary, masking drift between dev and prod. With it, the same bytes flow through both paths.
- **d3 v7 + dagre-d3 0.6.4 pair validated empirically.** The browser smoke test rendered three blue-outlined nodes labeled Transactions, Logins, UserRisk with two directed edges (Transactions → UserRisk, Logins → UserRisk) and logged `rendered nodes: 3 edges: 2` with zero console errors. No fallback to d3 v5.16.0 is needed; this pair is the canonical combo for the rest of the phase.
- **dagre-d3 bundle size discrepancy acknowledged.** The plan estimated ~85 KB; the actual file is 725181 bytes because the unpkg bundle inlines graphlib. The file is byte-identical to `https://unpkg.com/dagre-d3@0.6.4/dist/dagre-d3.min.js`, so we accept the larger size and record the actual value in VENDOR.md.

## Deviations from Plan

None — plan executed exactly as written. The dagre-d3 size note above is a clarification of a plan estimate, not a deviation from the plan's acceptance criteria.

## Issues Encountered

None — the two blocking anti-patterns flagged before execution (vendored-JS smoke test, `.textContent` XSS prevention for later plans) were acknowledged up front. The smoke test was the designed checkpoint for Task 3 and passed on first attempt.

## User Setup Required

None — vendored assets are part of the repository and the binary build. No environment variables, no external services.

## Next Phase Readiness

- `rust-embed` is compile-time ready for Plan 10-03's `UiAssets` struct (`#[folder = "src/server/ui/"]`).
- All three vendored libraries are on disk with verified SHA256s for Plan 10-05's drift-detection tests (`static_htmx_is_vendored_and_hashed`, `static_d3_is_vendored_and_hashed`, `static_dagre_is_vendored_and_hashed`).
- Plan 10-02 (ThroughputTracker) has no dependency on this plan's outputs and can run in parallel once Wave 1 dispatches it.

---
*Phase: 10-debug-ui*
*Completed: 2026-04-10*
