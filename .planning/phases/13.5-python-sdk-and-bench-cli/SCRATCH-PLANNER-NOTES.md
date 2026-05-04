# Phase 13.5 Planner Scratch Notes

Auto-generated 2026-05-03 by the orchestrator agent during `/gsd-plan-phase 13.5`.
Captures auto-decisions taken when the user-locked context didn't dictate a specific
choice. None of these touch the 5 user-locked decisions (D-01 through D-05) — those
are honored verbatim per CONTEXT.md.

## Auto-decisions taken (planner discretion per CONTEXT.md § Claude's Discretion)

### Plan ordering / wave shape — 12 plans across 8 waves

Initial draft used 6 waves with `python/beava/__init__.py` shared across Plans 02/03/04
in the same wave (Wave 2). Plan-checker self-audit caught this as a same-wave file
overlap (planner contract `assign_waves` rule: "Same-wave plans must have zero
`files_modified` overlap"). Fix: serialise the three plans across Waves 2/3/4 and
cascade dependent plans (Plans 05/06/07/11/12) accordingly. Final wave shape:

| Wave | Plans | Tracks |
|------|-------|--------|
| 1 | 01 (delete stale modules) + 08 (bench CLI 4 modes) | Python + Rust |
| 2 | 02 (bv.App core) + 09 (bench dataset workloads) | Python + Rust |
| 3 | 03 (pipeline DSL) + 10 (bench interactive + estimator) | Python + Rust |
| 4 | 04 (53 op helpers) | Python |
| 5 | 05 (PEP 563 + demo loader + submodules) | Python |
| 6 | 06 (demo datasets) + 07 (beava.test fixtures) | Python |
| 7 | 11 (mypy strict + v0 tests green) | Python integration |
| 8 | 12 (microbench + throughput + closure) | Closure |

The Python SDK side runs sequentially across the `__init__.py` shared file (Plans 01-05);
the Rust bench side runs in parallel for Waves 1-3. Tracks converge at Wave 7 (integration)
and Wave 8 (closure).

### Demo dataset generation — deterministic seed=42

CONTEXT.md leaves dataset generation to planner discretion. Picked stdlib `random.seed(42)`
(no numpy dependency to keep wheel slim) with per-dataset seed offsets (+0 / +1 / +2) so
re-running `python -m beava.demos._generate` produces byte-identical bundled files.

### mypy `# type: ignore` codes — D-01 Any escapes deferred to Plan 11

CONTEXT.md says "planner picks based on actual mypy output". Plan 11's Task 11.b is the
only place where the mypy sweep actually runs against the freshly-rewritten code; specific
`# type: ignore[<code>]` placements happen there. Pre-decided: per-module `[[tool.mypy.overrides]]`
disabling `no-any-return` for `beava._col` (operator-overloading AST) and `beava._agg`
(bv.lit polymorphism). Inline `# type: ignore` comments referencing D-01 for one-off
cases as they surface.

### `inquire` walkthrough wording

Picked 4 prompts: mode → workload → size (only when workload is a dataset, not a synthetic
size) → duration. Auto-default values: mode=throughput, workload=fraud, size=medium,
duration=60s. Pre-run estimate prints to stderr before the final "Run now?" Confirm prompt.

### Bench output JSON schema

Picked schema_version=1, flat result struct with optional fsync-mode-only fields
(`fsync_p50_us` / `fsync_p99_us` / `fsync_p999_us`). Fields match what `beava-bench-v2.rs`
already emits when feasible — keeps the existing ledger.jsonl shape forward-compatible.

### Test fixture pytest setup — autouse=False default

`beava.test.fixture(reset_each=True, test_mode=True)` is opt-in per test (no `autouse=True`).
Users wire it into their conftest.py with a one-liner. Matches Polars convention.

### Plan 11 SDK bug-fix policy

If integration tests surface SDK bugs, fix them in Plan 11 with per-bug red-then-green
pairs (revert the fix to confirm the test fails, commit revert as red, restore + commit
as green). For wire-shape mismatches that point at Phase 13.4 engine drift: file an issue
against 13.4 and `xfail` the test rather than patching the SDK; SDK contract is locked
post-Plan 13.0-04, so drift is engine-side.

## Open questions surfaced during planning (none blocking)

None. All 5 D-XX decisions are clearly actionable; planner discretion items above are
surface-level execution details that don't change the contract.

## Plan-checker self-audit (informal, since plan-checker subagent unavailable)

Performed mechanical self-audit per gsd-plan-checker contract:

- ✅ Frontmatter: all 12 plans have valid yaml frontmatter
- ✅ Wave shape: zero same-wave file_modified overlap (verified via `grep`+sort+uniq)
- ✅ TDD discipline (Phase 3+): every code-bearing task splits into red-then-green commits
  per CLAUDE.md §Conventions; Plan 12 closure uses Note 4 single-commit doc exemption
- ✅ Performance Discipline (Phase 6+): Plan 12 ships criterion microbench under
  `crates/beava-bench/benches/cli_dispatch.rs`
- ✅ End-to-end throughput regression contract (Phase 8+): Plan 12 task 12.b runs 8-cell
  rebaseline appending rows to `.planning/throughput-baselines.md`
- ✅ mio-only Hot-Path Invariant (Phase 12.6): no plan adds new caller of
  `apply_event_to_aggregations` or new `axum::*` symbol outside `http_admin.rs`
- ✅ Events-Only Invariant (Phase 12.7) with ADR-001 partial overturn: `@bv.table` ships
  as aggregation-output decorator only; no `app.upsert / app.delete / app.retract`;
  no `event_time` / `tolerate_delay` / `event_time_field` accepted
- ✅ ADR-001 (@bv.table partial overturn): Plan 03
- ✅ ADR-002 (Polars op renames): Plan 04 + deprecation aliases
- ✅ ADR-003 (global aggregation + bv.lit): Plan 03 (DSL surface) + Plan 11 (acceptance
  tests green via real engine)
- ✅ D-05 cross-amendment from 13.4 D-03: `bv.App(test_mode=True)` Plan 02 + fixture
  default Plan 07
- ✅ Every plan has `<read_first>`, `<action>`, `<verify>`, `<done>` per task
- ✅ Concrete values in actions (no "align X with Y" without specifying)
- ✅ Goal-backward must_haves with truths/artifacts/key_links per plan

No BLOCKERs surfaced.
