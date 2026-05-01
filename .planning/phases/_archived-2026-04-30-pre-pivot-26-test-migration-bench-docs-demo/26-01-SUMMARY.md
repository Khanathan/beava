---
phase: 26-test-migration-bench-docs-demo
plan: 01
subsystem: test-migration
tags: [test-migration, api-cleanup, v0-closeout]
dependency-graph:
  requires: []
  provides:
    - "zero-old-api-references-in-scoped-tree"
    - "all-three-test-suites-green"
    - "test-count-floor-exceeded-744"
  affects:
    - "docs/*"
    - "benchmark/*"
    - "python/tests/*"
    - "tests/integration/*"
tech-stack:
  added: []
  patterns:
    - "v0 @tl.stream / @tl.table decorator surface everywhere in public docs + scripts"
    - "skip-module with reason='port in 26-03' for replay CLI tests pending 26-03"
key-files:
  created:
    - ".planning/phases/26-test-migration-bench-docs-demo/26-01-INVENTORY.md"
    - ".planning/phases/26-test-migration-bench-docs-demo/26-01-SUMMARY.md"
  modified:
    - "README.md"
    - "launch/reddit-posts.md"
    - "docs/index.md"
    - "docs/quickstart.md"
    - "docs/comparison.md"
    - "docs/operators.md"
    - "docs/python-sdk.md"
    - "docs/blog/streaming-shouldnt-require-a-platform-team.md"
    - "benchmark/tally-throughput/RESULTS.md"
    - "benchmark/fraud-pipeline/bench_fraud.py"
    - "demo.py"
    - "scripts/demo-recording.sh"
    - "tests/integration/test_replay_30d.py"
  deleted:
    - "python/tests/test_app.py"
    - "python/tests/test_integration.py"
decisions:
  - "Scoped the old-API grep assertion to `python/ tests/ benchmark/ docs/` (plan must-have phrasing); .claude/skills/tally/SKILL.md is out-of-scope."
  - "Blog snippet minimally ported; full narrative rewrite deferred to plan 26-03 as scoped."
  - "Replay CLI integration tests skipped with reason='port in 26-03' pending 26-03 CLI port."
  - "test_app.py / test_integration.py deleted; coverage subsumed by test_v0_* suites (register roundtrip, joins, stream-table, watermark, get-multi, push-table e2e)."
metrics:
  duration: "~2h (session continuation; most work pre-committed)"
  completed: "2026-04-14"
  test_count: 1628
---

# Phase 26 Plan 01: Test migration + old API deletion Summary

One-liner: Eliminated every `@tl.source` / `@tl.dataset` / `EventSet` / `FeatureSet` reference from `python/ tests/ benchmark/ docs/`, deleted two module-skipped pre-v0 test files (coverage already absorbed by v0 suites), skipped three replay CLI integration tests pending 26-03, and confirmed all three test runners (cargo / pytest python / pytest integration) green well above the 744-test floor.

## What shipped

- **Grep assertion met.** `rg -n "@tl\.(source|dataset)|EventSet|FeatureSet"` scoped to `python/ tests/ benchmark/ docs/` (excluding `.planning/`, `target/`, `__pycache__/`) returns **zero** hits. Before: 17 files / 115 occurrences. After: 0.
- **`_dataframe` grep** under `python/` returns **zero** hits (was already clean on entry per inventory).
- **Pre-v0 skipped tests removed:** `python/tests/test_app.py` and `python/tests/test_integration.py` (both module-`pytest.skip`'d, v0-migrated). Coverage absorbed by `test_v0_register_roundtrip.py`, `test_v0_joins_e2e.py`, `test_v0_stream_table_join.py`, `test_push_table_e2e.py`, `test_get_multi_e2e.py`, `test_watermark_e2e.py`, `test_client.py`, `test_protocol.py`.
- **Replay CLI integration (3 tests)** marked `@pytest.mark.skip(reason="port in 26-03")` at module level via `tests/integration/test_replay_30d.py`; 26-03 owns un-skip.
- **Docs ported:** `README.md`, `launch/reddit-posts.md`, `docs/index.md`, `docs/quickstart.md`, `docs/comparison.md`, `docs/operators.md` (16 operator reference examples), `docs/python-sdk.md` (30+ snippets + narrative rename of "Sources" -> "Streams"), blog (minimal 4-snippet port with `<!-- TODO(26-03): full rewrite -->` marker).
- **Benchmark + scripts ported:** `benchmark/fraud-pipeline/bench_fraud.py`, `benchmark/tally-throughput/RESULTS.md` (historical narrative rewrite only), `demo.py`, `scripts/demo-recording.sh`.

## Before / after counts

### Old-API grep (scoped: `python/ tests/ benchmark/ docs/`)

| State | Files with hits | Total occurrences |
|-------|-----------------|-------------------|
| Before (from inventory 2026-04-12) | 17 | 115 |
| After  | **0** | **0** |

### Skipped-test triage

| Disposition | Count |
|-------------|-------|
| PORT (file rewritten to v0 API) | 14 |
| DELETE (obsolete; coverage absorbed by v0 suites) | 2 — `test_app.py`, `test_integration.py` |
| UNSKIP (feature now live, skip marker removed) | 0 — no v0-migrated per-function skips remained on entry |
| KEEP SKIPPED (pytest) | 0 |
| KEEP IGNORED (rust `#[ignore]`) | 5 — all in `tests/bench_hybrid_ops.rs`; bare-metal perf benches, already excluded from `cargo test` |
| SKIP-FOR-26-03 | 3 — `tests/integration/test_replay_30d.py` module-level skip pending 26-03 CLI port |
| DEFER-26-03 (minimal port; full rewrite in 26-03) | 1 — `docs/blog/streaming-shouldnt-require-a-platform-team.md` |

## Deleted files (rationale)

- **`python/tests/test_app.py`** — module-level `pytest.skip(reason="v0 SDK rewrite - Phase 26 will port this against the new @tl.stream / @tl.table API")`. Register-topology assertions were tied to the removed class-decorator surface (`@source` / `@dataset` / `group_by` attribute form). Equivalent register-roundtrip coverage now lives in `test_v0_register_roundtrip.py` (REGISTER JSON v0 validation), and protocol-framing coverage in `test_client.py` + `test_protocol.py` + `test_push_table_e2e.py` + `test_get_multi_e2e.py`.
- **`python/tests/test_integration.py`** — module-level skip with the same reason. End-to-end event-push / feature-read coverage is subsumed by `test_push_table_e2e.py`, `test_get_multi_e2e.py`, `test_v0_joins_e2e.py`, `test_v0_register_roundtrip.py`, `test_watermark_e2e.py`, `test_v0_stream_table_join.py`.

## Tests kept skipped (with tracking references)

- `tests/integration/test_replay_30d.py::*` (3 tests) — module-level skip with `reason="port in 26-03"`. Tracked in plan **26-03** (Phase 20 traction demo rebuild). Replay CLI at `benchmark/replay/replay_30d.py` still imports `dataset, group_by, source` from `tally`; 26-03 owns the port.
- `tests/bench_hybrid_ops.rs` (5 `#[ignore]` tests) — intentional bare-metal perf benches from Phase 22-04; `cargo test` already excludes them; not v0-migration gated. Retained as-is.

## Final test counts

| Runner | Passed | Failed | Skipped / Ignored |
|--------|-------:|-------:|------------------:|
| `cargo test --workspace` | **1170** | 0 | 5 ignored |
| `pytest python/tests/` | **451** | 0 | 0 |
| `pytest tests/integration/` | **7** | 0 | 1 module (3 tests) |

**Total green runnable: 1628** — well above the 744-test floor (+884 headroom; +200 from v0 additions as projected in 26-CONTEXT.md plus the full Phase 24/25 integration corpus).

## Deviations from Plan

### Auto-fixed Issues

None — the plan was executed as written; the only substantive deviation is a scoping clarification below.

### Scope clarification (not a fix)

- **`.claude/skills/tally/SKILL.md`** retains 2 `@tl.source` / `@tl.dataset` occurrences at lines 127 and 132. The inventory originally listed this file PORT; however, the plan's must-have truth statement explicitly scopes the grep assertion to `python/ tests/ benchmark/ docs/` (not `.claude/`). The file is a Claude Code skill template, not part of the Tally public surface, and runtime policy currently blocks edits to it from this agent. Documented here so 26-04 sign-off knows to either update the skill template via the correct channel or re-confirm the out-of-scope decision.
- Grep re-run scoped per plan must-have returned **zero** hits — success criterion #1 satisfied as specified.

## Known Stubs

None.

## Threat Flags

None.

## Notes for downstream plans

- **26-02 (benchmark gate):** suite is clean — ready to run 9-cell matrix on v0 engine. `bench_v0.py` imports were scrubbed as part of 26-01.
- **26-03 (blog + demo rebuild):** `benchmark/replay/replay_30d.py` still imports `dataset, group_by, source` from `tally`; un-skipping the three `test_replay_30d.py` tests is the acceptance gate. Blog has `<!-- TODO(26-03): full rewrite -->` marker inside the code fence at the launch blog; replace the whole narrative in 26-03.
- **26-04 (sign-off):** decide whether `.claude/skills/tally/SKILL.md` needs updating via the skill-template channel or stays as documented-out-of-scope.

## Self-Check: PASSED

- Created file: `.planning/phases/26-test-migration-bench-docs-demo/26-01-INVENTORY.md` — FOUND
- Created file: `.planning/phases/26-test-migration-bench-docs-demo/26-01-SUMMARY.md` — FOUND (this file)
- Task commits: f00e6cf, d92ef2b, b77f33c, 79b744f, da444ce, 7b87b96, 33934c3 — all FOUND in `git log`
- Scoped grep (`python/ tests/ benchmark/ docs/`) for `@tl\.(source|dataset)|EventSet|FeatureSet` — zero hits
- `cargo test --workspace` — 1170 pass / 0 fail
- `pytest python/tests/` — 451 pass / 0 fail
- `pytest tests/integration/` — 7 pass / 1 module skipped (scoped to 26-03)
- Total green: 1628 >= 744 floor
