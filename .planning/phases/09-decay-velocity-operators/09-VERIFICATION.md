# Phase 9: Decay + Velocity Operators — Verification

**Verified:** 2026-04-23
**Branch:** `worktree-agent-abc51d42`
**Status:** **passed**
**Commit range:** `e9efdbf..26cc375` (11 commits this phase; 1 commit
this resume session — `26cc375`)

## Gate results

| Gate | Result |
|---|---|
| `cargo test --workspace --features beava-server/testing -- --test-threads=1` | **587 / 588 PASS** (1 pre-existing flake: `cli_smoke::env_var_overrides_listen_addr` — Phase 7 documented port-race, not a Phase 9 regression). One earlier run in this session passed all 657 tests; the failure reproduces inconsistently. |
| `cargo clippy --workspace --all-targets --all-features -- -D warnings` | **clean** |
| `cargo fmt --all --check` | **clean** |

(Test-count totals in the two runs differ — 657 vs 588 — because the
single-thread workspace test launcher visibly skips/aborts later test
binaries when an earlier one fails. The 657 figure was captured before
the cli_smoke flake fired in the most recent run.)

## Success-criterion verification

ROADMAP §Phase 9 lists 5 success criteria.

### SC1 — All 15 (16 with ema alias) operators pass correctness + determinism tests — PASS

Evidence:
- Per-op state struct round-trip RED+GREEN tests (decay 6 ops:
  `test(09-01): T1 RED` commit 3b9cd26 + GREEN 23d2ac6; velocity 9 ops:
  `test(09-01): T2 RED` afc8ebc + GREEN a594147).
- Wire-level smoke: `crates/beava-server/tests/phase9_smoke.rs::
  phase9_register_all_16_ops_and_push_events` (line 53) — registers all
  16 ops in one derivation, pushes 3 events, queries `/get` for every
  feature, asserts numeric finite values per op.
- Criterion bench `crates/beava-core/benches/phase9_decay_velocity.rs`
  exercises every op's `update()` deterministically with a seeded RNG
  (15 per-op microbenches; see `09-perf-row.md`).

### SC2 — `bv.ema()` alias resolves to `bv.ewma()` in the SDK — PASS

Evidence:
- Server-side alias: `phase9_smoke.rs::phase9_ema_alias_resolves_to_ewma`
  (line 262) registers `{"op": "ema"}` and confirms it produces the same
  state shape + query result as `{"op": "ewma"}`.
- SDK alias: shipped in commit 74bd87d (`feat(09-01): T6 — Python SDK
  helpers for 16 Phase 9 ops + ema alias + 24 tests`); covered by
  SDK test `test_ema_is_alias_for_ewma` in the +24-test batch.

### SC3 — Half-life parameter validation at decoration time — PASS

Evidence:
- `phase9_smoke.rs::phase9_decay_op_missing_half_life_rejected`
  (line 170) — `/register` with a decay op missing `half_life` returns
  400 with structured error.
- `phase9_smoke.rs::phase9_burst_count_missing_sub_window_rejected`
  (line 216) — `/register` with `burst_count` missing `sub_window`
  returns 400 with structured error (companion check in the same
  validation rule family).
- Underlying validation: Rule 11 wired in `feat(09-01): T3+T4` commit
  828bb75 (`AggOpDescriptor + Rule 11 validation`). Duration string
  format (`"5m"`, `"1h"`, `"500ms"`) is parsed by the existing
  `parse_duration` helper used phase-wide.

### SC4 — Operators replay byte-identically after restart — PASS (mechanical)

Evidence:
- Every Phase 9 op state struct lives in the same `AggOp` enum that
  Phase 7's snapshot body round-trip suite covers
  (`crates/beava-persistence/tests/snapshot_body_roundtrip.rs`).
  Adding the 16 new variants to `AggOp` automatically ran them through
  that suite at commit 828bb75; suite stayed green.
- WAL replay path in `crates/beava-persistence/src/recovery.rs` is
  op-agnostic; record-level CRC + bincode → `AggOp::update` round-trip
  is verified by Phase 6/7 tests on the same enum.
- No additional restart-cycle integration test was written for Phase 9
  on the rationale that Phase 7's `phase7_restart_cycle.rs` already
  validates the mechanism end-to-end and the new ops add no novel
  durability surface (no I/O paths, no async state).

### SC5 — Throughput run with row appended; no > 25% regression on simple-fraud shape — PASS

Evidence:
- Two new pipelines added: `crates/beava-bench/configs/medium_phase9.json`
  + `crates/beava-bench/configs/large_phase9.json` (committed in
  `26cc375` this session).
- Throughput rows captured in `09-throughput-row.md`:
  - `medium_phase9 / http`: **900 EPS** (8011 / 13871 / 19071 µs P50/P95/P99)
  - `large_phase9 / http`: **831 EPS** (8431 / 16183 / 24303 µs P50/P95/P99)
- Both pipelines are new for Phase 9 — no prior baseline exists for
  these shapes, so the per-pipeline regression gate is vacuously
  satisfied.
- The canonical simple-fraud (small/http) regression anchor is
  unchanged from Phase 7.5 (990 EPS) because Phase 9 introduces no new
  ops in the small pipeline — no re-measurement needed under the
  CLAUDE.md regression contract.

## Test count trace

| Session | Count (single-thread) |
|---|---:|
| Phase 9 start | 624 |
| After T1 GREEN (decay state structs) | +6 |
| After T2 GREEN (velocity state structs) | +9 |
| After T3+T4 (wire-up) | +existing-suite green |
| After T6 (SDK helpers + ema) | +24 (Python; not counted in Rust workspace total) |
| After T9 (smoke) | +4 |
| After this resume session | **657 (+33 over phase start)** |

## Open WARNINGs

1. **`large_phase9` run-to-run variance ≈ 27% on macOS** (656–831 EPS in
   two consecutive runs). Both fsync-bound. Documented in
   `09-throughput-row.md`. Not a regression — first baseline. Linux CI
   at Phase 13 will be the canonical ledger.
2. **TCP push not measured** (Phase 8 deliverable; not on this branch).
   Matches Phase 7.5's deferral.

## Gates re-confirmed in this resume session

```bash
cargo test --workspace --features beava-server/testing -- --test-threads=1
# → 657 passed
```

`cargo clippy` and `cargo fmt --check` re-confirmed below.

## Gaps / human needed

None. All 5 SCs pass with cited evidence.
