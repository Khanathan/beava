---
phase: 55
slug: stream-table-cascade-crossshard-and-source-tables
status: human_needed
engineering_complete: true
verified: 2026-04-20
perf_gate_commit: 9a1a78b
ship_gate_commit: pending-close-commit
baseline_phase: 54
baseline_eps: 1339446
candidate_eps: 1246190
gate_floor_eps: 1138529
gate_result: PASSED
---

# Phase 55 Verification — stream-table-cascade-crossshard-and-source-tables

**Phase:** 55-stream-table-cascade-crossshard-and-source-tables
**Status:** `human_needed` (SC-6 boot rematerialization cross-shard fan-out at N>1 deferred to 55-NEXT #1 per user decision)
**Engineering close:** 2026-04-20
**Perf gate:** `9a1a78b` (`perf(55-W4): run perf gate + commit 55-PERF-GATE.md + source_lsn verify script`)
**Ship gate + close:** see final commit hash in 55-04-SUMMARY.md

## Per-Success-Criterion Status

| SC | Requirement      | Evidence                                                                                                   | Status         |
|----|------------------|------------------------------------------------------------------------------------------------------------|----------------|
| 1  | TPC-CORR-07      | `cargo test --release --test cross_shard_tt_cascade_ownership` 2/2 passed                                  | passed         |
| 1  | TPC-CORR-07      | `cargo test --release --test sharding_parity tt_cascade` 2/2 proptests passed                              | passed         |
| 2  | TPC-SOURCE-01    | `cargo test --release --test source_table_cdc -- --ignored --test-threads=1` 7/7 passed (4 SC-2 tests)     | passed         |
| 2  | TPC-SOURCE-01    | `pytest python/tests/test_source_table_decorator.py` 3/3 passed                                            | passed         |
| 3  | TPC-SOURCE-01    | `cargo test --release --test source_table_cdc -- --ignored --test-threads=1` — 3 SC-3 tests in the 7/7     | passed         |
| 4  | TPC-CORR-07      | `cargo test --release --test cross_shard_backpressure` 1/1 passed                                          | passed         |
| 4  | TPC-CORR-07      | `cargo test --release --test cross_shard_cascade_recovery` 1/1 passed                                      | passed         |
| 5  | TPC-CORR-07      | `cargo test --release --test cascade_metrics` 2/2 passed                                                   | passed         |
| 6  | TPC-CORR-07      | `cargo test --release --test boot_rematerialization` 5/5 passed (v8→v9 boot + truncation + v9-reject + inmem + helper) | human_needed (N=1 automated; N>1 fan-out deferred) |
| 7  | TPC-PERSIST-05A  | See `55-PERF-GATE.md`: **1,246,190 EPS** ≥ 1,138,529 (gate floor) — 9.5% headroom; −7.0% vs 1,339,446 baseline | passed         |

## Requirements Coverage

- **TPC-CORR-07** (Stream-Table downstream on `hash(output_key) % N`): **closed (engineering)** — verified by SC-1, SC-4, SC-5, SC-7; SC-6 `human_needed` at N>1 boot migration (see note below).
- **TPC-SOURCE-01** (source-table SDK + wire with `source_lsn` echo): **closed** — verified by SC-2, SC-3.

## SC-6 Note — Deferred Cross-Shard Fan-out at Boot Rematerialization

**Status:** `human_needed`.

**Passed automated at N=1:** `tests/boot_rematerialization.rs` — 5/5 passed
(`v8_snapshot_boots_and_rematerializes_to_v9`,
`truncated_event_log_hard_fails_with_actionable_error`,
`v8_server_rejects_v9_snapshot`,
`state_inmem_build_skips_rematerialization`,
`pipeline_engine_rematerialize_helpers_exist`).

**Deferred:** Cross-shard fan-out inside `SyncCascadeTargets::dispatch_batch`.
Wave 3 (Plan 55-03 Task 2) landed `SyncCascadeTargets` as the `CascadeTarget`
trait impl for boot-time replay, but the same-shard fast path is used
(`sibling_shards=None` in `push_with_cascade_on_shard`). At **N=1** this is
fully correct (trivially — a single shard is always the correct owner). At
**N>1** a v8→v9 boot migration would place downstream rows on the source's
shard rather than `hash(output_key) % N`.

**Operational impact:** Pre-Phase-55 installations are overwhelmingly
single-shard (cross-shard was never fully correct there — TPC-CORR-07 is
*new* in Phase 55). A multi-shard v8→v9 migration would need either (a) the
fix landed under 55-NEXT #1, or (b) a `tally rebuild --from-source` pass.

**55-NEXT #1 (filed):** Thread `CascadeBuffer` + `SyncCascadeTargets` through
`push_with_cascade_on_shard` / `cascade_table_upsert_on_shard_buffered` so
boot replay at N>1 routes each replayed event's cross-shard output through
the same coalesce-and-dispatch path as live ingest. Estimate: ~80 LOC +
a two-shard unit test. The trait seam is already in place.

**Human verification options:**
- (a) Confirm N=1 boot rematerialization against a real pre-55 snapshot
  before any multi-shard migration occurs, **OR**
- (b) Accept 55-NEXT #1 as the remediation for N>1 upgrades (the narrow,
  one-shot migration path) and ship Phase 55 engineering-complete today.

## Ship-Gate Tests

| Gate                                                       | Result  |
|-----------------------------------------------------------|---------|
| `scripts/verify-source-lsn-echoed.sh`                     | exit 0  |
| `cargo test --release --test cascade_ship_gate phase_55_grep_gates_pass` | passed |
| `grep -rl '#\[ignore = "55-W[0-9]"\]' tests/ \| wc -l`     | 0       |

The ship-gate `phase_55_grep_gates_pass` enforces four invariants on every
default test run:

1. Zero files in `tests/` carry a Phase 55 wave-scoped ignore attribute
   (W0–W4 all flipped).
2. `scripts/verify-source-lsn-echoed.sh` exits 0 (source_lsn echoed on every
   source-table ack path; 5 cascade metrics emit in `src/`).
3. All five Phase 55 cascade metric name-literals emit under `src/`
   (`beava_cascade_cross_shard_total`, `beava_cascade_intra_shard_total`,
   `beava_cascade_queue_depth`, `beava_cascade_lag_seconds`,
   `beava_shard_inbox_high_watermark_total`).
4. All four Phase 55 TCP source-table opcodes pinned in
   `src/server/protocol.rs` (`OP_UPSERT_TABLE_ROW`, `OP_DELETE_TABLE_ROW`,
   `OP_UPSERT_TABLE_BATCH`, `OP_DELETE_TABLE_BATCH`).

## Metrics Exposed

`/metrics` now exposes the five Phase 55 cascade series (D-D4 + ROADMAP-locked):

- `beava_cascade_cross_shard_total{source, target}` — Counter
- `beava_cascade_intra_shard_total{shard}` — Counter
- `beava_cascade_queue_depth{source, target}` — Gauge
- `beava_cascade_lag_seconds{source, target}` — Histogram
- `beava_shard_inbox_high_watermark_total{shard}` — Counter (fires at 75% fill)

Pre-existing `beava_shard_inbox_full_total{shard}` continues to fire on
backpressure (TrySendError::Full) per Phase 54 Wave 2.

## Test Baselines

| Suite                                       | Count                                 |
|---------------------------------------------|---------------------------------------|
| `cargo test --release --lib` (default/fjall)| 796 passed / 0 failed / 35 ignored    |
| `cargo test --release --lib --features state-inmem` | 800 passed / 0 failed / 35 ignored |
| Phase 55 W1 suite (9 tests, all flipped)    | 9 passed                              |
| Phase 55 W2 suite (7 Rust + 3 Python)       | 7 + 3 = 10 passed (Rust serial)       |
| Phase 55 W3 suite (5 tests including bonus) | 5 passed                              |
| Phase 55 W4 ship gate                       | 1 passed                              |

**Delta vs Phase 54 close:** +6 new lib unit tests (Plan 55-03 snapshot shim
tests), +16 Phase 55 integration tests (W0 landed the scaffolding; W1–W4
flipped GREEN). No lib regressions. No Phase 54 regressions (scanned).

## Known Pre-existing Issues (out of scope)

| Test file / suite             | Failures | Origin                         | Disposition                                                                    |
|-------------------------------|----------|--------------------------------|--------------------------------------------------------------------------------|
| `tests/test_concurrent.rs`    | 6/6      | Pre-dates Phase 55 (54-NEXT #4)| Tracked in `deferred-items.md`; out of scope per executor scope-boundary rule |

See `deferred-items.md` for the full list.

## Perf Gate Evidence

Committed at `9a1a78b`:

- Candidate EPS: **1,246,190** over 60 s measurement window
  (`MODE=complex DURATION=60 CPUS=8 CLIENTS=8 BEAVA_SHARD_INBOX_SIZE=1048576`)
- Floor: **1,138,529 EPS** (85% of Phase 54 Wave 5 baseline 1,339,446 EPS)
- Headroom: +107,661 EPS (+9.5%)
- Gate result: **PASSED**

Full evidence + per-client breakdown + raw stdout in
`.planning/phases/55-stream-table-cascade-crossshard-and-source-tables/55-PERF-GATE.md`
and `perf-evidence/20260420T220619Z.txt`.

## Manual-only verifications (operator-run; not blocking engineering close)

| Behavior                                               | Requirement     | Why manual                                                | Instructions                                                                                                                              |
|--------------------------------------------------------|-----------------|-----------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------|
| Debezium-style CDC connector end-to-end replication    | TPC-SOURCE-01   | Requires external Postgres + Debezium runtime             | Operator spins up Postgres + Debezium → `POST /table/{name}`; verifies `source_lsn` echo + resume-from-LSN semantics; logs to 55-VERIFICATION. |
| Operational soak at Phase 55 HEAD                      | TPC-PERSIST-04  | Operator-run 8h Hetzner CCX43 soak                        | Re-runs `scripts/soak-hetzner-ccx43.sh` at Phase 55 HEAD; commits `soak-evidence/<ts>.json`.                                              |
| N=1 boot rematerialization against a real pre-55 snapshot | TPC-CORR-07  | Requires a pre-55 snapshot on disk                        | Copy a real pre-55 base snapshot into a fresh data dir; start Phase 55 binary; verify `Pre-v9 snapshot detected` log + downstream rows correct. |

## Deferred to 55-NEXT

Full list in `deferred-items.md`. Top 5:

1. **55-NEXT #1** — Cross-shard fan-out in boot rematerialization (SC-6 at N>1).
2. **55-NEXT #2** — Triage pre-existing `tests/test_concurrent.rs` 6/6 failures.
3. **55-NEXT #3** — Counter hoist (Phase 61 territory).
4. **55-NEXT #7** — ROADMAP.md Phase 55 SC #7 EPS cosmetic fix (935,000 → 1,138,529).
5. **55-NEXT #8** — Graceful `bench.py` client shutdown at EOS (cosmetic).

## Acceptance for phase close

All criteria except SC-6 at N>1 are automatically verifiable and GREEN. SC-6
at N=1 is automated-green (5/5 in `boot_rematerialization.rs`); SC-6 at N>1
is deferred to 55-NEXT #1 per the locked user decision to avoid scope creep
at Wave 3 close. The trait seam is in place; the remediation is well-scoped.

**Phase 55 is engineering-complete. TPC-CORR-07 and TPC-SOURCE-01 are both
closed. The cross-shard boot-rematerialization path at N>1 awaits the
55-NEXT #1 ticket or a human verify-path-A decision, consistent with the
Phase 54 `human_needed` precedent for TPC-PERSIST-04.**
