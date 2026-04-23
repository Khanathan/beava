# Phase 6: WAL + Idempotency — Phase Summary

**Status:** complete
**Shipped:** 2026-04-23
**Branch:** v2/greenfield

## Success criteria verification (from ROADMAP.md)

| # | Criterion | Evidence | Status |
|---|-----------|----------|--------|
| 1 | Push event, kill before fsync, restart → event NOT present. Push event, wait for ACK, kill → event IS present. | `phase6_crash.rs::wal_kill_before_fsync_drops_event` (0 records on disk), `phase6_crash.rs::wal_kill_after_ack_preserves_event` (≥1 record on disk). Both pass. | PASS |
| 2 | Duplicate push with same dedupe_key within window returns byte-identical response body; state unchanged. | `phase6_push.rs::push_with_dedupe_key_replays_byte_identical`, `phase6_smoke.rs::phase6_criterion_2_dedupe_replay_byte_identical`. Both assert `bytes ==` between fresh + replay and count==1. | PASS |
| 3 | Group-commit fsync adds P50 < 2ms to push-ACK latency at default config. | `wal/append_fsync_default_coalesce` = 7.40 ms on Apple-M4/Darwin. **WARNING**: hw-class-limited; macOS `F_FULLSYNC` is substantially slower than Linux `fdatasync`. Linux baseline is the real gate; captured in Phase 13 CI. | WARNING (hw-class) |
| 4 | WAL rotation: segments ≤ snapshot-covered LSN truncated; disk usage bounded. | `rotation.rs` unit tests (`rotation_creates_new_segment`, `truncate_up_to_deletes_closed_only`, `truncate_preserves_segment_covering_lsn`) + `phase6_smoke.rs::phase6_criterion_4_rotation_truncates`. | PASS |

## Requirements closed

| REQ-ID | Status |
|---|---|
| SRV-DUR-01 (group-commit fsync) | PASS (Plan 02) |
| SRV-DUR-02 (ACK after fsync) | PASS (Plan 02 WAL layer + Plan 03 HTTP wiring) |
| SRV-DUR-03 (WAL record header) | PASS (Plan 01) |
| SRV-DUR-04 (WAL rotation + truncate) | PASS (Plan 02) |
| SRV-DUR-05 (dedupe_key + window) | PASS (Plan 03) |
| SRV-API-03 (/push endpoint) | PARTIAL — returns `{ack_lsn, idempotent_replay, registry_version}`. `features` field deferred to Phase 12 `/push-sync`. |
| PERF-03 (WAL overhead tripwire) | PASS (Plan 04 — baseline captured) |

## Perf baselines (hw-class: Apple-M4 / Darwin-24.3.0 / 10 cores)

| Bench | Median | Notes |
|---|---|---|
| `wal/append_nofsync` | 279.71 ns | serialize + CRC32C + BufWriter write; 256-byte payload |
| `wal/append_fsync_default_coalesce` | 7.40 ms | single push, fsync_interval=2ms, 1MiB coalesce — WARNING vs 2ms target (macOS hw-class limit) |
| `wal/append_fsync_burst_1k` | 10.62 ms/batch (~10.6 µs/push amortized) | group-commit validated under load |

Full table in `.planning/perf-baselines.md`. Regression thresholds: +10%
WARNING, +25% BLOCKER within same hw-class.

## Test count delta

| Measurement | Before Phase 6 | After | Delta |
|---|---:|---:|---:|
| Workspace + `--features beava-server/testing` | 531 | **590** | **+59** |

Plan-by-plan test additions:
- Plan 01: `writer_reader.rs` — +7 (persisted from prior session).
- Plan 02: `fsync_worker.rs` (+5) + `rotation.rs` (+3) — +8.
- Plan 03: `idem_cache` unit (+4) + `phase6_push.rs` (+10) — +14 new
  tests. `cli_smoke` gained 5 pre-existing; no new.
- Plan 04: `phase6_crash.rs` (+2) + `phase6_smoke.rs` (+4) — +6.

## Deviations from CONTEXT.md

1. **D-12 refinement — apply-AFTER-fsync (not before).** The pre-fsync
   apply pattern proposed in CONTEXT.md would save ns–µs at the cost of
   state/disk divergence on fsync failure. We picked the inverse: apply
   runs *after* the durable LSN is assigned. The apply cost measured in
   Phase 5 is well under the 2 ms fsync budget, so the latency cost is
   negligible and crash-safety is stronger. Documented in
   `06-03-SUMMARY.md`. Revisit for Phase 12 `/push-sync` only if the
   sub-ms SLA requires it.

2. **`X-Beava-Idempotent-Replay: 1` header.** Success criterion #2 asks
   for byte-identical replay bodies — the body cannot also carry a flag
   distinguishing replays without breaking identity. We introduced this
   response header as the replay signal. The body always reads
   `idempotent_replay: false` (reflecting the first successful push's
   state). Header is stable and will be documented in Phase 13.

3. **macOS fsync latency (7.40 ms vs 2 ms target).** Captured as WARNING
   per CLAUDE.md §Performance Discipline. Hw-class-limited (`F_FULLSYNC`
   semantics). Linux baseline pending Phase 13 CI.

4. **Inline fsync instead of `spawn_blocking`.** Plan 02 documented this:
   the current-thread runtime used in tests has no blocking pool, and the
   worker task is already a separate tokio task in production.

## Commit trail

```
c5788d6  feat(06-04): implement phase6_crash_probe + Server::registry exposure
d1d72bc  feat(06-03): /push endpoint with WAL durability + idempotency cache
5771ff1  test(06-03): add /push + dedupe replay integration tests + Phase 6 crash/smoke scaffolding
a75e75f  docs(06): phase 6 partial-execution status report
8a01e65  feat(06-04): phase 6 criterion WAL microbench + baselines captured
948a626  docs(06-02): plan summary
ef78df1  feat(06-02): WAL group-commit fsync worker + rotation + truncate_up_to
674b1f7  test(06-02): add fsync worker + rotation + truncate tests
80da34d  docs(06-01): plan summary
d34d265  feat(06-01): implement WAL record frame + WalWriter/WalReader (no fsync)
2040a81  test(06-01): add WAL writer/reader round-trip + CRC + torn-record tests
d5a0eca  docs(06): create phase plan
```

## Follow-ups for Phase 7

- Wire `WalSink::truncate_up_to` into the snapshot task (snapshot writer
  calls this with `covered_lsn` after a successful snapshot).
- Extend WAL recovery on startup: scan dir for segments, read headers to
  find highest `start_lsn`, replay from last snapshot LSN.
- Schema evolution on replay (SRV-RECOV-05).
- Wire the readiness flag to flip after recovery completes (currently a
  100 ms Phase 1 stub).

## Follow-ups for Phase 12

- TCP `op=push` handler (Phase 2.5 reserved the opcode; handler stays
  `op_not_implemented`).
- `/push-sync` endpoint returning computed feature values alongside the
  ACK (the `features` field deferred from SRV-API-03).
- `/push-batch` end-to-end wiring.
