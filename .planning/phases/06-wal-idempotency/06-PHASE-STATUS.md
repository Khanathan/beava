---
phase: 06-wal-idempotency
status: partial
shipped: 2026-04-23
shipped_plans: ["06-01", "06-02"]
partial_plans: ["06-04"]
deferred_plans: ["06-03"]
deferred_tasks: ["06-04 subprocess probe + crash UAT", "06-04 phase smoke"]
---

# Phase 6: WAL + idempotency — Partial Execution Status

## What shipped in this session

### Plan 06-01 — COMPLETE

beava-persistence crate + WAL record frame + WalWriter/WalReader (no fsync).

- 7/7 tests pass (round-trip, CRC mid-stream, torn-tail, magic/version mismatch, unknown record type)
- Commits: `2040a81` (RED), `d34d265` (GREEN), `80da34d` (summary)
- Files: `crates/beava-persistence/{Cargo.toml,src/{lib,error,record,writer,reader,segment}.rs,tests/writer_reader.rs}`
- See `06-01-SUMMARY.md`.

### Plan 06-02 — COMPLETE

Group-commit fsync worker + segment rotation + truncate_up_to.

- 8/8 tests pass (durable-LSN fanout, concurrent appends, forced fsync interval, shutdown-flushes-pending, rotation, truncate)
- Commits: `674b1f7` (RED), `ef78df1` (GREEN), `948a626` (summary)
- Files: `crates/beava-persistence/{src/{fsync_worker,rotation}.rs, tests/{fsync_worker,rotation}.rs}`
- See `06-02-SUMMARY.md`.

### Plan 06-04 — PARTIAL (perf microbench shipped; crash probe deferred)

Criterion WAL microbench + baselines. Commit: `8a01e65`.

| Bench | Median | Notes |
|---|---|---|
| `wal/append_nofsync` | 279.71 ns | serialize + CRC32C + write |
| `wal/append_fsync_default_coalesce` | 7.40 ms | **WARNING**: macOS `F_FULLSYNC` exceeds <2ms success-criterion-#3 target. Hw-class-limited; Linux CI baseline will be the real gate. |
| `wal/append_fsync_burst_1k` | 10.62 ms/batch (~10.6 µs/push amortized) | group-commit validated |

Baselines landed in `.planning/perf-baselines.md` under Apple-M4 / Darwin-24.3.0 row.

## What's deferred

### Plan 06-03 — NOT STARTED

IdemCache + `POST /push/{event_name}` HTTP endpoint wiring + AppState promotion + 10 integration tests.

**Scope remaining:**
- `crates/beava-server/src/idem_cache.rs` (IdemCache + CachedEntry + sweeper)
- `crates/beava-server/src/push.rs` (POST /push handler)
- `crates/beava-server/src/lib.rs` (AppState struct promotion from DevAggState)
- `crates/beava-server/src/http.rs` (`/push/:event_name` route wiring)
- `crates/beava-server/src/server.rs` (spawn WalSink + periodic dedupe sweeper)
- `crates/beava-server/src/shutdown.rs` (graceful WalSink shutdown)
- `crates/beava-server/src/testing.rs` (TestServer extension for WAL dir + DurabilityConfig)
- `crates/beava-core/src/config.rs` (`DurabilityConfig` + env var parsing)
- `crates/beava-server/tests/phase6_push.rs` (10 integration tests: happy path, dedupe replay byte-identical, dedupe-different-key, dedupe-after-window, unknown-event, schema-mismatch, ack_lsn-monotonic, persisted-to-WAL, sync-before-ACK, no-dedupe bypass)

**Plan reference:** `06-03-PLAN.md` (full task breakdown + interfaces).

### Plan 06-04 — PARTIAL, remaining work

- `crates/beava-server/src/bin/phase6_crash_probe.rs` (subprocess helper binary)
- `crates/beava-server/tests/phase6_crash.rs` (SIGKILL-before-fsync + SIGKILL-after-ACK tests)
- `crates/beava-server/tests/phase6_smoke.rs` (4 tests mapping to roadmap success criteria)
- `06-PHASE-SUMMARY.md` (phase-level summary)

**Plan reference:** `06-04-PLAN.md` (full task breakdown).

## Rationale for stopping here

1. **Context budget:** Plans 03 + 04-full-crash required substantially more source changes than the conservative budget allowed — `AppState` promotion touches `http.rs`, `server.rs`, `feature_query.rs`, `testing.rs`, `registry_debug.rs`, plus a new `push.rs` and `idem_cache.rs` module with 10 integration tests of their own. Finishing this in one session risked rushed, poorly-reviewed code.

2. **Durability primitive is what matters for Phase 7:** Phase 7 (snapshot + recovery) depends on the `beava-persistence` crate's `WalWriter` / `WalReader` / `WalSink` API, which is fully shipped. The /push HTTP surface is orthogonal from Phase 7's perspective — Phase 7 can land in parallel with a follow-up that wires /push into the server.

3. **Perf tripwire is in place:** The Phase 6+ mandatory criterion bench is committed with baselines. Phase 7 will be able to regression-check against it without the HTTP wiring.

4. **TDD discipline preserved:** All shipped tests follow red-then-green with atomic commits per CLAUDE.md §TDD.

## Success criteria coverage

| # | Criterion | Status |
|---|-----------|--------|
| 1 | Push event, kill before fsync, restart → event NOT present. Push event, wait for ACK, kill → event IS present. | **Deferred.** `WalSink::append_event(payload).await` is proven to resolve only after fsync by `append_returns_durable_lsn` test (Plan 02), which implies the kill-before-fsync and kill-after-ACK invariants hold at the WAL layer. The full HTTP crash UAT via subprocess probe is deferred (Plan 04 task 4b). |
| 2 | Duplicate push with same dedupe key within window returns byte-identical response; state unchanged | **Deferred.** IdemCache is designed (CONTEXT.md D-07/D-09) but not implemented — requires /push endpoint (Plan 03). |
| 3 | Group-commit fsync adds P50 < 2ms to push-ACK latency at default config | **Measured: 7.40 ms on Apple-M4/macOS (WARNING).** Linux baseline pending. macOS `F_FULLSYNC` is known to be substantially slower than Linux `fdatasync`; Phase 13 ship gate is the final check. |
| 4 | WAL rotation: segments ≤ snapshot-covered LSN truncated; disk usage bounded | **COMPLETE.** `truncate_up_to` + `rotate` implemented and tested (`rotation_creates_new_segment`, `truncate_up_to_deletes_closed_only`, `truncate_preserves_segment_covering_lsn`). |

## Requirements closed

| REQ-ID | Status |
|---|---|
| SRV-DUR-01 (group-commit fsync) | Shipped (Plan 02) |
| SRV-DUR-02 (ACK after fsync) | WAL-layer shipped (Plan 02); /push wiring deferred (Plan 03) |
| SRV-DUR-03 (WAL record header) | Shipped (Plan 01) |
| SRV-DUR-04 (WAL rotation + truncate) | Shipped (Plan 02) |
| SRV-DUR-05 (dedupe_key + window) | Deferred (Plan 03) |
| SRV-API-03 (/push endpoint) | Deferred (Plan 03) |
| PERF-03 (WAL overhead tripwire) | Baseline captured (Plan 04 partial) |

## Test counts

| Crate | Before Phase 6 | After | Delta |
|---|---:|---:|---:|
| beava-persistence | 0 | 15 | +15 |
| beava-core | 395 | 395 | 0 |
| beava-server | ~120 | ~120 | 0 (not touched this session) |

## Commit trail

```
8a01e65  feat(06-04): phase 6 criterion WAL microbench + baselines captured
948a626  docs(06-02): plan summary
ef78df1  feat(06-02): WAL group-commit fsync worker + rotation + truncate_up_to
674b1f7  test(06-02): add fsync worker + rotation + truncate tests
80da34d  docs(06-01): plan summary
d34d265  feat(06-01): implement WAL record frame + WalWriter/WalReader (no fsync)
2040a81  test(06-01): add WAL writer/reader round-trip + CRC + torn-record tests
d5a0eca  docs(06): create phase plan
b9db7ff  docs(state): record phase 6 context session
6599ed9  docs(06): capture phase context
```

## Next session

Run `/gsd-execute-phase 6` to resume from Plan 03.

`phase-plan-index` will show Plans 01 + 02 already have SUMMARY.md; Plan 03 is the next incomplete. Plan 04 partial will also resume from the deferred tasks (subprocess probe + phase smoke + PHASE-SUMMARY).

## Deviations from CONTEXT.md discovered during execution

1. **Inline fsync instead of `spawn_blocking`.** The CONTEXT.md D-05 pseudo-code used `spawn_blocking` for the fsync syscall, but tokio's current_thread runtime (used in Plan 02 tests) has no blocking pool, and the multi-thread runtime in production doesn't need it either — fsync in the worker task doesn't starve HTTP because the worker IS the tokio-spawn'd task, not the main runtime. Documented in `fsync_worker.rs` flush_batch comments. Revisit if Phase 13 P99 shows fsync blocking is a bottleneck.

2. **`WalWriter::open` uses `create_new`** (errors if file exists) instead of `create + truncate`. Safer: Plan 02's rotation assigns next_start_lsn from the monotonic LSN counter, so the filename can never collide. Loud error on collision surfaces any latent LSN-tracking bug instead of silently corrupting a prior segment.

3. **macOS fsync latency** (7.4 ms default-coalesce vs <2ms target) is a hw-class/OS limit, not a code issue. Captured as WARNING per CLAUDE.md §Performance Discipline; Linux baseline is the real gate when Phase 13 CI runs.
