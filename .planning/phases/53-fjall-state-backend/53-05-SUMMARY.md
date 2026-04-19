---
phase: 53-fjall-state-backend
plan: 05
subsystem: storage / crash-recovery + parity + metrics
tags: [fjall, crash-recovery, sigkill, proptest, parity, metrics, tdd, w-3, w-4, w-8]
dependency_graph:
  requires:
    - 53-03-SUMMARY.md (Shard::with_partition + fjall_backend helpers)
    - 53-03B-SUMMARY.md (ephemeral_test_keyspace + default-build boot plumbing)
  provides:
    - "tests/test_fjall_crash_recovery.rs::sigkill_mid_workload_restores_acked_writes (TPC-PERSIST-02 proof)"
    - "tests/proptests/sharding_parity.rs ported to fjall (TPC-PERSIST-05 part B)"
    - "src/shard/metrics.rs::record_fjall_write_bytes / record_fjall_compaction_bytes / update_fjall_fsync_latency (3 fjall helpers)"
    - "src/shard/metrics.rs::METRIC_FJALL_WRITE_BYTES / METRIC_FJALL_COMPACTION_BYTES / METRIC_FJALL_FSYNC_LATENCY_MS constants"
    - "Shard.write_bytes_since_sample + Shard::take_write_bytes() per-shard byte accumulator"
    - "shard_event_loop gauge-tick emission of beava_fjall_write_bytes_total + beava_fjall_compaction_bytes_total"
  affects:
    - 53-06-PLAN.md (operations.md: document the 3 fjall metrics; OMIT cache_hit_ratio alert per W-4 spike outcome)
requirements:
  - TPC-PERSIST-02 (CLOSED — SIGKILL test proves journal auto-replay on Keyspace::open restores last-ack'd writes)
  - TPC-PERSIST-05 (CLOSED for part B — N=1↔N=8 parity green on fjall-backed Shards)
tech_stack:
  added: []
  patterns:
    - "W-8: ephemeral port via TcpListener::bind('127.0.0.1:0') + BEAVA_TCP_PORT env var (no hardcoded port constants)"
    - "W-3: file-level #![cfg(not(feature = \"state-inmem\"))] on tests/proptests/sharding_parity.rs — run_batch uses shard.state.iter() which is fjall-only API"
    - "W-4: cache_hit_ratio gauge OMITTED entirely (spike recorded cache_stats_available: false); no placeholder 1.0 anywhere"
    - "RAII ChildGuard (Drop kills subprocess on panic) — T-53-05-01 mitigation for orphan server processes"
    - "Survival-count via post-kill keyspace reopen + partition.iter() — avoids the /features HTTP handler's dependency on the legacy StateStore"
    - "Per-shard byte accumulator (Shard.write_bytes_since_sample: u64, non-atomic, single-writer) drained every gauge tick"
key_files:
  created:
    - tests/test_fjall_crash_recovery.rs
  modified:
    - tests/proptests/mod.rs
    - tests/proptests/sharding_parity.rs
    - src/shard/metrics.rs
    - src/shard/mod.rs
    - src/shard/thread.rs
decisions:
  - "D-05-01 (survival verification independent of live server): the test does NOT call GET /features/{key} on the restarted child to assert durability — the HTTP /features handler reads from the legacy StateStore (AHashMap), not the fjall partitions, so it would not observe fjall-durable writes (known gap documented in 53-03B). Instead, the test shuts the second child down cleanly and opens the fjall keyspace directly via open_keyspace_from_env + open_shard_partition + partition.iter() from the test process, counting every entity key that survives. This is a stricter durability contract anyway ('bytes landed on disk and replay worked') and is 100% fjall-focused."
  - "D-05-02 (write_bytes tracking via per-shard accumulator, not atomic): Shard is single-writer (the shard thread owns it). A plain u64 field is faster than AtomicU64 and equally correct. The shard_event_loop calls take_write_bytes() on the same 1000-events / 100ms tick cadence as the existing gauges — no extra sampling code, no per-insert metric call."
  - "D-05-03 (compaction_bytes emits 0, not omitted): fjall 2.11 does not expose per-compaction byte counters. The plan gave two options — omit the gauge, or emit it for forward-compat. I chose EMIT-0. Rationale: the counter series is stable and visible from the first /metrics scrape, so Plan 06's alert rules can target it today and a future fjall release that adds a compaction-byte API flips the value from 0 to real without breaking the alert. Omitting would require an operator to re-configure Prometheus when fjall upgrades — worse UX."
  - "D-05-04 (fsync_latency_ms not wired in shard_event_loop): the hot-path fjall inserts use PersistMode::Buffer (default) — no sync runs per-event. The gauge is only meaningful at explicit sync-fence sites (migrate tool, admin fsyncs). The helper is public; Plan 04's migrate tool already calls keyspace.persist(SyncAll) and could wire the metric, but that is a follow-up to avoid Plan 05 scope creep. For now the gauge stays at 0 on shards that have not been explicitly sync-fenced."
  - "D-05-05 (RED+GREEN single commit for Task 1): the crash-recovery test passes on first run (the binary already honors BEAVA_TCP_PORT + BEAVA_DATA_DIR from pre-existing code paths, and fjall's journal auto-replay is already working end-to-end on the default build after Plan 03B). The plan explicitly permits a single commit in this case. Task 2 does the plan's stricter RED→GREEN split (commits 5ebf5a9 + 7660900) since the metric helpers are genuinely new."
metrics:
  duration_s: 977
  duration_human: "~16m"
  completed: 2026-04-19
  tasks_total: 2
  tasks_completed: 2
  commits: 3
  files_touched: 6
---

# Phase 53 Plan 05: SIGKILL Crash-Recovery + Proptest Parity Port + Fjall Metrics Summary

## One-liner

Prove TPC-PERSIST-02 (SIGKILL + restart → last-ack'd fjall writes survive) via a subprocess-spawn integration test on an OS-assigned ephemeral port (W-8); port the N=1↔N=8 proptest parity harness to fjall-backed Shards with a file-level `#![cfg(not(feature = "state-inmem"))]` gate (W-3); land 3 unconditional per-shard fjall metrics and deliberately omit `cache_hit_ratio` per the Plan 01 spike outcome (W-4).

---

## Commit chain

- `d065b4a` `test(53-05): fjall crash-recovery SIGKILL integration test with ephemeral port (W-8)` — Task 1 (RED+GREEN single commit; passed first run)
- `5ebf5a9` `test(53-05): RED — proptest parity port to fjall (W-3) + failing fjall-metrics unit test` — Task 2 RED
- `7660900` `feat(53-05): GREEN — fjall metrics helpers + shard-loop emission (W-4)` — Task 2 GREEN

---

## What Was Built

### 1. `tests/test_fjall_crash_recovery.rs` (NEW, 358 lines) — Task 1, TPC-PERSIST-02 proof

File-level gate: `#![cfg(unix)]` + `#![cfg(not(feature = "state-inmem"))]` — macOS/Linux only, default build only.

**Test body (`sigkill_mid_workload_restores_acked_writes`):**

1. **Ephemeral port binding (W-8 revision).** `bind_ephemeral_port()` binds `TcpListener::bind("127.0.0.1:0")`, reads `local_addr().port()`, drops the listener, returns the port. Two fresh ports picked — one for TCP, one for HTTP. NO hardcoded 7777/7787/7791/etc.
2. **Subprocess spawn 1.** Fork `beava` via `env!("CARGO_BIN_EXE_beava")` with args `["--shards", "2"]` and env:
   - `BEAVA_DATA_DIR=<tempdir>` (forces fjall keyspace to live at `tempdir/fjall/`)
   - `BEAVA_TCP_PORT=<ephemeral>` / `BEAVA_HTTP_PORT=<ephemeral>`
   - `BEAVA_FJALL_FSYNC_MS=5`
   - `BEAVA_SNAPSHOT=false` + `BEAVA_EVENT_LOG=false` (lean test)
   - `BEAVA_WORKER_THREADS=2`
   - `BEAVA_ADMIN_TOKEN` / `BEAVA_PUBLIC_MODE` explicitly unset (admin-loopback auth path — POSTs from 127.0.0.1 pass without token)
3. **Readiness probe.** `wait_for_tcp_port("127.0.0.1:{http_port}", 30s)` polls `TcpStream::connect` every 50 ms.
4. **Register + push.** Raw HTTP POST to `/pipelines` to register `Transactions` with `key_field=user_id` + a `count_1h` feature. Then 500 pushes to `/push/Transactions?sync=1` with `{"user_id": "user-N", "value": N}`.
5. **Measure qps** — `500 / push_elapsed.as_secs_f64()` — lands around 14.5k EPS on this dev box.
6. **Pre-kill fence.** Sleep 20 ms so fjall's background fsync thread (`fsync_ms=5`) ticks at least twice.
7. **SIGKILL** (`child.kill()` — plan 01 gate 3 verified this is SIGKILL on macOS), then `wait()` to reap.
8. **Subprocess spawn 2.** Fresh ephemeral ports. Same data_dir. fjall's `Keyspace::open` auto-replays the journal.
9. **Wait until HTTP port is reachable.** Proves journal replay didn't corrupt the keyspace.
10. **Clean shutdown of child 2.** `kill()` + `wait()` — releases the keyspace lock so the test process can open it.
11. **Survival count (D-05-01).** Open the keyspace via `open_keyspace_from_env` + `open_shard_partition` per shard; iterate `partition.iter()` and count surviving entities.
12. **Tolerance assertion.** `lost <= ceil(fsync_ms × qps / 1000) + 1`.
13. **Meaningful-recovery assertion.** At least 1 of the acked writes must survive (not all-or-nothing).

**Helpers:**

- `bind_ephemeral_port()` — W-8 primary path.
- `wait_for_tcp_port(addr, timeout)` — polling connect loop.
- `http_post(host, port, path, body)` — raw HTTP/1.1 POST (no hyper/reqwest dep).
- `ChildGuard` — RAII wrapper that kills + waits on drop (T-53-05-01 mitigation for orphan processes on test panic).
- `spawn_beava(dir, tcp, http, fsync, shards)` — one-shot subprocess spawn with test-hygiene env.
- `register_stream(port, name)` / `push_one(port, stream, n)` — HTTP harness.
- `count_entities_in_fjall(dir, n_shards)` — reopen fjall directly + walk partitions.

**Result:** `pushed=500 qps=14500.6 survived=500 tolerance=74 fsync_ms=5` — zero loss in practice; tolerance is the formal budget per the plan's contract.

### 2. `tests/proptests/sharding_parity.rs` (PORTED, ~530 lines) — Task 2a, TPC-PERSIST-05 part B

Re-ported from the Plan 03B-era AHashMap harness. Top-of-file `#![cfg(not(feature = "state-inmem"))]` attribute (W-3) — the file compiles only under the default (fjall) build.

**run_batch:** creates a fresh fjall keyspace per call via `common::ephemeral_test_keyspace(n_shards)` (which returns `(Arc<Keyspace>, Vec<PartitionHandle>, TempDir, FjallConfig)` from Plan 03B's `tests/common/mod.rs`), wraps each partition in `Shard::with_partition`, routes events via `shard_hint_for_event % n`, and collects the last feature map per key. `TempDir` is dropped at end of the call for isolation between proptest cases.

**run_batch_fork:** mirrors run_batch but routes events via `compute_target_shard(upstream_n=1, downstream_n=N, hint=0)` (rehash-always path from Plan 52-05). Same fjall-keyspace-per-call pattern.

**assert_parity:** identical contract to the pre-ported version — exact equality for non-HLL features, 2% relative tolerance for `distinct_*` features (T-52-07-02 mitigation).

**9 tests:**

| # | Name | Coverage |
|---|------|----------|
| 1 | `test_generator_determinism` | Same seed → same batch (proptest shrinking reproducibility, T-52-07-03) |
| 2 | `fjall_shard_state_iter_roundtrips` | NEW — W-3 compile guard + 2-entity round-trip smoke |
| 3 | `proptest_filter_parity` | Filter (where_expr) |
| 4 | `proptest_map_parity` | Map/Derive |
| 5 | `proptest_agg_count_parity` | Agg — Count |
| 6 | `proptest_agg_sum_parity` | Agg — Sum |
| 7 | `proptest_agg_hll_parity` | Agg — DistinctCount (HLL, 2% tol) |
| 8 | `proptest_join_parity` | Join (co-located key) |
| 9 | `proptest_fork_parity` | Fork/replica (N=1→N=8 rehash) |

**`tests/proptests/mod.rs`** — gate flipped from Plan 03B's interim `#[cfg(feature = "state-inmem")]` → unconditional `pub mod sharding_parity`. The file-level cfg on the harness itself keeps the state-inmem build skipping it.

### 3. Fjall metrics (W-4 revision) — Task 2b

**`src/shard/metrics.rs`:**

Three new constants + three pub fns:

```rust
pub const METRIC_FJALL_WRITE_BYTES: &str = "beava_fjall_write_bytes_total";
pub const METRIC_FJALL_COMPACTION_BYTES: &str = "beava_fjall_compaction_bytes_total";
pub const METRIC_FJALL_FSYNC_LATENCY_MS: &str = "beava_fjall_fsync_latency_ms";

pub fn record_fjall_write_bytes(shard_index: usize, bytes: u64);
pub fn record_fjall_compaction_bytes(shard_index: usize, bytes: u64);
pub fn update_fjall_fsync_latency(shard_index: usize, latency_ms: f64);
```

`register_shard_metrics` touches all three per-shard so they appear in `/metrics` from the first scrape.

**`beava_fjall_cache_hit_ratio` is DELIBERATELY OMITTED (W-4).** The Plan 01 spike recorded `cache_stats_available: false`: fjall 2.11 exposes only `Keyspace::cache_capacity()`, not hit/miss counters. Emitting a hardcoded `1.0` placeholder would make Plan 06's `< 0.8 sustained` alert vacuous. Module-level comment documents this with an explicit W-4 marker.

**`src/shard/mod.rs`:**

- `Shard.write_bytes_since_sample: u64` — per-shard non-atomic accumulator (single-writer invariant means no locking needed).
- `Shard::take_write_bytes() -> u64` — drain helper (std::mem::replace to 0).
- `with_partition` updated to initialize the field to 0.
- `StoreView::Sharded::with_entity_mut`: the post-insert path computes `byte_count = bytes.len()` and adds it to the accumulator via `shard.write_bytes_since_sample = shard.write_bytes_since_sample.saturating_add(byte_count)`.

**`src/shard/thread.rs`:**

Inside `shard_event_loop`'s existing gauge tick (`event_count % 1000 == 0 || elapsed >= 100 ms`), drain the accumulator and emit:

```rust
#[cfg(not(feature = "state-inmem"))]
{
    let bytes = shard.take_write_bytes();
    if bytes > 0 {
        crate::shard::metrics::record_fjall_write_bytes(shard_index, bytes);
    }
    crate::shard::metrics::record_fjall_compaction_bytes(shard_index, 0);
}
```

`fsync_latency_ms` is NOT updated here — the hot path uses `PersistMode::Buffer` (no per-insert sync). The helper is public so future sync-fence sites (migrate tool, admin endpoints) can call it.

---

## Verification

### Acceptance grep grid

| Check | Expected | Actual |
|-------|----------|--------|
| `test -f tests/test_fjall_crash_recovery.rs && wc -l < tests/test_fjall_crash_recovery.rs` | ≥ 140 | 358 ✓ |
| `grep -c "fn sigkill_mid_workload_restores_acked_writes" tests/test_fjall_crash_recovery.rs` | 1 | 1 ✓ |
| `grep -c "#!\[cfg(unix)\]" tests/test_fjall_crash_recovery.rs` | ≥ 1 | 1 ✓ |
| `grep -c "child.kill" tests/test_fjall_crash_recovery.rs` | ≥ 1 | 4 ✓ |
| `grep -c "BEAVA_FJALL_FSYNC_MS" tests/test_fjall_crash_recovery.rs` | ≥ 1 | 2 ✓ |
| **W-8:** `grep -c '127\.0\.0\.1:0' tests/test_fjall_crash_recovery.rs` | ≥ 1 | 3 ✓ |
| **W-8:** `grep -c "BEAVA_TCP_PORT" tests/test_fjall_crash_recovery.rs` | ≥ 1 | 2 ✓ |
| **W-8:** `! grep -E "(777[0-9]\|778[0-9]\|779[0-9])" tests/test_fjall_crash_recovery.rs` | (empty) | NONE ✓ |
| **W-3:** `head -35 tests/proptests/sharding_parity.rs \| grep -c '#!\[cfg(not(feature = "state-inmem"))\]'` | ≥ 1 | 2 ✓ |
| `grep -c "Shard::with_partition" tests/proptests/sharding_parity.rs` | ≥ 1 | 4 ✓ |
| `grep -c "Shard::new()" tests/proptests/sharding_parity.rs` | 0 | 0 ✓ |
| `grep -c "shard\.state\.iter()" tests/proptests/sharding_parity.rs` | ≥ 1 | 6 ✓ |
| `grep -c "pub mod sharding_parity" tests/proptests/mod.rs` | ≥ 1 | 1 ✓ |
| `grep -cE "pub fn (record_fjall_write_bytes\|record_fjall_compaction_bytes\|update_fjall_fsync_latency)" src/shard/metrics.rs` | 3 | 3 ✓ |
| `grep -c "beava_fjall_write_bytes_total" src/shard/metrics.rs` | ≥ 1 | 2 ✓ |
| **W-4:** `grep -c "update_fjall_cache_hit_ratio" src/shard/metrics.rs` | 0 (helper absent) | 0 ✓ |
| **W-4:** `grep -c "W-4" src/shard/metrics.rs` | ≥ 1 | 6 ✓ |
| **W-4:** `! grep -nE "cache_hit_ratio.*=.*1\.0" src/shard/thread.rs src/shard/metrics.rs` | (empty) | NONE ✓ |
| HEAD commits | RED → GREEN | d065b4a, 5ebf5a9, 7660900 ✓ |

(Note: the grep for `beava_fjall_cache_hit_ratio` itself returns 2 — both occurrences are in module comments that explicitly document the W-4 omission. No `metrics!()` call emits the series and no const defines its name as an emitted-series constant. The acceptance check's intent — that the series never lands in a Prometheus scrape — is satisfied.)

### Test runs

| Command | Result |
|---------|--------|
| `cargo test --release --test test_fjall_crash_recovery -- --nocapture` | **1/1 PASS** (pushed=500 survived=500 tolerance=74) |
| `PROPTEST_CASES=5 cargo test --release --test sharding_parity -- --test-threads=1` | **9/9 PASS** in 21.8s |
| `PROPTEST_CASES=50 cargo test --release --test sharding_parity -- --test-threads=1` | **9/9 PASS** in 216s (< 5 min budget) |
| `cargo test --release --lib shard::metrics` | **6/6 PASS** (includes new fjall_metrics_helpers_do_not_panic) |
| `cargo test --release --lib` (full library suite, default build) | **884 passed, 0 failed** |
| `cargo test --release --test shard_store_fjall` | **4/4 PASS** |
| `cargo test --release --test shard_fjall_backend` | **5/5 PASS** (Plan 03 regression guard) |
| `cargo test --release --test test_migrate_to_fjall -- --test-threads=1` | **8/8 PASS** (Plan 04 regression guard) |
| `cargo test --release --test test_reshard_fjall_aware -- --test-threads=1` | **3/3 PASS** (Plan 04 regression guard) |
| `cargo build --release --features state-inmem --tests` | green |
| `cargo test --release --features state-inmem --lib` | **888 passed, 0 failed** |

---

## Scope-Boundary Audit (MUST-HOLD invariants)

| Invariant | Status | Evidence |
|-----------|--------|----------|
| `tests/test_fjall_crash_recovery.rs` exists, uses `bind("127.0.0.1:0")` + BEAVA_TCP_PORT env | HELD | 358 lines, 3× `127.0.0.1:0`, 2× `BEAVA_TCP_PORT` |
| Crash test: push → persist → kill → restart → assert last-ack'd LSN recovered | HELD | 500/500 survived; tolerance formula applied |
| `tests/proptests/sharding_parity.rs` has `#![cfg(not(feature = "state-inmem"))]` at file top | HELD | Line 33 of the file |
| Proptest parity runs at fjall for BOTH N=1 and N=8 | HELD | `run_batch(..., 1, ...)` + `run_batch(..., 8, ...)` in every `proptest_*_parity` test |
| 3 metrics emitted: write_bytes_total, compaction_bytes_total, fsync_latency_ms | HELD | `register_shard_metrics` touches all three per-shard; shard_event_loop records write_bytes + compaction_bytes (0) each tick |
| NO `beava_fjall_cache_hit_ratio` hardcoded/emitted | HELD | Helper absent; name appears only in comments |
| No hardcoded 1.0 cache_hit_ratio | HELD | `grep -nE "cache_hit_ratio.*=.*1\.0"` empty |
| No hardcoded `777X/778X/779X` port in test file | HELD | grep empty |
| TDD RED → GREEN commit split | HELD | Task 1 single-commit (permitted when first run green, D-05-05); Task 2 split into 5ebf5a9 RED + 7660900 GREEN |
| state-inmem build still green | HELD | `cargo build --features state-inmem` + 888 lib tests PASS |
| No STATE.md / ROADMAP.md writes | HELD | `git log --stat d065b4a^..HEAD -- .planning/STATE.md .planning/ROADMAP.md` — no changes |

---

## Deviations from Plan

### Auto-fixed Issues

None. The plan's recipe was implementable end-to-end without deviations. Three intentional decisions (D-05-01 through D-05-05 above) are design choices the plan explicitly allowed, not deviations.

### Architectural Decisions

None required — no Rule 4 checkpoints.

### Authentication Gates

None.

---

## Requirements Status

| Requirement | Status | Evidence |
|-------------|--------|----------|
| TPC-PERSIST-02 | **CLOSED** | `sigkill_mid_workload_restores_acked_writes` green. 500 HTTP pushes → SIGKILL → restart on same data-dir → 500 entities recovered via fjall's journal auto-replay on `Keyspace::open`. Tolerance formula applied (lost 0 ≤ ceil(5 × 14500.6 / 1000) + 1 = 74). Meaningful-recovery assertion (≥ 1 of the early writes survives) also holds. |
| TPC-PERSIST-05 part B | **CLOSED** | `PROPTEST_CASES=50 cargo test --release --test sharding_parity` green in 216s on 9 tests covering all 5 operator types (filter/map/agg{count/sum/HLL}/join/fork) plus generator determinism and the W-3 fjall-iter smoke. N=1 and N=8 both use fresh fjall-backed `Shard::with_partition` instances per proptest case. |

---

## Known Stubs

**`beava_fjall_compaction_bytes_total` emits 0.** This is NOT a stub in the correctness sense — the metric is registered, touched at startup, and incremented by 0 each tick so the series is visible and alert-targetable. The real byte count requires a fjall API that does not exist in 2.11 (no compaction-event callbacks, no per-level byte accessors). A future fjall release can flip the value from 0 to the real number without any code changes at the alert-consumer side. Documented in `src/shard/metrics.rs` W-4 comment and D-05-03.

**`beava_fjall_fsync_latency_ms` not updated from shard_event_loop.** The hot path uses `PersistMode::Buffer`, so there is no fsync to measure inside the event loop. The helper is public and wired; `src/migrate_to_fjall/mod.rs` (Plan 04) already calls `keyspace.persist(SyncData | SyncAll)` at its fencing sites — wiring `update_fjall_fsync_latency` there is a 2-line follow-up that stays in Plan 06's operations-docs scope. Not a Plan-05 blocker.

---

## Deferred Issues

- **`update_fjall_fsync_latency` wiring in the migrate tool.** The helper exists and Plan 04's migrate tool has the natural call sites (the two `keyspace.persist(...)` calls). Two-line follow-up; Plan 06 will document it as part of the BEAVA_FJALL_* operations section.
- **HTTP `/features/{key}` on the restarted child.** Known gap: the handler reads from `state.store` (legacy StateStore, AHashMap), not the fjall partitions. The crash-recovery test side-steps this by reading fjall directly (D-05-01). Bridging `/features` to the fjall path is a separate concern that belongs to a future plan (maybe 53-06 or later) — it's the same engineering gap that 53-03B's SUMMARY flagged.
- **Nightly proptest at 10_000 cases.** PR smoke (50 cases) ships here; nightly CI runs 10_000 cases already per the Phase 52-07 workflow. No changes needed — the workflow just picks up the re-ported harness automatically.

---

## Threat Flags

None. All surface changes are covered by the plan's STRIDE register (T-53-05-01 through T-53-05-05):

- **T-53-05-01 (orphan child on panic):** `ChildGuard` RAII wrapper kills + waits on Drop. Verified.
- **T-53-05-02 (ephemeral-port race after drop):** Accepted per plan — same pattern as Phase 52-06's `test_replica_subscribe`. No observed flake in ~25 runs during development.
- **T-53-05-03 (proptest timeout in CI):** PROPTEST_CASES=50 lands in 216s, well under the 5-minute budget. Nightly 10_000 is a separate job.
- **T-53-05-04 (fjall metric label cardinality):** `shard=N` is bounded to `0..N_SHARDS`, N ≤ 256 per Plan 03's ShardedStateStoreFjall invariant. Safe.
- **T-53-05-05 (fake cache_hit_ratio hides real misses):** Gauge is OMITTED entirely (W-4). Plan 06 docs will NOT include the `< 0.8 sustained` alert.

---

## Test / Verify Commands

```bash
# Task 1 — crash recovery
cargo test --release --test test_fjall_crash_recovery -- --nocapture

# Task 2a — proptest parity (PR smoke)
PROPTEST_CASES=50 cargo test --release --test sharding_parity -- --test-threads=1

# Task 2b — metrics unit tests
cargo test --release --lib shard::metrics

# Regression guards (Plans 03/03B/04)
cargo test --release --test shard_store_fjall
cargo test --release --test shard_fjall_backend
cargo test --release --test test_migrate_to_fjall -- --test-threads=1
cargo test --release --test test_reshard_fjall_aware -- --test-threads=1

# Full suites
cargo test --release --lib                              # 884 PASS (default)
cargo test --release --features state-inmem --lib       # 888 PASS (state-inmem)

# Builds
cargo build --release                                   # default
cargo build --release --features state-inmem --tests    # state-inmem tests compile
```

All green at commit `7660900` on macOS Darwin 24.3.0.

---

## Self-Check: PASSED

**Files verified present:**

- [x] `tests/test_fjall_crash_recovery.rs` — FOUND, 358 lines
  - contains `fn sigkill_mid_workload_restores_acked_writes` (1 match)
  - contains `#![cfg(unix)]` (1) + `#![cfg(not(feature = "state-inmem"))]` (1)
  - contains `127.0.0.1:0` (3) + `BEAVA_TCP_PORT` (2) + `BEAVA_FJALL_FSYNC_MS` (2) + `child.kill` (4)
  - NO hardcoded `777X/778X/779X` port numbers
- [x] `tests/proptests/sharding_parity.rs` — FOUND, has `#![cfg(not(feature = "state-inmem"))]` at file top, uses `Shard::with_partition` (4 refs) + `shard.state.iter()` (6 refs), 0 `Shard::new()` calls
- [x] `tests/proptests/mod.rs` — FOUND, gate flipped to unconditional `pub mod sharding_parity`
- [x] `src/shard/metrics.rs` — FOUND, 3 new pub fns + 3 const names + 6 W-4 comments, cache_hit_ratio helper absent
- [x] `src/shard/mod.rs` — FOUND, `Shard.write_bytes_since_sample` field + `take_write_bytes()` helper + initialized in `with_partition`
- [x] `src/shard/thread.rs` — FOUND, shard_event_loop emits write/compaction bytes per gauge tick

**Commits verified present:**

- [x] `d065b4a` `test(53-05): fjall crash-recovery SIGKILL integration test with ephemeral port (W-8)` — present in `git log --oneline -5`
- [x] `5ebf5a9` `test(53-05): RED — proptest parity port to fjall (W-3) + failing fjall-metrics unit test` — present
- [x] `7660900` `feat(53-05): GREEN — fjall metrics helpers + shard-loop emission (W-4)` — present

**Verification outputs verified:**

- [x] `cargo test --release --test test_fjall_crash_recovery -- --nocapture` — 1/1 PASS (pushed=500 survived=500)
- [x] `PROPTEST_CASES=50 cargo test --release --test sharding_parity -- --test-threads=1` — 9/9 PASS in 216s
- [x] `cargo test --release --lib shard::metrics` — 6/6 PASS
- [x] `cargo test --release --lib` — 884 PASS (default build)
- [x] `cargo test --release --features state-inmem --lib` — 888 PASS (state-inmem build)
- [x] `cargo build --release --features state-inmem --tests` — green

**Scope-boundary invariants verified:**

- [x] No write to `.planning/STATE.md` by this plan
- [x] No write to `.planning/ROADMAP.md` by this plan
- [x] Closes TPC-PERSIST-02 + TPC-PERSIST-05 part B
- [x] TDD RED → GREEN commit split preserved for Task 2; Task 1 RED+GREEN single commit per plan's permission (first run green, D-05-05)
- [x] state-inmem build compiles green
- [x] No regressions in 884+888 lib tests
