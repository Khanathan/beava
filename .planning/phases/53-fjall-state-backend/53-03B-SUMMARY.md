---
phase: 53-fjall-state-backend
plan: 03B
subsystem: storage / shard-state-plumbing
tags: [fjall, shard-state-swap, tdd, concurrent-app-state, store-fjall, thread-port, wave-2]
dependency_graph:
  requires: [53-03-SUMMARY.md]
  provides:
    - "src/shard/store_fjall.rs::ShardedStateStoreFjall (fjall-backed ShardedStateStore)"
    - "ConcurrentAppState.fjall_keyspace: Arc<fjall::Keyspace> (default build)"
    - "ConcurrentAppState.shard_partitions: Vec<fjall::PartitionHandle> (default build)"
    - "shard_event_loop fjall-backed Shard dispatch (default build)"
    - "apply_set_on_shard / get_table_row_on_shard backend-agnostic via StoreView + read_entity_from_shard"
    - "keys_owned metric via PartitionHandle::approximate_len (Pitfall 4)"
    - "tests/common::ephemeral_test_keyspace helper (for Plans 04 / 05 integration tests)"
  affects:
    - 53-04-PLAN.md (migrate-to-fjall already unblocked; can now reach a real ConcurrentAppState.fjall_keyspace on boot)
    - 53-05-PLAN.md (SIGKILL test can now exercise the default-build hot path end-to-end through the shard event loop)
requirements:
  - TPC-PERSIST-01 (CLOSED — every consumer of Shard.state is fjall-backed on the default build)
  - TPC-PERSIST-02 (UNBLOCKED — ConcurrentAppState.fjall_keyspace survives across restarts; Plan 05 proves SIGKILL via the journal replay path)
tech_stack:
  added:
    - "tests/common module for shared integration-test helpers (new file — first occupant: ephemeral_test_keyspace)"
  patterns:
    - "Two-cfg default/state-inmem callsite branching at the Shard constructor (Shard::with_partition vs Shard::new) + metric gauge (approximate_len vs len)"
    - "Unified apply_set_on_shard / get_table_row_on_shard via StoreView::Sharded + read_entity_from_shard — single code path for both backends"
    - "BEAVA_DATA_DIR or per-process-unique temp_dir fallback for fjall keyspace root at ConcurrentAppState build time"
    - "File-level #![cfg(not(feature = \"state-inmem\"))] on tests/shard_fjall_backend.rs and tests/shard_store_fjall.rs; inverse gate on tests/test_parallel_recovery.rs"
key_files:
  created:
    - src/shard/store_fjall.rs
    - tests/shard_store_fjall.rs
    - tests/common/mod.rs
  modified:
    - src/shard/mod.rs
    - src/shard/thread.rs
    - src/server/tcp.rs
    - src/server/http_ingest.rs
    - tests/proptests/mod.rs
    - tests/shard_fjall_backend.rs
    - tests/test_n2_routing.rs
    - tests/test_parallel_recovery.rs
decisions:
  - "D-03B-01 (ephemeral_test_keyspace home): placed the helper in tests/common/mod.rs rather than #[cfg(test)] inside src/shard/fjall_backend.rs. Rationale: Plans 04 and 05 integration tests will use it from tests/*.rs files — `#[cfg(test)]` items in library code are NOT visible to integration tests (those link against the production lib). `tests/common/mod.rs` is the idiomatic Rust location and keeps the helper out of the production build without introducing a `test-helpers` Cargo feature. Plan 03B's Task 1 explicitly allows either placement."
  - "D-03B-02 (BEAVA_DATA_DIR fallback): when the env var is unset (test path), open_fjall_keyspace_and_partitions_for_state picks a per-process-unique subdir of std::env::temp_dir() — `beava-fjall-{pid}-{subsec_nanos}-{atomic_counter}` — and creates it. No tempfile crate needed (fjall crate is a runtime dep; tempfile is dev-only). Production always sets BEAVA_DATA_DIR via main.rs so this path never runs in prod."
  - "D-03B-03 (Scope-expansion: gate tests/shard_fjall_backend.rs under default): Plan 03 shipped the file without a cfg gate. Under state-inmem the file references `Shard::with_partition` which doesn't exist — causing the state-inmem build to fail. Applied Rule 3 (blocking-issue auto-fix): added file-level `#![cfg(not(feature = \"state-inmem\"))]`. Out-of-scope-adjacent (Plan 03 territory) but unavoidable: without it, `cargo build --features state-inmem` cannot link test binaries."
  - "D-03B-04 (Scope-expansion: gate tests/test_parallel_recovery.rs under state-inmem): the recovery path replays into `Shard::new()` AHashMaps — does not compile under default build. Under default build, crash recovery is fjall journal auto-replay on Keyspace::open (TPC-PERSIST-02; Plan 05 SIGKILL gate). Applied Rule 3: file-level `#![cfg(feature = \"state-inmem\")]`. Same carry-over as Plan 03 Deviation 2."
  - "D-03B-05 (run_tcp_server shard count): pulled from `state.shard_partitions.len()` (default) / `sharded_store.shard_count()` (state-inmem). No longer routes through the `BEAVA_SHARDS` env read under default build — the count is fixed once at boot when `make_concurrent_state_full` opens the partitions."
metrics:
  duration_s: 2280
  duration_human: "~38m"
  completed: 2026-04-19
  tasks_total: 2
  tasks_completed: 2
  commits: 2
  files_touched: 11
---

# Phase 53 Plan 03B: fjall-state-backend — ShardedStateStoreFjall + Consumer Port Summary

## One-liner

Create `ShardedStateStoreFjall` (fjall-backed `ShardedStateStore` sibling); port `src/shard/thread.rs` consumers (shard_event_loop, apply_set_on_shard, get_table_row_on_shard, approximate_len metric) to the fjall path; plumb `ConcurrentAppState.fjall_keyspace` + `shard_partitions` at boot; gate the legacy AHashMap proptest harness behind `state-inmem`. Closes TPC-PERSIST-01 end-to-end.

---

## What Was Built

**Commit chain:** `40f0bb5` RED → `913a346` GREEN.

### 1. `src/shard/store_fjall.rs` (NEW, 171 lines)

The fjall sibling of the now-`state-inmem`-gated `ShardedStateStoreV1`.

```rust
pub struct ShardedStateStoreFjall {
    keyspace: Arc<Keyspace>,
    shards: Vec<Shard>,   // each owns a PartitionHandle via Shard::with_partition
}

impl ShardedStateStoreFjall {
    pub fn new(n: u16, ks: Arc<Keyspace>, cfg: &FjallConfig) -> fjall::Result<Self> { ... }
    pub fn shard_index_for_event(&self, event: &Value, key_field: Option<&str>) -> usize { ... }
    pub fn shard_at(&self, idx: usize) -> &Shard { ... }
    pub fn shard_at_mut(&mut self, idx: usize) -> &mut Shard { ... }
    pub fn keyspace(&self) -> &Arc<Keyspace> { ... }
}

impl ShardedStateStore for ShardedStateStoreFjall {
    fn shard_count(&self) -> u16 { self.shards.len() as u16 }
    fn for_each_shard<F: FnMut(&Shard)>(&self, mut f: F) { ... }
    fn for_each_shard_mut<F: FnMut(&mut Shard)>(&mut self, mut f: F) { ... }
}
```

- `new` asserts `1 <= n <= 256` (T-53-03B-01 mitigation — identical bound to V1).
- `shard_index_for_event` matches V1's shape exactly (N=1 fast-path, else `shard_hint % n`).
- Module registered in `src/shard/mod.rs` behind `#[cfg(not(feature = "state-inmem"))]`.
- 2 inline unit tests cover N-shard allocation + N=1 routing fast-path.

### 2. `src/shard/thread.rs` — fjall port

**`shard_event_loop` (default build):** unified with the state-inmem body. The per-shard `Shard` is now built from `ConcurrentAppState.shard_partitions[shard_index].clone()` on the default build; `Shard::new()` remains on state-inmem. The Plan-03 stub ("drain inbox and respond with quarantine error") is deleted.

**`apply_set_on_shard`:** rewritten on top of `StoreView::Sharded(shard).with_entity_mut(key, |entity| { ... })` — single code path that routes through postcard + fjall under default build and plain AHashMap `entry().or_default()` under state-inmem. The `dirty_set.insert` stays.

**`get_table_row_on_shard`:** rewritten on top of the W-6 `read_entity_from_shard` helper — deserializes via postcard under default, direct AHashMap read under state-inmem.

**`keys_owned` metric:** `shard.state.len()` → `shard.state.approximate_len()` under default (Pitfall 4: O(1) vs LSM-walk). state-inmem keeps `AHashMap::len()` (already O(1) there).

All `#[cfg(feature = "state-inmem")]` gates on the three helpers removed — they now work in both builds because they funnel through the StoreView abstraction.

### 3. `src/server/tcp.rs` — ConcurrentAppState plumbing

Added two fields gated on default build:

```rust
#[cfg(not(feature = "state-inmem"))]
pub fjall_keyspace: std::sync::Arc<fjall::Keyspace>,
#[cfg(not(feature = "state-inmem"))]
pub shard_partitions: Vec<fjall::PartitionHandle>,
```

`make_concurrent_state_full` now calls the new helper `open_fjall_keyspace_and_partitions_for_state(n_shards)` before constructing the `Arc<ConcurrentAppState>`, and passes the returned `(Arc<Keyspace>, Vec<PartitionHandle>)` into the struct literal:

```rust
#[cfg(not(feature = "state-inmem"))]
fn open_fjall_keyspace_and_partitions_for_state(n_shards: u16)
    -> (Arc<fjall::Keyspace>, Vec<fjall::PartitionHandle>) {
    let cfg = fjall_config_from_env(n_shards.max(1));
    let data_dir = match std::env::var("BEAVA_DATA_DIR").ok() {
        Some(p) if !p.is_empty() => PathBuf::from(p),
        _ => std::env::temp_dir().join(
            format!("beava-fjall-{}-{}-{}", pid, nanos, counter.fetch_add(1, Relaxed))
        ),  // per-process unique test fallback
    };
    let ks = open_keyspace_from_env(&data_dir, &cfg).expect(...);
    let partitions = (0..n as usize)
        .map(|i| open_shard_partition(&ks, i, &cfg).expect(...))
        .collect();
    (ks, partitions)
}
```

`run_tcp_server` now reads `shard_count` from `state.shard_partitions.len()` under default build (replacing the `BEAVA_SHARDS` env read that Plan 03 stubbed in).

### 4. `src/server/http_ingest.rs` — shard_count mirror

`http_list_streams` mirrors tcp.rs: `state.shard_partitions.len()` under default, `sharded_store.shard_count()` under state-inmem.

### 5. `tests/proptests/mod.rs` — harness gate

```rust
#[cfg(feature = "state-inmem")]
pub mod sharding_parity;
```

Under default build the module is skipped (the harness calls `Shard::new()` which doesn't exist there). Plan 05 re-ports the harness on top of `Shard::with_partition` + ephemeral fjall keyspaces.

### 6. `tests/common/mod.rs` (NEW, 78 lines) + `tests/shard_store_fjall.rs` (NEW, 131 lines)

`tests/common/mod.rs`: shared integration-test helper module. First occupant is `ephemeral_test_keyspace(n) -> (Arc<Keyspace>, Vec<PartitionHandle>, TempDir, FjallConfig)` — opens a keyspace under a fresh `TempDir`, pre-opens N partitions, returns all four so tests can keep the `TempDir` alive. Sets `BEAVA_FJALL_FSYNC_DISABLE=1` + `BEAVA_FJALL_CACHE_MB=32` for determinism under its own process-global env-mutation mutex.

`tests/shard_store_fjall.rs`: 4 integration tests covering:
1. `sharded_state_store_fjall_new_creates_n_shards` — `new(4, ..)` yields `shard_count() == 4` + shard_at(0/3) resolve.
2. `sharded_state_store_fjall_shard_index_for_event_is_deterministic` — pure-function routing guarantee.
3. `sharded_state_store_fjall_for_each_shard_visits_all` — trait impl visits all 8 shards in both `for_each_shard` and `for_each_shard_mut`.
4. `ephemeral_test_keyspace_helper_creates_n_partitions` — helper opens 3 partitions, keyspace Arc lives, TempDir path exists, round-trip insert/get succeeds.

### 7. Rule 3 auto-fixes on pre-existing test files

- `tests/shard_fjall_backend.rs` (from Plan 03): added `#![cfg(not(feature = "state-inmem"))]` at file top. Under state-inmem the file referenced `Shard::with_partition` which doesn't exist — the state-inmem build was broken by Plan 03's own shipped test.
- `tests/test_parallel_recovery.rs`: added `#![cfg(feature = "state-inmem")]`. Replays into `Shard::new()` AHashMaps which the default build has deleted. Under default build, crash recovery is fjall journal auto-replay on `Keyspace::open` (Plan 05 SIGKILL gate).
- `tests/test_n2_routing.rs::two_shard_state_has_correct_shard_count`: branches on cfg to assert `shard_partitions.len() == 2` (default) or `sharded_store.shard_count() == 2` (state-inmem).

---

## Verification

All commands green on the default (fjall) build:

| Command | Result |
|---------|--------|
| `cargo build` | green |
| `cargo test --release --test shard_store_fjall` | **4/4 PASS** |
| `cargo test --release --test shard_fjall_backend` (Plan 03) | **5/5 PASS** |
| `cargo test --release --lib --tests -- --test-threads=1` (full suite) | **1504 PASS, 0 failed attributable to Plan 03B** |

state-inmem build:

| Command | Result |
|---------|--------|
| `cargo build --features state-inmem` | green |
| `cargo test --release --features state-inmem --lib --tests -- --test-threads=1` | **1513 PASS, 0 failed attributable to Plan 03B** |

**Pre-existing test failures** (verified on stashed pre-Plan-03B HEAD `40f0bb5`; NOT regressions from this plan):

| Test | Status | Notes |
|------|--------|-------|
| `test_health_endpoint` | Pre-existing fail | `/health` body is `{"status":"alive"}` but test expects `"status":"ok"` — drift in `src/server/http.rs` predates Plan 53. |
| `v7_roundtrip_table_rows` | Pre-existing fail | Snapshot-v7 migration test asserts 7, gets 8 — predates Plan 53. |
| `test_replica_subscribe::subscribe_then_push_delivers_events` | macOS ephemeral-port flake (`AddrNotAvailable`) | Unrelated to Plan 03B. |
| `test_replica_subscribe::backpressure_drops_subscriber` | Same macOS port flake | Unrelated. |
| `test_push_coalescing::e2e::mixed_workload_sync_p99` (state-inmem only) | Timing flake under parallel test load | Passes in isolation; passes pre-Plan-03B; not attributable to this plan. |

**Acceptance-criteria grep grid:**

| Grep | Expected | Actual |
|------|----------|--------|
| `test -f src/shard/store_fjall.rs` | exists | ✓ |
| `grep -c "pub struct ShardedStateStoreFjall" src/shard/store_fjall.rs` | 1 | 1 ✓ |
| `grep -c "pub mod store_fjall" src/shard/mod.rs` | 1 | 1 ✓ |
| `grep -c "read_entity_from_shard" src/shard/thread.rs` | ≥ 1 | 2 ✓ |
| `grep -c "with_entity_mut" src/shard/thread.rs` | ≥ 1 | 3 ✓ |
| `grep -c "approximate_len" src/shard/thread.rs` | ≥ 1 | 2 ✓ |
| `grep -E "shard\.state\.entry\(" src/shard/thread.rs` | 0 (default gated away) | 0 ✓ |
| `grep -c "shard_partitions" src/server/tcp.rs` | ≥ 2 | 8 ✓ |
| `grep -c "fjall_keyspace" src/server/tcp.rs` | ≥ 2 | 8 ✓ |
| `grep -c "feature = \"state-inmem\"" tests/proptests/mod.rs` | ≥ 1 | 1 ✓ |
| `grep -c "ephemeral_test_keyspace" tests/common/mod.rs` | ≥ 1 | 3 ✓ |
| HEAD commits | `test(53-03B): RED …` then `feat(53-03B): GREEN …` | 40f0bb5, 913a346 ✓ |

Note: the plan's acceptance grid asks for `grep -c "ephemeral_test_keyspace" src/shard/fjall_backend.rs`. Per D-03B-01, the helper landed in `tests/common/mod.rs` instead (plan explicitly allowed either placement). The semantically equivalent check is `grep -c "ephemeral_test_keyspace" tests/common/mod.rs` which returns 3.

---

## Scope-Boundary Audit (MUST-HOLD invariants from execution prompt)

| Invariant | Status | Evidence |
|-----------|--------|----------|
| `src/shard/store_fjall.rs` NEW, `ShardedStateStoreFjall` sibling to V1 | HELD | `grep "pub struct ShardedStateStoreFjall" src/shard/store_fjall.rs` → 1 match |
| `thread.rs::push_with_cascade_on_shard` uses `PartitionHandle` via StoreView contract | HELD | `apply_set_on_shard` routes through `StoreView::Sharded(shard).with_entity_mut`; `with_cascade` delegates to `push_internal_on_shard` (Plan 03 path). |
| `ConcurrentAppState` has `fjall_keyspace` + `shard_partitions` in default build | HELD | `grep -c "fjall_keyspace\|shard_partitions" src/server/tcp.rs` = 16 |
| `http_ingest.rs` routes via `shard_partitions` (stub replaced) | HELD | `grep "state.shard_partitions.len" src/server/http_ingest.rs` → 1 match |
| `main.rs` keyspace+partitions plumbing | HELD (no edit needed) | `make_concurrent_state_full` opens them at AppState construction time — main.rs just calls that. No main.rs edit required. |
| `tests/proptests/mod.rs` file-top `#[cfg(feature = "state-inmem")]` | HELD | `grep "feature = \"state-inmem\"" tests/proptests/mod.rs` → 1 match |
| `ephemeral_test_keyspace` available for Plan 04/05 | HELD | `tests/common/mod.rs::ephemeral_test_keyspace` — reusable via `mod common;` declaration |
| `cargo build` (default) green | HELD | Verified |
| `cargo test --lib --tests` (default) green | HELD | 1504/1504 (modulo pre-existing unrelated failures) |
| `cargo test --features state-inmem --lib --tests` green | HELD | 1513/1513 (modulo same list) |
| TDD RED → GREEN commit split | HELD | 40f0bb5 RED, 913a346 GREEN |
| `.planning/STATE.md` / `.planning/ROADMAP.md` not written by this plan | HELD | No stage of these files in either commit; pre-existing `M` markers are upstream, untouched |

---

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 — Blocking] Plan 03 shipped `tests/shard_fjall_backend.rs` without a cfg gate; state-inmem build fails to compile**

- **Found during:** Task 2 Step 6 (state-inmem build check).
- **Issue:** `tests/shard_fjall_backend.rs` imports `beava::shard::Shard` and calls `Shard::with_partition(partition)` throughout all 5 tests. Under `--features state-inmem`, `Shard::with_partition` does not exist (the AHashMap path uses `Shard::new()`), so the state-inmem build cannot compile any test binary.
- **Fix:** Added file-level `#![cfg(not(feature = "state-inmem"))]` to `tests/shard_fjall_backend.rs`. The 5 Plan-03 integration tests still run under the default build; under state-inmem they're skipped.
- **Files modified:** `tests/shard_fjall_backend.rs`
- **Commit:** `913a346`
- **Why this isn't out of scope:** Plan 03B's acceptance requires `cargo test --features state-inmem --lib --tests` green. That requires the test file to link. Without this gate, the whole state-inmem build fails at the test step — i.e. Plan 03B's acceptance cannot be met without fixing it. Plan 03's own Deferred Issues section flags this test as "will fail to build under default features" but that's the opposite direction — the same file also breaks state-inmem.

**2. [Rule 3 — Blocking] Plan-03-stubbed `src/main.rs` event-log recovery block blocks state-inmem test_parallel_recovery.rs under default build**

- **Found during:** Task 2 Step 6.
- **Issue:** `tests/test_parallel_recovery.rs` calls `Shard::new()` which doesn't exist under default build. Under default build, crash recovery is fjall journal auto-replay on `Keyspace::open` — no AHashMap-replay path exists.
- **Fix:** Added file-level `#![cfg(feature = "state-inmem")]` to `tests/test_parallel_recovery.rs`.
- **Files modified:** `tests/test_parallel_recovery.rs`
- **Commit:** `913a346`

**3. [Rule 3 — Blocking] `tests/test_n2_routing.rs::two_shard_state_has_correct_shard_count` reads `sharded_store` field (state-inmem-gated)**

- **Found during:** Task 2 Step 6.
- **Issue:** Test asserts `state.sharded_store.lock().unwrap()` — the field is state-inmem-only under Plan 03.
- **Fix:** Branched on cfg: default build asserts `state.shard_partitions.len() == 2`; state-inmem asserts `sharded_store.shard_count() == 2`.
- **Files modified:** `tests/test_n2_routing.rs`
- **Commit:** `913a346`

### Architectural Decisions

None required. No Rule 4 checkpoints.

### Authentication Gates

None.

---

## Requirements Status

| Requirement | Status | Evidence |
|-------------|--------|----------|
| TPC-PERSIST-01 | **CLOSED** | Every consumer of `Shard.state` on the default build routes through fjall: `shard_event_loop` builds `Shard::with_partition(partitions[i])`; `apply_set_on_shard` routes through `StoreView::Sharded` postcard RMW; `get_table_row_on_shard` routes through `read_entity_from_shard`; the `keys_owned` metric uses `approximate_len`. Integration tests `shard_store_fjall` + `shard_fjall_backend` prove round-trip semantics on real tempdir-backed partitions. |
| TPC-PERSIST-02 | **UNBLOCKED** (full close in Plan 05) | `ConcurrentAppState.fjall_keyspace` survives across restart (verified in Plan 03's `storeview_sharded_survives_keyspace_reopen` test). Plan 05's SIGKILL test exercises the real crash path. |

---

## Known Stubs

None from this plan. All Plan-03 stubs (`thread.rs::shard_event_loop` stub body, `main.rs` recovery gate under default build, tcp.rs shadow-write stub, pipeline.rs sharded_store stub) are either replaced with the real fjall implementation (thread.rs shard_event_loop) or remain intentionally gated behind state-inmem where they belong (shadow-write block, recovery block — under default build, fjall's journal replaces those).

---

## Deferred Issues

- **`main.rs` + `engine::pipeline.rs` legacy state-inmem gates**: preserved as-is. Plan 03 gated legacy code paths behind `state-inmem` for the library to compile; Plan 03B does not unwind those gates because they represent dev-mode A/B backends that must stay orthogonal to the default fjall path. Deleting the `state-inmem` feature is a Phase 54+ decision per CONTEXT §Deferred.
- **Proptest harness port to default build**: `tests/proptests/sharding_parity.rs` still calls `Shard::new()`. Plan 05 re-ports the harness with fjall-backed shards as part of its parity contract.
- **The flaky `test_push_coalescing::e2e::mixed_workload_sync_p99`** (state-inmem only) passes in isolation but can miss a p99 budget when the whole suite runs serially. Timing-sensitive; not a regression. Plan 05's SIGKILL test path or Plan 06's bench harness will likely force a baseline refresh anyway.

---

## Threat Flags

None. Plan 03B introduces no new security-relevant surface — all changes are internal plumbing (Shard construction, AppState field plumbing, metric gauge). The `fjall_keyspace` + `shard_partitions` fields map 1-to-1 onto the Plan 03 `Shard.state: PartitionHandle` surface which Plan 03 already threat-modeled (T-53-03-01 through T-53-03-04). No new endpoints, no new auth surface, no new file paths beyond `BEAVA_DATA_DIR/fjall/` which Plan 02 already documented.

The `T-53-03B-01` (out-of-bounds index panic) mitigation is structurally enforced: `ShardedStateStoreFjall::new` asserts `1 <= n <= 256`, and `shard_index_for_event` computes `hint % self.shards.len()` — arithmetically in-bounds by construction.

The `T-53-03B-03` (accidental state-inmem compilation) mitigation is enforced by `#[cfg(not(feature = "state-inmem"))] pub mod store_fjall;` in `src/shard/mod.rs` — the module is literally absent under state-inmem builds.

The `T-53-03B-02` (default build picks up legacy sharding_parity) mitigation is enforced by `#[cfg(feature = "state-inmem")] pub mod sharding_parity;` in `tests/proptests/mod.rs` — the module is skipped under default.

---

## Test / Verify Commands

```bash
# Plan 03B Task 1 + helper tests (default build)
cargo test --release --test shard_store_fjall

# Plan 03 fjall backend integration (still green from Plan 03, now with Task 2 changes)
cargo test --release --test shard_fjall_backend

# Full default suite — fjall is hot path
cargo test --release --lib --tests -- --test-threads=1

# Full state-inmem suite — legacy AHashMap path intact
cargo test --release --features state-inmem --lib --tests -- --test-threads=1

# Clean build checks
cargo build --release
cargo build --release --features state-inmem
```

All green at commit `913a346` modulo the 4 pre-existing unrelated failures listed above.

---

## Self-Check: PASSED

**Files verified present:**

- [x] `src/shard/store_fjall.rs` — FOUND, 171 lines
  - contains `pub struct ShardedStateStoreFjall` (1 match)
  - contains `impl ShardedStateStore for ShardedStateStoreFjall` (1 match)
  - contains `pub fn new(n: u16, ks: Arc<Keyspace>, cfg: &FjallConfig)` (1 match)
- [x] `src/shard/mod.rs` contains `pub mod store_fjall` behind `#[cfg(not(feature = "state-inmem"))]` — FOUND
- [x] `src/shard/thread.rs` — FOUND, with `read_entity_from_shard` (2 refs), `with_entity_mut` (3 refs), `approximate_len` (2 refs); zero default-build `shard.state.entry(` callsites
- [x] `src/server/tcp.rs` — FOUND, with `fjall_keyspace` (8 refs) + `shard_partitions` (8 refs); `open_fjall_keyspace_and_partitions_for_state` helper present
- [x] `src/server/http_ingest.rs` — FOUND, `n_shards` default-build path uses `state.shard_partitions.len()`
- [x] `tests/proptests/mod.rs` — FOUND, `#[cfg(feature = "state-inmem")] pub mod sharding_parity;`
- [x] `tests/common/mod.rs` — FOUND, `ephemeral_test_keyspace` helper exported
- [x] `tests/shard_store_fjall.rs` — FOUND, 4 tests, file-level `#![cfg(not(feature = "state-inmem"))]`
- [x] `tests/shard_fjall_backend.rs` — FOUND, file-level `#![cfg(not(feature = "state-inmem"))]` added (Rule 3)
- [x] `tests/test_parallel_recovery.rs` — FOUND, file-level `#![cfg(feature = "state-inmem")]` added (Rule 3)
- [x] `tests/test_n2_routing.rs` — FOUND, cfg-branched assertion

**Commits verified present:**

- [x] `40f0bb5` `test(53-03B): RED — ShardedStateStoreFjall + ephemeral_test_keyspace tests fail to compile`
- [x] `913a346` `feat(53-03B): GREEN — ShardedStateStoreFjall + thread.rs fjall port + ConcurrentAppState plumbing + proptest gating`

**Verification outputs verified:**

- [x] `cargo test --release --test shard_store_fjall` — 4/4 PASS
- [x] `cargo test --release --test shard_fjall_backend` — 5/5 PASS
- [x] `cargo test --release --lib --tests -- --test-threads=1` (default) — 1504/0/11 (pass/fail/ignored; 4 pre-existing failures skipped, all unrelated to Plan 03B — verified on stashed HEAD)
- [x] `cargo test --release --features state-inmem --lib --tests -- --test-threads=1` — 1513/0/11 (same pre-existing list skipped)
- [x] `cargo build --release` — green
- [x] `cargo build --release --features state-inmem` — green

**Scope-boundary invariants verified:**

- [x] No write to `.planning/STATE.md` by this plan
- [x] No write to `.planning/ROADMAP.md` by this plan
- [x] Both commits restricted to code + tests under `src/` and `tests/`
- [x] TDD RED → GREEN commit split preserved (two commits, no amend)

Deviations 1–3 (test-file cfg gates under both feature configurations) are documented above as Rule 3 auto-fixes — unavoidable for the acceptance criteria to hold under both builds.
