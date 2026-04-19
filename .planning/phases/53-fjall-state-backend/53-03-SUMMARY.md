---
phase: 53-fjall-state-backend
plan: 03
subsystem: storage / shard-state-swap
tags: [fjall, shard-state-swap, tdd, storeview, cargo-feature, wave-2]
dependency_graph:
  requires: [53-02-SUMMARY.md]
  provides:
    - "Shard.state: fjall::PartitionHandle (default build)"
    - "Shard::with_partition(PartitionHandle) -> Shard"
    - "StoreView::Sharded arms routed through postcard + fjall"
    - "read_entity_from_shard<F, R>(&Shard, &str, F) -> Option<R> (W-6)"
    - "state-inmem Cargo feature (D-03; OFF by default)"
    - "src/shard/store.rs file-level gate behind state-inmem"
  affects:
    - 53-03B-PLAN.md (unblocked — Shard::with_partition + read_entity_from_shard now exist)
    - 53-04-PLAN.md (migrate-to-fjall will insert into PartitionHandle via Shard::with_partition)
    - 53-05-PLAN.md (SIGKILL crash recovery gates on the fjall journal, not AHashMap replay)
requirements:
  - TPC-PERSIST-01 (PARTIAL — fjall-backed Shard.state shipped; ConcurrentAppState + thread.rs plumbing is Plan 03B)
tech_stack:
  added:
    - "`state-inmem` Cargo feature flag (dev-mode AHashMap fallback; D-03)"
  patterns:
    - "Two #[cfg]-guarded Shard struct variants — same field names, different types"
    - "entity_to_bytes / entity_from_bytes helpers over SerializableEntityState (EntityState is NOT Serialize — plan interface had a factual error)"
    - "StoreView::Sharded closure-based RMW that survives backend swap (Pattern 2 from 53-RESEARCH)"
    - "Compile-fix Rule 3 cfg-gates on legacy AHashMap callsites across pipeline.rs / tcp.rs / thread.rs / main.rs / http_ingest.rs (minimal surface so default build stays green)"
key_files:
  created:
    - tests/shard_fjall_backend.rs
  modified:
    - src/shard/mod.rs
    - src/shard/store.rs
    - src/shard/thread.rs
    - src/engine/pipeline.rs
    - src/server/tcp.rs
    - src/server/http_ingest.rs
    - src/main.rs
    - Cargo.toml
decisions:
  - "D-03-01 (Serialization wire format): Use `SerializableEntityState` (not `EntityState` directly) as the postcard wire format. `EntityState` is NOT `Serialize`/`Deserialize` — it carries an `AtomicU64` and AHashMap fields. The plan's <interfaces> block prescribed `postcard::from_bytes::<EntityState>(bytes)`, which would not compile. Factored into `entity_to_bytes` / `entity_from_bytes` helpers so the three call sites (StoreView with_entity_mut, get_entity_ref, read_entity_from_shard) share the conversion. Same wire format as snapshot v8."
  - "D-03-02 (Scope of compile-fix patches): the W-1 revision said default `cargo build --release` is 'expected to fail with errors CONFINED to src/shard/thread.rs / src/server/tcp.rs'. In practice the errors span pipeline.rs, tcp.rs, thread.rs, http_ingest.rs, and main.rs — and an integration test CANNOT link unless the lib builds, which contradicts the plan's own acceptance criterion that `cargo test --test shard_fjall_backend` must exit 0. Applied Rule 3 (Blocking issue fix): minimal #[cfg]-gated callsite edits across all five files. Plan 03B replaces these stubs with the proper fjall-aware implementations."
  - "D-03-03 (Default-build shard thread behavior): In the default (fjall) build, `shard_event_loop` is a stub that drains the SPSC inbox and responds with an error variant. It does NOT process pushes until Plan 03B wires ConcurrentAppState.fjall_keyspace + shard_partitions. Integration tests in Plan 03 (shard_fjall_backend) construct Shards directly via `Shard::with_partition`, bypassing the thread loop entirely — those are what this plan's acceptance criteria exercise."
metrics:
  duration_s: 2100
  duration_human: "~35m"
  completed: 2026-04-19
  tasks_total: 2
  tasks_completed: 2
  commits: 2
  files_touched: 8
---

# Phase 53 Plan 03: fjall-state-backend — Shard.state Swap Summary

## One-liner

Swap `Shard.state: AHashMap<EntityKey, EntityState>` → `Shard.state: fjall::PartitionHandle` in the default build; reshape `StoreView::Sharded` to round-trip through postcard + fjall; add the W-6 `read_entity_from_shard` read-only helper; gate the legacy `ShardedStateStoreV1` behind `--features state-inmem`. Plan 03B finishes the ConcurrentAppState plumbing, the shard event loop, and the proptest harness port.

---

## What Was Built

**Commit chain:** `57293bf` RED → `8d7056f` GREEN.

### 1. `src/shard/mod.rs` — the load-bearing swap (371 lines total; +251/−20 vs Plan 02 baseline)

**Two `#[cfg]`-guarded `Shard` variants.** Same field shape (`state`, `dirty_set`, `event_log`, `watermark`), different `state` type:

```rust
#[cfg(not(feature = "state-inmem"))]
pub struct Shard {
    pub state: fjall::PartitionHandle,
    pub dirty_set: HashSet<EntityKey>,
    pub event_log: Option<EventLog>,
    pub watermark: WatermarkState,
}

#[cfg(feature = "state-inmem")]
pub struct Shard {
    pub state: AHashMap<EntityKey, EntityState>,
    /* ... same fields ... */
}
```

**Constructors.** `Shard::with_partition(PartitionHandle) -> Self` exists in the default build; `Shard::new()` + `with_event_log()` are gated to state-inmem. `Default` impl follows the same split.

**Serialization helpers.** Because `EntityState` is NOT `Serialize`/`Deserialize` (it carries `AtomicU64` + `AHashMap`), the plan's prescribed `postcard::from_bytes::<EntityState>(bytes)` would not compile. I use `SerializableEntityState` as the wire format — the same format already used by snapshot v8:

```rust
#[cfg(not(feature = "state-inmem"))]
fn entity_to_bytes(entity: &EntityState) -> Vec<u8> { /* -> SerializableEntityState -> postcard::to_stdvec */ }

#[cfg(not(feature = "state-inmem"))]
fn entity_from_bytes(bytes: &[u8]) -> Option<EntityState> { /* postcard::from_bytes::<SerializableEntityState> -> EntityState */ }
```

**Reworked `StoreView::Sharded` arms.** The `with_entity_mut` arm:
```rust
#[cfg(not(feature = "state-inmem"))]
StoreView::Sharded(shard) => {
    let mut entity = shard.state.get(key.as_bytes()).ok().flatten()
        .and_then(|bytes| entity_from_bytes(&bytes))
        .unwrap_or_default();
    let r = f(&mut entity);
    let bytes = entity_to_bytes(&entity);
    shard.state.insert(key.as_bytes(), bytes).expect("fjall partition insert");
    r
}
```
Corrupt bytes → `entity_from_bytes` returns `None` → treated as missing + overwritten (T-53-03-01 mitigation). The `get_entity_ref` arm is the symmetric read-only form.

**W-6 read helper.** `pub fn read_entity_from_shard<F, R>(shard: &Shard, key: &str, f: F) -> Option<R>` in both `#[cfg]` variants. `grep -c "pub fn read_entity_from_shard" src/shard/mod.rs == 2` satisfies the W-6 acceptance criterion.

**Module registration.** `pub mod store;` is now `#[cfg(feature = "state-inmem")]` — the default build doesn't compile the legacy `ShardedStateStoreV1` module at all.

### 2. `src/shard/store.rs` — file-level feature gate

Added `#![cfg(feature = "state-inmem")]` at the top of the file (first non-doc line). The entire module only exists under state-inmem.

### 3. `Cargo.toml` — `state-inmem` feature

```toml
[features]
default = ["server"]
# ... existing ...
# Phase 53-03 (D-03): dev-only fallback to Phase 49 AHashMap-backed
# ShardedStateStoreV1. OFF by default — production binary ships with the
# fjall backend only. Enable via `cargo build --features state-inmem` to
# run A/B benchmarks against the pre-Phase-53 legacy path.
state-inmem = []
```

### 4. `tests/shard_fjall_backend.rs` (316 lines, NEW) — 5 integration tests

| # | Test | What it asserts |
|---|------|-----------------|
| 1 | `storeview_sharded_write_then_read_round_trips_through_fjall` | StoreView::Sharded RMW + get_entity_ref work against a real tempdir-backed partition |
| 2 | `storeview_sharded_survives_keyspace_reopen` | Clean drop + reopen preserves values (semantic proxy for TPC-PERSIST-02; SIGKILL test is Plan 05) |
| 3 | `two_shard_partitions_isolated_no_cross_contention` | Two threads, two partitions, 200 inserts each → no cross-contamination after persist |
| 6 | `read_entity_from_shard_returns_none_on_missing_key` | W-6: absent key → None, no write-back |
| 7 | `read_entity_from_shard_returns_deserialized_entity` | W-6: seeded key → Some(closure result); second read stable (no write-back) |

### 5. `src/shard/mod.rs::#[cfg(test)] mod tests` — 2 unit tests

| # | Test | Cfg |
|---|------|-----|
| 4 | `shard_state_approximate_len_returns_usize_not_result` | default build — proves `approximate_len()` is O(1) usize (Pitfall 4) |
| 5 | `inmem_build_compiles_and_uses_ahashmap` | `feature = "state-inmem"` — AHashMap path compiles + is empty on `Shard::new()` |

### 6. Minimal compile-fix patches (Rule 3 — Blocking) across 5 files

The plan's W-1 revision prescribed that default `cargo build --release` was "allowed to fail with errors confined to `src/shard/thread.rs` / `src/server/tcp.rs`". In reality the callsite blast radius hit 5 files, and — crucially — an integration test cannot LINK unless the library builds. That makes the plan's own acceptance criteria mutually inconsistent: default build fails AND `cargo test --test shard_fjall_backend` passes cannot both hold for a library crate.

I applied Rule 3 (Blocking-issue auto-fix): minimal `#[cfg]`-gated edits so the lib compiles under both feature sets. Plan 03B's GREEN commit will replace these stubs with proper fjall-aware implementations.

| File | Change |
|------|--------|
| `src/shard/thread.rs` | `shard_event_loop` split into a fjall-build stub (drains inbox, responds with error) + the legacy state-inmem body. `apply_set_on_shard`, `get_table_row_on_shard`, `json_to_feature_value_local` gated behind state-inmem. |
| `src/engine/pipeline.rs` | `PipelineEngine.sharded_store` field + `with_shards(n)` body gated behind state-inmem. Four callsites at lines 1639 / 1742 / 1977 / 2904 reworked: read-only ones route through `read_entity_from_shard`; RMW ones route through `StoreView::Sharded(shard).with_entity_mut(...)`. |
| `src/server/tcp.rs` | `ConcurrentAppState.sharded_store` + `make_concurrent_state_full` field construction gated behind state-inmem. `run_tcp_server`'s shard_count read falls back to `BEAVA_SHARDS` env in the fjall build. The "shadow write" block at line 1788 is gated. `n_shards` parameter carries `#[cfg_attr(not(feature = "state-inmem"), allow(unused_variables))]`. |
| `src/server/http_ingest.rs` | `http_list_streams`'s shard_count read mirrors run_tcp_server's cfg split. |
| `src/main.rs` | The parallel log-recovery block (870–923) is entirely gated behind state-inmem. In the fjall build, crash recovery is fjall journal replay on `Keyspace::open` (Plans 03B + 05). |

---

## Verification

All commands green:

| Command | Result |
|---------|--------|
| `cargo test --test shard_fjall_backend` | **5/5 PASS** |
| `cargo test --lib shard::tests` | **1/1 PASS** (Test 4: approximate_len) |
| `cargo test --features state-inmem --lib` | **887/887 PASS** (includes Test 5 + 881 pre-existing) |
| `cargo build --release --features state-inmem` | green, 1 dead-code warning (pre-existing) |
| `cargo build` (default fjall) | green, 1 dead-code warning (pre-existing) |

**Raw fjall backend test output:**
```
running 5 tests
test storeview_sharded_survives_keyspace_reopen ... ok
test read_entity_from_shard_returns_none_on_missing_key ... ok
test two_shard_partitions_isolated_no_cross_contention ... ok
test read_entity_from_shard_returns_deserialized_entity ... ok
test storeview_sharded_write_then_read_round_trips_through_fjall ... ok

test result: ok. 5 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 1.80s
```

**Acceptance grep check grid:**

| Grep | Expected | Actual |
|------|----------|--------|
| `grep -c "pub state: fjall::PartitionHandle" src/shard/mod.rs` | 1 | 1 ✓ |
| `grep -c "pub state: AHashMap" src/shard/mod.rs` | 1 | 1 ✓ |
| `grep -c "pub fn with_partition" src/shard/mod.rs` | 1 | 1 ✓ |
| `grep -c "pub fn read_entity_from_shard" src/shard/mod.rs` | 2 (W-6) | 2 ✓ |
| `grep -c "!\\[cfg(feature = \"state-inmem\")\\]" src/shard/store.rs` | 1 | 1 ✓ |
| `grep -c "state-inmem" Cargo.toml` | ≥ 1 | 2 ✓ |
| `grep -c 'default = \["server"\]' Cargo.toml` | 1 | 1 ✓ |
| `grep -c "postcard::to_stdvec" src/shard/mod.rs` | ≥ 1 | 1 ✓ |
| `grep -c "postcard::from_bytes" src/shard/mod.rs` | ≥ 2 (prescribed) | 1 — see Deviation 1 |

The `postcard::from_bytes` grep count is 1 instead of ≥ 2 because I factored the deserialization into an `entity_from_bytes` helper called from three sites (StoreView::Sharded `with_entity_mut`, `get_entity_ref`, and `read_entity_from_shard`). The plan's ≥ 2 expectation counted inline postcard calls. Semantic intent (deserialize-mutate-reserialize on every RMW) is preserved; the call sites just share one helper. This is documented as Deviation 1 below.

---

## Scope-Boundary Audit (MUST-HOLD invariants from execution prompt)

| Invariant | Status | Evidence |
|-----------|--------|----------|
| `Shard.state: fjall::PartitionHandle` under default features | HELD | `grep "pub state: fjall::PartitionHandle" src/shard/mod.rs` → 1 match |
| `read_entity_from_shard` exported | HELD | 2 `pub fn` matches (W-6: both cfg variants) |
| `StoreView::Sharded` RMW via postcard + fjall | HELD | `entity_to_bytes` / `entity_from_bytes` helpers + `shard.state.get/insert` in the default arm |
| `ShardedStateStoreV1` gated behind `state-inmem` | HELD | `#![cfg(feature = "state-inmem")]` at src/shard/store.rs line 11 |
| `[features] state-inmem = []` in Cargo.toml | HELD | Confirmed; NOT in `default = ["server"]` |
| `tests/proptests/mod.rs` UNTOUCHED | HELD | `git diff f87137c..HEAD --stat -- tests/proptests/mod.rs` → no change |
| No STATE.md / ROADMAP.md writes | HELD | Pre-existing `M` markers are upstream, not from this plan |

**Scope note:** The plan's MUST-HOLD list included `src/shard/thread.rs` UNTOUCHED and `src/server/tcp.rs` UNTOUCHED. Those MUST-HOLDs are NOT held — see Deviation 2 for the full rationale (lib-must-compile constraint contradicted the plan's own acceptance criteria; Rule 3 applied).

---

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 — Bug] `EntityState` is not `Serialize`/`Deserialize`; switched to `SerializableEntityState` wire format**

- **Found during:** Task 2 Step 3 (rewriting `StoreView::Sharded` arms)
- **Issue:** The plan's `<interfaces>` block prescribed `postcard::from_bytes::<EntityState>(&bytes)` and `postcard::to_stdvec(&entity)`. `EntityState` in `src/state/store.rs:179` carries a `dirty_gen: AtomicU64` and `AHashMap` fields that do not implement `Serialize`/`Deserialize`. The prescribed code would fail to compile with trait-bound errors on `EntityState: Serialize`.
- **Fix:** Factored serialization into two module-private helpers: `entity_to_bytes(&EntityState) -> Vec<u8>` and `entity_from_bytes(&[u8]) -> Option<EntityState>`. Both use `SerializableEntityState` (already in `src/state/snapshot.rs`) as the wire format — the same format used by snapshot v8. Three callers (StoreView::Sharded's two arms + `read_entity_from_shard`) share the helpers.
- **Files modified:** `src/shard/mod.rs`
- **Commit:** `8d7056f`
- **Semantic equivalence:** Every round-trip still goes through postcard on every RMW (per the plan's intent). The helper pattern is DRY-er; the only downside is that `grep -c "postcard::from_bytes"` = 1 instead of ≥ 2. Documented in the acceptance-grep table above.

**2. [Rule 3 — Blocking] The plan's MUST-HOLDs contradict the library-link constraint**

- **Found during:** Task 2 Step 6 (initial `cargo build`)
- **Issue:** The plan's `<action>` Step 6 explicitly says `cargo build --release` (default features) "is allowed to fail with errors confined to `src/shard/thread.rs` / `src/server/tcp.rs`". But Step 4 also says `cargo test --test shard_fjall_backend` must exit 0 — and integration tests LINK THE LIBRARY. For a library crate, a failing lib build means no integration tests can run. These two requirements cannot both be satisfied while also leaving `thread.rs` / `tcp.rs` untouched. Additionally, `src/engine/pipeline.rs`, `src/main.rs`, and `src/server/http_ingest.rs` also referenced `shard.state.entry()` / `shard.state.get()` / `Shard::new()` / `sharded_store` — the callsite blast radius was wider than the plan anticipated.
- **Fix:** Applied Rule 3 (auto-fix blocking issues) with **minimum-surface, cfg-gated** edits to the following files. Every edit is cfg-guarded so the legacy state-inmem build remains unchanged:
  - `src/shard/thread.rs`: `shard_event_loop` split into fjall-build stub + legacy body. Helper functions gated.
  - `src/engine/pipeline.rs`: `sharded_store` field gated; four hot-path callsites rewritten to use `StoreView::Sharded` / `read_entity_from_shard`.
  - `src/server/tcp.rs`: `sharded_store` field gated; `run_tcp_server` reads `BEAVA_SHARDS` under fjall; shadow-write block gated.
  - `src/server/http_ingest.rs`: `http_list_streams` shard_count read mirrors tcp.rs.
  - `src/main.rs`: parallel log-recovery block fully gated (fjall build relies on journal auto-replay).
- **Files modified:** `src/shard/thread.rs`, `src/engine/pipeline.rs`, `src/server/tcp.rs`, `src/server/http_ingest.rs`, `src/main.rs`
- **Commit:** `8d7056f`
- **Why this isn't out-of-scope:** The plan's scope list says "Do NOT touch `src/shard/thread.rs`, `src/server/tcp.rs`, or `tests/proptests/mod.rs` (Plan 03B owns those files)". I did touch thread.rs and tcp.rs — but the plan's own acceptance criteria REQUIRED integration tests to pass, which REQUIRED the lib to build, which REQUIRED these files to compile. Plan 03B can (and will) replace my cfg-gated stubs with the real fjall-aware bodies; the file-level ownership is preserved (my edits are additive in the non-state-inmem path, not invasive rewrites). `tests/proptests/mod.rs` was NOT touched.

### Architectural Decisions

None required. No Rule 4 checkpoints.

### Authentication Gates

None.

---

## Requirements Status

| Requirement | Status | Evidence |
|-------------|--------|----------|
| TPC-PERSIST-01 | **PARTIAL** (Shard.state swapped + StoreView plumbing shipped; ConcurrentAppState + thread.rs plumbing is Plan 03B) | `grep "pub state: fjall::PartitionHandle" src/shard/mod.rs` = 1; integration tests prove RMW correctness on real partitions. |

TPC-PERSIST-01 is NOT marked complete — Plan 03B finishes the wire-up of the fjall-backed shard event loop, `ShardedStateStoreFjall`, and the `ConcurrentAppState.fjall_keyspace` + `shard_partitions` fields. Those together deliver the full requirement.

---

## Known Stubs

- **`src/shard/thread.rs::shard_event_loop` (default build)**: drains inbox, responds with `"shard thread in Phase-53-03 stub mode; fjall plumbing pending Plan 03B"` error. **Intentional** — Plan 03B replaces this body with the real fjall-backed dispatch loop. Not a completeness issue: the default-build stub exists only so the library link-checks for Plan 03's integration tests, which construct Shards directly via `Shard::with_partition` and never route through the event loop.
- **`PipelineEngine.sharded_store` (default build)**: field does not exist. Plan 03B introduces a new `sharded_store_fjall: ShardedStateStoreFjall` (or equivalent) on the same struct.
- **`ConcurrentAppState.sharded_store` (default build)**: same — Plan 03B's responsibility.
- **Parallel log recovery in `src/main.rs` (default build)**: entire block gated out. Plan 03B + Plan 05 replace it with fjall journal replay on `Keyspace::open` (auto, no application code needed).

All stubs are cfg-gated so the state-inmem feature still ships the full Phase 49 legacy path.

---

## Deferred Issues

- **Proptest harness port (`tests/proptests/sharding_parity.rs`) under default features**: per plan, explicitly deferred to Plan 03B (`tests/proptests/mod.rs` UNTOUCHED invariant). Under default features, the proptest integration test currently fails to compile because it calls `Shard::new()`. Plan 03B will port it to `Shard::with_partition(tempdir_keyspace.open_partition("shard-N"))`. Not a Plan 03 gate.
- **`tests/test_parallel_recovery.rs`**: also uses `Shard::new()`; will fail to build under default features. Legacy recovery test; gate or port is Plan 03B's job (out of this plan's scope).

---

## Threat Flags

None. All surface changes are within the plan's STRIDE register (T-53-03-01 through T-53-03-04). The `entity_from_bytes` helper explicitly treats postcard-decode failure as `None` (T-53-03-01 mitigation: bad row → treated as missing, no panic); concurrent partition access is documented in the module-level doc comment (T-53-03-02 single-writer-by-convention). No new cross-partition batch calls (T-53-03-01 variant) are introduced.

---

## Test / Verify Commands

```bash
# Integration tests (Plan 03 <verification> #1 — exercises the W-6 helper + StoreView RMW on real fjall partitions)
cargo test --test shard_fjall_backend

# Lib unit tests (Plan 03 <verification> #2 — Test 4 approximate_len)
cargo test --lib shard::tests

# state-inmem full lib suite (Plan 03 <verification> #3 — legacy path unchanged)
cargo test --features state-inmem --lib

# Release build under state-inmem (Plan 03 <verification> #3 — dev-mode fallback still ships)
cargo build --release --features state-inmem

# Default build (NEW — this plan makes it green instead of the plan's "expected to fail")
cargo build --release
```

All green on macOS (Darwin 24.3.0) at commit `8d7056f`.

---

## Self-Check: PASSED

**Files verified present:**

- [x] `tests/shard_fjall_backend.rs` — 316 lines; FOUND
- [x] `src/shard/mod.rs` — 371 lines; FOUND
  - contains `pub state: fjall::PartitionHandle` (1 match)
  - contains `pub state: AHashMap` (1 match; state-inmem gated)
  - contains `pub fn with_partition` (1 match)
  - contains `pub fn read_entity_from_shard` (2 matches; W-6)
  - contains `entity_from_bytes` / `entity_to_bytes` helpers
- [x] `src/shard/store.rs` — contains `#![cfg(feature = "state-inmem")]` at top (1 match)
- [x] `Cargo.toml` — contains `state-inmem = []` under `[features]`
- [x] `src/shard/thread.rs`, `src/engine/pipeline.rs`, `src/server/tcp.rs`, `src/server/http_ingest.rs`, `src/main.rs` — cfg-gated compile-fix patches applied

**Commits verified present:**

- [x] `57293bf` `test(53-03): RED — shard fjall backend integration + unit tests + read_entity_from_shard fail to compile`
- [x] `8d7056f` `feat(53-03): GREEN — Shard.state = PartitionHandle, StoreView fjall RMW, read_entity_from_shard helper, state-inmem feature`

**Verification outputs verified:**

- [x] `cargo test --test shard_fjall_backend` — 5/5 PASS
- [x] `cargo test --lib shard::tests` (default) — 1/1 PASS
- [x] `cargo test --features state-inmem --lib` — 887/887 PASS
- [x] `cargo build --release --features state-inmem` — green
- [x] `cargo build` (default) — green

**Scope-boundary invariants verified:**

- [x] No write to `.planning/STATE.md` by this plan (pre-existing `M` marker is upstream)
- [x] No write to `.planning/ROADMAP.md` by this plan (pre-existing `M` marker is upstream)
- [x] `tests/proptests/mod.rs` UNTOUCHED
- [x] W-6 acceptance: `grep -c "pub fn read_entity_from_shard" src/shard/mod.rs` = 2

Deviations 1 (EntityState Serialize bug) and 2 (thread.rs / tcp.rs scope expansion for lib-must-compile) are both documented above as auto-fixes under Rules 1 and 3.
