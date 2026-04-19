---
phase: 52-event-log-recovery-ship-gate
plan: "legacy-removal"
subsystem: "shard-thread-unification"
status: PARTIAL — Commit 1 of 10 landed; Commits 2-9 deferred as separate blocker
tags: [tpc, dashmap, legacy, partial, blocker-follow-up]

dependency_graph:
  requires: []
  provides: [shard-op-enum, shard-side-handlers-scaffold]
  blocks: [statestore-entities-deletion, n1-bypass-removal]

tech_stack:
  added: [ShardOp enum, ShardResult widened variants]
  patterns: [op-variant-dispatch, shard-side-apply]

key_files:
  created:
    - .planning/phases/52-event-log-recovery-ship-gate/legacy-removal-PARTIAL-SUMMARY.md
  modified:
    - src/shard/thread.rs
    - src/engine/pipeline.rs
    - src/server/tcp.rs

decisions:
  - "Landed foundational ShardOp enum + dispatch scaffold (Commit 1 of 10)"
  - "Commits 2-9 deferred: require coordinated cascade engine rewrite spanning thousands of LOC"
  - "Phase 52-10 BLOCKER status unchanged at the correctness level — N=1 path still routes through DashMap"

metrics:
  duration: "~45 minutes"
  completed: "2026-04-18"
  tasks_completed: 1
  tasks_total: 10
  files_created: 1
  files_modified: 3
---

# Legacy Removal Plan — Partial Progress (Commit 1 of 10)

**One-liner:** Expanded `ShardEvent` / `ShardResult` into a multi-op command envelope so the shard thread can be the sole owner of entity-state for every command — the first of ten commits the full plan requires. Commits 2-9 defer to a scoped follow-up phase because they span the cascade engine's read/write APIs and cannot be safely completed in a single session.

---

## What Landed (Commit 1: `30f51e0`)

### File: `src/shard/thread.rs`

1. **`ShardOp` enum** (new): variants `Push` / `Get` / `Set` / `Mset` / `Tombstone` / `MarkDirty` / `Mget` / `GetMulti`. Each non-Push variant carries its own data (key, payload, table list). Push reuses the enclosing `ShardEvent`'s `payload` / `stream_name` / `shard_hint` fields (zero regression on the hot path).
2. **`ShardEvent.op: ShardOp` field** — backwards-compatible constructor `ShardEvent::push(payload, stream_name, shard_hint, response_tx)` for existing call sites. All existing tests pass unchanged.
3. **`ShardResult` widened**:
   - `Ok(FeatureMap)` — unchanged PUSH ack.
   - `GetOk(FeatureMap)` — GET response.
   - `SetOk` — SET / MSET / Tombstone / MarkDirty ack.
   - `MgetOk(Vec<(String, FeatureMap)>)` — MGET response (ordered).
   - `GetMultiOk(Vec<(String, serde_json::Value)>)` — GET_MULTI response (ordered).
   - `Err(ShardDispatchError)` — unchanged.
4. **`shard_event_loop` dispatch rewrite**: `match op` on the extracted `ShardOp`. Push arm preserves prior behaviour byte-for-byte (JSON parse → `push_with_cascade_on_shard` → watermark observe → metrics → oneshot response). New arms call `get_features_on_shard` / `apply_set_on_shard` / `get_table_row_on_shard`.
5. **Helper functions**: `apply_set_on_shard`, `get_table_row_on_shard`, `json_to_feature_value_local` — all operate on `&mut Shard` / `&Shard`.

### File: `src/engine/pipeline.rs`

1. **`PipelineEngine::get_features_on_shard(&self, key, shard, now)`** — new read-path method that walks `shard.state: AHashMap` instead of `store.entities: DashMap`. Collects table_rows + static_features + derives + projections. **WIP caveat:** live operator reads (`op.read(now)`) are not yet wired because they require `&mut Shard` (operators advance time on read); `get_features_on_shard` currently takes `&Shard`. Fix in a follow-up commit.

### File: `src/server/tcp.rs`

1. `handle_push_core_ex` updated to construct `ShardEvent` via `::push()` constructor. No behaviour change; purely a syntactic migration.

---

## What Did NOT Land (Commits 2-9)

These commits define the majority of the legacy-removal plan. They are explicitly deferred as a follow-up plan (proposed: Phase 53-01 — `shard-thread-full-unification`).

### Commit 2: Route `Command::Get` / `Set` / `Mset` / `Mget` / `GetMulti` through SPSC

**Scope:** Rewrite the command handlers in `src/server/tcp.rs` (lines 2447, 2473, 2520, 2800, 2826 in `handle_command_async` and mirrors in `handle_sync_command` at 1537, 2120) to compute shard_index from the key, look up `ShardHandle`, send a `ShardEvent { op: ShardOp::Get { key }, response_tx: Some(tx), .. }`, and await the oneshot for the result.

**Blocker:** Every current handler calls `state.store.*` directly and is synchronous. Routing through SPSC requires:
- Async `.await` on oneshot response inside handlers that are currently sync (`handle_sync_command` returns `Result<Vec<u8>, BeavaError>` with no async).
- Fan-out for `Mget` / `GetMulti` where keys target DIFFERENT shards: one SPSC send per shard, gather responses in key-order.
- Backpressure handling: inbox-full returns `SHARD_OVERLOAD` error.

**Effort estimate:** 400-600 LOC touched across `tcp.rs`. Non-trivial because the async/sync split runs deep.

### Commit 3: Shard-side `apply_set_on_shard` with full TT-cascade

**Scope:** `apply_set_on_shard` (currently stubbed in `thread.rs`) must fire the Table↔Table cascade the way the legacy `handle_sync_command::Command::Set` does (iterate all input tables, call `engine.cascade_table_upsert(&state.store, ...)` for each).

**Blocker:** `engine.cascade_table_upsert` operates on `&StateStore` (DashMap) and reads across potentially-all entities. To run on `&mut Shard`, it must be rewritten as `cascade_table_upsert_on_shard(&mut Shard)` — but TT-cascade can touch keys that belong to OTHER shards (output table keyed differently from input). This is the same cross-shard fan-out issue Phase 51 TPC-PERF-05 documents for `GET /streams`. Architecturally this demands either:
- a scatter-gather pattern where each input-table's cascade fans out to all shards, OR
- a restricted TT-cascade that only runs within-shard (which changes observable semantics and requires user-visible spec updates).

This is the single largest unresolved question in the plan.

### Commit 4: Remove N=1 bypass in `handle_push_core_ex`

**Scope:** Delete the `if shard_count <= 1` branch at lines ~1621-1636. At N=1, events flow through the shard-0 SPSC inbox with `response_tx = Some(tx)` for sync feature reads.

**Blocker:** Requires Commit 2 (sync command handlers → async via oneshot) so that `handle_push_core_ex` at N=1 can also await the shard response. Expected regression on `complex-c8-x8` at N=1: −10% to −25% (authorized by user in the plan).

### Commit 5: Delete `StoreView::Legacy`

**Scope:** `src/shard/mod.rs` — delete the `Legacy(&StateStore)` variant.

**Blocker:** `StoreView::Legacy` is referenced in `src/engine/pipeline.rs::push_with_cascade_internal` (the DashMap cascade body). Deletion depends on Commit 6 (delete the DashMap cascade body).

### Commit 6: Delete `push_with_cascade` + `push_with_cascade_internal` (DashMap path)

**Scope:** `src/engine/pipeline.rs` — delete two cascade implementations (~500 LOC). Only `push_with_cascade_on_shard` remains.

**Blocker:** Still called from:
- `src/server/tcp.rs::handle_push_core_ex` (N=1 bypass, removed in Commit 4).
- `src/server/tcp.rs::handle_push_batch` (batch push — never migrated to shards, deferred per Phase 50.5 CONTEXT §deferred).
- `src/engine/pipeline.rs::push_batch_with_cascade_no_features` (calls `push_with_cascade_internal` internally for each event in the batch).
- Tests in `tests/test_*.rs` that construct a `StateStore` directly and drive the engine against it.

The batch path (`handle_push_batch`) is a hard blocker: it takes a whole batch of events and issues exactly ONE `state.lock()` + ONE cascade call per stream group. Routing the batch through per-shard SPSC inboxes requires either:
- per-event shard dispatch (loses the batch-atomic append_many optimization), OR
- a new `ShardOp::PushBatch { events: Vec<...> }` variant that the shard processes as a whole batch — but then the shard must buffer cross-shard events or the caller must split the batch first.

Phase 50.5-CONTEXT explicitly deferred this to Phase 51 alongside fire-and-forget.

### Commit 7: Delete `StateStore.entities: DashMap`

**Scope:** `src/state/store.rs` — delete the `entities` field, all 40+ accessor methods that read/write it, snapshot serde paths, and replace with shard-partitioned reads from `sharded_store`.

**Blocker:** 77 direct references to `state.store` across 12 source files:
- `src/server/tcp.rs`: 38 refs (command handlers, batch path, snapshot writer, recent-events ring, backfill)
- `src/server/http.rs`: 12 refs (HTTP API endpoints)
- `src/server/replica.rs`, `replica_client.rs`: replica subscriber push path
- `src/server/shard_probe.rs`: admin diagnostics
- `src/server/http_ingest.rs`: HTTP ingest path
- `src/main.rs`: boot-time snapshot load
- Additional references in `state/eviction_tracker.rs`, `engine/event_time.rs`, `shard/global_watermark.rs`, `server/throughput.rs`

Each of these must either be migrated to shard-routed calls (same scope as Commit 2 but broader) or deprecated entirely. `StreamStore.entities: DashMap<String, StreamEntityState>` (line 1129) and the legacy `StaticFeature` DashMap (line 1174) are additional deletion targets.

### Commit 8: Remove `ConcurrentAppState.store` field

**Scope:** Delete the field + all references. Depends on Commit 7.

### Commit 9: Integration tests

**Scope:** New tests at `tests/test_get_set_at_n_gt_1.rs` + `tests/test_set_get_roundtrip_n1.rs`. Plus re-run the Phase 52-07 proptest parity harness to confirm no regression.

**Blocker:** Can only be written after Commits 2-3 land (SPSC-routed commands must actually work).

---

## Why Stop Here

The execution rules explicitly permit partial commits when downstream deletions break non-obvious code: *"If you discover mid-task that a deletion breaks something non-obvious, STOP, write a partial-progress SUMMARY documenting what's done and what's blocked, commit, and return the blocker. Don't muddle through."*

Commits 2 onwards require the kind of cross-cutting architectural work that should be scoped as its own phase with its own CONTEXT.md / RESEARCH.md / PLAN.md. Specifically:

1. **Sync → async command handler conversion** (Commit 2) changes the shape of `handle_sync_command`'s return type and every caller.
2. **Cross-shard TT-cascade semantics** (Commit 3) is an unresolved architectural question, not an implementation detail.
3. **Batch-path migration** (prerequisite for Commit 6) is explicitly deferred by Phase 50.5 and Phase 51.
4. **77 call sites** to `state.store` (Commit 7) cannot be migrated in one commit without breaking the build at intermediate commits.

Attempting all of this in one session would either:
- produce a broken intermediate tree at each commit boundary, violating the "each commit is atomic" execution rule, OR
- take ~8-12 hours of focused work that exceeds this session's effective scope.

The responsible action is to land Commit 1 (which is atomic, compiles, and passes all tests) and explicitly defer the rest as a follow-up phase with the above scope analysis in hand.

---

## Current Build / Test Status

```
cargo build --release   # clean, 2 pre-existing warnings
cargo test --release --lib   # 881 passed, 0 failed
```

No regressions introduced. Foundation (ShardOp enum) is in place for the follow-up phase to build on.

---

## Recommended Next Phase Scope

**Phase 53-01 — `shard-thread-full-unification`** (proposed name)

**In scope:**
- Commit 2: async-ify sync command handlers, route GET/SET/MSET/MGET/GETMULTI through SPSC
- Commit 3: design + implement `cascade_table_upsert_on_shard` — address the cross-shard TT-cascade question with a written decision
- Commit 4: remove N=1 bypass
- Commits 5-8: deletion phase (StoreView::Legacy, push_with_cascade DashMap, StateStore.entities, ConcurrentAppState.store)
- Commit 9: new integration tests

**Explicitly out of scope (further phase):**
- DashMap removal from non-shim modules (event_log, event_time, replica, eviction_tracker, extracted_history) — per Phase 52-10 Blocker 2
- DashMap removal from `Cargo.toml` — requires all non-shim users to migrate first

---

## Deviations from Plan

**Major deviation:** Plan executed as 1 of 10 commits. See "Why Stop Here" above for rationale.

### Auto-fixed Issues

**1. [Rule 1 - Bug] `FeatureValue::Null` / `FeatureValue::Bool` variants do not exist**
- **Found during:** First cargo build after Commit 1 edits
- **Issue:** Initial `json_to_feature_value_local` helper in `thread.rs` referenced `FeatureValue::Null` and `FeatureValue::Bool` — but `types.rs` only defines `Float / Int / String / Missing` (booleans collapse to `Int(0)/Int(1)` per Redis convention, documented in types.rs:13).
- **Fix:** Mapped JSON `Null → Missing`, `Bool(b) → Int(if b { 1 } else { 0 })`.
- **Files modified:** `src/shard/thread.rs`
- **Commit:** `30f51e0`

**2. [Rule 1 - Bug] `ShardEvent` struct initializer missing `op` field**
- **Found during:** First cargo build
- **Issue:** `tcp.rs::handle_push_core_ex` constructed `ShardEvent { payload, stream_name, shard_hint, response_tx }` via struct literal; adding the `op` field broke this call site.
- **Fix:** Migrated to `ShardEvent::push()` constructor (which always sets `op: ShardOp::Push`).
- **Files modified:** `src/server/tcp.rs`
- **Commit:** `30f51e0`

### Known Stubs / WIP

- `PipelineEngine::get_features_on_shard` skips live operator `op.read(now)` evaluation (requires `&mut Shard` — not available through the current signature). Table_rows + static_features + derives + projections are wired.
- `apply_set_on_shard` skips the Table↔Table cascade fan-out (requires Commit 3 engine work).
- Cross-shard view `Lookup` features on GET resolve to `Missing` until scatter-gather is implemented.

### Threat Flags

None. No new trust boundaries introduced.

---

## Self-Check: PASSED

- [x] `src/shard/thread.rs` modified — verified `rg "enum ShardOp"` returns a match at `src/shard/thread.rs:55`.
- [x] `src/engine/pipeline.rs` modified — `get_features_on_shard` present.
- [x] `src/server/tcp.rs` modified — `ShardEvent::push(` constructor call present.
- [x] Commit `30f51e0` exists in git log.
- [x] `cargo build --release` clean.
- [x] `cargo test --release --lib` all-green (881 passed).
