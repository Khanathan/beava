# Tally Performance Findings — Gap Analysis

**Source:** `benchmark/FINDINGS.md` (4 priorities, ~6 weeks projected work, ~50-75x for medium workloads)
**Date:** 2026-04-11

---

## 1. Current State Summary Table

| # | Priority (FINDINGS) | Claimed effort | Current status | Evidence |
|---|---|---|---|---|
| 1 | Binary wire protocol | 3-5 days | **Partial / misleading label** — frame + opcode header is binary, but PUSH/SET payloads and all responses are still JSON. | `src/server/protocol.rs:8-14` opcodes; `:116-121` `read_json_payload` used inside every PUSH/SET/REGISTER; `src/server/tcp.rs:216, 385` `feature_map_to_json` serializes responses. |
| 2 | `push()` fire-and-forget + `push_sync()` split | 3-5 days | **Not started.** Only one opcode `OP_PUSH = 0x01`; every PUSH writes a response frame synchronously. Python SDK has one `push()` that round-trips. | `src/server/protocol.rs:9` single PUSH opcode; `src/server/tcp.rs:188-193` unconditional `writer.write_all(&resp_bytes)`; `python/tally/_app.py:82` single `def push(...) -> FeatureResult`. |
| 3 | Per-entity locking via DashMap + AtomicU64 | 1-2 weeks | **Not started.** Single global `Arc<Mutex<AppState>>`; inside, `StateStore` uses `AHashMap<EntityKey, EntityState>`. No DashMap, no AtomicU64. No `dashmap` crate in `Cargo.toml`. | `src/server/tcp.rs:91` `pub type SharedState = Arc<Mutex<AppState>>`; `src/state/store.rs:89-96` `StateStore { entities: AHashMap<EntityKey, EntityState>, ... }`. |
| 4 | Cached HLL estimates | 2-3 days | **Not started.** HLL `read()` merges every non-empty bucket and calls `count()` fresh on every call. | `src/engine/hll.rs:178-196`; `TODOS.md:40-44` P2 still open. |

**Bottom line:** all four priorities are **0% shipped in substance**. Priority 1 has a superficial framing layer, but payload decode still goes through `serde_json::from_slice` and responses through `serde_json::to_vec`. The ~50% cost FINDINGS attributes to JSON is still being paid.

**Correction:** FINDINGS Priority 3 references `src/engine/view.rs` which does not exist. View/lookup logic lives in `src/engine/pipeline.rs:757-803` inside `get_features()`.

**Tokio runtime:** `src/main.rs:27` is `#[tokio::main(flavor = "current_thread")]` — single OS thread. The existing `Mutex<AppState>` is never actually contended. Changing threading is the hinge of Priority 3.

---

## 2. Gap Analysis

### Priority 1 — Binary wire protocol
**Remaining:**
- Binary PUSH event payloads (today: stream_name + JSON event blob)
- Binary feature responses (today: `serde_json::to_vec(feature_map)`)
- SET/MSET payloads
- REGISTER stays JSON (not hot path)
- Symmetric Python SDK in `_protocol.py` + `_client.py`
- Regression tests + end-to-end compat test

**Tensions:** none. Pure wire-format change.
**Risk:** binary format must cover all `FeatureValue` variants including `Missing`.

### Priority 2 — `push()` async + `push_sync()` split
**Remaining:**
- New opcode `OP_PUSH_ASYNC` (0x07; 0x06 is MGET)
- Split 175-line PUSH arm (`src/server/tcp.rs:201-375`) into async (skips response) and sync branches
- Conditional response write
- Python SDK split (API break either way)
- Loss-mode docs, CLAUDE.md §TCP Protocol update

**Tensions — material:**
- **FINDINGS' 30x is not reachable in Tally's shape.** FINDINGS benchmark was 7-feature single-stream. Tally's push must still run cascade + fan-out + event log append + dirty-marking. Real savings: skip derive/view eval and response serialization/write. Realistic: **2-5x on Linux for pure ingest, not 30x**.
- Phase 10.2 latency instrumentation sits in the same handler — split must preserve hooks.
- Throughput tracker already dedups across primary+cascade+fan-out. Stays correct.

**Risk:** drops silently on TCP RST — opt-in only, not flag-flip.

### Priority 3 — DashMap + AtomicU64
**Remaining (the biggest):**
- Add `dashmap` dependency
- `StateStore.entities: AHashMap<...>` → `DashMap<EntityKey, Arc<EntityState>>`
- `EntityState` holds `Mutex<OperatorState>` per stream + `AtomicU64` cache fields
- Rewrite push hot path (`src/engine/pipeline.rs:341-493`)
- Rewrite cross-stream lookup (`src/engine/pipeline.rs:769-797`) to atomic loads
- Rewrite snapshot serialization (intersects Phase 9 dirty set)
- Rewrite eviction
- `SharedState` type itself changes — ripples into every `/debug/*` handler + Phase 10.2 latency tracker
- Tokio `current_thread` → `multi_thread` (cascades through every `.await`)

**Tensions (documentation-level):**
- **Breaks "Single-threaded core (v1)"** — `CLAUDE.md:17, 451, 471`
- **Contradicts REQUIREMENTS.md Out-of-Scope** row: `.planning/REQUIREMENTS.md:78`
- **Contradicts TODOS.md P3** "Key-partitioned multi-threading — XL. Different architecture, not a bolt-on."
- **Contradicts `.planning/research/ARCHITECTURE.md:28`** "The Mutex is never contended in practice..."
- **Phase 9 dirty set** (`src/state/store.rs:93`) must move
- **Phase 10/10.1/10.2** debug UI lock-once-then-build-JSON pattern dies; every endpoint becomes best-effort snapshot

**Risk:** rewrite of the core, not a refactor. Regression surface = entire operator test suite + snapshot round-trip + cascade DAG.

### Priority 4 — Cached HLL estimates
**Remaining:**
- `cached_estimate: AtomicU64` on `Hll` or `DistinctCountOp`
- Background refresh task in `main.rs`
- Integration test for lag bounds and accuracy
- `precision='exact'` opt-in

**Tensions:** background refresh walks all entities. Under current single-mutex, refresh blocks PUSH during sweep. Under P3 DashMap, per-entity mutex still costs. **P3 should ship before P4 or P4's refresh becomes its own hot-path contention source.**

**Risk:** low — local to `hll.rs` + operator wrapper.

---

## 3. Architectural Tensions

### 3.1 "Single-threaded core" is load-bearing in docs
- `CLAUDE.md:17` — Core Design Principles bullet
- `CLAUDE.md:451` — Key Technical Decisions row
- `CLAUDE.md:471` — Benchmarks target "single thread"

### 3.2 "No key-partitioned multi-threading in v1"
- `.planning/REQUIREMENTS.md:78` Out-of-Scope row
- `TODOS.md:76-80` P3 entry

### 3.3 Phase 10.x instrumentation contracts
- `.planning/phases/10-debug-ui/10-RESEARCH.md:93` — "Throughput counter writes MUST be O(1) and cannot introduce contention beyond the existing mutex scope." False post-refactor.
- `src/server/throughput.rs` may need sharded/atomic rewrite
- `.planning/phases/10.2-latency-debugger/10.2-02-PLAN.md` references `SharedState = Arc<Mutex<AppState>>` as stable

### 3.4 Phase 9 dirty set
Moves from "single HashSet under AppState mutex" to one of:
- `Arc<AtomicBool>` per entity
- Sharded dirty-set per DashMap shard
- MPSC channel to snapshotter task

Different delta-generation semantics.

### 3.5 Phases whose contracts break
- **Phase 6** — EntityState survives structurally, gets re-refactored
- **Phase 7** — `push_with_cascade` (`src/engine/pipeline.rs:551-615`) holds `&mut StateStore` across cascade; under fine-grained locks must acquire multiple entity locks
- **Phase 8** — `push_for_backfill` holds `&mut StateStore`; backfill currently serializes with live traffic
- **Phase 9** — dirty set moves
- **Phase 10/10.1** — debug UI handlers need rewriting
- **Phase 10.2** — latency histograms on AppState under mutex — need per-shard merge-on-read

None blocking, all solvable, but total surface justifies milestone treatment.

---

## 4. Cascade Deadlock Risk

### 4.1 Streams touched per PUSH today
1. **Primary** — 1 stream, 1 entity key
2. **Cascade downstream** — N streams, **same entity key** (easy case)
3. **Fan-out** — N streams with **different `key_field`s** extract their own key from the event (hard case)

**Upper bound:** an event with `user_id + merchant_id + device_id + ip_address + session_id` and streams keyed on each hits 5 distinct entity keys in one PUSH, plus cascade. Nothing caps this.

### 4.2 Lock ordering feasibility
- **Static order by (stream, entity_key):** pre-collect, sort, acquire. Safe; requires pre-computing cascade reachability before entity lookup.
- **Try-lock with backoff:** starves under contention
- **Coarse up-front acquire-all:** negates fine-grained benefit
- **Actor per entity:** heavier refactor, eliminates question

**The FINDINGS bench did NOT model multi-entity fan-out.** Its 10-stage DAG used one key type. **8.1M/sec is the ceiling for the easy case only.** Real Tally's fan-out case needs its own benchmark before the P3 speedup can be trusted.

### 4.3 Recommendation
Treat "multi-entity cascade correctness under fine-grained locks" as a **mandatory research-phase prerequisite** before P3 is committed. Needs a microbenchmark variant and a proof (or at least a static lint rule) of deadlock-freedom before production code lands.

---

## 5. Recommended Phasing (tradeoffs, not decisions)

**Option A — single phase inside v1.1.** Stuff 4 more phases into v1.1 (milestone goal: "Composable Pipeline & Event Log"). Performance rework does not fit. **Not recommended.**

**Option B — one new milestone v1.2 "Performance."** Four phases (Phase 11 wire, 12 async push, 13 per-entity locks, 14 HLL cache). Clean boundary. ~6 weeks. **Recommended if the team accepts breaking single-threaded invariant without a major version bump.**

**Option C — two milestones: v1.2 (wire+API) + v2.0 (core rewrite).**
- **v1.2 — Wire Protocol & Split PUSH.** Phases 11+12+14 (HLL cache independent of threading). Backward-compat, single-threaded invariant intact. ~2 weeks. ~3-6x of projected speedup.
- **v2.0 — Multi-threaded Core.** Phase 13 as its own milestone. Major version bump reflects architectural break. Remaining ~2-5x on top of v1.2.

Preserves architectural honesty: v1.x is "single-threaded Redis-shaped", v2.x is "multi-core per-entity-locked". **Recommended if the architectural break is SemVer-major.**

**Option D — three milestones.** Split v1.2 into binary wire (v1.2) and push_async+HLL (v1.3). Overkill.

| Axis | A | B | C | D |
|---|---|---|---|---|
| Bookkeeping simplicity | ★★★ | ★★ | ★ | ✗ |
| Honest architectural labeling | ✗ | ★ | ★★★ | ★★★ |
| Ships incrementally | ★ | ★★ | ★★★ | ★★★ |
| Preserves v1 contract for existing users | ✗ | ✗ | ★★★ | ★★★ |
| SemVer honesty | ✗ | ✗ | ★★★ | ★★★ |

**My read: Option C** is the cleanest fit for Tally's current docs. Option B is acceptable if "single-threaded core" is guidance, not contract.

---

## 6. Key Open Questions

1. **Is "single-threaded core" a contract or a default?** Load-bearing in CLAUDE.md and REQUIREMENTS.md. P3 requires deleting these. SemVer-major (v2.0) or implementation-detail?
2. **What does `push_async()` actually skip?** FINDINGS measured ~30x on pure single-stream. Tally's push must still run cascade/fan-out/event-log/dirty-marking. Need a sub-benchmark against a realistic cascade pipeline before committing to "30x for ingest."
3. **Phase 9 dirty set under DashMap — which strategy?** (a) AtomicBool per entity; (b) sharded dirty-sets per DashMap shard; (c) MPSC to snapshotter. Different delta semantics.
4. **Multi-entity fan-out deadlock strategy?** Pick one before coding.
5. **Binary wire: keep JSON fallback for curl/debugging?** Version handshake byte, separate opcodes, or cut JSON entirely?
6. **Who captures the "before" number?** No `benches/` or `criterion` in current Tally. Probably warrants a pre-phase "Phase 11.0: in-tree baseline benchmark."
7. **Phase 10.x debug UI during P3 refactor?** Freeze, shim, or rewrite concurrently?
8. **Does `push_sync()` keep cascade+fan-out semantics?** Today returns primary features only. Should sync-mode expand?
9. **Python SDK API break.** Rename current `push()` to `push_sync()` (breaks everyone) or keep `push()` sync and add `push_async()` / `ingest()`?
10. **macOS vs Linux for launch numbers.** Need Linux staging hardware before benchmark phase ships numbers.

---

## Appendix — File reference

| File | Priorities | Role |
|---|---|---|
| `src/server/protocol.rs` | 1, 2 | Frame + opcode parsing; replace JSON decode/encode; add OP_PUSH_ASYNC |
| `src/server/tcp.rs` | 1, 2, 3 | Command dispatch; split PUSH arm; adapt to new lock types |
| `src/state/store.rs` | 3 | AHashMap → DashMap; AtomicU64 cache; move dirty set |
| `src/state/snapshot.rs` | 3 | Serialization under fine-grained locks |
| `src/state/eviction.rs` | 3 | Per-entity lock-and-remove |
| `src/engine/pipeline.rs` | 2, 3, 4 | Push path; cascade; cross-stream lookup; `FeatureDef::DistinctCount` precision field |
| `src/engine/hll.rs` | 4 | `cached_estimate: AtomicU64` |
| `src/engine/operators.rs` | 4 | `DistinctCountOp` read path |
| `src/main.rs` | 3, 4 | Tokio flavor; spawn HLL refresh task |
| `src/server/http.rs` | 3 | Debug endpoints adapt |
| `src/server/throughput.rs` | 3 | May need sharding |
| `src/server/latency.rs` | 3 | May need per-shard histograms |
| `Cargo.toml` | 3 | Add `dashmap`; bump tokio to rt-multi-thread |
| `python/tally/_client.py` | 1, 2 | Binary encode/decode; async PUSH opcode |
| `python/tally/_protocol.py` | 1, 2 | Wire format |
| `python/tally/_app.py` | 2 | API split |
| `python/tally/_operators.py` | 4 | `precision=` kwarg |
| `CLAUDE.md` | 2, 3 | §Core Design Principles, §TCP Protocol, §Key Technical Decisions, §Benchmarks |
| `.planning/REQUIREMENTS.md` | 3 | Amend Out-of-Scope row |
| `TODOS.md` | 3, 4 | Promote P2 HLL hint and P3 multi-threading to milestone phases |
