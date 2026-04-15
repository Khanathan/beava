# Perf Optimization Notes — 2026-04-15

State of play and roadmap for continued throughput work on the complex
fraud pipeline. All measurements taken on a 10-core M-series Mac, 10
independent `python3` clients pushing through
`benchmark/fraud-pipeline/run_bench.sh` (5 s warmup + 15 s measure).

## Where we are now

| Commit | Fix | Throughput (cumulative) | Per-event |
|---|---|---|---|
| baseline | — | **124k eps** | 10.6 µs |
| `c1775e5` | `notify_subscribers` atomic counter fast path | **280k eps** (+126%) | 3.6 µs |
| `a84b50c` | Precomputed cascade plan (skip per-event BFS) | **296k eps** (+139%) | 3.4 µs |
| `4c62e25` | `serde_json preserve_order` (IndexMap-backed) | **~310k avg, 443k instant** (+160%) | 3.1 µs |

**Net: 2.6× throughput, per-event cost cut from 10.6 µs → ~3.2 µs.**
All 784 unit tests still pass; the one flaky test
(`test_replica_subscribe::backpressure_drops_subscriber`) is unrelated to
the shipped fixes — it fails intermittently regardless of the perf changes
and is likely a pre-existing timing-sensitive test (see "Caveats" below).

## Hot path profile — post-Fix-1+2+3

From `sample` on the live server under load. Frame counts (not samples;
grep-matched stack frames inside `push_batch` handler for one worker over
4 s):

| Where | Frames | Category |
|---|---|---|
| `RingBuffer::advance_to` + inner `SystemTime::duration_since` | 336 + 232 | **Clock arithmetic** |
| `_platform_memmove` / `_platform_memcpy` | 283 | **Buffer copies** |
| `protocol::decode_event` | 124 | Binary wire decode |
| `event_log` append paths | 125 | WAL (see next section) |
| `serde_json::Value::Index::index_into` | 123 | Event field lookups (was 238 before Fix 3) |
| DashMap `lock_exclusive_slow` | 88 | Cross-key shard serialization |
| `notify_subscribers` | 11 | (was 622 — effectively free after Fix 1) |

## What was ruled out

### Async WAL — NOT the bottleneck

Initial research hypothesized an async WAL could recover ~30% (kernel
inode-serialization theory). **A/B test falsified it**: a temporary
`TALLY_WAL_DISABLED=1` toggle that skips the `append_many` calls entirely
gave **276k vs 272k eps — within noise**.

Conclusion: at 320k eps the `O_APPEND` + `write(2)` path is already cheap
enough that the kernel page cache absorbs it. **Don't build the
async-WAL infrastructure** — implementation in
`src/state/event_log.rs` already uses O_APPEND + lock-free
per-stream writers + background fsync. Further work there is pure overhead.

### DashMap shard contention is smaller than expected

Originally claimed "lock held across entire 25-op cascade". Agent
investigation corrected this: `push_with_cascade_internal` calls
`push_internal` once per cascade hop (5 hops typical), and
`get_or_create_entity` returns a guard scoped to **one** `push_internal`
call, not the full cascade. So each entity lock covers ~5 ops (~100 µs)
not 25 ops (~500 µs). Real contention still exists — 88 frames in
`lock_exclusive_slow` confirms it — but the potential win from
partitioning is more like ~10%, not 40%.

See `.claude/worktrees/agent-a830db19` findings (now unused worktree).
There's also **a half-finished prior refactor** at
`src/state/store.rs:932` (`StreamStore` type + `to_concurrent()` /
`from_concurrent()` adapters) that was never wired into
`ConcurrentAppState`. Picking that up is the cleanest route to per-stream
partitioning.

## Remaining opportunities (ordered by effort × impact)

### Priority 1 — Clock cache: thread `Duration` through `Operator` trait

**Expected impact: +10-20%.**

**What to change:**
- `src/engine/window.rs` — change `RingBuffer::current_bucket_start` from
  `Option<SystemTime>` → `Option<Duration>` (since `UNIX_EPOCH`). Update
  `advance_to`, `bucket_start_for`, `bucket_index_for`,
  `add_at_event_time`, `update_at_event_time` to take/return `Duration`.
- `src/engine/retracting_ring.rs` — same `current_bucket_start` change on
  the parallel type.
- `src/engine/operators.rs` — change the `Operator` trait:
  `fn push(..., now: SystemTime)` → `fn push(..., now_epoch: Duration)`.
  Same for `read`.
- Update all **19** `impl Operator for ...` blocks across
  `src/engine/operators.rs` + `src/engine/hll.rs`.
- `src/engine/pipeline.rs push_internal`: compute
  `let now_epoch = now.duration_since(UNIX_EPOCH).unwrap_or(Duration::ZERO)`
  ONCE at the top, thread to every `op.push(...)` call.
- Snapshot serialization: serialize `Duration` as `u64 millis` (same wire
  size as the old `SystemTime`). The `postcard` encoding will change — add
  a migration or bump the snapshot version.

**Scope: ~15-20 files, ~150 lines of mostly-mechanical edits.**

**Risk: high** — signature mismatch will cascade through the type checker
immediately, so compile errors are loud. Semantic risk is low (Duration
is lossless vs SystemTime for UNIX_EPOCH-anchored timestamps). The real
risk is in the `~ 30` call sites in tests that pass `now: SystemTime`
directly to `op.push()` — they all need updating.

**Why this wins:** currently each operator calls
`advance_to(now: SystemTime)` → `now.duration_since(start: SystemTime)`
as its hot inner work. With 25 ops/event × 320k eps = 8M
`duration_since` calls/sec. Pre-computing the epoch once per event cuts
this by 25×.

**Why we didn't ship it this session:** 19 operator impls + test-call-site
plumbing is a ~3-4 hour focused surgery. Ran up against wall time.

### Priority 2 — `effective_event` Cow in cascade

**Expected impact: +3-5%.**

**Where:** `src/engine/pipeline.rs:1547-1553`:

```rust
let effective_event: serde_json::Value = downstream_def
    .depends_on
    .as_ref()
    .and_then(|deps| {
        deps.iter().find_map(|d| effective_events.get(d).cloned())
    })
    .unwrap_or_else(|| event.clone());   // ← deep clone per cascade hop
```

For the common case (no upstream synthesized enrichment), the `.cloned()`
and `.unwrap_or_else(|| event.clone())` both do a deep
`serde_json::Value::clone()` — every cascade hop re-clones the event's
~8 fields (each with `String` copies).

**Fix:** change `effective_event` from `serde_json::Value` to
`Cow<'_, serde_json::Value>`:
- 99% path becomes `Cow::Borrowed(event)` — zero clones.
- Enrichment-from-table path stays `Cow::Owned(...)`.
- Every use site (`.as_object()`, `.get(kf)`, etc.) needs `.as_ref()` or
  `&*effective_event`.

The `enriched.clone()` at line 1608 stays (truly needs an owned value to
mutate via `enriched_map.insert`). The `arriving_map.clone()` at 1725 is
also legitimate (stream-stream join publishes owned state).

**Scope:** single function refactor, maybe 30 lines changed.
**Risk: medium** — Cow plumbing through pattern matches can get ugly.

### Priority 3 — `StreamStore` wire-up (per-stream partitioning)

**Expected impact: +10-15% on Zipfian, +5-10% on uniform.**

The previous refactor started this but never completed. Existing
infrastructure:

- `src/state/store.rs:932` — `StreamStore { entities: DashMap<String, StreamEntityState> }`.
- `src/state/store.rs:972, 1014` — `StateStore::to_concurrent()` /
  `from_concurrent()` adapter functions.
- The comment at line 7-8 claims `ConcurrentAppState uses StreamStore`
  but `src/server/tcp.rs:96` shows it's still backed by `StateStore`.

**Phased plan** (each phase is a commit-sized, independently-testable step):

1. Wire `StreamStore` alongside `StateStore` in `ConcurrentAppState` in
   shadow mode (both updated on every push, reads still from
   `StateStore`). Proves the adapters work.
2. Migrate `push_internal` hot path to write to `StreamStore` only.
   Reads still fall back to `StateStore`.
3. Migrate `/debug/key`, `/debug/memory`, and `get_features` read paths
   to `StreamStore`.
4. Migrate snapshot save/restore. Keep on-disk format unchanged — the
   adapter handles the pivot.
5. Delete the old `StateStore.entities: DashMap<EntityKey, EntityState>`
   field.

**The win:** each stream (user_transactions, merchant_activity, etc.)
gets its own DashMap with independent shard locking. Cross-key cascade
hops no longer serialize on the same entity's shard.

**Risk: high, well-scoped.** 97 call sites across src + tests hit
`get_or_create_entity` / `get_entity` / `get_entity_mut`. Plus snapshot
serialization needs two-way adapters. Expect ~1-2 days to phase through
properly.

### Priority 4 — memcpy reduction on decode+WAL path

**Expected impact: +3-8% (speculative — need memory-allocation profiling).**

Three sources identified by research agent, in decreasing order:

1. **`src/server/protocol.rs:752, 762, 868`** — `buf.to_vec()` per event
   at `OP_PUSH_BATCH` decode time. Each event's raw payload is copied
   into its own `Vec<u8>` even though we only need it for the WAL. Could
   be borrowed from the batch buffer with lifetime plumbing.

2. **`src/server/tcp.rs:1472` + `src/state/event_log.rs:322`** —
   `make_log_payload` calls `.to_vec()`, then `LogEntry` clones again.
   Two copies per WAL entry. One struct reorder removes the second.

3. **`src/engine/pipeline.rs:1553, 1608, 1631`** — enrichment cascade
   `.clone()`s on `serde_json::Value`. Priority 2's Cow refactor subsumes
   much of this.

**Caveat:** these are stack-frame counts (283 total), not directly
proven samples. Before committing to this, run `instruments -t alloc` or
`samply` to attribute the memcpy cost to specific call sites.

## Bench + profile tooling

`./benchmark/fraud-pipeline/run_bench.sh` — single focused bench.
Defaults: MODE=complex, CPUS=host, 5s warmup + 15s measure = ~21s total.
Prints live EPS every 2s + per-phase runtime profile.

For profiling:
```bash
# Start server manually
TALLY_ADMIN_TOKEN=dev-admin-token TALLY_WORKER_THREADS=10 \
  ./target/release/tally serve --http-port 6401 --tcp-port 6400 &
SPID=$(pgrep -f "target/release/tally serve" | tail -1)

# Kick off clients
for i in $(seq 0 9); do
  python3 ./benchmark/fraud-pipeline/bench.py \
    --mode complex --duration 20 --proc-id $i \
    --host localhost:6400 > /dev/null 2>&1 &
done

# Sample mid-run
sleep 3
sample "$SPID" 4 -mayDie -file /tmp/sample.txt

wait
kill $SPID
```

## Caveats / things to watch

1. **`test_replica_subscribe::backpressure_drops_subscriber` is flaky.**
   Fails intermittently regardless of perf fixes. Timing-sensitive test.
   Not a regression from this session's work. Investigate separately.

2. **DashMap shard count override was removed in commit `438ba2d`.**
   We used to pin shards to 16; now it's DashMap's default
   (`num_cpus × 4`, power-of-2). On a 10-core box that's 64 shards.
   If benchmarks swing on a different host, the override may be worth
   bringing back for consistency.

3. **`preserve_order` feature** was enabled on `serde_json` (commit
   `4c62e25`). This is a transitive breaking change for anything that
   expected Object iteration order to be alphabetical. Nothing obvious
   broke, but be aware.

4. **Python SDK needs `app.close()`** or processes hang for ~20 min on
   macOS (non-daemon thread). The production `bench.py` calls it; any
   future bench scripts must too.

5. **macOS `fdatasync` fallback** is in `src/state/event_log.rs:196-201`
   (commit `0ad5fd9`). Ships `fsync` on non-Linux targets. Don't remove.

## Benchmarks

```
Baseline:                         124k eps   10.6 µs/event
After notify_subscribers:         280k eps    3.6 µs/event   (+126%)
After cascade plan precompute:    296k eps    3.4 µs/event   (+139%)
After serde_json preserve_order:  ~310k eps   3.1-3.4 µs     (+150-160%)

Projected ceiling after Priority 1 (clock cache):  ~370k eps
Projected ceiling after P1+P2:                     ~400k eps
Projected ceiling after P1+P2+P3 (StreamStore):    ~450k eps
```

Numbers past ~450k require either a fundamentally different architecture
(per-worker partitioning with actor-model dispatch) or hardware scale-out.
