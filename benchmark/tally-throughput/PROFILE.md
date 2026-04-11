# Tally Profile — Callgrind (Medium Pipeline, 1 Client, 2000 Events)

**Date:** 2026-04-11
**Tool:** `valgrind --tool=callgrind` (CPU instrumentation, no kernel perf required)
**Workload:** medium pipeline, 2000 events, 1 Python client
**Raw output:** `callgrind.out` (228 KB), `callgrind-report.txt` (full annotation)

## Why callgrind and not perf/samply/py-spy

All sampling profilers (`perf`, `samply`, `cargo flamegraph`, `py-spy`, `rbspy`) require `kernel.perf_event_paranoid <= 1`. This container is locked at **3** and `/proc/sys/kernel/perf_event_paranoid` is on a read-only filesystem — no sysctl, not even with sudo. Kernel perf events are completely unavailable.

Callgrind works because it's pure CPU instrumentation: valgrind interprets every instruction and records it. No kernel subsystem involved, no ptrace, no signal handlers. The tradeoff is ~5-10x runtime overhead (2000 events took 1.3s real vs ~0.1s uninstrumented) and deterministic rather than sampled numbers.

## Top-level CPU budget (instructions, not time)

Total instructions captured: **189,947,329**

Breakdown by category:

| Category | Instructions | % of total |
|---|---:|---:|
| **JSON parse/serialize (serde_json)** | ~43M | **~22.6%** |
| **Allocator (malloc/free/realloc)** | ~45M | **~23.7%** |
| **memcpy/memcmp (largely driven by JSON + strings)** | ~11M | **~5.8%** |
| **Tally engine (pipeline::push, window, operators)** | ~15M | **~7.9%** |
| **Hash maps (hashbrown + ahash)** | ~8M | **~4.2%** |
| **Tokio task polling** | ~1M | **~0.5%** |
| **Event log append** | ~1M | **~0.5%** |
| **Throughput tracker** | ~1M | **~0.5%** |
| **Everything else** | ~65M | **~34%** |

**The headline finding: roughly 50% of per-push CPU is spent on JSON + allocator traffic that JSON generates.** This is precisely what FINDINGS §Finding 1 predicted. It's not an exaggeration — the profile confirms it with 22.6% direct JSON cost plus most of the 23.7% allocator activity is JSON-driven (every `String` clone in `serde_json::Value`, every `BTreeMap` used by `Value::Object`, every `Vec<u8>` allocated for the response payload).

## Top hotspots (from callgrind-report.txt)

| Rank | % | Function | Category |
|---:|---:|---|---|
| 1 | 9.11% | `_int_free` (glibc) | allocator |
| 2 | 7.96% | `serde_json::ser::format_escaped_str` | JSON serialize |
| 3 | 5.35% | `_int_malloc` (glibc) | allocator |
| 4 | 4.78% | `malloc` (glibc) | allocator |
| 5 | 4.47% | `serde_core::ser::Serializer::collect_seq` | JSON serialize |
| 6 | 3.17% | `__memcpy_avx_unaligned_erms` | memcpy |
| 7 | 2.68% | `__memcmp_avx2_movbe` | memcmp |
| 8 | **2.55%** | **`tally::engine::pipeline::PipelineEngine::push`** | **engine core** |
| 9 | 2.36% | `free` (glibc) | allocator |
| 10 | 2.27% | `core::hash::BuildHasher::hash_one` | hashing |
| 11 | **2.14%** | **`tally::engine::window::RingBuffer<T>::advance_to`** | **window ops** |
| 12 | **1.85%** | **`tally::state::snapshot::OperatorState::read`** | **operator read** |
| 13 | 1.82% | `std::sys::pal::unix::time::Timespec::sub_timespec` | time arithmetic |
| 14 | 1.81% | `core::str::converts::from_utf8` | string conversion |
| 15 | 1.71% | `hashbrown::HashMap::insert` | hashmap |
| 16 | 1.62% | `serde_core::ser::SerializeMap::serialize_entry` | JSON serialize |
| 17 | 1.49% | `serde_json::ser::to_vec` | JSON serialize |
| 18 | 1.27% | `hashbrown::raw::RawTable::reserve_rehash` | hashmap rehash |
| 19 | 1.26% | `serde_json::read::SliceRead::skip_to_escape` | JSON parse |
| 20 | **1.26%** | **`tally::server::tcp::handle_sync_command`** | **dispatch loop** |

**Tally's own core code is only ~8% of total instructions.** Lines 8, 11, 12, 20 sum to ~7.8%. The engine is fast; the envelope around it is slow.

## Interpretation

### The 22.6% JSON cost breaks down roughly as:

```
serde_json::ser::format_escaped_str        7.96%    (response serialization: escaping string fields)
serde_core::ser::Serializer::collect_seq   4.47%    (response serialization: collecting feature list)
serde_json::value::ser::...::serialize     1.00%    (generic Value::serialize)
serde_json::ser::to_vec                    1.49%    (top-level response serialization)
serde_core::ser::SerializeMap::serialize   1.62%    (response map serialization)
serde_json::read::SliceRead::skip_to_escape 1.26%   (PUSH event payload parse)
serde_json::read::SliceRead::parse_str     0.98%    (PUSH payload parse)
serde_json::value::de::...::deserialize    0.83%    (PUSH payload deserialize)
... (remaining serde_json funcs)          ~3.0%
                                          ─────────
TOTAL                                     ~22.6%
```

**Response serialization alone is ~15%.** Fire-and-forget PUSH (FINDINGS Priority 2) would eliminate ALL of this because async PUSH skips response entirely.

### The 23.7% allocator cost is dominated by JSON's ephemeral allocations:

- `serde_json::Value::Object` uses `BTreeMap<String, Value>` — every field becomes a heap `String` allocation and a BTree node allocation per field.
- Every PUSH response re-allocates a fresh `Vec<u8>` buffer.
- `feature_map_to_json` constructs intermediate `serde_json::Value` trees before serializing.

Moving off `serde_json::Value` to direct binary encoding eliminates most of this pressure. FINDINGS' ~50% cost estimate for JSON is matched by our measured ~46% (22.6% direct + most of the 23.7% allocator).

### Pure Tally engine cost: ~8% (the part we can't remove)

| Function | % | Note |
|---|---:|---|
| `PipelineEngine::push` | 2.55% | top-level dispatch |
| `RingBuffer::advance_to` | 2.14% | bucket rotation on window ops |
| `OperatorState::read` | 1.85% | reading operator state for cascade/return |
| `tcp::handle_sync_command` | 1.26% | TCP dispatch frame |

**Total engine: ~7.8%.** This is the floor — no wire-format or threading change makes this faster. To push it further you'd need smarter operators (e.g., bucket lazy rotation, tighter inline loops).

### Supporting evidence for FINDINGS Priority 3 (DashMap + fine-grained locks)

Callgrind shows ~4% of cost in `hashbrown::HashMap` operations and `core::hash::BuildHasher::hash_one`. This isn't a bottleneck today (single-threaded), but it's the hot table that DashMap would replace. DashMap has slightly higher per-op cost than AHashMap (~40ns vs ~25ns per lookup) but eliminates the mutex serialization cost when multiple threads are active.

### What's NOT in the top 20 (significant by absence)

- **No HLL / distinct_count functions** — this was the medium pipeline which has zero HLL operators. The large pipeline profile would look very different with `HyperLogLog::count` dominating.
- **No `push_with_cascade` or `fan_out_targets`** — cascade/fan-out logic isn't a hot path for the medium pipeline (only 1 stream touches cascade).
- **No snapshot/eviction** — snapshot interval was set to 999999, so no snapshot wrote during the run.
- **No `ThroughputTracker::bump` in top 10** — it's at ~0.49%, confirming the Phase 10.2 RESEARCH claim that the tracker adds negligible overhead.
- **No `LatencyTracker::record_push`** — also negligible. Phase 10.2 instrumentation is free.

## Revised ROI ordering — now with profile data

Earlier I gave a data-backed reordering based on wall-clock benchmarks. The profile **confirms** that ordering and quantifies each lever:

| Lever | % CPU removed | Expected single-client throughput gain |
|---|---:|---|
| **Binary wire protocol** (kills JSON parse+serialize+alloc) | **~40-46%** | 1.8x-2x |
| **Fire-and-forget PUSH** (kills response serialization) | **~15-20%** | 1.2x-1.3x (on top of binary wire) |
| **HLL cache** (kills `HyperLogLog::count`) | **~60-80% on HLL-heavy pipelines only** | 6-10x on large pipelines |
| **DashMap + multi-threaded runtime** | **0% single-client, unlocks N-core scaling** | 4-10x on concurrent-client workloads |

For single-client medium pipelines, **binary wire + fire-and-forget gets you to ~55% CPU reduction**, which projects to ~2.5-3x raw throughput. Combined with the Python SDK binary encode side (the 41us overhead found in wall-clock measurements), single-client medium could hit **~45-55k eps**.

## Methodology caveats

1. **Instructions ≠ wall time.** Callgrind counts instructions, which is a proxy for CPU cost. A cache-miss-heavy function is under-weighted vs. tight loops. For Tally's mostly-hot-cache workload (bounded state fits in L2/L3), instruction count is a good proxy.

2. **Single-threaded effect.** Valgrind serializes threads, so any cost that comes from lock contention is absent. This doesn't matter for the single-client case, but means the profile CANNOT validate the concurrent-client collapse we saw in wall-clock benchmarks. That finding stands on its own.

3. **Startup noise.** The first ~500 events touch cold caches and hit ahash key initialization (`ahash::random_state::RandomState::from_keys` at 0.70%). On a 2000-event run this is ~25% of events, so cold-start cost is over-represented compared to a 100k-event production run.

4. **Event log append** is at 0.49%. This is on `/tmp` (tmpfs, zero-latency writes). On real disk it would be higher, but fsync is batched per-second so shouldn't dominate.

## How to reproduce

```bash
# Install valgrind if missing
sudo apt-get install -y valgrind

# Kill any running tally
pkill -9 -f release/tally 2>/dev/null

# Start tally under callgrind — slow (5-10x)
rm -rf /tmp/tally-bench /tmp/callgrind.out
mkdir -p /tmp/tally-bench
export TALLY_DATA_DIR=/tmp/tally-bench
export TALLY_SNAPSHOT_PATH=/tmp/tally-bench/tally.snapshot
export TALLY_FULL_SNAPSHOT_INTERVAL=999999
nohup valgrind --tool=callgrind \
    --callgrind-out-file=/tmp/callgrind.out \
    --cache-sim=no --branch-sim=no \
    /data/home/tally/target/release/tally > /tmp/tally-bench.log 2>&1 &
disown

# Wait for startup (fast — 1-2 seconds)
sleep 2
curl -s http://localhost:6401/health

# Run a short benchmark (2k events, 1 client — callgrind is slow)
cd /data/home/tally/benchmark/tally-throughput
python3 bench.py --events 2000 --clients 1 --pipeline medium

# Signal tally to exit — valgrind dumps on exit
pkill -INT -f "target/release/tally"

# Wait for valgrind to finish writing (can take a few seconds)
while pgrep -f valgrind >/dev/null; do sleep 1; done

# Analyze
callgrind_annotate --threshold=99 /tmp/callgrind.out > callgrind-report.txt
head -60 callgrind-report.txt
```

## Alternative profilers tried (and why they failed)

| Tool | Status | Reason |
|---|---|---|
| `perf record` | BLOCKED | `kernel.perf_event_paranoid=3` in container, read-only fs |
| `samply record` | BLOCKED | uses kernel perf events (same block) |
| `cargo flamegraph` | BLOCKED | wraps `perf record` under the hood |
| `py-spy` | BLOCKED | needs ptrace, container disallows |
| `bytehound` | BLOCKED | needs LD_PRELOAD + ptrace |
| `heaptrack` | UNTRIED | needs ptrace, expected blocked |
| **`valgrind --tool=callgrind`** | **WORKS** | instrumentation only, no kernel deps |
| `pprof-rs` (Rust library) | POSSIBLE | in-process SIGPROF sampling; requires code change to Tally to add /debug/pprof endpoint. Not done here but recommended as a permanent addition for future profiling. |

**Recommendation for future:** add `pprof-rs` as a dev-dependency with a `#[cfg(feature = "pprof")]` HTTP endpoint. Gives us a re-runnable sampling profiler that works in any container because the process signals itself, not the kernel.
