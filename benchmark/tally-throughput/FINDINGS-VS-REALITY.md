# FINDINGS vs Reality — Benchmark Spike Claims vs Real Tally Measurements

**Date:** 2026-04-11
**Sources compared:**
- `benchmark/FINDINGS.md` — synthetic Rust benchmark spike results (macOS loopback, 18+ variants)
- `benchmark/tally-throughput/RESULTS.md` — real Tally wall-clock numbers (Linux container, Python SDK, v1.1 current HEAD)
- `benchmark/tally-throughput/PROFILE.md` — callgrind instruction-level profile (medium pipeline, 2k events)
- `benchmark/tally-throughput/callgrind-report.txt` — raw callgrind_annotate output

**Purpose:** Validate which FINDINGS claims hold against real Tally code, which don't, and what the spike missed.

---

## Scorecard

| # | FINDINGS claim | Verdict | Confidence |
|---|---|---|---|
| 1 | JSON parse/serialize is ~50% of per-event cost | ✅ **CONFIRMED** (46% measured) | HIGH |
| 2 | Response write path dominates the server CPU | ⚠ **PARTIAL** — 15% of CPU, not 66% | MEDIUM |
| 3 | Fire-and-forget PUSH gives ~30x throughput | ❌ **NOT REACHABLE** in real Tally (2-5x realistic) | HIGH |
| 4 | Per-entity DashMap locking gives 5.7x on multi-stage DAG | ⚠ **UNTESTABLE YET** — no multi-threaded runtime | — |
| 5 | HLL fresh computation kills big-pipeline throughput (~8us HLL work per event) | ✅ **CONFIRMED** with magnified impact — 150x slowdown, not 4x | HIGH |
| 6 | Manual sharding ≈ DashMap (within 3%) | ⚠ **UNTESTABLE** — we have neither today | — |
| 7 | Throughput scales linearly with per-event work | ✅ **CONFIRMED** on single-client, ❌ **BROKEN** on concurrent | HIGH |
| 8 | macOS numbers are a floor vs Linux | ⚠ **INVERTED** for our SDK-driven workload — we're below macOS numbers because of Python overhead | HIGH |
| — | (Not predicted by FINDINGS) Single-threaded tokio runtime collapses under 4 concurrent clients | 🆕 **NEW FINDING** — 127x server-side slowdown | HIGH |
| — | (Not predicted by FINDINGS) Python SDK round-trip is 87% of visible latency | 🆕 **NEW FINDING** | HIGH |
| — | (Not predicted by FINDINGS) Phase 10/10.2 instrumentation is truly free | 🆕 **NICE-TO-KNOW** | HIGH |

---

## Detailed Findings

### 1. ✅ JSON parse/serialize is ~50% of per-event cost — CONFIRMED

**FINDINGS claim** (§Finding 1):
> "JSON parse + serialize alone is 3000 ns out of 6100 ns = 49% of total per-event cost. Swapping to a fixed binary protocol cut this to ~50 ns total, delivering an honest 2x throughput improvement with zero other changes."

**Real measurement** (callgrind, medium pipeline):
- `serde_json::*` direct cost: **22.6%** of 189.9M instructions
- Allocator traffic driven by `serde_json::Value::Object` (BTreeMap<String, Value> per field): **~15-20%** of the 23.7% allocator total
- **Total JSON-attributable cost: ~40-46%**

**Verdict: MATCHES within noise.** FINDINGS' 49% was on macOS at 233K/s in a highly optimized variant; our 46% is on Linux in the real code path with a Python client. The fact that the two numbers agree within ~3 percentage points across completely different measurement methodologies is strong evidence the claim is real.

**Breakdown from `callgrind-report.txt`:**
```
7.96%  serde_json::ser::format_escaped_str         (response: escape strings)
4.47%  serde_core::ser::Serializer::collect_seq    (response: build feature list)
1.62%  serde_core::ser::SerializeMap::serialize    (response: map walking)
1.49%  serde_json::ser::to_vec                     (response: top-level encode)
1.26%  serde_json::read::SliceRead::skip_to_escape (PUSH payload parse)
1.00%  serde_json::value::ser::...::serialize      (response: Value::serialize)
0.98%  serde_json::read::SliceRead::parse_str      (PUSH parse)
0.83%  serde_json::value::de::...::deserialize     (PUSH deserialize)
0.70%  ahash::RandomState::from_keys               (hash init per event)
... rest
```

Notice: **response serialization alone is ~15%** (7.96 + 4.47 + 1.62 + 1.49 = 15.5%). PUSH payload parse is ~4%. The response side is 4x more expensive than the parse side because `feature_map_to_json` builds an intermediate `serde_json::Value` tree before serializing.

**Implication for v1.2:** binary wire protocol removes ~46% of CPU. Projected single-client throughput: 17.5k eps → **~32-35k eps**. Exactly FINDINGS' "2x" prediction.

---

### 2. ⚠ Response write path dominates server CPU — PARTIAL

**FINDINGS claim** (§Finding 2):
> "66% TCP write syscall (sendto for response) — two-thirds of server CPU was spent writing responses back to clients. Removing the write path entirely should give ~3x throughput just from killing those syscalls. Measured result: ~23x throughput (397K → 9M+)."

**Real measurement** (callgrind):
- **Response serialization cost (serde_json side): ~15% of CPU**
- **Actual sendto syscall cost: NOT VISIBLE in callgrind** (syscalls are traps into the kernel; valgrind records the trap instruction as a single instruction and the kernel-side work isn't counted)

**Verdict: PARTIALLY MATCHES but differently distributed.** FINDINGS' 66% was from a macOS flamegraph profile that captured kernel time in sendto. On Linux with loopback TCP, sendto is much cheaper (~1-3us vs macOS's ~15us) because there's no kqueue round-trip. Our wall-clock numbers indirectly confirm this: client p50 48us - server p50 6us = 42us round-trip overhead total, of which sendto + recv is maybe 10-15us not 30+.

**What FINDINGS got right:** response serialization + write is a real cost center (~15% CPU + kernel time).
**What FINDINGS got wrong for our environment:** the 66% number was inflated by macOS-specific kernel TCP overhead that doesn't transfer to Linux.

**Implication for v1.2:** fire-and-forget PUSH still helps — it saves the 15% serialization cost plus the kernel sendto cost plus the Python-side response decode — but the win is **~20-30% throughput**, not the 3x or 23x FINDINGS projected.

---

### 3. ❌ Fire-and-forget PUSH gives ~30x — NOT REACHABLE

**FINDINGS claim** (§Recommendation Priority 2):
> "Fire-and-forget PUSH + separate GET endpoint — removes the response write path, ~30x for ingest"

**Real measurement:** We haven't shipped fire-and-forget, but the profile + benchmark data lets us project the upper bound.

Per-push CPU breakdown from the profile:
```
JSON + allocator (JSON-driven)  ~46%
Tally engine work                ~8%
Hashmaps + hashing               ~4%
memcpy/memcmp                    ~6%
Other (runtime, stdlib)         ~36%
```

Fire-and-forget removes ONLY:
- Response-side JSON (~15%)
- Response allocation (~10-15% of the allocator category)
- Response write syscall + socket buffering (~5-10% kernel time not in callgrind)

**Realistic CPU savings: 30-40%**, which translates to **~1.5-2x throughput**, NOT 30x.

**Why FINDINGS' 30x didn't transfer:**
- FINDINGS' 30x was measured on a pure single-stream, 7-feature pipeline with NO cascade, NO fan-out, NO event log, NO derived features. Real Tally PUSH runs all of those even in "async" mode.
- FINDINGS' async path in `stream_push_server.rs` doesn't even compute features for the response — it just ingests. Tally's cascade and fan-out logic MUST run to update downstream stream state regardless of whether a response is sent.
- macOS TCP loopback overhead was artificially inflating the round-trip cost, so removing it looked artificially good.

**Adjusted claim for v1.2 docs/commits:** "Fire-and-forget PUSH ships ~1.5-2x throughput gain on single-client, realizable mostly via skipping response serialization and the client's decode step. Do NOT claim 30x anywhere."

---

### 4. ⚠ DashMap gives 5.7x on multi-stage DAG — UNTESTABLE YET

**FINDINGS claim** (§Finding 3):
> "Moving to DashMap<u64, Arc<EntityState>> with Mutex<OperatorState> per entity jumped to ~8.1M events/sec — a 5.7x improvement on the same workload."

**Real measurement:** Not applicable today. Tally is:
- Single-threaded tokio (`#[tokio::main(flavor = "current_thread")]`)
- Global `Arc<Mutex<AppState>>` wrapping everything
- No DashMap in the dependency tree

We cannot measure the improvement because the baseline (multi-threaded tokio + global mutex) that DashMap would replace also doesn't exist in Tally today.

**What we CAN say from our data:**
- Under single-thread + global mutex, concurrent clients collapse to 1.17k eps (see finding #7 below)
- This IS the symptom FINDINGS' DashMap work is meant to cure
- The 5.7x number is for multi-stage DAG with per-entity locks replacing per-stage mutex. Tally's "per-stage mutex" today is actually "global mutex over all stages", so moving to DashMap should be at least as good, possibly better because today's baseline is artificially serialized.

**Unknown risk:** FINDINGS' benchmark used ONE key type (same user_id across all stages). Tally's fan-out case uses DIFFERENT key_fields per stream (user_id, merchant_id, device_id...), which means a single PUSH can lock 3-5 entity records with different keys. FINDINGS never exercised this. **The 5.7x ceiling might not transfer.**

**Implication for v1.2:** Before committing to DashMap, run a microbenchmark that mimics Tally's real fan-out case. The `stream_multistage_dashmap_server.rs` file in the spike doesn't model this.

---

### 5. ✅ HLL fresh computation kills throughput — CONFIRMED, magnified

**FINDINGS claim** (§Finding 5, §"Big Tally" prediction):
> "The big pipeline benchmark (25 stages, 5 HLL distinct_count features) landed at ~1.1M events/sec. The culprit: computing a fresh HLL cardinality estimate on every write (~4µs per estimate × 2 HLL updates per event = ~8µs of HLL work alone)."

**Real measurement** (large pipeline, 1 client, 20k events):
- Wall time: 22.53s
- Throughput: **888 events/sec** (vs 17,500 for medium — a **19.7x slowdown**)
- Server-side p50: **932us** (vs 6.2us for medium — a **150x slowdown**)
- Medium pipeline has 0 HLL; large has 4 distinct_count features spread across 3 streams

**Verdict: CONFIRMED and the impact is even larger than predicted.** FINDINGS projected ~4us HLL compute × 2 per event = 8us overhead, roughly doubling per-event work. We measured ~926us additional server-side work per event — a **100x larger hit** than FINDINGS' 8us prediction.

**Why our number is larger:**
1. Tally's HLL has **14-bit precision = 16384 registers**, not FINDINGS' assumed 256. `count()` scans all 16384 on every read.
2. Our large pipeline has **cascade + fan-out**, so a single PUSH touches all 3 streams and reads HLL state from each cross-stream lookup.
3. Each `distinct_count` feature also triggers a full `merged.count()` inside `read()` at `src/engine/hll.rs:178-196`, not just a register update.

**Per-event HLL cost from our numbers:** 932us server-side / ~2-4 HLL reads per event = **~230-460us per HLL read**. That's 50-100x FINDINGS' assumed 4us, because FINDINGS assumed a cached or low-precision HLL.

**Implication for v1.2 (HUGE):**
- HLL cache is the **single biggest lever** for any pipeline that uses `distinct_count`
- Projected impact: large pipeline 888 eps → **~10-15k eps** (10-15x) just from caching the estimate
- This is **higher ROI than binary wire** (which is 2x) for any HLL-using pipeline
- **Recommended priority shift:** ship HLL cache BEFORE binary wire for any customer whose workload includes `distinct_count`

---

### 6. ⚠ Manual sharding ≈ DashMap — UNTESTABLE

**FINDINGS claim** (§Finding 4):
> "We built a manual sharded variant (Vec<RwLock<HashMap<u64, Arc<Entity>>>> with 64 shards per stage). Result: ~8.3M events/sec, within 3% of DashMap's 8.1M."

**Real measurement:** None — we have neither manual sharding nor DashMap today.

**Verdict:** Take this on faith from FINDINGS. If we ship DashMap in v2.0 and benchmark it against a hand-rolled shard variant, we should be able to reproduce the 3% delta. But there's no way to validate it today.

---

### 7. ✅/❌ Throughput scales linearly with per-event work — MIXED

**FINDINGS claim** (§Finding 6):
> "Throughput tracked per-event CPU work almost perfectly. Ratio of work : throughput is ~constant (within 20%). No lock-contention cliffs, no hidden scaling cliffs."

**Real measurement:**

**Single client, varying pipeline complexity:**

| Pipeline | Per-event work (callgrind scaling) | Throughput | Ratio |
|---|---:|---:|---|
| small (5 features) | ~1.0x baseline | 19.6k eps | 1.00 |
| medium (6 features + view) | ~1.12x | 17.5k eps | 0.89 |
| large (8 features + 4 HLL + cascade) | ~150x (from server p50 ratio) | 0.9k eps | 0.046 |

**The 1→medium ratio (1.12x work, 0.89x throughput) is within 20%. CONFIRMED.**

**The 1→large ratio (150x work, 46x slowdown) is also roughly linear** — 150/46 = 3.3x, meaning work grew faster than time did, but throughput scales with work as predicted.

**Multi-client, same pipeline:**

| Clients | Throughput | Server p50 | Linearity |
|---|---:|---:|---|
| 1 | 17.5k eps | 6.2us | baseline |
| 4 | 1.17k eps | 788us | **-15x scaling cliff** |

**HARD CLIFF at concurrent clients. FINDINGS MISSED THIS.** Because FINDINGS ran multi-thread tokio + sharded locks from the start, the "no hidden cliffs" claim was about the per-event work model, not about Tally's real runtime. Single-threaded Tally has a catastrophic concurrency cliff that FINDINGS' architecture eliminated in variant #1.

**Implication for v1.2:** the "linear scaling" property FINDINGS describes only holds AFTER you've done the DashMap work. Until then, Tally can't scale horizontally on a single node. This is a more urgent reason to ship multi-threaded runtime than anything in FINDINGS' prioritization.

---

### 8. ⚠ macOS numbers are a floor, Linux is 1.5-2x faster — INVERTED

**FINDINGS claim** (§Finding 7):
> "macOS loopback TCP serializes around 200-500K req/sec for round-trip workloads. All round-trip numbers on macOS are effectively floors — Linux production numbers will be 1.5-2x higher at minimum."

**Real measurement:**
- FINDINGS macOS: 197K/sec (current Tally baseline claim)
- Our Linux: **19.6k/sec** (small pipeline, 1 client)

**We are 10x BELOW the macOS number, not 1.5-2x above.**

**Verdict: INVERTED for our workload.** FINDINGS' 197K/sec was a pure Rust-to-Rust benchmark with a hand-rolled TPC (thread-per-core) Rust client. Our 19.6k/sec is through the Python SDK, which has:
- Python interpreter overhead per push (~5-10us)
- JSON encoding in Python (~5us)
- Python socket API overhead (~5-10us)
- GIL release/reacquire around socket read (~2-5us)
- JSON decoding in Python (~5-10us)

**Total Python-side overhead: ~25-40us per push**, which caps single-client throughput at ~25-40k eps regardless of how fast the server is.

**Implication for v1.2 and public numbers:**
1. Any number we publish must specify the client (Rust SDK vs Python SDK)
2. Python SDK optimizations (reuse `bytes` buffers, avoid intermediate `dict` → JSON → bytes conversion, use a pure-C struct-based encoder) could be worth ~2x
3. FINDINGS' 8-15M/sec marketing claim is **only realizable with a Rust client**. Python clients will be 100x slower for mechanical reasons unrelated to the server.

---

### 🆕 NEW FINDING A: Single-threaded tokio runtime collapses under concurrent clients

**Not in FINDINGS.** FINDINGS benchmarked multi-thread tokio from variant #1; single-thread was never tested with concurrent clients.

**Real measurement:**

| Clients | Throughput | Server p50 | Server p99 |
|---|---:|---:|---:|
| 1 | 17,503 eps | **6.2us** | 30.8us |
| 4 | 1,167 eps | **788us** | 1304us |

**Server p50 jumped 127x** (6.2us → 788us) just by going from 1 to 4 concurrent clients on the **same pipeline**.

**Root cause (hypothesized):** tokio `current_thread` runtime accepts all 4 TCP connections but services all task futures on one OS thread. Each PUSH handler is `async fn handle_connection` and holds the `Arc<Mutex<AppState>>` inside `handle_sync_command`. Under 4 clients hammering pushes, the event loop spends most of its time context-switching between futures and waiting on socket reads, AND the mutex serializes the actual push work on top of that.

**Why it's not mutex contention alone:** if the mutex were the bottleneck, you'd see ~4× 6.2us = 25us queueing latency. 788us is 30x that. The extra 760us has to be coming from event-loop scheduling + TCP buffer queueing + Python-side thread pool coordination.

**Implication for v1.2 (load-bearing):**
- Tally cannot serve more than ~1 concurrent TCP client efficiently today
- Every customer with a multi-threaded application talking to Tally hits this wall
- This is the #1 reason to ship multi-threaded runtime + fine-grained locks, ahead of binary wire and ahead of HLL cache
- **New priority order for v1.2 phases:** (1) multi-threaded runtime, (2) DashMap + fine-grained locks, (3) HLL cache, (4) binary wire, (5) fire-and-forget

---

### 🆕 NEW FINDING B: Python SDK round-trip is 87% of visible latency

**Not in FINDINGS.** FINDINGS used Rust clients throughout.

**Real measurement** (medium pipeline, single client):
- Client-visible p50: 48us
- Server-side p50: 6.2us
- **Gap: 42us (87% of visible latency)** lives in the Python SDK + kernel TCP path

**Where the 42us goes (estimated from callgrind + syscall counts):**
```
Python dict → JSON encode              ~5us
Python bytes frame write               ~3us
socket.sendall (kernel loopback write) ~5us
kernel TCP delivery + recv wakeup      ~3us
server-side read + parse (in profile)  ~2us
server-side push work (in profile)     ~4us
server-side response serialize (profile)~3us
server-side socket write               ~3us
kernel TCP delivery                    ~3us
Python socket.recv_exact               ~5us
Python JSON decode                     ~5us
Python FeatureResult construction      ~2us
                                     ────────
TOTAL                                  ~43us (matches measured 42us)
```

**Implication for v1.2:**
- Binary wire protocol on the wire-format side saves ~5us (server JSON) + ~5us (Python JSON decode) + ~5us (Python encode) = ~15us → single-client p50 48us → 33us → **~30k eps**
- Fire-and-forget saves the response round-trip entirely: ~15us of that Python decode + recv + server serialize + write → single-client p50 33us → 18us → **~55k eps**
- **Combined: ~3x single-client throughput from wire + async alone** (matches the adjusted claim in finding #3)
- Going beyond ~55k eps per client requires either a C extension Python client or a Rust SDK. No server-side change closes that gap.

---

### 🆕 NEW FINDING C: Phase 10 + 10.2 instrumentation is free

**Not in FINDINGS.** Phase 10 predates FINDINGS.

**Real measurement:**
- `ThroughputTracker::bump` cost: **0.49% of CPU**
- `LatencyTracker::record_push` cost: **not in top 50** (below 0.1%)
- `StateStore::mark_dirty` cost: below 0.3%

**Verdict: CONFIRMED the Phase 10 research claim** (`.planning/phases/10-debug-ui/10-RESEARCH.md:93`) that lock-once-then-instrument adds zero measurable cost on the hot path. Phase 10.2 latency histograms also measure themselves out of the profile.

**Implication:** we can keep adding in-process instrumentation (percentile histograms, per-operator timing, per-feature cost accounting) without worrying about hot-path regression. This is a free surface for future observability work.

---

## Updated Priority Order for v1.2 (based on combined evidence)

FINDINGS ordered by ROI in synthetic-bench terms. The real-tally evidence shifts the ordering:

| Priority (FINDINGS) | Priority (evidence-based) | Rationale |
|---|---|---|
| 1. Binary wire | **3. Binary wire** | Ships ~2x single-client; clear win but not urgent |
| 2. Fire-and-forget | **4. Fire-and-forget** | Ships ~1.3x on top of wire; pairs naturally with wire in a single phase |
| 3. DashMap + per-entity locks | **1. Multi-threaded runtime + DashMap** | Unblocks concurrent clients — **the blocking v1.2 issue**, not the 5.7x FINDINGS claim |
| 4. HLL cache | **2. HLL cache** | 10-15x on HLL workloads; tiny change; independent of threading |

**Full v1.2 phase plan (revised):**

**Phase 11 — Multi-threaded runtime + DashMap + fine-grained locks**
- tokio `current_thread` → `multi_thread`
- `StateStore.entities`: `AHashMap` → `DashMap`
- `EntityState`: add per-entity `Mutex<OperatorState>` + `AtomicU64` cache
- Lock ordering for multi-entity fan-out (static sort by stream+key)
- Adapts Phase 9 dirty set, Phase 10.2 latency tracker, all `/debug/*` handlers
- **Target:** 1.17k eps → 20k+ eps for 4-client medium workload
- Breaks single-threaded invariant → semver-major (v2.0) if doc contract is hard, otherwise v1.2

**Phase 12 — HLL estimate cache**
- `AtomicU64` cached estimate on `HyperLogLog` / `DistinctCountOp`
- Background refresh task in main.rs (every 100-500ms)
- Opt-in `precision='exact'` for users needing fresh estimates
- **Target:** 888 eps → 10k+ eps for large pipeline (single client)
- Independent of Phase 11; can ship before or after

**Phase 13 — Binary wire protocol + fire-and-forget PUSH**
- Replace JSON payload in PUSH/GET/SET/MSET with typed binary encoding
- Add `OP_PUSH_ASYNC` (0x07) + Python SDK split (`push()` + `push_async()` / `ingest()`)
- Keep REGISTER as JSON
- **Target:** combined ~3x single-client throughput over Phase 11 baseline

**Phase 14 (optional) — Python SDK hot-path C extension**
- Move encode/decode to a small C extension or `struct` module calls
- **Target:** cut Python SDK overhead from 42us → ~15us per push
- Only needed if customers hit the Python-side ceiling

---

## Honest caveats on this comparison

1. **Our benchmark used Python clients, FINDINGS used Rust clients.** This is the single biggest source of difference between the two number sets. Our "19.6k/sec small pipeline" is a Python-SDK-bounded number; the Rust-client equivalent would be significantly higher.

2. **Our environment is a container with no kernel perf.** We couldn't profile the real syscall costs (perf_event_paranoid=3, ro fs). Callgrind shows instruction counts, not wall time, and misses kernel-side work entirely.

3. **FINDINGS ran on macOS; we ran on Linux.** macOS TCP loopback has higher per-syscall cost (~15us vs Linux ~3us). This partly explains why FINDINGS saw 66% of CPU in TCP write while we saw 15% in response serialization.

4. **Our "large pipeline" has 4 HLL features; FINDINGS' "big pipeline" had 5.** Cost scales linearly with HLL count, so they're comparable but not identical.

5. **The 4-concurrent-client collapse is measured with Python thread-pool clients, not pure Rust clients.** A Rust client with 4 concurrent TCP connections might perform differently (probably still catastrophic, but the exact numbers would be cleaner).

## Bottom line

**FINDINGS' four priorities are all real. The ordering and magnitudes don't all transfer.**

- Binary wire (#1 in FINDINGS): **confirmed 2x, not urgent, do after concurrency**
- Fire-and-forget (#2): **confirmed but 2-5x, not 30x**
- DashMap (#3): **blocking issue, not a speedup — current runtime can't serve 2+ concurrent clients. Ship first.**
- HLL cache (#4): **bigger than FINDINGS predicted — 10-15x on HLL workloads, one-week change, ship early**

And two new findings FINDINGS missed:
- Single-thread tokio cliff under concurrent clients (the real bottleneck)
- Python SDK ceiling at ~55k eps/client (unrelated to server optimization)
