# Path from 17.5k → 100k → 1M events/sec

**Question:** what would it take Tally to hit 100k eps? 1M eps?
**Short answer:** 100k single-node is a one-phase change. 1M single-node is reachable but not with Python clients on round-trip semantics.

**Data sources:**
- `RESULTS.md` — real wall-clock numbers from v1.1 HEAD
- `PROFILE.md` / `callgrind-report.txt` — 46% JSON, 8% Tally engine, ~36% other
- `FINDINGS-VS-REALITY.md` — validated + debunked claims from the earlier spike
- `FINDINGS.md` — the spike itself

---

## 1. Where we are today (measured, v1.1 HEAD)

| Setup | Pipeline | Clients | Throughput | Server p50 | Client p50 |
|---|---|---:|---:|---:|---:|
| Real Tally (Python SDK) | medium | 1 | **17,503 eps** | 6.2us | 48us |
| Real Tally (Python SDK) | medium | 4 | **1,167 eps** ⚠ | 788us ⚠ | 3155us |
| Real Tally (Python SDK) | large | 1 | **888 eps** ⚠ | 932us ⚠ | 1034us |

**Baseline: 17.5k eps on medium, single Python client, round-trip.** Everything else either scales down (HLL, concurrent) or shows what's blocking.

---

## 2. Cost model — where each microsecond goes per PUSH (medium pipeline, 1 client)

From callgrind + wall-clock subtraction:

```
Client-visible per-push:                  48 us  (p50)
├─ Python encode + dict→JSON:              ~8 us  │
├─ socket.sendall + kernel TCP write:      ~5 us  │ Python SDK + kernel
│                                                 │  = 42us (87% of visible)
├─── [server receives bytes]                      │
│                                                 │
├─ serde_json::from_slice (parse event):  ~0.5us  │
├─ feature_map_to_json + serde_json::to_vec:~1us  │ Server work
├─ PipelineEngine::push (inc. ring buffer):~1.5us │  = 6.2us (13%)
├─ RingBuffer::advance_to + operator reads:~1.5us │
├─ hashmap + ahash + throughput tracker:  ~1.2us  │
├─ response write + ThroughputTracker:    ~1.0us  │
│                                                 │
├─── [response bytes sent back]                   │
│                                                 │
├─ kernel TCP delivery + socket.recv:      ~5 us  │ Python SDK + kernel
├─ Python JSON decode:                     ~5 us  │
├─ Python FeatureResult construction:      ~2 us  │
└─ other Python:                           ~2 us  │
                                         ─────────
                                           48 us
```

**Two ceilings to understand:**

- **Server-side ceiling (single-thread, medium, no HLL):** 1 / 6.2us = **~161k eps theoretical**. Gated by engine work.
- **Python SDK ceiling (round-trip, one client):** 1 / 48us = **~20.8k eps actual**. Gated by Python + kernel round-trip.

**The gap between these two (17.5k measured vs 161k theoretical) is ~90% Python + kernel overhead, only ~10% server work.**

---

## 3. What each lever saves (from the profile)

Numbers below are per-event cost change, with the resulting single-client Python SDK ceiling.

### Lever A: HLL estimate cache (FINDINGS P4)

**Scope:** only affects pipelines with `distinct_count` features.
**Cost removed:** for large pipeline, ~900us of `HyperLogLog::count()` scans per event. For medium, zero.
**Effort:** 2-3 days (AtomicU64 cached field + background refresh task).
**Impact:**
- **Large pipeline single-client:** 888 eps → **~10-15k eps** (10-15x)
- Medium: unchanged

### Lever B: Binary wire protocol (FINDINGS P1)

**Scope:** all pipelines.
**Cost removed:** 46% of server CPU (JSON + JSON-driven allocator traffic) + ~10us of Python JSON encode/decode per push.
**Effort:** 1 week (new binary codec + Python SDK mirror).
**Impact (single client, medium):**
- Server per-event: 6.2us → **~3.3us**
- Python overhead: 42us → **~32us**
- Client-visible p50: 48us → **~35us**
- **Throughput: 17.5k eps → ~28-32k eps (~1.8x)**

### Lever C: Fire-and-forget PUSH (FINDINGS P2)

**Scope:** all pipelines, but only when client opts in (no feature response).
**Cost removed:** response serialization (~1us server), response network round-trip (~5us), Python decode (~5us). Total ~11us per event from client-visible side.
**Effort:** 3-5 days (new opcode + SDK split).
**Impact (stacked on Lever B, single client, medium):**
- Client-visible p50: 35us → **~18-20us**
- **Throughput: 32k eps → ~50-55k eps** (single client, pipelined writes)

### Lever D: Multi-threaded runtime + DashMap + per-entity locks (FINDINGS P3)

**Scope:** concurrent clients only. Zero benefit on single-client workloads.
**Cost removed:** the single-threaded tokio cliff that causes the **127x server-side regression** (6.2us → 788us) when 4 clients connect.
**Effort:** 2-3 weeks (the SemVer-major change — state store refactor + lock ordering + Phase 9/10/10.2 adaptation).
**Impact (4 clients, medium, stacked on B+C):**
- Per-client throughput (after A+B+C): ~50k eps
- **4 clients × 50k = ~200k eps per node** (best case, lock ordering works)
- **8 clients × 50k = ~400k eps per node**

### Lever E: Batching — N events per TCP round-trip

**Scope:** all pipelines. Client-side change only.
**Cost removed:** amortizes the 48us Python round-trip over N events.
**Effort:** 3-5 days (client bundles events into one PUSH frame, server iterates in one lock acquire).
**Impact (100 events per batch, single client):**
- Round-trip cost per event: 48us / 100 = **0.48us** amortized
- Server work per event (medium, no HLL): still 6.2us each (or 3.3us with binary wire)
- **Bottleneck flips to server:** total per-event = max(0.48us client, 6.2us server) = 6.2us
- **Single-client throughput: 17.5k eps → ~120-150k eps** (just from batching, NO server changes)
- With binary wire stacked: single-client → ~280-330k eps
- **FINDINGS dismissed this because FINDINGS was already in fire-and-forget mode.** For round-trip workloads, batching is the single biggest lever on Python clients.

### Lever F: Rust client SDK (not in FINDINGS' priority list)

**Scope:** client-side overhead only.
**Cost removed:** Python encode (8us) + Python decode (5us) + FeatureResult (2us) + GIL coordination (2-5us). Total ~15-20us of the 42us Python round-trip.
**Effort:** 2-3 weeks (new SDK crate, maintenance burden).
**Impact (single client):**
- Round-trip cost: 48us → **~15-20us**
- **Single-client throughput: 17.5k → 55-70k eps** (pure Rust, round-trip, no batching, no server changes)
- Stacked with all other levers: **300-500k eps per client**

---

## 4. Target tiers — what it takes to reach each throughput

### Tier 1: 100k eps on a single node

**Three paths that each work independently:**

**Path 1a — Cheapest: Batching only (Lever E)**
- **Effort:** 3-5 days
- **Server changes:** none (or minor — MSET-style multi-event PUSH handling)
- **Client changes:** Python SDK batches events before sending
- **Result:** single client → **~120k eps** on medium pipeline
- **Caveat:** round-trip semantics (feature response) work but are per-batch, not per-event. User has to accept that features come back as an array-per-batch.

**Path 1b — Classic: Binary wire + fire-and-forget + 4 Python clients (Levers B + C + D)**
- **Effort:** 3-4 weeks (wire + async + multi-thread runtime)
- **Result:** 4 clients × 30k eps = **~120k eps**
- **Prerequisite:** Lever D (multi-thread) is mandatory because 4 clients on single-thread tokio collapses.

**Path 1c — Hardcore: Multi-thread runtime alone + 8-16 Python clients (Lever D only)**
- **Effort:** 2-3 weeks
- **Result:** 8 clients × ~15k eps each = **~120k eps**
- **Simplest theoretically** but requires the most invasive code change (SemVer-major).

**Recommended:** **Path 1a (batching)** gets to 100k fastest with the smallest change, and works for any customer who can tolerate batched feature responses. Ship batching first, then decide if the remaining levers are worth it.

---

### Tier 2: 300-500k eps on a single node

**Realistic path:** Binary wire + fire-and-forget + multi-thread runtime + DashMap (Levers B + C + D).

- Per-event server cost: 6.2us → ~2.4us (wire + async-skip-response)
- Concurrent-client scaling: 1x → ~4-8x (multi-thread)
- Python SDK overhead: 48us → ~18us (wire + async)
- Per-client throughput: 50-60k eps
- **8 clients × 60k = 480k eps per node**

Effort: ~6 weeks total (the full FINDINGS v1.2 priority 1-3 set).
Prerequisite: 8-core Linux host.
Caveat: this is Python round-trip. Fire-and-forget clients never see feature responses inline.

**Alternative with Lever E (batching) instead of Lever C (fire-and-forget):** 4 clients × 250k eps batched = 1M eps. See Tier 3.

---

### Tier 3: 1M eps on a single node

**Two realistic paths:**

**Path 3a — Python clients with batching (Levers A + B + D + E)**
- Binary wire: server cost → 3.3us
- Multi-thread + DashMap: unlocks 8-core scaling
- Batching (100 events per PUSH): amortizes Python overhead to 0.48us
- Effective per-event cost (server-bound): 3.3us / 8 cores = **0.41us with perfect parallelism**
- **Theoretical ceiling: ~2.4M eps**
- **Realistic (cache effects, lock ordering, tokio scheduling): ~1-1.5M eps**

Effort: ~5-6 weeks (wire + DashMap + batching).
Caveat: feature responses come back per-batch, not per-event. The SDK builds a list of 100 FeatureResults per `push_batch()` call.

**Path 3b — Rust client (no Python, Lever F + B + C + D)**
- Server cost: 2.4us (wire + async, no response path)
- Rust client round-trip: ~3us (no Python overhead)
- Single-client: 1 / 3us = **~330k eps per client thread**
- 4 Rust client threads × 330k = **~1.3M eps per node**

Effort: ~3 weeks of Rust SDK work on top of the server work.
Pros: no SDK API changes for existing Python users; Rust SDK is opt-in for high-throughput customers.
Cons: you now maintain two SDKs.

**Path 3c — Fire-and-forget batching (the nuclear option)**
- Server: binary wire + multi-threaded + DashMap + HLL cache
- Client: Rust or Python batches N events, no response expected
- Effective per-event budget: just the server-side operator work on N/N cores
- **FINDINGS bench variant #5 hit 12M/sec on macOS in this shape.** Linux should be 1.5-2x.
- **Realistic: 8-15M eps per node** (matches FINDINGS' projected Linux "medium pipeline" number)

Effort: adds ~3 days to Path 3a for the fire-and-forget PUSH opcode.
Caveat: lose per-event feature responses entirely. This is the Kafka-producer-acks=0 mode.

---

## 5. The thing FINDINGS missed: batching changes everything for Python clients

FINDINGS §"Things we ruled out" dismissed batching:
> "Wire-level batching (N events per request) — Would add SDK complexity for at most 20-30% gain on small/medium pipelines and ~5% on big pipelines"

**That was wrong for round-trip Python workloads.** FINDINGS measured batching in fire-and-forget mode, where the round-trip is already eliminated. For round-trip Python, the round-trip IS the bottleneck (42us of 48us), so batching is a **10-100x lever**, not 20%.

**Updated math for 100 events/batch, medium pipeline, 1 Python client, round-trip:**

```
Per-batch cost:
  Python encode 100 events into one frame:  ~15us
  socket.sendall (bigger payload):           ~8us
  Server deserialize + loop 100 pushes:     ~350us  (100 × 3.3us with wire)
  Server serialize 100 feature maps:         ~150us  (100 × 1.5us)
  socket.recv + Python decode 100 results:  ~25us
                                          ─────────
                                           ~548us per batch

Per-event amortized: 548 / 100 = 5.48us per event
Throughput: 1 / 5.48us = ~180k events/sec PER CLIENT
```

**Single Python client + binary wire + batching: ~180k eps. No multi-threading needed, no fire-and-forget needed, no Rust client needed.**

**4 Python clients batched (single-thread server): 4 × 180k / single-thread serialization = ???**
Here you hit the 127x concurrency cliff again. Multi-thread runtime is still needed for concurrent clients, but batching single-client is surprisingly cheap.

---

## 6. What each target really needs (summary)

| Target | Required levers | Effort | SemVer | Caveats |
|---|---|---|---|---|
| **50k eps** (1 Python client) | B (binary wire) + E (batching 10/batch) | 1-2 weeks | v1.2 minor | round-trip still works |
| **100k eps** (1 Python client) | B + E (batching 100/batch) | 1-2 weeks | v1.2 minor | response is per-batch |
| **200k eps** (4 Python clients) | B + D (multi-thread runtime) | 3-4 weeks | v1.2 minor or v2.0 | breaks single-thread invariant |
| **500k eps** (8 Python clients) | B + C + D | 5-6 weeks | v2.0 likely | fire-and-forget, no inline features |
| **1M eps** (Python, batched) | B + D + E | 4-5 weeks | v2.0 likely | round-trip OK, per-batch responses |
| **1M eps** (Rust client) | B + C + D + F | 5-6 weeks | v2.0 | new SDK, lower total effort than it sounds |
| **5M eps** (fire-and-forget Rust) | A + B + C + D + F | 6-8 weeks | v2.0 | no feature responses at all |
| **FINDINGS' 8-15M eps claim** | everything above + Linux bare metal | 8 weeks + hardware | v2.0 | marketing number, realistic on 16-core |

---

## 7. The fundamental floors (nothing makes these faster)

1. **Tally engine work (the 8% core):** 0.5us per event for `push` + `advance_to` + `OperatorState::read` on a hot cache. Can be reduced by smarter operators (lazy bucket rotation, SIMD where possible) but probably not below ~0.3us for a realistic feature count. **Floor: 0.3-0.5us per event on medium pipeline.**

2. **Loopback TCP round-trip:** ~2-3us for sendto + recvfrom pair on Linux. Can't go below this without UDS or shared memory. **Floor: 3us per round-trip.**

3. **HLL fresh computation:** ~230-460us per HLL feature per event on 14-bit precision. Caching eliminates it for reads (1us), but writes still touch registers (~25ns). Lowering precision to 12-bit (4096 registers) cuts scan cost 4x. **Floor after caching: ~25-50ns per HLL write.**

4. **Python interpreter overhead:** ~1-2us per function call. Encode/decode for binary format with native struct module: ~2-5us per event. **Floor: 5-10us per event per Python client.** This is why Path 3a uses batching — it amortizes this floor across N events.

---

## 8. My recommended ordering for hitting each tier

### Stage 1 (2 weeks) — Ship to 100k eps
- **Batching first (Lever E).** 3-5 days. Biggest single-lever gain for Python SDK users. `push_batch(events: list)` returns `list[FeatureResult]`. No server changes needed, just multi-event PUSH framing.
- **HLL cache (Lever A).** 2-3 days. Orthogonal; helps any HLL-using customer regardless of throughput.
- **Combined result:** 100-150k eps single Python client on medium pipelines, 10-15k eps on large pipelines.

### Stage 2 (3 weeks) — Ship binary wire + fire-and-forget (Levers B + C)
- Binary wire cuts server CPU 46% and Python encode/decode 10us per event.
- Fire-and-forget opcode for ingest-only workloads.
- **Combined with Stage 1: 180-250k eps single Python client batched.**

### Stage 3 (3-4 weeks) — Multi-threaded runtime + DashMap (Lever D)
- The SemVer-major change. Unblocks concurrent clients.
- **Combined result:** 500k-1M eps per node for 4-8 concurrent Python clients, ~250k per client.

### Stage 4 (optional, 2-3 weeks) — Rust SDK (Lever F)
- Only if customers actually hit the Python ceiling at ~250k eps/client.
- Gets individual clients to 500k-1M eps.

**Total to 1M eps:** ~8-10 weeks of focused work (matches FINDINGS' original estimate, but with different phase ordering).

---

## 9. What doesn't matter (scoping out)

- **Compio / monoio / glommio runtimes.** FINDINGS already ruled these out; same conclusion applies.
- **Lock-free operators.** Tally's ring buffer operators are already lock-free per entity (the lock is around `EntityState`, not per-operator). No gain here.
- **SIMD for window scans.** 60-bucket scan is ~200ns; SIMD might cut to 50ns, saving ~0.15us per event. Nice-to-have, not load-bearing.
- **Custom allocator (mimalloc / jemalloc).** ~5-15% throughput gain. Drops into Cargo.toml in 1 hour. Can ship as a freebie in any phase.
- **Storage optimizations (LSM, custom store).** No — state is in-memory. No change needed.

---

## 10. Bottom-line answer to "what's the gap?"

**Today:** 17.5k eps single Python client, round-trip, medium pipeline.
**FINDINGS' marketing number:** 8-15M eps per node.

**The gap is 500-1000x. It breaks down into four pieces:**

1. **~7x** — Python SDK round-trip overhead (fixable with batching OR Rust SDK)
2. **~2x** — JSON serialization in the wire (fixable with binary protocol)
3. **~2-3x** — response write path (fixable with fire-and-forget)
4. **~8-16x** — single-threaded runtime (fixable with multi-thread + DashMap)

Multiplied: 7 × 2 × 2.5 × 12 = **~420x**, which matches FINDINGS' 100-500x projection.

**Each optimization compounds. None of them alone gets you past ~100k. All four together gets you into the 1-10M range.**

**If you want 100k fast: ship batching (Stage 1). 1 week of work.**
**If you want 1M realistic: ship all of Stage 1-3. 6-8 weeks of work. Requires v2.0 break.**
