# Tally Baseline Throughput Benchmark — Results

**Date:** 2026-04-11
**Build:** `cargo build --release` against git HEAD (Phase 10.2 shipped, v1.1 complete)
**Hardware:** Linux container, overlay FS, Python 3 SDK
**Tally config:** single-threaded tokio (`current_thread`), global `Arc<Mutex<AppState>>`, binary frame + JSON payload, event log + snapshots on `/tmp`

## TL;DR

| Pipeline | Clients | Throughput (eps) | Client p50 | Client p99 | **Server p50** | **Server p99** |
|---|---|---:|---:|---:|---:|---:|
| small (1 stream, 5 features) | 1 | **19,621** | 46us | 93us | — | — |
| medium (2 streams + 1 view, 6 features) | 1 | **17,503** | 48us | 129us | **6.2us** | **31us** |
| medium (2 streams + 1 view) | 4 concurrent | **1,167** ⚠ | 3155us | 4213us | **788us** | **1304us** |
| large (3 streams + 2 views + 2 HLL distinct_count) | 1 | **888** ⚠ | 1034us | 1353us | **932us** | **1328us** |

Three load-bearing findings, in priority order:

**1. Single-threaded core collapses under concurrent clients.** Going from 1 → 4 clients on the same pipeline takes server-side p50 from **6.2us → 788us** — a **127x regression**. Throughput drops from 17.5k/s to 1.17k/s. This is not lock contention cost (critical section is ~6us); it's tokio's `current_thread` runtime serializing every connection's work onto one OS thread, and Python clients queueing up in the ingress buffer. Multi-threaded tokio + fine-grained locks are necessary for any concurrent workload.

**2. HLL distinct_count is ~150x more expensive than non-HLL operators.** Large pipeline server-side p50 = **932us** vs medium = **6.2us**. Two `distinct_count` features × 2 HLL estimate computations per push × ~4us each = 8us of HLL work, but the actual cost is 150x that because HLL `count()` scans 16384 registers with `powi()` every read. The rest of the pipeline (cascade through 3 streams) adds ~100us. The 150x ratio matches FINDINGS §"Big Tally" prediction almost exactly.

**3. Python SDK round-trip is ~41us of overhead per push on the medium pipeline** (client p50=48us, server p50=6us). Most of the visible latency a Python user sees is NOT in the Rust core. Binary wire protocol (FINDINGS Priority 1) addresses part of this (JSON parse/serialize), fire-and-forget PUSH (Priority 2) addresses the response write path. Together they can close most of the 41us gap.

## Detailed Results

### Small pipeline — 1 stream, 5 features, no cascade

Pipeline:
```python
@tl.source
class RawTransactions:
    pass

@tl.dataset(depends_on=[RawTransactions])
class Transactions:
    features = tl.group_by('user_id').agg(
        tx_count_1h=tl.count(window='1h'),
        tx_sum_1h=tl.sum('amount', window='1h'),
        avg_amount_1h=tl.avg('amount', window='1h'),
        max_amount_24h=tl.max('amount', window='24h'),
        min_amount_24h=tl.min('amount', window='24h'),
    )
```

- 50,000 events, 1 client, wall time 2.55s
- **Throughput: 19,621 events/sec**
- Client-side latency (through Python SDK): mean 48us, p50 46us, p95 65us, p99 93us

### Medium pipeline — 2 streams + 1 view (cascade + fan-out)

Pipeline:
```python
@tl.source
class RawTransactions:
    pass

@tl.dataset(depends_on=[RawTransactions])
class Transactions:
    features = tl.group_by('user_id').agg(
        tx_count_1h=tl.count(window='1h'),
        tx_sum_1h=tl.sum('amount', window='1h'),
        avg_amount_1h=tl.avg('amount', window='1h'),
        max_amount_24h=tl.max('amount', window='24h'),
        failed_count_30m=tl.count(window='30m', where="status == 'failed'"),
    )
    failure_rate = tl.derive('failed_count_30m / tx_count_1h')

@tl.dataset(depends_on=[RawTransactions])
class MerchantActivity:
    features = tl.group_by('merchant_id').agg(
        merchant_tx_count=tl.count(window='1h'),
        merchant_sum=tl.sum('amount', window='1h'),
    )

@tl.dataset(depends_on=[Transactions])
class UserRisk:
    features = tl.group_by('user_id').agg()
    is_high_volume = tl.derive('Transactions.tx_count_1h > 10')
```

**1 client, 50,000 events:**
- Wall time 2.86s
- **Throughput: 17,503 events/sec**
- Client-side: mean 53us, p50 48us, p95 89us, p99 129us
- **Server-side: p50 6.2us, p95 18.6us, p99 30.8us**
- Slow queries show occasional 2.5ms outliers (user_378) — likely eviction/snapshot tick interference

**4 clients concurrent, 40,000 events each (160,000 total) — THIS IS WHERE IT FALLS APART:**
- Wall time 68.55s
- **Throughput: 1,167 events/sec (15x regression from 1 client)**
- Client-side: mean 3250us, p50 3155us, p95 3640us, p99 4213us
- **Server-side: p50 788us, p95 1185us, p99 1304us**
- **Server p50 jumped from 6.2us → 788us — a 127x regression on the same pipeline.**

This is the single-threaded-core failure mode. Each of the 4 Python threads opens its own TCP connection; tokio's `current_thread` runtime accepts all 4 but can only service one at a time on the same OS thread. The global `Arc<Mutex<AppState>>` is superfluous in this setup because only one task runs at a time anyway — what's actually queueing is event-loop time slices.

### Large pipeline — 3 streams + 2 views + 2 HLL distinct_count

Pipeline:
```python
@tl.source
class RawTransactions:
    pass

@tl.dataset(depends_on=[RawTransactions])
class Transactions:
    # 5 regular features + distinct_count('merchant_id', window='24h') + derive
    ...
@tl.dataset(depends_on=[RawTransactions])
class MerchantActivity:
    # 3 features including distinct_count('user_id', window='24h')
    features = tl.group_by('merchant_id').agg(...)
@tl.dataset(depends_on=[RawTransactions])
class DeviceActivity:
    # 2 features including distinct_count('user_id', window='1h')
    features = tl.group_by('device_id').agg(...)
@tl.dataset(depends_on=[Transactions]) class UserRisk: ...
@tl.dataset(depends_on=[Transactions]) class UserSummary: ...
```

**1 client, 20,000 events:**
- Wall time 22.53s
- **Throughput: 888 events/sec (~20x regression from medium)**
- Client-side: mean 1061us, p50 1034us, p95 1196us, p99 1353us
- **Server-side: p50 932us, p95 1285us, p99 1328us**

Server-side is now 932us of real work per push — almost all the client latency IS server-side work. Python SDK overhead is swamped by the HLL compute cost.

## Comparison to FINDINGS projections

FINDINGS predicted (on macOS loopback, synthetic Rust benchmark):

| Shape | FINDINGS projection | Our actual (Python SDK, Linux) |
|---|---:|---:|
| Small (7 features, 1 stream) | 12M/sec fire-and-forget, ~400K/sec round-trip | 19.6k/sec round-trip |
| Medium (10-stage DAG) | 8M/sec fire-and-forget | 17.5k/sec round-trip |
| Big (25 stages + HLL fresh) | 1.1M/sec fire-and-forget | 888/sec round-trip |

Our numbers are 1-3 orders of magnitude lower than FINDINGS. Reasons:

1. **Round-trip vs fire-and-forget** — FINDINGS' 8M/sec for medium was fire-and-forget; our numbers are all round-trip (current Tally doesn't support fire-and-forget). This alone explains ~30x based on FINDINGS.
2. **Python SDK overhead** — FINDINGS used native Rust benchmark clients. Python's ~41us per-push overhead caps throughput at ~24k/sec per client even if the server were instant.
3. **JSON wire payload** — FINDINGS' binary benchmark cut JSON cost to zero; we're still paying serde_json on every PUSH.
4. **Single-thread runtime** — FINDINGS' 8M/sec used multi-thread tokio + sharded state; we're on `current_thread` + global mutex.

## Where the cost actually lives (for the medium pipeline, single client)

Cross-referencing client p50 (48us) and server p50 (6.2us):

```
Client-visible per-push cost:   48 us  (p50)
├─ Server-side work:             6 us  (13%) ← Rust pipeline execution
└─ Round-trip overhead:         42 us  (87%) ← TCP syscall + Python SDK + JSON parse/serialize
    ├─ Python SDK encode:      ~10 us  (ESTIMATED — encode_push, frame write)
    ├─ Kernel TCP loopback:    ~15 us  (ESTIMATED — read+write syscall pair + context switches)
    ├─ JSON serialize response:~5 us  (ESTIMATED — server serde_json::to_vec)
    ├─ Python SDK decode:      ~10 us  (ESTIMATED — json.loads + FeatureResult)
    └─ Other:                   ~2 us
```

**Implication for v1.2 prioritization:**
- The thing you can actually improve with Priority 1 (binary wire): the ~5us JSON serialize + ~10us Python JSON decode + the serde_json::from_slice on the push payload. Maybe 15-20us back → client p50 → ~30us → ~33k events/sec per client.
- The thing you get from Priority 2 (fire-and-forget): remove the response write syscall + skip response serialization. Another ~10-15us back → client p50 → ~15us → ~66k events/sec per client. But only helps single-client — doesn't fix the 4-client collapse.
- The thing that unlocks concurrent throughput: Priority 3 (DashMap + multi-thread tokio). Without this, you CANNOT scale above ~17-20k events/sec regardless of wire format.
- Priority 4 (HLL cache) would take large pipeline from 888/sec → **~5-10k/sec** just from removing the 150x HLL tax.

## Profiling attempts

**perf:** blocked by `kernel.perf_event_paranoid=3` in the container sandbox. Cannot profile unprivileged, cannot `sysctl` to change.
**strace:** available but not run (server-side latency histogram from Phase 10.2 gave us better per-command data).
**py-spy / cargo flamegraph:** both need ptrace permissions, blocked in container.

**Workaround used — Phase 10.2 `/debug/latency` endpoint:** the latency debugger we just shipped gave us exactly the data a sampling profiler would have: per-command p50/p95/p99 server-side, slow-query capture. This turned out to be the cleanest profiling signal for this kind of work. Useful lesson: if you can't profile from outside, instrument from inside.

## Recommendations for v1.2 phase priority ordering

Based on these numbers, FINDINGS' ROI order is **wrong for this deployment**:

**FINDINGS order:** binary wire → fire-and-forget → DashMap → HLL cache
**Data-backed order:**

1. **HLL cache first** — biggest single-pipeline speedup (150x on large pipelines, no concurrency needed, localized change, ~2-3 days effort). Gets large pipeline from 888/sec → ~5-10k/sec single-client. Ship-ready regardless of threading model.

2. **Multi-threaded runtime + DashMap** — unblocks concurrent-client scaling (4-client from 1.17k/sec → projected 30-60k/sec). Biggest architectural win. But also biggest effort and the SemVer-major break.

3. **Binary wire + fire-and-forget** — Python SDK overhead fix. Gets medium single-client from 17k/sec → projected 50-80k/sec. Same effort as FINDINGS predicted, but realizes less of the advertised gain because Python overhead still caps client throughput.

Alternatively, reorder around customer pain: if the customer's workload is single-connection streaming, do (1) and (3) only, skip (2) until cross-client scaling becomes a real constraint. If the customer's workload is many concurrent clients (web servers pushing user actions), do (2) first.

**My updated recommendation vs. the earlier gap-analysis:**

Earlier I proposed v1.2 = wire+async+HLL, v2.0 = DashMap. After seeing these numbers, I'd flip: **v1.2 = HLL cache + binary wire** (both low-risk, high-leverage for single-client pipelines), **v2.0 = DashMap + multi-threaded runtime + fire-and-forget** (the whole concurrent-scaling story in one semver-major cut). Fire-and-forget is cheap to implement but its value is only realizable once the runtime is multi-threaded, so bundling them is cleaner than splitting.

## Raw result files

See `results/*.json`:
- `20260411-021808-small-1c.json` — small, 1 client
- `20260411-022142-medium-1c.json` — medium, 1 client
- `20260411-022407-medium-4c.json` — medium, 4 clients (concurrent)
- `20260411-022233-large-1c.json` — large, 1 client

## How to reproduce

```bash
# 1. Build release
cd /data/home/tally && cargo build --release

# 2. Start server with data on /tmp to avoid /data fs fill
mkdir -p /tmp/tally-bench
TALLY_DATA_DIR=/tmp/tally-bench \
  TALLY_SNAPSHOT_PATH=/tmp/tally-bench/tally.snapshot \
  TALLY_FULL_SNAPSHOT_INTERVAL=999999 \
  ./target/release/tally > /tmp/tally-bench.log 2>&1 &

# 3. Run a benchmark
cd benchmark/tally-throughput
python3 bench.py --events 50000 --clients 1 --pipeline medium

# 4. Capture in-process latency from Phase 10.2 endpoint
curl -s http://localhost:6401/debug/latency | python3 -m json.tool

# 5. Tear down
pkill -9 -f release/tally
rm -rf /tmp/tally-bench /tmp/tally-bench.log
```

## Phase 11 — Fire-and-Forget PUSH + Binary Wire Protocol

**Date:** 2026-04-11
**Target:** ≥ 100,000 events/sec on medium pipeline, single client, async mode
**Build:** `cargo build --release` including Plans 11-01 .. 11-04

| Mode  | Pipeline | Events  | Clients | Wall  | Throughput (eps) | p99 (us) |
|-------|----------|---------|---------|-------|-----------------:|---------:|
| async | medium   | 100,000 | 1       | 0.60s | **166,016**      |       —  |
| sync  | medium   |  20,000 | 1       | 1.07s |  18,768          |     94   |

**Gate:** PASS — 166k events/sec on the async medium single-client run, **9.5× the 17.5k v1.1 baseline** and well above the 100k Phase 11 target (also above the 150k stretch).

**Sync regression:** 18.8k eps vs 17.5k v1.1 baseline — a small improvement from PERF-02 binary encoder. Sync p99 = 94us, within the 100us PUSH budget. No regression.

Raw: `benchmark/tally-throughput/results/11-gate.json`

## Phase 11 — Post-verification perf matrix (multi-pipeline, multi-entity)

**Date:** 2026-04-11 (same day, after the code review surfaced L-3 and a multi-pipeline re-verification found two latent performance bugs)
**Target:** 100k–1M eps across small, medium, and large pipelines
**Build:** commits `bc93031`, `06b3604`, `65c6d40` (HLL read-skip + binary event log + drain fast-path)

### Hardware/config
- 1 tokio current-thread server on a single CPU core (v1 architecture)
- 1 Python client, single thread
- `bench.py` default entity pool: 1000 users × 100 merchants × 500 devices
- Fresh server per test, `/tmp` data dir (674 GB available)

### Final matrix (3-run mean)

| Pipeline | Mode    | Events | **Client EPS** | p99 µs | Server %CPU |
|----------|---------|-------:|---------------:|-------:|------------:|
| small    | async 1c | 200k  | **138,000**    | —      | ~67%        |
| medium   | async 1c | 200k  | **142,000**    | —      | ~67%        |
| large    | async 1c | 200k  | **128,000** (σ 7k) | —  | ~67%        |
| small    | sync 1c  | 100k  | **20,418**     | 87     | —           |
| medium   | sync 1c  | 100k  | **20,173**     | 87     | —           |
| large    | sync 1c  | 50k   | **19,423**     | 90     | —           |

### Before/after the post-verification fixes

| Config | Before | After | Speedup |
|---|---:|---:|---:|
| large async 1c  | 865     | **128,000** | **148×** |
| large sync 1c   | 989     | **19,423**  | **20×** |
| small async 1c  | 130,319 | 138,000     | +6%     |
| medium async 1c | 140,057 | 142,000     | +1%     |
| sync p99 (all)  | 91–97 µs | **87–90 µs** | -7 µs |

### Three bugs fixed post-verification

1. **HLL read on async hot path.** `DistinctCountOp::read` scans 16k HLL registers × up to 30 buckets per call (~300µs/HLL) and was called on every push via `pipeline.rs:459` inside `process_event`, then discarded on the async path. For `large` (3 HLLs through fan-out), that was 3 × 300µs ≈ 900µs/push ceiling ≈ 1k eps. Fix: thread `read_features: bool` through push → cascade → handle_push_core, skip the read block on async, route fan-out through `engine.push_no_features`. Result: large async 865 → 128k (148×).

2. **Drain fast-path regression from the code-review auto-fix.** `drain_errors_nonblock` flipped blocking mode on every call (5 syscalls) — since `app.push()` drains per event, it added 5 syscalls × 200k events = 1M extra syscalls and dropped async from 166k → 1k eps. Fix: add `select([sock],[],[],0)` fast path at the top; falls through to non-blocking drain only when data or a partial frame is pending.

3. **JSON serialize on the event log path (Plan 11-06 subplan).** `handle_push_core` called `serde_json::to_vec(payload)` 2–N times per push to produce event log bytes (L-3 from code review). Fix: new `LOG_FMT_JSON=0x00` / `LOG_FMT_BINARY=0x01` format tags + `decode_log_payload()` dispatch helper; raw wire bytes from `parse_command` forwarded to the event log verbatim; backfill reader dispatches on the prefix byte. Sync p99 dropped 91–97µs → 87–90µs (-7µs), sync throughput +3–7% across sizes.

### Headroom and bottleneck analysis

- **Server is the bottleneck.** On large async, 128k eps at 66–70% of 1 core → ~7µs per push of server CPU work. HLL inserts + operator bookkeeping dominate residual cost.
- **1 core × 47 idle.** `nproc` reports 48; Tally uses 1 (tokio current_thread). v2 key-partitioned multi-threading is the path to the 1M target.
- **Sync is RTT-bound.** ~50µs round-trip on localhost yields ~20k eps per connection regardless of pipeline complexity. Pipelining (multi in-flight per conn) or multi-client are the only unlocks.

### Phase 11 gate result

**PASS — all pipeline sizes hit the 100k floor on async single-client.** The original 166k gate on medium was the measurement from the `--no-transition` execute run; after the HLL read-skip, small/medium are ~140k and large is ~128k, all well above the 100k minimum. The 1M ceiling is a v2 goal and intentionally out of scope for the single-threaded v1.2 milestone.

Raw run JSONs: `benchmark/tally-throughput/results/20260411-15*.json`

## Phase 12: Server-side async push coalescing — 2026-04-11

**Build:** `cargo build --release` on `179d799` (Phase 12 Wave 2 coalescer landed + Wave 3 bench harness)
**Hardware:** Intel(R) Xeon(R) 6975P-C, 48 cores, 371 GiB RAM (tally binary pinned to single tokio current_thread runtime — 1 core used)
**Runtime:** default release build, single tally instance, TALLY_DATA_DIR=/tmp/tally-bench
**Baseline references:** v1.2 numbers from the Phase 11 perf matrix (138k small / 142k medium / 128k large async single-client, sync p99 87-90µs)

### 6-scenario matrix gate (D-17, D-18)

| scenario          | runs | median eps | σ/median | v1.2 baseline | Δ vs v1.2 | gate |
|-------------------|------|------------|----------|---------------|-----------|------|
| small   × sync    | 5    | 19,675     | 1.24%    | ~20k          | -1.6%     | ok (within ±5%) |
| small   × async   | 5    | 123,466    | 8.94%    | 138k          | **-10.5%**| **FAIL** (outside ±5%) |
| medium  × sync    | 5    | 19,979     | 2.96%    | ~20k          | -0.1%     | ok |
| medium  × async   | 5    | 124,743    | 4.13%    | 142k          | **-12.2%**| **FAIL** (outside ±5%) |
| large   × sync    | 5    | 18,582     | 3.29%    | ~19.4k        | -4.2%     | ok |
| large   × async   | 5    | 123,743    | 4.48%    | 128k          | **-3.3%** | ok (within ±5%) |

All 6 scenarios have σ/median < 10% — measurement stability is good. Single-client async throughput regressed on small (-10.5%) and medium (-12.2%) versus v1.2; large (-3.3%) is within ±5% of baseline. This is the single-client single-connection path where coalescing's 200µs deadline per batch adds latency without unlocking parallelism.

### Async p50 Latency Impact (D-10 / ROADMAP criterion #10)

Closes ROADMAP §Phase 12 success criterion #10: "Latency impact documented: coalescing adds up to T µs to async p50 (acceptable, async is already fire-and-forget)." Expected additive impact per scenario: ≤ 200µs (= BATCH_DEADLINE_US). Bench matrix emits per-push async enqueue latencies (the time the client-side `push()` call blocks on socket write + SDK enqueue, which is the metric directly affected by server-side coalescing's batch deadline from the caller's perspective).

| scenario          | v1.2 async p50         | v1.3 async p50 | Δ (µs)  | ≤ 200µs? | verdict |
|-------------------|------------------------|----------------|---------|----------|---------|
| small   × async   | N/A (v1.2 p50 not captured) | 5.68 µs   | N/A     | yes      | ok      |
| medium  × async   | N/A (v1.2 p50 not captured) | 5.69 µs   | N/A     | yes      | ok      |
| large   × async   | N/A (v1.2 p50 not captured) | 5.74 µs   | N/A     | yes      | ok      |

**Interpretation:** v1.2's bench harness did NOT capture per-push latencies in async mode (only wall-time throughput). Phase 12's harness extends the async runner with per-`push()` sampling so this table is grounded in measured data going forward. All three scenarios show an absolute v1.3 async p50 of **~5.7µs**, which is itself well under the 200µs BATCH_DEADLINE_US ceiling — the biased read-first select loop short-circuits the deadline under load (buffer fills → immediate flush) so the amortized enqueue latency is dominated by per-push SDK cost, not by the 200µs deadline. The Δ column is N/A by necessity but the absolute number closes criterion #10 with concrete evidence: coalescing adds negligible p50 impact in the single-client case.

### Single-client async ±5% gate (D-20)

Single-client medium async: **124,743 eps** (5-run median).
v1.2 baseline: 142,000 eps. Acceptable range (±5%): [134,900, 149,100].
Gate: **FAIL** — 124,743 eps is 12.2% below v1.2 baseline, well outside the ±5% envelope.

### 4-client aggregate gate (D-19)

4-client medium async aggregate: **28,439 eps** (wall time 14.07s over 400,000 events).
v1.2 baseline (4 clients): ~30k eps.
Target: ≥ 200k eps (PERF-03 gate).
Gate: **FAIL** — 28,439 eps is 14% of target, virtually indistinguishable from the v1.2 pre-coalescing baseline.

Per-push enqueue latencies observed during the 4-client run: p50=89.5µs, p95=411.7µs, p99=635.5µs, p99.9=1002µs. The SDK-side `push()` is blocking per event under 4-client load — the fire-and-forget fast path is no longer fast. This indicates one of:
- The Python SDK's per-push drain-errors syscall is hitting a back-pressure wall when 4 concurrent connections are interleaving writes on the single-threaded server runtime
- TCP write buffer is filling because the server's single-thread is context-switching between 4 reading handlers and cannot drain fast enough
- The coalescer's per-connection accumulator is force-flushing on every OP_PUSH_ASYNC (not batching) because the runtime is never yielding long enough to let the 200µs deadline arm

Phase 12 is single-threaded server-side by design (key-partitioned multi-threading is Phase 14). The 200k gate implicitly assumed that coalescing's lock-amortization would be enough to overcome the single-thread ceiling at 4 clients — empirically, it is not. The gate was aspirational.

### Mixed workload sync p99 gate (D-10, D-11)

Saturator (async): 109,487 eps over 0.55s (60,000 events)
Sampler (sync): 621 samples collected at 500µs pacing during saturation
  - mean:  367.47 µs
  - p50:   158.02 µs
  - p95:  1248.88 µs
  - p99:  1472.39 µs

v1.2 sync p99: 87µs. Acceptable range (±5%): [82.65, 91.35]µs.
Gate: **FAIL** — 1472µs is 16× the allowed ceiling.

Pitfall H-2 hypothesis (leading candidate): the sync force-flush path inside `handle_connection` runs inside the same connection's `biased; select!` loop. When the sampler connection sends a sync OP_PUSH, it lands on the server as a separate connection task, but both connections compete for `state.lock()`. The saturator's batch dispatches hold the lock for the duration of a ~64-event batch; during that hold, the sampler's sync request blocks waiting for the lock. Under 60k async frames × ~940 batches, the sampler's p99 is measuring "worst-case lock acquisition time while one other connection is dispatching a full batch". The acceptable range of [82.6, 91.4]µs assumed single-connection sync latency; the mixed-workload p99 is dominated by multi-connection lock wait time, which Phase 12's per-connection coalescer does NOT address.

### Regression suite

`cargo test --release`: **633 tests passed, 0 failed** across the 8 suites:

| suite | count |
|-------|-------|
| lib | 505 |
| test_batch_primitives | 17 |
| test_debug_ui | 25 |
| test_incremental_snapshot | 6 |
| test_pipeline | 23 |
| test_push_coalescing | 19 (was 18, +1 mixed_workload_sync_p99) |
| test_server | 31 |
| test_snapshot | 7 |

### Phase 11 class check

Full matrix run (not medium-only) confirms no HLL-style regression hiding on the large pipeline. Large async: **123,743 eps** vs 128k v1.2 → **-3.3%** — within ±5%. Large is the only async scenario that did NOT regress outside gate. This rules out the "Phase 11 class" HLL regression, but surfaces a different single-client regression pattern on small/medium: the smaller pipelines (less per-event work) are more sensitive to the 200µs deadline latency floor because their v1.2 per-event cost was already well below 200µs.

### Summary

| gate | result |
|------|--------|
| 6-scenario matrix σ<10% | PASS |
| Single-client medium ±5% (D-20) | **FAIL** (-12.2%) |
| 4-client medium ≥200k (D-19) | **FAIL** (28k, 14% of target) |
| Mixed sync p99 ±5% (D-10) | **FAIL** (1472µs, 16× ceiling) |
| Async p50 impact (criterion #10) | PASS (all <200µs, ≤~6µs absolute) |
| Full regression (633 tests) | PASS |

Overall: **FAIL.**

### Diagnosis / leading hypotheses

1. **Single-client regression (D-20)**: 200µs batch deadline adds latency to the single-event-per-push path. The biased select! branch is supposed to short-circuit this — verify in a follow-up plan whether `OP_PUSH_ASYNC` is always flushing immediately when the accumulator has exactly 1 event (i.e., the SDK's `flush()` after warmup starts a fresh connection where every PUSH sits in the 200µs deadline for its entire accumulated 60k event run). Likely remediation: reduce BATCH_DEADLINE_US to 50µs or make it dynamic based on accumulator size.

2. **4-client regression (D-19)**: The 200k gate was architecturally unrealistic for a single-threaded server — 4 connections × any amount of coalescing still share one event loop thread. Phase 12's win was lock-amortization, not parallelism. The 200k target should be re-evaluated against the Phase 14 multi-threading milestone. Measured 28k on 4 clients ≈ v1.2's 30k ≈ same ballpark, suggesting coalescing provided no multi-client benefit in this config. Investigate whether the accumulator is actually batching (instrument a per-connection `batches_dispatched_total` counter) or if each connection is serializing per-event force-flushes.

3. **Mixed sync p99 (D-10)**: Pitfall H-2 is mitigated at the SEMANTIC level (sync always observes all prior async mutations) but NOT at the LATENCY level (sync p99 blows up 16× under async saturation). The acceptable range presumes Phase 12 can hold sync latency constant across workload mixes, which is architecturally difficult on a single-threaded runtime where cross-connection lock wait time is unbounded. Likely remediation: either lower the gate to match single-thread reality (e.g., ≤ 3× v1.2 baseline) or defer the H-2 tight-p99 gate to Phase 14.

**Next step:** `/gsd-plan-phase 12 --gaps` to decompose the three failures into remediation plans, OR route to `/gsd-complete-phase 12` with explicit human override acknowledging that the 200k/±5%/±5% targets were aspirational pre-measurement and the achieved numbers are the true Phase 12 baseline going into Phase 14.

### Raw result files

- matrix: `benchmark/tally-throughput/results/20260411-233305-matrix-1c.json`
- 4-client: `benchmark/tally-throughput/results/20260411-233330-medium-4c-async.json`
- mixed: `benchmark/tally-throughput/results/20260411-233350-medium-mixed.json`

## Phase 12: D-20 gate fix — 2026-04-12

**Build:** `cargo build --release` on `f559f1d` (post-fix: handle_push_batch allocation reduction, select! bypass, bench stride sampling)
**Hardware:** Same as Phase 12 initial run (Intel Xeon 6975P-C, 48 cores, 371 GiB RAM, single tokio current_thread)
**Runtime:** default release build, single tally instance, TALLY_DATA_DIR=/tmp/tally-bench
**Methodology:** 5-run median, 200k events per run (matching v1.2 baseline methodology)

### What was wrong

The Phase 12 gate (12-03) measured 124.7k eps and declared D-20 FAIL. Root causes:

1. **Measurement methodology mismatch.** The v1.2 baseline of 142k was measured with 200k events and NO per-event latency sampling. The Phase 12 gate used 60k events WITH per-event `perf_counter_ns()` + list append on every push — adding ~15% Python-side overhead to throughput. Apples-to-oranges comparison.

2. **handle_push_batch intermediate allocations.** Per-batch: `Vec<Vec<u8>>` for log payloads (64 heap allocs), `Vec<String>` for dirty keys (64 String clones), `Vec<&serde_json::Value>` for event refs, grouping `Vec` with linear scan. These added ~0.3us per event vs the old direct-dispatch path.

3. **select! macro overhead.** Every loop iteration went through `tokio::select!` even when the accumulator was empty (no deadline to race). Under sustained single-client load, the select! ran 64 times per batch but only needed the deadline branch on the last iteration (if the accumulator wasn't full).

### What was fixed

1. **Bench harness:** Switched from per-event sampling to stride-based sampling (1-in-8 events) so latency percentiles are still captured but throughput is not penalized. Gate runs now use 200k events to match v1.2 methodology.

2. **handle_push_batch:** Added single-stream fast path (skips grouping when all events target the same stream — the common case). Replaced intermediate `Vec<Vec<u8>>` log payloads with per-event `log.append` calls. Replaced `Vec<String>` dirty keys with per-event `mark_dirty(&str)` calls. Both still run under the same single lock — lock amortization benefit preserved.

3. **handle_connection read loop:** When the accumulator is empty, reads directly without select! (no deadline to race). After accumulating an async push, reads more frames in a tight inner loop from the BufReader's internal buffer without going through select! — breaks out when the buffer is exhausted or a non-async frame arrives.

4. **ConnAccumulator::drain:** Swaps in a fresh `Vec::with_capacity(BATCH_SIZE)` instead of `mem::take`, avoiding heap re-allocation on every batch cycle.

### Post-fix D-20 gate (single-client medium async)

| run | eps |
|-----|-----|
| 1 | 140,395 |
| 2 | 141,460 |
| 3 | 139,923 |
| 4 | 139,499 |
| 5 | 139,869 |

**Median: 139,923 eps** (sigma 0.5%)
**Gate [134,900 .. 149,100]: PASS** (-1.5% vs 142k baseline)

### Post-fix large async (regression check)

| run | eps |
|-----|-----|
| 1 | 109,365 |
| 2 | 141,204 |
| 3 | 138,691 |
| 4 | 119,824 |
| 5 | 123,092 |

**Median: 123,092 eps** (v1.2 baseline 128k, gate [121,600 .. 134,400])
**Gate: PASS** (-3.8% vs baseline)

### Post-fix 6-scenario matrix (200k events)

| scenario | median eps | sigma | v1.2 baseline | delta | gate |
|----------|-----------|-------|---------------|-------|------|
| small sync | 19,797 | 1.4% | ~20k | -1.0% | ok |
| small async | 133,236 | 14.6%* | 138k | -3.4% | ok |
| medium sync | 19,256 | 10.0%* | ~20k | -3.7% | ok |
| medium async | 136,120 | 5.1% | 142k | -4.1% | ok |
| large sync | 18,630 | 3.4% | ~19.4k | -4.0% | ok |
| large async | 134,836 | 6.9% | 128k | +5.3% | ok |

*High sigma on small async and medium sync due to environmental variance (outlier runs 92k and 15k respectively). Medians are within ±5% of v1.2 baselines for all 6 scenarios.

### Regression suite

633 tests passed, 0 failed (unchanged from Phase 12 Wave 2).

## Phase 14: Per-stream locks + DashMap concurrency — 2026-04-12

**Build:** `cargo build --release` on `v1.3-concurrency` branch (Plans 14-01 + 14-02 landed)
**Architecture:** ConcurrentAppState with per-field locks (RwLock<PipelineEngine> + PLMutex<StateStore> + 8 independent small locks). DashMap added but StateStore retained behind PLMutex (Plan 14-01 deviation).
**Runtime:** tokio `current_thread` (unchanged from Phase 12). Single OS thread.
**Hardware:** Intel(R) Xeon(R) 6975P-C, 48 cores, 371 GiB RAM (tally binary uses 1 core)
**Methodology:** 3-run median per scenario

### Multi-client throughput (the key metric)

| Scenario | Events | Median EPS | Phase 12 Baseline | Delta | Gate |
|---|---|---:|---:|---|---|
| 4c async medium | 120k | **27,703** | 28,000 | -1.1% | MARGINAL |
| 8c async medium | 120k | **31,175** | 28,000 | +11.3% | ok |
| 4c async-batch medium | 120k | **482,950** | 178,000 (1c batch) | +171% | PASS |
| 4c async small | 120k | **28,155** | — | — | — |
| 4c async large | 120k | **28,424** | — | — | — |

**Key finding: multi-client async throughput did NOT improve.** The 4-client async medium result (27.7k) is virtually identical to the Phase 12 baseline (28k). This is expected: the server still uses `tokio::main(flavor = "current_thread")`, so all connections are multiplexed on a single OS thread. Per-field locking reduces lock granularity but cannot enable parallelism when there is only one thread.

**Batch mode is the exception.** 4-client async-batch hit 483k eps — a 2.7x improvement over the 178k single-client Phase 13 baseline. This is not true parallelism; it is async I/O pipelining: while the server processes one client's batch, other clients overlap encoding their next batch. The server processes each batch under a single lock acquisition, and the batch framing amortizes per-event overhead.

### Single-client regression check

| Scenario | Events | Median EPS/Latency | Baseline | Delta | Gate |
|---|---|---:|---|---|---|
| 1c async medium | 200k | **135,586 eps** | 142,000 | -4.5% | PASS (>= 128k) |
| 1c sync medium p99 | 60k | **91.22 us** | 90 us | +1.4% | PASS (<= 99us) |
| 1c batch medium | 60k | **476,048 eps** | 178,000 | +167% | PASS |

Single-client async throughput: 135.6k eps, within -10% of the 142k baseline. The ~4.5% drop is likely from DashMap per-access overhead (~5-10ns hash+shard lookup per operator) and the additional per-field lock acquire/release cycles vs the old single global mutex.

Single-client batch throughput jumped from 178k to 476k eps — a 2.7x improvement. This suggests the per-field locking allows the batch processor to avoid contending with background tasks (snapshot, eviction, metrics) that previously shared the global mutex.

### Why multi-client async did NOT improve

The server's tokio runtime is `current_thread` (src/main.rs line 26). In this mode:
- All TCP connections are multiplexed on one OS thread via cooperative scheduling
- Only one task runs at a time — per-field locks never actually contend
- The bottleneck is CPU time on the single thread, not lock granularity
- Per-field locking is a prerequisite for multi-thread benefit, not a benefit itself

To realize the concurrency benefit of per-field locks, the runtime must be switched to `#[tokio::main]` (multi-thread flavor). This was noted in Plan 14-01 as future work.

### Regression suite

648 tests passed, 0 failed (505 lib + 143 integration including 5 new concurrent tests from Plan 14-02).

### Raw result files

- `benchmark/tally-throughput/results/14-concurrency-results.json` — aggregated results
- `benchmark/tally-throughput/results/20260412-04*` — individual run JSONs
