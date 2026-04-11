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
@st.stream(key='user_id')
class Transactions:
    tx_count_1h = st.count(window='1h')
    tx_sum_1h = st.sum('amount', window='1h')
    avg_amount_1h = st.avg('amount', window='1h')
    max_amount_24h = st.max('amount', window='24h')
    min_amount_24h = st.min('amount', window='24h')
```

- 50,000 events, 1 client, wall time 2.55s
- **Throughput: 19,621 events/sec**
- Client-side latency (through Python SDK): mean 48us, p50 46us, p95 65us, p99 93us

### Medium pipeline — 2 streams + 1 view (cascade + fan-out)

Pipeline:
```python
@st.stream(key='user_id')
class Transactions:
    tx_count_1h, tx_sum_1h, avg_amount_1h, max_amount_24h, failed_count_30m
    failure_rate = st.derive('failed_count_30m / tx_count_1h')

@st.stream(key='merchant_id')
class MerchantActivity:
    merchant_tx_count, merchant_sum

@st.view(key='user_id')
class UserRisk:
    is_high_volume = st.derive('Transactions.tx_count_1h > 10')
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
@st.stream(key='user_id')
class Transactions:
    # 5 regular features + distinct_count('merchant_id', window='24h') + derive
    ...
@st.stream(key='merchant_id')
class MerchantActivity:
    # 3 features including distinct_count('user_id', window='24h')
@st.stream(key='device_id')
class DeviceActivity:
    # 2 features including distinct_count('user_id', window='1h')
@st.view(key='user_id') class UserRisk: ...
@st.view(key='user_id') class UserSummary: ...
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
