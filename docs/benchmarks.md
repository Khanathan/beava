# Tally Benchmarks

Measured on a host with 48 CPUs / 371 GB RAM, with the server pinned to 8 CPUs (`taskset -pc 0-7`) to simulate an 8-core production box. `TALLY_WORKER_THREADS=8`. Release build.

## Pipeline shapes

The matrix benchmark runs three progressively heavier aggregation shapes over a single stream of events keyed by `user_id` (1,000 distinct keys):

| Shape | Features / key | Operators |
|---|---|---|
| `small` | 5 | count, sum, avg, max, min over 1h/24h windows |
| `medium` | 5 | `small` + filtered count (WHERE clause) |
| `large` | 7 | `medium` + filtered count + `count_distinct(merchant_id)` (HLL-backed) |

All three are 1 source → 1 aggregation. No joins or enrichment.

## Single-stream async throughput (single-event PUSH)

`python3 benchmark/tally-throughput/bench_v0.py --matrix --events 30000`

Driver uses `app.push()` (OP_PUSH_ASYNC, fire-and-forget) in a `ThreadPoolExecutor` of N client threads within one Python process. Final `flush()` before measuring wall time.

| Cell (pipeline × clients) | eps |
|---|---|
| small × 1 | ~110,000 |
| small × 4 | ~30,000 |
| small × 8 | ~31,000 |
| medium × 1 | ~110,000 |
| medium × 4 | ~28,000 |
| medium × 8 | ~31,000 |
| large × 1 | ~109,000 |
| large × 4 | ~28,000 |
| large × 8 | ~30,000 |

**What this actually measures:** Python-side GIL throughput. The ThreadPool runs N threads in ONE Python interpreter; they contend on the GIL for event encoding. 1-client is at the GIL ceiling per process (~110k eps). More threads within one process doesn't help — only hurts due to contention.

**Do not cite `--clients 8` numbers as server-side throughput.** They are client-harness-bound, not server-bound.

## Batched single-connection throughput

`push_many(batch_size=1000)` — one wire frame per 1,000 events, ~0.3 µs Python overhead per event.

| Scenario | eps |
|---|---|
| 1 process, 1 connection, batched | ~553,000 |

This is the headline "one client, batched" number. Roughly equivalent to what the launch blog's 483k figure measured on earlier code.

## Multi-process server-side throughput (the real scaling number)

8 separate Python processes (no shared GIL), each holding its own connection, each pushing batched.

| Scenario | eps (aggregate) | per-process |
|---|---|---|
| 8 procs × single-event, 1 shared stream | ~385,000 | ~50,000 |
| 8 procs × single-event, 8 distinct streams | ~540,000 | ~85,000 |
| 8 procs × batched, 1 shared stream | ~790,000 | ~110,000 |
| 8 procs × batched, 8 distinct streams | ~920,000 wall / ~1,060,000 sum | ~130,000 |

### History of this number (single-event, 8 procs, 1 stream)

| Change | Aggregate eps |
|---|---|
| Phase 40 baseline | 368,000 |
| Phase 41 (atomic metrics/throughput/latency) | 385,000 |
| Phase 42 (O_APPEND lock-free log) | 385,000 *(log not the bottleneck)* |
| Phase 43 (lock-free WatermarkTracker / LateDropCounters) | ~790,000 batched (+46%) |
| DashMap shard tune (256 → 16) | not measured post-commit |

Watermark `fetch_max` removing the per-event exclusive lock was the single biggest win.

## Running the bench

```bash
# 1. Build release binary
cargo build --release --bin tally

# 2. Start server pinned to 8 CPUs
rm -rf /tmp/tally-bench-data && mkdir -p /tmp/tally-bench-data
TALLY_DATA_DIR=/tmp/tally-bench-data \
  TALLY_TCP_PORT=6400 TALLY_HTTP_PORT=6401 \
  TALLY_ADMIN_TOKEN=bench \
  TALLY_WORKER_THREADS=8 \
  target/release/tally &
SERVER_PID=$!
taskset -pc 0-7 $SERVER_PID
sleep 2 && curl -s http://127.0.0.1:6401/debug/ready

# 3. Pre-register stream (don't rely on the bench doing it under contention)
python3 - <<'PY'
import sys; sys.path.insert(0, 'python')
import tally as tl
@tl.stream
class RawTxns:
    user_id: str
    amount: float
@tl.table(key="user_id")
def Transactions(raw: RawTxns) -> tl.Table:
    return raw.group_by("user_id").agg(
        c=tl.count(window="1h"), s=tl.sum("amount", window="1h"))
tl.App("127.0.0.1:6400", timeout=30.0).register(RawTxns, Transactions)
PY

# 4. Run the matrix (1 process, threaded — GIL-bound)
python3 benchmark/tally-throughput/bench_v0.py --matrix --events 30000

# 5. Run the real-scaling bench (8 processes)
for i in 0 1 2 3 4 5 6 7; do
  python3 benchmark/tally-throughput/push_batched.py 127.0.0.1:6400 300000 $i 1000 &
done; wait
```

## Interpreting results — pitfalls

- **GIL-bound numbers don't reflect server capacity.** If you run `bench_v0.py --clients 8`, you're measuring Python thread contention, not Tally. Multi-process benches are the only way to expose real server-side scaling.
- **Batched vs single-event matters enormously.** Batched amortizes ~7 µs → ~0.3 µs of Python per event. On the server, a batched OP_PUSH_BATCH does one log append + N operator applies. The ratio of batch-size-to-per-event-work determines whether you're measuring disk/log throughput, operator work, or Python overhead.
- **Shared-stream vs distinct-stream.** Phase 40 introduced per-stream log-file locks. Phase 42 made log append lock-free (O_APPEND). Post-43 both cases scale comparably, but on distinct streams you get file-handle parallelism the kernel can exploit at the inode layer.
- **Watermark + late-drop was the bottleneck people miss.** Even after log and metrics are lock-free, every PUSH updates per-stream watermarks. If this is a `write()` lock, every producer serializes on it. Phase 43 fixed this with atomic `fetch_max` on per-stream `AtomicU64`.
- **DashMap shard count defaults to `num_cpus() * 4` rounded up to power of 2.** On a 48-CPU host that's 256 shards per DashMap regardless of how many workers you pin the server to. The `STATE_SHARD_AMOUNT = 16` constant matches 8-worker deployments better; memory footprint drops without adding contention.
- **Ordering in the log is not deterministic across concurrent producers.** Tally's correctness model is event-time + watermark (Phase 24), so arrival-order in the log doesn't affect operator output. Clients that need strict ordering should stamp `_event_time` explicitly.

## When the benchmark is misleading

- **You run on a laptop without CPU pinning:** tokio runtime and Python driver compete for the same cores, numbers drop by ~30%. Always pin the server.
- **First run of the benchmark after startup:** cold caches, JIT-like warmup on tokio's task scheduler. Discard the first run.
- **Snapshot runs mid-benchmark:** a snapshot takes `engine.read()` for ~300 ms on even a small state. That shows up as a plateau in the middle of the run. Either wait for a snapshot cycle to complete before benching, or disable snapshots via config for pure throughput measurements.
- **Replica mode:** `tally serve --replica-from` rejects local PUSH. Don't bench against a replica.

## What's next for perf

After Phase 43 + shard tuning, the 8-proc 1-stream batched gap from ~790k to a theoretical ~4M (8 × 554k solo) is roughly 5x. The remaining cost is in:
- Per-event operator state mutation (DashMap shard acquires, sketch updates).
- JSON `payload.get(key_field)` per event.
- Postcard decode.
- Tokio task scheduling overhead per connection-level read.

These are all pure-compute overheads that don't block on locks; the path to further scaling is either fewer per-event syscalls (batching at the operator level), SIMD-accelerated JSON parsing, or a binary wire format for common-case events.
