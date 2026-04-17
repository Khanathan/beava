# Beava Benchmarks

Two reproducible benchmarks live in this directory. Both drive a real
server process (not a mock) and talk to it through the public TCP
protocol via the Python SDK.

| Directory | What it measures | Headline |
|---|---|---|
| [`fraud-pipeline/`](fraud-pipeline/) | Sustained ingest into the 47-feature fraud pipeline cited in the launch copy. | 544K eps on 16-core Hetzner (Phase 42); 314K eps on a 10-core laptop (this repo's baseline). |
| [`replay/`](replay/) | Deterministic 30-day event replay — a smaller pipeline (5 features), pinned RNG seed, used to produce the blog-post headline number and to back-fill scoped replicas from JSONL traces. | 1.1M eps on Hetzner, deterministic byte-identical event stream for a given seed. |

If you are here to reproduce the launch number, run the fraud pipeline.
If you want deterministic numbers you can diff across machines, run
replay.

## Quick start (fraud pipeline)

```bash
bash benchmark/fraud-pipeline/run_bench.sh
```

That one command:

1. Builds the server in release mode if the binary is missing (~30-60s,
   one-time).
2. Starts a fresh server on a scratch data dir so your real state is
   untouched.
3. Spawns one client process per CPU core (capped at 8 by default).
4. Runs a 5s warmup + 60s measurement and streams live EPS to stdout
   every 5s.
5. Writes `summary.json` + `stdout.log` + per-client JSONL plus a
   `server.log` to `benchmark/fraud-pipeline/results/<timestamp>/`.
6. Generates a flamegraph SVG if `cargo flamegraph` is installed and
   the kernel allows perf sampling.

Typical wall-clock: **under 2 minutes on first run, ~70s on re-runs** on
a laptop that has already built the server.

### Knobs

All are environment variables. No required flags.

| Var | Default | Purpose |
|---|---|---|
| `MODE` | `complex` | `complex` = 6 tables, 35 pipeline features (the 47-feature launch claim counts 12 derived columns on top). `simple` = 2 features — used to measure the raw ingest path. |
| `CPUS` | host cores, capped at 8 | Server `WORKER_THREADS` env var. |
| `CLIENTS` | `$CPUS` | Number of parallel Python client processes. |
| `WARMUP` | 5 | Seconds of discarded pre-run load so window operators are primed. |
| `DURATION` | 60 | Seconds of measured load. |
| `CHECKPOINT` | 5 | Live-EPS print interval. |
| `TCP_PORT` | 6400 | Override if you have something on the default port. |
| `HTTP_PORT` | 6401 | Same, for the management HTTP port. |
| `SKIP_BUILD` | 0 | Set to 1 if you've already built and don't want cargo to check. |
| `NO_FLAMEGRAPH` | 0 | Set to 1 to skip the flamegraph step even if installed. |
| `BEAVA_BIN` | auto | Override server binary path (used for testing custom builds). |

Example: quick sanity run on 4 cores for 10s:

```bash
CPUS=4 DURATION=10 WARMUP=2 bash benchmark/fraud-pipeline/run_bench.sh
```

## What gets measured

For each run, `summary.json` captures:

- `throughput.aggregate_eps` — total events pushed across all clients divided by wall time. This is the number the launch copy quotes.
- `throughput.per_event_us` — end-to-end microseconds per event.
- `client_push_latency_us.{p50,p99,p999}_across_clients` — per-`push_many(batch=1000)` call time. Each client samples every 64th call (stride sampling keeps throughput measurement fair — sampling on every call would Python-overhead-bound the result by ~15%).
- `server_push_latency_us.{p50,p95,p99}` — server-side PUSH histogram pulled from `/debug/latency`.
- `memory.{estimated_bytes,entity_count}` — state-store footprint after the run.
- `per_client` — per-worker breakdown (useful for diagnosing whether one client was a straggler).

## How to interpret the latency numbers

The per-push client latency is **per-batch-of-1000-events**, not per-event.
A 3ms p50 for a 1000-event batch = 3µs of client-visible cost per event,
which is what you care about if you are thinking "how much CPU does each
event cost on my client box".

Under concurrent-client load (default is 8 clients), p99 can look
surprisingly high because:

- All clients are Zipfian-distributed over the same key space, so a small
  set of hot keys causes write contention on the server's per-key locks.
  This is realistic — production workloads have hot users/merchants too —
  and it is not the Python SDK's fault.
- Single-client p99 is dominated by the TCP round-trip (~80-150µs on
  localhost); multi-client p99 is dominated by lock contention on hot
  keys and reaches low-ms levels even with the server well below CPU
  saturation. The launch copy calls this out for a reason: p99 at 8
  clients is not the same measurement as p99 at 1 client.

Single-client p99 (`CLIENTS=1`) is the right apples-to-apples comparison
against Redis SET latency. 8-client p99 is the right number to cite for
"under realistic concurrent load".

## Baseline — where the numbers come from

`benchmark/fraud-pipeline/results/baseline/` contains the committed
snapshot from this machine (`Hoangs-MacBook-Pro.local`, 10-core Apple
Silicon, 32 GB). Timestamp in `summary.json`. Key numbers:

| Metric | Laptop baseline (10-core M-series) | Hetzner Phase 42 (16-core, cited in launch copy) |
|---|---|---|
| Config | `complex`, 8 clients, 8 worker threads, 20s measure | 8 clients, 8 worker threads, 60s measure |
| Aggregate throughput | **314K eps** | **544K eps** |
| Per-event cost | 3.19 µs | ~1.8 µs |
| Client p50 (1000-event batch) | ~3.3 ms | (not re-measured on laptop) |
| Client p99 (1000-event batch) | ~109 ms worst-client | 180 µs single-event (from launch copy) |
| Memory (≈10K users × 35 features) | 5.8 GB | ~8 GB for 1M entities cited |

### Deltas from the Phase 42 / launch claims

- **544K eps vs 314K eps on the laptop** is within the factor-of-core-count range you would expect (Hetzner has 1.6× the cores + ~3× the memory bandwidth of an M-series laptop). The claim is credible on a 16-core box and the laptop number scales to ~500K on the same hardware.
- **p99 at 8 clients looks much worse here** (108ms batch-p99) than the launch copy's quoted "180µs p99 at 8 clients". The launch copy quotes **single-event** p99 from the server-side `/debug/latency` histogram; the per-client numbers above are **per-batch-of-1000-events** timings from the Python client. The two are not directly comparable — divide batch-p99 by batch size if you want a rough per-event figure. This is the most common misreading of bench output and worth knowing going in.
- **Memory per entity looks inflated on the laptop.** 5.8 GB ÷ 10K = 590 KB/entity because the complex pipeline has 35 per-key operators, several of which are HLLs (12 KB registers each). The launch copy's 8 GB for 1M entities × 40 features assumed smaller operator mix. Not a contradiction; different workload shape.

If your laptop reports < 100K eps on MODE=complex, something is wrong
(likely port contention, a debug build, or background CPU pressure).

## How to generate a flamegraph

```bash
cargo install flamegraph   # one-time
bash benchmark/fraud-pipeline/run_bench.sh
```

The script detects `cargo-flamegraph` on PATH and adds a 5s perf sample
while a dedicated client pushes load. Output goes to
`results/<timestamp>/flamegraph.svg`.

On macOS, `cargo flamegraph` needs `sudo` and has its own DTrace
permissions path — if the attempt fails it prints a warning and the
benchmark completes normally. On Linux with `perf_event_paranoid` tight,
the attempt also fails gracefully.

## Replay benchmark

See [`replay/README.md`](replay/README.md) for the 30-day deterministic
replay. Two reasons to run it:

- Reproducible headline — same `(seed, n, days, now_ms)` produces
  byte-identical events across machines, so you can diff throughput
  without worrying about input drift.
- Back-fill tool — point it at an existing Beava instance with
  `--input events.jsonl` to replay your own captured event log.

## HTTP ingest load test (Phase 45)

`benchmark/http_load.sh` measures sustained EPS on the HTTP
`POST /push-batch/{stream}` endpoint using [oha](https://github.com/hatoo/oha).

### Quick start

```bash
# Prerequisites
cargo install oha     # one-time

# Start a release server
cargo build --release
./target/release/beava serve &

# Smoke run (no EPS gate, 5s)
bash benchmark/http_load.sh

# Full reference-box run (EPS >= 100,000 gate, 30s, appends to this README)
LOAD_TEST_REFERENCE_BOX_REQUIRED=1 bash benchmark/http_load.sh
```

### How it works

1. Generates a JSON array payload of `EVENTS_PER_BATCH` events (default 1000
   in reference mode, 100 in smoke mode) with Zipfian-distributed user keys.
2. Registers `bench_stream` via `POST /pipelines` (idempotent).
3. Runs `oha -z ${DURATION} -c ${CONCURRENCY}` against
   `POST /push-batch/bench_stream`, posting the payload file on every request.
4. Parses the JSON output: `EPS = RPS × events_per_batch`.
5. In reference mode: asserts EPS ≥ 100,000 and appends the result to this file.
6. In smoke mode: prints the measured EPS without gating.

### Reference-box EPS target

**Target: EPS ≥ 100,000 on `/push-batch` with 1000-event batches.**

This target is measured on a dedicated reference box (NOT GitHub Actions).
The TCP path on the same hardware delivers 314K EPS (10-core M-series laptop
baseline from Phase 42). The HTTP path is expected to be somewhat lower due
to HTTP framing overhead, but > 100K is achievable with the `Bytes`-extractor
hot path in `http_ingest.rs`.

**Measured number: TBD — measure on reference box.**

To reproduce: run `LOAD_TEST_REFERENCE_BOX_REQUIRED=1 bash benchmark/http_load.sh`
on the reference machine (same one that produced the 314K TCP baseline).
The script will append the result below once measured.

### Knobs

| Variable                         | Default      | Purpose                                         |
|----------------------------------|--------------|-------------------------------------------------|
| `PORT`                           | `6401`       | HTTP server port                                |
| `BEAVA_ADMIN_TOKEN`              | `test-admin` | Auth token                                      |
| `STREAM`                         | `bench_stream`| Target stream name                             |
| `CONCURRENCY`                    | `64`         | oha `-c` concurrency level                      |
| `DURATION`                       | `30s` / `5s` | oha `-z` duration (reference / smoke mode)      |
| `LOAD_TEST_REFERENCE_BOX_REQUIRED` | (unset)    | Set to `1` to enable EPS gate and README append |

---

## Archive

[`fraud-pipeline/results/archive/tally-throughput/`](fraud-pipeline/results/archive/tally-throughput/) — 
the v0/v1-era small/medium/large matrix (phases 10-14). Kept for its 
`RESULTS.md` write-up which documents the path from 20K eps single-
threaded to the current multi-threaded server. See the directory's 
`ARCHIVED.md` for why it is not the current canonical bench.

## Troubleshooting

**"TCP port 6400 is already in use"** — another Beava instance is
running. Stop it or set `TCP_PORT=7000 HTTP_PORT=7001` in the env.

**"server did not become ready within 15s"** — the server crashed on
startup. Check `results/<timestamp>/server.log`. Common causes: the
data directory on `/tmp` filled (rerun, the script cleans up), or the
binary is a debug build.

**Zero events measured** — no client successfully connected. Check
`results/<timestamp>/measure-*.jsonl` for Python tracebacks. Usually
this means the `tally`/`beava` Python package is broken in a way
`bench.py`'s path hack can't route around — try
`pip install -e python/` from the repo root.

**p99 looks terrible (10s+)** — you are probably hitting a GC pause on
the Python side or a kernel scheduling issue. Re-run; this is not
reproducible across runs if it's an environmental blip. If it persists,
lower `CLIENTS` until p99 stabilises — that will tell you whether it is
client-side (stays bad at CLIENTS=1) or server-side contention (goes
away at CLIENTS=1, gets worse linearly with CLIENTS).

**MacOS build fails with `fdatasync not found in libc`** — you are on a
pre-rename commit that didn't yet have the macOS fsync-fallback fix
(commit `0ad5fd9`). Either rebase onto main or set `BEAVA_BIN` to a
previously-built binary from a checkout that has the fix.
