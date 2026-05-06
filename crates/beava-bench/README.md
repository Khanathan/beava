# beava-bench — internal benchmark harness

A single canonical `beava-bench` binary with subcommands. **Pick a subcommand
by goal.**

`beava-bench throughput --parallel N` is the production benchmark surface —
the one used to reproduce ledger numbers in
[`.planning/throughput-baselines.md`](../../.planning/throughput-baselines.md).
`mixed / memory / fsync` are CI-friendly smoke surfaces that run a single-
threaded loop against an in-process `TestServer` for quick sanity checks.

```bash
cargo build -p beava-bench --release
```

```bash
# Production headline benchmark (~660K EPS sustained on Apple-M4 / 10 cores
# small/tcp; see throughput-baselines.md for hw-class-conditional rows).
cd crates/beava-bench
../../target/release/beava-bench throughput \
    --pipeline small \
    --transport tcp \
    --wire-format msgpack \
    --duration-secs 60 \
    --parallel 16 \
    --pipeline-depth 1024 \
    --no-ledger
```

Plan 13.7.6-32 consolidated the previous 4-binary cruft (`beava-bench` /
`beava-bench-legacy` / `beava-bench-v18` / `beava-bench-v2`) into this one
entry point. v18's production harness lives in
[`src/harness/production.rs`](src/harness/production.rs); the legacy and v2
archival binaries were deleted (v0.1+ multi-shard benchmarking can be
re-introduced under a renamed subcommand on a clean foundation).

## Subcommands

| Subcommand | Goal | Surface |
|------------|------|---------|
| `throughput` | Production EPS / latency / RSS numbers; ledger reproduction | Real parallelism via `--parallel N` (production harness, msgpack/TCP fast path, sustained-rate or fixed-event-blast). Boots `ServerV18` directly in-process. |
| `mixed` | Read+write ratio sanity (CI-friendly) | Single-threaded against in-process `TestServer`. `--read-write-ratio=70/30`. |
| `memory` | Per-entity RSS / overhead (CI-friendly) | Single-threaded; loads `--entities N` distinct keys, samples RSS, reports `bytes/entity p99`. |
| `fsync` | acks=all per-push fsync wait latency (CI-friendly) | Single-threaded; every push performs `force_snapshot_now()` so latency includes fsync wait. |

---

## `throughput` — production benchmark

Reads `./configs/{pipeline}.json` relative to CWD — **`cd crates/beava-bench`
first**, or pass an explicit `--pipeline /abs/path/to.json`.

### Sustained-rate measurement (60-second window)

```bash
cd crates/beava-bench
../../target/release/beava-bench throughput \
    --pipeline small \
    --transport tcp \
    --wire-format msgpack \
    --duration-secs 60 \
    --parallel 16 \
    --pipeline-depth 1024 \
    --no-ledger
```

When `elapsed >= duration_secs * 0.95`, the run reports `sustained_eps:`. When
`--total-events` caps the run early, the label flips to `burst_eps:` so a
reader can tell at a glance which methodology produced the number (Plan
13.7.6-27).

### Fixed-event blast (zipfian / hot-key shapes)

```bash
cd crates/beava-bench
../../target/release/beava-bench throughput \
    --total-events 1_000_000 \
    --blast-shape zipfian \
    --cardinality 10000 \
    --pipeline small \
    --transport tcp --wire-format msgpack \
    --duration-secs 30 \
    --parallel 16 --pipeline-depth 1024 \
    --no-ledger --isolation-mode
```

`--isolation-mode` adds `wall_clock_ms` / `send_drain_ms` / `ack_lag_ms`
columns so you can attribute drag to send-side or ack-side independently.

### Smoke run (CI-friendly)

```bash
../../target/release/beava-bench throughput \
    --pipeline small --duration-secs 1 --parallel 1 --pipeline-depth 1 --no-ledger
```

`--parallel 1 --pipeline-depth 1` keeps the run cheap; the production
harness still drives the full `ServerV18` boot + register + push loop.

### Bundled pipelines (`configs/`)

- `small.json` / `medium.json` / `large.json` — standard fraud-shape sweep
- `fraud-team.json` — 14-node, 110-feature realistic fraud workload (the
  primary tuning bench; see `project_fraud_team_primary_bench` memory note)
- `medium-with-sketches.json` / `large-with-sketches.json` — sketch-heavy
  variants
- `phase8.json` / `medium_phase9.json` / `large_phase9.json` — historical
  phase-locked configs kept for baseline reproducibility
- `geo.json` — geo-feature variant

Re-baseline helper script: `scripts/run_19_1_rebaseline.sh` (legacy; rewrite
to invoke `beava-bench throughput` is tracked separately).

### Key flags (selected)

| Flag | Purpose |
|------|---------|
| `--pipeline NAME` | Pipeline config (looked up under `./configs/{NAME}.json`) or absolute path. |
| `--transport tcp\|http` | Transport. `tcp` is the fast path (msgpack); `http` is the JSON-on-HTTP/1.1 reach surface. |
| `--wire-format json\|msgpack` | TCP wire format. HTTP always uses JSON regardless. |
| `--duration-secs N` | Wall-clock seconds. Use `--duration-secs 60` without `--total-events` for sustained-rate measurements. |
| `--parallel N` | Number of parallel push-worker connections. Defaults to `min(8, num_cpus)`. Plan 28's headline number was captured at `--parallel 16`. |
| `--pipeline-depth N` | TCP inflight cap per worker; `1024` keeps the apply thread saturated. HTTP transport ignores this. |
| `--total-events N` | Fixed-event blast (caps the run). Reports `burst_eps:` instead of `sustained_eps:`. |
| `--blast-shape fixed\|uniform\|zipfian\|mixed` | Distribution that the pre-encoded frame Pool=N is built from. |
| `--continuous-pipeline=false` | Falls back to the burst send_n→read_n pattern. Default is `true` (sender/receiver split with semaphore-gated inflight queue). |
| `--no-ledger` | Suppress markdown ledger row; only print human summary. |
| `--io-threads N` | Override `BEAVA_IO_THREADS` (Redis-style ratio default = `max(2, available_parallelism / 4)`). |
| `--remote-addr host:http,host:tcp` | Connect to an existing server (multi-bench-client / single-server pattern). |
| `--isolation-mode` | Print `wall_clock_ms / send_drain_ms / ack_lag_ms` columns. |

`beava-bench throughput --help` prints the full flag reference.

---

## `mixed` / `memory` / `fsync` — smoke surfaces

These three subcommands run single-threaded loops against an in-process
`TestServer` and emit a polished `BenchResult` (human / JSON / markdown) +
optional JSONL ledger append. CI-friendly; not production benchmarks.

```bash
beava-bench mixed --workload small --duration 30s --read-write-ratio 70/30
beava-bench memory --workload small --entities 100000
beava-bench fsync --workload small --duration 30s
```

The `--workload` flag accepts the same set as the `throughput` subcommand's
`--pipeline`: `small / medium / large / fraud-team / adtech / fraud /
ecommerce / ...`. Subcommand `--help` prints the per-mode flag list.

---

## Caveats

- All EPS numbers are **hardware-class-conditional**. Apple-M4 / 10 cores
  produces different absolute numbers than 16-core Linux servers; see
  [`.planning/throughput-baselines.md`](../../.planning/throughput-baselines.md)
  for the committed baselines per hw-class and the regression-gate rules
  (10% warn / 25% block).
- For sustained-rate measurements use `--duration-secs 60` **without**
  `--total-events`. The latter caps the run; at our throughput, 1M events
  finishes in ~1.5 s — that is a burst rate, not a sustained one. The
  `sustained_eps:` vs `burst_eps:` label discipline (Plan 13.7.6-27) makes
  the methodology distinction visible at the line level.
- Microbenches (`cargo bench -p beava-bench`) live under `benches/` and are a
  separate per-phase regression-gate surface; see CLAUDE.md §Performance
  Discipline and `.planning/perf-baselines.md`. They do not produce EPS
  numbers — that is what this harness is for.
- `--continuous-pipeline=true` (default) keeps `pipeline_depth` pushes
  always-in-flight via a sender/receiver split with a semaphore-gated
  inflight queue. The receiver path closes the semaphore at exit
  (`sem.close()` immediately before `sender_handle.await`) so the
  deadline-only sustained path doesn't deadlock — Plan 13.7.6-27's fix is
  preserved across the v18 → `harness/production.rs` migration.
