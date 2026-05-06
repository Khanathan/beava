# beava-bench — internal benchmark harness

Multiple binaries, each tuned for a specific workflow. **Pick by goal.**

The polished `beava-bench` CLI is a CI-friendly smoke-test surface; the
production EPS / latency / RSS numbers committed to
[`.planning/throughput-baselines.md`](../../.planning/throughput-baselines.md)
come from `beava-bench-v18`. **Using the wrong binary will produce numbers
that are off by ~100×.**

## Decision tree

| Goal | Use this binary | Example |
|------|-----------------|---------|
| Smoke test (CI-friendly, 1 thread) | `beava-bench` (the polished CLI) | `cargo run -p beava-bench --bin beava-bench --release -- throughput --workload small` |
| Production-shape EPS numbers | `beava-bench-v18` | `cd crates/beava-bench && ../../target/release/beava-bench-v18 --pipeline small --transport tcp --wire-format msgpack --parallel 32` |
| Phase 7.5-style HTTP fsync-bottleneck baseline | `beava-bench-legacy` | _(deprecated; archival only)_ |
| Multi-shard / cluster sweeps | `beava-bench-v2` | _(early-stage; v0.1+ scope)_ |

Build all four with:
```bash
cargo build -p beava-bench --release
```

---

## `beava-bench` — polished CLI (smoke-test only)

- 4 subcommands: `throughput / mixed / memory / fsync`
- **Single-threaded — the `throughput` subcommand silently ignores `--parallel N`**
  (this is intentional; for real parallelism use `beava-bench-v18`)
- Designed for CI smoke (~1K–10K EPS); reproducible defaults
- Demo workloads bundled: `adtech`, `ecommerce`, `fraud-team`

```bash
cargo run -p beava-bench --bin beava-bench --release -- throughput \
    --workload small --duration-secs 10
```

If you measured a number with this binary and it looks low, that is the
expected smoke-test ceiling — switch to `beava-bench-v18` before drawing any
conclusion about beava's throughput.

---

## `beava-bench-v18` — production benchmarks

This is the binary that produces the rows in
`.planning/throughput-baselines.md`.

- Mode: pipeline-shaped throughput sweep
- Multi-worker (real parallelism via `--parallel N`)
- Wire format: msgpack (default for TCP) or JSON
- Transport: HTTP or TCP
- Boots `ServerV18` directly in-process (no external `beava server` needed)
- Reads `./configs/{pipeline}.json` relative to CWD — **`cd crates/beava-bench`
  first**, or pass an explicit `--pipeline /abs/path/to.json`

Sustained-rate measurement (60-second window):
```bash
cd crates/beava-bench
../../target/release/beava-bench-v18 \
    --pipeline small \
    --transport tcp \
    --wire-format msgpack \
    --duration-secs 60 \
    --parallel 32 \
    --no-ledger
```

Fixed-event blast (e.g. for distribution / hot-key zipfian shapes):
```bash
cd crates/beava-bench
../../target/release/beava-bench-v18 \
    --total-events 1_000_000 \
    --blast-shape zipfian \
    --pipeline small \
    --transport tcp --wire-format msgpack \
    --duration-secs 30 \
    --parallel 16 --pipeline-depth 1024 \
    --no-ledger --isolation-mode
```

Bundled pipelines (under `configs/`):
- `small.json` / `medium.json` / `large.json` — standard fraud-shape sweep
- `fraud-team.json` — 14-node, 110-feature realistic fraud workload (the
  primary tuning bench; see `project_fraud_team_primary_bench` memory note)
- `medium-with-sketches.json` / `large-with-sketches.json` — sketch-heavy
  variants
- `phase8.json` / `medium_phase9.json` / `large_phase9.json` — historical
  phase-locked configs kept for baseline reproducibility
- `geo.json` — geo-feature variant

Re-baseline helper script: `scripts/run_19_1_rebaseline.sh`.

---

## `beava-bench-legacy` — archival

- Phase 7.5-era HTTP-only bench; fsync-bottleneck baseline
- Drives the engine through the in-process `TestServer` (not `ServerV18`)
- Kept for historical comparison with pre-Phase-12.6 numbers; do not use for
  new work

```bash
cargo run -p beava-bench --release --bin beava-bench-legacy -- \
    --pipeline small --transport http --duration-secs 60
```

---

## `beava-bench-v2` — early-stage (multi-shard / cluster)

- Multi-target streaming bench: `--targets host:tcp_port,host:tcp_port,...`
- Shard strategies: `round-robin` (worker→target pin) or `hash` (key→target,
  Redis-cluster style)
- Built-in `/metrics` scraper per admin endpoint; per-shard + aggregate report
- Pattern sweep: `--sweep-pipeline-depth 1,16,256,1024 --sweep-blast-shape uniform,zipfian`
- Pipelines must be **registered externally (curl) before this bench runs**;
  the bench itself only pushes events.
- Out of v0 scope; v0.1+ candidate. **CLI surface will likely change.**

---

## Caveats

- All binaries assume the engine is built with `cargo build --workspace --release`.
- All EPS numbers are **hardware-class-conditional**. Apple-M4 / 10 cores
  produces different absolute numbers than 16-core Linux servers; see
  [`.planning/throughput-baselines.md`](../../.planning/throughput-baselines.md)
  for the committed baselines per hw-class and the regression-gate rules
  (10% warn / 25% block).
- For sustained-rate measurements use `--duration-secs 60` **without**
  `--total-events` (the latter caps the run; at our throughput, 1M events
  finishes in ~1.5s — that is a burst rate, not a sustained one).
- Microbenches (`cargo bench -p beava-bench`) live under `benches/` and are a
  separate per-phase regression-gate surface; see CLAUDE.md §Performance
  Discipline and `.planning/perf-baselines.md`. They do not produce EPS
  numbers — that is what this harness is for.
