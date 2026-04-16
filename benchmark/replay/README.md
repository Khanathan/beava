# 30-day replay benchmark

A deterministic, multi-process replay CLI for Beava. Synthesizes 30 days of
fraud-shaped events, replays them into a running instance as fast as possible,
and prints a compact wall-clock report.

This tool has **two purposes**:

1. **Launch benchmark.** Produces the headline number for the v2.1 launch
   blog post — "Beava replayed 30 days in `X` seconds". The generator seed
   is pinned to `42` so the number is reproducible on identical hardware.
2. **Historical backfill tool.** Point `--host/--port` at your own Beava
   and pipe in a JSONL trace via `--input` to replay real historical
   events. Window operators bucket on event time (Phase 8 SCHM-03), so
   30 days of pre-stamped events replayed in 30 seconds of wall-clock
   produces the same feature values you would have computed streaming
   in real time.

## Quick start

```bash
# Launch headline run (production-sized box)
python benchmark/replay/replay_30d.py --events 30000000 --workers 8

# Smoke run against a local dev server
python benchmark/replay/replay_30d.py --events 100000 --workers 4

# Backfill from a captured JSONL trace
python benchmark/replay/replay_30d.py --input events.jsonl --workers 8
```

The report is 7 `key=value` lines on stdout:

```
events_total=30000000
elapsed_seconds=27.413
events_per_sec=1094371
p50_push_us=41.0
p99_push_us=180.0
keys_total=99874
final_state_mb=312.4
```

Use `--output report.json` to also dump the report as JSON for CI tooling.

## Determinism

`generator.generate(n, seed=42)` is pure: the same `(seed, n, days, now_ms)`
tuple produces byte-identical events. In production runs, `now_ms` defaults
to `time.time_ns() // 1_000_000`, so while the *event stream* is deterministic
for a pinned `now_ms`, the *timestamps* track current wall-clock when you
don't pin it — this is what you want for "replay the last 30 days from now".
Tests always pin `now_ms` so they stay wall-clock independent.

Seed pool sizes (100k users, 5k merchants) are chosen to produce realistic
fraud-pipeline fan-out at the 1.1M eps baseline; they are NOT tuning knobs
for the benchmark — change them and you are measuring a different workload.

## Flags

| Flag | Default | Purpose |
|------|---------|---------|
| `--events N` | `30000000` | Event count |
| `--workers W` | `8` | Parallel worker processes |
| `--batch-size B` | `1000` | Events per `push_many` call |
| `--host` | `localhost` | Beava TCP host |
| `--port` | `6400` | Beava TCP port |
| `--mgmt-port` | `6401` | Management HTTP port (used for post-run p50/p99 and keys_total) |
| `--days` | `30` | Width of the timestamp window |
| `--seed` | `42` | Generator RNG seed (do not change for launch runs) |
| `--warmup` / `--no-warmup` | on | 10k-event untimed warmup before measured run |
| `--input path.jsonl` | _none_ | Backfill mode: read events from JSONL, skip generator |
| `--output path.json` | _none_ | Dump the report to a JSON file in addition to stdout |

## CI

Integration test at reduced scale (100k events × 4 workers) lives at
[`tests/integration/test_replay_30d.py`](../../tests/integration/test_replay_30d.py)
and asserts the 7 report fields + an events/sec floor. Run with:

```bash
python -m pytest tests/integration/test_replay_30d.py -x -q
```

Unit tests for the generator (determinism, schema, timestamp spread,
failure-rate distribution) live at
[`tests/integration/test_replay_generator.py`](../../tests/integration/test_replay_generator.py).

## Why multi-process, not threads?

Python's GIL caps single-process SDK-side encoding at ~540k eps (per
Phase 19 benchmarking). To saturate the server's ingest path at 1M+ eps
we need true parallelism — hence `multiprocessing.Pool` with per-worker
`bv.App` connections. Each shard is disjoint by `hash(user_id) % workers`,
minimizing cross-shard contention on the server's per-stream DashMap locks
(Phase 14). See `_shard_events` in `replay_30d.py` for the hashing logic.
