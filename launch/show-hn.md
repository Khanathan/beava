# Show HN Post

## Title

Show HN: Beava — a single-binary feature server in Rust (pipeline in, data out, microseconds)

## URL

https://github.com/petrpan26/beava

## First Comment (post immediately after submission)

Hi HN — launching Beava, a single-binary feature server in Rust. Define
a pipeline in Python, push events over TCP, query aggregated features
in microseconds. One process. All state in RAM. Apache 2.0.

Why: we've built real-time features at Faire, Viggle, and Fennel. The
pattern was always the same — the math is simple, the infrastructure is
not. Kafka + Flink + Redis, three on-call rotations, and weeks of
platform work before the first feature shipped. We wanted the short
path for teams that don't have (and don't want to hire for) a streaming
platform group.

What's novel is `bv.fork()` — a Python `with` block that spawns a
scoped replica of live production state. You iterate features against
real production bytes, close the context, prod never sees your reads.
Closes the "staging data says 47.3, prod says 50.1, you burn two days"
bug.

```python
with bv.fork("beava-prod.internal", scope={"user_id": "u123"}):
    print(OnboardingSignals.get("u123").clicks_10m)
```

Numbers (47-feature fraud pipeline, reproducible):
- 544K eps sustained on 16-core Hetzner AX52
- 314K eps on 10-core M-series laptop (baseline committed in repo)
- p99 <100µs single-client reads, HdrHistogram, 256B payload, 1M-key
  cardinality, coordinated-omission corrected
- Contention curve: 180µs @ 8 writers · 480µs @ 32 · 1.2ms @ 64 on
  one key (shard or debounce beyond)

Reproduce in 70 seconds: `bash benchmark/fraud-pipeline/run_bench.sh`.
Full methodology + the "batch-p99 vs per-event-p99" caveat is in
`benchmark/README.md`.

Failure modes documented up front:
- WAL fsync before client ack (~1s worst-case data loss on primary
  crash). Async replica ack NOT required before client ack.
- RAM ceiling returns STATUS_SERVER_BUSY; SDK retries with exponential
  backoff by default.
- At-least-once delivery with event_id Bloom-filter dedup (per-key LRU,
  64 B/key, 5-min window, target FPR 0.1%).
- Primary/replica async log-shipping. Replica lag typically <100ms at
  544K eps; **RPO bound ≈ replica lag**. Manual `bv failover --promote`
  ~2 min RTO.
- fsync/snapshot stalls: p99 ingest lag during snapshot stays <20ms on
  NVMe; gp3 degrades ~2×.
- Observability: Prometheus `/metrics`, JSON logs, `/health`, RUNBOOK.md.

Honest about scope: pre-launch OSS, single region, working set must
fit in RAM (modern instances reach 1.5 TB+). No SOC2/HIPAA today —
Beava Cloud Q4 2026. Solo-maintainer; dated commitment to second
committer by Q3 2026 or a scheduled GitHub Action commits
`abandoned.md` and you fork under Apache 2.0 + no CLA.

If you already run Flink well, keep running Flink — Beava isn't trying
to displace working infrastructure. Beava exists for teams who haven't
built that platform yet and would rather not have to.

Feedback welcome, especially adversarial reads on SEMANTICS.md and
UNSAFE.md. Two design-partner slots open (90 days, direct Slack).

Blog post with the long-form story: https://beava.dev/blog
