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
- p99 <100µs single-client reads (server-side histogram)
- ~180µs p99 at 8 concurrent writers on hot keys — contention, not
  scale regression (shard or debounce)

Reproduce in 70 seconds: `bash benchmark/fraud-pipeline/run_bench.sh`.
Full methodology + the "batch-p99 vs per-event-p99" caveat is in
`benchmark/README.md`.

Honest about scope: pre-launch OSS, single region, working set must
fit in RAM (modern instances reach 1.5 TB+), WAL fsync before ack,
primary/replica with manual failover, at-least-once delivery. No
SOC2/HIPAA today — Beava Cloud is planned for Q4 2026. One maintainer;
lock-in exit ramp via Apache 2.0 + no CLA + documented on-disk log
format.

Failure modes spelled out in the README (what happens at RAM ceiling,
mid-window crash, hot-key contention, observability via Prometheus +
JSON logs + /health).

If you already run Flink well, keep running Flink — Beava isn't trying
to displace working infrastructure. Beava exists for teams who haven't
built that platform yet and would rather not have to.

Feedback welcome, especially adversarial reads on SEMANTICS.md and
UNSAFE.md. Two design-partner slots open (90 days, direct Slack).

Blog post with the long-form story: https://beava.dev/blog
