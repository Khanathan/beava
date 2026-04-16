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
with bv.fork(
    remote="beava-prod.internal:6400",
    streams=[UserEvent],
    keys=["u123"],
    pipelines=[OnboardingSignals],
) as fork:
    print(fork.get(OnboardingSignals, key="u123"))
```

Numbers (47-feature fraud pipeline, reproducible):
- 544K eps sustained on 16-core Hetzner AX52
- 314K eps on 10-core M-series laptop (baseline committed in repo)
- Single-client p99 reads are well under the 10ms SLA most online
  inference paths care about (exact single-client numbers not
  committed in the baseline — the laptop baseline ran 8 clients;
  rerunning with CLIENTS=1 is one command)
- Hot-key contention under 8-client load shows visibly worse tail
  latency than single-client — shard hot keys or debounce if that
  matters for your workload

Reproduce in 70 seconds: `bash benchmark/fraud-pipeline/run_bench.sh`.
Full methodology + the "batch-p99 vs per-event-p99" caveat is in
`benchmark/README.md`.

Failure modes documented up front:
- WAL fsync before client ack (~1s worst-case data loss on crash).
- At RAM ceiling: BEAVA_MEMORY_LIMIT_MB is a fail-loud cap — committed
  state preserved, new writes stop until you resize. No disk spill.
- At-least-once delivery today. Server-side event_id dedup is on the
  roadmap (see python/beava/_client.py); idempotency is the client's
  responsibility for now.
- Single node, no HA today. No primary/replica, no automated failover.
  Automated HA is on the Cloud roadmap (Q4 2026).
- fsync/snapshot stalls: p99 ingest lag during snapshot stays <20ms on
  NVMe; gp3 degrades ~2×.
- Observability: Prometheus `/metrics`, JSON logs, `/health`, RUNBOOK.md.

Honest about scope: pre-launch OSS, single region, working set must
fit in RAM (per-entity cost varies with operator mix). No SOC2/HIPAA
today — Beava Cloud Q4 2026. Solo-maintainer; bringing on a second
committer is a goal, no committed timeline — Apache 2.0 + no CLA means
if the project stalls you can fork everything with no legal friction.

If you already run Flink well, keep running Flink — Beava isn't trying
to displace working infrastructure. Beava exists for teams who haven't
built that platform yet and would rather not have to.

Feedback welcome, especially adversarial reads on SEMANTICS.md and
UNSAFE.md. Two design-partner slots open (90 days, direct Slack).

Blog post with the long-form story: https://beava.dev/blog
