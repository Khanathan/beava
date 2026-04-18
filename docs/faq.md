# FAQ

See also: [docs/comparison.md](comparison.md) · [docs/architecture.md](architecture.md) · [docs/event-time.md](event-time.md)

---

## Will it scale?

Short answer: **vertically, yes. Horizontally, not today.**

Beava runs on one box with NVMe. The published fraud-pipeline baseline is 315 K events/sec
sustained on a 10-core Apple M4 laptop; HTTP push-batch exceeds 100 K EPS on the same
hardware (see [benchmark/](../benchmark/)).

For a single-box workload that fits in RAM — sub-millisecond p99 reads, <500 K EPS
ingest, <64 GB of state — Beava is sufficient.

For horizontal scale-out, the roadmap is:

- **v1.2**: thread-per-core runtime (unlocks higher per-node throughput by eliminating
  cross-thread lock contention).
- **v1.3+**: multi-node via Kafka (partition by key; each shard is an independent Beava
  process).
- **Beava Cloud** (Q4 2026): managed HA with automated failover.

If you need horizontal scale today, Flink + Redis is the right choice. That is honest.

See [docs/architecture.md § Scaling Posture](architecture.md#scaling-posture) for
the full tier breakdown.

---

## What about Flink?

Flink and Beava overlap on event-time streaming aggregations. Flink is a mature,
battle-tested distributed system with exactly-once semantics, full SQL, and a multi-year
production track record at scale.

**Pick Flink if:**
- You need multi-node horizontal scale today.
- You need exactly-once.
- You have a platform team that can run a Kafka + Flink + Redis stack.
- Your state exceeds what fits in RAM on the largest available machine.
- You need complex windowing: session windows, temporal patterns (CEP), global windows.

**Pick Beava if:**
- You want one binary, one API, one mental model.
- You do not have a streaming platform team yet.
- You want training/serving parity via the same Python file.
- Your working set fits in RAM and your scale is single-node.
- You want to ship a real-time feature pipeline in a day, not a week.

Beava is not trying to replace Flink for the companies already running Flink well.
Beava is for the long tail that has not built that platform and does not want to.

See [docs/comparison.md § Beava vs Flink + Redis](comparison.md#beava-vs-flink--redis)
for the detailed pairwise.

---

## Is this production-ready?

**No — not in the "bet your SaaS on it" sense.** Beava v0.1.0 is an early public
release. APIs may shift between v0.x releases. No SOC2, HIPAA, or PCI. Single-maintainer
today (bus factor 1 — see [GOVERNANCE.md](../GOVERNANCE.md)).

**Yes — in the "run it for real workloads with eyes open" sense.** Every correctness
claim has a test; the ship gate (Phase 46) verified backfill → crash → recover →
feature-parity end-to-end. Durability is WAL fsync before ack (configurable). Recovery
is 7 s for 4.7 GB of state.

For regulated workloads (healthcare, finance, gov), wait for Beava Cloud. For
side-projects, internal tools, and exploratory ML infra — Beava is ready.

---

## How does it compare to Feast?

Feast is a feature-store standard: offline + online store, feature registry,
materialization pipelines. Feast orchestrates storage; it does not own the ingest path.

Beava owns the ingest path. You push events directly — no materialization job, no
offline→online sync. The tradeoff: Feast is a better fit if your features are
batch-computed from Snowflake or BigQuery and you need them served at read-time. Beava
is a better fit if your features are computed over event streams and you want
sub-millisecond read latency.

The two are not mutually exclusive — a team could run Feast for batch-derived features
and Beava for the real-time ones.

See [docs/comparison.md § Beava vs Feast](comparison.md#beava-vs-feast) for the full
head-to-head table.

---

## Why not use Kafka + Flink + Redis?

If you already run Kafka + Flink + Redis and it works, keep it. The three systems
are excellent.

If you are greenfield and evaluating: Kafka + Flink + Redis is 5-8 systems to deploy,
a Kafka cluster to tune, a Flink operator pattern to learn, and a Redis failover story.
Beava is one binary.

The comparison is apples-to-oranges at scale — a single-box Beava will not outperform
a well-tuned distributed pipeline on sustained throughput. The question is whether your
workload needs distributed infrastructure. For a lot of ML and analytics use cases, it
does not.

---

## How does event-time work?

Beava assigns each event to a bucket based on the `_event_time` field in the event
payload (Unix milliseconds or RFC 3339 string). If absent, server wall-clock is used.
Watermarks track event-time progress and gate late-arrival handling.

See [docs/event-time.md](event-time.md) for the full reference:
- Bucket assignment and UNIX-epoch-relative bucket boundaries
- Watermark lateness defaults (5 s) and per-stream overrides
- Crash-replay determinism (bit-identical feature values after restart)
- TTL semantics driven by event-time, not wall-clock
- Fork watermark propagation for local replicas

---

## Does it support exactly-once?

No. At-least-once is the honest semantic. Exactly-once at the push layer requires
client-side `event_id` dedup; accumulating operators (`count`, `sum`) are the client's
responsibility to make idempotent.

---

## What languages does it support?

Any language that can speak HTTP is supported. Push events with `curl`, read features
with `curl`. No SDK required.

For native Python pipelines with decorator syntax (`@bv.stream`, `@bv.table`) and
the `bv.fork` workflow, use the Python SDK. See [docs/python-sdk.md](python-sdk.md).

---

## Can I run it on Kubernetes?

Yes, as a StatefulSet with a single replica + PersistentVolume. Beava does not ship k8s
manifests; the single-node wrapper is straightforward. Multi-replica is NOT a valid
deployment — there is no leader election today. A multi-replica StatefulSet would give
you independent instances with no shared state, which is probably not what you want.

---

## Why Rust?

- Predictable latency (no GC pauses).
- `unsafe` is explicit and auditable; Beava has a small number of FFI blocks covering
  `write` / `fdatasync` / `fsync` in `src/state/event_log.rs`. See
  [UNSAFE.md](../UNSAFE.md) for the full inventory.
- Single static binary; no runtime to install.
- Tokio + axum for the network stack; no custom async reactor.

---

## How do I contribute?

Read [CONTRIBUTING.md](../CONTRIBUTING.md). Apache 2.0 license, no CLA; PRs welcome.

---

## Where is the Docker image?

`beavadb/beava:latest` on Docker Hub. See [Getting Started](getting-started.md) for the
quickstart or [docs/docker-publish-runbook.md](docker-publish-runbook.md) for the
publish runbook.

---

## What is the roadmap?

The roadmap is phase-milestone, not date-gated (except Beava Cloud). See
[.planning/ROADMAP.md](../.planning/ROADMAP.md) if browsing the repo. Dates promised
become trust lost when they slip, so the public roadmap is intentionally open-ended
beyond the Q4 2026 Beava Cloud milestone.
