# Streaming Shouldn't Require a Platform Team

When I was at Viggle, we needed real-time aggregations for our recommendation system. Standard stuff: count user actions in the last hour, track unique items per session, compute moving averages. The logic took a day to write. Setting up Kafka took three weeks.

Not because Kafka is bad. Kafka is a great system. But we had to provision brokers, configure topics and partitions, set up schema registry, write Flink jobs, tune checkpointing, set up a Redis serving layer, and build monitoring for all of it. We were a small team. We didn't have a platform engineer. Every hour spent on infrastructure was an hour not spent on product.

And the tuning never really ends. Stateful management in Flink is genuinely difficult. When something goes wrong at 3 AM, you need someone who deeply understands consumer groups, rebalancing, checkpoint intervals, and RocksDB compaction. At a 20-person startup, that person is usually you, and you'd rather be building features.

I saw the same pattern at Faire and Fennel. Teams that needed 50–100 real-time aggregations over a few million entities. The computation was simple. The infrastructure to support it was not.

## The question I kept asking

Most of the platforms in this space are built on a premise: you already have Kafka, you already have a streaming infrastructure team, and you need a tool that plugs into that ecosystem. For larger companies, that's a fair premise.

But for a 10–50 person startup? You don't have Kafka. You don't have a streaming team. You just need some numbers to update when events come in. The durability guarantees Kafka provides matter less than the operational burden of running it. You'd trade some durability for something you can spin up in five minutes and never think about again.

So the question was: what if you kept everything in memory on one machine? How far does that get you?

## Pretty far, it turns out

Ten million entities at 8 KB each is 80 GB. That's one cloud instance. Modern instances go up to 2–4 TB of RAM. For most fraud detection, ML feature serving, or real-time personalization workloads, the state fits comfortably on a single node.

If you accept that constraint, a lot of complexity disappears:

- No distributed coordination. No consensus protocols. No split-brain recovery.
- No serialization to disk on the hot path. State is a HashMap. Reads are sub-microsecond.
- No checkpoint orchestration. Periodic snapshots plus an append-only event log for durability.
- No separate serving layer. Reads come from the same in-memory state that writes update.

The tradeoff is real: you're bounded by the RAM on one machine. But durability is solid. Every event is written to an append-only log, fsynced at a configurable cadence. On crash, state recovers from the last snapshot plus log replay. Worst case you lose about a second of events — comparable to Redis with AOF.

## What v0 ships

[Tally](https://github.com/petrpan26/tally) is a single Rust binary. Define pipelines, push events over TCP, read results from memory.

The surface in v0 is deliberately small. Two types. One query model. One event-time story. Everything else is either built on those primitives or explicitly deferred.

**Two types.** A `Stream` is an append-only log of events keyed by a field you choose. A `Table` is keyed current-state you upsert into directly, or derive from one or more Streams by writing a function. Class-form decorators declare sources; function-form decorators declare derivations. The DAG is auto-discovered from the function's parameter types — there's no separate topology file.

**DataFrame-parity operators.** `filter`, `map`, `select`, `drop`, `rename`, `with_columns`, `cast`, `fillna`, `group_by().agg()`, `join`, `union`. If you've written pandas or Polars, the shape is already familiar. Expressions are built with `tl.col("field")` and the usual arithmetic / comparison / boolean surface.

**Aggregation catalog.** `count`, `sum`, `avg`, `variance`, `stddev`, `min`, `max`, `first`, `last`, `first_n`, `last_n`, `ema`, `lag` as exact operators, plus three hybrid-sketch operators: `percentile` (UDDSketch with a configurable exact → approximate transition), `count_distinct` (exact-set → HLL at cardinality cap), and `top_k` (Count-Min Sketch plus a heap of candidates). The hybrids start exact and switch to the sketch when they outgrow the memory budget — you see a one-time α-drift when that boundary is crossed, documented per-operator, rather than a permanent approximation tax.

**Joins.** Stream↔Stream windowed (inner and left) with a `within=` duration. Stream↔Table enrichment: every PUSH to the Stream looks up the current Table row for its join key and emits an enriched event. Table↔Table same-key joins for merging current-state records across entities.

**Event time.** Events carry an optional timestamp in a reserved `_event.timestamp` field; operators bucket by event time, not wall-clock arrival time. A fixed 5-second watermark admits late events up to that bound; beyond it they're rejected with an incrementing `tally_late_events_dropped_total{stream}` counter. Tunable lateness ships post-v0.

**Query surface.** `GET key` returns every feature for that key across every Stream and Table. `MGET` batches across keys. `GET_MULTI` batches across Tables in one round trip with null-collapse — if a key is missing in a Table, that Table's columns come back as `null` rather than failing the whole read.

**TTLs.** Tables default to 30-day retention, Stream history to 90 days, tombstones to a 7-day grace window. All configurable per-entity.

**Observability.** `/metrics` exposes the Prometheus surface: `tally_events_total`, `tally_current_eps`, `tally_push_latency_p99_seconds`, `tally_late_events_dropped_total{stream}`, memory, keys, snapshot counters. `/debug/warnings` is a unified feed of anything the server wants to flag — hot keys, rising late-drop rates, config smells. A `tally suggest-config` CLI reads live server state and tells you what to change.

Here's what a fraud pipeline looks like:

```python
import tally as tl

@tl.stream
class RawTxns:
    user_id: str
    merchant_id: str
    amount: float
    status: str
    country: str

@tl.table(key="user_id")
def UserFeatures(raw: RawTxns) -> tl.Table:
    return raw.group_by("user_id").agg(
        tx_count_1h=tl.count(window="1h"),
        tx_sum_1h=tl.sum("amount", window="1h"),
        avg_amount_1h=tl.avg("amount", window="1h"),
        max_amount_24h=tl.max("amount", window="24h"),
        failed_count_30m=tl.count(window="30m", where="status == 'failed'"),
        unique_merchants_24h=tl.count_distinct("merchant_id", window="24h"),
    )
```

Register it, push events, read features:

```python
app = tl.App("localhost:6400")
app.register(RawTxns, UserFeatures)

app.push(RawTxns, {
    "user_id": "u123",
    "merchant_id": "m456",
    "amount": 50.0,
    "status": "success",
    "country": "US",
})

features = app.get("u123")
# {'tx_count_1h': 7, 'failed_count_30m': 1, 'unique_merchants_24h': 5, ...}
```

Enriching a Stream from a Table:

```python
@tl.table(key="merchant_id")
class Merchants:
    merchant_id: str
    risk_band: str
    chargeback_rate_30d: float

@tl.stream
def EnrichedTxns(raw: RawTxns, merchants: Merchants) -> tl.Stream:
    return raw.join(merchants, on="merchant_id", how="left")
```

Percentiles and heavy hitters, using the hybrid sketches:

```python
@tl.table(key="user_id")
def LatencyFeatures(raw: RawTxns) -> tl.Table:
    return raw.group_by("user_id").agg(
        p50_amount_1h=tl.percentile("amount", q=0.50, window="1h"),
        p99_amount_1h=tl.percentile("amount", q=0.99, window="1h"),
        top_merchants_24h=tl.top_k("merchant_id", k=10, window="24h"),
    )
```

Reading multiple Tables in one round trip:

```python
app.get_multi([UserFeatures, LatencyFeatures], "u123")
# {'UserFeatures': {...}, 'LatencyFeatures': {...}}
# Missing tables come back as null rather than erroring.
```

The first SDK is Python, but Python never touches the hot path. Pipeline definitions are serialized at registration time, parsed by the server, and every subsequent event goes through Rust only. The binary TCP protocol is documented, so clients in any language can be built against the spec.

## What v0 does **not** do

I'm writing this section out in full because I'd rather you know before you pick the tool than find out in production.

- **Table-input `group_by().agg()`.** v0 aggregates over Streams only. Aggregating over a Table (where rows can be upserted and deleted) requires DAG retraction propagation; we deferred that to keep v0 shippable. v0.1.
- **DAG retraction propagation.** When a sketch emits a late-arriving correction, or a Table row is tombstoned, the correction doesn't automatically flow through multi-hop derivations in v0. Single-hop (Stream → Table) is correct; multi-hop propagation is a v0.1 problem that we've sketched a plan for but not built.
- **Outer joins.** Inner and left in v0. Right and full join are deferred.
- **Session windows.** Tumbling and sliding in v0. Session windows — where the window boundary is defined by a gap in activity — are not implemented.
- **CEP / `match_recognize`.** No pattern-matching DSL.
- **`SCAN` / `SUBSCRIBE`.** Reserved opcodes (`0x10`, `0x11`) in the protocol. Not implemented in v0.
- **Horizontal scale-out.** Key-partitioned multi-threading is a documented path, not a shipped feature. Single-process handles most workloads we've seen; when you outgrow it, you shard clients.
- **CI/CD integration for the regression gate.** The gate runs; wiring it into GitHub Actions and a cross-platform test matrix is post-v0 work.

"Coming soon" is not on this list. These are things v0 doesn't do.

## Performance

Benchmarks are in the repo (`benchmark/tally-throughput/bench_v0.py`). The nine-cell matrix — small/medium/large pipelines × 1/4/8 clients — is the pre-launch regression gate; v0 sign-off requires every cell within 5% of the v2.0 baseline.

The launch headline number will be the 30-day replay on the production VM: thirty million synthetic fraud-shaped events, pushed through the fraud pipeline (counts, sums, percentiles, distinct counts, heavy hitters), measured on a [Hetzner CX22]({{DEMO_URL}}) (2 vCPU, 4 GB RAM). That number fills in here once the 5-day bare-metal run completes:

```
$ python3 benchmark/replay/replay_30d.py --events 30000000 --workers 8
events_total=30000000
elapsed_seconds=<TBD after deploy>
events_per_sec=<TBD after deploy>
p50_push_us=<TBD>
p99_push_us=<TBD>
```

The live demo at **{{DEMO_URL}}** polls `/public/stats` every two seconds and shows the counters update in real time — read-only, nothing to set up. Sketch micro-benchmarks are in the repo too: UDDSketch insert under 500 ns, Count-Min Sketch insert under 200 ns, HyperLogLog insert under 200 ns, on commodity hardware.

## How this fits in the landscape

Tally is a real-time feature server. The closest adjacent tools each solve a different-shaped problem; it's worth being explicit about where we overlap and where we don't.

**Flink.** Flink is the reference implementation for distributed event-time streaming: proper watermarks, side outputs for late data, exactly-once state with RocksDB checkpoints, SQL, CEP, rich window semantics including sessions. The cost is operational: a JobManager, TaskManagers, checkpoint storage, and a team that understands them. Flink wins for large-scale, multi-team, durability-critical workloads. Tally doesn't try to replace Flink there — v0 runs a single binary with a 5-second watermark and no CEP. Where Tally wins is the small-team case: you get event-time correctness inside the watermark, a DataFrame API, and one process to operate.

**ksqlDB.** ksqlDB is SQL on top of Kafka Streams: great if you're already invested in the Kafka ecosystem, weak if you aren't. It inherits Kafka's operational model (brokers, topics, consumer groups) and its retraction semantics (late events update emitted results inside the grace period, drop after). Tally's surface is a Python DataFrame API rather than SQL, and the storage layer is the process itself rather than Kafka — no broker to run, no grace-period tuning beyond the 5-second watermark.

**Materialize.** Materialize is a standing-view SQL database with strong incremental-view-maintenance semantics: everything is retraction-native via differential dataflow, updates propagate correctly through multi-hop views, and the consistency story is genuinely state-of-the-art. It's also a full distributed database (coordinator, workers, a persistent storage layer). Tally overlaps on "incremental views of streaming data" but picks a much smaller point in the design space: single process, in-memory, a subset of retraction semantics (single-hop propagation in v0, multi-hop in v0.1), and a DataFrame API rather than SQL. If you need the full retraction-correct SQL surface, use Materialize. If you need something you can run in a Docker container and forget about, use Tally.

**Fennel.** Fennel pioneered the function-form `@dataset` decorator pattern that Tally v0 borrows heavily from. Fennel's focus is managed feature infrastructure: a hosted service that ingests from your sources, runs your Python transforms on Spark/Rust, and serves features with lineage tracking. Tally is self-hosted, single-binary, and trades lineage + managed infrastructure for simplicity. (Disclosure: I worked at Fennel. Databricks acquired Fennel in 2025, which was part of the context behind Tally.)

None of these are bad systems. They're just shaped for different problems. If you were going to run one of them without a platform team, you'd probably pick the wrong one.

## Who this is for

Tally is for teams that want real-time compute without the infrastructure commitment. If you can run a binary and write a few lines of Python, you can have real-time features in five minutes. It's not a Flink replacement for a 500-person data org; it's a tool for the 20-person startup that deferred "real-time" because the setup felt too heavy.

## Try it

Apache 2.0. One binary.

```bash
git clone https://github.com/petrpan26/tally.git
cd tally
cargo build --release --bin tally
target/release/tally &

cd python && pip install -e .
python3 - <<'PY'
import tally as tl

@tl.stream
class Clicks:
    user_id: str
    url: str

@tl.table(key="user_id")
def ClickFeatures(c: Clicks) -> tl.Table:
    return c.group_by("user_id").agg(
        clicks_1h=tl.count(window="1h"),
        last_url=tl.last("url"),
    )

app = tl.App("localhost:6400")
app.register(Clicks, ClickFeatures)
app.push(Clicks, {"user_id": "u1", "url": "/home"})
print(app.get("u1"))
PY
```

Live demo at **{{DEMO_URL}}**. If you use [Claude Code](https://claude.ai/claude-code), the repo ships with a `/tally` skill that walks through pipeline design, capacity planning, and live diagnostics against your own server.

[GitHub](https://github.com/petrpan26/tally) · [Architecture](https://github.com/petrpan26/tally/blob/main/docs/architecture.md) · [Operators](https://github.com/petrpan26/tally/blob/main/docs/operators.md) · [Python SDK](https://github.com/petrpan26/tally/blob/main/docs/python-sdk.md)

I'd appreciate feedback on the design, the API, or the benchmarks.
