# Building a Real-Time Compute Engine for the Rest of Us

When I was at Viggle, we needed real-time aggregations for our recommendation system. Standard stuff: count user actions in the last hour, track unique items per session, compute moving averages. The logic took a day to write. Setting up Kafka took three weeks.

Not because Kafka is bad. Kafka is a great system. But we had to provision brokers, configure topics and partitions, set up schema registry, write Flink jobs, tune checkpointing, set up a Redis serving layer, and build monitoring for all of it. We were a small team. We didn't have a platform engineer. Every hour spent on infrastructure was an hour not spent on product.

And Kafka is hard to master. The tuning takes years of experience to get right. Stateful management in Flink is genuinely difficult. When something goes wrong at 3 AM, you need someone who deeply understands consumer groups, rebalancing, checkpoint intervals, and RocksDB compaction. At a 20-person startup, that person is usually you, and you'd rather be building features.

I saw the same pattern at Faire and Fennel. Teams that needed maybe 50-100 real-time aggregations over a few million entities. The computation was simple. The infrastructure to support it was not.

## The question I kept asking

Most of the platforms in this space are built on a premise: you already have Kafka, you already have a streaming infrastructure team, and you need a tool that plugs into that ecosystem. For a lot of companies, especially larger ones, that's true.

But for a 10-50 person startup? You don't have Kafka. You don't have a streaming team. You just need some numbers to update when events come in. The data durability guarantees that Kafka provides are less of a concern than the operational burden of running it. You'd trade some durability for something you can spin up in 5 minutes and never think about again.

So the question was: what if you just kept everything in memory on one machine? How far does that get you?

## Pretty far, it turns out

10 million entities at 8 KB each is 80 GB. That's one cloud instance. Modern instances go up to 2-4 TB of RAM. For most fraud detection, ML feature serving, or real-time personalization workloads, the state fits comfortably on a single node.

If you accept that constraint, a lot of complexity disappears:

- No distributed coordination. No consensus protocols. No split-brain recovery.
- No serialization to disk on the hot path. State is a HashMap. Reads are ~0.1 us.
- No checkpoint orchestration. Periodic snapshots to disk, like Redis.
- No separate serving layer. Reads come from the same in-memory state that writes update.

The tradeoff is real: you're bounded by the RAM on one machine, and if the process crashes you lose up to ~30 seconds of state (recovered from the last snapshot). For most startup use cases, that's fine. For a bank processing wire transfers, it's not. Know your requirements.

## What I built

[Tally](https://github.com/petrpan26/tally) is a single Rust binary. Define pipelines, push events over TCP, read results from memory. That's it.

I chose Rust for deterministic memory management. No garbage collector means no GC pauses at any state size, no off-heap workarounds, and no serialization overhead on reads. State lives in a HashMap. Scaling up means getting a bigger instance and restarting. There's nothing to tune.

Every write is synchronous and atomic. All operators across all pipeline stages update in one pass. Reads serve the latest state in microseconds. The consistency model is simple: read-after-write always reflects the latest state.

Here's what a fraud detection pipeline looks like:

```python
import tally as tl

@tl.source
class RawTransactions:
    pass

@tl.dataset(depends_on=[RawTransactions])
class UserTransactions:
    features = tl.group_by("user_id").agg(
        tx_count_1h=tl.count(window="1h"),
        tx_count_24h=tl.count(window="24h"),
        tx_sum_1h=tl.sum("amount", window="1h"),
        tx_avg_24h=tl.avg("amount", window="24h"),
        unique_merchants_24h=tl.distinct_count("merchant_id", window="24h"),
        unique_countries_24h=tl.distinct_count("country", window="24h"),
        last_country=tl.last("country"),
    )
    velocity_spike = tl.derive("(tx_count_1h / 1) / (tx_count_24h / 24)")
    country_hop_flag = tl.derive("unique_countries_24h > 3")

@tl.dataset(depends_on=[RawTransactions], filter="status == 'failed'")
class UserFailedTxns:
    features = tl.group_by("user_id").agg(
        failed_count_1h=tl.count(window="1h"),
        failed_count_24h=tl.count(window="24h"),
    )

@tl.dataset(depends_on=[RawTransactions])
class MerchantActivity:
    features = tl.group_by("merchant_id").agg(
        merch_tx_count_24h=tl.count(window="24h"),
        merch_unique_users_24h=tl.distinct_count("user_id", window="24h"),
    )
```

That's a subset. The full benchmark pipeline has 47 features across 5 entity types: [`bench_fraud.py`](https://github.com/petrpan26/tally/blob/main/benchmark/fraud-pipeline/bench_fraud.py).

Push events, read results. One event fans out to multiple entity types (a transaction updates both user and merchant state). Pipeline stages cascade through a DAG in topological order. 16 operators: counts, sums, averages, percentiles, HLL distinct counts, exponential moving averages, and more.

The first SDK is Python, but Python never touches the hot path. Pipeline definitions are serialized at registration time. All computation happens in Rust. The binary TCP protocol is documented and open, so clients in any language can be built against the spec.

## Numbers

Measured on a 48-core Xeon with the full 47-feature pipeline. Zipfian distribution over 10K users, 2K merchants, 5K devices, 8K IPs.

| Metric | Value |
|--------|-------|
| Throughput (8 clients) | 430-510K events/sec |
| Throughput (single client) | 270K events/sec |
| Sustained load | 29M events, 722K entities, no degradation |
| Memory per entity | 7.6 KB (15 features incl. HLL++) |
| p99 latency | < 100 us |

These are benchmark numbers, not production numbers at scale. The benchmark script is in the repo. Run it on your hardware.

At 7.6 KB per entity, 10M entities fit in 76 GB. 100M entities fit in 760 GB. One machine.

## What's in v0 and what's next

**v0 ships with:** 16 operators, sliding windows, pipeline DAGs, a Python SDK, binary TCP protocol, periodic snapshots, append-only event log.

**On the roadmap:** SQL access layer, session windows, event-time watermarks, connectors, additional SDK languages. Nothing in the architecture prevents these. They're just not built yet.

**Single node today.** Failover with standby replicas is available in the managed service and will be open-sourced soon. Distributed sharding is a future option, but with instances going up to 2-4 TB of RAM, most workloads won't need it.

## Who this is for

Tally is for teams that want real-time compute without the infrastructure commitment. If you can spin up a Docker container and write a few lines of Python, you can have real-time aggregations running in 5 minutes.

It's not for everyone. If you need distributed exactly-once processing, multi-TB state across many nodes, or the Kafka connector ecosystem, use Flink. It's a good system and it solves real problems.

But if you've been putting off real-time features because the infrastructure felt too heavy, this might be worth 5 minutes of your time.

## Try it

Apache 2.0. One binary.

```bash
git clone https://github.com/petrpan26/tally.git && cd tally
docker compose up -d
cd python && pip install -e .
python3 benchmark/fraud-pipeline/bench_fraud.py --events 100000 --clients 4
```

[GitHub](https://github.com/petrpan26/tally) | [Architecture](https://github.com/petrpan26/tally/blob/main/docs/architecture.md) | [Operators](https://github.com/petrpan26/tally/blob/main/docs/operators.md) | [Comparison](https://github.com/petrpan26/tally/blob/main/docs/comparison.md)

I'd appreciate feedback on the design, the API, or the benchmarks.
