# The lane Databricks left open

_Learnings from building Beava, part 1. An OSS, single-binary, Python-decorator streaming engine for real-time ML features. Parts 2 and 3 follow._

---

In April 2025, Databricks acquired Fennel.

If you have not followed the feature-store space, that one sentence is the whole setup for the post. Fennel was the real-time, Python-first, Rust-backed feature engineering platform that data scientists kept recommending to each other. It was maybe the cleanest answer in the space to "I want to write features in Python and have them compute in real time against streaming data, without rewriting them in Scala or paying a Java cluster to exist." Databricks bought them. Fennel is a Databricks product now.

Chalk.ai is the other serious contender. They raised a $50M Series A at a $500M valuation in May 2025 ([Felicis led](https://www.chalk.ai/)). Their pitch is real, their tech is real ("Symbolic Python Interpreter" that transpiles Python resolvers to Velox), but they are closed-source and sell top-down into fintech at six-figure ACVs. You cannot `pip install chalk` and run it on your laptop.

Feast is the incumbent open-source feature store. Feast is great. Feast is also not a streaming engine. It is a serving facade with connectors to your data warehouse and your online cache. If you want real-time feature computation, Feast asks you to bring your own Kafka, your own Flink, and your own DevOps team.

So here is where an ML engineer in 2026 lands when they say "I want real-time features in Python, open-source, without a cluster":

- Fennel: owned by Databricks, comes with the lakehouse.
- Chalk: call sales.
- Feast: bring your own Flink.
- Flink: hire two SREs.
- Kafka Streams: learn Java.
- Redis: you are on your own for event time, sketches, windows, replay.

None of those answers are "open the terminal, install a thing, have working features in a minute." The lane is open.

I spent the last eight months building the thing that goes in that lane. It is called Beava. This post is about why the lane is real, and why an open-source solo-built binary is actually a sensible answer to a problem most people assume needs a cluster.

## Why one box is now the right answer

In 2014 you built distributed streaming platforms because a single machine could not handle the load. Kafka came out of LinkedIn in 2011, Flink out of the Berlin TU cluster, Storm before that. They were products of their decade. The underlying assumption was that any real-time workload worth caring about would saturate one machine and need to shard across many.

The assumption stopped being true some time around 2020, and by 2026 it is actively wrong for the majority of real-time feature workloads.

A reference measurement. On a 10-core M4 laptop, Beava does 315K events per second sustained on a "complex" 9-cell matrix workload: mixed cardinality, real operator state, event-time aggregations, joins, per-entity reads interleaved with writes. On a 16-core Linux server (Hetzner CCX43 class, costs about USD per month) we measure 918K events per second. Project to 32 cores and the number is in the 2M-3M range.

To put that in perspective: Uber's entire real-time fraud pipeline at the scale where they went public processed low-billions of events per day, which is about 50K per second average. Most production real-time ML feature workloads I have seen live in the 10K-100K events per second range. They fit in one box. The cluster was not the requirement; the cluster was the 2014 hardware assumption.

Three things have shifted in the last 24 months that make a single-binary, solo-built, open-source streaming engine viable in a way it was not before:

1. **Hardware caught up.** Modern laptops have more throughput than 2014 production servers. Modern Hetzner boxes have more throughput than 2016 clusters. A dev can prototype at production scale.
2. **ML engineers write Python. Full stop.** Any engine that asks them to rewrite in Scala is dead on arrival. Chalk figured this out. Fennel figured this out. The message has landed.
3. **AI-assisted coding compressed infra build time maybe 30x.** I am going to say this plainly because I have lived it: a solo developer with Claude, `gstack`, and Codex can ship the engineering work that used to need a team of ten. Beava is 54 phases of test-driven, reviewed, measured work. Shipped by one person. The remaining 20% that still needs a human, architecture decisions, correctness invariants, DX taste, is the part that matters. But the grunt work is gone.

Those three shifts together mean the design space has reopened. A new OSS streaming engine in 2026 does not have to be a Kafka replacement. It can be something closer in shape to Redis, installed in one command, with event time and sketches and replay added on top.

That is the bet.

## The constraint that ate every other decision

When I sat down to write the first line of Beava code, I wrote a different line first. It went into `PROJECT.md`:

> A skeptical engineer evaluating Beava on GitHub should go from landing on the repo to a correct, live feature value in under 60 seconds, from any language.

Not 60 minutes. Not "after you install Docker Compose and read the tutorial." 60 seconds. Pip install. Spin up. `curl` push. `curl` read. See the feature update in real time. If this fails, nothing else matters. No benchmark, no blog post, no clever architecture rescues a failed 60-second evaluation. The first impression is the product.

Here is what 60 seconds looks like in practice today:

```bash
$ pip install beava
$ beava serve &
$ curl -X POST localhost:8080/push/transactions \
     -d '{"user_id":"u42","amount":19.99,"_event_time":1713600000}'
$ curl localhost:8080/features/u42?stream=transactions
{"count_24h": 1, "sum_24h": 19.99, "p95_amount_7d": 19.99, ...}
```

That is it. No topology. No job graph. No savepoint. No checkpoint tuning. No `bootstrap.servers`. If you can use Redis, you can use this.

The decorator API is where the actual feature declarations live:

```python
import beava as bv

@bv.stream(shard_key="user_id")
class Transactions:
    amount: float
    user_id: str
    _event_time: int

    count_24h = bv.count(window="24h")
    sum_7d = bv.sum("amount", window="7d")
    p95_amount_1h = bv.p95("amount", window="1h")
    unique_merchants_30d = bv.hll("merchant_id", window="30d")

app = bv.App(streams=[Transactions])
```

That is a full feature definition. One class. Typed fields. Declarative operators. The `shard_key` tells the engine how to route events across cores for thread-per-core parallelism (we will come back to that in part 2). Every operator returns a typed result with attribute access. Event time is mandatory, not a footnote. Windows are first-class.

Compare to Flink, where the same four features involve declaring sources, keyed streams, window assigners, triggers, evictors, and a `KeyedProcessFunction`. The mechanical complexity of the Flink version is not incidental; it is a product of the distributed assumptions it was designed under. Every piece of flexibility exists because someone somewhere needs it. When you are on one box serving the 95% case, most of that flexibility is dead weight.

## The boring choice that matters most: bundled binary, not PyO3

Here is a decision I want to call out because I think more engineers should make it.

The natural design for a Python SDK over a Rust engine is PyO3. Write your Rust, expose it to Python through a native extension module, ship a wheel, done. Most people reach for this. I did. For six weeks Beava was a PyO3 extension.

It does not work in production for a Python ML serving workload.

The reason is Gunicorn. Or uWSGI. Or any Python production WSGI setup. These servers spawn multiple worker processes, typically one per CPU core. Each worker has isolated memory. If your feature engine lives inside the Python process as a native extension, each worker has _its own copy_ of the engine state. Worker 1 sees one set of features. Worker 4 sees a different set. Write-to-worker-1, read-from-worker-4, get the wrong number. Silent wrong. This is not a theoretical concern; this is what actually happens the first time you deploy.

The fix is not to pin to a single worker (kills your throughput) or to use `gevent` (does not solve the underlying sharing problem). The fix is to run the engine as a separate process and have Python talk to it over localhost.

Beava ships as a bundled Rust binary inside the pip wheel, like `ruff` does. When you `pip install beava`, you get a Python SDK that knows how to start and talk to a Rust sidecar over localhost TCP. All workers hit the same sidecar. Shared state is preserved. The cost is ~50 µs per call, which is invisible next to the kind of work a feature lookup does anyway.

This is the kind of decision that does not sound interesting but determines whether your engine survives contact with a production Python environment. The short version: **if you are writing a Rust library that Python ML serving workloads will use, bundle it as a binary and talk over localhost, not PyO3.**

## What Redis gets right, and what we fix

The blog post title I almost used was "the Redis for real-time features." I did not use it because it sets up a comparison I do not quite want. Beava is Redis-shaped in the way that matters: one binary, one install, obvious API, fits in your head. It is not Redis in the things that matter for ML features.

What Redis gets right, and we copy:

- **One binary, one command, one dependency.** No Zookeeper. No cluster. No operator.
- **Obvious mental model.** Keys and values. `INCR` when a thing happens. `GET` to read it.
- **Low operational surface area.** Your oncall does not fear it.

What Redis gets wrong for real-time ML, and we fix:

- **No event time.** Redis treats everything as wall-clock time. Real-time features need to aggregate by the event's timestamp, not the ingest timestamp. Late arrivals, out-of-order events, backfills all break Redis-style counters.
- **No sketches.** Want p99 latency over a 7-day rolling window? Redis gives you a list and good luck. We ship UDDSketch, CMS+heap, HLL as first-class operators.
- **No replay.** If you deploy a new feature definition, Redis cannot recompute the historical values. You start from zero and wait a week. Beava has a per-stream event log; you add a new operator, you hit replay, you have historical values.
- **No correctness under crash.** Redis persists on a timer and restarts from that snapshot. Beava durably writes via LSM-tree state (fjall) and replays its journal on restart; state is byte-identical to the last acknowledged write.

The short version: Redis is the right _shape_. Beava adds the semantics that real-time ML features actually need.

## Why this matters for the ecosystem

Every open-source category eventually has one. Databases had Postgres. Queues had Kafka. Caches had Redis. Key-value stores had RocksDB. Feature stores had Feast, at the serving layer. Real-time feature computation did not have one; the leading Python-native streaming option got bought.

An OSS answer in this lane matters for three reasons:

1. **ML engineers on tight budgets need somewhere to go.** Not every team has Databricks money. Not every startup can call Chalk sales. An OSS answer means someone on a seed-stage team can ship real-time fraud detection this week.
2. **The lockhouse pattern is getting tired.** Databricks and Snowflake are eating every adjacent category via acquisition. An OSS answer that is actually good is a counterweight. Not competition to Databricks; that is silly. A counterweight, so the category is not entirely vendor-captured.
3. **Single-binary is a feature.** On a 2026 laptop, one binary _is_ the infrastructure. Running Beava on a dev box is indistinguishable from running it in production. No staging environment divergence. No "works on my machine." The feedback loop is tight in a way that only comes from small, simple systems.

## What comes next

The Python decorator API is the surface. Underneath is a stateful Rust engine running thread-per-core, with per-shard fjall LSM partitions, a per-shard event log, an N=1↔N=8 property parity harness as the hard correctness gate, and some quite specific performance lessons that surprised me.

Part 2 of this series is about those lessons. The thesis there: two pieces of conventional wisdom in the Rust systems community are wrong in ways that change how you build a stateful streaming engine. Both of them took a microbench that said "catastrophic" and an integration bench that said "fine" to find. Both of them are load-bearing for why Beava runs on one binary on your laptop.

Part 3 is the vision piece: where this goes when agents and data scientists start submitting their own compute plans against live state.

For now, the ask is the same one the project started with.

```bash
pip install beava
beava serve &
curl -X POST localhost:8080/push/transactions -d '{"user_id":"u42","amount":19.99,"_event_time":1713600000}'
curl localhost:8080/features/u42?stream=transactions
```

Under 60 seconds. Feedback welcome at [beava.dev](https://beava.dev) or on the repo.

---

_DRAFT: ~2400 words. Pulled positioning from `.planning/research/ds-ownership-positioning.md` (Fennel acquisition, Chalk Series A numbers). Open questions for the author: (1) are we comfortable naming Fennel-Databricks acquisition as the hook? It is public, but it's also a competitor-in-spirit. (2) Is the 30x AI-compression claim too strong? Stands behind it or soften? (3) Any numbers I cited (315K, 918K, USD/month) need a final fact-check before publish._
