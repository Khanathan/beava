# Show HN Post

## Title

Show HN: Beava — single-binary feature server with bv.fork() for live prod replicas

## URL

https://github.com/petrpan26/beava

## First Comment (post immediately after submission)

When I was at Viggle, setting up Kafka for real-time aggregations took
three weeks. The actual computation logic took a day. We were a small
team with no platform engineer, and every hour on infrastructure was an
hour not on product.

I saw the same pattern at Faire and Fennel. Small teams that needed
windowed counts, sums, distinct counts over a few million entities. The
math was simple. The infrastructure to support it was not.

So I built Beava. Single Rust binary, all state in memory, push events
over TCP, read results in microseconds. The tradeoff: bounded by RAM on
one machine (modern instances go up to 2-4 TB). For most fraud detection
and ML feature workloads, that's enough.

The part I'm proudest of is `bv.fork()` — a Python `with` block that
spawns a local replica of live prod state, scoped to whatever keys you
name. You iterate features against REAL production bytes (not stale
staging data), then close the context and prod doesn't care. Closes the
staging-data skew axis: the bug where your test says 47.3 and prod says
50.1 and you burn two days finding the difference.

```python
import beava as bv

with bv.fork("beava-prod.internal", scope={"user_id": "u123"}):
    # Iterate against scoped live-prod state. Close → prod untouched.
    print(OnboardingSignals.get("u123").clicks_10m)
```

Numbers (47-feature fraud pipeline, reproducible): 544K eps on a 16-core
Hetzner, 314K on a 10-core M-series Mac, sub-100µs p99 single-client.
Zero unsafe outside FFI in the hot path (UNSAFE.md is the audit). Run
yourself: `bash benchmark/fraud-pipeline/run_bench.sh`.

Not distributed. SQL, session windows, and event-time semantics are on
the roadmap, not in v0. If you need distributed exactly-once across
regions, use Flink. If you've been putting off real-time features
because the infrastructure felt too heavy — this might be worth 5
minutes.

Apache 2.0. Two design partner slots open this quarter (90 days, direct
Slack channel). Feedback welcome — especially adversarial reads on the
fork semantics (SEMANTICS.md is grounded in source pointers).

Blog post with the long-form story: https://beava.dev/blog
