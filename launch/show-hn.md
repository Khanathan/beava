# Show HN Post

## Title

Show HN: Tally -- single-binary real-time compute engine in Rust (in-memory, 430K eps)

## URL

https://github.com/petrpan26/tally

## First Comment (post immediately after submission)

When I was at Viggle, setting up Kafka for real-time aggregations took three weeks. The actual computation logic took a day. We were a small team with no platform engineer, and every hour on infrastructure was an hour not on product.

I saw the same pattern at Faire and Fennel. Small teams that needed windowed counts, sums, distinct counts over a few million entities. The math was simple. The infrastructure to support it was not. Most streaming platforms assume you already have Kafka. Most startups don't.

So I built Tally. Single Rust binary, all state in memory, push events over TCP, read results in microseconds. The tradeoff: you're bounded by RAM on one machine (modern instances go up to 2-4 TB). For most fraud detection and ML feature workloads, that's plenty.

Numbers (47-feature fraud pipeline, 48-core Xeon): 430-510K eps, 7.6 KB/entity, sub-100us p99. Benchmark in the repo, run it yourself.

Not distributed. SQL, session windows, and event-time semantics are on the roadmap but not in v0. If you need distributed exactly-once, use Flink. If you've been putting off real-time features because the infrastructure felt too heavy, this might be worth 5 minutes.

Blog post with the full reasoning: [link](https://github.com/petrpan26/tally/blob/main/docs/blog/why-real-time-features-dont-need-kafka.md). Apache 2.0. Feedback welcome.
