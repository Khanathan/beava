# Fraud-scoring pipeline (HTTP variant)

A working fraud-scoring pipeline with 3 streams (Transaction, Device, Login),
2 tables (UserFraudScore, UserLoginPattern), and 10k synthetic events pushed
via HTTP `/push-batch/{stream}`.

This is an HTTP-first variant of the `benchmark/fraud-pipeline/` benchmark.
Pipeline registration uses the Python SDK over TCP; all event ingest and feature
reads use the HTTP API — making the ingest path usable from any language.

## What it demonstrates

- Multi-stream, multi-table pipeline registration via the Python SDK.
- Bulk ingestion via HTTP `/push-batch` (language-agnostic path).
- Feature readback via HTTP `/features/{key}`.
- Per-operator windowed aggregations: count, sum, avg, max, min, stddev.
- Cardinality aggregation: `count_distinct` for unique merchants per user.
- Last-value context features: `last(merchant)`, `last(amount)`.
- Event-time bucketing — events within a 1-hour window bucket correctly.

## Prerequisites

- Docker (pulls `beavadb/beava:latest` automatically)
- Python 3.10+

## Run

```bash
bash run.sh
```

`run.sh` will:

1. Start a Beava container on ports 6900 (HTTP) and 6400 (TCP) if not already
   running.
2. Install the `requests` library.
3. Register the pipeline (Transaction, Device, Login streams + 2 tables).
4. Push 10k synthetic events across all 3 streams via HTTP `/push-batch`.
5. Print features for user `u0001`.

## What you will see

After ~5 seconds:

```json
{
  "UserFraudScore": {
    "tx_count_30m": 49,
    "tx_count_1h": 98,
    "tx_count_24h": 98,
    "tx_sum_1h": 4912.47,
    "tx_avg_1h": 50.13,
    "tx_max_1h": 298.41,
    "unique_merchants_1h": 10,
    "last_merchant": "doordash",
    "last_amount": 17.34
  },
  "UserLoginPattern": {
    "login_count_10m": 8,
    "login_count_1h": 9,
    "last_ip": "10.142.37.201"
  }
}
```

Each user has their fraud feature vector computed across sliding windows and
served from RAM in microseconds.

## Pipeline structure

```
Transaction ──► UserFraudScore   (15 windowed features, keyed by user_id)
Device       (registered but no table — available for future joins)
Login ───────► UserLoginPattern  (4 login-velocity features, keyed by user_id)
```

## Explore further

```bash
# Read features for any user
curl http://localhost:6900/features/u0042

# List registered streams
curl http://localhost:6900/streams

# Single-stream feature filter
curl http://localhost:6900/features/u0001?table=UserFraudScore
```

## Compare to

- `benchmark/fraud-pipeline/` — TCP-push variant used for the published
  315K EPS headline benchmark. Uses the Python SDK end-to-end (no HTTP push).
- `examples/session-features/` — simpler single-stream example to start with.
- `examples/curl-ingest/` — zero-Python variant using only curl and bash.

## Cleanup

```bash
docker stop beava-fraud-scoring
```
