# Fraud detection, 60 seconds

You've got a stream of card transactions. You want a real-time feature
vector per user — counts, sums, distinct merchants, last seen — so a
downstream model can score every event in microseconds. Normally this is
Kafka plus Flink plus Redis plus a platform team.

Here it's one binary, one JSON file, and 200 events. Let's run it.

## What you need

- Docker (with `docker compose`), or a local `cargo build --release` of Beava.
- Python 3.10+ on your PATH (used to push events; the SDK is pure-Python).
- 60 seconds.

## Run it

From the repo root:

```bash
docker compose up -d           # starts the Beava server on 6400 + 6401
bash examples/fraud/demo.sh
```

That's it. You should see something like:

```
==> Checking Beava server at http://localhost:6401/health
    server is already running.

==> Registering UserFeatures pipeline
    pipeline registered.

==> Pushing 200 sample events to stream UserFeatures (tcp 6400)
    pushed  200 events in    3 ms (66,754 eps)

==> Fetching features for u123

    user_id                  u123
    tx_count_1h              62
    tx_sum_1h                $ 2,296.46
    avg_amount               $ 37.04
    max_amount_1h            $ 200.77
    unique_merchants         8
    last_merchant            shell_gas
    last_amount              $ 47.05
```

u123 is the hottest user in `sample_events.jsonl` (62 of the 200 events).
Every number above was computed on the fly as each event landed — no batch
job, no materialized view, no flush. Ask for `u456` or `u789` and you get
their own vectors.

```bash
curl -s http://localhost:6401/public/features/u456 | python3 -m json.tool
```

## What just happened

### 1. The pipeline (pipeline.json)

A single keyed stream called `UserFeatures`, grouped by `user_id`, with
seven features:

```json
{
  "name": "UserFeatures",
  "key_field": "user_id",
  "features": [
    {"name": "tx_count_1h",      "type": "count",          "window": "1h"},
    {"name": "tx_sum_1h",        "type": "sum",            "field": "amount",      "window": "1h"},
    {"name": "avg_amount",       "type": "avg",            "field": "amount",      "window": "1h"},
    {"name": "max_amount_1h",    "type": "max",            "field": "amount",      "window": "1h"},
    {"name": "unique_merchants", "type": "distinct_count", "field": "merchant_id", "window": "1h"},
    {"name": "last_merchant",    "type": "last",           "field": "merchant_id"},
    {"name": "last_amount",      "type": "last",           "field": "amount"}
  ]
}
```

`push_events.py` ships this JSON as the payload of a TCP `OP_REGISTER`
frame — the same flat shape the HTTP `POST /pipelines` endpoint accepts
(see [`docs/http-api.md`](../../docs/http-api.md)). Behind the scenes the
server builds the ring-buffer windows, the HyperLogLog sketch, and the
latest-value slots — all in memory, on a single node.

If you prefer curl, the HTTP route works the same from loopback:

```bash
curl -X POST http://localhost:6401/pipelines \
  -H 'Content-Type: application/json' \
  -d @examples/fraud/pipeline.json
```

(`demo.sh` uses TCP so it works identically whether Beava is running
bare-metal or inside `docker compose`, where the HTTP admin gate would
otherwise refuse the Docker bridge peer IP.)

### 2. The push (push_events.py)

200 events streamed in over TCP via the Python SDK:

```python
import beava as bv

@bv.stream
class UserFeatures:
    user_id: str
    merchant_id: str
    amount: float
    country: str
    status: str

app = bv.App("localhost:6400")
for e in events:
    app.push(UserFeatures, e)   # fire-and-forget
app.flush()                      # drain before reading
```

On the wire: a length-prefixed binary frame per event. On the server: one
pass through the pipeline DAG, all operators updated atomically, state
immediately consistent. You can read `u123` the nanosecond after the push
returns.

### 3. The query

```bash
curl -s http://localhost:6401/public/features/u123
```

This hits an in-memory `HashMap::get`. No disk read, no network hop, no
serialization to RocksDB. Median read latency is a couple of microseconds.

## Poke around

While the server's up:

- **All features for any user** — `GET /public/features/{user_id}`
- **Full operator state** (loopback only) — `GET /debug/key/u123`
  Every ring-buffer bucket, every HLL register, every last-value slot.
- **List pipelines** — `GET /pipelines`
- **Pipeline definition** — `GET /pipelines/UserFeatures`
- **Memory rollup per stream** — `GET /debug/memory`

To stop:

```bash
docker compose down
```

## Next steps

- **Author your own pipeline** — the JSON schema lives in
  [`docs/http-api.md`](../../docs/http-api.md); every operator is listed in
  [`docs/operators.md`](../../docs/operators.md).
- **Use the Python SDK end-to-end** — see
  [`docs/python-sdk.md`](../../docs/python-sdk.md) for the decorator API
  (`@bv.stream`, `@bv.table`, `bv.fork()`, `bv.replay()`).
- **Benchmark it** — [`benchmark/fraud-pipeline/`](../../benchmark/fraud-pipeline/)
  runs the full 47-feature fraud pipeline at ~400K events/sec on a laptop.

## Files in this directory

| File | Purpose |
|---|---|
| `pipeline.json` | The 7-feature pipeline definition, POSTed to `/pipelines`. |
| `sample_events.jsonl` | 200 pre-generated transaction events, 10 users, Zipfian skew. |
| `push_events.py` | ~30-line Python script that pushes the events via `bv.App`. |
| `demo.sh` | One-command runner — boots the server if needed, registers, pushes, queries. |
| `README.md` | This file. |
