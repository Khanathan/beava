# Beava Python SDK

See also: [docs/http-api.md](http-api.md) · [docs/architecture.md](architecture.md) · [docs/concepts.md](concepts.md)

The Beava Python SDK is a thin client for the Beava real-time feature server. You define
pipelines in Python, register them with the server, push events, and read computed features.
Python never touches the hot path -- all computation happens server-side in Rust.

## Installation

Requires Python 3.10+.

```bash
pip install beava
```

Or install from source (development):

```bash
cd python
pip install -e .
```

There are no external dependencies. The SDK uses only the Python standard library.

## Quick Start

```python
import beava as bv

# 1. Declare an event source
@bv.stream
class Transactions:
    user_id: str
    amount: float
    merchant_id: str

# 2. Define a dataset with features
@bv.table(key="user_id")
def UserFeatures(tra: Transactions) -> bv.Table:
    return tra.group_by("user_id").agg(
        tx_count_1h=bv.count(window="1h"),
        tx_sum_1h=bv.sum("amount", window="1h"),
        avg_amount_1h=bv.avg("amount", window="1h"),
    )

# 3. Connect and register
app = bv.App("localhost:6400")
app.register(UserFeatures)  # registers Transactions automatically

# 4. Push an event
app.push(Transactions, {
    "user_id": "u123",
    "amount": 50.0,
})
app.flush()

# 5. Read features
features = app.get("u123")
print(features.tx_count_1h)    # 1
print(features.avg_amount_1h)  # 50.0
```

## Defining Sources

A **source** is an event stream entry point. Events flow in through sources and feed into
datasets. Sources themselves do not compute features -- they declare where raw events enter
the pipeline.

Use the `@bv.stream` decorator:

```python
@bv.stream
class Transactions:
    user_id: str
    amount: float
    merchant_id: str
```

### Source options

You can pass optional parameters to control TTL behavior:

```python
@bv.stream(entity_ttl="5m", history_ttl="72h")
class Transactions:
    user_id: str
    amount: float
```

| Parameter      | Description                                              |
|----------------|----------------------------------------------------------|
| `entity_ttl`   | How long to keep entity state after the last event.      |
| `history_ttl`  | How long to retain event history for backfill/replay.    |

### Typed event schemas

For pipeline validation, declare the event schema directly on the `@bv.stream` class:

```python
# v0 streams are plain @bv.stream-decorated classes with type-annotated fields

@bv.stream
class Transactions:
    user_id: str
    amount: float
    merchant_id: str
    status: str
```

This enables `bv.validate()` to check that operator field references match the event schema
before you register with the server.

## Defining Datasets

A **dataset** is a keyed aggregation pipeline. It depends on one or more sources (or other
datasets), groups events by a key field, and computes features using operators.

```python
@bv.table(key="user_id")
def UserMetrics(tra: Transactions) -> bv.Table:
    return tra.group_by("user_id").agg(
        tx_count_1h=bv.count(window="1h"),
        tx_sum_1h=bv.sum("amount", window="1h"),
    )
```

### The `group_by().agg()` pattern

Every dataset declares a `features` attribute using `bv.group_by("key_field").agg(...)`.
This tells the server which event field to use as the entity key and which aggregations
to maintain.

```python
features = bv.group_by("user_id").agg(
    feature_name=bv.operator(...),
    another_feature=bv.operator(...),
)
```

### Derived features

Add derived features using `.with_columns()` chained after `.agg()`:

```python
@bv.table(key="user_id")
def UserMetrics(tra: Transactions) -> bv.Table:
    return tra.group_by("user_id").agg(
        tx_count_1h=bv.count(window="1h"),
        tx_count_24h=bv.count(window="24h"),
    ).with_columns(
        velocity_spike=bv.derive("(tx_count_1h / 1) / (tx_count_24h / 24)"),
    )
```

### Cascading datasets

A dataset can depend on another dataset, enabling multi-stage pipelines:

```python
@bv.stream
class RawEvents:
    user_id: str
    amount: float
    merchant_id: str

@bv.table(key="user_id")
def UserTxns(raw: RawEvents) -> bv.Table:
    return raw.group_by("user_id").agg(
        tx_count_1h=bv.count(window="1h"),
    )

@bv.table(key="merchant_id")
def MerchantTxns(raw: RawEvents) -> bv.Table:
    return raw.group_by("merchant_id").agg(
        merch_tx_count_1h=bv.count(window="1h"),
    )
```

### Union sources

Combine multiple sources into a single dataset input with `bv.union()`:

```python
@bv.stream
class CardPayments:
    user_id: str
    amount: float
    merchant_id: str

@bv.stream
class BankTransfers:
    user_id: str
    amount: float
    merchant_id: str

@bv.table(key="user_id")
def AllTransactions(src: bv.union(CardPayments, BankTransfers)) -> bv.Table:
    return src.group_by("user_id").agg(
        total_count_1h=bv.count(window="1h"),
    )
```

### Dataset options

| Parameter      | Description                                                        |
|----------------|--------------------------------------------------------------------|
| `depends_on`   | List of sources or datasets this dataset reads from. **Required.** |
| `filter`       | Expression string to filter events before aggregation.             |
| `entity_ttl`   | TTL for entity state after last event.                             |
| `history_ttl`  | TTL for event history retention.                                   |

## Operators

All operators are available as `bv.<name>(...)`. Window durations are strings like
`"30m"`, `"1h"`, `"24h"`, `"7d"`.

### bv.count

Count events in a sliding window.

```python
tx_count_1h = bv.count(window="1h")
failed_count = bv.count(window="30m", where="status == 'failed'")
```

**Parameters:**

| Name     | Type           | Required | Description                                       |
|----------|----------------|----------|---------------------------------------------------|
| `window` | `str`          | Yes      | Window duration (e.g. `"30m"`, `"1h"`, `"24h"`). |
| `where`  | `str \| None`  | No       | Filter expression. Only matching events count.    |
| `bucket` | `str \| None`  | No       | Bucket granularity (e.g. `"1m"`).                 |

### bv.sum

Sum a numeric field in a sliding window.

```python
tx_sum_1h = bv.sum("amount", window="1h")
```

**Parameters:**

| Name       | Type           | Required | Description                                  |
|------------|----------------|----------|----------------------------------------------|
| `field`    | `str`          | Yes      | Event field to sum (positional).             |
| `window`   | `str`          | Yes      | Window duration.                             |
| `optional` | `bool`         | No       | Skip events where field is missing.          |
| `bucket`   | `str \| None`  | No       | Bucket granularity.                          |

### bv.avg

Average a numeric field in a sliding window.

```python
avg_amount = bv.avg("amount", window="24h")
```

**Parameters:** Same as `bv.sum`.

### bv.min

Minimum value in a sliding window (bucketed approximation).

```python
min_amount = bv.min("amount", window="24h")
```

**Parameters:**

| Name     | Type           | Required | Description                    |
|----------|----------------|----------|--------------------------------|
| `field`  | `str`          | Yes      | Event field (positional).      |
| `window` | `str`          | Yes      | Window duration.               |
| `bucket` | `str \| None`  | No       | Bucket granularity.            |

### bv.max

Maximum value in a sliding window (bucketed approximation).

```python
max_amount = bv.max("amount", window="24h")
```

**Parameters:** Same as `bv.min`.

### bv.exact_min

Exact minimum in a sliding window (BTreeMap-based, retractable). More memory than `bv.min`
but always accurate.

```python
exact_min_amount = bv.exact_min("amount", window="24h")
```

**Parameters:** Same as `bv.min`.

### bv.exact_max

Exact maximum in a sliding window (BTreeMap-based, retractable).

```python
exact_max_amount = bv.exact_max("amount", window="24h")
```

**Parameters:** Same as `bv.min`.

### bv.stddev

Standard deviation of a numeric field in a sliding window.

```python
amount_stddev = bv.stddev("amount", window="24h")
```

**Parameters:**

| Name       | Type           | Required | Description                                  |
|------------|----------------|----------|----------------------------------------------|
| `field`    | `str`          | Yes      | Event field (positional).                    |
| `window`   | `str`          | Yes      | Window duration.                             |
| `optional` | `bool`         | No       | Skip events where field is missing.          |
| `where`    | `str \| None`  | No       | Filter expression.                           |
| `bucket`   | `str \| None`  | No       | Bucket granularity.                          |

### bv.percentile

Percentile of a numeric field in a sliding window.

```python
p95_amount = bv.percentile("amount", 0.95, window="24h")
p50_amount = bv.percentile("amount", 0.50, window="1h")
```

**Parameters:**

| Name       | Type           | Required | Description                                     |
|------------|----------------|----------|-------------------------------------------------|
| `field`    | `str`          | Yes      | Event field (positional).                       |
| `quantile` | `float`        | Yes      | Quantile between 0.0 and 1.0 (positional).     |
| `window`   | `str`          | Yes      | Window duration.                                |
| `optional` | `bool`         | No       | Skip events where field is missing.             |
| `where`    | `str \| None`  | No       | Filter expression.                              |
| `bucket`   | `str \| None`  | No       | Bucket granularity.                             |

### bv.count_distinct

Approximate unique count using HyperLogLog. Fixed ~12KB memory per key.

```python
unique_merchants = bv.count_distinct("merchant_id", window="24h")
```

**Parameters:**

| Name     | Type           | Required | Description                    |
|----------|----------------|----------|--------------------------------|
| `field`  | `str`          | Yes      | Event field (positional).      |
| `window` | `str`          | Yes      | Window duration.               |
| `bucket` | `str \| None`  | No       | Bucket granularity.            |

### bv.last

Most recent value of a field. No window -- always tracks the latest value.

```python
last_country = bv.last("country")
last_merchant = bv.last("merchant_id")
```

**Parameters:**

| Name    | Type  | Required | Description                    |
|---------|-------|----------|--------------------------------|
| `field` | `str` | Yes      | Event field (positional).      |

### bv.first

First value ever seen for a field. Once set, never overwrites.

```python
first_country = bv.first("country")
```

**Parameters:**

| Name       | Type   | Required | Description                              |
|------------|--------|----------|------------------------------------------|
| `field`    | `str`  | Yes      | Event field (positional).                |
| `optional` | `bool` | No       | Skip if field missing on first event.    |

### bv.lag

Return the value from N events ago (event-count based, no window).

```python
prev_amount = bv.lag("amount", n=1)
two_ago_amount = bv.lag("amount", n=2)
```

**Parameters:**

| Name       | Type   | Required | Description                              |
|------------|--------|----------|------------------------------------------|
| `field`    | `str`  | Yes      | Event field (positional).                |
| `n`        | `int`  | Yes      | Number of events to lag by.              |
| `optional` | `bool` | No       | Skip events where field is missing.      |

### bv.ema

Exponential moving average with time-based decay. No window -- decays continuously.

```python
ema_amount = bv.ema("amount", half_life="30m")
```

**Parameters:**

| Name        | Type           | Required | Description                                  |
|-------------|----------------|----------|----------------------------------------------|
| `field`     | `str`          | Yes      | Event field (positional).                    |
| `half_life` | `str`          | Yes      | Decay half-life duration (e.g. `"30m"`).     |
| `optional`  | `bool`         | No       | Skip events where field is missing.          |

### bv.last_n

Store the last N values of a field as a JSON array.

```python
recent_amounts = bv.last_n("amount", n=5)
```

**Parameters:**

| Name       | Type   | Required | Description                              |
|------------|--------|----------|------------------------------------------|
| `field`    | `str`  | Yes      | Event field (positional).                |
| `n`        | `int`  | Yes      | Number of recent values to keep.         |
| `optional` | `bool` | No       | Skip events where field is missing.      |

### bv.variance

Variance of a numeric field in a sliding window.

```python
amount_var = bv.variance("amount", window="24h")
```

**Parameters:** Same as `bv.stddev`.

### bv.top_k

Top-K most frequent values of a field in a sliding window (Count-Min Sketch backed).

```python
top_merchants = bv.top_k("merchant_id", k=5, window="24h")
```

**Parameters:**

| Name     | Type           | Required | Description                    |
|----------|----------------|----------|--------------------------------|
| `field`  | `str`          | Yes      | Event field (positional).      |
| `k`      | `int`          | Yes      | Number of top values to return.|
| `window` | `str`          | Yes      | Window duration.               |
| `bucket` | `str \| None`  | No       | Bucket granularity.            |

### bv.first_n

Store the first N values of a field (in order of arrival). Once N values have been
seen, no further updates occur.

```python
first_merchants = bv.first_n("merchant_id", n=3)
```

**Parameters:**

| Name       | Type   | Required | Description                              |
|------------|--------|----------|------------------------------------------|
| `field`    | `str`  | Yes      | Event field (positional).                |
| `n`        | `int`  | Yes      | Number of first values to keep.          |
| `optional` | `bool` | No       | Skip events where field is missing.      |

### bv.derive

Expression computed over other features. Evaluated on read, stores no state.

```python
failure_rate = bv.derive("failed_count_1h / tx_count_1h")
is_suspicious = bv.derive("tx_count_1h > 10 and unique_countries_24h > 3")
normalized = bv.derive("(last_amount - tx_avg_24h) / (tx_stddev_24h + 0.001)")
```

**Parameters:**

| Name   | Type  | Required | Description                    |
|--------|-------|----------|--------------------------------|
| `expr` | `str` | Yes      | Expression string (positional).|

See [Derived Features](#derived-features) for expression syntax details.

### bv.lookup

Cross-key feature reference. Looks up a feature value from a different entity's state.

```python
merchant_risk = bv.lookup("MerchantActivity.chargeback_count_24h", on="merchant_id")
```

**Parameters:**

| Name     | Type  | Required | Description                                       |
|----------|-------|----------|---------------------------------------------------|
| `target` | `str` | Yes      | `"DatasetName.feature_name"` reference (positional). |
| `on`     | `str` | Yes      | Event field to use as the lookup key.             |

### Common optional parameters

Most operators accept these additional keyword arguments:

| Name       | Type   | Default | Description                                          |
|------------|--------|---------|------------------------------------------------------|
| `backfill` | `bool` | `False` | Replay from event log on registration if `True`.     |

## Derived Features

The `bv.derive()` expression language supports:

### Arithmetic

```python
bv.derive("tx_sum_1h / tx_count_1h")
bv.derive("(tx_count_1h / 1) / (tx_count_24h / 24)")
```

Operators: `+`, `-`, `*`, `/`

### Comparison

```python
bv.derive("tx_count_1h > 10")
bv.derive("amount_vs_avg >= 3.0")
```

Operators: `>`, `<`, `>=`, `<=`, `==`, `!=`

### Boolean logic

```python
bv.derive("tx_count_1h > 10 and unique_countries_24h > 3")
bv.derive("is_flagged or velocity_spike > 5")
bv.derive("not is_verified")
```

Operators: `and`, `or`, `not`

### Builtins

```python
bv.derive("abs(amount_deviation)")
bv.derive("min(tx_count_1h, 100)")
bv.derive("max(velocity_spike, 0)")
```

Available: `abs()`, `min()`, `max()`, `now()`

### Field references

- `feature_name` -- reference a feature in the same dataset
- `DatasetName.feature_name` -- reference a feature in another dataset
- `_event.field_name` -- reference a field from the current event

## Filtering

Use `.filter()` on the stream before `.group_by()` to only process events matching a
condition. The filter expression uses `bv.col()` comparisons.

```python
@bv.table(key="user_id")
def FailedTransactions(tra: Transactions) -> bv.Table:
    return tra.filter(bv.col("status") == "failed").group_by("user_id").agg(
        failed_count_30m=bv.count(window="30m"),
        failed_count_1h=bv.count(window="1h"),
        failed_sum_24h=bv.sum("amount", window="24h"),
    )
```

Events where `status != 'failed'` are silently dropped before reaching the operators.

You can also use `where=` on individual operators for per-feature filtering:

```python
features = bv.group_by("user_id").agg(
    total_count=bv.count(window="1h"),
    failed_count=bv.count(window="1h", where="status == 'failed'"),
)
```

## Feature Projection

Control which features appear in responses with `.select()` and `.drop()`.

### .select()

Only include the named features:

```python
@bv.table(key="user_id")
def UserMetrics(tra: Transactions) -> bv.Table:
    return tra.group_by("user_id").agg(
        tx_count_1h=bv.count(window="1h"),
        tx_sum_1h=bv.sum("amount", window="1h"),
        tx_avg_1h=bv.avg("amount", window="1h"),
    )

# Only tx_count_1h and tx_avg_1h will appear in responses
UserMetricsSlim = UserMetrics.select(["tx_count_1h", "tx_avg_1h"])
```

### .drop()

Exclude the named features:

```python
# Everything except tx_sum_1h
UserMetricsLite = UserMetrics.drop(["tx_sum_1h"])
```

Both methods return a new `DatasetDef` -- the original is unchanged.

## Client API

### App(address, timeout=5.0)

Create a client connection to a Beava server.

```python
app = bv.App("localhost:6400")
app = bv.App("10.0.0.5:6400", timeout=10.0)
```

The address format is `"host:port"`. If you omit the port, it defaults to 6400.

The `App` class supports the context manager protocol:

```python
with bv.App("localhost:6400") as app:
    app.register(MyDataset)
    app.push(MySource, {"key": "val"})
    app.flush()
```

### app.register(*classes)

Register pipeline definitions with the server. Accepts any mix of sources and datasets.
When you register a dataset, all of its upstream dependencies (sources, other datasets)
are registered automatically.

```python
# These are equivalent:
app.register(RawTransactions, UserMetrics, MerchantMetrics)
app.register(UserMetrics, MerchantMetrics)  # RawTransactions registered implicitly
```

### app.push(source_class, event_dict)

Push a single event. Fire-and-forget -- returns immediately without waiting for the
server to process the event.

```python
app.push(Transactions, {
    "user_id": "u123",
    "amount": 50.0,
    "merchant_id": "m456",
    "status": "success",
    "country": "US",
})
```

Errors from this push (or any prior async push) surface on the next call to `push`,
`push_sync`, `flush`, `get`, `set`, `mset`, or `register`.

### app.push_many(source_class, events_list)

Push a batch of events in a single wire frame. Significantly lower per-event overhead
compared to individual `push()` calls.

```python
events = [
    {"user_id": "u1", "amount": 10.0, "status": "success"},
    {"user_id": "u2", "amount": 25.0, "status": "failed"},
    {"user_id": "u3", "amount": 99.0, "status": "success"},
]
app.push_many(Transactions, events)
```

Maximum 16,384 events per batch (server hard cap).

### app.push_sync(source_class, event_dict)

Push an event and wait for the response. Returns a `FeatureResult` with the updated
features for the event's entity key.

```python
features = app.push_sync(Transactions, {
    "user_id": "u123",
    "amount": 50.0,
})
print(features.tx_count_1h)
```

Slower than `push()` but gives you the computed features inline.

### app.flush()

Block until all prior fire-and-forget pushes are processed by the server.

```python
app.push(Transactions, event1)
app.push(Transactions, event2)
app.push(Transactions, event3)
app.flush()  # all three events are now processed
```

Call `flush()` before reading features or before program exit to ensure all pending
pushes have been applied.

### app.get(key)

Read all current features for an entity key. Returns a `FeatureResult`.

```python
features = app.get("u123")
print(features.tx_count_1h)
print(features.avg_amount_1h)
print(features.to_dict())  # {"tx_count_1h": 7, "avg_amount_1h": 42.5, ...}
```

Returns an empty `FeatureResult` if the key is unknown.

### app.mget(keys)

Fetch features for multiple keys in a single round trip.

```python
results = app.mget(["u123", "u456", "u789"])
for key, features in results.items():
    print(key, features.tx_count_1h)
```

Returns a `dict[str, FeatureResult]`. Unknown keys map to empty results.

### app.set(key, features_dict)

Directly write feature values for a key. Bypasses the pipeline engine -- useful for
batch features computed offline.

```python
app.set("u123", {"lifetime_value": 4500.0, "segment": "high_value"})
```

### app.mset(entries)

Bulk direct write of feature values for multiple keys.

```python
app.mset({
    "u123": {"lifetime_value": 4500.0, "segment": "high_value"},
    "u456": {"lifetime_value": 1200.0, "segment": "medium_value"},
})
```

### app.close()

Close the TCP connection. Also called automatically when using `App` as a context manager.

```python
app.close()
```

### FeatureResult

The object returned by `get()`, `mget()`, and `push_sync()`. Supports both attribute
access and dictionary-style access.

```python
features = app.get("u123")

# Attribute access
features.tx_count_1h       # 7

# Dictionary access
features["tx_count_1h"]    # 7

# Check if a feature exists
"tx_count_1h" in features  # True

# Convert to plain dict
features.to_dict()         # {"tx_count_1h": 7, ...}
```

Accessing a feature that does not exist raises `AttributeError` (attribute access)
or `KeyError` (dictionary access).

## Fork: Scoped Local Replicas

`bv.fork()` spawns a scoped local replica of a remote Beava server. The replica
receives the production event stream (historical catchup via `LOG_FETCH`, then live
tail via `SUBSCRIBE`) and runs independent local pipelines against it — without
touching production state.

This is the primary workflow for data scientists experimenting with new features
against live production data.

```python
import beava as bv

@bv.stream
class Transactions:
    user_id: str
    amount: float

@bv.table(key="user_id")
def TxnSummary(t: Transactions) -> bv.Table:
    return t.group_by("user_id").agg(
        count_1h=bv.count(window="1h"),
        sum_1h=bv.sum("amount", window="1h"),
    )

with bv.fork(
    remote="prod.beava.dev:6400",
    streams=[Transactions],
    keys=["u1", "u2"],          # optional: scope to specific keys
    token="replica-token",       # or set BEAVA_REPLICA_TOKEN env var
    pipelines=[TxnSummary],      # optional: local pipeline definitions
) as fork:
    # query the local replica
    result = fork.get(TxnSummary, key="u1")
    print(result)  # {"count_1h": 42, "sum_1h": 1234.5}
```

### bv.fork() parameters

| Parameter       | Type                    | Default           | Description |
|-----------------|-------------------------|-------------------|-------------|
| `remote`        | `str`                   | required          | Upstream server as `"HOST:PORT"` |
| `streams`       | `list`                  | required          | `@bv.stream` descriptors to replicate |
| `keys`          | `list[str] \| None`     | `None`            | Exact keys to replicate (mutex with `key_prefix`) |
| `key_prefix`    | `str \| None`           | `None`            | Key prefix filter (mutex with `keys`) |
| `since`         | `str \| int`            | epoch 0           | ISO-8601 UTC or unix-ms start timestamp |
| `token`         | `str \| None`           | `None`            | Replica admin token; falls back to `BEAVA_REPLICA_TOKEN` env |
| `pipelines`     | `list \| None`          | `None`            | `@bv.table` descriptors to register on the fork |
| `extract_at`    | `list \| None`          | `None`            | Timestamps for historical feature extraction snapshots |
| `local_port`    | `int \| None`           | auto-allocated    | HTTP port for the local fork |
| `binary_path`   | `str \| None`           | PATH lookup       | Path to the `beava` binary |
| `ready_timeout` | `float`                 | `30.0`            | Seconds to wait for the fork to become ready |
| `env`           | `dict[str, str] \| None`| `None`            | Extra env vars for the subprocess |

### ForkedReplica methods

| Method | Description |
|--------|-------------|
| `fork.get(pipeline_or_stream, *, key)` | Fetch feature values for a key |
| `fork.inspect(*keys)` | Batch-query raw computed_features for multiple keys |
| `fork.extract_history()` | Retrieve historical extraction snapshots (requires `extract_at=`) |
| `fork.stop()` | Terminate the subprocess and clean up temp files (idempotent) |
| `fork.local_port` | HTTP port the fork is listening on |
| `fork.local_url` | `http://127.0.0.1:{local_port}` |
| `fork.pid` | PID of the fork subprocess |
| `fork.log_path` | Path to the fork's stderr log (for debugging) |

### Fork error types

| Exception | When raised |
|-----------|-------------|
| `bv.ForkError` | Base class for all fork failures |
| `bv.ForkValidationError` | Invalid arguments (raised before subprocess spawn) |
| `bv.ForkTimeoutError` | `/debug/ready` did not return 200 within `ready_timeout` |
| `bv.ForkSubprocessError` | The `beava fork` subprocess exited unexpectedly |

See [docs/architecture.md § Fork Replica Model](architecture.md#fork-replica-model) for
the full design and watermark correctness guarantee.

## Pipeline Validation

Use `bv.validate()` to check pipeline definitions for errors before registering with
the server. Validation runs entirely in Python -- no server connection needed.

```python
from beava import validate, ValidationError

errors = bv.validate(Transactions, UserMetrics, MerchantMetrics)
if errors:
    for e in errors:
        print(f"[{e.kind}] {e.path}: {e.message}")
```

### What it checks

- **Cycles:** Circular dependencies in the dataset graph.
- **Missing dependencies:** A dataset depends on a source or dataset not in the provided definitions.
- **Type mismatches:** An operator references a field name not found in the upstream `@bv.stream` class annotations.

### ValidationError

Each error has three attributes:

| Attribute | Type  | Description                                              |
|-----------|-------|----------------------------------------------------------|
| `path`    | `str` | Dot-separated location (e.g. `"UserMetrics.amount_sum"`). |
| `message` | `str` | Human-readable description of the issue.                 |
| `kind`    | `str` | One of `"cycle"`, `"missing_dep"`, `"type_mismatch"`.    |

### Example: catching a field mismatch

```python
import beava as bv

@bv.stream
class Transactions:
    user_id: str
    amount: float
    merchant_id: str
    status: str

@bv.table(key="user_id")
def UserMetrics(tra: Transactions) -> bv.Table:
    return tra.group_by("user_id").agg(
        total=bv.sum("price", window="1h"),  # "price" not in Transactions schema
    )

errors = bv.validate(Transactions, UserMetrics)
# [ValidationError(kind='type_mismatch',
#   path='UserMetrics.total',
#   message="operator references field 'price' not found in upstream stream schema ...")]
```

## Error Handling

All SDK exceptions inherit from `BeavaError`.

```python
from beava import BeavaError, ConnectionError, ProtocolError
```

| Exception         | When it is raised                                          |
|-------------------|------------------------------------------------------------|
| `BeavaError`      | Base class for all Beava SDK errors.                       |
| `ConnectionError` | TCP connection to the server failed or was lost.           |
| `ProtocolError`   | Protocol-level error: bad frame, server returned an error. |

### Example

```python
import beava as bv
from beava import ConnectionError, ProtocolError

try:
    app = bv.App("localhost:6400")
    app.register(MyDataset)
except ConnectionError as e:
    print(f"Cannot reach server: {e}")
except ProtocolError as e:
    print(f"Server rejected registration: {e}")
```

Errors from fire-and-forget `push()` calls are deferred. They surface on the next
call to any `App` method (`push`, `flush`, `get`, `set`, etc.). Always call `flush()`
before reading features to ensure errors from prior pushes are raised.

## Real-World Example: Fraud Detection Pipeline

This example models a mid-size fintech with 5 entity types and 47 features across
multiple window tiers. Adapted from `benchmark/fraud-pipeline/bench.py`.

### Pipeline definition

```python
import beava as bv

# --- Event source ---

@bv.stream
class RawTransactions:
    """Raw payment events with user_id, merchant_id, device_id, ip_address."""
    user_id: str
    amount: float
    merchant_id: str

# --- Entity 1: User transaction behavior (25 features) ---

@bv.table(key="user_id")
def UserTransactions(raw: RawTransactions) -> bv.Table:
    return raw.group_by("user_id").agg(
        # Volume across window tiers
        tx_count_30m=bv.count(window="30m"),
        tx_count_1h=bv.count(window="1h"),
        tx_count_24h=bv.count(window="24h"),
        tx_count_7d=bv.count(window="7d"),
        # Amount aggregations
        tx_sum_1h=bv.sum("amount", window="1h"),
        tx_sum_24h=bv.sum("amount", window="24h"),
        tx_avg_1h=bv.avg("amount", window="1h"),
        tx_avg_24h=bv.avg("amount", window="24h"),
        tx_max_24h=bv.max("amount", window="24h"),
        tx_min_24h=bv.min("amount", window="24h"),
        tx_stddev_24h=bv.stddev("amount", window="24h"),
        # Cardinality
        unique_merchants_1h=bv.count_distinct("merchant_id", window="1h"),
        unique_merchants_24h=bv.count_distinct("merchant_id", window="24h"),
        unique_countries_24h=bv.count_distinct("country", window="24h"),
        unique_devices_24h=bv.count_distinct("device_id", window="24h"),
        unique_ips_24h=bv.count_distinct("ip_address", window="24h"),
        # Context
        last_country=bv.last("country"),
        last_merchant=bv.last("merchant_id"),
        last_amount=bv.last("amount"),
    ).with_columns(
        # Derived signals
        velocity_spike=bv.derive("(tx_count_1h / 1) / (tx_count_24h / 24)"),
        amount_vs_avg=bv.derive("last_amount / tx_avg_24h"),
        spend_acceleration=bv.derive("tx_sum_1h / (tx_sum_24h / 24)"),
        high_value_ratio=bv.derive("tx_max_24h / tx_avg_24h"),
        merchant_diversity_1h=bv.derive("unique_merchants_1h / tx_count_1h"),
        country_hop_flag=bv.derive("unique_countries_24h > 3"),
    )

# --- Entity 2: Failed transactions (4 features) ---

@bv.table(key="user_id")
def UserFailedTxns(raw: RawTransactions) -> bv.Table:
    return raw.filter(bv.col("status") == "failed").group_by("user_id").agg(
        failed_count_30m=bv.count(window="30m"),
        failed_count_1h=bv.count(window="1h"),
        failed_count_24h=bv.count(window="24h"),
        failed_sum_24h=bv.sum("amount", window="24h"),
    )

# --- Entity 3: Merchant risk profile (8 features) ---

@bv.table(key="merchant_id")
def MerchantActivity(raw: RawTransactions) -> bv.Table:
    return raw.group_by("merchant_id").agg(
        merch_tx_count_1h=bv.count(window="1h"),
        merch_tx_count_24h=bv.count(window="24h"),
        merch_tx_sum_24h=bv.sum("amount", window="24h"),
        merch_avg_amount=bv.avg("amount", window="24h"),
        merch_unique_users_1h=bv.count_distinct("user_id", window="1h"),
        merch_unique_users_24h=bv.count_distinct("user_id", window="24h"),
        merch_max_amount_24h=bv.max("amount", window="24h"),
        merch_stddev_24h=bv.stddev("amount", window="24h"),
    )

# --- Entity 4: Device fingerprint (5 features) ---

@bv.table(key="device_id")
def DeviceActivity(raw: RawTransactions) -> bv.Table:
    return raw.group_by("device_id").agg(
        device_tx_count_1h=bv.count(window="1h"),
        device_tx_count_24h=bv.count(window="24h"),
        device_unique_users_1h=bv.count_distinct("user_id", window="1h"),
        device_unique_users_24h=bv.count_distinct("user_id", window="24h"),
        device_unique_merchants_24h=bv.count_distinct("merchant_id", window="24h"),
    )

# --- Entity 5: IP address activity (5 features) ---

@bv.table(key="ip_address")
def IPActivity(raw: RawTransactions) -> bv.Table:
    return raw.group_by("ip_address").agg(
        ip_tx_count_1h=bv.count(window="1h"),
        ip_tx_count_24h=bv.count(window="24h"),
        ip_unique_users_1h=bv.count_distinct("user_id", window="1h"),
        ip_unique_users_24h=bv.count_distinct("user_id", window="24h"),
        ip_unique_devices_24h=bv.count_distinct("device_id", window="24h"),
    )
```

### Running the pipeline

```python
ALL_DATASETS = [
    RawTransactions,
    UserTransactions,
    UserFailedTxns,
    MerchantActivity,
    DeviceActivity,
    IPActivity,
]

app = bv.App("localhost:6400")
app.register(*ALL_DATASETS)

# Push events -- a single event fans out to all datasets that share its keys
app.push(RawTransactions, {
    "user_id": "user_000123",
    "merchant_id": "merch_000456",
    "device_id": "dev_000789",
    "ip_address": "ip_001234",
    "amount": 49.99,
    "country": "US",
    "status": "success",
    "currency": "USD",
})
app.flush()

# Read features for a user
features = app.get("user_000123")
print(features.tx_count_1h)          # 1
print(features.velocity_spike)       # computed from derive expression
print(features.unique_merchants_1h)  # 1

# Batch push for throughput
events = [generate_event() for _ in range(1000)]
app.push_many(RawTransactions, events)
app.flush()
```

### Key patterns demonstrated

1. **Multi-entity fan-out:** One event updates `UserTransactions` (keyed by `user_id`),
   `MerchantActivity` (keyed by `merchant_id`), `DeviceActivity` (keyed by `device_id`),
   and `IPActivity` (keyed by `ip_address`).

2. **Filtered datasets:** `UserFailedTxns` only sees events where `status == 'failed'`.

3. **Multi-window tiers:** The same entity tracks 30m, 1h, 24h, and 7d windows
   simultaneously.

4. **Derived signals:** Velocity spikes, spend acceleration, and anomaly flags are
   computed from base aggregations with zero additional state.

5. **Batch throughput:** `push_many()` sends up to 16,384 events per wire frame for
   high-throughput ingestion.
