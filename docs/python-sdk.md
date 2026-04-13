# Tally Python SDK

The Tally Python SDK is a thin client for the Tally real-time feature server. You define
pipelines in Python, register them with the server, push events, and read computed features.
Python never touches the hot path -- all computation happens server-side in Rust.

## Installation

Requires Python 3.10+.

Install from source:

```bash
cd python
pip install -e .
```

There are no external dependencies. The SDK uses only the Python standard library.

## Quick Start

```python
import tally as tl

# 1. Declare an event source
@tl.source
class Transactions:
    pass

# 2. Define a dataset with features
@tl.dataset(depends_on=[Transactions])
class UserFeatures:
    features = tl.group_by("user_id").agg(
        tx_count_1h=tl.count(window="1h"),
        tx_sum_1h=tl.sum("amount", window="1h"),
        avg_amount_1h=tl.avg("amount", window="1h"),
    )
    velocity = tl.derive("tx_count_1h / (tx_sum_1h + 1)")

# 3. Connect and register
app = tl.App("localhost:6400")
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

Use the `@tl.source` decorator:

```python
@tl.source
class Transactions:
    pass
```

### Source options

You can pass optional parameters to control TTL behavior:

```python
@tl.source(entity_ttl="5m", history_ttl="72h")
class Transactions:
    pass
```

| Parameter      | Description                                              |
|----------------|----------------------------------------------------------|
| `entity_ttl`   | How long to keep entity state after the last event.      |
| `history_ttl`  | How long to retain event history for backfill/replay.    |

### Typed event schemas

For pipeline validation, you can attach an `EventSet` schema to a source:

```python
from tally import EventSet, Field

class TxnEvent(EventSet):
    user_id: str = Field()
    amount: float = Field()
    merchant_id: str = Field()
    status: str = Field()

@tl.source
class Transactions:
    event = TxnEvent
```

This enables `tl.validate()` to check that operator field references match the event schema
before you register with the server.

## Defining Datasets

A **dataset** is a keyed aggregation pipeline. It depends on one or more sources (or other
datasets), groups events by a key field, and computes features using operators.

```python
@tl.dataset(depends_on=[Transactions])
class UserMetrics:
    features = tl.group_by("user_id").agg(
        tx_count_1h=tl.count(window="1h"),
        tx_sum_1h=tl.sum("amount", window="1h"),
    )
```

### The `group_by().agg()` pattern

Every dataset declares a `features` attribute using `tl.group_by("key_field").agg(...)`.
This tells the server which event field to use as the entity key and which aggregations
to maintain.

```python
features = tl.group_by("user_id").agg(
    feature_name=tl.operator(...),
    another_feature=tl.operator(...),
)
```

### Derived features

Add derived features as class attributes alongside `features`:

```python
@tl.dataset(depends_on=[Transactions])
class UserMetrics:
    features = tl.group_by("user_id").agg(
        tx_count_1h=tl.count(window="1h"),
        tx_count_24h=tl.count(window="24h"),
    )
    velocity_spike = tl.derive("(tx_count_1h / 1) / (tx_count_24h / 24)")
```

### Cascading datasets

A dataset can depend on another dataset, enabling multi-stage pipelines:

```python
@tl.source
class RawEvents:
    pass

@tl.dataset(depends_on=[RawEvents])
class UserTxns:
    features = tl.group_by("user_id").agg(
        tx_count_1h=tl.count(window="1h"),
    )

@tl.dataset(depends_on=[RawEvents])
class MerchantTxns:
    features = tl.group_by("merchant_id").agg(
        merch_tx_count_1h=tl.count(window="1h"),
    )
```

### Union sources

Combine multiple sources into a single dataset input with `tl.union()`:

```python
@tl.source
class CardPayments:
    pass

@tl.source
class BankTransfers:
    pass

@tl.dataset(depends_on=[tl.union(CardPayments, BankTransfers)])
class AllTransactions:
    features = tl.group_by("user_id").agg(
        total_count_1h=tl.count(window="1h"),
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

All operators are available as `tl.<name>(...)`. Window durations are strings like
`"30m"`, `"1h"`, `"24h"`, `"7d"`.

### tl.count

Count events in a sliding window.

```python
tx_count_1h = tl.count(window="1h")
failed_count = tl.count(window="30m", where="status == 'failed'")
```

**Parameters:**

| Name     | Type           | Required | Description                                       |
|----------|----------------|----------|---------------------------------------------------|
| `window` | `str`          | Yes      | Window duration (e.g. `"30m"`, `"1h"`, `"24h"`). |
| `where`  | `str \| None`  | No       | Filter expression. Only matching events count.    |
| `bucket` | `str \| None`  | No       | Bucket granularity (e.g. `"1m"`).                 |

### tl.sum

Sum a numeric field in a sliding window.

```python
tx_sum_1h = tl.sum("amount", window="1h")
```

**Parameters:**

| Name       | Type           | Required | Description                                  |
|------------|----------------|----------|----------------------------------------------|
| `field`    | `str`          | Yes      | Event field to sum (positional).             |
| `window`   | `str`          | Yes      | Window duration.                             |
| `optional` | `bool`         | No       | Skip events where field is missing.          |
| `bucket`   | `str \| None`  | No       | Bucket granularity.                          |

### tl.avg

Average a numeric field in a sliding window.

```python
avg_amount = tl.avg("amount", window="24h")
```

**Parameters:** Same as `tl.sum`.

### tl.min

Minimum value in a sliding window (bucketed approximation).

```python
min_amount = tl.min("amount", window="24h")
```

**Parameters:**

| Name     | Type           | Required | Description                    |
|----------|----------------|----------|--------------------------------|
| `field`  | `str`          | Yes      | Event field (positional).      |
| `window` | `str`          | Yes      | Window duration.               |
| `bucket` | `str \| None`  | No       | Bucket granularity.            |

### tl.max

Maximum value in a sliding window (bucketed approximation).

```python
max_amount = tl.max("amount", window="24h")
```

**Parameters:** Same as `tl.min`.

### tl.exact_min

Exact minimum in a sliding window (BTreeMap-based, retractable). More memory than `tl.min`
but always accurate.

```python
exact_min_amount = tl.exact_min("amount", window="24h")
```

**Parameters:** Same as `tl.min`.

### tl.exact_max

Exact maximum in a sliding window (BTreeMap-based, retractable).

```python
exact_max_amount = tl.exact_max("amount", window="24h")
```

**Parameters:** Same as `tl.min`.

### tl.stddev

Standard deviation of a numeric field in a sliding window.

```python
amount_stddev = tl.stddev("amount", window="24h")
```

**Parameters:**

| Name       | Type           | Required | Description                                  |
|------------|----------------|----------|----------------------------------------------|
| `field`    | `str`          | Yes      | Event field (positional).                    |
| `window`   | `str`          | Yes      | Window duration.                             |
| `optional` | `bool`         | No       | Skip events where field is missing.          |
| `where`    | `str \| None`  | No       | Filter expression.                           |
| `bucket`   | `str \| None`  | No       | Bucket granularity.                          |

### tl.percentile

Percentile of a numeric field in a sliding window.

```python
p95_amount = tl.percentile("amount", 0.95, window="24h")
p50_amount = tl.percentile("amount", 0.50, window="1h")
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

### tl.distinct_count

Approximate unique count using HyperLogLog. Fixed ~12KB memory per key.

```python
unique_merchants = tl.distinct_count("merchant_id", window="24h")
```

**Parameters:**

| Name     | Type           | Required | Description                    |
|----------|----------------|----------|--------------------------------|
| `field`  | `str`          | Yes      | Event field (positional).      |
| `window` | `str`          | Yes      | Window duration.               |
| `bucket` | `str \| None`  | No       | Bucket granularity.            |

### tl.last

Most recent value of a field. No window -- always tracks the latest value.

```python
last_country = tl.last("country")
last_merchant = tl.last("merchant_id")
```

**Parameters:**

| Name    | Type  | Required | Description                    |
|---------|-------|----------|--------------------------------|
| `field` | `str` | Yes      | Event field (positional).      |

### tl.first

First value ever seen for a field. Once set, never overwrites.

```python
first_country = tl.first("country")
```

**Parameters:**

| Name       | Type   | Required | Description                              |
|------------|--------|----------|------------------------------------------|
| `field`    | `str`  | Yes      | Event field (positional).                |
| `optional` | `bool` | No       | Skip if field missing on first event.    |

### tl.lag

Return the value from N events ago (event-count based, no window).

```python
prev_amount = tl.lag("amount", n=1)
two_ago_amount = tl.lag("amount", n=2)
```

**Parameters:**

| Name       | Type   | Required | Description                              |
|------------|--------|----------|------------------------------------------|
| `field`    | `str`  | Yes      | Event field (positional).                |
| `n`        | `int`  | Yes      | Number of events to lag by.              |
| `optional` | `bool` | No       | Skip events where field is missing.      |

### tl.ema

Exponential moving average with time-based decay. No window -- decays continuously.

```python
ema_amount = tl.ema("amount", half_life="30m")
```

**Parameters:**

| Name        | Type           | Required | Description                                  |
|-------------|----------------|----------|----------------------------------------------|
| `field`     | `str`          | Yes      | Event field (positional).                    |
| `half_life` | `str`          | Yes      | Decay half-life duration (e.g. `"30m"`).     |
| `optional`  | `bool`         | No       | Skip events where field is missing.          |

### tl.last_n

Store the last N values of a field as a JSON array.

```python
recent_amounts = tl.last_n("amount", n=5)
```

**Parameters:**

| Name       | Type   | Required | Description                              |
|------------|--------|----------|------------------------------------------|
| `field`    | `str`  | Yes      | Event field (positional).                |
| `n`        | `int`  | Yes      | Number of recent values to keep.         |
| `optional` | `bool` | No       | Skip events where field is missing.      |

### tl.derive

Expression computed over other features. Evaluated on read, stores no state.

```python
failure_rate = tl.derive("failed_count_1h / tx_count_1h")
is_suspicious = tl.derive("tx_count_1h > 10 and unique_countries_24h > 3")
```

**Parameters:**

| Name   | Type  | Required | Description                    |
|--------|-------|----------|--------------------------------|
| `expr` | `str` | Yes      | Expression string (positional).|

See [Derived Features](#derived-features-1) for expression syntax details.

### tl.lookup

Cross-key feature reference. Looks up a feature value from a different entity's state.

```python
merchant_risk = tl.lookup("MerchantActivity.chargeback_count_24h", on="merchant_id")
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

The `tl.derive()` expression language supports:

### Arithmetic

```python
tl.derive("tx_sum_1h / tx_count_1h")
tl.derive("(tx_count_1h / 1) / (tx_count_24h / 24)")
```

Operators: `+`, `-`, `*`, `/`

### Comparison

```python
tl.derive("tx_count_1h > 10")
tl.derive("amount_vs_avg >= 3.0")
```

Operators: `>`, `<`, `>=`, `<=`, `==`, `!=`

### Boolean logic

```python
tl.derive("tx_count_1h > 10 and unique_countries_24h > 3")
tl.derive("is_flagged or velocity_spike > 5")
tl.derive("not is_verified")
```

Operators: `and`, `or`, `not`

### Builtins

```python
tl.derive("abs(amount_deviation)")
tl.derive("min(tx_count_1h, 100)")
tl.derive("max(velocity_spike, 0)")
```

Available: `abs()`, `min()`, `max()`, `now()`

### Field references

- `feature_name` -- reference a feature in the same dataset
- `DatasetName.feature_name` -- reference a feature in another dataset
- `_event.field_name` -- reference a field from the current event

## Filtering

Use the `filter=` parameter on `@tl.dataset` to only process events matching a condition.
The filter expression uses the same syntax as `tl.derive()`.

```python
@tl.dataset(depends_on=[Transactions], filter="status == 'failed'")
class FailedTransactions:
    features = tl.group_by("user_id").agg(
        failed_count_30m=tl.count(window="30m"),
        failed_count_1h=tl.count(window="1h"),
        failed_sum_24h=tl.sum("amount", window="24h"),
    )
```

Events where `status != 'failed'` are silently dropped before reaching the operators.

You can also use `where=` on individual operators for per-feature filtering:

```python
features = tl.group_by("user_id").agg(
    total_count=tl.count(window="1h"),
    failed_count=tl.count(window="1h", where="status == 'failed'"),
)
```

## Feature Projection

Control which features appear in responses with `.select()` and `.drop()`.

### .select()

Only include the named features:

```python
@tl.dataset(depends_on=[Transactions])
class UserMetrics:
    features = tl.group_by("user_id").agg(
        tx_count_1h=tl.count(window="1h"),
        tx_sum_1h=tl.sum("amount", window="1h"),
        tx_avg_1h=tl.avg("amount", window="1h"),
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

Create a client connection to a Tally server.

```python
app = tl.App("localhost:6400")
app = tl.App("10.0.0.5:6400", timeout=10.0)
```

The address format is `"host:port"`. If you omit the port, it defaults to 6400.

The `App` class supports the context manager protocol:

```python
with tl.App("localhost:6400") as app:
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

## Pipeline Validation

Use `tl.validate()` to check pipeline definitions for errors before registering with
the server. Validation runs entirely in Python -- no server connection needed.

```python
from tally import validate, ValidationError

errors = tl.validate(Transactions, UserMetrics, MerchantMetrics)
if errors:
    for e in errors:
        print(f"[{e.kind}] {e.path}: {e.message}")
```

### What it checks

- **Cycles:** Circular dependencies in the dataset graph.
- **Missing dependencies:** A dataset depends on a source or dataset not in the provided definitions.
- **Type mismatches:** An operator references a field name not found in the upstream `EventSet` schema.

### ValidationError

Each error has three attributes:

| Attribute | Type  | Description                                              |
|-----------|-------|----------------------------------------------------------|
| `path`    | `str` | Dot-separated location (e.g. `"UserMetrics.amount_sum"`). |
| `message` | `str` | Human-readable description of the issue.                 |
| `kind`    | `str` | One of `"cycle"`, `"missing_dep"`, `"type_mismatch"`.    |

### Example: catching a field mismatch

```python
class TxnEvent(EventSet):
    user_id: str = Field()
    amount: float = Field()

@tl.source
class Transactions:
    event = TxnEvent

@tl.dataset(depends_on=[Transactions])
class UserMetrics:
    features = tl.group_by("user_id").agg(
        total=tl.sum("price", window="1h"),  # "price" not in TxnEvent
    )

errors = tl.validate(Transactions, UserMetrics)
# [ValidationError(kind='type_mismatch',
#   path='UserMetrics.total',
#   message="operator references field 'price' not found in upstream EventSet ...")]
```

## Error Handling

All SDK exceptions inherit from `TallyError`.

```python
from tally import TallyError, ConnectionError, ProtocolError
```

| Exception         | When it is raised                                          |
|-------------------|------------------------------------------------------------|
| `TallyError`      | Base class for all Tally SDK errors.                       |
| `ConnectionError` | TCP connection to the server failed or was lost.           |
| `ProtocolError`   | Protocol-level error: bad frame, server returned an error. |

### Example

```python
import tally as tl
from tally import ConnectionError, ProtocolError

try:
    app = tl.App("localhost:6400")
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
multiple window tiers. Adapted from `benchmark/fraud-pipeline/bench_fraud.py`.

### Pipeline definition

```python
import tally as tl

# --- Event source ---

@tl.source
class RawTransactions:
    """Raw payment events with user_id, merchant_id, device_id, ip_address."""
    pass

# --- Entity 1: User transaction behavior (25 features) ---

@tl.dataset(depends_on=[RawTransactions])
class UserTransactions:
    features = tl.group_by("user_id").agg(
        # Volume across window tiers
        tx_count_30m=tl.count(window="30m"),
        tx_count_1h=tl.count(window="1h"),
        tx_count_24h=tl.count(window="24h"),
        tx_count_7d=tl.count(window="7d"),
        # Amount aggregations
        tx_sum_1h=tl.sum("amount", window="1h"),
        tx_sum_24h=tl.sum("amount", window="24h"),
        tx_avg_1h=tl.avg("amount", window="1h"),
        tx_avg_24h=tl.avg("amount", window="24h"),
        tx_max_24h=tl.max("amount", window="24h"),
        tx_min_24h=tl.min("amount", window="24h"),
        tx_stddev_24h=tl.stddev("amount", window="24h"),
        # Cardinality
        unique_merchants_1h=tl.distinct_count("merchant_id", window="1h"),
        unique_merchants_24h=tl.distinct_count("merchant_id", window="24h"),
        unique_countries_24h=tl.distinct_count("country", window="24h"),
        unique_devices_24h=tl.distinct_count("device_id", window="24h"),
        unique_ips_24h=tl.distinct_count("ip_address", window="24h"),
        # Context
        last_country=tl.last("country"),
        last_merchant=tl.last("merchant_id"),
        last_amount=tl.last("amount"),
    )
    # Derived signals
    velocity_spike = tl.derive("(tx_count_1h / 1) / (tx_count_24h / 24)")
    amount_vs_avg = tl.derive("last_amount / tx_avg_24h")
    spend_acceleration = tl.derive("tx_sum_1h / (tx_sum_24h / 24)")
    high_value_ratio = tl.derive("tx_max_24h / tx_avg_24h")
    merchant_diversity_1h = tl.derive("unique_merchants_1h / tx_count_1h")
    country_hop_flag = tl.derive("unique_countries_24h > 3")

# --- Entity 2: Failed transactions (4 features) ---

@tl.dataset(depends_on=[RawTransactions], filter="status == 'failed'")
class UserFailedTxns:
    features = tl.group_by("user_id").agg(
        failed_count_30m=tl.count(window="30m"),
        failed_count_1h=tl.count(window="1h"),
        failed_count_24h=tl.count(window="24h"),
        failed_sum_24h=tl.sum("amount", window="24h"),
    )

# --- Entity 3: Merchant risk profile (8 features) ---

@tl.dataset(depends_on=[RawTransactions])
class MerchantActivity:
    features = tl.group_by("merchant_id").agg(
        merch_tx_count_1h=tl.count(window="1h"),
        merch_tx_count_24h=tl.count(window="24h"),
        merch_tx_sum_24h=tl.sum("amount", window="24h"),
        merch_avg_amount=tl.avg("amount", window="24h"),
        merch_unique_users_1h=tl.distinct_count("user_id", window="1h"),
        merch_unique_users_24h=tl.distinct_count("user_id", window="24h"),
        merch_max_amount_24h=tl.max("amount", window="24h"),
        merch_stddev_24h=tl.stddev("amount", window="24h"),
    )

# --- Entity 4: Device fingerprint (5 features) ---

@tl.dataset(depends_on=[RawTransactions])
class DeviceActivity:
    features = tl.group_by("device_id").agg(
        device_tx_count_1h=tl.count(window="1h"),
        device_tx_count_24h=tl.count(window="24h"),
        device_unique_users_1h=tl.distinct_count("user_id", window="1h"),
        device_unique_users_24h=tl.distinct_count("user_id", window="24h"),
        device_unique_merchants_24h=tl.distinct_count("merchant_id", window="24h"),
    )

# --- Entity 5: IP address activity (5 features) ---

@tl.dataset(depends_on=[RawTransactions])
class IPActivity:
    features = tl.group_by("ip_address").agg(
        ip_tx_count_1h=tl.count(window="1h"),
        ip_tx_count_24h=tl.count(window="24h"),
        ip_unique_users_1h=tl.distinct_count("user_id", window="1h"),
        ip_unique_users_24h=tl.distinct_count("user_id", window="24h"),
        ip_unique_devices_24h=tl.distinct_count("device_id", window="24h"),
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

app = tl.App("localhost:6400")
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
