# Beava Operator Reference

Complete reference for all 16 operators in the Beava real-time feature server.

---

## Table of Contents

- [Windowed Aggregation Operators](#windowed-aggregation-operators)
  - [count](#count)
  - [sum](#sum)
  - [avg](#avg)
  - [min](#min)
  - [max](#max)
  - [stddev](#stddev)
  - [percentile](#percentile)
  - [distinct_count](#distinct_count)
  - [exact_min](#exact_min)
  - [exact_max](#exact_max)
- [Value Operators](#value-operators)
  - [last](#last)
  - [first](#first)
  - [lag](#lag)
  - [last_n](#last_n)
  - [ema](#ema)
- [Computed Operators](#computed-operators)
  - [derive](#derive)
- [Where Clauses](#where-clauses)
- [Window Mechanics](#window-mechanics)
- [Cross-Stream References](#cross-stream-references)

---

## Windowed Aggregation Operators

### count

Count events in a sliding window.

**Python constructor:**

```python
bv.count(window="1h")
bv.count(window="30m")  # v0: use .filter(bv.col(...) == ...) on the stream instead of where=
bv.count(window="24h", bucket="15m")
```

**Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `window` | `str` | Yes | Duration string (e.g. `"30m"`, `"1h"`, `"24h"`). |
| `where` | `str` | No | Filter expression. Only events matching the condition are counted. |
| `bucket` | `str` | No | Bucket granularity (e.g. `"1m"`). Defaults to `window / 30` if omitted. |
| `backfill` | `bool` | No | If `True`, replay from event log on registration. Default `False`. |

**Description:**

Counts all events that arrive within the window. Does not require any specific field on the event -- every event that passes the optional `where` filter increments the counter by 1.

Returns an integer. Returns `Missing` if zero events exist in the window.

**Window behavior:**

Uses a `RingBuffer<u64>` with `num_buckets = ceil(window / bucket)`. Each bucket holds a partial count. On push, the current bucket is incremented by 1. On read, all non-expired buckets are summed.

**Memory per key:**

`num_buckets * 8 bytes`. For a 1h window with 1m buckets: `60 * 8 = 480 bytes`.

**Example:**

```python
import beava as bv

@bv.stream
class RawTransactions:
    user_id: str
    merchant_id: str
    amount: float
    status: str
    country: str

@bv.table(key="user_id")
def UserTransactions(raw: RawTransactions) -> bv.Table:
    return raw.group_by("user_id").agg(
        tx_count_30m=bv.count(window="30m"),
        tx_count_1h=bv.count(window="1h"),
        tx_count_24h=bv.count(window="24h"),
        failed_30m=bv.count(window="30m")  # v0: use .filter(bv.col(...) == ...) on the stream instead of where=
    )
```

---

### sum

Sum a numeric field in a sliding window.

**Python constructor:**

```python
bv.sum("amount", window="1h")
bv.sum("amount", window="24h", optional=True)
```

**Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `field` | `str` | Yes | Name of the numeric event field to sum (positional). |
| `window` | `str` | Yes | Duration string. |
| `optional` | `bool` | No | If `True`, events missing the field are silently skipped. Default `False` (missing field raises a type error). |
| `bucket` | `str` | No | Bucket granularity. |
| `backfill` | `bool` | No | Replay from event log on registration. |

**Description:**

Sums numeric values of the specified field across all events in the window. Accepts both integer and floating-point values. Non-numeric values raise a type error.

Returns a float. Returns `Missing` if zero events exist in the window. A sum of `0.0` from actual events is returned as `Float(0.0)`, not `Missing`.

**Window behavior:**

Uses two parallel ring buffers: `RingBuffer<f64>` for the running sum and `RingBuffer<u64>` for event count. The event count buffer distinguishes "no events" from "sum is zero" -- without it, a window full of zero-valued events would incorrectly return `Missing`.

**Memory per key:**

`num_buckets * 16 bytes` (8 bytes for sum + 8 bytes for count per bucket). For a 1h window with 1m buckets: `60 * 16 = 960 bytes`.

**Example:**

```python
@bv.stream
class RawTransactions:
    user_id: str
    merchant_id: str
    amount: float
    status: str
    country: str

@bv.table(key="user_id")
def UserTransactions(raw: RawTransactions) -> bv.Table:
    return raw.group_by("user_id").agg(
        tx_sum_1h=bv.sum("amount", window="1h"),
        tx_sum_24h=bv.sum("amount", window="24h"),
    )
```

---

### avg

Average a numeric field in a sliding window.

**Python constructor:**

```python
bv.avg("amount", window="1h")
bv.avg("amount", window="24h", optional=True)
```

**Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `field` | `str` | Yes | Name of the numeric event field (positional). |
| `window` | `str` | Yes | Duration string. |
| `optional` | `bool` | No | If `True`, skip events missing the field. Default `False`. |
| `bucket` | `str` | No | Bucket granularity. |
| `backfill` | `bool` | No | Replay from event log on registration. |

**Description:**

Computes the arithmetic mean of a numeric field across all events in the window. Internally maintains paired count and sum buffers; divides on read.

Returns a float. Returns `Missing` if zero events exist in the window (never returns NaN).

**Window behavior:**

Uses two parallel ring buffers: `RingBuffer<u64>` for count and `RingBuffer<f64>` for sum. On read, both are summed across all buckets, then `avg = sum / count`.

**Memory per key:**

`num_buckets * 16 bytes` (8 bytes count + 8 bytes sum per bucket). For a 1h window with 1m buckets: `60 * 16 = 960 bytes`.

**Example:**

```python
@bv.stream
class RawTransactions:
    user_id: str
    merchant_id: str
    amount: float
    status: str
    country: str

@bv.table(key="user_id")
def UserTransactions(raw: RawTransactions) -> bv.Table:
    return raw.group_by("user_id").agg(
        avg_amount_1h=bv.avg("amount", window="1h"),
        avg_amount_24h=bv.avg("amount", window="24h"),
    )
```

---

### min

Minimum value of a numeric field in a sliding window (bucketed approximation).

**Python constructor:**

```python
bv.min("amount", window="1h")
bv.min("amount", window="24h", bucket="15m")
```

**Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `field` | `str` | Yes | Name of the numeric event field (positional). |
| `window` | `str` | Yes | Duration string. |
| `bucket` | `str` | No | Bucket granularity. |
| `backfill` | `bool` | No | Replay from event log on registration. |

**Description:**

Tracks the minimum value of a field within the window. Each bucket stores the per-bucket minimum. On read, the global minimum is computed across all non-expired buckets.

This is a **bucketed approximation**: within a single bucket, only the minimum is retained. If the true global minimum expires when its bucket rolls off, the reported minimum jumps to the smallest per-bucket minimum among remaining buckets. For exact results, use `exact_min`.

Returns a float. Returns `Missing` if zero events exist in the window.

**Window behavior:**

Uses `RingBuffer<MinBucket>` where each bucket defaults to `+INFINITY`. On push, the bucket value is conditionally replaced if the new value is smaller. On read, the minimum across all buckets whose value is not `+INFINITY` is returned. A parallel `RingBuffer<u64>` tracks event counts.

**Memory per key:**

`num_buckets * 16 bytes` (8 bytes MinBucket + 8 bytes count per bucket). For a 1h window with 1m buckets: `60 * 16 = 960 bytes`.

**Example:**

```python
@bv.stream
class RawTransactions:
    user_id: str
    merchant_id: str
    amount: float
    status: str
    country: str

@bv.table(key="user_id")
def UserTransactions(raw: RawTransactions) -> bv.Table:
    return raw.group_by("user_id").agg(
        min_amount_24h=bv.min("amount", window="24h"),
    )
```

---

### max

Maximum value of a numeric field in a sliding window (bucketed approximation).

**Python constructor:**

```python
bv.max("amount", window="1h")
bv.max("amount", window="24h", bucket="15m")
```

**Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `field` | `str` | Yes | Name of the numeric event field (positional). |
| `window` | `str` | Yes | Duration string. |
| `bucket` | `str` | No | Bucket granularity. |
| `backfill` | `bool` | No | Replay from event log on registration. |

**Description:**

Tracks the maximum value of a field within the window. Mirrors `min` with per-bucket maximum tracking.

This is a **bucketed approximation**. For exact results, use `exact_max`.

Returns a float. Returns `Missing` if zero events exist in the window.

**Window behavior:**

Uses `RingBuffer<MaxBucket>` where each bucket defaults to `-INFINITY`. On push, the bucket value is conditionally replaced if the new value is larger. On read, the maximum across all non-`-INFINITY` buckets is returned.

**Memory per key:**

`num_buckets * 16 bytes` (8 bytes MaxBucket + 8 bytes count per bucket). For a 1h window with 1m buckets: `60 * 16 = 960 bytes`.

**Example:**

```python
@bv.stream
class RawTransactions:
    user_id: str
    merchant_id: str
    amount: float
    status: str
    country: str

@bv.table(key="user_id")
def UserTransactions(raw: RawTransactions) -> bv.Table:
    return raw.group_by("user_id").agg(
        max_amount_24h=bv.max("amount", window="24h"),
    )
```

---

### stddev

Standard deviation of a numeric field in a sliding window.

**Python constructor:**

```python
bv.stddev("amount", window="1h")
bv.stddev("amount", window="24h", optional=True)
bv.stddev("amount", window="1h")  # v0: filter on the source stream, not on the op
```

**Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `field` | `str` | Yes | Name of the numeric event field (positional). |
| `window` | `str` | Yes | Duration string. |
| `optional` | `bool` | No | If `True`, skip events missing the field. Default `False`. |
| `where` | `str` | No | Filter expression. |
| `bucket` | `str` | No | Bucket granularity. |
| `backfill` | `bool` | No | Replay from event log on registration. |

**Description:**

Computes the **population standard deviation** of a numeric field within the window. Each bucket stores `(count, sum, sum_of_squares)`. On read, these are aggregated across all buckets and the standard deviation is computed as `sqrt(sum_sq/count - mean^2)`.

Returns a float. Returns `Missing` if zero events exist. Returns `0.0` if exactly one event exists. Floating-point rounding that produces tiny negative variance is clamped to zero.

**Window behavior:**

Uses `RingBuffer<StddevBucket>` where each `StddevBucket` holds `{count: u64, sum: f64, sum_sq: f64}`. On push, all three fields are updated in the current bucket. On read, the totals are aggregated across all buckets.

**Memory per key:**

`num_buckets * 24 bytes` (8 bytes count + 8 bytes sum + 8 bytes sum_sq per bucket). For a 1h window with 1m buckets: `60 * 24 = 1,440 bytes`.

**Example:**

```python
@bv.stream
class RawTransactions:
    user_id: str
    merchant_id: str
    amount: float
    status: str
    country: str

@bv.table(key="user_id")
def UserTransactions(raw: RawTransactions) -> bv.Table:
    return raw.group_by("user_id").agg(
        amount_stddev_1h=bv.stddev("amount", window="1h"),
        avg_amount_1h=bv.avg("amount", window="1h"),
    )
    # v0: append .with_columns(amount_vs_norm=<bv.col expression for "((_event.amount - avg_amount_1h) / amount_stddev_1h)">) to the table pipeline above
```

---

### percentile

Percentile estimation of a numeric field in a sliding window.

**Python constructor:**

```python
bv.percentile("amount", 0.95, window="1h")
bv.percentile("latency_ms", 0.50, window="30m", optional=True)
bv.percentile("amount", 0.99, window="24h")  # v0: filter on the source stream, not on the op
```

**Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `field` | `str` | Yes | Name of the numeric event field (positional). |
| `quantile` | `float` | Yes | Quantile value between 0.0 and 1.0 (e.g. `0.95` for p95). Positional. |
| `window` | `str` | Yes | Duration string. |
| `optional` | `bool` | No | If `True`, skip events missing the field. Default `False`. |
| `where` | `str` | No | Filter expression. |
| `bucket` | `str` | No | Bucket granularity. |
| `backfill` | `bool` | No | Replay from event log on registration. |

**Description:**

Computes an exact percentile within each bucket and merges across buckets on read. Each bucket stores a sorted `Vec<f64>` of all values pushed during that bucket's time range. On read, all values from non-expired buckets are collected, sorted, and the quantile is computed using linear interpolation (same method as numpy's default).

Returns a float. Returns `Missing` if zero events exist in the window.

**Window behavior:**

Uses `RingBuffer<PercentileBucket>` where each bucket holds a `Vec<f64>` of all values. On read, values from all buckets are merged into a single sorted array and the quantile is computed via linear interpolation.

**Memory per key:**

`O(total_events_in_window * 8 bytes)`. This operator stores every value, not just an aggregate. Memory grows linearly with event throughput. For 1000 events/hour in a 1h window: `~8 KB`.

**Example:**

```python
@bv.stream
class RawTransactions:
    user_id: str
    merchant_id: str
    amount: float
    status: str
    country: str

@bv.table(key="user_id")
def UserTransactions(raw: RawTransactions) -> bv.Table:
    return raw.group_by("user_id").agg(
        p50_amount_1h=bv.percentile("amount", 0.50, window="1h"),
        p95_amount_1h=bv.percentile("amount", 0.95, window="1h"),
        p99_amount_1h=bv.percentile("amount", 0.99, window="1h"),
    )
```

---

### distinct_count

Approximate unique count of a field in a sliding window using adaptive HLL++.

**Python constructor:**

```python
bv.count_distinct("merchant_id", window="24h")
bv.count_distinct("ip_address", window="1h", bucket="5m")
```

**Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `field` | `str` | Yes | Name of the event field to count unique values of (positional). |
| `window` | `str` | Yes | Duration string. |
| `bucket` | `str` | No | Bucket granularity. |
| `backfill` | `bool` | No | Replay from event log on registration. |

**Description:**

Counts the approximate number of unique values of a field within the window. Uses a three-phase adaptive sketch per bucket, automatically promoting as cardinality grows:

1. **Exact phase** (0-16 elements): Sorted array of hashes. Zero error. ~128 bytes max.
2. **HashSet phase** (17-512 elements): Vec of unique u64 hashes. Zero error. ~8 bytes per unique.
3. **HLL++ phase** (513+ elements): HyperLogLog++ with bias correction (Heule et al. 2013). ~1.6% error at p=12 precision. Fixed 4 KB per bucket.

On read, all non-empty bucket sketches are merged (union semantics) and the combined cardinality is estimated.

Returns a float (the estimated count). Returns `Missing` if zero events exist in the window.

**Window behavior:**

Uses `RingBuffer<Hll>` where each bucket holds an adaptive sketch. On push, the field value is hashed and inserted into the current bucket's sketch. On read, all bucket sketches are merged into a single sketch and `count()` is called.

**Memory per key:**

Depends on cardinality per bucket:

| Uniques per bucket | Bytes per bucket | 30-bucket window |
|--------------------|------------------|------------------|
| 5 | ~40 B | ~1.2 KB |
| 50 | ~400 B | ~12 KB |
| 500 | ~4 KB | ~120 KB |
| 5000+ | ~4 KB (HLL dense) | ~120 KB |

Most fraud use cases (user sees ~5-20 merchants/hour) stay in the exact or hash set phase, using far less memory than a full HLL.

**Example:**

```python
@bv.stream
class RawTransactions:
    user_id: str
    merchant_id: str
    amount: float
    status: str
    country: str

@bv.table(key="user_id")
def UserTransactions(raw: RawTransactions) -> bv.Table:
    return raw.group_by("user_id").agg(
        unique_merchants_24h=bv.count_distinct("merchant_id", window="24h"),
        unique_countries_1h=bv.count_distinct("country", window="1h"),
    )
```

---

### exact_min

Exact retractable minimum in a sliding window.

**Python constructor:**

```python
bv.exact_min("amount", window="1h")
bv.exact_min("amount", window="24h", bucket="15m")
```

**Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `field` | `str` | Yes | Name of the numeric event field (positional). |
| `window` | `str` | Yes | Duration string. |
| `bucket` | `str` | No | Bucket granularity. |
| `backfill` | `bool` | No | Replay from event log on registration. |

**Description:**

Computes the **exact** minimum value of a field within the window. Unlike `min` (which tracks only per-bucket minimums), `exact_min` retains all individual values using a `BTreeMap<OrderedFloat<f64>, u32>` for O(log n) minimum lookups. When buckets expire, their values are retracted from the sorted map.

Use this when you need precise minimum values and the per-bucket approximation of `min` is not acceptable. The tradeoff is higher memory usage since every value is stored.

Returns a float. Returns `Missing` if zero events exist in the window.

**Window behavior:**

Uses `RingBuffer<ValBucket>` where each bucket stores a `Vec<f64>` of all values pushed during that bucket's time range. A parallel `BTreeMap` maintains a sorted multiset of all in-window values. On read, the BTreeMap is rebuilt from non-expired bucket values and the smallest key is returned.

**Memory per key:**

`O(total_events_in_window * ~40 bytes)` (8 bytes per value in bucket Vecs + ~32 bytes per unique value in BTreeMap entry). For 1000 events/hour in a 1h window: `~40 KB`.

**Example:**

```python
@bv.stream
class RawTransactions:
    user_id: str
    merchant_id: str
    amount: float
    status: str
    country: str

@bv.table(key="user_id")
def UserTransactions(raw: RawTransactions) -> bv.Table:
    return raw.group_by("user_id").agg(
        exact_min_amount_1h=bv.exact_min("amount", window="1h"),
    )
```

---

### exact_max

Exact retractable maximum in a sliding window.

**Python constructor:**

```python
bv.exact_max("amount", window="1h")
bv.exact_max("amount", window="24h", bucket="15m")
```

**Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `field` | `str` | Yes | Name of the numeric event field (positional). |
| `window` | `str` | Yes | Duration string. |
| `bucket` | `str` | No | Bucket granularity. |
| `backfill` | `bool` | No | Replay from event log on registration. |

**Description:**

Computes the **exact** maximum value of a field within the window. Same approach as `exact_min` but returns the largest key from the BTreeMap.

Returns a float. Returns `Missing` if zero events exist in the window.

**Window behavior:**

Identical to `exact_min`. On read, the BTreeMap is rebuilt and the largest key is returned.

**Memory per key:**

Same as `exact_min`: `O(total_events_in_window * ~40 bytes)`.

**Example:**

```python
@bv.stream
class RawTransactions:
    user_id: str
    merchant_id: str
    amount: float
    status: str
    country: str

@bv.table(key="user_id")
def UserTransactions(raw: RawTransactions) -> bv.Table:
    return raw.group_by("user_id").agg(
        exact_max_amount_1h=bv.exact_max("amount", window="1h"),
    )
```

---

## Value Operators

These operators do not use time windows. They track individual values or sequences.

### last

Most recent value of a field.

**Python constructor:**

```python
bv.last("country")
bv.last("merchant_id")
```

**Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `field` | `str` | Yes | Name of the event field (positional). |
| `backfill` | `bool` | No | Replay from event log on registration. |

**Description:**

Stores the most recent value of a field. No window -- always returns the last-seen value regardless of how long ago it was pushed. Accepts any value type: numbers, strings, booleans (booleans are stored as `1`/`0`).

Only updates if the event timestamp is greater than or equal to the previously stored timestamp, ensuring out-of-order events do not overwrite newer values.

Returns the stored value (integer, float, or string). Returns `Missing` if no event has been pushed.

**Window behavior:**

None. Single value + timestamp. O(1) state.

**Memory per key:**

~100 bytes (one `FeatureValue` + one `Option<SystemTime>`).

**Example:**

```python
@bv.stream
class RawTransactions:
    user_id: str
    merchant_id: str
    amount: float
    status: str
    country: str

@bv.table(key="user_id")
def UserTransactions(raw: RawTransactions) -> bv.Table:
    return raw.group_by("user_id").agg(
        last_country=bv.last("country"),
        last_merchant=bv.last("merchant_id"),
        last_amount=bv.last("amount"),
    )
```

---

### first

First value ever seen for a field.

**Python constructor:**

```python
bv.first("signup_source")
bv.first("country", optional=True)
```

**Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `field` | `str` | Yes | Name of the event field (positional). |
| `optional` | `bool` | No | If `True`, missing field on first event is skipped. Default `False`. |
| `backfill` | `bool` | No | Replay from event log on registration. |

**Description:**

Stores the first value ever seen for an entity key. Once set, all subsequent events are ignored -- the value never changes. Useful for capturing initial state like signup source, first country, or registration channel.

Returns the stored value. Returns `Missing` if no value has been captured yet.

**Window behavior:**

None. Single value + timestamp. O(1) state. After the first value is stored, `push()` returns immediately without examining the event.

**Memory per key:**

~100 bytes.

**Example:**

```python
@bv.stream
class RawTransactions:
    user_id: str
    merchant_id: str
    amount: float
    status: str
    country: str

@bv.table(key="user_id")
def UserTransactions(raw: RawTransactions) -> bv.Table:
    return raw.group_by("user_id").agg(
        first_country=bv.first("country"),
        signup_source=bv.first("source"),
    )
```

---

### lag

Previous Nth value of a field (event-count-based).

**Python constructor:**

```python
bv.lag("amount", n=1)
bv.lag("country", n=3, optional=True)
```

**Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `field` | `str` | Yes | Name of the event field (positional). |
| `n` | `int` | Yes | Number of events to lag by. `n=1` returns the previous event's value. |
| `optional` | `bool` | No | If `True`, skip events missing the field. Default `False`. |
| `backfill` | `bool` | No | Replay from event log on registration. |

**Description:**

Returns the value from N events ago for the same entity key. Uses a `VecDeque` ring buffer of size N. On push, the new value is appended to the back; if the buffer exceeds N, the oldest value is popped from the front. On read, the front (oldest) value is returned.

Returns `Missing` until N events have been pushed (the buffer is not yet full).

**Window behavior:**

None. Event-count-based, not time-based. The lag is measured in number of events, not duration.

**Memory per key:**

`O(N * ~100 bytes)` per value stored.

**Example:**

```python
@bv.stream
class RawTransactions:
    user_id: str
    merchant_id: str
    amount: float
    status: str
    country: str

@bv.table(key="user_id")
def UserTransactions(raw: RawTransactions) -> bv.Table:
    return raw.group_by("user_id").agg(
        prev_amount=bv.lag("amount", n=1),
        prev_country=bv.lag("country", n=1),
    )
    # v0: append .with_columns(amount_change=<bv.col expression for "_event.amount - prev_amount">) to the table pipeline above
```

---

### last_n

Last N values of a field as a JSON array.

**Python constructor:**

```python
bv.last_n("amount", n=5)
bv.last_n("merchant_id", n=10, optional=True)
```

**Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `field` | `str` | Yes | Name of the event field (positional). |
| `n` | `int` | Yes | Number of recent values to keep. |
| `optional` | `bool` | No | If `True`, skip events missing the field. Default `False`. |
| `backfill` | `bool` | No | Replay from event log on registration. |

**Description:**

Stores the last N values of a field, returned as a JSON array string. Unlike `lag` (which returns a single value from N events ago), `last_n` returns **all** N recent values as a list.

The values are returned as a serialized JSON array string (e.g. `"[50.0, 75.0, 100.0]"`), since `FeatureValue` does not have a native list variant.

Returns `Missing` if no events have been pushed. Returns partial results if fewer than N events have been seen.

**Window behavior:**

None. Event-count-based using a `VecDeque` of capacity N.

**Memory per key:**

`O(N * ~100 bytes)` per value stored.

**Example:**

```python
@bv.stream
class RawTransactions:
    user_id: str
    merchant_id: str
    amount: float
    status: str
    country: str

@bv.table(key="user_id")
def UserTransactions(raw: RawTransactions) -> bv.Table:
    return raw.group_by("user_id").agg(
        last_5_amounts=bv.last_n("amount", n=5),
        last_10_merchants=bv.last_n("merchant_id", n=10),
    )
```

---

### ema

Exponential moving average with time-based decay.

**Python constructor:**

```python
bv.ema("amount", half_life="30m")
bv.ema("latency_ms", half_life="1h", optional=True)
```

**Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `field` | `str` | Yes | Name of the numeric event field (positional). |
| `half_life` | `str` | Yes | Duration string for the EMA half-life (e.g. `"30m"`, `"1h"`). After one half-life of elapsed time, the weight of a past value is halved. |
| `optional` | `bool` | No | If `True`, skip events missing the field. Default `False`. |
| `backfill` | `bool` | No | Replay from event log on registration. |

**Description:**

Computes an exponential moving average with continuous time-based decay. On each event:

```
alpha = exp(-ln(2) * elapsed_seconds / half_life_seconds)
current = alpha * current + (1 - alpha) * new_value
```

The first event initializes the EMA to the event's value. Subsequent events blend the new value based on how much time has elapsed since the last event.

Returns a float. Returns `Missing` if no events have been pushed.

**Window behavior:**

None. O(1) state -- just the current EMA value, the last event timestamp, and the half-life parameter. The decay is applied continuously based on elapsed wall-clock time between events, not on discrete windows or buckets.

**Memory per key:**

~48 bytes (one `f64` for current value, one `f64` for half_life, one `Option<SystemTime>`, one `bool`).

**Example:**

```python
@bv.stream
class RawTransactions:
    user_id: str
    merchant_id: str
    amount: float
    status: str
    country: str

@bv.table(key="user_id")
def UserTransactions(raw: RawTransactions) -> bv.Table:
    return raw.group_by("user_id").agg(
        ema_amount_30m=bv.ema("amount", half_life="30m"),
        ema_amount_4h=bv.ema("amount", half_life="4h"),
    )
    # v0: append .with_columns(ema_divergence=<bv.col expression for "ema_amount_30m / ema_amount_4h">) to the table pipeline above
```

---

## Computed Operators

### derive

Expression computed over other features. No state, evaluated on read.

**Python constructor:**

```python
bv.derive("failed_tx_30m / tx_count_30m")
bv.derive("_event.amount / avg_amount_1h")
bv.derive("tx_count_1h > 10 and login_count_1h < 2")
```

**Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `expr` | `str` | Yes | Expression string (positional). |

**Description:**

Evaluates an expression over other features at read time. Has no state of its own -- it is a pure function of other feature values. The expression is parsed into an AST at pipeline registration time and evaluated in Rust at event time.

**Supported expression syntax:**

| Category | Operators |
|----------|-----------|
| Arithmetic | `+`, `-`, `*`, `/` |
| Comparison | `>`, `<`, `>=`, `<=`, `==`, `!=` |
| Boolean | `and`, `or`, `not` |
| Field access | `feature_name`, `StreamName.feature_name`, `_event.field_name` |
| Builtins | `abs()`, `min()`, `max()`, `now()` |

**Window behavior:**

None. O(1) -- no state, computed on read.

**Memory per key:**

0 bytes. The AST is stored once per pipeline definition, not per entity key.

**Example:**

```python
@bv.stream
class RawTransactions:
    user_id: str
    merchant_id: str
    amount: float
    status: str
    country: str

@bv.table(key="user_id")
def UserTransactions(raw: RawTransactions) -> bv.Table:
    return raw.group_by("user_id").agg(
        tx_count_30m=bv.count(window="30m"),
        tx_count_1h=bv.count(window="1h"),
        tx_count_24h=bv.count(window="24h"),
        failed_tx_30m=bv.count(window="30m")  # v0: use .filter(bv.col(...) == ...) on the stream instead of where=
        avg_amount_1h=bv.avg("amount", window="1h"),
    )

    # Derived features
    # v0: append .with_columns(failure_rate=<bv.col expression for "failed_tx_30m / tx_count_30m">) to the table pipeline above
    # v0: append .with_columns(velocity_spike=<bv.col expression for "(tx_count_1h / 1) / (tx_count_24h / 24)">) to the table pipeline above
    # v0: append .with_columns(amount_vs_avg=<bv.col expression for "_event.amount / avg_amount_1h">) to the table pipeline above
```

---

## Filtering events before aggregation

The pre-v0 decorator-level `where=` parameter was removed. In v0, filtering
happens on the stream with `.filter(bv.col(...) == value)` before
`.group_by(...).agg(...)`. To get per-feature filtering, define a table that
filters a sub-stream and either merge it into the main table or join the
sub-aggregation back.

**Examples:**

```python
# Equality
txs.filter(bv.col("status") == "failed").group_by("user_id").agg(
    failed_count=bv.count(window="30m"),
)

# Comparison
txs.filter(bv.col("amount") > 100).group_by("user_id").agg(
    big_tx_count=bv.count(window="1h"),
)

# Boolean logic
txs.filter((bv.col("status") == "success") & (bv.col("amount") > 50)).group_by("user_id").agg(
    good_count=bv.count(window="1h"),
)
```

The filter expression runs against each incoming event; events that do not
match are skipped for the downstream table entirely. Other tables that do not
apply the filter process the same event normally.

---

## Window Mechanics

All windowed operators use a **bucketed ring buffer** (`RingBuffer<T>`) that divides the window into fixed-duration time buckets.

### How it works

1. **Bucket count**: `num_buckets = ceil(window_duration / bucket_duration)`. For a 30m window with 1m buckets, that is 30 buckets.

2. **On event arrival**: The ring buffer advances to the current time, zeroing any buckets that have been skipped. The event data is then added to the current (head) bucket.

3. **On read**: The ring buffer advances to the current time (expiring stale buckets), then aggregates across all remaining buckets.

4. **Lazy expiration**: There are no background timers. Stale buckets are zeroed only when `advance_to()` is called during a push or read. This is safe in Beava's single-threaded design.

### Bucket granularity tradeoff

Finer buckets (more buckets per window) give more accurate time boundaries but use more memory:

| Window | Bucket | Buckets | Memory (count) | Accuracy |
|--------|--------|---------|----------------|----------|
| 1h | 1m | 60 | 480 B | Events expire within 1m of window edge |
| 1h | 5m | 12 | 96 B | Events expire within 5m of window edge |
| 24h | 1m | 1440 | 11.5 KB | High accuracy |
| 24h | 15m | 96 | 768 B | Events expire within 15m of window edge |

An event entering a bucket stays in that bucket until the bucket itself expires. This means the effective window length is between `window_duration` and `window_duration + bucket_duration`.

### Out-of-order events

Events with timestamps earlier than the current bucket start are assigned to the current bucket (not dropped). This preserves all data but may cause slight inaccuracy in bucket boundaries. The `advance_to()` method uses `unwrap_or(Duration::ZERO)` for negative time differences, preventing panics on out-of-order timestamps.

### Full window gap

If the time gap since the last event exceeds the full window duration, **all** buckets are zeroed. This correctly resets the state for entities that have been inactive longer than the window.

---

## Cross-Stream References

The `derive` operator supports referencing features from other streams using the `StreamName.feature_name` syntax. This enables cross-stream computed features.

### Within a view

Views (`@bv.view`) are the primary mechanism for cross-stream references:

```python
@bv.stream
class RawTransactions:
    user_id: str
    merchant_id: str
    amount: float
    status: str
    country: str

@bv.stream
class RawLogins:
    user_id: str
    merchant_id: str
    amount: float
    status: str
    country: str

@bv.table(key="user_id")
def Transactions(raw: RawTransactions) -> bv.Table:
    return raw.group_by("user_id").agg(
        tx_count_1h=bv.count(window="1h"),
    )

@bv.table(key="user_id")
def Logins(raw: RawLogins) -> bv.Table:
    return raw.group_by("user_id").agg(
        login_count_1h=bv.count(window="1h"),
    )

@bv.view(key="user_id")
class UserRisk:
    # v0: append .with_columns(tx_to_login_ratio=<bv.col expression for "Transactions.tx_count_1h / Logins.login_count_1h">) to the table pipeline above
    # v0: append .with_columns(is_suspicious=<bv.col expression for "Transactions.tx_count_1h > 10 and Logins.login_count_1h < 2">) to the table pipeline above
```

### Event field access

Use `_event.field_name` in derive expressions to reference raw fields from the current event:

```python
# v0: append .with_columns(amount_vs_avg=<bv.col expression for "_event.amount / avg_amount_1h">) to the table pipeline above
```

### Cross-key lookups

Use `bv.lookup()` to reference features from a different entity key:

```python
@bv.stream
class RawMerchantEvents:
    user_id: str
    merchant_id: str
    amount: float
    status: str
    country: str

@bv.table(key="merchant_id")
def MerchantActivity(raw: RawMerchantEvents) -> bv.Table:
    return raw.group_by("merchant_id").agg(
        chargeback_count_24h=bv.count(window="24h")  # v0: use .filter(bv.col(...) == ...) on the stream instead of where=
    )

@bv.view(key="user_id")
class FraudSignals:
    merchant_chargebacks = bv.lookup(
        "MerchantActivity.chargeback_count_24h",
        on="merchant_id"
    )
```

The `on` parameter specifies which field in the current event contains the foreign key to use for the lookup.
