# DataFrame Time Semantics for Streaming — Research

**Date:** 2026-04-12

## The Core Question

What does `groupby + rolling window` mean in a streaming context — and how should
Tally's DataFrame API make the groupby key, the time axis, and the window explicit
rather than hiding them behind magic?

In batch DataFrames, every operation is explicit:
- **Groupby key:** `df.groupby("user_id")`
- **Time column:** `.rolling("1h", on="timestamp")`
- **Aggregation:** `.sum()`

In Tally's streaming model, two of these three are implicit:
- The **groupby key** is declared once at stream registration (`key="user_id"`)
- The **time axis** is always NOW (event arrival time) — there is no timestamp column
- Only the **window** and **aggregation** are per-feature

The question: should Tally's API make all three explicit (like Polars), or
acknowledge its streaming nature where key and time are fixed?

---

## How Each Library Does It

### Pandas: Batch, Explicit Everything

```python
# Grouped rolling aggregation — all 3 axes explicit
df.groupby("user_id").rolling("1h", on="timestamp")["amount"].sum()

# Equivalent with set_index
df.set_index("timestamp").groupby("user_id").rolling("1h")["amount"].sum()

# Resample (tumbling windows)
df.set_index("timestamp").groupby("user_id").resample("1h")["amount"].sum()

# As-of join — temporal join by nearest timestamp
pd.merge_asof(
    trades.sort_values("time"),
    quotes.sort_values("time"),
    on="time",           # time axis
    by="ticker",         # groupby key
    direction="backward" # look back in time
)
```

**Key insight:** All three axes explicit every time. Verbose but unambiguous.

### Polars: Batch, Expression-Based

```python
# group_by_dynamic — tumbling/hopping windows with explicit time column
df.group_by_dynamic(
    "timestamp",          # time column — explicit
    every="1h",           # window interval
    period="1h",          # window size (= every for tumbling)
    by="user_id",         # groupby key — explicit
).agg(
    pl.col("amount").sum().alias("total_1h"),
    pl.col("amount").count().alias("count_1h"),
)

# rolling — one window per row, explicit time column
df.rolling(
    "timestamp",          # time column
    period="1h",          # window size
    by="user_id",         # groupby key
).agg(
    pl.col("amount").sum().alias("rolling_sum_1h"),
)

# As-of join
trades.join_asof(
    quotes,
    on="time",            # time axis
    by="ticker",          # groupby key
    strategy="backward",  # look back
    tolerance="5m",       # max time distance
)
```

**Key insight:** Four distinct axes: time column (`on`), key (`by`), window
(`every`/`period`), aggregation (`.agg()`). `rolling()` creates one window per
row — closest to Tally's per-event model.

### PySpark Structured Streaming: Streaming, Window-as-GroupBy-Key

```python
from pyspark.sql.functions import window, col, sum, count

# The window() function creates a virtual groupby key from a time column
df.withWatermark("timestamp", "10 minutes") \
  .groupBy(
      col("user_id"),                          # entity key
      window(col("timestamp"), "1 hour")       # time window AS a group key
  ).agg(
      sum("amount").alias("total_1h"),
      count("*").alias("count_1h"),
  )

# Stream-stream join with watermarks
txns.withWatermark("ts", "1h") \
    .join(
        logins.withWatermark("ts", "1h"),
        on="user_id",                          # join key
        how="left"
    ).where(                                   # time constraint
        "txns.ts BETWEEN logins.ts AND logins.ts + INTERVAL 1 HOUR"
    )
```

**Key insight:** Time window is a GROUPBY KEY. `groupBy(user_id, window(ts, "1h"))`
= one row per (user, window). Tumbling/hopping model, NOT sliding like Tally.

### Flink: Streaming, Window + Temporal Join

```sql
-- Tumbling window aggregation (Flink SQL)
SELECT
    user_id,
    TUMBLE_START(event_time, INTERVAL '1' HOUR) AS window_start,
    SUM(amount) AS total_1h,
    COUNT(*) AS count_1h
FROM transactions
GROUP BY
    user_id,
    TUMBLE(event_time, INTERVAL '1' HOUR)

-- Temporal join — versioned table lookup at event time
SELECT t.*, r.rate
FROM transactions t
JOIN currency_rates FOR SYSTEM_TIME AS OF t.event_time AS r
  ON t.currency = r.currency
```

```python
# PyFlink Table API
txns.window(
    Tumble.over(lit(1).hour).on(col("event_time")).alias("w")
).group_by(
    col("user_id"), col("w")
).select(
    col("user_id"),
    col("amount").sum.alias("total_1h"),
)
```

**Key insight:** `FOR SYSTEM_TIME AS OF` = look up table B's state at event time.
Tally's `@st.view` does this implicitly (reads latest state on GET).

### Fennel: Streaming Feature Platform (Closest to Tally)

```python
from fennel.datasets import dataset, field, pipeline, Dataset
from fennel.dtypes import Continuous
from fennel.lib import inputs, outputs
from fennel.connectors import source

@dataset
class Transactions:
    user_id: str = field(key=True)        # groupby key — declared on field
    amount: float
    timestamp: datetime = field(timestamp=True)  # time axis — declared on field

@dataset
class UserFeatures:
    user_id: str = field(key=True)
    count_1h: int
    total_1h: float
    timestamp: datetime = field(timestamp=True)

    @pipeline
    @inputs(Transactions)
    def pipeline(cls, txns: Dataset):
        return txns.groupby("user_id").aggregate(
            count_1h=Count(window=Continuous("1h")),
            total_1h=Sum(of="amount", window=Continuous("1h")),
        )
```

**Key insight:** Closest prior art. Key on field (`key=True`), time on field
(`timestamp=True`), window on aggregation (`Continuous("1h")`). The `Continuous`
window = "sliding, always up-to-date" — exactly Tally's model.

---

## Time Join Patterns

### Pattern 1: As-Of Join (Pandas/Polars)

For each row in table A, find the most recent row in table B with matching key.

```python
# Pandas
pd.merge_asof(trades, quotes, on="time", by="ticker")

# Polars
trades.join_asof(quotes, on="time", by="ticker", strategy="backward")
```

**Streaming equivalent:** On event in A, look up latest state of B for same key.
This is exactly what Tally's `@st.view` already does.

### Pattern 2: Temporal Table Join (Flink)

Join against a versioned table — look up the value as it was at event time.

```sql
SELECT t.*, r.rate
FROM orders t
JOIN currency_rates FOR SYSTEM_TIME AS OF t.order_time AS r
  ON t.currency = r.currency
```

**Streaming equivalent:** Tally's `st.lookup()` (cross-key) and `@st.view`
(same-key) = `FOR SYSTEM_TIME AS OF NOW`.

### Pattern 3: Windowed Join (PySpark)

Events from both streams must fall within a time window of each other.

```python
txns.join(logins, on="user_id").where(
    "txns.ts BETWEEN logins.ts - INTERVAL 5 MINUTES AND logins.ts"
)
```

**Streaming equivalent:** Not supported in Tally — requires buffering raw events.

### Summary: Which Joins Tally Supports

| Join Type | Batch Example | Tally Equivalent | Supported? |
|-----------|---------------|------------------|------------|
| As-of (same key) | `merge_asof(a, b, on="ts", by="key")` | `@st.view` reads latest from both | YES |
| As-of (cross key) | `merge_asof(a, b, on="ts", by="other_key")` | `st.lookup(B.feat, on="other_key")` | YES |
| Point-in-time | `a.join(b, on="key")` at query time | `app.get("key")` returns all features | YES |
| Windowed event join | `a.join(b).where("a.ts BETWEEN b.ts - 5m AND b.ts")` | Not supported | NO |

---

## The Same Feature in Every Library

**Task:** Count transactions per user in a 1-hour sliding window, sum amount,
compute velocity = count_1h / count_24h.

### Pandas (batch)
```python
g = df.groupby("user_id").rolling("1h", on="timestamp")
result = g.agg(count_1h=("amount", "count"), total_1h=("amount", "sum"))
# Second pass for 24h window, then join — awkward
```

### Polars (batch)
```python
r1h = df.rolling("timestamp", period="1h", by="user_id").agg(
    pl.col("amount").count().alias("count_1h"),
    pl.col("amount").sum().alias("total_1h"))
r24h = df.rolling("timestamp", period="24h", by="user_id").agg(
    pl.col("amount").count().alias("count_24h"))
result = r1h.join(r24h, on=["user_id", "timestamp"])  # must join two calls
```

### PySpark Structured Streaming
```python
agg_1h = df.groupBy("user_id", window("timestamp", "1 hour")).agg(
    count("*").alias("count_1h"), sum("amount").alias("total_1h"))
# Tumbling, NOT sliding. Joining 1h and 24h windows is non-trivial.
```

### Fennel
```python
txns.groupby("user_id").aggregate(
    count_1h=Count(window=Continuous("1h")),
    count_24h=Count(window=Continuous("24h")),
    total_1h=Sum(of="amount", window=Continuous("1h")))
# velocity requires a separate @featureset extractor
```

### Tally — Current `@st.stream` API
```python
@st.stream(key="user_id")
class UserFeatures:
    count_1h   = st.count(window="1h")
    count_24h  = st.count(window="24h")
    total_1h   = st.sum("amount", window="1h")
    velocity   = st.derive("(count_1h / 1) / (count_24h / 24)")
```

### Tally — Proposed DataFrame API
```python
txns = st.table("Transactions", key="user_id")
txns["count_1h"]  = txns.count(window="1h")
txns["count_24h"] = txns.count(window="24h")
txns["total_1h"]  = txns["amount"].sum(window="1h")
txns["velocity"]  = (txns["count_1h"] / 1) / (txns["count_24h"] / 24)
```

**Observation:** Tally is the ONLY system where mixed-window features (1h AND 24h)
coexist naturally in a single definition. Every batch library requires separate
groupby/rolling calls per window size, then a join. Fennel matches Tally here
because it has the same streaming model.

---

## What Tally's Model Already Is

Tally is a **continuously materialized `groupby(key).rolling(now, period=window).agg()`**.

Spelled out:
1. `st.table("Transactions", key="user_id")` = `df.groupby("user_id")`
2. `window="1h"` on each operator = `.rolling(period="1h")` anchored at NOW
3. `.count()`, `.sum("amount")` = `.agg(count(), sum("amount"))`
4. Every PUSH event = one new row appended to the group
5. Every GET = read the current rolling aggregate (no re-scan needed)

The streaming model eliminates two batch concerns: which time column (always NOW)
and which rows to scan (none — state is incremental). Tally's API should be honest
about this rather than pretending to be a general DataFrame.

---

## Proposed Tally API: Joins

### Same-Key Join (View)

```python
txns = st.table("Transactions", key="user_id")
txns["count_1h"] = txns.count(window="1h")

logins = st.table("Logins", key="user_id")
logins["count_1h"] = logins.count(window="1h")

# Join — both keyed by user_id, reads latest state from both
risk = txns.join(logins, on="user_id")
risk["tx_to_login"] = txns["count_1h"] / logins["count_1h"]
```

Compiles to the same JSON as `@st.view`. Semantics: on GET, read current state
from both tables for the requested key. This is an as-of join where "as of" = NOW.

### Cross-Key Lookup

```python
merchants = st.table("MerchantActivity", key="merchant_id")
merchants["chargebacks_24h"] = merchants.count(window="24h", where="type == 'chargeback'")

# Lookup: resolve merchant_id from the event payload
txns["merchant_cbacks"] = txns.lookup(merchants["chargebacks_24h"], on="merchant_id")
```

Compiles to `st.lookup()`. Semantics: on PUSH to txns, read `_event.merchant_id`,
look up `MerchantActivity[merchant_id].chargebacks_24h`.

**Not supported:** Windowed event joins (buffering raw events from both streams).
Use Flink or PySpark for that.

---

## Recommendation

### 1. Keep key on the table, window on the operator

```python
txns = st.table("Transactions", key="user_id")  # key declared once
txns["count_1h"]  = txns.count(window="1h")      # window per feature
txns["count_24h"] = txns.count(window="24h")      # mixed windows = natural
```

This matches Fennel's model and is honest about what Tally is: a system where
the groupby key is fixed at registration and each feature can have its own window.
Putting the key on `table()` rather than on each `.agg()` call is correct because
Tally's key truly is per-stream, not per-query.

### 2. Do NOT add a time column parameter

No `on="timestamp"` — Tally timestamps at arrival. If event-time semantics come
later, make it table-level (`st.table("Txns", key="uid", time="ts")`), not
per-aggregation.

### 3. Use `.join()` for same-key views, `.lookup()` for cross-key

```python
risk = txns.join(logins, on="user_id")                           # same-key view
txns["merchant_cbacks"] = txns.lookup(merchants["cbacks"], on="merchant_id")  # cross-key
```

Both are point-in-time joins (as-of NOW). This is the only join semantic that
makes sense for a system that materializes aggregates incrementally.

### 4. Name the difference explicitly in docs — make it a selling point

> **Tally is a continuously materialized `groupby().rolling().agg()`.**
> In Pandas you write `df.groupby("uid").rolling("1h", on="ts")["amt"].sum()`
> and wait for it to scan your data. In Tally you write
> `txns["total_1h"] = txns["amount"].sum(window="1h")` and the answer is
> always ready — updated on every event, readable in microseconds.
