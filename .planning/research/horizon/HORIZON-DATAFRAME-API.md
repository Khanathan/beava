# DataFrame-Style Stream API — Horizon Research

**Date:** 2026-04-12
**Status:** Exploratory (next milestone candidate)
**Research question:** Can Tally's Python SDK expose keyed streams as "tables" and keyless streams as "event streams" with a pandas/Polars-like DataFrame API, while compiling to the same pipeline JSON the Rust server already understands?

---

## Executive Summary

Yes — and it requires **zero server changes**. The DataFrame API is a pure SDK-layer rewrite that compiles proxy objects (`Table`, `Column`, `Expr`) into the same `RegisterRequest` JSON that `@st.stream` produces today. The recommended design is **Form B (bracket assignment, Pandas-like)** because it matches the user's stated vision, is the most intuitive for data scientists, and has the simplest Python implementation via `__setitem__`/`__getitem__` proxy objects.

Key findings:
- Every major streaming framework (PySpark, Flink Table API, Pathway, Fennel) has converged on a DataFrame-like API for defining streaming computations.
- The "lazy expression tree that compiles to a backend plan" pattern is battle-tested (Polars, Ibis, SQLAlchemy Core, PySpark).
- Tally already has this architecture — `@st.stream` classes serialize to JSON. The new API is syntactic sugar.
- Three new server-side operators would unlock the most-requested DataFrame operations: `lag` (shift), `stddev`, and `ema` (exponential moving average).
- The `@st.stream` decorator API can coexist with `st.table()` — both produce the same JSON.

---

## Prior Art Survey

### Platform Comparison Matrix

| Platform | API Style | Column Access | Aggregation | Windowing | Join | Filter | Streaming? | Lazy? |
|----------|-----------|---------------|-------------|-----------|------|--------|------------|-------|
| **Pandas** | `df["col"]`, `df.col` | `__getitem__` returns Series | `.sum()`, `.mean()` | `.rolling(N)` | `.merge()` | `df[df["x"] > 5]` | No (batch) | No (eager) |
| **Polars** | `df.select(pl.col("x"))` | `pl.col()` expr builder | `.sum()`, `.mean()` | `.rolling()`, `.over()` | `.join()` | `.filter()` | No (batch) | Yes (lazy mode) |
| **PySpark SS** | `df.select("col")` | `df["col"]` or `col("x")` | `.agg(sum("x"))` | `window("ts", "1h")` | `.join()` | `.filter()` | Yes | Yes (catalyst) |
| **Flink Table** | `table.select()` | `table.col("x")` | `.group_by().select(agg)` | `Tumble.over("1h")` | `.join()` | `.where()` | Yes | Yes (plan) |
| **Pathway** | `t.select(pw.this.x)` | `pw.this.col` or `t.col` | `pw.reducers.sum()` | `pw.temporal.sliding()` | `.join()` | `.filter()` | Yes | Yes (Rust engine) |
| **Fennel** | `@dataset` + `@pipeline` | `col("x")` | `.aggregate(Count())` | `Continuous("1d")` | `.join()` | `.filter()` | Yes | Yes (server plan) |
| **Chalk** | `@features` class + `@online` | Type annotations | Resolvers | N/A (point-in-time) | Via resolvers | Via resolvers | Partial | Yes (plan) |
| **Hamilton** | Functions = nodes | Function params = deps | Return values | N/A | Function composition | N/A | No | Yes (DAG) |
| **Ibis** | `t["col"]`, `t.col` | `__getitem__` returns Column | `.sum()`, `.mean()` | `.over(ibis.window())` | `.join()` | `.filter()` | Partial | Yes (SQL compile) |
| **Bytewax** | Dataflow operators | `map`, `flat_map` | `fold_window` | `TumblingWindow` | N/A | `filter` | Yes | No (eager dataflow) |

### Key Patterns Observed

**Pattern 1: Expression proxy objects (Polars, Ibis, PySpark, Fennel)**
Every lazy system uses proxy objects that capture operations as an AST instead of executing them. Polars `Expr`, PySpark `Column`, Ibis `Column`, and Fennel `col()` all override `__add__`, `__gt__`, `__eq__` etc. to build expression trees.

**Pattern 2: Bracket assignment for feature definition (Pandas, Polars)**
`df["new_col"] = expr` is the most universally understood pattern among data practitioners. Polars supports it in eager mode; Pandas uses it as the primary API.

**Pattern 3: Windowing as a modifier, not a separate concept (PySpark, Pathway)**
PySpark: `window("timestamp", "1 hour")` as a groupBy key. Pathway: `pw.temporal.sliding(hop=..., duration=...)`. Fennel: `Continuous("1d")` passed to aggregate operators.

**Pattern 4: Groupby IS the key (Fennel, Pathway)**
In Fennel, `.groupby("user_id").aggregate(...)` is exactly what Tally calls a "keyed stream." The key field defines the grouping. This is the mental model bridge: `st.table("Txns", key="user_id")` means "this table is grouped by user_id, and every aggregation runs per-group."

---

## The Mental Model

```
Tally Concept          | DataFrame Analogy          | User Thinks...
-----------------------|----------------------------|---------------------------
Keyed stream           | Table grouped by key       | "A table of per-user stats"
Keyless stream         | Unbounded event stream     | "A firehose of raw events"
Operator (count, sum)  | Grouped aggregation        | "df.groupby('uid').sum()"
Window                 | Rolling window             | "df.rolling('1h').sum()"
Derive                 | Computed column            | "df['new'] = df['a'] / df['b']"
View                   | Join result                | "users.join(logins)"
Lookup                 | Cross-key join             | "Left join on merchant_id"
Push event             | Append row to stream       | "df.append(row)"
Get features           | Read current row for key   | "df.loc['user_123']"
```

The mapping is clean because Tally's keyed streams ARE implicitly grouped tables — every operator already computes per-key. The DataFrame API just makes this explicit.

---

## Operator Mapping: DataFrame to Tally

| DataFrame Operation | Tally Equivalent Today | New Operator? | Notes |
|---|---|---|---|
| `df["col"].sum()` | `st.sum("col", window=)` | No | Needs window param; could default to "forever" |
| `df["col"].count()` | `st.count(window=)` | No | |
| `df["col"].mean()` | `st.avg("col", window=)` | No | |
| `df["col"].min()` | `st.min("col", window=)` | No | |
| `df["col"].max()` | `st.max("col", window=)` | No | |
| `df["col"].nunique()` | `st.distinct_count("col", window=)` | No | Approximate (HLL) |
| `df["col"].last()` | `st.last("col")` | No | |
| `df["a"] + df["b"]` | `st.derive("a + b")` | No | Expression objects replace strings |
| `df["a"] > 5` | `where="a > 5"` | No | Boolean expr for filter |
| `df.filter(expr)` | `where=` on operators | No | Stream-level filter exists via `filter=` |
| `a.join(b, on="key")` | `@st.view` + `st.lookup` | No | Syntax sugar needed |
| `df["col"].rolling(N).mean()` | `st.avg("col", window=)` | No | Conceptual match |
| `df["col"].shift(1)` | -- | **YES: lag** | Needs ring buffer of recent values |
| `df["col"].std()` | -- | **YES: stddev** | Sum-of-squares bucketed ring buffer |
| `df["col"].var()` | -- | **YES: variance** | Same as stddev (just skip sqrt) |
| `df["col"].ewm(span=N).mean()` | -- | **YES: ema** | O(1) state, exponential decay |
| `df["col"].quantile(0.95)` | -- | Future: t-digest | Bounded sketch, ~1KB |
| `df["col"].diff()` | -- | Derive from lag | `lag(col, 1)` then subtract |
| `df["col"].pct_change()` | -- | Derive from lag | `(col - lag(col, 1)) / lag(col, 1)` |
| `df["col"].cumsum()` | -- | `st.sum(window="forever")` | No new operator |
| `df["col"].apply(fn)` | -- | Not planned | UDFs run in Python = hot path violation |
| `df.groupby("col").agg(...)` | Keyed stream IS this | No | The key field is the groupby |

### Gap Analysis

**Must-have for DataFrame parity (3 operators):**
1. `lag(field, n)` — store last N values per key. O(N) state. Enables `.shift()`, `.diff()`, `.pct_change()`.
2. `stddev(field, window)` — online Welford algorithm over bucketed ring buffer. O(window/bucket) state.
3. `ema(field, span=N)` — exponential moving average. O(1) state per key. Already identified in HORIZON-SURVEY.md as a priority.

**Nice-to-have (2 operators):**
4. `quantile(field, q, window)` — t-digest sketch. ~1KB per operator per key.
5. `first(field)` — complement to `last()`. Trivial implementation.

---

## Proposed API Design

### Recommended: Form B — Bracket Assignment (Pandas-like)

This form was chosen because:
- Matches the user's stated vision (`a["test"] += 5`, `a.filter(...)`)
- Most intuitive for anyone who has used Pandas
- `__setitem__` / `__getitem__` are the simplest Python dunder methods to implement
- Expression trees via operator overloading are well-understood (Polars, SQLAlchemy)

#### Keyed Stream (Table)

```python
import tally as st

# Create a table (keyed stream)
txns = st.table("Transactions", key="user_id")

# Event-field aggregations — column access returns a Column proxy
txns["tx_count_1h"]  = txns.count(window="1h")                    # count all events
txns["tx_count_30m"] = txns.count(window="30m", where="status == 'failed'")
txns["tx_sum_1h"]    = txns["amount"].sum(window="1h")             # sum a specific field
txns["avg_amount"]   = txns["amount"].mean(window="1h")            # avg
txns["max_amount"]   = txns["amount"].max(window="24h")            # max
txns["unique_merch"] = txns["merchant_id"].nunique(window="24h")   # distinct_count (HLL)
txns["last_country"] = txns["country"].last()                      # last value

# Derived features — operator overloading builds expression trees
txns["velocity"]      = txns["tx_count_1h"] / 24
txns["failure_rate"]  = txns["tx_count_30m"] / txns["tx_count_1h"]
txns["amount_vs_avg"] = txns.event["amount"] / txns["avg_amount"]  # _event.amount access

# New operators (require server-side implementation)
txns["prev_amount"]   = txns["amount"].lag(1)                      # lag/shift
txns["amount_change"] = txns["amount"] - txns["prev_amount"]       # derive from lag
txns["amount_std"]    = txns["amount"].std(window="1h")            # stddev
txns["amount_ema"]    = txns["amount"].ema(span=10)                # exponential MA
```

#### Keyless Stream (Event Stream)

```python
# Create an event stream (no key — not grouped)
raw = st.stream("RawEvents")

# Filter creates a derived stream
failed = raw.filter(raw["status"] == "failed")

# Keyless streams can feed keyed streams
failed_txns = st.table("FailedTransactions", key="user_id", source=failed)
failed_txns["count_1h"] = failed_txns.count(window="1h")
```

#### Cross-Stream Views (Joins)

```python
logins = st.table("Logins", key="user_id")
logins["login_count_1h"] = logins.count(window="1h")

# Join tables on shared key — creates a View
risk = txns.join(logins, on="user_id")
risk["tx_to_login"] = txns["tx_count_1h"] / logins["login_count_1h"]
risk["suspicious"]  = (txns["tx_count_1h"] > 10) & (logins["login_count_1h"] < 2)

# Cross-key lookup
merchants = st.table("MerchantActivity", key="merchant_id")
merchants["chargebacks_24h"] = merchants.count(window="24h", where="type == 'chargeback'")

# Lookup: resolve merchant_id from the event to get merchant features
txns["merchant_cbacks"] = txns.lookup(merchants["chargebacks_24h"], on="merchant_id")
txns["high_risk"] = (txns["velocity"] > 3) & (txns["merchant_cbacks"] > 5)
```

#### Client Usage (unchanged)

```python
app = st.App("localhost:6400")
app.register(txns, logins, merchants, risk)

features = app.push(txns, {"user_id": "u123", "amount": 50.0, "status": "success"})
print(features["tx_count_1h"])
print(features["velocity"])
```

### Alternative Forms Considered

#### Form A: Method Chaining (Polars-like)

```python
txns = st.table("Transactions", key="user_id")
txns.add_feature("count_1h", st.count(window="1h"))
txns.add_feature("total", txns.col("amount").sum(window="1h"))
txns.add_feature("velocity", txns.col("count_1h") / 24)
```

**Pros:** Explicit, no magic. Easy to type-check.
**Cons:** Verbose. `add_feature()` is unfamiliar. Doesn't feel like a DataFrame.
**Verdict:** Rejected. Too verbose for the target audience.

#### Form C: Augmented Assignment (User's raw vision)

```python
a["test"] += 5  # What does this mean for a streaming table?
```

**Pros:** Extremely terse.
**Cons:** `+=` is ambiguous — does it mean "add 5 to the current value" (a mutation) or "define a feature that adds 5" (a derivation)? In a pipeline builder context, there is no "current value" at definition time. This breaks the lazy compilation model.
**Verdict:** Partially adopted. `+=` is not supported (too ambiguous), but the bracket-assignment syntax from Form C is the core of Form B. The user's `a.filter(...)` and `a.join(b)` syntax is adopted directly.

### Comparison Summary

| Criterion | Form A (Method) | Form B (Bracket) | Form C (Augmented) |
|-----------|-----------------|-------------------|--------------------|
| Familiarity | Low | High (Pandas) | Medium |
| Ambiguity | Low | Low | High (`+=`) |
| Python complexity | Simple | Medium | Hard |
| Backward compat | Easy | Easy | Easy |
| **Recommendation** | No | **Yes** | Partial |

---

## Implementation Architecture

### Core Principle: Zero Server Changes

The entire DataFrame API compiles to the same `RegisterRequest` JSON that `@st.stream` and `_to_register_json()` already produce. The proxy objects are SDK-only.

### Class Hierarchy

```python
class Table:
    """Proxy for a keyed stream. Created by st.table()."""
    _name: str
    _key_field: str | None
    _features: OrderedDict[str, OperatorBase]   # same type as today
    _filter: str | None
    _source: Table | None

    def __getitem__(self, name: str) -> Column:
        """Return a Column proxy for an event field or defined feature."""
        return Column(table=self, name=name)

    def __setitem__(self, name: str, value: OperatorBase | Expr):
        """Register a feature definition."""
        if isinstance(value, Expr):
            # Convert expression tree to st.derive("...") string
            self._features[name] = Derive(value.to_expr_string())
        elif isinstance(value, OperatorBase):
            self._features[name] = value
        else:
            raise TypeError(f"Cannot assign {type(value)} as a feature")

    def count(self, *, window: str, where: str | None = None) -> OperatorBase:
        return Count(window=window, where=where)

    def filter(self, expr: Expr) -> Table:
        """Create a filtered stream (maps to depends_on + filter=)."""
        new = Table(self._name + "_filtered", key=self._key_field)
        new._source = self
        new._filter = expr.to_expr_string()
        return new

    def join(self, other: Table, on: str | None = None) -> View:
        """Create a cross-stream view (maps to @st.view)."""
        key = on or self._key_field
        return View(left=self, right=other, key=key)

    def lookup(self, column: Column, on: str) -> OperatorBase:
        """Cross-key lookup (maps to st.lookup)."""
        target = f"{column.table._name}.{column.name}"
        return Lookup(target=target, on=on)

    def _to_register_json(self) -> dict:
        """Serialize to the same JSON as StreamMeta._to_register_json()."""
        d = {
            "name": self._name,
            "key_field": self._key_field,
            "features": [op.to_json(name) for name, op in self._features.items()],
        }
        if self._filter:
            d["filter"] = self._filter
        if self._source:
            d["depends_on"] = [self._source._name]
        return d


class Column:
    """Proxy for a column reference. Supports aggregation methods and operators."""
    table: Table
    name: str

    def sum(self, *, window: str) -> OperatorBase:
        return Sum(field=self.name, window=window)

    def mean(self, *, window: str) -> OperatorBase:
        return Avg(field=self.name, window=window)

    def max(self, *, window: str) -> OperatorBase:
        return Max(field=self.name, window=window)

    def min(self, *, window: str) -> OperatorBase:
        return Min(field=self.name, window=window)

    def nunique(self, *, window: str) -> OperatorBase:
        return DistinctCount(field=self.name, window=window)

    def last(self) -> OperatorBase:
        return Last(field=self.name)

    def std(self, *, window: str) -> OperatorBase:
        return StdDev(field=self.name, window=window)  # NEW operator

    def lag(self, n: int = 1) -> OperatorBase:
        return Lag(field=self.name, n=n)               # NEW operator

    def ema(self, *, span: int) -> OperatorBase:
        return Ema(field=self.name, span=span)         # NEW operator

    # Operator overloading — returns Expr nodes, not values
    def __add__(self, other) -> Expr:
        return BinOp("+", self._to_expr(), _wrap(other))

    def __sub__(self, other) -> Expr:
        return BinOp("-", self._to_expr(), _wrap(other))

    def __mul__(self, other) -> Expr:
        return BinOp("*", self._to_expr(), _wrap(other))

    def __truediv__(self, other) -> Expr:
        return BinOp("/", self._to_expr(), _wrap(other))

    def __gt__(self, other) -> Expr:
        return BinOp(">", self._to_expr(), _wrap(other))

    def __lt__(self, other) -> Expr:
        return BinOp("<", self._to_expr(), _wrap(other))

    def __eq__(self, other) -> Expr:
        return BinOp("==", self._to_expr(), _wrap(other))

    def __and__(self, other) -> Expr:
        return BinOp("and", self._to_expr(), _wrap(other))

    def __or__(self, other) -> Expr:
        return BinOp("or", self._to_expr(), _wrap(other))

    def _to_expr(self) -> Expr:
        return Ref(self.name)


class Expr:
    """Base class for expression tree nodes."""
    def to_expr_string(self) -> str:
        raise NotImplementedError

    # Forward all operators so exprs compose: (a + b) > 5
    def __add__(self, other) -> Expr: return BinOp("+", self, _wrap(other))
    def __sub__(self, other) -> Expr: return BinOp("-", self, _wrap(other))
    def __mul__(self, other) -> Expr: return BinOp("*", self, _wrap(other))
    def __truediv__(self, other) -> Expr: return BinOp("/", self, _wrap(other))
    def __gt__(self, other) -> Expr: return BinOp(">", self, _wrap(other))
    def __lt__(self, other) -> Expr: return BinOp("<", self, _wrap(other))
    def __eq__(self, other) -> Expr: return BinOp("==", self, _wrap(other))
    def __and__(self, other) -> Expr: return BinOp("and", self, _wrap(other))
    def __or__(self, other) -> Expr: return BinOp("or", self, _wrap(other))
    def __radd__(self, other) -> Expr: return BinOp("+", _wrap(other), self)
    def __rsub__(self, other) -> Expr: return BinOp("-", _wrap(other), self)
    def __rmul__(self, other) -> Expr: return BinOp("*", _wrap(other), self)
    def __rtruediv__(self, other) -> Expr: return BinOp("/", _wrap(other), self)


class Ref(Expr):
    """Reference to a feature or event field by name."""
    def __init__(self, name: str): self.name = name
    def to_expr_string(self) -> str: return self.name

class Literal(Expr):
    """A constant value."""
    def __init__(self, value): self.value = value
    def to_expr_string(self) -> str: return repr(self.value)

class BinOp(Expr):
    """Binary operation node."""
    def __init__(self, op: str, left: Expr, right: Expr):
        self.op, self.left, self.right = op, left, right
    def to_expr_string(self) -> str:
        return f"({self.left.to_expr_string()} {self.op} {self.right.to_expr_string()})"

def _wrap(x) -> Expr:
    """Wrap a Python literal into an Expr node."""
    if isinstance(x, Expr): return x
    if isinstance(x, Column): return x._to_expr()
    return Literal(x)
```

### Compilation Example

```python
txns = st.table("Transactions", key="user_id")
txns["count_1h"] = txns.count(window="1h")
txns["total"]    = txns["amount"].sum(window="1h")
txns["velocity"] = txns["count_1h"] / 24
```

Compiles to (identical to what `@st.stream` produces):

```json
{
  "name": "Transactions",
  "key_field": "user_id",
  "features": [
    {"name": "count_1h", "type": "count", "window": "1h"},
    {"name": "total", "type": "sum", "field": "amount", "window": "1h"},
    {"name": "velocity", "type": "derive", "expr": "(count_1h / 24)"}
  ]
}
```

### Event Field Access (`_event.amount`)

Tally's expression language supports `_event.field` for accessing the raw event payload. In the DataFrame API:

```python
txns.event["amount"]  # returns Column with name "_event.amount"
# Or equivalently, when used inside a derive:
txns["amount_vs_avg"] = txns.event["amount"] / txns["avg_amount"]
# Compiles to: derive("_event.amount / avg_amount")
```

The `Table.event` property returns a special `EventProxy` whose `__getitem__` prefixes names with `_event.`.

---

## New Operators Needed

### Priority 1: lag (shift)

| Property | Value |
|----------|-------|
| DataFrame equiv | `df["col"].shift(n)` |
| State | Ring buffer of last N values, O(N) |
| Server change | New operator type in `src/engine/operators.rs` |
| Enables | `.diff()`, `.pct_change()`, rate-of-change features |
| Effort | Small — similar to `Last` but keeps N values |

### Priority 2: stddev (standard deviation)

| Property | Value |
|----------|-------|
| DataFrame equiv | `df["col"].std()` |
| State | Welford online algorithm over bucketed ring buffer |
| Server change | New operator in `src/engine/operators.rs`, new variant in `OperatorState` |
| Enables | Anomaly detection (z-score = `(value - mean) / std`) |
| Effort | Medium — needs sum, sum_of_squares, count per bucket |

### Priority 3: ema (exponential moving average)

| Property | Value |
|----------|-------|
| DataFrame equiv | `df["col"].ewm(span=N).mean()` |
| State | Single f64 + timestamp, O(1) |
| Server change | New operator, already identified in HORIZON-SURVEY.md |
| Enables | Smoothed trend features, momentum indicators |
| Effort | Small — one f64 per key, trivial update formula |

### Total server-side effort for all three: ~2-3 days

---

## Backward Compatibility

The `@st.stream` decorator API and the new `st.table()` API can coexist perfectly:

1. **Both produce the same JSON.** `Table._to_register_json()` outputs the identical dict as `StreamMeta._to_register_json()`.
2. **Both work with `app.register()`.** The `App` class just calls `_to_register_json()` on whatever is passed.
3. **No migration needed.** Existing `@st.stream` code keeps working. New code can use either style.
4. **Interop.** A `@st.stream` class and a `st.table()` can reference each other in views and lookups — they produce the same server-side pipeline objects.

Implementation: add a duck-typing check in `App.register()` — if the argument has `_to_register_json()`, call it. Both `StreamMeta` and `Table` provide this method.

### Migration path

```python
# BEFORE (still works, no changes needed)
@st.stream(key="user_id")
class Transactions:
    tx_count_1h = st.count(window="1h")
    tx_sum_1h = st.sum("amount", window="1h")
    velocity = st.derive("tx_count_1h / 1")

# AFTER (new option, same result)
txns = st.table("Transactions", key="user_id")
txns["tx_count_1h"] = txns.count(window="1h")
txns["tx_sum_1h"] = txns["amount"].sum(window="1h")
txns["velocity"] = txns["tx_count_1h"] / 1
```

---

## Recommendation

**API form:** Form B (bracket assignment). It is the most natural for data scientists, matches the user's vision, and has clean Python implementation semantics.

**Server changes:** Zero required for the core DataFrame API. Three new operators (`lag`, `stddev`, `ema`) are recommended to close the most impactful DataFrame parity gaps, estimated at 2-3 days of Rust work.

**SDK implementation effort:** ~3-4 days. The proxy classes (`Table`, `Column`, `Expr`, `View`) are straightforward Python with operator overloading. The expression-to-string compiler is ~50 lines. Tests can verify JSON output matches `@st.stream` equivalents.

**Milestone scope:** Small. This is a v1.4 or v1.5 candidate, not a multi-week effort. The SDK work is independent of any server changes and can ship first with existing operators, then gain `lag`/`stddev`/`ema` when those land.

**Risk:** Low. The compilation target (RegisterRequest JSON) is stable and well-tested. The new API is additive — no existing code breaks. The only risk is API design bikeshedding (method names, whether `window` should be optional with a default, etc.), which should be resolved by prototyping with 3-4 real use cases before committing.
