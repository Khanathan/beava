# Online Feature Compute Operators — Horizon Research

**Date:** 2026-04-12
**Status:** Exploratory

## Executive Summary

Tally's current operator set covers ~60% of what production feature platforms offer. The critical gaps are:

1. **Percentile/quantile** — table stakes for fraud and risk scoring (z-score requires stddev; anomaly detection requires percentiles)
2. **Variance/stddev** — required for z-score normalization, a top-3 fraud feature pattern
3. **Exponential moving average (EMA/EWMA)** — O(1) memory, used everywhere for trend detection and decay-weighted features
4. **Top-K / last-N** — needed for "last 5 merchants" or "top 3 transaction countries" style features
5. **First/last with timestamp** — `last` exists but `first` and `last_n` do not

UDFs are **not table stakes** for v1. A richer expression language in `derive()` (adding string functions, conditionals, coalesce/null handling, math functions) can cover 80-90% of what teams use UDFs for. WASM UDFs are a strong v2 escape hatch — wasmtime call overhead is ~nanoseconds, instantiation is ~5 microseconds.

Session windows and CEP (complex event processing) are nice-to-have, not blockers. They serve niche use cases that most fraud/risk teams work around with fixed windows.

**One-line recommendation:** Add `stddev`, `percentile` (DDSketch), `ema`, `top_k`, and `last_n` — these five operators close the gap with Tecton/Chalk and cover 95% of production fraud/risk feature patterns.

## Production Platform Operator Survey

| Operator | Tecton | Chalk | Fennel | Chronon | Feast | Flink | RisingWave | Materialize | **Tally** |
|---|---|---|---|---|---|---|---|---|---|
| count | Y | Y | Y | Y | Y | Y | Y | Y | **Y** |
| sum | Y | Y | Y | Y | Y | Y | Y | Y | **Y** |
| avg/mean | Y | Y | Y | Y | Y | Y | Y | Y | **Y** |
| min | Y | Y | Y | Y | Y | Y | Y | Y | **Y** |
| max | Y | Y | Y | Y | Y | Y | Y | Y | **Y** |
| stddev | Y | Y | ? | ? | - | Y | Y | Y | **N** |
| variance | Y | Y | ? | ? | - | Y | Y | Y | **N** |
| percentile (approx) | Y | Y | ? | ? | - | Y | N(batch) | - | **N** |
| distinct_count | Y | Y | Y | Y | - | Y | Y | Y | **Y** |
| last | Y | Y | Y | Y | - | Y | Y | Y | **Y** |
| last_n | Y | Y | ? | Y | - | Y | Y | Y | **N** |
| first / first_n | Y | - | ? | Y | - | Y | Y | Y | **N** |
| last_distinct_n | Y | - | ? | ? | - | - | - | - | **N** |
| top_k (approx) | - | Y(weighted) | ? | - | - | Y | Y | - | **N** |
| EMA / exp decay | - | - | ? | - | - | UDF | - | - | **N** |
| derive / expr | - | Y(Python) | Y(Python) | - | Y(Python) | Y(SQL) | Y(SQL) | Y(SQL) | **Y** |
| lookup (cross-key) | - | Y | Y | Y | - | Y(join) | Y(join) | Y(join) | **Y** |
| where (filter) | Y | Y | Y | Y | Y | Y | Y | Y | **Y** |
| UDFs | Y(Python) | Y(Python) | Y(Python) | Y(Scala) | Y(Python) | Y(Java/Python) | Y(Python/Rust) | - | **N** |
| Session windows | - | - | Y | - | - | Y | Y | - | **N** |
| CEP/patterns | - | - | - | - | - | Y(MATCH_RECOGNIZE) | - | - | **N** |

**Key observations:**
- Tecton has the richest built-in operator set: count, sum, mean, min, max, var_pop, var_samp, stddev_pop, stddev_samp, approx_percentile, first(n), last(n), first_distinct(n), last_distinct(n), distinct count
- Chalk added weighted approximate top-K, vector aggregations, and array statistical functions in 2025
- Chronon (Airbnb/Stripe) focuses on count, sum, average, first_k, last_k with bucketed time windows
- Feast has minimal built-in aggregations — relies on external compute (Spark/Flink) for complex features
- Flink/RisingWave offer full SQL aggregation suites but are general-purpose stream processors, not feature servers

**UDF languages across platforms:**
- Tecton: Python (Pandas/PySpark) — runs in batch/streaming pipelines, not in hot serving path
- Chalk: Python resolvers — runs server-side
- Fennel: Python pipelines — compiled to execution graph
- Chronon: Scala — runs in Spark/Flink
- Flink: Java, Python, SQL — runs in JVM or external process
- RisingWave: Python, Rust UDFs — runs as external functions

## Fraud Detection Feature Patterns

Based on research into production fraud detection systems at major fintechs:

### Tier 1: Core velocity features (Tally covers these)
- Transaction count in 1m, 5m, 30m, 1h, 24h windows
- Transaction sum in same windows
- Average transaction amount in windows
- Max transaction amount in 24h
- Distinct merchant count in 24h
- Last country, last merchant, last device

### Tier 2: Statistical anomaly features (Tally gaps)
- **Z-score of current amount vs rolling mean/stddev** — requires `stddev` operator
- **Percentile rank of current transaction** — requires `percentile` operator
- **"Amount is > p99 of user's 30-day history"** — requires approximate percentile
- **Exponentially decayed transaction velocity** — requires `ema` operator
- **Ratio of current amount to EMA of past amounts** — requires `ema` + `derive`

### Tier 3: Behavioral pattern features (Tally partial coverage)
- **Last N merchants visited** — requires `last_n` operator
- **Top 3 transaction countries in 30 days** — requires `top_k` operator
- **"New merchant" flag** (merchant not in last_distinct_n) — requires `last_distinct_n`
- **Time since last transaction** — covered by `derive("now() - last_timestamp")`
- **Transaction amount vs user's historical percentile** — requires `percentile`

### Tier 4: Complex patterns (nice-to-have)
- Session-based features (transactions in current "session") — requires session windows
- Sequence detection ("failed then succeeded within 5m") — requires CEP
- Cross-entity graph features — out of scope for single-key model

### What fraud teams actually compute (from research papers and platform docs):

| Feature Pattern | Operators Needed | Tally Today |
|---|---|---|
| tx_count per window | count | Y |
| tx_sum per window | sum | Y |
| avg_amount per window | avg | Y |
| amount_zscore | stddev + derive | **N (need stddev)** |
| amount_percentile_rank | percentile | **N** |
| velocity_ema | ema | **N** |
| amount_vs_ema | ema + derive | **N** |
| distinct_merchants_24h | distinct_count | Y |
| last_5_merchants | last_n | **N** |
| top_3_countries | top_k | **N** |
| is_new_merchant | last_distinct_n + derive | **N** |
| time_since_last_tx | last + derive(now()) | Y |
| failed_tx_ratio | count(where) + derive | Y |
| high_risk_merchant_flag | lookup + derive | Y |

## Tally Gap Analysis

| Operator | Status | Priority | Effort | Can Derive/Approximate? |
|---|---|---|---|---|
| **stddev** | Missing | **P0 — Critical** | Medium (reuse sum+count bucket infra, add sum-of-squares) | No — requires sum of squares tracking |
| **percentile (approx)** | Missing | **P0 — Critical** | High (DDSketch or t-digest implementation) | No — fundamentally different data structure |
| **ema / exp_decay** | Missing | **P1 — High** | Low (O(1) state: single float + timestamp) | Partially — can approximate with multiple windows but wastes memory |
| **last_n** | Missing | **P1 — High** | Low (bounded ring buffer of values) | No — last only stores one value |
| **top_k** | Missing | **P2 — Medium** | Medium (Space-Saving or Count-Min Sketch + heap) | No — distinct_count only counts, doesn't track identities |
| **first** | Missing | **P2 — Medium** | Trivial (like last, but never overwrite) | No |
| **first_n** | Missing | **P2 — Medium** | Low (bounded list, stop accepting after N) | No |
| **last_distinct_n** | Missing | **P2 — Medium** | Medium (bounded set + ring buffer) | No |
| **variance** | Missing | **P3 — Low** | Trivial (derive from stddev: variance = stddev^2) | Yes — add as derive sugar over stddev |
| **session windows** | Missing | **P3 — Low** | High (timeout-based window lifecycle management) | Partially — use small tumbling windows as proxy |
| **CEP / patterns** | Missing | **P4 — Backlog** | Very High (state machine per entity) | No |
| **UDFs** | Missing | **P4 — Backlog** | High (WASM runtime integration) | Partially — richer derive expressions cover most cases |

### Memory cost per operator per key:

| Operator | State Size | Notes |
|---|---|---|
| stddev | Same as avg (sum + count + sum_sq buckets) | ~3x a count operator |
| percentile (DDSketch) | ~1-4 KB per sketch | Fixed size, configurable accuracy |
| ema | 8 bytes (f64) + 8 bytes (timestamp) | O(1) constant — cheapest possible |
| last_n(10) | ~10 * value_size | Bounded ring buffer |
| top_k(10) | ~10 * (key_size + counter) | Space-Saving algorithm |

## The UDF Question

### Do production systems need UDFs?

**Short answer: Not for v1.** The evidence from production platforms:

1. **Tecton** uses Python UDFs in batch/streaming pipelines (PySpark), but real-time serving uses pre-computed aggregations — no Python in the hot path.

2. **Chalk** runs Python "resolvers" server-side, but these are for data fetching and enrichment, not hot-path aggregation. Core aggregations are built-in.

3. **Flink** supports Java/Python UDFs but the best-performing deployments use built-in operators and SQL. UDFs add 10-100x latency per invocation (especially Python UDFs via serialization overhead).

4. **The pattern**: Platforms with UDFs use them for **data access** (call an external API, query a database) and **format transformation** (parse JSON, decode protobuf), not for mathematical aggregation.

### What UDFs are used for that built-in operators can't cover:

1. **String manipulation** — regex matching, normalization, parsing (e.g., extract domain from email)
2. **External lookups** — call a risk scoring API, check a blocklist
3. **Complex business logic** — multi-branch conditional logic too complex for simple expressions
4. **Custom encodings** — one-hot encoding, bucketing, binning
5. **ML model inference** — run a sub-model inside a feature pipeline

### Can Tally's `derive(expr)` replace most UDF needs?

With the current expression language (arithmetic, comparison, boolean, abs/min/max/now), Tally covers ~50% of UDF use cases. Adding these expression features would push to ~85%:

- **String functions**: `contains()`, `starts_with()`, `len()`, `lower()`, `substr()`
- **Conditional**: `if(cond, then, else)` or ternary operator
- **Null handling**: `coalesce()`, `is_null()`
- **Math**: `log()`, `exp()`, `pow()`, `ceil()`, `floor()`, `round()`
- **Casting**: `to_int()`, `to_float()`, `to_string()`
- **Date/time**: `hour_of_day()`, `day_of_week()` from timestamps

This covers string manipulation, custom encodings, and most complex business logic without any UDF runtime.

### WASM UDFs as an escape hatch (v2+)

For the remaining 15%, WASM UDFs provide a compelling middle ground:

**Performance characteristics (wasmtime):**
- Module instantiation: ~5 microseconds (down from ~2ms in older versions)
- Per-call overhead: single-digit nanoseconds
- Memory: sandboxed, configurable limits per module
- Languages: Rust, Go, C/C++, AssemblyScript compile to WASM; Python via experimental compilers

**Prior art:**
- **Redpanda**: WASM transforms for inline data processing in the broker. Uses JIT compilation, deployed alongside broker process.
- **SingleStore**: WASM UDFs and UDAFs (user-defined aggregate functions) via CREATE FUNCTION. Production-proven.
- **ScyllaDB**: WASM UDFs via wasmtime. Replaced Lua UDFs with WASM for better performance and sandboxing.

**Recommendation for Tally:**
- v1: No UDFs. Enrich the expression language instead.
- v2: Add WASM UDF support using wasmtime. User writes Rust (or any WASM-targeting language), compiles to .wasm, registers with Tally. Tally pre-compiles to native code on registration. Per-event invocation cost: <100ns.
- Never: Python UDFs in the hot path. Defeats the entire architecture.

## Priority Recommendations

If Tally adds 5 operators, in order:

### 1. `stddev(field, window)` — P0

**Why:** Z-score is the single most important fraud feature pattern. `amount_zscore = (event.amount - avg_amount_24h) / stddev_amount_24h` detects anomalous transactions instantly. Every serious fraud team computes this.

**Implementation:** Extend the existing bucketed ring buffer. Track `sum_of_squares` alongside `sum` and `count`. Stddev = sqrt((sum_sq/n) - (sum/n)^2). Same bucket expiration logic. Variance comes free as `stddev^2` via derive.

**Memory:** ~same as avg (one additional f64 ring buffer for sum-of-squares).

### 2. `percentile(field, window, p=0.99)` — P0

**Why:** "Is this transaction amount in the 99th percentile for this user?" is a core fraud signal. Also needed for SLA monitoring, anomaly detection, and risk scoring.

**Implementation:** DDSketch (not t-digest). Reasons:
- DDSketch: O(1) update, O(1) query, fully mergeable, relative error guarantees, ~1-4KB per sketch
- t-digest: higher accuracy but 20x slower updates, non-trivial merge semantics
- DDSketch fits Tally's "bounded memory per key" philosophy perfectly

**Memory:** ~1-4KB per sketch depending on configured accuracy.

### 3. `ema(field, half_life)` — P1

**Why:** Exponential moving average is the most memory-efficient trend feature. O(1) state (one f64 + one timestamp). Used for: trend detection, smoothed velocity, baseline computation, anomaly scoring. "Is current spending velocity above the exponentially-weighted historical average?"

**Implementation:** `ema_new = alpha * value + (1 - alpha) * ema_old * decay(time_delta, half_life)`. Single f64 state. No windows, no buckets. The `half_life` parameter controls the effective memory of the average.

**Memory:** 16 bytes per feature per key. The cheapest operator possible.

### 4. `last_n(field, n)` — P1

**Why:** "Last 5 merchants", "last 3 countries", "last 10 transaction amounts" are bread-and-butter fraud features. Enables pattern detection in derive expressions (e.g., "are all last 5 transactions from different countries?").

**Implementation:** Bounded ring buffer of `Value` entries. Max N configurable (cap at 100-1000 to bound memory). Returns JSON array.

**Memory:** O(N * value_size) per feature per key. For N=10, ~200-500 bytes.

### 5. `top_k(field, window, k)` — P2

**Why:** "Top 3 merchants by transaction count in 24h" or "most frequent transaction country in 7d". Less critical than stddev/percentile but enables a class of features that can't be approximated otherwise.

**Implementation:** Space-Saving algorithm (deterministic, bounded memory, exact for heavy hitters). Maintains k counters. On new value: if tracked, increment; if not, evict minimum and replace.

**Memory:** O(K * (key_size + counter)) per feature per key. For K=10, ~200-500 bytes.

## Advanced Patterns

### Session Windows
**What:** Group events by activity sessions (gap-based). A session ends when no event arrives for a configurable timeout (e.g., 30 minutes).
**Use case:** "Number of transactions in current session", "session duration", "events per session".
**Viability:** Medium effort. Requires per-key session state tracking (session_start, last_event_time). The main complexity is: what happens when a session expires mid-computation? Need a timer/eviction mechanism.
**Recommendation:** P3 — defer. Most teams use fixed 30m/1h windows as proxy. Add if customer demand materializes.

### Sliding Window Joins (stream-stream)
**What:** Join two event streams within a time window. "For each transaction, find the most recent login within the past 5 minutes."
**Use case:** Correlating events across streams for the same entity.
**Viability:** Tally already handles this via cross-stream views + lookup. A formal join operator would be syntactic sugar over existing lookup mechanics, not a new capability.
**Recommendation:** P4 — the existing view + lookup model covers the use case.

### Event Sequence Detection (CEP)
**What:** Detect patterns like "failed login, then password reset, then large transaction within 10 minutes."
**Use case:** Fraud pattern matching, compliance monitoring.
**Viability:** High effort. Requires per-key state machines, pattern compilation, timeout management. Flink's MATCH_RECOGNIZE is the gold standard but extremely complex.
**Recommendation:** P4 — defer. Most teams implement CEP in application logic consuming Tally features, not inside the feature server. The 80% solution: use `last_n` + `derive` to check recent event types.

### Time-Decay / EMA Features
**What:** Exponentially weighted aggregations where recent events matter more.
**Use case:** Trend detection, smoothed baselines, adaptive thresholds.
**Viability:** Low effort — EMA is the simplest possible stateful operator (see recommendation #3 above).
**Recommendation:** P1 — add as `ema(field, half_life)`. Could extend to `exp_decay_sum` and `exp_decay_count` later.

### Multi-Granularity Windows
**What:** Computing the same aggregation at 1m, 5m, 1h, 24h simultaneously.
**Use case:** Universal — every fraud model uses multiple window sizes.
**Viability:** Tally already supports this. Users define separate features per window:
```python
tx_count_5m = st.count(window="5m")
tx_count_1h = st.count(window="1h")
tx_count_24h = st.count(window="24h")
```
Each is an independent operator instance with its own bucket ring buffer.
**Recommendation:** Already covered. No work needed.

### Approximate Percentiles: DDSketch vs t-digest
**What:** Streaming quantile estimation.
**DDSketch:** Relative error guarantee (e.g., 1% relative error at any quantile). Fully mergeable. ~120-670 buckets depending on data distribution. Update: O(1). Query: O(log n) over buckets. Memory: 1-4KB.
**t-digest:** Better absolute accuracy at tail quantiles. Not trivially mergeable. Update: O(log n) amortized. Query: O(log n). Memory: ~2-8KB.
**Recommendation:** DDSketch for Tally. Reasons: simpler implementation, guaranteed error bounds, smaller memory footprint, O(1) updates align with low-latency goal. The `tdigest` crate exists in Rust but DDSketch's properties are better aligned with Tally's architecture.

---

## Summary of Recommendations

| Priority | Operator | Effort | Impact |
|---|---|---|---|
| P0 | `stddev(field, window)` | Medium | Unlocks z-score — #1 fraud feature |
| P0 | `percentile(field, window, p)` | High | Unlocks anomaly detection, risk scoring |
| P1 | `ema(field, half_life)` | Low | Trend detection, O(1) memory |
| P1 | `last_n(field, n)` | Low | Behavioral patterns, sequence features |
| P2 | `top_k(field, window, k)` | Medium | Frequency analysis, heavy hitters |
| v2 | WASM UDFs | High | Escape hatch for edge cases |
| v2 | Richer expressions | Medium | Replaces 80% of UDF needs |
| Defer | Session windows | High | Niche use case |
| Defer | CEP / patterns | Very High | Application-layer concern |
