# Phase 1: Core Engine - Research

**Researched:** 2026-04-09
**Domain:** Rust in-memory state store, bucketed ring buffer windowing, streaming operators, expression parsing/evaluation
**Confidence:** HIGH

## Summary

Phase 1 builds the foundational compute core of Tally: an in-memory state store backed by AHashMap, a bucketed ring buffer for sliding window aggregation, three core operators (count, sum, avg), and an expression evaluator using winnow's built-in Pratt parser. This phase has zero networking -- it is a pure Rust library tested entirely through unit and integration tests.

All key technology choices are locked decisions from STATE.md and CONTEXT.md: AHashMap for hashing, SystemTime for timestamps, winnow for parsing, thiserror for errors, postcard for future serialization (serde derives added now). The crate is a single binary crate named `tally` with a single `TallyError` enum.

**Primary recommendation:** Build bottom-up: types.rs first, then window.rs (ring buffer), then operators.rs (count/sum/avg using the ring buffer), then expression.rs (winnow Pratt parser), then state/store.rs (EntityState + state store), then engine/pipeline.rs (stream definition + push-through orchestration). Every module gets comprehensive unit tests before moving to the next.

<user_constraints>

## User Constraints (from CONTEXT.md)

### Locked Decisions
- Uniform 1-minute bucket granularity as default for all windows (30m = 30 buckets, 24h = 1440 buckets)
- Bucket granularity is configurable per-operator, with global default fallback
- Non-divisible window durations: round up bucket count
- No minimum window duration enforced -- any duration >= 1 bucket size is valid
- Multi-tier buckets deferred to v2
- FeatureValue variants: Float(f64), Int(i64), String(String), Missing
- Redis-strict type enforcement: errors on type violations (e.g. string field in sum -> push error), not silent Missing
- Fields used by operators are implicitly typed by the operator: sum("amount") means "amount" must be numeric when present
- optional=True flag on operators: absent field produces Missing without error. Without optional=True, absent field -> error on push
- count(window=...) needs no field -- always succeeds regardless of event shape
- last("field") accepts any type -- no numeric requirement
- No implicit type coercion beyond Int+Float->Float in arithmetic expressions
- Division-by-zero -> Missing (value-level concern, not type error)
- Zero events in window -> Missing (no events means no value, not 0)
- Single crate for v1 -- one binary, one test suite. Extract to workspace when Python FFI needs it
- Integration tests in tests/ dir, unit tests inline with #[cfg(test)] mod tests
- Single TallyError enum with thiserror -- variants for Parse, Type, Window, Expression, Protocol
- "Tally" naming everywhere from day one (not "Streamlet") per approved rename decision

### Claude's Discretion
- Exact Rust struct layouts and field naming within the patterns established by CLAUDE.md
- Ring buffer implementation details (VecDeque vs fixed array vs custom)
- Expression AST node structure
- Test fixture design and helper utilities

### Deferred Ideas (OUT OF SCOPE)
- Multi-tier buckets (fine-grained recent + coarse older) -- v2 optimization
- Schema evolution (add/remove features without reset) -- post-v1

</user_constraints>

<phase_requirements>

## Phase Requirements

| ID | Description | Research Support |
|----|-------------|------------------|
| ENG-01 | In-memory state store (HashMap<EntityKey, EntityState>) with live and static features | AHashMap 0.8.12 as drop-in HashMap replacement; EntityState struct with live_features and static_features sub-maps; serde derives for future snapshot support |
| ENG-02 | Sliding windows use bucketed ring buffer with configurable bucket granularity | Custom `RingBuffer<T>` struct with Vec<T> backing, head pointer, bucket_duration, and window_duration; 1-min default granularity; lazy expiration on read |
| ENG-03 | count operator tracks event count within a time window | Counter variant using `RingBuffer<u64>`; increments current bucket on push; sums non-expired buckets on read |
| ENG-04 | sum operator accumulates a numeric field within a time window | Sum variant using `RingBuffer<f64>`; extracts named field from event JSON; type-checks numeric; adds to current bucket |
| ENG-05 | avg operator computes running average of a numeric field within a time window | Avg variant using paired `RingBuffer<u64>` (count) + `RingBuffer<f64>` (sum); divide on read; returns Missing when count is 0 |
| ENG-06 | Expression evaluator parses derive/where expressions at registration time into AST | winnow 1.0.1 `expression()` Pratt parser; parse at registration into `Expr` enum AST; store AST in pipeline definition |
| ENG-07 | Expression evaluator supports arithmetic, comparison, boolean, field access, builtins | Pratt parser with binding power levels for each precedence tier; field access patterns (bare, dotted, `_event.`); builtin functions as prefix calls |
| ENG-08 | Expression evaluator returns Missing on division-by-zero or missing inputs | Evaluation returns `FeatureValue`; division checks denominator; Missing propagates through arithmetic; no panics |

</phase_requirements>

## Project Constraints (from CLAUDE.md)

- **Language:** Rust, single binary
- **Threading:** Single-threaded v1 (no locks, no contention)
- **State:** In-memory AHashMap (not std HashMap) -- locked decision from STATE.md
- **Time:** SystemTime (not Instant) -- client-supplied Unix timestamps must be comparable
- **Serialization:** postcard (not bincode) -- RUSTSEC-2025-0141 advisory on bincode
- **Expression parser:** winnow (evolved from nom) -- locked decision from STATE.md
- **Error handling:** thiserror with single TallyError enum
- **Naming:** "Tally" everywhere (not "Streamlet")
- **HyperLogLog:** Direct implementation in hll.rs (Phase 5, not Phase 1)
- **Project structure:** Single crate, src/ with engine/, state/, server/ modules

## Standard Stack

### Core

| Library | Version | Purpose | Why Standard |
|---------|---------|---------|--------------|
| ahash | 0.8.12 | Fast hash map (AHashMap) | 2-10x faster than SipHash; DOS-resistant via hardware AES; drop-in HashMap replacement [VERIFIED: crates.io API] |
| winnow | 1.0.1 | Expression parser (Pratt parsing) | Built-in `expression()` combinator for operator precedence; evolved from nom; stable 1.0 release [VERIFIED: crates.io API] |
| thiserror | 2.0.18 | Error type derivation | Standard Rust error handling; derive(Error) for enum variants [VERIFIED: crates.io API] |
| serde | 1.0.228 | Serialization framework | Required for postcard snapshots in Phase 4; add derives now to avoid retrofitting [VERIFIED: crates.io API] |
| serde_json | 1.0.149 | JSON event parsing | Events arrive as JSON; need to extract fields by name for operators [VERIFIED: crates.io API] |

### Supporting (needed for Phase 1 but not primary)

| Library | Version | Purpose | When to Use |
|---------|---------|---------|-------------|
| postcard | 1.1.3 | Binary serialization (Phase 4) | Add as dependency now with `features = ["use-std", "alloc"]`; add serde derives to all state types so Phase 4 is seamless [VERIFIED: crates.io API] |

### Alternatives Considered

| Instead of | Could Use | Tradeoff |
|------------|-----------|----------|
| ahash | rustc-hash (FxHashMap) | FxHashMap is faster for small keys but NOT DOS-resistant -- unacceptable for user-supplied entity keys [ASSUMED] |
| winnow expression() | Hand-rolled Pratt parser | More control but unnecessary -- winnow's built-in Pratt parser handles our exact use case |
| Vec-backed ring buffer | VecDeque | VecDeque adds overhead of double-ended semantics we don't need; a simple Vec with head index is more cache-friendly for fixed-size buffers [ASSUMED] |
| Vec-backed ring buffer | ringbuffer crate | External dependency for trivial data structure; our ring buffer needs custom time-bucket semantics anyway |

**Installation (Cargo.toml dependencies):**
```toml
[dependencies]
ahash = "0.8"
winnow = "1.0"
thiserror = "2.0"
serde = { version = "1.0", features = ["derive"] }
serde_json = "1.0"
postcard = { version = "1.1", features = ["use-std", "alloc"] }
```

## Architecture Patterns

### Recommended Project Structure (Phase 1 scope)
```
tally/
├── Cargo.toml
├── src/
│   ├── main.rs              # Placeholder (Phase 2 adds real entry point)
│   ├── lib.rs               # Crate root, re-exports
│   ├── types.rs             # FeatureValue, Timestamp, EntityKey, FeatureMap
│   ├── error.rs             # TallyError enum
│   ├── engine/
│   │   ├── mod.rs           # Engine re-exports
│   │   ├── pipeline.rs      # StreamDefinition, OperatorDef, pipeline registration
│   │   ├── operators.rs     # CountOp, SumOp, AvgOp -- operator trait + impls
│   │   ├── window.rs        # RingBuffer<T>, bucket time math
│   │   └── expression.rs    # AST types, winnow parser, evaluator
│   └── state/
│       ├── mod.rs           # State module re-exports
│       └── store.rs         # StateStore, EntityState, LiveFeature, StaticFeature
├── tests/
│   ├── test_operators.rs    # Integration tests for count/sum/avg
│   ├── test_window.rs       # Window expiration, bucket rollover
│   └── test_expression.rs   # Expression parse + evaluate end-to-end
```

### Pattern 1: FeatureValue Enum with Missing Propagation
**What:** A four-variant enum (Float, Int, String, Missing) that propagates Missing through arithmetic like Option, with strict type checking.
**When to use:** Every feature read, every expression evaluation, every operator output.
**Example:**
```rust
// Source: CONTEXT.md locked decisions
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum FeatureValue {
    Float(f64),
    Int(i64),
    String(String),
    Missing,
}

impl FeatureValue {
    /// Extract as f64, promoting Int to Float. Returns None for String/Missing.
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            FeatureValue::Float(f) => Some(*f),
            FeatureValue::Int(i) => Some(*i as f64),
            _ => None,
        }
    }
}
```

### Pattern 2: Ring Buffer with Time Buckets
**What:** A fixed-capacity Vec with a head pointer and timestamp-based bucket selection. Buckets are lazily expired on read (not background timer).
**When to use:** Every windowed operator (count, sum, avg).
**Example:**
```rust
// Source: CLAUDE.md architecture + CONTEXT.md decisions
pub struct RingBuffer<T: Default + Copy> {
    buckets: Vec<T>,           // Fixed-size, allocated once
    head: usize,               // Index of current (newest) bucket
    bucket_duration: Duration, // Default 60s (1 minute)
    window_duration: Duration, // e.g., 30m, 1h, 24h
    current_bucket_start: SystemTime, // Start time of the bucket at head
}
```
**Key insight:** On `advance_to(now)`, calculate how many bucket slots to skip, zero them out, and update `head`. This is O(buckets_skipped), not O(total_buckets). On read, iterate all buckets and sum non-expired ones.

### Pattern 3: Operator Trait
**What:** A trait that all operators implement, with `push()` to ingest an event field and `read()` to get the current value.
**When to use:** The pipeline engine calls `push` on each operator when an event arrives, then `read` to collect features.
**Example:**
```rust
pub trait Operator {
    /// Process an incoming event. Returns Ok(()) or type error.
    fn push(&mut self, event: &serde_json::Value, now: SystemTime) -> Result<(), TallyError>;
    /// Read the current aggregate value.
    fn read(&self, now: SystemTime) -> FeatureValue;
}
```

### Pattern 4: Expression AST with Pratt Parsing
**What:** Parse expression strings at registration time into an `Expr` enum. Evaluate at event time by walking the AST.
**When to use:** All `derive` and `where` expressions.
**Example:**
```rust
// Source: winnow docs + CLAUDE.md expression spec
#[derive(Debug, Clone)]
pub enum Expr {
    Literal(f64),
    StringLit(String),
    FieldAccess(FieldRef),       // "field_name" or "Stream.field" or "_event.field"
    BinaryOp { op: BinOp, left: Box<Expr>, right: Box<Expr> },
    UnaryOp { op: UnOp, operand: Box<Expr> },
    FnCall { name: String, args: Vec<Expr> },
}

#[derive(Debug, Clone)]
pub enum FieldRef {
    Local(String),               // "tx_count_30m"
    Qualified(String, String),   // "Transactions.tx_count_30m"
    Event(String),               // "_event.amount"
}

#[derive(Debug, Clone)]
pub enum BinOp {
    Add, Sub, Mul, Div,
    Gt, Lt, Gte, Lte, Eq, Neq,
    And, Or,
}

#[derive(Debug, Clone)]
pub enum UnOp {
    Not, Neg,
}
```

### Pattern 5: TallyError Enum
**What:** Single error enum covering all error domains with thiserror derive.
**When to use:** All fallible operations return `Result<T, TallyError>`.
**Example:**
```rust
// Source: CONTEXT.md locked decision
#[derive(Debug, thiserror::Error)]
pub enum TallyError {
    #[error("parse error: {0}")]
    Parse(String),
    
    #[error("type error: expected {expected}, got {got} for field '{field}'")]
    Type { field: String, expected: String, got: String },
    
    #[error("window error: {0}")]
    Window(String),
    
    #[error("expression error: {0}")]
    Expression(String),
    
    #[error("protocol error: {0}")]
    Protocol(String),
}
```

### Anti-Patterns to Avoid
- **Background expiration timers:** Do NOT use a separate thread or timer to expire buckets. Expire lazily on read/push. Background expiration adds complexity with zero benefit for correctness.
- **Storing computed values eagerly:** `derive` expressions must be evaluated on read, not cached on push. Caching creates stale-value bugs when upstream features change.
- **Using f64 for count:** Counters must use u64 to avoid floating-point accumulation errors at high event volumes. Only convert to f64 at the expression evaluation boundary.
- **Panicking on bad input:** Never panic on user-supplied data (malformed JSON, wrong types, division by zero). Always return TallyError or FeatureValue::Missing.
- **String-keyed operator lookup inside hot loop:** Store operator references in a Vec indexed by position in the stream definition, not in a HashMap keyed by feature name. HashMap lookup per operator per event is unnecessary overhead.

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| Hash map | Custom hash table | ahash::AHashMap | Proven, DOS-resistant, 2-10x faster than SipHash |
| Expression precedence parsing | Manual recursive descent with precedence | winnow `expression()` Pratt combinator | Built-in, tested, handles left/right associativity and binding power correctly |
| Error boilerplate | Manual Display + Error impl | thiserror derive | Eliminates 50+ lines of boilerplate per error enum |
| JSON parsing | Custom JSON tokenizer | serde_json::Value | Battle-tested, zero-copy capable, handles all edge cases |
| Serialization traits | Manual to_bytes/from_bytes | serde derive + postcard | Forward-compatible wire format, handles versioning |

**Key insight:** The only truly custom data structure in Phase 1 is the time-bucketed ring buffer. Everything else should use established crates.

## Memory Budget Analysis

**CRITICAL FINDING:** The 1-minute default bucket granularity creates a tension with the <5KB per key memory target for 24h windows.

### Per-operator memory with 1-minute buckets:

| Operator | 30m window | 1h window | 24h window |
|----------|-----------|-----------|------------|
| count (u64) | 30 * 8 = 240B | 60 * 8 = 480B | 1440 * 8 = 11,520B |
| sum (f64) | 30 * 8 = 240B | 60 * 8 = 480B | 1440 * 8 = 11,520B |
| avg (u64+f64) | 30 * 16 = 480B | 60 * 16 = 960B | 1440 * 16 = 23,040B |

### Example: 10 mixed operators from CLAUDE.md spec
Using the Transactions stream definition as reference (tx_count_30m, tx_count_1h, tx_count_24h, tx_sum_1h, avg_amount_1h, max_amount_24h, unique_merchants_24h, failed_tx_30m + derive/last):
- 3 counters (30m, 1h, 24h): 240 + 480 + 11,520 = 12,240B
- 1 sum (1h): 480B
- 1 avg (1h): 960B
- 1 max (24h -- Phase 5): 11,520B
- 1 distinct_count (24h -- Phase 5): ~12,288B
- 1 count with where (30m): 240B
- 2 derive: 0B (no state)
- 2 last: ~128B

**Phase 1 total (count/sum/avg only):** ~14KB for keys using 24h windows with 1-min buckets. This EXCEEDS the 5KB target.

**Mitigation (already decided):** Per-operator configurable bucket granularity. For 24h windows, use 5-minute or 15-minute buckets:
- count_24h with 15-min buckets: 96 * 8 = 768B (vs 11,520B)
- This brings the example down to ~3KB for Phase 1 operators

**Recommendation:** Implement the configurable granularity from day one. Document that 1-minute buckets for 24h windows cost ~11KB per operator per key. The default should be 1-minute for windows <= 1h, but tests should verify the per-operator override works correctly. [ASSUMED -- the exact default thresholds are Claude's discretion per CONTEXT.md]

## Common Pitfalls

### Pitfall 1: SystemTime Arithmetic Overflow
**What goes wrong:** `SystemTime::duration_since()` returns `Result`, not `Duration`. Subtracting a future time from a past time panics if you unwrap.
**Why it happens:** Client-supplied timestamps may be out of order, or system clock may drift.
**How to avoid:** Always use `checked_duration_since()` or `duration_since().unwrap_or(Duration::ZERO)`. Never unwrap SystemTime arithmetic without handling the error case.
**Warning signs:** Panic in test when processing events with decreasing timestamps.
[VERIFIED: Rust std::time docs]

### Pitfall 2: Ring Buffer Off-By-One in Bucket Index
**What goes wrong:** Events land in the wrong bucket, or the bucket count is wrong (e.g., 30-minute window gets 29 or 31 buckets).
**Why it happens:** Confusion between "number of buckets" and "number of bucket boundaries." A 30-minute window with 1-minute buckets needs exactly 30 buckets (indices 0-29), but the window spans bucket 0 through bucket 29 inclusive.
**How to avoid:** Bucket count = `ceil(window_duration / bucket_duration)`. Use consistent 0-based indexing. Write a dedicated test that pushes events at exact bucket boundaries and verifies counts.
**Warning signs:** Off-by-one in aggregated values at window edges.

### Pitfall 3: Stale Bucket Data After Time Gap
**What goes wrong:** If no events arrive for a long time (e.g., 2 hours), then a new event arrives, the ring buffer still contains old data from 2 hours ago that appears "current."
**Why it happens:** Lazy expiration only happens on push/read. If the advance logic doesn't zero out all intervening buckets, stale data leaks into aggregates.
**How to avoid:** On `advance_to(now)`, if the gap exceeds the full window duration, zero ALL buckets (the entire window has expired). If the gap is partial, zero only the skipped buckets. Test this explicitly with a "long gap" scenario.
**Warning signs:** Aggregates include events that should have expired.

### Pitfall 4: Floating-Point Precision in Avg
**What goes wrong:** After millions of events, sum accumulates floating-point error, making avg inaccurate.
**Why it happens:** f64 addition is not associative. Repeated addition of small values to a large accumulator loses precision.
**How to avoid:** Since we use bucketed windows (not global accumulators), precision loss is bounded by bucket count. A 1-hour window with 60 buckets sums at most 60 f64 values -- precision loss is negligible. This is a strength of the bucketed approach.
**Warning signs:** avg diverges from expected value after sustained high-volume ingestion.

### Pitfall 5: Expression Parser Ambiguity with Field Names
**What goes wrong:** The parser confuses field names with keywords. For example, `and_count` gets parsed as keyword `and` + `_count`, or `not_fraud` as `not` + `_fraud`.
**Why it happens:** Greedy keyword matching without checking that the keyword is a complete token (not a prefix of an identifier).
**How to avoid:** After matching a keyword (`and`, `or`, `not`), verify the next character is not alphanumeric or underscore. Use winnow's `terminated(literal("and"), peek(not(alphanumeric1)))` pattern or equivalent.
**Warning signs:** Parse errors on legitimate field names containing keywords as substrings.

### Pitfall 6: Missing Propagation in Boolean Expressions
**What goes wrong:** `tx_count_1h > 10 and login_count_1h < 2` should return Missing if either input is Missing, but naive implementation might treat Missing as falsy (0 or false).
**Why it happens:** Confusing "no value" (Missing) with "zero" or "false."
**How to avoid:** Define clear Missing propagation rules: any arithmetic on Missing returns Missing; any comparison with Missing returns Missing; `and`/`or` with Missing returns Missing (SQL NULL semantics, not JavaScript falsy semantics).
**Warning signs:** Derive expressions silently produce false/0 instead of Missing when upstream features have no data.

## Code Examples

### Ring Buffer Core Operations

```rust
// Source: Architecture patterns derived from CLAUDE.md spec + Rust std patterns
impl<T: Default + Copy + std::ops::AddAssign> RingBuffer<T> {
    pub fn new(window_duration: Duration, bucket_duration: Duration) -> Self {
        let num_buckets = (window_duration.as_secs() as f64 
            / bucket_duration.as_secs() as f64).ceil() as usize;
        Self {
            buckets: vec![T::default(); num_buckets],
            head: 0,
            bucket_duration,
            window_duration,
            current_bucket_start: SystemTime::UNIX_EPOCH, // Initialized on first event
        }
    }

    /// Advance time, zeroing expired buckets. Returns the index of the current bucket.
    pub fn advance_to(&mut self, now: SystemTime) -> usize {
        if self.current_bucket_start == SystemTime::UNIX_EPOCH {
            // First event: initialize
            self.current_bucket_start = self.bucket_start_for(now);
            self.head = 0;
            return self.head;
        }
        
        let elapsed = now
            .duration_since(self.current_bucket_start)
            .unwrap_or(Duration::ZERO);
        let buckets_to_advance = (elapsed.as_secs() / self.bucket_duration.as_secs()) as usize;
        
        if buckets_to_advance == 0 {
            return self.head;
        }
        
        let num_buckets = self.buckets.len();
        if buckets_to_advance >= num_buckets {
            // Entire window has expired; zero everything
            for b in self.buckets.iter_mut() {
                *b = T::default();
            }
        } else {
            // Zero only the skipped buckets
            for i in 1..=buckets_to_advance {
                let idx = (self.head + i) % num_buckets;
                self.buckets[idx] = T::default();
            }
        }
        
        self.head = (self.head + buckets_to_advance) % num_buckets;
        self.current_bucket_start += self.bucket_duration * buckets_to_advance as u32;
        self.head
    }

    /// Sum all buckets in the current window.
    pub fn sum_all(&self) -> T where T: std::iter::Sum {
        self.buckets.iter().copied().sum()
    }
    
    fn bucket_start_for(&self, time: SystemTime) -> SystemTime {
        let since_epoch = time.duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or(Duration::ZERO);
        let bucket_secs = self.bucket_duration.as_secs();
        let aligned = (since_epoch.as_secs() / bucket_secs) * bucket_secs;
        SystemTime::UNIX_EPOCH + Duration::from_secs(aligned)
    }
}
```

### Expression Parser with winnow Pratt Combinator

```rust
// Source: winnow 1.0.1 docs - expression() API [VERIFIED: docs.rs/winnow]
use winnow::prelude::*;
use winnow::combinator::{expression, Expression, Infix, Prefix, alt, delimited, dispatch};
use winnow::token::{any, literal};

// Binding power levels (higher = tighter binding)
// or: 1, and: 2, comparison: 3, add/sub: 5, mul/div: 7, unary: 10

fn parse_expr(input: &mut &str) -> Result<Expr> {
    expression(parse_operand)
        .prefix(parse_prefix_op)
        .infix(parse_infix_op)
        .parse_next(input)
}

// Operands: literals, field access, parenthesized expressions, function calls
fn parse_operand(input: &mut &str) -> Result<Expr> {
    alt((
        parse_number,
        parse_string_literal,
        parse_fn_call,
        parse_field_ref,
        delimited('(', parse_expr, ')'),
    )).parse_next(input)
}
```

### winnow Infix Operator Definition

```rust
// Source: winnow 1.0.1 expression() docs [VERIFIED: docs.rs/winnow]
use winnow::combinator::Infix::Left;

fn parse_infix_op(input: &mut &str) -> winnow::Result<Infix<impl FnOnce(Expr, Expr) -> Expr>> {
    dispatch! {preceded(space0, alt((
        literal("and"),
        literal("or"),
        literal(">="),
        literal("<="),
        literal("=="),
        literal("!="),
        literal(">"),
        literal("<"),
        literal("+"),
        literal("-"),
        literal("*"),
        literal("/"),
    )));
        "or"  => Left(1, |_, a, b| Ok(Expr::BinaryOp { op: BinOp::Or, left: Box::new(a), right: Box::new(b) })),
        "and" => Left(2, |_, a, b| Ok(Expr::BinaryOp { op: BinOp::And, left: Box::new(a), right: Box::new(b) })),
        ">="  => Left(3, |_, a, b| Ok(Expr::BinaryOp { op: BinOp::Gte, left: Box::new(a), right: Box::new(b) })),
        // ... etc
        "+"   => Left(5, |_, a, b| Ok(Expr::BinaryOp { op: BinOp::Add, left: Box::new(a), right: Box::new(b) })),
        "*"   => Left(7, |_, a, b| Ok(Expr::BinaryOp { op: BinOp::Mul, left: Box::new(a), right: Box::new(b) })),
        _ => fail,
    }
}
```

### Expression Evaluator with Missing Propagation

```rust
// Source: CONTEXT.md decisions on Missing semantics
pub fn eval(expr: &Expr, ctx: &EvalContext) -> FeatureValue {
    match expr {
        Expr::Literal(f) => FeatureValue::Float(*f),
        Expr::FieldAccess(field_ref) => ctx.resolve_field(field_ref),
        Expr::BinaryOp { op, left, right } => {
            let l = eval(left, ctx);
            let r = eval(right, ctx);
            eval_binary(*op, l, r)
        }
        Expr::UnaryOp { op: UnOp::Not, operand } => {
            match eval(operand, ctx) {
                FeatureValue::Missing => FeatureValue::Missing,
                FeatureValue::Int(0) | FeatureValue::Float(f) if f == 0.0 => FeatureValue::Int(1),
                FeatureValue::Int(_) | FeatureValue::Float(_) => FeatureValue::Int(0),
                _ => FeatureValue::Missing,
            }
        }
        // ...
    }
}

fn eval_binary(op: BinOp, left: FeatureValue, right: FeatureValue) -> FeatureValue {
    // Missing propagation: any Missing input -> Missing output
    if matches!(left, FeatureValue::Missing) || matches!(right, FeatureValue::Missing) {
        return FeatureValue::Missing;
    }
    
    match op {
        BinOp::Div => {
            let r = right.as_f64().unwrap(); // safe: already checked not Missing
            if r == 0.0 {
                FeatureValue::Missing  // Division by zero -> Missing
            } else {
                FeatureValue::Float(left.as_f64().unwrap() / r)
            }
        }
        BinOp::Add => {
            // Int + Int -> Int; anything with Float -> Float
            match (&left, &right) {
                (FeatureValue::Int(a), FeatureValue::Int(b)) => FeatureValue::Int(a + b),
                _ => FeatureValue::Float(left.as_f64().unwrap() + right.as_f64().unwrap()),
            }
        }
        // ... comparison ops return Int(0) or Int(1) for false/true
        _ => todo!(),
    }
}
```

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| nom for parsing | winnow 1.0 (successor to nom) | 2025-2026 | winnow 1.0 has built-in Pratt parser, no need for separate pratt crate |
| bincode for serialization | postcard 1.1 | 2025 (RUSTSEC-2025-0141) | bincode has security advisory; postcard is no_std compatible with stable wire format |
| thiserror 1.x | thiserror 2.0 | 2025 | Improved derive macros, same API surface |
| SipHash (std HashMap) | AHash (AHashMap) | Ongoing | 2-10x faster hashing for hash-heavy workloads |

**Deprecated/outdated:**
- bincode: Has RUSTSEC-2025-0141 advisory, unmaintained. Use postcard instead. [VERIFIED: STATE.md locked decision]
- nom: Superseded by winnow. nom is in maintenance mode. [ASSUMED]

## Assumptions Log

> List all claims tagged [ASSUMED] in this research.

| # | Claim | Section | Risk if Wrong |
|---|-------|---------|---------------|
| A1 | FxHashMap is NOT DOS-resistant for user-supplied keys | Alternatives Considered | Low -- AHashMap is locked anyway; this just explains why |
| A2 | Vec-backed ring buffer is more cache-friendly than VecDeque for fixed-size | Alternatives Considered | Low -- either works; VecDeque would add ~0 overhead in practice |
| A3 | nom is in maintenance mode, superseded by winnow | State of the Art | Low -- winnow is locked decision regardless |
| A4 | Default bucket granularity thresholds (1-min for <=1h) | Memory Budget Analysis | Medium -- if users expect 1-min for all windows by default, 24h windows will use 11KB+ per counter per key |

## Open Questions

1. **Boolean representation in FeatureValue**
   - What we know: CONTEXT.md specifies Float, Int, String, Missing. No Bool variant.
   - What's unclear: Should boolean expressions return Int(0)/Int(1)? This is the C/Redis convention.
   - Recommendation: Use Int(0) for false, Int(1) for true. This is consistent with the "Redis-strict" philosophy and avoids adding a fifth variant. Document this convention clearly.

2. **Event timestamp source**
   - What we know: SystemTime is locked for window buckets. Events are JSON.
   - What's unclear: Does the event JSON contain a timestamp field, or do we use SystemTime::now() on arrival?
   - Recommendation: For Phase 1 (no networking), accept `now: SystemTime` as a parameter to `push()`. This makes tests deterministic. Phase 2 can decide whether to extract from event JSON or use arrival time.

3. **String comparison in expressions**
   - What we know: Expression language has `==` and `!=` operators. FeatureValue has String variant.
   - What's unclear: Can expressions compare strings? e.g., `last_country == 'US'`
   - Recommendation: Support string equality (`==`, `!=`) but not ordering (`>`, `<` on strings). String in arithmetic -> Missing.

## Environment Availability

| Dependency | Required By | Available | Version | Fallback |
|------------|------------|-----------|---------|----------|
| Rust toolchain (rustc + cargo) | All compilation | **NOT FOUND** | -- | Must install via rustup |

**Missing dependencies with no fallback:**
- **Rust toolchain:** Not installed on this machine. Phase 1 cannot begin without it. Install via: `curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh` and then `source $HOME/.cargo/env`. Target Rust stable (currently 1.94.1). [VERIFIED: `which rustc` and `which cargo` both returned "not found"]

**Missing dependencies with fallback:**
- None. All other dependencies are Rust crates resolved by Cargo.

## Validation Architecture

### Test Framework

| Property | Value |
|----------|-------|
| Framework | Rust built-in test framework (libtest) |
| Config file | None needed -- `cargo test` discovers `#[test]` functions automatically |
| Quick run command | `cargo test` |
| Full suite command | `cargo test -- --include-ignored` |

### Phase Requirements -> Test Map

| Req ID | Behavior | Test Type | Automated Command | File Exists? |
|--------|----------|-----------|-------------------|-------------|
| ENG-01 | EntityState stores live and static features | unit | `cargo test state::store::tests` | Wave 0 |
| ENG-02 | Ring buffer expires old buckets, configurable granularity | unit | `cargo test engine::window::tests` | Wave 0 |
| ENG-03 | count operator tracks event count in window | unit + integration | `cargo test engine::operators::tests::test_count` | Wave 0 |
| ENG-04 | sum operator accumulates numeric field in window | unit + integration | `cargo test engine::operators::tests::test_sum` | Wave 0 |
| ENG-05 | avg operator computes running average in window | unit + integration | `cargo test engine::operators::tests::test_avg` | Wave 0 |
| ENG-06 | Expression parser produces AST from string | unit | `cargo test engine::expression::tests::test_parse` | Wave 0 |
| ENG-07 | Expression evaluator handles arithmetic, comparison, boolean, fields, builtins | unit | `cargo test engine::expression::tests::test_eval` | Wave 0 |
| ENG-08 | Division-by-zero and missing inputs return Missing | unit | `cargo test engine::expression::tests::test_missing` | Wave 0 |

### Sampling Rate
- **Per task commit:** `cargo test`
- **Per wave merge:** `cargo test -- --include-ignored`
- **Phase gate:** Full suite green before `/gsd-verify-work`

### Wave 0 Gaps
- [ ] `src/types.rs` -- FeatureValue, type definitions (needed by all tests)
- [ ] `src/error.rs` -- TallyError enum (needed by all tests)
- [ ] `Cargo.toml` -- project manifest with all dependencies
- [ ] `src/lib.rs` -- crate root with module declarations
- [ ] `src/main.rs` -- placeholder main (required for binary crate)

## Security Domain

Security enforcement is not explicitly disabled in config.json, so including this section.

### Applicable ASVS Categories

| ASVS Category | Applies | Standard Control |
|---------------|---------|-----------------|
| V2 Authentication | No | Phase 1 has no networking or user-facing auth |
| V3 Session Management | No | No sessions in pure engine phase |
| V4 Access Control | No | No access control in library-only phase |
| V5 Input Validation | **Yes** | serde_json for JSON parsing; strict type checking on operator fields; TallyError for invalid input |
| V6 Cryptography | No | No crypto operations in Phase 1 |

### Known Threat Patterns for Rust In-Memory Engine

| Pattern | STRIDE | Standard Mitigation |
|---------|--------|---------------------|
| Malformed JSON causing panic | Denial of Service | serde_json returns Result, never unwrap on user input |
| Hash flooding (many collisions) | Denial of Service | AHashMap with randomized keys (DOS-resistant by design) |
| Expression injection | Tampering | Expression language is limited (no I/O, no system calls); parsed into restricted AST |
| Integer overflow in counters | Information Disclosure / Integrity | u64 counters overflow after 1.8x10^19 events -- practically impossible in single-thread model |
| f64 NaN propagation | Integrity | Check for NaN after arithmetic; treat as Missing |

## Sources

### Primary (HIGH confidence)
- [crates.io API: ahash 0.8.12](https://crates.io/crates/ahash) - Version verified via API
- [crates.io API: winnow 1.0.1](https://crates.io/crates/winnow) - Version verified via API
- [crates.io API: thiserror 2.0.18](https://crates.io/crates/thiserror) - Version verified via API
- [crates.io API: serde 1.0.228](https://crates.io/crates/serde) - Version verified via API
- [crates.io API: serde_json 1.0.149](https://crates.io/crates/serde_json) - Version verified via API
- [crates.io API: postcard 1.1.3](https://crates.io/crates/postcard) - Version verified via API
- [docs.rs/winnow - Expression combinator](https://docs.rs/winnow/latest/winnow/combinator/fn.expression.html) - Pratt parser API verified
- [docs.rs/winnow - Expression struct](https://docs.rs/winnow/latest/winnow/combinator/struct.Expression.html) - Configuration API verified
- [Rust std::time::SystemTime](https://doc.rust-lang.org/std/time/struct.SystemTime.html) - duration_since pitfalls verified
- [Rust stable 1.94.1](https://blog.rust-lang.org/releases/latest/) - Latest stable version

### Secondary (MEDIUM confidence)
- [AHashMap docs](https://docs.rs/ahash/latest/ahash/struct.AHashMap.html) - Drop-in replacement API confirmed
- [Rust Performance Book - Hashing](https://nnethercote.github.io/perf-book/hashing.html) - AHash performance characteristics

### Tertiary (LOW confidence)
- None. All claims either verified or explicitly tagged [ASSUMED].

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH - all versions verified against crates.io API, all choices are locked decisions
- Architecture: HIGH - patterns derived directly from CLAUDE.md spec and CONTEXT.md decisions
- Pitfalls: HIGH - SystemTime pitfalls verified from Rust std docs; ring buffer and expression pitfalls from established domain knowledge
- Memory budget: HIGH - arithmetic is straightforward and verified against the spec's operator/window combinations

**Research date:** 2026-04-09
**Valid until:** 2026-05-09 (stable domain -- Rust crate versions may increment but APIs are stable)
