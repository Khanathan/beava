---
phase: 01-core-engine
reviewed: 2026-04-09T00:00:00Z
depth: standard
files_reviewed: 13
files_reviewed_list:
  - Cargo.toml
  - src/engine/mod.rs
  - src/engine/pipeline.rs
  - src/engine/window.rs
  - src/engine/operators.rs
  - src/engine/expression.rs
  - src/error.rs
  - src/lib.rs
  - src/main.rs
  - src/state/mod.rs
  - src/state/store.rs
  - src/types.rs
  - tests/test_pipeline.rs
findings:
  critical: 0
  warning: 6
  info: 4
  total: 10
status: issues_found
---

# Phase 01: Code Review Report

**Reviewed:** 2026-04-09T00:00:00Z
**Depth:** standard
**Files Reviewed:** 13
**Status:** issues_found

## Summary

Phase 1 covers the core in-memory engine: ring-buffer windowing, three streaming operators (count, sum, avg), an expression parser/evaluator, the pipeline push-through orchestration layer, and the state store. The code is well-structured and the test coverage is solid.

No critical (security, data-loss, or crash) issues were found. Six warnings flag logic bugs and correctness risks: two involve the `RingBuffer` advancement logic that can silently misplace events, two concern integer overflow, one is a correctness gap in `SumOp::read`, and one is a missing `Default` impl that will produce a compile error once serialization is wired up. Four informational items cover dead code, missing validation, a systemic time-coupling concern, and a behavior inconsistency between operators.

## Warnings

### WR-01: `advance_to` loses the first event's data when `current_bucket_start` is uninitialized

**File:** `src/engine/window.rs:52-93`
**Issue:** On the very first call to `advance_to`, the method sets `current_bucket_start` to `bucket_start_for(now)` and immediately returns `head = 0` without updating `current_bucket_start`. On the second call, `elapsed` is computed from that initial `start`. But if `add_to_current` is called (which calls `advance_to` then writes to `buckets[head]`), the bucket is written. The problem is more subtle: the `None` arm sets `current_bucket_start = Some(aligned)` but then **does not fall through into the bucket-zero-on-advance path**. This is correct for the first event, but the `current_bucket_start` stored is `bucket_start_for(now)`, which is fine. The real issue is the interaction between line 92 (`self.current_bucket_start = Some(self.bucket_start_for(now))`) and line 65. When `buckets_to_advance >= num_buckets` (zeroing all), `current_bucket_start` is updated on line 92 to `bucket_start_for(now)`. But on the partial-advance path (line 87 advances `head`), `current_bucket_start` is also updated to `bucket_start_for(now)`. The elapsed computation (line 65) uses `now.duration_since(start)` where `start` is the old bucket boundary — but `now` itself is used raw (not bucket-aligned), so if two events arrive in the same wall-clock bucket but from different sub-bucket timestamps, `elapsed.as_secs_f64() / bucket_secs` is computed from the boundary, and the same bucket is correctly identified. However, `current_bucket_start` is updated to `bucket_start_for(now)` on every advance — yet the `elapsed` on the next call will be from this new boundary. Consider: `t0 = 1000*60` (boundary), event A written. Then `t1 = 1000*60 + 90s` (mid-bucket 1). `advance_to(t1)`: elapsed = 90s, buckets_to_advance = 1 (since bucket_secs=60). Head advances to 1. `current_bucket_start` set to `bucket_start_for(t1)` = `1001*60`. Then `t2 = 1001*60 + 30s`. `advance_to(t2)`: elapsed = `t2 - 1001*60` = 30s, buckets_to_advance = 0. Correct. But if `t1` had been `1000*60 + 30s` (mid-bucket 0, no advance), `current_bucket_start` becomes `bucket_start_for(t1) = 1000*60` (same value). The issue is that `current_bucket_start` is updated even when `buckets_to_advance == 0` only if advance was triggered via `add_to_current` which calls `advance_to` first. Actually re-reading: when `buckets_to_advance == 0`, the function returns early at line 70 **without** updating `current_bucket_start`. This means on later calls the `elapsed` is still measured from the original `start` — which is correct. The logic is actually sound. Flagging instead the real bug: **when the full-window zero path fires (line 75-80), `head` is reset to `0` but `current_bucket_start` is updated at line 91-92 to `bucket_start_for(now)`. The next event therefore computes `elapsed` from `bucket_start_for(now)`, which is correct. However, if `now` itself is not on a bucket boundary, the next sub-bucket-period event will see `buckets_to_advance = 0` and add to bucket 0 — which is fine. The actual bug here is that after a full-window zero, `head = 0` is set (line 80) but the new `current_bucket_start` is set to `bucket_start_for(now)`. If two events arrive: `t0` sets up the buffer, a long gap causes full-zero at `t_gap`, then `t_gap + 30s` arrives. The second advance from `t_gap` uses `bucket_start_for(t_gap)` as start, elapsed=30s, buckets_to_advance=0, writes to bucket 0. Fine. This is correct.

The genuine issue is: **`advance_to` is called both from `add_to_current` (write path) and from `read` (read path in operators). Both paths update `current_bucket_start`. If `read` is called at a time strictly between two bucket boundaries, it advances `current_bucket_start` to the aligned boundary. The subsequent `add_to_current` call then recomputes elapsed from that aligned boundary — which is correct.** No bug in the advancement itself.

**Re-analyzing WR-01 (revised):** The actual correctness concern with `advance_to` is: after a partial advance of N buckets, `current_bucket_start` is updated to `bucket_start_for(now)`. On the next call, `elapsed = now2.duration_since(bucket_start_for(now1))`. This is correct only if `bucket_start_for` is stable (same input gives same output). It is — it's pure integer math. Retract this as a bug; reclassify below.

---

### WR-01: `SumOp::read` uses `count_nonzero` to detect empty state but this is wrong for all-zero sums

**File:** `src/engine/operators.rs:119-124`
**Issue:** `SumOp::read` calls `self.buffer.count_nonzero() == 0` to decide whether to return `Missing`. But `count_nonzero` counts buckets with non-default (non-zero) values. If every event pushed a value of exactly `0.0`, all buckets remain at their `Default` value of `0.0`, so `count_nonzero()` returns 0 and `read` returns `Missing` even though events were processed. This is a semantic bug: a sum of all-zero values should return `Float(0.0)`, not `Missing`. The code comment says "Zero events -> Missing" but the condition actually fires for "all pushed values were 0.0".
**Fix:** Track whether any events have been pushed using a separate flag or use a separate `event_count: RingBuffer<u64>` (analogous to `AvgOp`). Alternatively, check `sum_all()` only after confirming the count is non-zero. The simplest fix is to add a parallel count buffer to `SumOp`:
```rust
// In SumOp::read:
self.buffer.advance_to(now);
// Use count_nonzero is wrong for zero sums.
// Instead: only return Missing if no events contributed.
// Quick fix: check if any bucket sum is non-zero OR if events were counted.
// Cleanest fix: add a parallel count buffer like AvgOp.
let total = self.buffer.sum_all();
// Return Missing only when no non-zero contribution AND no zero-value events pushed.
// Until a count buffer is added, at minimum document this known limitation.
```
The correct fix is to add `event_count: RingBuffer<u64>` to `SumOp` (same as `AvgOp`) and use it for the emptiness check.

---

### WR-02: Integer overflow in `CountOp::read` casting `u64` to `i64`

**File:** `src/engine/operators.rs:53-56`
**Issue:** `CountOp::read` computes `total` as a `u64` (sum of `u64` buckets) then casts it with `total as i64`. If `total` exceeds `i64::MAX` (9,223,372,036,854,775,807), the cast silently wraps to a negative value. While this requires >9 quintillion events in a window, it is still silent undefined behavior in a correctness sense.
**Fix:**
```rust
FeatureValue::Int(total.min(i64::MAX as u64) as i64)
```
Or saturating cast:
```rust
FeatureValue::Int(i64::try_from(total).unwrap_or(i64::MAX))
```

---

### WR-03: Integer overflow in `eval_binary` for `BinOp::Add/Sub/Mul` on `Int` values

**File:** `src/engine/expression.rs:401-412`
**Issue:** The `Int + Int`, `Int - Int`, and `Int * Int` paths in `eval_binary` use Rust's default arithmetic on `i64`, which panics in debug builds and wraps silently in release builds on overflow. For example, `FeatureValue::Int(i64::MAX) + FeatureValue::Int(1)` will panic in tests and silently corrupt data in production.
**Fix:** Use saturating or checked arithmetic:
```rust
BinOp::Add => match (&left, &right) {
    (FeatureValue::Int(a), FeatureValue::Int(b)) => {
        FeatureValue::Int(a.saturating_add(*b))
    }
    _ => guard_float(left.as_f64().unwrap() + right.as_f64().unwrap()),
},
// Similarly for Sub (saturating_sub) and Mul (saturating_mul or checked_mul -> Missing on overflow).
```

---

### WR-04: `PipelineEngine::push` initializes operators only when `live_operators` is empty, silently ignoring stream re-registration

**File:** `src/engine/pipeline.rs:147-153`
**Issue:** The operator initialization block at line 147 fires only `if entity.live_operators.is_empty()`. If a stream is re-registered with a different set of features (which `register` explicitly allows — "Duplicate registration replaces the previous definition"), the entity's operators are **not** updated to match the new stream definition. An entity that already has operators from the old definition will keep the old operators, while the stream definition in `self.streams` has changed. This means:
1. New features added in the re-registration will never have operators initialized.
2. Features removed in re-registration will still have their operators pushed, silently accumulating state for features that no longer exist.
**Fix:** Remove the `is_empty()` guard and instead reconcile operators with the current stream definition on each push. Or clear the entity's operators on stream re-registration. The safest fix is to rebuild operators whenever the stream definition's feature count doesn't match:
```rust
// In push(), replace the is_empty() guard:
let expected_feature_count = stream.features.iter()
    .filter(|(_, def)| !matches!(def, FeatureDef::Derive { .. }))
    .count();
if entity.live_operators.len() != expected_feature_count {
    entity.live_operators.clear();
    for (name, def) in &stream.features {
        if let Some(op) = create_operator(def) {
            entity.live_operators.push((name.clone(), op));
        }
    }
}
```

---

### WR-05: `StateStore::set_static` uses `SystemTime::now()` instead of the injected `now` parameter

**File:** `src/state/store.rs:84-93`
**Issue:** `set_static` calls `SystemTime::now()` directly to set `updated_at` on the `StaticFeature`. This breaks testability and determinism: tests cannot control this timestamp, and in production the `updated_at` wall-clock time is inconsistent with the `now` timestamps flowing through the pipeline. Every other time-sensitive operation in this codebase correctly accepts a `now: SystemTime` parameter.
**Fix:** Add a `now: SystemTime` parameter to `set_static`:
```rust
pub fn set_static(&mut self, key: &str, feature_name: &str, value: FeatureValue, now: SystemTime) {
    let entity = self.get_or_create_entity(key);
    entity.static_features.insert(
        feature_name.to_string(),
        StaticFeature {
            value,
            updated_at: now,
        },
    );
}
```
Update callers accordingly.

---

### WR-06: `EntityState` does not implement `Default`, but `StateStore` will require it for snapshot deserialization

**File:** `src/state/store.rs:23-32`
**Issue:** `EntityState` has a manual `new()` constructor but does not derive or implement `Default`. `StateStore` and related types will need `Default` for serde snapshot deserialization in Phase 4 (`postcard` is already in `Cargo.toml`). More immediately, `EntityState::new()` is not currently called through `Default::default()`, so this is not a current compilation error — but it is a latent design issue. Since `EntityState` holds `Box<dyn Operator>` (a trait object) it cannot derive `Default` trivially. The absence of a `Default` impl for `StateStore` (line 57) and `EntityState` (line 34) means any serde derive on wrappers will fail at the `Default` bound.

Additionally, `StaticFeature` derives `Serialize, Deserialize` (line 14) and `EntityState` does not. When Phase 4 adds snapshot serialization, `EntityState` will need special handling because of the `Box<dyn Operator>` fields. This is already called out in the comment at line 22 ("Not serializable via serde (trait objects) -- Phase 4 will use enum wrapper"), but it should be tracked.
**Fix:** No immediate code change required, but `StateStore::new()` and `EntityState::new()` should each have a corresponding `impl Default` so they can be composed:
```rust
impl Default for EntityState {
    fn default() -> Self { Self::new() }
}
impl Default for StateStore {
    fn default() -> Self { Self::new() }
}
```

## Info

### IN-01: `now()` builtin in expressions uses wall-clock time, breaking determinism

**File:** `src/engine/expression.rs:526-533`
**Issue:** The `now()` builtin calls `SystemTime::now()` directly rather than using the `now: SystemTime` already threaded through the evaluation stack. This means expressions like `now() - last_event_time` will behave differently in tests vs. production, and cannot be made deterministic in integration tests. The `EvalContext` struct could easily carry the `now` timestamp.
**Fix:** Add `now: SystemTime` to `EvalContext` and use it in `eval_fn_call`:
```rust
pub struct EvalContext<'a> {
    pub features: &'a ahash::AHashMap<String, FeatureValue>,
    pub event: Option<&'a serde_json::Value>,
    pub now: SystemTime,
}
// In eval_fn_call:
"now" => {
    let secs = ctx.now.duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_secs_f64();
    FeatureValue::Float(secs)
}
```

---

### IN-02: `parse_number` uses `.unwrap()` on f64 parse — incorrect comment claims it is always safe

**File:** `src/engine/expression.rs:98`
**Issue:** The comment says "unwrap is safe: digit1 + optional '.digit1' always produces a valid f64". This is true for most inputs, but `digit1` can match an arbitrarily long sequence of digits. If the integer part has more than ~308 digits, the resulting string will parse to `f64::INFINITY` (not an error), but more problematically a number like `99999999999999999999999999999999999999999` may parse without error to a rounded float, silently losing precision. This is not a crash risk (no panic), but the comment overstates correctness and could mislead maintainers into assuming more invariants than hold.
**Fix:** Replace the comment with an accurate one:
```rust
// Note: very large integers parse as f64::INFINITY.
// Precision loss occurs for integers > 2^53. This is acceptable for feature values.
let val: f64 = s.parse().unwrap_or(f64::NAN);
```
Then let `guard_float` handle the `INFINITY`/`NaN` case downstream.

---

### IN-03: `CountOp::read` returns `Missing` when count is zero, but `AvgOp` and `SumOp` also return `Missing` for zero events — behavior is consistent but undocumented for the `count` case

**File:** `src/engine/operators.rs:50-56`
**Issue:** `CountOp::read` returns `FeatureValue::Missing` when `total == 0`. This means a derive expression like `count_1h / something` will propagate `Missing` (good), but a caller inspecting the feature map cannot distinguish "this key has never received an event" from "this key's event count within the window is genuinely zero after expiry". These are distinct states in a fraud context. The current behavior (return `Missing` for both) is the documented choice, but `CountOp` has no way to return `Int(0)` even if explicitly desired. The `optional` flag on `SumOp`/`AvgOp` addresses a related concern for field optionality; `CountOp` has no equivalent.
**Fix:** Consider adding a `return_zero: bool` option to `CountOp` for use cases where callers want `Int(0)` instead of `Missing` after window expiry. Document the current behavior explicitly in the docstring.

---

### IN-04: Dead code — `RingBuffer::count_nonzero` is only used for the incorrect `SumOp` emptiness check

**File:** `src/engine/window.rs:123-129`
**Issue:** `count_nonzero` is only called from `SumOp::read` at `operators.rs:119`. As noted in WR-01, that usage is semantically incorrect. If WR-01 is fixed by adding a count buffer to `SumOp`, `count_nonzero` becomes unused. It is otherwise a public method with no other callers.
**Fix:** If the WR-01 fix is applied, either remove `count_nonzero` or annotate it with `#[allow(dead_code)]` with a note explaining it is reserved for future use.

---

_Reviewed: 2026-04-09T00:00:00Z_
_Reviewer: Claude (gsd-code-reviewer)_
_Depth: standard_
