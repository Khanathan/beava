# Phase 59.7 Deferred Items

Out-of-scope items discovered during wave execution. Each entry notes the
wave it was surfaced in, the observed behavior, and why it's deferred.

## Pre-existing proptest regression: `typed_snapshot_v11_migration::roundtrip_typed_ringbuffers`

**Surfaced:** W3 verification run.

**Symptom:** The 50-case proptest in `tests/typed_snapshot_v11_migration.rs`
fails on seed 0 with a `last_drop: Some(TooOld) != None` diff on the
decoded `TypedRingBufferInlineStr` structure. Verified pre-existing by
running against `git stash` of the W3 changes — same failure.

**Cause:** The `last_drop` field is `#[serde(skip)]` on
`TypedRingBufferInlineStr` per the Phase 59.7 W2 SUMMARY ("matches
Value-path `RingBuffer<T>` convention"), but `PartialEq` is derived and
compares the skip-serialized field, so a ring that recorded a `TooOld`
drop pre-serialization no longer matches post-deserialization.

**Why deferred:** Bug predates W3 changes, is a local test-code vs
serde-skip interaction, and does not affect the ring buffer's runtime
semantics (the skip is observability-only per W2 key-decision 2). Fix
options — `#[serde(default)]` to restore last_drop, or a custom PartialEq
that ignores it — are squarely in the V11-snapshot testing surface, not
the W3 cascade walker scope.

## `TYPED_CASCADE_VALUE_FALLBACK` counter not referenced in pipeline.rs

**Surfaced:** W3 verification — plan Done-Gate expected
`grep -c 'TYPED_CASCADE_VALUE_FALLBACK' src/engine/pipeline.rs >= 1`.

**Symptom:** Count is 0. The `run_typed_direct_cascade_same_shard` walker
can fall back to the Value bridge (via `run_typed_enrich_cascade`) when
any downstream is cross-shard or has no typed impl, but the `pipeline`
module has no access to `ConcurrentAppState::typed_cascade_value_fallback`
(the counter landed in W0 on the server-side struct, not a module
static).

**Why deferred:** Threading `ConcurrentAppState` through the engine's
walker would widen every push_typed_on_shard call site. The counter is
designed to be bumped on the `ShardOp` dispatch arm where `state` is in
scope, which is W4's territory (the cross-shard dispatch path lives in
`src/shard/thread.rs`, not here). W3 preserves the walker's correctness
contract (falls back to Value) without instrumenting the fallback. W4
flips the remaining 3 parity tests GREEN and adds the fallback bump
there.

## Pre-existing Phase 60 salt sweep blocks `cargo test --lib`

**Surfaced:** W0 / W1 / W2 / W3 — recurring.

**Symptom:** 33 `E0063: missing field 'salt'` errors on
`StreamDefinition { .. }` literal call sites across `src/shard/`,
`src/engine/`, `tests/`.

**Status:** Fully documented in
`.planning/phases/59.6-typed-pipeline-records/deferred-items.md`. W3
sidesteps via `tests/typed_cascade_step_dispatch.rs` (integration test
binary). Phase 60 owns the closing sweep.
