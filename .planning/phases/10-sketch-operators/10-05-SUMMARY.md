# Plan 10-05 — Sketch operator wiring — SUMMARY

**Status:** DONE
**TDD trace:** test(10-05) → feat(10-05) interleaved per task

## What landed

- `Value::Json(serde_json::Value)` variant added to `row.rs` for `top_k` structured output (array of `{value, count}`).
- `AggKind` extended to 13 variants — 8 core + 5 sketch: `CountDistinct`, `Percentile`, `TopK`, `BloomMember`, `Entropy`.
- `SketchParams { percentile_q, top_k_k, bloom_capacity, bloom_fpr }` struct on `AggOpDescriptor` (optional, defaults).
- 5 wrapper structs in `agg_state.rs` (`CountDistinctStateWrap`, `PercentileStateWrap`, `TopKStateWrap`, `BloomMemberStateWrap`, `EntropyStateWrap`) — each handles field extraction + the per-op update/query.
- `AggOp` enum extended with 5 boxed sketch variants. `AggOp::new`, `update`, `query`, and `update_with_row` dispatch covers them.
- `AggOp::new_lifetime(kind, sketch_params)` extracted as a re-usable lifetime constructor (used by both `AggOp::new` and `WindowedOp::fresh_op`).
- `WindowedOp::new_with_params(kind, window_ms, sketch_params)` — sketch params persisted on the windowed wrapper so each tumbling-bucket re-init honors user-supplied `k`/`q`/`fpr`/`capacity`. Constructor panics for `BloomMember` (windowless-only) — defensive guard backing the register-time rejection.
- `WindowedOp::query` arms for the 5 sketches:
  - `Entropy`: merges `EntropyHistogram` across active buckets via `merge`.
  - `CountDistinct`, `Percentile`, `TopK`: returns the **most-recently-active bucket's** value (v0 simplification — see "v0 limitations" below).
  - `BloomMember`: unreachable (defensive `Bool(false)`).
- `output_type_for` extended:
  - `CountDistinct` → `I64`
  - `Percentile`, `Entropy` → `F64`
  - `TopK` → `Json`
  - `BloomMember` → `Bool`
- `agg_compile.rs`:
  - `parse_agg_kind` recognises 5 new op names (`count_distinct`, `percentile`, `top_k`, `bloom_member`, `entropy`).
  - `extract_agg_params` parses sketch kwargs (`q`, `k`, `expected_n`/`capacity`, `target_fpr`/`fpr`).
  - Sketch field-required check (all 5 sketches require a `field`).
  - Sketch-specific param validation:
    - `bloom_member` + `window=` → `ErrorCode::WindowNotSupported`
    - `percentile.q` ∉ (0, 1) → `ErrorCode::InvalidPercentileQ`
    - `top_k.k` ∉ (0, 1024] → `ErrorCode::InvalidTopKK`
    - `bloom_member.fpr` ∉ (0, 1) → `ErrorCode::InvalidBloomFpr`
- `register_validate.rs`: 4 new `ErrorCode` variants.
- `beava-server/src/register.rs::error_code_to_wire_str`: maps the 4 new error codes to their snake_case wire strings.
- `value_to_json` (both `feature_query.rs` and `registry_debug.rs`) passes through `Value::Json` natively.
- `EntityKey::from_row` rejects `Value::Json` group keys (defensive).

## Tests added

| Test file | Tests | Note |
|---|---|---|
| `crates/beava-core/src/row.rs::value_json_variant_exists` | +1 | red→green |
| `crates/beava-core/src/agg_op.rs::agg_kind_has_sketch_variants` | +1 | red→green |
| `crates/beava-core/src/agg_compile.rs::tests::rule11_*` | +7 | sketch op-name + sketch-param validation |
| `crates/beava-server/tests/phase10_sketch_smoke.rs` | +2 | end-to-end register/push/get + bloom-window rejection |

**Test count delta:** 687 → 698 (+11)

## v0 limitations (deferred to v0.1)

1. **bloom_member query placeholder**: returns `Value::Bool(true)` once the filter has at least one insertion (i.e. it's a non-empty signal, not a membership test). The full `bloom_member.test(value)` API needs a GET-with-arg endpoint design — deferred to v0.1.
2. **Windowed `count_distinct` / `percentile` / `top_k`**: query returns **most-recently-active bucket** rather than merging across buckets. Acceptable for v0 because:
   - Most fraud-shape queries hit the most recent activity bucket (recent-event bias).
   - HLL has `merge`, UDDSketch / TopK do not — true merge is a v0.1 task.
   - Entropy DOES merge across buckets (`EntropyHistogram::merge` from Plan 10-01).
3. **Custom HLL precision (`hybrid_precision` kwarg)**: AggOpDescriptor stores `bloom_capacity`/`bloom_fpr`/`top_k_k`/`percentile_q`. HLL precision is fixed at p=12 in `CountDistinctState::new(_)`. Plumbing custom `p` is v0.1+.
4. **windowed_member op (windowed Bloom)**: AGG-SKETCH-04 is explicitly windowless. Windowed bloom is a v0.1 ask if user demand surfaces.

## Gates

- `cargo fmt --all --check` — clean
- `cargo clippy --workspace --all-targets --all-features -- -D warnings` — clean
- `cargo test --workspace --features beava-server/testing -- --test-threads=1` — 698 passed
