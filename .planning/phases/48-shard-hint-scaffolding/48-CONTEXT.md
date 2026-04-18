# Phase 48: shard-hint-scaffolding - Context

**Gathered:** 2026-04-18
**Status:** Ready for planning

<domain>
## Phase Boundary

Wire `EventSource::shard_hint(&self, event: &Event) -> u32` through the TCP and HTTP push entry points so every event acquires a deterministic routing slot at dispatch time. At `N_SHARDS=1` the function is a no-op (always returns 0) — nothing else in Beava changes. Land a `hash(key)` micro-bench (criterion, under `benches/`) that establishes the <100 ns per-event budget. The SPSC-roundtrip micro-bench does **not** land in this phase — deferred to Wave 1 where the first `Shard` struct makes an end-to-end roundtrip measurable.

Ship-gate: 9-cell benchmark matrix within ±1% of committed v1.0-launch baseline (scaffolding must be observationally inert at N=1).

</domain>

<decisions>
## Implementation Decisions

### Shard-hint computation model

- **D-01:** `shard_hint` is a **routing function, not a propagated field.** It is computed at exactly one point (the ingest dispatcher, inside the TCP/HTTP parser path before any per-shard allocation) and used immediately to choose a shard inbox. It is **not** stored on the `Event` struct, not serialized on the wire, and not read by any downstream code after dispatch. Shard threads never see the shard_hint that routed an event to them.
- **D-02:** The upstream `shard_hint` mentioned in the design doc's fork/replica path (`OP_LOG_FETCH` metadata) is a **separate optimization hint** used only by the replica dispatcher to skip re-hashing when upstream_N == downstream_N. It is not the same routing value D-01 refers to and is NOT implemented in Wave 0 — that's a Wave 4 concern.

### Trait signature

- **D-03:** Add to `EventSource`:
  ```rust
  fn shard_hint(&self, event: &Event) -> u32;
  ```
  Default impl: hash the primary-key field of the event (first dataclass field at N=1; explicit `shard_key` at Wave 1+) via `ahash` (already re-exported in the tree — no new crate). Call site: inside the TCP + HTTP dispatcher, immediately before the event enters any shard routing. At `N_SHARDS=1`, the function SHOULD still be called in release builds (no conditional) — the caller computes `shard_hint % 1 == 0` and routes to shard 0. In debug builds we may add a `debug_assert_eq!(N_SHARDS, 1)` sanity check.
- **D-04:** `u32` return type is sufficient (max physical core count on any realistic 2026 box is well under 2³²). Do not widen to `u64` or use an associated type — keeps the trait object-safe and ergonomic for future sources (Kafka, replica log metadata) that return small integers.

### Hash function

- **D-05:** Use `ahash` (already re-exported by `tally` via the `state` module). Quality is adequate for non-adversarial key distributions, speed satisfies the <100 ns budget, and we avoid introducing a new crate. `fxhash` / `xxhash_rust` not evaluated further — can swap later if `shard_probe` surfaces distribution problems. Cross-arch determinism is not a Wave 0 concern (replica-fork consistency is a Wave 4 problem, resolved by "always re-hash on ingest" per Q4 of the design doc).

### Micro-bench location and cadence

- **D-06:** New `benches/shard_scaffold.rs` using `criterion`. Contains **one** bench for Wave 0: `bench_shard_hint` — measures `EventSource::shard_hint(&event)` time over representative event shapes (single-field string key, tuple two-field key, numeric primary key). Target: <100 ns p50 per invocation on the reference box. Criterion's statistical output gets committed to `benchmark/shard_scaffold/README.md` as the baseline for future waves.
- **D-07:** Cadence: **nightly CI** (new job in `.github/workflows/bench-nightly.yml`). Per-PR runs are advisory (`cargo bench` on the dev's box, not gated). The existing 9-cell matrix remains the per-PR hard regression gate. Rationale: criterion single-microbenchmarks have enough run-to-run variance on shared CI runners to false-alarm on every PR; nightly on a dedicated reference box gives signal without noise.
- **D-08:** SPSC-roundtrip bench is **deferred to Wave 1** (Phase 49). Wave 0 has no shard threads yet, so the <10 μs SPSC target can't be measured end-to-end honestly. Wave 1's `Shard` struct scaffold will produce the first real `listener → SPSC → shard → response` loop; that's where the bench belongs. TPC-INFRA-01 wording ("micro-benches for hash overhead AND SPSC roundtrip") is satisfied across Waves 0+1, not in Wave 0 alone.

### Scope guardrails (not implemented in Wave 0)

- **D-09:** `BEAVA_SHARDS` env var + `--shards` CLI → Wave 1 (TPC-INFRA-02).
- **D-10:** `Shard` struct, per-shard state store → Wave 1 (TPC-PERF-01).
- **D-11:** SPSC channels, core_affinity pinning, SO_REUSEPORT → Wave 2 (TPC-PERF-02/03/04).
- **D-12:** Prometheus `metrics` + labeled per-shard series → Wave 2 (TPC-INFRA-03/04).
- **D-13:** Python SDK `shard_key=` surface → Wave 1 (TPC-DX-01). Wave 0's default impl "hash the primary-key field" reads the field position from the already-registered stream definition; no SDK change.

### Claude's Discretion

- Exact criterion bench harness shape (single `Bencher::iter` vs `BenchmarkGroup` for multi-shape sweep): Claude picks per criterion idioms.
- Shape of the `EventSource` trait method's default-impl body (explicit `ahash::AHasher` vs `std::hash::Hasher` shim): Claude picks the one that tests out fastest against the <100 ns budget.

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Design + research (source of truth)
- `.planning/arch/TPC-SHARD-DESIGN.md` §2 "Shard-hint API" — trait signature, call-site location, fallback behavior. §"Goals" bullets 1 and 4 — "every event routed at the earliest possible point" + "sources emit a shard_hint."
- `.planning/arch/TPC-RESEARCH.md` §1.1 (tokio current_thread details for Wave 0's tokio path), §3 Q5 (shard_key locking doesn't affect Wave 0 directly but inform trait shape).
- `.planning/research/SUMMARY.md` §"Wave 0: Scaffolding" — minimum ship list + requires + ship-gate.
- `.planning/research/STACK.md` §3 + §4 — crate additions deferred to later waves; Wave 0 adds only `rstest` (dev-dep) and optionally `criterion` if not already in tree.
- `.planning/research/ARCHITECTURE.md` §1 "Module-level impact map" — lists `src/server/tcp.rs` and `src/server/http.rs` as the parser call sites.

### Requirements
- `.planning/REQUIREMENTS.md` TPC-INFRA-01 — the sole REQ owned by this phase.

### Existing code (integration points)
- `src/server/tcp.rs` — TCP listener + `handle_push_core_ex` (primary parser call site).
- `src/server/http.rs` — axum HTTP router + push handlers.
- `src/server/shard_probe.rs` — existing cross-shard contention probe (not modified in Wave 0, but the micro-bench complements it).
- `src/server/throughput.rs` — existing throughput harness.
- `Cargo.toml` — confirm `ahash`, `criterion` (dev-dep), `rstest` (dev-dep) presence.

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- `ahash` — already re-exported via existing state module; use its `AHasher` directly in the trait default impl.
- `src/server/shard_probe.rs` — infrastructure for measuring cross-shard contention already exists; Wave 0 doesn't modify it but Wave 2+ will extend it. Treat as a reference for `BEAVA_*` config plumbing.
- `benches/` directory — check whether criterion-backed `benches/*.rs` already exist; extend the pattern if yes, establish the pattern if no.

### Established Patterns
- Beava's existing trait methods return concrete scalar types (no associated types in public traits). Following that convention, `shard_hint` returns `u32` directly.
- Micro-benches that aren't part of the 9-cell matrix live under `benches/` or `benchmark/` — never in `tests/`.
- Existing `BEAVA_*` env vars are all read once at server startup from `std::env::var` — no runtime reconfiguration. Wave 0 doesn't add a new env var (deferred to Wave 1); follow the pattern then.

### Integration Points
- `handle_push_core_ex` (in `src/server/tcp.rs`) — single choke point for TCP push path.
- `push` / `push_batch` handlers (in `src/server/http.rs`) — HTTP push path entry points.
- The router-level "decide which shard inbox to feed" point is where `EventSource::shard_hint` is called. At N=1 this call is a no-op routing to the sole (today: non-existent) shard inbox; Wave 0 lands the call site but not the inbox.

</code_context>

<specifics>
## Specific Ideas

- **`shard_hint` is a routing function, not a field** (D-01, user clarification). This changes the scaffolding model compared to what a literal read of the design doc might suggest (threading a tuple `(event, shard_hint)` downstream). Downstream agents: design the trait and call site around a call-and-discard pattern, not a carry-along pattern.
- Wave 0 must be observationally inert at N=1. If the 9-cell matrix drifts by more than ±1% after Wave 0 lands, the scaffolding has a cost bug and the phase fails verification. Budget is tight deliberately.
- Phase 48 does NOT add `num_cpus`, `core_affinity`, `crossbeam-channel`, `metrics`, or `metrics-exporter-prometheus` to Cargo.toml. Those land in Waves 1–2 where they're first exercised.

</specifics>

<deferred>
## Deferred Ideas

- SPSC roundtrip micro-bench (<10 μs budget) → Wave 1 (Phase 49), alongside the first real `Shard` struct.
- `BEAVA_SHARDS` env + `--shards` CLI → Wave 1 (Phase 49), REQ TPC-INFRA-02.
- Hash-function swap (`ahash` → `fxhash`/`xxhash_rust`) if `shard_probe` surfaces distribution problems at N>1 → revisit in Wave 2 ship-gate.
- Upstream `shard_hint` fast-path for replica/fork → Wave 4 (Phase 52), REQ TPC-CORR-06.
- `shard_key=` Python SDK decorator surface → Wave 1 (Phase 49), REQ TPC-DX-01.

</deferred>

---

*Phase: 48-shard-hint-scaffolding*
*Context gathered: 2026-04-18*
