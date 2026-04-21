# Phase 60: 60-hotkey-mitigation-via-application-salting — Context

**Gathered:** 2026-04-20 (auto; plan-phase orchestrator, CONTEXT + 5 plans in one pass)
**Status:** Ready for planning
**Mode:** Auto (context-constrained; all decisions LOCKED in this file)

<domain>
## Phase Boundary

Phase 60 fixes the Zipf-1.2 hot-shard bottleneck exposed by the Phase 59 handoff:
shard-0 saturates at ~450K EPS under Pareto-80/20 workloads while shards 1–7 sit
idle. `/debug/shards` reports `inbox_depth=65536` on shard-0 vs `0` everywhere
else. A single shard's ceiling becomes the whole cluster's ceiling.

**Approach:** application-layer salting. Users declare `shard_key="user_id:salt(N)"`
on `@bv.stream`. At ingest, Beava appends `:0..:N-1` to the key's routing-only
derivation (not the stored entity key for writes that care about per-salt
isolation; see D-C2) so N hot events spread across N virtual sub-shards. On
read, scatter-gather across all salt variants; the operator's existing combine
semantics (sum, count, last-value-by-event_time) aggregate them correctly.

**Precondition satisfied by Phase 59:** the per-event TCP PUSH path is ~11%
lighter (JSON round-trip eliminated), leaving the shard thread idle enough to
absorb salt fan-out cost on the read path.

**Four structural changes land this phase:**

1. **Parser extension (Wave 1):** `shard_key` spec gains a `:salt(N)` suffix
   form. Parse into `StreamDefinition.salt_cardinality: Option<u8>`; same
   `ShardKeySpec` enum carries the base key. `N` clamped to `[2, 256]` and
   MUST be a power of 2.
2. **Ingest salting (Wave 2):** on PUSH, `derive_shard_idx` for a salted
   stream appends `:0..:N-1` based on `ahash(primary_event_id)` (D-C1). The
   suffix is applied to the routing hash only — the stored entity key uses
   the suffixed form so per-salt rows are physically distinct (this is what
   makes each salt a virtual sub-shard).
3. **Read-side scatter-gather (Wave 3):** `EnrichFromTable` + direct key
   reads + aggregation operators query all N salt variants when the driving
   key's stream declares salt. Same-shard fast path preserved when
   `salt_cardinality=1` (the default — no salting).
4. **Perf gate + hot-shard metric (Wave 4):** Pareto-80/20 Criterion bench
   (`benches/pareto_workload.rs`) extended to assert ≥ +50% EPS vs Phase 59
   baseline; `beava_shard_hot_key_owner_ratio` gauge exposes the top-1%
   key concentration per shard so operators can identify candidates.

**Out of scope (explicit):**
- Automatic salt selection — operators declare salt cardinality per stream.
- Salt rebalance at runtime — `salt_cardinality` is fixed at `register()`.
  Changing it requires re-registration + event-log replay (Phase 60 does not
  provide an in-place migration tool).
- Double-salting (D-G3 contingency C2 only — not default).
- HTTP / TCP wire-format changes. `shard_key="user_id:salt(16)"` travels as
  an opaque string inside the existing `StreamDefinition.shard_key` REGISTER
  JSON field; server parses server-side.
- Hot-shard auto-salt suggestion UI (future polish).
- Cross-shard JOIN salt propagation — if a stream declares salt and
  participates in a join, the join.on field MUST equal the salted key's base
  name; mixed salted/unsalted joins are explicitly rejected at `register()`
  (see D-D2).
- Phase 61 metrics hoist, Phase 62 allocator pooling, Phase 63 fjall tuning,
  Phase 64 Rust bench client.

</domain>

<decisions>
## Implementation Decisions (ALL LOCKED — no grey areas carried into plans)

### Area A — Salt Syntax + Cardinality

- **D-A1 (syntax):** `shard_key="user_id:salt(N)"` on single-key streams.
  Tuple form: `shard_key=("region", "user_id:salt(16)")` — salt attaches to
  the specific field that carries the hot-key skew (typically the tail
  field). Parser rejects salt on more than one tuple element (D-A5).
- **D-A2 (cardinality range):** `N ∈ {2, 4, 8, 16, 32, 64, 128, 256}`
  (powers of 2, lower bound 2 upper bound 256). Non-power-of-2 rejected at
  parse time with actionable error. Default (no salt suffix) = no salting.
- **D-A3 (storage):** `StreamDefinition.salt_cardinality: Option<u8>`. `None`
  means no salting (default). `Some(N)` means salt-N. Serialized to REGISTER
  JSON as `"salt_cardinality": N` (int) alongside the (unchanged) `shard_key`
  field which retains the user-declared string including `:salt(N)` for
  diagnostic display via `/debug/shards`.
- **D-A4 (SDK surface):** Python `@bv.stream(shard_key="user_id:salt(16)")`
  parses client-side to `{"shard_key": "user_id:salt(16)", "salt_cardinality":
  16}` in REGISTER payload. Server re-parses authoritatively; Python validation
  is advisory only (fail-fast on obvious mistakes like `salt(0)` or
  `salt(1000)`, but server is the source of truth).
- **D-A5 (tuple-with-salt):** At most one tuple element may carry `:salt(N)`.
  Multiple salted elements rejected at parse time:
  `"shard_key tuple may declare :salt(N) on at most one element; got N on both 'region' and 'user_id'"`.
- **D-A6 (rename):** The user-visible `shard_key` string is preserved verbatim
  for `/debug/shards` diagnostic display. Internal resolution splits
  base-key from salt-cardinality. `ShardKeySpec::Single(s)` stores the BASE
  key (e.g. `"user_id"`) — `:salt(N)` suffix lives exclusively in
  `salt_cardinality: Option<u8>`. Compat: Phase 49-04 snapshot loads with
  no `salt_cardinality` field treat the stream as unsalted via
  `#[serde(default)]`.

### Area B — Ingest Path Salting

- **D-B1 (suffix source):** `ahash(primary_event_id) % N` — deterministic
  across retries of the same event (idempotency for replicas). Event without
  an explicit `primary_event_id` field falls back to `ahash(event_json_bytes)`
  on the routing path only. DO NOT use `rand::random()` — that breaks
  determinism for replica fork ingest (TPC-CORR-06 requires rehash
  reproducibility).
- **D-B2 (suffix semantic):** The suffix produces a NEW entity key:
  `"<original_key>:<salt_index>"`. Storage key = derived key (physical
  separation across salts). Routing hash input = derived key. Salt is opaque
  in UI/metrics — operators see `inbox_depth` per physical shard, not per
  salt index.
- **D-B3 (suffix encoding):** Colon-delimited ASCII: `"<key>:<digit>"` where
  digit is `0..N-1`. Collision hazard with user keys already containing `:`:
  PREVENTED by a register-time validation that a salted stream's `key_field`
  MUST NOT produce keys containing `:`. If a sample event's key contains `:`
  at register time, `register()` errors with actionable message (D-G1).
  Rationale: source tables and joins already key-hash strings verbatim;
  re-using the same ASCII delimiter keeps the derivation trivially
  greppable and debuggable.
- **D-B4 (cascade behavior):** Downstream cascades re-shuffle by their OWN
  `key_field` (Phase 55 TPC-CORR-07 semantics preserved). Phase 60 does
  NOT propagate salting across cascades — a salted `Transactions` stream
  feeding a `MerchantActivity` table still hashes by `merchant_id` for the
  downstream, not by `user_id:<salt>`. If `MerchantActivity` itself needs
  salting, it declares its own `shard_key="merchant_id:salt(N)"`.
- **D-B5 (tuple + salt suffix):** For `shard_key=("region", "user_id:salt(16)")`,
  the derived routing key is `<region>|<user_id>:<salt_index>` (pipe
  delimiter matches `encode_group_by` semantics per `StreamDefinition.group_by_keys`).
  Salt suffix attaches to the final composite key, not to individual tuple
  parts.

### Area C — Read-Side Scatter-Gather

- **D-C1 (scope of read fan-out):** When a read operation targets a stream
  whose `salt_cardinality=Some(N)`, the read MUST issue N probe-reads
  (one per salt index) and combine results. Applies to:
  - `EnrichFromTable` right-side lookup (Phase 56 TPC-CORR-08 surface).
  - Direct `read_entity_from_shard` calls via `get_entity` read paths.
  - Aggregation operators' rollup queries (sum/count/last-value).
  Same-shard fast path retained: if `salt_cardinality=None` (or 1 by
  implicit default), issue ONE read exactly as today — zero overhead.
- **D-C2 (combine semantics):** Delegate to the operator's existing combine:
  - `sum`, `count`: commutative, combine by addition.
  - `last-value` / `any`: pick the result with the freshest `event_time`
    across salt variants; ties broken by salt index (deterministic).
  - `min`, `max`: natural min/max across N results.
  - `hll`, `cms`, `uddsketch`: already commutative-associative sketches —
    merge via existing `.merge()` methods.
  - Any operator that is NOT commutative-associative (e.g. a hypothetical
    "first-by-arrival-order") is explicitly UNSUPPORTED under salting;
    `register()` rejects such streams with salt declared (D-G2). For
    v1.2 this is a non-issue (no such operators exist).
- **D-C3 (fan-out mechanism):** Reuse Phase 56's `ShardOp::ReadEntityBatch`
  infrastructure — extend to carry `Vec<String>` keys per target shard where
  each key is one salt variant. Source-shard coalesces the N salt-variant
  lookups into per-target-shard batches. Same-shard salt hits stay inline.
- **D-C4 (fan-out cost ceiling):** Salting is opt-in precisely because the
  read-side fan-out is N× the baseline cost. If a user declares `salt(256)`
  for a read-heavy stream they pay 256× per read. This is a user-visible
  contract; documented in SDK docstring and `docs/architecture-tpc.md`.
  Phase 60 does not automate the tradeoff.

### Area D — Register-Time Validation

- **D-D1 (parse validation):** `parse_shard_key_with_salt(s: &str) -> Result<(ShardKeySpec, Option<u8>), ShardKeyParseError>` in `src/engine/join_validator.rs`. Returns error for:
  - `salt(0)`, `salt(1)` — below min cardinality.
  - `salt(N)` where N > 256.
  - `salt(N)` where N is not a power of 2.
  - Non-integer N (`salt(abc)`).
  - Multiple `:salt(...)` suffixes on one field.
  - Salt on both tuple elements.
  Error format matches Phase 51's `JoinShardKeyMismatch` / Phase 56's
  `CrossShardJoinWarning` tone: name the stream, name the offending input,
  show the fix.
- **D-D2 (join compatibility):** If a stream declares `salt_cardinality=Some(N)`
  and participates in a join where either side's `shard_key` does not also
  declare the same cardinality on the SAME field, `register()` EMITS a
  `SaltedJoinWarning` via the Phase 56 `/debug/warnings.cross_shard_joins`
  extension mechanism. This is a WARNING (not a reject) because joins
  already handle cross-shard fan-out via `ShardOp::SsjInsert` (Phase 56
  D-B1). Perf note flagged: "+N inbox hops per event on the salted side".
- **D-D3 (source-table salt rejection):** `@bv.source_table` streams CANNOT
  declare salt. Source tables are CDC-style keyed state — salting would
  break UPSERT idempotency (the same upstream row would hash to multiple
  derived keys non-deterministically). Rejected at `register()` with
  actionable error citing D-D3. Salt is strictly for `@bv.stream`.

### Area E — Metrics + Observability

- **D-E1 (new gauge):** `beava_shard_hot_key_owner_ratio{shard}` — fleet-wide
  metric reporting "percentage of events on this shard that came from the
  top-1% of keys by volume observed during a rolling 60-second window."
  Computed on a best-effort sampling basis (per-shard `LruCache<String, u64>`
  sized at `BEAVA_HOT_KEY_SAMPLE_SIZE` default 1024; ring-buffer reset every
  60s). NOT a correctness gate — purely an operator-facing diagnostic.
- **D-E2 (salt-aware /debug/shards):** `GET /debug/shards` response gains a
  per-shard `salted_streams: Vec<{name, salt_cardinality}>` field; operators
  inspecting the endpoint see which streams are splitting hot keys and at
  what cardinality. The existing `hot_shards` detection logic (Phase 51
  `BEAVA_HOT_SHARD_THRESHOLD=1.5`) is UNCHANGED — salt is an external
  mitigation, not a detection replacement.
- **D-E3 (counters):** Two new counters:
  - `beava_salt_fanout_reads_total{stream, salt_cardinality}` — read-side
    fan-out count (+1 per N-way scatter issued).
  - `beava_salt_ingest_writes_total{stream, salt_cardinality}` — write-side
    salted dispatches. Increments on every PUSH to a salted stream
    (regardless of salt index).
- **D-E4 (Prometheus labels):** Use existing `metrics` + `metrics-exporter-prometheus`
  crates (Phase 50). Pre-register labelsets at stream-register time to avoid
  per-event `metrics_util::Registry::get_or_create_counter` (which Phase 61
  will eliminate globally; Phase 60 just follows the pattern already established
  for shard-labeled metrics).

### Area F — Perf Gate Harness

- **D-F1 (harness):** Extend `benches/pareto_workload.rs` — it already
  establishes the Zipf-1.0 baseline at N_KEYS=10_000 with single-key streams.
  Phase 60 adds a second benchmark group `pareto-salted-c8-x8` that wraps
  the same Zipf sampler but routes through a salted `shard_key=("user_id:salt(16)")`
  declaration. Both groups run side-by-side so the +50% assertion is
  a direct A/B measurement on the same harness.
- **D-F2 (perf floor):** Pareto-salted group MUST exceed Pareto-unsalted
  by ≥ +50% aggregate EPS on the reference box. Phase 59 baseline (Pareto,
  `MODE=complex DURATION=60 CPUS=8 CLIENTS=8 BEAVA_SHARD_INBOX_SIZE=1048576
  BEAVA_MAX_CONNS_PER_SHARD=1024`) is the floor input; salted run must
  deliver ≥ 1.5× that number. Number captured in `60-PERF-GATE.md` at
  Wave 4 close.
- **D-F3 (steady-state inbox assertion):** Under the salted workload run
  (60s, CPUs=8, Pareto-80/20), `/debug/shards` polling every 5s MUST show
  every shard's `inbox_depth ≤ 0.5 × BEAVA_SHARD_INBOX_SIZE` in steady
  state. A single spike beyond is OK during ramp (first 10s); sustained
  breach for > 5 consecutive samples is a gate failure.
- **D-F4 (uniform-workload regression):** The standard 9-cell matrix run at
  N=CPU_COUNT with NO salted streams MUST be within ±2% of the Phase 59
  baseline. Salt is opt-in — zero-cost when unused. Guarded by an
  unsalted-path micro-bench in the same harness.
- **D-F5 (contingency ladder):**
  - **C1:** If salt(16) misses the +50% floor, retry with salt(64). If salt(64)
    passes, document and commit as the recommended default for Zipf-1.2 workloads.
  - **C2:** Double-salting — declare `salt_cardinality=16` plus a per-event
    micro-salt `:m(4)` that adds an additional 0..3 random suffix within the
    already-salted bucket. 4× more fan-out on read (64 variants total); only
    use if C1 fails.
  - **C3:** `human_needed` — surface the diagnostic on `/debug/shards`,
    document the miss in `60-VERIFICATION.md`, pair with Phase 61+ for a
    different angle (metrics hoist may free enough CPU to close the gap).

### Area G — Contingencies + Edge Cases

- **D-G1 (colon-in-key rejection):** At register time, if any sample event's
  resolved key_field value contains `:` and `salt_cardinality=Some(_)`,
  `register()` errors:
  `"Stream {name} has salt={N} but key_field {field} resolved a sample key containing ':'. Rename the key or remove salt. See docs/architecture-tpc.md § hot-key salting."`.
  Server collects the first 1 sample event via existing event-log
  lookahead (no new plumbing).
- **D-G2 (non-commutative operator rejection):** If a salted stream
  declares a feature backed by a non-commutative operator (reserved for
  future — none exist in v1.2), `register()` errors with the offending
  operator name. Current enumeration: all v1.2 operators pass, so this
  is a defensive future-proof check guarded by a `#[cfg(debug_assertions)]`
  sanity assertion only; no runtime check added to release builds.
- **D-G3 (DoS cap):** Salt cardinality range `[2, 256]` doubles as a DoS
  cap — a malicious REGISTER declaring `salt(2_000_000)` is rejected
  at parse (D-D1). Server does not allocate N-sized arrays based on
  untrusted input.
- **D-G4 (mixed salted/unsalted writes for same stream):** Cannot happen
  by construction — `salt_cardinality` is a stream-level attribute fixed
  at `register()`. Every write to a salted stream takes the salted path;
  every write to an unsalted stream takes the unsalted path. No per-event
  override is exposed.

### Claude's Discretion

- Pre-sizing of the per-shard `LruCache<String, u64>` for hot-key sampling
  (D-E1) — default 1024 is a starting point; benchmark may suggest 512 or
  2048. Pick whatever passes tests without measurable overhead.
- Format of the `SaltedJoinWarning` perf-note string (D-D2) — any
  descriptive sentence naming the stream, salt cardinality, and
  fan-out multiplier is acceptable.
- Internal naming: `SaltedJoinWarning` vs `SaltedStreamJoinWarning` vs
  other — pick whatever matches the existing `CrossShardJoinWarning`
  naming convention in `src/engine/join_validator.rs`.
- Order of fields in `StreamDefinition.salt_cardinality` declaration relative
  to `shard_key` — follow `#[derive(Default)]` alphabetical ordering used
  today.
- Exact text of `docs/architecture-tpc.md § hot-key salting` section — one
  subsection describing syntax, semantics, contingencies, and the
  read-fan-out tradeoff is sufficient.

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Phase 60 Source of Truth
- `.planning/ROADMAP.md` § Phase 60 — goal, success criteria, TPC-PERF-10
  requirement (NEW)
- `.planning/STATE.md` — Phase 59 baseline (1,494,631 EPS best-of-3 macOS,
  −1.3% below 1,514,095 floor within 6% run variance); Phase 59 Wave-4
  handoff notes on hot-key bottleneck
- `.planning/REQUIREMENTS.md` — TPC-PERF-10 row + traceability addition

### Architecture
- `.planning/arch/TPC-SHARD-DESIGN.md` — shard model baseline
- `.planning/arch/TPC-RESEARCH.md` — v1.2 research synthesis

### Phase 59 Handoff (directly consumed by Phase 60)
- `.planning/phases/59-binary-wire-format-for-push/59-CONTEXT.md` — wire
  format constants; ShardEvent.payload_fmt; payload passthrough
- `.planning/phases/59-binary-wire-format-for-push/59-PERF-GATE.md` —
  1,494,631 EPS best-of-3 baseline (Phase 60's +50% floor input)
- `.planning/phases/59-binary-wire-format-for-push/59-VERIFICATION.md` —
  SC-1..SC-5 format + acceptance pattern

### Phase 56 Primitives Reused (read-side fan-out)
- `.planning/phases/56-enrich-from-table-and-stream-stream-join-crossshard/56-CONTEXT.md`
  — `ShardOp::ReadEntityBatch` + per-target-shard coalesce contract
- `src/shard/thread.rs::ShardOp::ReadEntityBatch` — the batch-read primitive
  Phase 60 extends to carry salt-variant key lists

### Phase 55 Precedent (cascade shuffle semantics)
- `.planning/phases/55-stream-table-cascade-crossshard-and-source-tables/55-CONTEXT.md`
  — D-B4 preserves downstream-reshape behavior; Phase 60's salting does not
  cross cascade boundaries

### Phase 51 Precedent (scatter-gather + /debug/shards)
- `.planning/phases/51-cross-shard-queries-joins/51-03-PLAN.md` — `GET /debug/shards`
  schema + hot-shard detection (BEAVA_HOT_SHARD_THRESHOLD=1.5)
- `src/server/shard_probe.rs::ShardDiagnosticsReport` — the struct Phase 60
  extends with `salted_streams` field per D-E2

### Phase 49 Precedent (shard_key decorator surface)
- `.planning/phases/49-per-shard-state-store/49-04-PLAN.md` — Python
  `@bv.stream(shard_key=...)` wire integration; Phase 60 extends with
  salt-suffix parsing

### Benchmark Harness
- `benches/pareto_workload.rs` — existing Zipf-1.0 Criterion bench. Phase 60
  adds a `pareto-salted-c8-x8` group beside it.
- `benchmark/fraud-pipeline/run_bench.sh` — `MODE=complex DURATION=60 CPUS=8
  CLIENTS=8` harness used Phase 54 onward. Phase 60's perf gate runs this
  with a salted-stream variant of the fraud pipeline.

### Test Scaffolding
- `tests/sharding_parity.rs` — N=1↔N=8 proptest (Phase 52); extend with a
  salted-stream subcase at Wave 3.
- `tests/cross_shard_enrich_from_table.rs` — Phase 56 surface; extend with
  a salted right-side test at Wave 3.

### SDK Surface
- `python/beava/_stream.py::stream` — `@bv.stream(shard_key=...)` decorator
  (Phase 49-04); extend with salt-suffix parsing client-side (advisory only).
- `python/beava/_serialize.py:74-81` — REGISTER payload emission for
  `shard_key`; extend to include `salt_cardinality` field.

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets

- `src/engine/join_validator.rs::ShardKeySpec` — enum {Single(String), Tuple(Vec<String>)}.
  Phase 60 does NOT change this enum; salt lives in a sibling `salt_cardinality: Option<u8>`
  field on `StreamDefinition`.
- `src/engine/join_validator.rs::parse_shard_key_spec` — (will be added; currently
  the Python SDK strings come over the wire as `shard_key: str | [str, ...]`). Phase 60
  adds `parse_shard_key_with_salt` returning `(ShardKeySpec, Option<u8>)` from the raw
  string form.
- `src/engine/pipeline.rs::StreamDefinition.shard_key: Option<ShardKeySpec>` (line 404) —
  Phase 60 adds `pub salt_cardinality: Option<u8>` with `#[serde(default)]` for
  back-compat snapshot loads.
- `src/routing/shard_hint.rs::shard_hint_for_event` — Phase 60 adds a
  salt-aware variant `shard_hint_for_event_salted(event, key_field, salt_cardinality,
  primary_event_id)` that applies the salt suffix before hashing. Unsalted streams
  continue to use the existing function zero-cost.
- `src/shard/store.rs::shard_index_for_event` + `src/shard/store_fjall.rs::shard_index_for_event`
  — both call sites add a `salt_cardinality: Option<u8>` argument. When None, identical
  behavior; when Some(N), route to salted variant.
- `src/engine/pipeline.rs::derive_shard_idx` (multiple call sites around lines 1361,
  1368, 2206, 2328, 2473, 2662, 2733, 2739, 2957) — call sites thread through
  `stream.salt_cardinality`.
- `src/shard/thread.rs::ShardOp::ReadEntityBatch { target_shard, table_name, keys, reply }`
  — already carries `Vec<String>` keys. Phase 60's read-side fan-out BATCHES salt
  variants into this existing primitive. No new ShardOp variant needed.
- `src/server/shard_probe.rs::ShardDiagnosticsReport` — struct serialized for
  `/debug/shards`. Phase 60 adds `salted_streams` field.
- `src/shard/metrics.rs` — shard-labeled metric registration point. Phase 60 adds
  3 new metrics here (D-E1, D-E3).

### Established Patterns

- **`#[serde(default)]` for backward-compat new fields on snapshot structs** —
  pattern used at Phase 49-04 (`shard_key: Option<ShardKeySpec>`), Phase 57
  (`contributing_inputs: Option<ContribSet>`). Phase 60 reuses for
  `salt_cardinality: Option<u8>`.
- **REGISTER-time parse + structured warning via `/debug/warnings`** — Phase 51-04
  `JoinShardKeyMismatch` + Phase 56 `CrossShardJoinWarning`. Phase 60 adds
  `SaltedJoinWarning` (D-D2) through the same mechanism.
- **Opt-in zero-overhead defaults** — `salt_cardinality=None` is the common
  case; every code path checks `is_some()` before entering a salt-aware branch.
- **Grep-ZERO ship gate scripts** — Phase 54 pattern; Phase 60 adds
  `scripts/verify-salt-feature-complete.sh` that checks the 6 expected call-site
  updates to `derive_shard_idx` + read-fan-out sites.
- **Criterion side-by-side benchmark groups** — `benches/pareto_workload.rs`
  already uses `criterion_group!` + `BenchmarkId`. Phase 60 adds two more
  groups beside the existing one for direct A/B comparison.

### Hot-path touch summary

| File | Phase 60 change |
|------|-----------------|
| `src/engine/join_validator.rs` | add `parse_shard_key_with_salt` + `SaltedJoinWarning` |
| `src/engine/pipeline.rs` | add `salt_cardinality` to `StreamDefinition`; thread through 6 `derive_shard_idx` sites |
| `src/routing/shard_hint.rs` | add `shard_hint_for_event_salted` |
| `src/shard/store.rs` + `store_fjall.rs` | pass `salt_cardinality` into `shard_index_for_event` |
| `src/shard/thread.rs` | extend `ShardOp::ReadEntityBatch` usage (not shape) for salt fan-out; add hot-key LRU sampler |
| `src/shard/metrics.rs` | register 3 new metrics (D-E1, D-E3) |
| `src/server/shard_probe.rs` | add `salted_streams` to `ShardDiagnosticsReport` |
| `python/beava/_stream.py` | parse `:salt(N)` client-side; validate `N ∈ [2, 256]` power-of-2 |
| `python/beava/_serialize.py` | emit `salt_cardinality` in REGISTER payload |
| `benches/pareto_workload.rs` | add `pareto-salted-c8-x8` group |
| `tests/hot_key_salting.rs` (NEW) | RED→GREEN — parser + ingest + read |
| `tests/salted_stream_register_warning.rs` (NEW) | register-time warnings + rejections |
| `tests/sharding_parity.rs` | extend with salted-stream subcase |

</code_context>

<security>
## Threat Model

Trust boundaries touched by this phase:

| Boundary | Description |
|----------|-------------|
| SDK → REGISTER wire | Client-declared `shard_key` string with embedded `:salt(N)` suffix; parsed server-side. |
| PUSH → shard routing | Salt suffix derivation must be deterministic across retries and replica forks. |
| `/debug/shards` read → operator | New `salted_streams` field exposes stream-level config (no secrets). |

## STRIDE Threat Register

| ID | Category | Component | Disposition | Mitigation |
|----|----------|-----------|-------------|------------|
| T-60-01 | Denial of Service | `parse_shard_key_with_salt` | mitigate | Clamp N to [2, 256]; power-of-2 check; reject non-integer. Server never allocates N-sized structures from untrusted input (D-G3). |
| T-60-02 | Tampering | `ahash(primary_event_id)` salt derivation | accept | ahash is non-cryptographic; a malicious client can construct events that all hash to the same salt index (defeating salting for their own stream). No security impact — salt is a performance optimization, not an access control. Documented in D-B1. |
| T-60-03 | Information Disclosure | `/debug/shards.salted_streams` field | accept | Stream names + salt cardinality are already emitted via `/debug/warnings` and `list_streams`. No new secrets exposed. |
| T-60-04 | Tampering | Salt suffix producing key-collision with existing user keys containing `:` | mitigate | Register-time sample-event validation rejects key_field values containing `:` when salt is declared (D-G1). Existing streams (no salt) are unaffected. |
| T-60-05 | Correctness (boundary) | Mixed salted/unsalted writes for same stream | mitigate | By construction impossible — `salt_cardinality` is stream-level (D-G4). Per-event override not exposed. |
| T-60-06 | Denial of Service | Read-side N-way fan-out amplification | mitigate | Salt is opt-in (D-C4); max cardinality 256 bounds worst-case fan-out. Operators accept the tradeoff by explicit declaration. Client read-latency budget unchanged for unsalted streams. |
| T-60-07 | Spoofing | Salt suffix determinism under replica fork | mitigate | ahash is deterministic with fixed keys; D-B1 uses `primary_event_id` → same event on replica produces same salt. TPC-CORR-06 rehash invariant preserved. |
| T-60-08 | Repudiation | Salt selection observable in metrics | accept | `beava_salt_ingest_writes_total{stream, salt_cardinality}` provides audit trail; no new repudiation surface. |
</security>
</content>
</invoke>