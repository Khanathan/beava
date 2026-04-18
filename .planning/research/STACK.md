# TPC v1.2 — Stack Research

**Researched:** 2026-04-18
**Companion to:** TPC-SHARD-DESIGN.md + TPC-RESEARCH.md
**Scope:** Crate-level and tooling-level details for v1.2 implementation — complements existing research. Does NOT re-cover runtime landscape, channel shootout, or Python SDK patterns (those are in TPC-RESEARCH.md).

---

## 1. Crate version pins

| Crate | Current in Cargo.toml | Target version | Rationale | Breaking-change risk |
|---|---|---|---|---|
| `ahash` | `0.8` (SemVer range) | `0.8.12` (pin) | Already in tree; used in `shard_probe.rs` for `hash_one`. Tuple hashing works via standard `Hash` trait — `("region", "user_id")` tuples implement `Hash` so `RandomState::hash_one(&tuple)` is valid with no API changes. 0.8.x is stable; no breaking changes since 0.8.0. | LOW — no API changes in 0.8.x |
| `bytes` | `1.11` (SemVer range) | `1.11.1` (pin) | Already in tree. Zero-copy `Bytes` handle is the correct type for SPSC channel payloads — `Bytes::clone()` is O(1) refcount bump, not a copy. Wire format stable since 1.0; no breaking changes. | LOW |
| `crossbeam-channel` | Not in tree | `0.5.15` | Not currently a dependency. Add to `[dependencies]` (not dev-only). Bounded SPSC `crossbeam_channel::bounded(N)` is the Wave 0–2 channel primitive between listener and shard threads. The bounded form never blocks the producer (returns `Err` on full) which is what we want for backpressure. | LOW — mature, stable API since 0.5.0 |
| `num_cpus` | Not in tree | `1.17.0` | Not currently a dependency. Needed for `num_cpus::get_physical()` (default N_SHARDS in release builds per Q1). Works on Rust edition 2021 without issue. | LOW — API surface unchanged for years |
| `core_affinity` | Not in tree | `0.8.3` | Not currently a dependency. Provides `set_for_current()` for shard-thread pinning. 0.8.3 released Feb 2025. Best-effort on macOS/Apple Silicon (kernel silently ignores per-core pinning on aarch64 — not a crate bug, kernel limitation). | LOW |

**Red flag — three new runtime deps.** `crossbeam-channel`, `num_cpus`, and `core_affinity` are all absent from Cargo.toml today. They need to be added to `[dependencies]` (not `[dev-dependencies]`) because they are used in the production server path. None conflict with existing versions.

**No conflict risk on existing crates:** `ahash 0.8` and `bytes 1.11` are already present and the target pinned versions are within the existing SemVer ranges. `tokio 1.50` is already declared — `build_local()` was stabilized in tokio 1.x (introduced around 1.35); available at 1.50. If `build_local()` is absent for any reason, the fallback is `new_current_thread().build()` + `block_on()` with identical semantics for this use case.

---

## 2. Benchmark / test tooling

### 2.1 Property parity testing (N=1 vs N=K)

**Use `proptest`.** It is already in `[dev-dependencies]` at `"1.11"`. No addition needed. Strategy composition lets us generate arbitrary event streams with controlled key distributions, then assert feature-value equality between N=1 and N=K runs on the same event sequence — exactly what Wave 5's property parity gate requires.

Quickcheck is not in the tree. Proptest is strictly more capable for this use case: Strategies compose over types, constraints are first-class, and shrinking preserves structure. Do not add quickcheck.

### 2.2 Shard-count-parameterized tests

**Use `rstest`.** Not currently in the tree; add to `[dev-dependencies]` at `0.26.1` (current). `#[rstest]` with `#[case(1)]`, `#[case(4)]`, `#[case(8)]` generates one test per shard count with a single function body. This is the idiomatic Rust pattern for parameterizing integration tests by N_SHARDS without macro gymnastics.

```toml
# Add to [dev-dependencies]:
rstest = "0.26"
```

Alternative `test-case` crate has similar features but rstest is more widely adopted and includes fixture injection useful for shard-test setup/teardown.

### 2.3 Per-shard metrics (labeled gauges/counters)

**The `metrics` crate is NOT in the tree today.** Current version: `0.24.3`. It supports labeled metrics natively — `gauge!("beava_shard_reactor_utilization", "shard" => shard_id_str)` produces `beava_shard_reactor_utilization{shard="N"}` in Prometheus format when paired with `metrics-exporter-prometheus`.

All six labeled shard metrics from the design doc (§Q6) are expressible with this pair. `metrics-exporter-prometheus` 0.16.x is the current series compatible with `metrics` 0.24.x.

```toml
# Add to [dependencies]:
metrics = "0.24"
metrics-exporter-prometheus = "0.16"
```

The `metrics` crate is a thin facade — negligible overhead on the hot path via a global recorder. The existing debug endpoints (`/debug/shard_probe`, `/debug/throughput`) using manual `AtomicU64` and AHashMap-backed EWMAs can stay alongside it; they serve different consumer surfaces.

### 2.4 `oha` load tester

**External binary, not a Cargo dep.** Install via `cargo install oha` or `brew install oha`. Current version: 1.4.5. Already referenced for HTTP EPS measurement in v1.0-launch benchmarks. Directly reusable for TPC multi-shard load tests:

```bash
oha --no-tui -c 32 -n 2000000 -m POST \
    --body-file event.json \
    http://localhost:6401/push-batch/stream
```

The `--output-format json` flag produces machine-readable output for scripted regression gates. No Cargo.toml changes needed.

---

## 3. Runtime config surface

### 3.1 Existing pattern

The codebase uses raw `std::env::var` + `.parse().unwrap_or(default)` inline in `src/main.rs`. There is no `config.rs`, no `envy`, no `figment`, no `clap`-derived config struct. Every `BEAVA_*` env var is read at startup inside `start_server`. The `shard_probe.rs` module uses the same pattern via `init_from_env()`. This is the pattern to follow.

### 3.2 BEAVA_SHARDS integration

`BEAVA_SHARDS` follows the existing pattern exactly. Read once at startup, stored as `usize`:

```rust
// In start_server / main.rs startup block:
let n_shards: usize = if cfg!(debug_assertions) {
    std::env::var("BEAVA_SHARDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1)                               // debug default: 1 shard
} else {
    std::env::var("BEAVA_SHARDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(num_cpus::get_physical)     // release default: physical cores
};
```

This is consistent with every other `BEAVA_*` var in the tree. No config framework needed for v1.2.

### 3.3 cfg(debug_assertions) default

`cfg!(debug_assertions)` is a runtime boolean — evaluates to `true` in debug builds, `false` in release. The pattern above is correct and consistent with how the design doc (Q2) specifies the dev/release split. The key distinction: `cfg!` (macro) evaluates at runtime to a boolean; `#[cfg(...)]` (attribute) gates compilation of items. For startup config, the `cfg!` macro form is appropriate.

---

## 4. Snapshot format migration

### 4.1 Current serializer

**postcard** at `"1.1"` (current: 1.1.3, features `use-std` + `alloc`). The snapshot file layout is:

```
[version_byte: u8] [type_tag: u8] [postcard-encoded body: ...]
```

Current `SNAPSHOT_FORMAT_VERSION = 7`. `SnapshotHeader` contains `snapshot_type: SnapshotType` and `sequence: u64`. The migration pattern from v6→v7 (adding `table_rows` to `SerializableEntityState` via `#[serde(default)]`) is already established and is the correct template.

### 4.2 Adding shard_count to SnapshotHeader

Apply the established `#[serde(default)]` migration idiom:

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotHeader {
    pub snapshot_type: SnapshotType,
    pub sequence: u64,
    #[serde(default = "default_shard_count")]   // NEW for v1.2
    pub shard_count: u16,
}

fn default_shard_count() -> u16 { 1 }
```

**Backward compatibility:** postcard with `#[serde(default)]` deserializes a pre-v1.2 snapshot (which has no `shard_count` bytes) by substituting `1`. Pre-v1.2 snapshots load cleanly into v1.2 code as `shard_count = 1`. No migration tool needed for the N=1 upgrade path.

**Version bump:** Increment `SNAPSHOT_FORMAT_VERSION` to `8` when this field lands. Add `load_legacy_v7` following the same pattern as `load_legacy_v6` — deserialize into the old type (current `BaseSnapshotState` without `shard_count`), promote to v8 type by setting `shard_count = 1`.

**Critical constraint:** postcard's wire format is not self-describing — field order in the struct determines byte layout. Always append new fields at the struct tail. This is already the practice (`backfill_complete` was appended in Phase 9 with `#[serde(default)]`). Adding `shard_count` anywhere but the end would break deserialization of existing snapshots silently.

**Re-sharding tool (Wave 4):** Operates on event logs, not snapshots. Reads `data/shard-0/streams/*/log.bin` (old layout), replays through the shard hash function with new N, writes to `data/shard-N/streams/*/log.bin`. Snapshot format compatibility is a separate, simpler problem handled above.

---

## 5. Scatter-gather primitive

### 5.1 Wave 3 requirement

`GET /streams` must fan out to all N shards, collect each shard's listing, merge, and return. N is bounded by physical core count (≤32 for v1.2 targets). This is a pure futures join — no shared mutable state.

### 5.2 Recommended pattern

**`futures::future::join_all`** from the `futures` crate. Not currently in `Cargo.toml`. `tokio::join!` is a compile-time macro limited to a fixed number of futures — unusable for runtime-determined N. `futures::future::join_all` accepts `Vec<impl Future>` at runtime, which is required.

```toml
# Add to [dependencies]:
futures = "0.3"
```

Pattern for the axum handler:

```rust
async fn list_streams(State(shards): State<Arc<Vec<ShardHandle>>>) -> Json<Vec<StreamInfo>> {
    let futs: Vec<_> = shards.iter()
        .map(|sh| sh.request(ShardRequest::ListStreams))
        .collect();
    let per_shard: Vec<Vec<StreamInfo>> = futures::future::join_all(futs).await
        .into_iter()
        .flatten()
        .collect();
    Json(merge_stream_listings(per_shard))
}
```

Each `ShardHandle::request` sends over `crossbeam_channel` (SPSC) and awaits a `tokio::sync::oneshot::Receiver<ShardResponse>`. The shard thread receives synchronously (no locks — single owner), processes, sends response back. The oneshot receiver is the await point.

**Alternative `tokio::task::JoinSet`:** Suitable if scatter branches are spawned as separate tokio tasks. Adds task allocation overhead per branch; unnecessary since N ≤ 32 and the branches are I/O-free. Prefer `join_all` for this use case.

**No purpose-built scatter-gather crate.** The N is small, the pattern is textbook, `join_all` is the idiomatic Rust primitive.

### 5.3 Existing axum handler style

Current handlers in `src/server/http_ingest.rs` use `async fn handler(State(...): State<...>) -> impl IntoResponse` directly with no explicit task spawning in the handler body. The scatter-gather handler follows the same signature; `join_all` is awaited inline. No architectural break.

---

## 6. Shard-thread spawn pattern

### 6.1 No existing prototype in tree

No shard-thread spawn exists yet (confirmed via grep). The closest precedent is `src/state/event_log.rs`, which spawns plain `std::thread::spawn` closures for parallel log replay. That is the correct primitive to build on.

### 6.2 Recommended spawn pattern

**`std::thread::Builder` + `core_affinity::set_for_current` + `std::panic::catch_unwind` + single tokio `current_thread` runtime.** No helper crate needed; all components compose inline.

```rust
fn spawn_shard_thread(
    shard_id: usize,
    core_id: Option<core_affinity::CoreId>,
    inbox: crossbeam_channel::Receiver<ShardEnvelope>,
) -> std::thread::JoinHandle<()> {
    std::thread::Builder::new()
        .name(format!("beava-shard-{}", shard_id))
        .stack_size(8 * 1024 * 1024)   // 8 MiB; default 2 MiB can overflow deep async stacks
        .spawn(move || {
            // 1. Pin to core (best-effort; macOS/Apple Silicon will warn and continue)
            if let Some(core) = core_id {
                if !core_affinity::set_for_current(core) {
                    eprintln!(
                        "[shard-{}] core pinning unavailable on this platform; continuing kernel-scheduled",
                        shard_id
                    );
                }
            }

            // 2. Build single-threaded tokio runtime (one per shard thread)
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("failed to build shard runtime");

            // 3. Run shard event loop; catch panics at the thread boundary
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                rt.block_on(shard_event_loop(shard_id, inbox))
            }));

            if let Err(payload) = result {
                let msg = payload.downcast_ref::<&str>().copied().unwrap_or("<non-str panic>");
                eprintln!("[shard-{}] PANIC: {}; triggering shutdown", shard_id, msg);
                // Signal supervisor (AtomicBool shutdown flag or a tokio oneshot)
            }
        })
        .expect("failed to spawn shard thread")
}
```

**Why `build_local()` is not used:** `build_local()` returns a `LocalRuntime` scoped to a single thread but is designed to be created and used within the same synchronous call frame. The `build()` + `block_on()` pattern is the correct approach when the runtime is owned by a std thread spawned via `spawn()`. This matches what Iggy's shard thread implementation does.

**Core ID assignment:** `core_affinity::get_core_ids()` returns `Vec<CoreId>`. Assign `core_ids[shard_id % core_ids.len()]` to shard `shard_id`. On Linux this invokes `sched_setaffinity`; on macOS it becomes a QoS hint (best-effort, will succeed silently or warn once).

**Stack size rationale:** Default std thread stack is 2 MiB on Linux/macOS. Deep async state machines — particularly those holding per-shard pipeline operator state across `.await` points — can overflow this. 8 MiB is conservative and safe. Iggy's migration notes a similar stack-size bump as a required change.

**Panic handler rationale:** `catch_unwind` at the thread boundary (not inside the async runtime) ensures a panicking shard does not take down the whole server process. The supervisor (main thread or a watchdog tokio task watching `JoinHandle`) can detect thread exit and restart or trigger graceful shutdown. The design doc (Risk #3 / Failover item in Wave 5) requires this.

**No thread-pool crate.** Shard threads are permanent, pinned, and each owns a single runtime. They are not work-queue workers. `std::thread::Builder` gives full control and is already used in the tree.

---

## Sources

- [docs.rs/num_cpus 1.17.0](https://docs.rs/num_cpus/latest/num_cpus/) — current version, `get_physical()` API
- [docs.rs/core_affinity 0.8.3](https://docs.rs/core_affinity/latest/core_affinity/) — current version, Apple Silicon limitation documented
- [docs.rs/crossbeam-channel 0.5.15](https://docs.rs/crossbeam-channel/latest/crossbeam_channel/) — current version, bounded SPSC semantics
- [docs.rs/rstest 0.26.1](https://docs.rs/rstest/latest/rstest/) — current version, parameterized case attributes
- [docs.rs/metrics 0.24.3](https://docs.rs/metrics/latest/metrics/) — labeled gauge/counter support confirmed via Key+Label types
- [docs.rs/bytes 1.11.1](https://docs.rs/bytes/latest/bytes/) — current version, zero-copy handle
- [docs.rs/ahash 0.8.12](https://docs.rs/ahash/latest/ahash/) — current version; tuple Hash support via standard trait
- [docs.rs/postcard 1.1.3](https://docs.rs/postcard/latest/postcard/) — current version, stable wire format since v1.0; field-order sensitivity noted
- [crates.io/crates/oha 1.4.5](https://crates.io/crates/oha/1.4.5) — external load tester, JSON output mode
- [proptest vs quickcheck](https://altsysrq.github.io/proptest-book/proptest/vs-quickcheck.html) — proptest preference rationale
- [tokio-rs/tokio#6739](https://github.com/tokio-rs/tokio/issues/6739) — build_local() / LocalRuntime context
- Cargo.toml inspection — confirmed missing: crossbeam-channel, num_cpus, core_affinity, metrics, metrics-exporter-prometheus, futures, rstest
- src/state/snapshot.rs inspection — confirmed: postcard serializer, SNAPSHOT_FORMAT_VERSION=7, #[serde(default)] migration idiom
- src/server/shard_probe.rs inspection — confirmed: ahash RandomState::hash_one usage, std::env::var config pattern, no shard-thread spawn prototype
- src/main.rs inspection — confirmed: all BEAVA_* vars use raw std::env::var; no config framework; no envy/figment
