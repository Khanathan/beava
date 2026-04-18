# TPC + Full-Key-Shard — 2026 Research

**Researched:** 2026-04-18
**Companion to:** `.planning/arch/TPC-SHARD-DESIGN.md`
**Scope:** Validate the design doc against 2026 reality, close the six open questions, flag anything the doc gets wrong.
**Confidence:** HIGH on runtime landscape, prior art, Iggy case study. MEDIUM on specific benchmark numbers (ecosystem is moving fast, few apples-to-apples datasets). LOW on macOS dev-mode pinning behavior under Apple Silicon hybrid cores — treated separately.

---

## Summary

Three things dominate what's changed since the design doc was written:

1. **Apache Iggy shipped the exact migration Beava is contemplating — in February 2026.** They went from `tokio` work-stealing to thread-per-core over `compio` with `io_uring`, pinning one shard per core via `sched_setaffinity`. They evaluated and rejected `monoio` and `glommio`. They report P99 latency −60% and P9999 latency −57% at 32 partitions, plus +18% throughput in fsync mode. This is the single most load-bearing 2026 data point for the design doc — it validates both the direction and the specific technology choices.
2. **Glommio is effectively unmaintained** (Iggy's team called it out explicitly; DataDog has not shipped meaningful commits in 2025-2026). The doc's "Option B — glommio" is no longer a serious option.
3. **Compio is the 2026 cross-platform TPC runtime.** It supports `io_uring` on Linux, IOCP on Windows, and `kqueue`/`polling` on macOS — unlike glommio (Linux-only) and monoio (Linux + limited macOS via polling fallback). Apache Iggy picked it over monoio specifically because compio's driver is decoupled from the executor, and because its io_uring feature coverage is broader with faster maintenance. For Beava's macOS dev story, compio is the **only** TPC runtime that doesn't compromise.

**Primary recommendation for the design doc:** Keep Option A (tokio current_thread per pinned thread) as v1.1's migration step — it's the zero-risk move. But **add a fourth option: Option D — compio**, and name it as the likely v1.2+ endpoint. Frame the progression as "tokio current_thread → compio" rather than "tokio current_thread → glommio". Replace every "glommio" mention with "compio" or qualify glommio as "historical reference, unmaintained as of 2026."

Everything else in the doc is largely sound. The six open questions have clean answers below.

---

## 1. 2026 Rust TPC Runtime Landscape

### 1.1 Tokio `current_thread` + one-runtime-per-pinned-thread

**Status:** Canonical, mature, works on macOS + Linux + Windows.

**2025-2026 updates:**
- `Builder::new_current_thread().build_local()` is the streamlined API (avoids a manual `LocalSet`). Supports spawning `!Send` futures directly via `tokio::task::spawn_local`.
- Tokio team is exploring a `LocalRuntime` type ([tokio-rs/tokio#6739](https://github.com/tokio-rs/tokio/issues/6739)) that would make `tokio::spawn` and `spawn_local` behave identically inside a single-threaded runtime. This is the direction Tokio is heading for TPC users.
- **LocalSet has measurable overhead** vs. a `current_thread` runtime with `unsafe` tasks (Deno has documented this). For Beava, stick with `current_thread` + inherently-local tasks; do **not** use `LocalSet` on the hot path.

**Evidence:**
- [Tokio runtime docs](https://docs.rs/tokio/latest/tokio/runtime/index.html) — `new_current_thread()` supports TPC directly.
- [Pierre Zemb: "Tokio's Hidden Gems"](https://pierrezemb.fr/posts/tokio-hidden-gems/) — covers local execution patterns as of 2025.
- [Rust Forum: current_thread vs multi-thread CPU use](https://users.rust-lang.org/t/running-tokio-on-current-thread-for-time-sliced-io/108101) — current-thread mode can reduce CPU by ~71% for certain workloads.
- [Blog: How to pin Tokio workers with core_affinity](https://blog.veeso.dev/blog/en/how-to-configure-cpu-cores-to-be-used-on-a-tokio-with-core--affinity/) — `on_thread_start` hook pattern.

**Verdict:** Option A in the design doc is **correct**. No changes needed except to mention `build_local()` (not a manual `LocalSet`) and flag the future `LocalRuntime` migration path.

### 1.2 Glommio

**Status: unmaintained as of 2026.** DataDog has effectively walked away.

**Evidence:**
- [DataDog/glommio issues page](https://github.com/DataDog/glommio/issues) — open issues pile up, last meaningful release activity trails off mid-2024.
- [Apache Iggy migration blog (Feb 2026)](https://iggy.apache.org/blogs/2026/02/27/thread-per-core-io_uring/) explicitly states glommio was rejected because it "seems unmaintained" and because of opinionated design choices Iggy disagreed with.
- Glommio remains Linux-only (`io_uring`, minimum kernel 5.8). Macs are cut out.

**Verdict:** The design doc's "Option B — glommio" is **stale**. Replace with compio (Section 1.3).

### 1.3 Monoio (ByteDance)

**Status:** Active, production-used (ByteDance internally), but narrower ecosystem reach than compio.

**Evidence:**
- [bytedance/monoio](https://github.com/bytedance/monoio) — active maintenance.
- [Monoio benchmark vs Tokio](https://github.com/bytedance/monoio/blob/master/docs/en/benchmark.md) — at 4 cores: ~2× Tokio peak throughput; at 16 cores: ~3× Tokio peak. Single-core: slight Monoio edge but higher latency on few-connections workloads.
- [Comparing with Tokio and Glommio](https://zread.ai/bytedance/monoio/30-comparing-with-tokio-and-glommio) — Monoio is `!Send` by design, meaning no Sync/Send bounds on user code inside a shard.
- Monoio does provide a `legacy` feature for macOS (falls back from io_uring to epoll/kqueue), but it's a compromised mode not recommended for prod.

**Why Iggy rejected Monoio:** "limited io_uring feature coverage and insufficient maintenance pace relative to the evolving interface." Monoio has a tightly-coupled driver/executor which makes customization harder.

**Verdict:** Monoio is a real option, but compio is better-positioned for Beava because of the macOS dev story.

### 1.4 Compio ★ (the 2026 winner)

**Status:** Active, cross-platform, the choice Apache Iggy made in Feb 2026 after evaluating all alternatives.

**Evidence:**
- [compio-rs/compio](https://github.com/compio-rs/compio) — thread-per-core async runtime with IOCP/io_uring/polling. Inspired by monoio, but with a decoupled driver/executor architecture.
- [Compio docs site](https://compio.rs/docs) — "A story of compio" explains the design rationale.
- [docs.rs/compio](https://docs.rs/compio/latest/compio/) — current release is 0.18.x.
- **Real production user:** [Apache Iggy migration](https://iggy.apache.org/blogs/2026/02/27/thread-per-core-io_uring/) — 5,000 MB/s throughput = 5M msgs/sec at 1KB, WebSocket P9999 ~9.5ms with fsync-per-message.

**Platform support:**
- Linux: `io_uring` (with polling fallback on older kernels).
- Windows: IOCP.
- macOS: `kqueue` via the `polling` crate. **This is the load-bearing fact for Beava's macOS dev story.** Iggy reports they develop on macOS without compromise.

**Verdict:** Add as **Option D** in the design doc. Treat as the likely v1.2 endpoint. The v1.1 migration path stays tokio current_thread (Option A) to minimize risk.

### 1.5 Seastar-rs / Rust Seastar port

**Status:** Does not exist in production form. Seastar remains C++-only.

**Evidence:** No serious Rust Seastar port has shipped. Redpanda contributors maintain [redpanda-data/seastar-starter](https://github.com/redpanda-data/seastar-starter) but it's still C++.

**Verdict:** Not a real option. Keep the design doc's "prior art we lean on" reference to ScyllaDB/Redpanda/Seastar as inspiration only.

### 1.6 Smol / async-executor

**Status:** Fine for general use, not a serious TPC-shard-per-core runtime.

**Verdict:** Mention only if asked. Not an option for Beava.

### Summary table

| Runtime | 2026 status | macOS? | Linux kernel | Ecosystem | Recommended? |
|---|---|---|---|---|---|
| tokio `current_thread` | canonical, active | ✓ | any | axum / hyper / reqwest | ✓ for v1.1 |
| glommio | **unmaintained** | ✗ | ≥5.8 | minimal | ✗ |
| monoio | active | partial (legacy mode) | ≥5.6 | smaller | maybe |
| **compio** | active, Iggy-validated | ✓ native | ≥5.8 for uring; any for polling | growing, decoupled driver | ✓ for v1.2+ |
| seastar-rs | doesn't exist | — | — | — | ✗ |

---

## 2. Shard-per-Core Prior Art — 2026 Updates

### 2.1 ScyllaDB

**Key 2024-2025 content:**
- [Why ScyllaDB's Shard Per Core Architecture Matters (Oct 2024)](https://www.scylladb.com/2024/10/21/why-scylladbs-shard-per-core-architecture-matters/) — the authoritative "why" post.
- [Shard-Aware Port (2021, still definitive)](https://www.scylladb.com/2021/04/27/connect-faster-to-scylla-with-a-shard-aware-port/) — clients connect to a specific port whose local port number hashes to the shard they want. Eliminates intra-cluster shard-hopping. **Directly applicable lesson for Beava:** when a client consistently talks to the same shard (sticky TCP connection), keep that session routed to the same shard; don't re-hash. The Scylla Python/Rust drivers expose shard token-aware policies.
- [ScyllaDB shard-per-core architecture page](https://www.scylladb.com/product/technology/shard-per-core-architecture/) — "shard count = x86 core count" stated explicitly (but this is logical cores; see §3 / §6 for defaults discussion).
- [Hot partitions for a specific shard](https://github.com/scylladb/scylladb/issues/7797) — documented pain point: a single hot key creates a hot shard. Mitigation is application-level (key salting); not a framework problem. Beava's shard_probe already measures the distribution side of this.

### 2.2 Redpanda

**Key content:**
- [Redpanda architecture docs](https://docs.redpanda.com/current/get-started/architecture/) — partition→shard mapping is permanent (set at partition creation), stored in metadata via Raft. A shard owns multiple partitions if partition count > shard count, but a partition is never split across shards.
- [Performance: Adventures in Thread-per-Core Async with Redpanda and Seastar (QCon 2023)](https://www.infoq.com/presentations/high-performance-asynchronous3/) — the canonical lessons talk. Applicable reaffirmations: single reactor per core, permanent pinning, no thread pool, no cross-worker locks.
- [Investigate increasing partition replicas per shard from 1000 to 2000](https://github.com/redpanda-data/redpanda/issues/15132) — Redpanda's operational cap is ~1000 partitions/shard. For Beava, this translates to "single shard can own hundreds to thousands of streams without blowing up."

### 2.3 Apache Iggy ★ (the February 2026 case study)

This is the single most relevant piece of prior art for Beava. Iggy is a Rust message broker that, in v0.7.0 (late 2025 / early 2026), completed the exact migration Beava is considering.

**Source:** [Apache Iggy's migration journey to thread-per-core architecture powered by io_uring (Feb 2026)](https://iggy.apache.org/blogs/2026/02/27/thread-per-core-io_uring/)

**Key decisions (verified from the blog):**
- Migrated from `tokio` work-stealing to thread-per-core.
- Evaluated `monoio`, `glommio`, `compio`. Chose **compio**.
- Each CPU core runs its own shard, pinned via `sched_setaffinity` on Linux.
- Each shard has its own single-threaded compio runtime. **No cross-thread synchronization within a shard.**
- Inter-shard communication: uses **flume** channels (not kanal or crossbeam). (The blog names flume explicitly; no benchmark justification given — likely convenience over optimum.)

**Results:**
- P99 latency: **−60%** at 32 partitions vs tokio baseline.
- P9999 latency: **−57%** at 32 partitions.
- Throughput (fsync mode, 32 partitions): **+18%**.
- Peak throughput: 5,000 MB/s = **5M msgs/sec at 1KB** on their test hardware.
- Read throughput: 3,361 MB/s.

**Pitfalls they hit (directly applicable to Beava):**
- `RefCell` borrows across `.await` points panic at runtime. Shard-local state must not be accessed via `RefCell` across await boundaries.
- Background event broadcasts create non-deterministic state — problematic for crash-replay determinism (Beava cares deeply about this).
- `io_uring` completions are **not in submission order**. If Beava ever writes per-shard event logs through io_uring, the log writer must explicitly track completion-order → submission-order correctness.
- `io_uring` needs **heavy syscall batching** to beat epoll. Per-event uring submit is slower than epoll-batched writev.
- Hot shards (unbalanced key distribution) are still the dominant failure mode. Shard-probe-style instrumentation is essential.

**What Iggy did NOT document (gaps for Beava):**
- Default shard count (see §3).
- Listener-to-shard routing strategy (SO_REUSEPORT vs dispatch).

### 2.4 Bytewax, Faust, Quix (Python stream processing — for §5 "SDK impact")

- **Bytewax:** [Architecture docs](https://docs.bytewax.io/latest/guide/contributing/architecture.html). Stateful operators require `(key, value)` two-tuples. State key is a string; Bytewax's runtime **shards state among worker processes; only a single worker handles the state for each key**. This is the exact semantic Beava wants. The key extraction is explicit in the dataflow — not a decorator.
- **Faust:** [Agents docs](https://faust.readthedocs.io/en/latest/userguide/agents.html). Partitioning is based on Kafka message key. `concurrency > 1` is allowed for agents, but constrained: "An agent having concurrency > 1 can only read from a table, never write." This is the cleanest analog to Beava's "co-located join" constraint.
- **Quix Streams:** Python over Kafka; key-partitioning is implicit in the Kafka partition model — user doesn't declare shard key separately, partition is determined at produce time.

**Implication for Beava Python SDK:** the cleanest API is a `shard_key=` parameter on `@bv.stream()`. See §8 (Open Q5).

### 2.5 Neon / Turso / Materialize / RisingWave

- **Neon:** [Neon blog on storage scale](https://neon.com/blog/how-we-scale-an-open-source-multi-tenant-storage-engine-for-postgres-written-rust) — shards page ranges by hashing block numbers / stripe size to a pseudorandom pageserver. Not a per-core pattern; it's multi-process/multi-node. Not directly relevant.
- **Turso:** no public evidence of TPC-style per-core sharding (Turso is libSQL + embedded SQLite; single-writer serialization is at the DB level).
- **Materialize:** single-node streaming DB built on Timely Dataflow. [Materialize's Rust experience post](https://materialize.com/blog/our-experience-with-rust/) — they don't pin per-core; they lean on Timely's worker model.
- **RisingWave:** distributed streaming DB with decoupled frontend/backend. Not per-core thread-per-core; it's per-process/per-node partitioning.

**Verdict:** Of this cohort, **none** adopt shard-per-core the way Scylla/Redpanda/Iggy do. They're all horizontal-scale architectures where the equivalent exists but at a different granularity. Not useful as prior art for Beava's intra-node TPC story. The design doc is correct to focus on Scylla/Redpanda/Iggy.

---

## 3. Open Question Resolutions

### Q1 — Default `N_SHARDS`

**Finding:** Two defensible defaults: `num_cpus::get_physical()` (physical cores) or `num_cpus::get() - 1` (logical cores minus one for the listener/control plane). ScyllaDB uses logical cores = shard count. Redpanda's default is also "all cores" with one reactor per core. **Iggy pins one shard per physical core** via `sched_setaffinity` (does not run a separate listener thread — accept sockets live on one of the shard runtimes via SO_REUSEPORT).

**Evidence:**
- [num_cpus docs](https://docs.rs/num_cpus/latest/num_cpus/) — `get()` returns logical, `get_physical()` returns physical (supported on Linux/macOS/Windows).
- Scylla: shards = x86 cores ([ScyllaDB product page](https://www.scylladb.com/product/technology/shard-per-core-architecture/)).
- Redpanda: one reactor per core, no exception ([architecture docs](https://docs.redpanda.com/current/get-started/architecture/)).
- Iggy: confirmed physical-core pinning via sched_setaffinity in the migration blog.

**Recommendation for the design doc:**

Replace the current Q1 entry with:

> **Q1 — Default `N_SHARDS`: use physical core count.**
>
> Default: `BEAVA_SHARDS = num_cpus::get_physical()`. On a 16-core-with-HT box (32 logical, 16 physical), this yields 16 shards — matching ScyllaDB/Redpanda/Iggy practice. Hyperthread siblings fight for the same L1/L2 cache; running two shards on HT siblings reduces per-shard cache efficiency with no throughput upside for memory-bound work (which Beava is).
>
> Listener thread: **do not reserve a separate one**. Shard 0 owns the accept socket via SO_REUSEPORT (see Q3); other shards each own their own accept socket for the same port. No dedicated "listener" thread exists. This matches Iggy's topology.
>
> Env override: `BEAVA_SHARDS=N` always wins. `BEAVA_SHARDS=1` compiles to current single-writer behavior (migration compatibility from §7 of the design doc).

### Q2 — macOS dev experience

**Finding:** The canonical Rust idiom is `#[cfg(debug_assertions)]` to downshift in dev builds. On Apple Silicon, `core_affinity` does **not** pin to specific P-cores or E-cores — the kernel decides based on QoS class. The crate will "request highest performance" via QoS hints but cannot strictly pin. This is a fundamental macOS kernel limitation, not a crate bug.

**Evidence:**
- [core_affinity v0.8.3 docs](https://docs.rs/core_affinity/latest/core_affinity/) — "On some platforms like macOS on aarch64, it's not possible to pin a thread to a specific core."
- [gdt-cpus](https://wildpixelgames.github.io/gdt-cpus/) — even the most aggressive thread-pinning crate acknowledges the Apple Silicon limitation.
- Apple's scheduler routes threads by QoS class (user-interactive / user-initiated / utility / background) and decides E-core vs P-core dynamically.

**Recommendation for the design doc:**

Replace the current Q2 entry with:

> **Q2 — macOS dev experience: soft-downshift in debug builds; accept best-effort pinning.**
>
> - In debug builds (`cfg(debug_assertions)`): default `BEAVA_SHARDS=1`. Rationale: dev machines run many other processes; TPC benefits are invisible and the added complexity slows iteration.
> - In release builds: default `BEAVA_SHARDS=num_cpus::get_physical()`.
> - On macOS (any build): `core_affinity::set_for_current()` is called but treated as best-effort. A warn-level log once at startup if pinning silently failed ("shard %d: core-pinning unavailable on this platform; threads will be kernel-scheduled").
> - On Apple Silicon specifically: threads will land on P-cores under load via the default QoS class. The inability to strictly pin is **not** a correctness problem — shard ownership of state is enforced by the routing layer, not by CPU affinity. Affinity only affects L1/L2 cache locality. Dev throughput will be lower than a Linux prod box; document this and move on. Prod is Linux.

### Q3 — SO_REUSEPORT strategy

**Finding:** On Linux, SO_REUSEPORT with BPF-free default distributes new connections via a 4-tuple hash across sockets in the group. **It works well** for stripe-by-connection on 10+ listener sockets. There is no thundering-herd in the modern kernel path — each listener sees only its own connections, no shared accept queue. BPF reuseport programs (Linux 4.5+, common now) can additionally customize distribution (e.g., lock a connection to a shard by consistent-hashing the client IP). macOS supports `SO_REUSEPORT` but the semantics are closer to the original BSD `SO_REUSEADDR` (address sharing) rather than Linux's load-balancing behavior. **macOS does NOT have `SO_REUSEPORT_LB`** (FreeBSD-only since 2018). On macOS, the dev-mode N=1 configuration sidesteps this entirely.

**Evidence:**
- [LinuxJedi: Socket SO_REUSEPORT and kernel implementations](https://linuxjedi.co.uk/socket-so_reuseport-and-kernel-implementations/) — SO_REUSEPORT_LB is FreeBSD-only; macOS SO_REUSEPORT behavior is "believed similar but not extensively tested."
- [Medium: Performance Optimisation using SO_REUSEPORT](https://medium.com/high-performance-network-programming/performance-optimisation-using-so-reuseport-c0fe4f2d3f88) — kernel 4.17 benchmark: 48-core box, 1M sequential connections: 4m45s → 1m36s (~66% reduction) with SO_REUSEPORT on.
- [LWN: Avoiding unintended connection failures with SO_REUSEPORT](https://lwn.net/Articles/853637/) — documents the corner-case of closed-shard connection-drop; mitigation is SO_REUSEPORT + explicit socket migration, now handled in-kernel.

**Recommendation for the design doc:**

Replace the current Q3 entry with:

> **Q3 — SO_REUSEPORT strategy: shard-thread-owned accept, Linux primary, macOS fallback to single-listener.**
>
> **Linux (prod):** Each shard binds its own socket to the same address:port using `SO_REUSEPORT`. The kernel distributes new TCP connections across sockets via a 4-tuple hash. Each shard's own accept loop runs on its shard thread — no listener hop, no cross-thread handoff for the connect path. For key-stickiness (a client's events consistently land on the same shard), clients that push the same stream keys over the same TCP connection get connection-affinity for free — but event-level stickiness still requires the shard_hint routing layer inside the PUSH path (a client's connection may land on shard 2 but events with key hashing to shard 5 route through the internal SPSC queue). Accept the two-hop cost; it's bounded and small.
>
> **macOS (dev):** With `BEAVA_SHARDS=1` default, this is moot. If a user overrides to N>1 on macOS, we use a single listener thread + dispatcher (not SO_REUSEPORT), accepting the latency cost as a dev-mode compromise. Log a warn once on startup.
>
> **Measurement gate:** before committing to per-shard accept, Wave 0 micro-bench must show listener-dispatched overhead ≥ 10 μs added vs SO_REUSEPORT in realistic steady-state push traffic. If the gap is smaller, the dispatcher design is simpler and preferred.

### Q4 — Fork re-sharding (upstream N ≠ downstream N)

**Finding:** Kafka consumer rebalancing is the closest mainstream analog. The 2024-2025 state of the art is the **cooperative-sticky assignor** (Kafka 2.4+, solidified in Kafka 4.0 late-2024/early-2025), which avoids stop-the-world rebalances and minimizes partition movement across consumers. Faust uses a variant ("standby tables") that prefers to promote replicas with full data already local. For Beava's fork-replica problem, the simpler truth applies: **re-sharding at replica startup is fine, re-sharding mid-stream is not**. Offline replay reshards from upstream log; online subscribe uses upstream's shard_hint and re-routes locally by downstream's N. The math: for every event the replica sees, `downstream_shard = hash(event.key) mod downstream_N`, ignoring upstream's partition entirely unless downstream explicitly wants to inherit (which our design doc's `shard_hint = kafka_partition` path assumes).

**Evidence:**
- [Redpanda: Kafka Rebalancing](https://www.redpanda.com/guides/kafka-performance-kafka-rebalancing) — overview of triggers and mitigation.
- [Lydtech: Kafka 4.0 Next-Gen Rebalance Protocol](https://www.lydtechconsulting.com/blog/blog-kafka-rebalance-next-gen) — cooperative rebalancing details.
- [Faust docs: Application / Standby Tables](https://faust.readthedocs.io/en/latest/userguide/application.html) — Faust promotes workers that already have full replica data.
- [Bytewax Rescaling docs](https://docs.bytewax.io/stable/guide/concepts/rescaling.html) — Bytewax does NOT currently support mid-flight rescaling; requires a full restart.

**Recommendation for the design doc:**

Replace the current Q4 entry with:

> **Q4 — Fork re-sharding: always re-hash on ingest, make upstream N irrelevant.**
>
> The replica's downstream shard count is independent of the upstream's. At the ingest entrypoint inside the replica, we call `shard_hint(event) = hash(event.key) mod downstream_N`. The upstream's shard_hint in the `OP_LOG_FETCH` metadata is a **hint for optimization** (skip hashing if upstream_N == downstream_N and the key space partition matches), not a constraint. The default code path always re-hashes; the optimization is a fast-path check.
>
> Rationale: the alternative — requiring downstream N to match upstream N — is brittle (upstream may change N across restarts; multiple upstreams impossible) and provides no correctness benefit (Beava's join model already forces shard-key-agreement at stream registration time, independent of upstream).
>
> No `--reshard-from upstream-N` flag needed on `beava fork`. Delete that item from Wave 4. The silent-reshard-on-every-fork path is correct and the only one we should implement.

### Q5 — Python SDK impact

**Finding:** Three Python stream libs (Bytewax, Faust, Quix) all expose key-partitioning, but **with different surface syntax**. Bytewax threads `(key, value)` tuples through the dataflow — explicit and functional. Faust uses implicit Kafka-key-based partitioning — zero user code. Quix similarly inherits Kafka's key. For Beava, a decorator param is the most Rust-adjacent API and the most beginner-friendly. Faust's pattern is closest to what Beava should aim for: declare the key at stream registration time, then all routing is automatic.

**Evidence:**
- [Bytewax Architecture](https://docs.bytewax.io/latest/guide/contributing/architecture.html) — "State is sharded among workers; only a single worker handles state for each key."
- [Faust Agents](https://faust.readthedocs.io/en/latest/userguide/agents.html) — "messages with the same account id as key are always delivered to the same agent instance." Config via `concurrency=N` on the agent decorator.

**Recommendation for the design doc:**

Replace the current Q5 entry with:

> **Q5 — Python SDK impact: add `shard_key=` to `@bv.stream`.**
>
> ```python
> @bv.stream(shard_key="user_id")  # explicit; recommended for joins
> class Transactions:
>     user_id: str
>     amount: float
>     _event_time: int
> ```
>
> If `shard_key=` is omitted, we fall back to the stream's primary-key field (first field of the dataclass), which matches current Beava ergonomics. Joins require **explicit** `shard_key=` agreement between all joined streams (enforced at registration time with an actionable error; matches design doc §3).
>
> Multi-field shard keys (`shard_key=("region", "user_id")`) supported as tuple; the tuple is hashed via `ahash` server-side for deterministic shard assignment.
>
> `shard_hint` is not user-facing; it's an internal wire-format field (preserves upstream routing per §Q4). Python users only see `shard_key`.

### Q6 — Metrics

**Finding:** Redpanda's public metrics are the cleanest template. Beava should emit per-shard labeled series for: reactor utilization (time busy), task queue depth, events accepted/rejected, SPSC inbox backlog, cross-shard fanout counter (scatter-gather queries). ScyllaDB uses `reactor_utilization` as its single most important shard metric — a simple "what fraction of the last second was this reactor not idle" gauge.

**Evidence:**
- [Redpanda Public Metrics](https://docs.redpanda.com/current/reference/public-metrics-reference/) — metrics labeled with `shard` where applicable.
- [Redpanda Internal Metrics](https://docs.redpanda.com/current/reference/internal-metrics-reference/) — `vectorized_reactor_utilization`, task queue depth, alien receive batch queue length.
- [Redpanda issue #12608](https://github.com/redpanda-data/redpanda/issues/12608) — community request to promote `vectorized_reactor_utilization` to public metrics, confirming it's considered essential.
- [Scylla dev metrics.md](https://github.com/scylladb/scylladb/blob/master/docs/dev/metrics.md) — `reactor_utilization` tops the "must-monitor" list.

**Recommendation for the design doc:**

Replace the current Q6 entry with:

> **Q6 — Metrics: per-shard-labeled, reactor utilization first.**
>
> Add:
> - `beava_shard_reactor_utilization{shard="N"}` — gauge 0..1, fraction of the last 1-second window the shard reactor was not idle. **The single most important shard metric.**
> - `beava_shard_inbox_depth{shard="N"}` — gauge, SPSC queue backlog for the shard. Non-zero steady-state = the shard is falling behind.
> - `beava_shard_events_total{shard="N",outcome="accepted|dropped"}` — counter.
> - `beava_cross_shard_fanout_total{op="list_streams|global_watermark|scatter_read"}` — counter. Tracks operations that touch all shards.
> - `beava_shard_keys_owned{shard="N"}` — gauge, number of distinct keys currently routed to this shard (exposes hot-shard / cold-shard imbalance).
> - `beava_shard_watermark_lag_seconds{shard="N"}` — gauge. Each shard publishes its max seen event-time minus wall-clock lag; the global watermark is derived from min across shards.
>
> The final metric (`beava_watermark_lag_seconds` at the global level) is already requested by SRE-STREAM persona review; we add the `{shard="N"}` variant without breaking the unlabeled one. Both are emitted during the TPC transition; the unlabeled one becomes a derived `min(beava_shard_watermark_lag_seconds)`.
>
> **Drop** the `beava_shard_lag_seconds` name from the doc — it's too generic. Use the names above.

---

## 4. Design-Doc Claim Validation

### 4.1 "~2M EPS per 16-core box" target

**Finding:** Defensible but **calibration uncertain**. Apache Iggy hits 5M msgs/sec at 1KB on a message broker with persistence — that's a different workload (append-only log write vs stateful feature update), but it sets a plausible order-of-magnitude ceiling. Monoio benchmarks at 16 cores show ~3× Tokio throughput peak, which aligned with Beava's current 314K EPS baseline would give ~1M EPS (lower bound) for a naive port. The 2M number is realistic for stateful per-key aggregation if the shard-hint distribution is balanced AND keys are <50% cross-shard (per Beava's own shard_probe gating criterion). Call it "1.5M–2.5M EPS, range" rather than a point estimate.

**Evidence:**
- [Iggy migration benchmarks](https://iggy.apache.org/blogs/2026/02/27/thread-per-core-io_uring/) — 5M msgs/sec at 1KB.
- [Monoio benchmarks](https://github.com/bytedance/monoio/blob/master/docs/en/benchmark.md) — ~3× Tokio peak at 16 cores.
- Beava's current 314K EPS × 5–6× factor (design doc's claim) = 1.5M–1.9M EPS. Checks out if the 5–6× multiplier is achievable, which is what Iggy and Monoio independently corroborate.

**Recommendation for the design doc:** In the Benchmark Expectations table, soften the "2.0M" to "1.5M–2.5M (range; see prior art §Iggy, Monoio)" and explicitly state that real achievable number depends on the shard-probe cross_shard_fraction measured on the user's workload.

### 4.2 `core_affinity` crate choice

**Finding:** Still the right crate. Version 0.8.3 (released Feb 2025) is current; platform support covers Linux/macOS/Windows with the documented Apple Silicon limitation. A newer crate `gdt-cpus` exists for more advanced hybrid-core scheduling but is overkill for Beava's needs (it targets real-time audio / game engines).

**Evidence:**
- [core_affinity on crates.io](https://crates.io/crates/core_affinity) — active, 0.8.3 current.
- [gdt-cpus](https://wildpixelgames.github.io/gdt-cpus/) — real-time-focused; acknowledges same Apple Silicon limits.

**Recommendation:** Keep `core_affinity` in the design doc. No change.

### 4.3 SPSC channel choice

**Finding:** The 2026 landscape:
- **kanal** — claims fastest in fereidani benchmarks, but benchmarks are maintainer-run.
- **crossbeam-channel** — gold standard, widely deployed, well-tested.
- **flume** — what Iggy chose (February 2026). Reports claim "sometimes faster than crossbeam-channel."
- **rtrb** — wait-free SPSC ring buffer for real-time use; lowest latency for truly SPSC pure-value traffic.
- **crossfire** — newer; claims +70% on bounded SPSC over alternatives but limited production use.

For Beava's listener-to-shard handoff: we're moving `bytes::Bytes` (zero-copy handles) across the boundary. The overhead of the channel is dominated by atomic-RMW cost on the producer/consumer indices, not by the payload. For this workload, `crossbeam-channel::bounded(N)` in SPSC mode is the safe default; `rtrb` is the maximum-performance option if we ever bottleneck on the handoff itself. Iggy's flume choice is fine but probably not driven by a benchmark — they likely picked it for ergonomics. Beava should not blindly follow.

**Evidence:**
- [rust-channel-benchmarks](https://github.com/fereidani/rust-channel-benchmarks) — kanal-friendly but covers all of the above.
- [rtrb on docs.rs](https://docs.rs/rtrb/0.1.4/rtrb/) — wait-free, cache-padded, real-time-safe.
- [Building a High-Performance Lock-Free SPSC Queue in Rust (Feb 2026)](https://medium.com/@antoine.rqe/building-a-high-performance-lock-free-spsc-queue-in-rust-557ab59f3807) — recent practical comparison; custom ringbuffer p50 188µs / p99 207µs vs crossbeam ArrayQueue p50 255µs / p99 349µs (but the author notes benchmarks are brittle).

**Recommendation for the design doc:**

Replace Option §6's channel line ("`kanal` or `crossbeam-channel` SPSC") with:

> Channel: start with `crossbeam-channel::bounded()` in SPSC configuration (one producer = listener shard, one consumer = owning shard). If the handoff becomes a measured bottleneck (Wave 0 micro-bench: <10 μs budget), upgrade to `rtrb` for wait-free ring-buffer semantics. Do not start with `rtrb` — the `Bytes` handoff doesn't benefit enough to justify the ergonomic hit (fixed-size ring, no error-on-close semantics).

### 4.4 Tokio `LocalSet` as alternative to per-thread runtimes

**Finding:** Viable, but **not recommended**. Deno has documented that `LocalSet` has measurable overhead vs a `current_thread` runtime with `unsafe` tasks. The Tokio team is building a `LocalRuntime` ([tokio-rs/tokio#6739](https://github.com/tokio-rs/tokio/issues/6739)) that would replace both, but it's not stable as of April 2026.

**Evidence:**
- [Tokio #6739: LocalRuntime proposal](https://github.com/tokio-rs/tokio/issues/6739).
- [Rust forum: "let's talk about tokio's LocalSet"](https://users.rust-lang.org/t/lets-talk-about-tokios-localset/62707) — documented perf gap.
- [Tokio Builder docs](https://docs.rs/tokio/latest/tokio/runtime/struct.Builder.html) — `new_current_thread().build_local()` is the modern replacement.

**Recommendation for the design doc:** In the Runtime Choice section (design doc §1), add after Option A:

> Sub-option A': `Builder::new_current_thread().build_local()` is the modern Tokio (2025+) pattern — preferred over manual `LocalSet`, which has documented overhead. Track the upcoming `LocalRuntime` type as the canonical future API.

---

## 5. What the Design Doc Gets Wrong / Needs Updating

Short list of concrete edits:

| # | Section | Current text | Issue | Recommended edit |
|---|---|---|---|---|
| 1 | §"Runtime choice" Option B | "glommio ~2× throughput over tokio on pure I/O" | Glommio is effectively unmaintained in 2026; glommio → compio is the actual v1.2 path | Replace with "Option D — compio (io_uring on Linux, IOCP on Windows, kqueue on macOS)"; mention glommio only as "historical reference, abandoned by upstream" |
| 2 | §"Prior art we lean on" | glommio listed as prior art | Factually stale | Add Apache Iggy (Feb 2026) as the load-bearing TPC case study; keep glommio but qualify |
| 3 | §6 "HTTP / TCP listener → shard routing" | "`kanal` or `crossbeam-channel` SPSC" | Iggy uses flume; crossbeam is the safer default for Beava | Rewrite per §4.3 above |
| 4 | Benchmark expectations "2.0M" EPS | Hard point number | Better framed as a range contingent on shard-balance | "1.5M–2.5M EPS, contingent on shard_probe cross_shard_fraction <40%" |
| 5 | Open Q4 "--reshard-from upstream-N" flag | Proposes a CLI flag | Unnecessary — always re-hash | Delete flag; replace with "always re-hash on ingest" semantic |
| 6 | Risk #3 "Pinning behavior on macOS" | States it's best-effort | Under-sold the Apple Silicon specific problem | Add: "On Apple Silicon aarch64 specifically, core pinning is silently ignored by the XNU scheduler; threads land on P-cores via QoS class. Not a correctness problem; only a cache-locality one." |
| 7 | §"Prior Beava experiments to reuse" | shard_probe.rs mentioned | Fine, but should be explicit about the cross_shard_fraction gate | Add: "Gate TPC merge decision on shard_probe reporting cross_shard_fraction < 40% on the 9-cell matrix; if higher, the architecture bet is wrong for Beava's workload." |
| 8 | Wave 1 | `Compile-time N_SHARDS = 1 first` | Runtime-configurable is actually cleaner | Prefer: runtime-configurable from day 1 (compile-time complicates the migration compat story in §7 and saves no meaningful perf) |

---

## 6. Assumptions Log

| # | Claim | Section | Risk if wrong |
|---|---|---|---|
| A1 | Iggy's "5M msgs/sec at 1KB" benchmark is apples-to-close-enough-apples for Beava's workload | §4.1 | Our 2M EPS ceiling is optimistic; could be 1M real-world |
| A2 | `crossbeam-channel` SPSC is within 2× of `rtrb` latency for `Bytes` handoffs | §4.3 | We over-engineer by starting with rtrb, or under-engineer and have to rewrite |
| A3 | SO_REUSEPORT 4-tuple hash on Linux gives even distribution at 8–32 connection count (not just thousands) | §Q3 | Shard imbalance at low connection counts if true — would push us to listener-dispatched routing |
| A4 | compio's macOS kqueue path is performant enough for dev (not prod) use | §1.4 | Nothing — dev throughput doesn't matter for the architectural bet |
| A5 | Apple Silicon QoS-class dispatch to P-cores happens reliably for a long-running Rust process | §Q2 | Dev-machine throughput varies between runs; acceptable |

---

## 7. Open Questions Still Unanswered

1. **Exact compio performance on macOS via kqueue/polling**, compared to Linux io_uring. Iggy's migration blog implies it's "good enough for dev" but doesn't publish macOS numbers. **Recommendation:** Wave 0 smoke-bench on a macOS M-series laptop + a Linux box with identical workload; expect 2–3× Linux advantage but no show-stopper.
2. **NUMA on 32+ core boxes.** Beava's target deploy is ≤16 cores today, but Beava Cloud's roadmap hints at 64-core nodes. Tokio is explicitly not NUMA-aware. compio inherits the same. At 32 cores there's typically a NUMA boundary; naïve "pin every shard to one core" leaves cross-NUMA memory access paths expensive. Out of scope for v1.2 but flag for the "Beava Cloud" era.
3. **io_uring syscall batching strategy.** Iggy flagged "heavy syscall batching" as a must. Our shard event loop needs to batch reads/writes into uring submissions explicitly — not obvious how this interacts with the current per-event push contract. Design sub-question for Wave 2+.

---

## Sources

**HIGH confidence (primary sources):**
- [Apache Iggy: migration to thread-per-core (Feb 2026)](https://iggy.apache.org/blogs/2026/02/27/thread-per-core-io_uring/)
- [Apache Iggy 0.6.0 release (Dec 2025)](https://iggy.apache.org/blogs/2025/12/09/release-0.6.0/)
- [Apache Iggy WebSocket blog (Nov 2025)](https://iggy.apache.org/blogs/2025/11/17/websocket-io-uring/)
- [ScyllaDB: Why Shard Per Core Architecture Matters (Oct 2024)](https://www.scylladb.com/2024/10/21/why-scylladbs-shard-per-core-architecture-matters/)
- [ScyllaDB: Connect Faster with Shard-Aware Port](https://www.scylladb.com/2021/04/27/connect-faster-to-scylla-with-a-shard-aware-port/)
- [Redpanda architecture docs](https://docs.redpanda.com/current/get-started/architecture/)
- [Redpanda public metrics](https://docs.redpanda.com/current/reference/public-metrics-reference/)
- [Redpanda internal metrics](https://docs.redpanda.com/current/reference/internal-metrics-reference/)
- [Redpanda QCon 2023: Adventures in TPC with Seastar](https://www.infoq.com/presentations/high-performance-asynchronous3/)
- [compio-rs/compio](https://github.com/compio-rs/compio/)
- [compio docs site](https://compio.rs/docs)
- [bytedance/monoio](https://github.com/bytedance/monoio)
- [Monoio benchmarks](https://github.com/bytedance/monoio/blob/master/docs/en/benchmark.md)
- [DataDog/glommio issues](https://github.com/DataDog/glommio/issues) (for maintenance-status signal)
- [tokio docs (runtime, Builder, LocalSet)](https://docs.rs/tokio/latest/tokio/runtime/index.html)
- [tokio-rs/tokio#6739 — LocalRuntime proposal](https://github.com/tokio-rs/tokio/issues/6739)
- [core_affinity docs v0.8.3](https://docs.rs/core_affinity/latest/core_affinity/)
- [Bytewax architecture docs](https://docs.bytewax.io/latest/guide/contributing/architecture.html)
- [Faust agents docs](https://faust.readthedocs.io/en/latest/userguide/agents.html)
- [LWN: Avoiding unintended SO_REUSEPORT connection failures](https://lwn.net/Articles/853637/)

**MEDIUM confidence (secondary / verified):**
- [Pierre Zemb: Tokio's Hidden Gems](https://pierrezemb.fr/posts/tokio-hidden-gems/)
- [LinuxJedi: SO_REUSEPORT across kernel implementations](https://linuxjedi.co.uk/socket-so_reuseport-and-kernel-implementations/)
- [Medium: SO_REUSEPORT Performance Optimisation](https://medium.com/high-performance-network-programming/performance-optimisation-using-so-reuseport-c0fe4f2d3f88)
- [fereidani/rust-channel-benchmarks](https://github.com/fereidani/rust-channel-benchmarks)
- [rtrb on docs.rs](https://docs.rs/rtrb/0.1.4/rtrb/)
- [gdt-cpus](https://wildpixelgames.github.io/gdt-cpus/) — confirms Apple Silicon pinning limitation
- [Introduction to Monoio (chesedo)](https://chesedo.me/blog/monoio-introduction/)
- [Bytewax Rescaling docs](https://docs.bytewax.io/stable/guide/concepts/rescaling.html)
- [Redpanda: Kafka Rebalancing](https://www.redpanda.com/guides/kafka-performance-kafka-rebalancing)
- [Lydtech: Kafka 4.0 Next-Gen Rebalance](https://www.lydtechconsulting.com/blog/blog-kafka-rebalance-next-gen)

**LOW confidence (mentioned, not load-bearing):**
- [Antoine Rqe: High-performance SPSC (Feb 2026)](https://medium.com/@antoine.rqe/building-a-high-performance-lock-free-spsc-queue-in-rust-557ab59f3807) — author notes benchmarks brittle
- [Neon: multi-tenant storage engine](https://neon.com/blog/how-we-scale-an-open-source-multi-tenant-storage-engine-for-postgres-written-rust) — referenced to rule out as prior art
- [Materialize: Rust experience](https://materialize.com/blog/our-experience-with-rust/) — ditto

---

## Metadata

**Confidence breakdown:**
- Runtime landscape (compio, Iggy, glommio status): HIGH — multiple authoritative 2026 sources corroborate.
- Prior art (Scylla/Redpanda/Bytewax/Faust): HIGH — primary docs + well-established architectural literature.
- Open-question resolutions (Q1–Q6): MEDIUM-HIGH — answers derived from prior-art synthesis; Beava-specific measurement (Wave 0 micro-benches) still needed to commit.
- Specific benchmark numbers: MEDIUM — Iggy's data is the most recent; Monoio's is maintainer-run; rtrb bench is explicitly flagged brittle by the author. The "2M EPS target" stays directional.
- Apple Silicon / macOS pinning: LOW but well-scoped — kernel-level limitation, well-documented.

**Valid until:** ~2026-07-18 (3 months). Apache Iggy is actively iterating on their TPC implementation; compio v0.19+ may change API surfaces; Tokio `LocalRuntime` RFC may stabilize. Re-check before committing to Wave 2.

**Research date:** 2026-04-18
