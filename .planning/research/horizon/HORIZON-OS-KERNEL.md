# Horizon Research — OS / Kernel / Hardware Hacks (non-storage)

**Date:** 2026-04-11
**Scope:** Broad. Everything outside Tally's storage I/O path that affects latency, throughput, or density at the OS, kernel, scheduler, memory-subsystem, network, or hardware level.
**Companion to:** `HORIZON-STORAGE-IO.md` (storage half), `HORIZON-SURVEY.md` §4.3 (extended here). Do not duplicate §4 — references it where relevant.
**Confidence scale:** H/M/L (HIGH = kernel docs + production precedent; MED = blogs + benchmarks; LOW = extrapolation).

---

## Framing

Tally v1.3 is thread-per-shard with `parking_lot::Mutex<ShardStore>`, pinned via `core_affinity`. The storage half is covered in the sibling doc. This doc asks: what is everything *else* the Linux kernel, scheduler, and hardware give us to chase <100 µs p99 harder?

The short version: **CPU isolation + huge pages + NUMA-aware allocation + `io_uring` socket path + tight scheduler control** is the Seastar/Scylla playbook. Tally can cherry-pick from it without going all-in.

---

## §A CPU scheduling and affinity

### A.1 `sched_setaffinity` — pin everything, not just shards

**Syscall:** `sched_setaffinity(pid, cpusetsize, mask)`. Rust: `core_affinity` crate (already in v1.3 plan), or `nix::sched::sched_setaffinity`.

**v1.3 plan:** pin each shard worker to a specific core.

**What v1.3 does NOT currently pin** (based on ARCHITECTURE.md read):
- TCP accept thread (tokio multi-thread runtime uses a worker pool)
- HTTP management handler threads
- Snapshot writer thread
- Timer thread(s) for periodic fsync / eviction / snapshot tick
- Metrics/debug tasks

**Recommendation for v2:**
- **Shard workers** pinned to cores `0..N-1` (v1.3 already).
- **"Housekeeping core"** — one core runs: HTTP mgmt, snapshot writer, timer thread, metrics export. Let the OS preempt it freely; hot shard cores never pay the cost.
- **"Network front door core(s)"** — one or two cores for TCP accept + initial frame dispatch. See §D.1 on `SO_REUSEPORT` for the alternative where shards accept directly.

**Prior art.** Scylla's CPU assignment model (one shard per reactor, one housekeeping reactor). Chronicle Queue (Java, LMAX style) has a similar "core zero is for plumbing" recipe.

**Complexity:** Low. `core_affinity` crate handles it; just need to extend v1.3's pinning to more threads.

**Target phase.** Minor tuning alongside v1.3 or in v1.4 polish.

### A.2 `SCHED_FIFO` / `SCHED_RR` — real-time priority

**Syscall:** `sched_setscheduler(pid, policy, param)`, where policy = `SCHED_FIFO` or `SCHED_RR` and `param.sched_priority` is 1–99.

**What it does.** Bumps the thread above all `SCHED_OTHER` (default) tasks. A `SCHED_FIFO` thread runs until it yields or blocks.

**Upside for Tally.** Shard workers are never preempted by background kernel housekeeping, cron jobs, etc. **Tail latency drops by several µs** on contended hosts — the dominant tail source is "kernel preempted our shard worker to run something else."

**Downside.** **Dangerous on cores that run other things.** A runaway loop in a `SCHED_FIFO` thread starves everything else on that core — system becomes unresponsive, operator must power-cycle. `RLIMIT_RTTIME` exists as a safety valve but requires cgroups setup.

**Verdict.** **Opt-in, documented as "for dedicated hosts only, combined with isolcpus."** Not default. Gated by config flag `runtime.realtime_priority = true`.

**Rust.** `nix::sched::sched_setscheduler`. `thread-priority` crate wraps it cross-platform.

**Prior art.** Chronicle Queue, Aeron messaging (real-time ops), LMAX Disruptor recipes.

**Target phase.** v2 as opt-in, documented best practice.

### A.3 `SCHED_DEADLINE` — EDF scheduling for deterministic bounds

**Syscall:** `sched_setattr(pid, attr, flags)` with `attr.sched_policy = SCHED_DEADLINE`, plus runtime / deadline / period.

**What it does.** The kernel guarantees the thread gets `runtime` ns of CPU every `period` ns, finishing before its `deadline`. Earliest-Deadline-First scheduling. Linux 3.14+.

**For Tally.** Would let us literally ask the kernel for "1 ms of CPU every 10 ms" for a shard worker. In principle a stronger tail-latency guarantee than `SCHED_FIFO`.

**Catch.** `SCHED_DEADLINE` is **almost never used in production**. It's designed for hard real-time; the admission controller is strict; exceeding runtime kills the thread. Rust ecosystem support is thin — `nix` has the types but no high-level wrapper. Debugging is painful.

**Verdict.** **Theoretical interest, not worth pursuing.** `SCHED_FIFO` covers 95 % of the win with 5 % of the pain.

**Target phase.** Skip unless a very specific deterministic-latency customer appears.

### A.4 `isolcpus` + `nohz_full` + `rcu_nocbs` kernel command line

**What it does.** Kernel boot parameters:
- `isolcpus=4-15` — remove cores 4–15 from the default scheduler. Nothing runs there unless explicitly pinned via `sched_setaffinity`.
- `nohz_full=4-15` — disable the scheduler tick (1000 Hz normally) on those cores. No timer interrupt, no periodic bookkeeping.
- `rcu_nocbs=4-15` — RCU callbacks don't run on those cores; delegated to other cores.

**Combined effect.** Tally shard cores become **interrupt-free, tick-free islands**. Tail latency improvements are real and documented: Scylla measures **5–20 µs p99.9 reduction** from the combination.

**Anti-philosophy problem.** Requires operator to edit `/etc/default/grub` and reboot. Not a Tally concern — it's a deployment recipe. **Document, don't require.** An `install-optimized.sh` script could set this up but is fundamentally operator-hostile compared to the zero-ops story.

**Recommendation.** Ship a `docs/deployment/tuned-host.md` with the recipe. Tally itself does runtime detection: if the host has `isolcpus` set and matching CPU topology, pin shards to the isolated cores. Otherwise, pin to whatever.

**Rust.** N/A code-side. Detection via `/proc/cmdline` parse.

**Target phase.** Documentation v1.4; runtime detection v2.

### A.5 IRQ affinity — steer NIC / NVMe interrupts off hot cores

**Tool.** Write to `/proc/irq/N/smp_affinity` — bitmask of cores the interrupt may fire on.

**What it does.** NVMe completion and NIC receive interrupts normally fire on whatever core the device was registered with. On a busy Tally shard core, an interrupt costs ~3–5 µs of hot-path latency via context switch.

**Recipe.** Map all NVMe + NIC IRQs to the housekeeping core. Shard cores never get interrupted.

**Tool chain.** `irqbalance` daemon is the usual culprit — it dynamically remaps IRQs and defeats manual affinity. **Disable `irqbalance` or configure `IRQBALANCE_BANNED_CPUS`**.

**Rust.** N/A code-side. Deployment recipe.

**Prior art.** Every low-latency shop. Aeron messaging docs spell it out explicitly. Scylla docs.

**Target phase.** Documentation, v1.4.

### A.6 `sched_yield` vs `thread::yield_now` vs `_mm_pause`

In a shard worker spin loop (e.g., waiting briefly for a cross-shard channel message), three yield primitives exist:

- **`sched_yield()`** (libc syscall) — forces a context switch into the kernel, ~1 µs. Usually **wrong** for short waits — it adds latency and can deschedule the thread.
- **`std::thread::yield_now()`** — calls `sched_yield` on Linux. Same caveat.
- **`core::hint::spin_loop()`** / `_mm_pause` x86 — emits a `PAUSE` instruction. ~35 cycles on modern Intel, ~25 on AMD Zen. Hints the CPU "I'm in a busy wait, don't hammer the memory bus and let the sibling hyperthread progress." **This is the right primitive for tight spin loops.**

**Rule of thumb:** if you're busy-waiting for < ~10 µs, use `spin_loop()`. Longer, use a proper wait (futex / mutex / channel). **Never** `sched_yield` on a shard hot path.

**Prior art.** `crossbeam-utils::Backoff` is the canonical wrapper — it combines `spin_loop` with exponential backoff and falls through to `thread::yield_now` after enough iterations. Tally's cross-shard channels should use `Backoff` on the receive side.

**Target phase.** v1.3 or v1.4 cleanup — audit existing spin paths.

---

## §B Memory subsystem

### B.1 Transparent Huge Pages (THP)

Extends HORIZON-SURVEY §4.3.

**Kernel modes** (`/sys/kernel/mm/transparent_hugepage/enabled`):
- `always` — kernel tries to back every anonymous allocation with 2 MB pages. **Dangerous** — hash map growth can cause "huge page inflation" where a 50 MB allocation blows up to 2 GB of RSS.
- `madvise` — only when the app explicitly asks via `madvise(MADV_HUGEPAGE)`. **Correct for databases.**
- `never` — disabled.

**Recommended for Tally.** `madvise` system mode + `madvise(MADV_HUGEPAGE)` on the shard's entity HashMap region. Zero operator burden (most distros default to `madvise` already).

**Fragmentation concern.** After days of uptime, kernel memory fragments and huge page allocation starts to fail. `/proc/sys/vm/compact_memory` can force compaction. Prior art: Redis explicitly warns against `always` mode (documented in Redis's `redis-server` startup warnings) because of fork+COW RSS blow-up. Tally doesn't fork, so it dodges the specific Redis issue but the fragmentation concern still applies.

**Win size.** Documented 5–20 % speedup on hash-map-traversal-heavy workloads (ScyllaDB benchmarks, DragonflyDB). For Tally's shard store, MED confidence **3–8 % hot path speedup** from TLB miss reduction.

**Rust.** `nix::sys::mman::madvise(ptr, len, MADV_HUGEPAGE)`. But the tricky part is getting it called on the right allocation, which requires the allocator (mimalloc, jemalloc) to cooperate. Both do, behind the right feature flag.

**Target phase.** v1.4 polish (tied to NEXT-STEPS item 6 `mimalloc` evaluation).

### B.2 Explicit hugepages via `hugetlbfs`

Different from THP. `hugetlbfs` is a filesystem that exposes pre-reserved hugepages (2 MB or 1 GB). Allocate via `mmap(..., MAP_HUGETLB | MAP_HUGE_2MB, ...)` or via `memfd_create(..., MFD_HUGETLB | MFD_HUGE_2MB)`.

**Setup cost.** Operator must `echo 2048 > /proc/sys/vm/nr_hugepages` (reserves 4 GB of 2 MB pages). This is **prealloc, not on-demand**, so fragmentation risk is zero. Prior art: Scylla requires this, DPDK requires this, QEMU uses this.

**Tally application.** Per-shard HashMap backing storage in a dedicated hugepage-backed region. Ensures **zero TLB pressure regardless of kernel fragmentation state**.

**Complexity.** Requires a custom allocator or manual `mmap` on startup, slicing the hugepage region into per-shard subregions. Non-trivial — custom arena allocator work.

**Target phase.** v2 bundled with allocator refactor.

### B.3 `mlock` / `mlockall` — pin pages in RAM, no swap

**Syscall:** `mlock(addr, len)`, `mlockall(MCL_CURRENT | MCL_FUTURE)`.

**What it does.** Prevents the kernel from swapping the locked pages out under memory pressure.

**Tally.** We never want Tally's hot state swapped. Swap-on-hot-state is a latency catastrophe (~ms-scale stalls). `mlockall(MCL_CURRENT | MCL_FUTURE)` at startup pins everything.

**Cost.** `RLIMIT_MEMLOCK` must be high enough. Default is often 64 KB (useless). Operator must raise it via `ulimit -l unlimited` or systemd `LimitMEMLOCK=infinity`.

**Alternative.** Disable swap entirely (`swapoff -a`). Simpler, same effect for Tally.

**Recommendation.** **Document "disable swap on the host" as best practice**, and optionally implement `mlockall(MCL_CURRENT | MCL_FUTURE)` as a runtime option. Most production Tally deployments will be swapless.

**Rust.** `nix::sys::mman::mlockall`.

**Prior art.** Kafka, Elasticsearch (`bootstrap.memory_lock: true`), Redis all support mlockall. Redis docs recommend disabling swap, not mlock.

**Target phase.** v1.4 polish (the runtime option).

### B.4 NUMA awareness

**Syscalls:** `set_mempolicy`, `mbind`, `move_pages`, `numa_*` (libnuma).

**Why it matters.** On multi-socket servers (most high-core-count machines Tally wants to run on), cross-socket memory access is ~2× slower than local-socket access. A shard worker on socket 0 accessing entity state allocated on socket 1 pays a penalty on every operator read.

**The right pattern for Tally.**
1. At startup, detect NUMA topology (`libnuma` or `/sys/devices/system/node/`).
2. Pin each shard's thread to a specific core.
3. Ensure each shard's state is allocated on the same NUMA node as its thread.
4. Cross-shard channels are the only cross-NUMA traffic.

**How to actually do #3.**
- **mimalloc** has first-class NUMA awareness via `mi_option_numa_node`. Simple flag.
- **jemalloc** supports arenas and `MADV_WILLNEED` + NUMA placement hints, but the integration is more manual.
- Alternative: `set_mempolicy(MPOL_BIND, mask)` before each shard's allocation path. Requires Rust wrapper around libnuma or raw syscalls.

**Rust.** `libnuma-sys` (unmaintained 2022), `numa` crate (LOW maintenance), or roll via `nix`. **The ecosystem is thin.** Likely path: use mimalloc's NUMA mode and accept the rest.

**Prior art.** Scylla has full NUMA awareness (one reactor per core, memory local to that NUMA node). DragonflyDB same. Every tail-latency shop does this.

**Win size.** MED confidence **10–30 % latency tail improvement** on 2-socket hosts. Single-socket hosts: zero change.

**Target phase.** v2 (when we care about scaling above 16 cores, which implies multi-socket).

### B.5 `/proc/sys/vm/*` tunables

Dangerous waters. Some matter, most don't.

| Tunable | Default | Recommended for Tally | Rationale |
|---|---|---|---|
| `vm.swappiness` | 60 | **1 or 0** | Never prefer swapping Tally's hot state over dropping page cache |
| `vm.dirty_ratio` | 20 | lower (10) | Cap absolute dirty page amount; prevents large fsync tail |
| `vm.dirty_background_ratio` | 10 | 5 | Trigger background writeback earlier, smoother IO |
| `vm.dirty_expire_centisecs` | 3000 | 1500 (15 s) | Page can't stay dirty longer than this |
| `vm.dirty_writeback_centisecs` | 500 | 250 | Writeback wakes up more often |
| `vm.overcommit_memory` | 0 | 1 for Redis; **0 (default) for Tally** | Tally does not fork, Redis advice doesn't apply |
| `vm.min_free_kbytes` | auto | Consider raising on large-memory hosts | Keeps reclaim headroom, prevents stalls |
| `vm.max_map_count` | 65530 | Raise if mmap-heavy (restore path) | Needed if we mmap many segments |
| `vm.panic_on_oom` | 0 | 0 | Default — let the OOM killer work; Tally is not the kind of system that should panic |

**Recommendation.** Document a "tuned-host.md" with `sysctl` overrides. Do NOT ship Tally as "requires this sysctl" — the zero-ops promise holds.

**Target phase.** v1.4 docs.

### B.6 Allocator choice (extending HORIZON-SURVEY §4.3)

Post v1.3 shape is many small allocations in the hot path. The allocator shape that matters is:

| Allocator | Small-alloc speed | Multi-thread contention | NUMA | Huge pages | Binary size |
|---|---|---|---|---|---|
| **glibc malloc (ptmalloc)** | OK | Arena-per-thread, sometimes contended | No | Via THP only | 0 (system) |
| **jemalloc** | Good | Arenas, low contention | Yes (manual) | Yes | +1 MB |
| **mimalloc** | **Excellent** | Heap-per-thread, ~zero contention | Yes (flag) | **Yes, clean** | +500 KB |
| **snmalloc** | Excellent | Heap-per-thread, radix tree | Limited | Yes | +700 KB |
| **tcmalloc** | Excellent | Thread-caches | Yes | Yes | +1.5 MB |

**Rust ecosystem.**
- `mimalloc` crate (0.1.x, maintained, straightforward feature flag to enable huge pages)
- `tikv-jemallocator` (actively maintained by TiKV team, 0.6.x)
- `snmalloc-rs` (maintained, less ubiquitous)
- `tcmalloc` — no clean Rust crate; would need FFI.

**Recommendation for Tally.** **mimalloc with `mi-large-pages` feature flag** as the first move (it's already item 6 in NEXT-STEPS). snmalloc is the backup if mimalloc surprises. jemalloc is the "safe, proven" fallback.

**Custom arena per shard.** Beyond replacing the global allocator, v2 could use per-shard bump arenas for operator state that has a well-defined lifetime (e.g., "this bucket is valid until the next snapshot tick"). Win: **zero free cost** — entire arena is dropped at once. Cost: complexity of lifetime management (Rust's borrow checker will hate it).

**Target phase.** v1.4 for mimalloc swap. v2 for arena.

### B.7 cgroups v2 memory accounting

**Relevance:** Tally inside a container/Kubernetes pod. `memory.max` hard limit triggers OOM if exceeded; `memory.high` causes the kernel to throttle allocations aggressively.

**Tally behavior.** Nothing special — Rust's allocator propagates OOM to `handle_alloc_error` which aborts. If Tally wants graceful degradation under cgroup memory pressure, it needs to monitor memory.current and shed load. Out of scope for v1.4; v2 "graceful OOM" feature maybe.

**Recommendation.** Document running Tally with `memory.max` set to expected RSS + 10 % headroom, and swap disabled. Target phase: docs.

---

## §C Kernel tunables and cgroups (summary — see §B.5 for detail)

Covered inline above. No separate content.

---

## §D Networking path

### D.1 `SO_REUSEPORT` — kernel load balance across shard accept threads

**Sockopt:** `setsockopt(sock, SOL_SOCKET, SO_REUSEPORT, &1, sizeof(int))`. Linux 3.9+.

**What it does.** Multiple sockets can bind to the same port. The kernel hashes the 4-tuple of each incoming connection to pick which socket receives it, and does the accept there.

**Today's Tally (v1.3 plan).** Single accept thread. It reads `OP_*` frames and dispatches to shards via crossbeam channels. The accept thread is a serialization bottleneck above ~500 K conns/s (probably not an issue, but "probably" is uncomfortable).

**`SO_REUSEPORT` alternative.** Each shard worker binds its own socket to port 6400 with `SO_REUSEPORT`. The kernel distributes connections via 4-tuple hash. **No cross-thread handoff on accept.** Each shard processes its own connections end-to-end.

**Problem.** The kernel's 4-tuple hash doesn't align with Tally's routing hash (which is keyed by entity key, not socket 4-tuple). A connection lands on shard X but its PUSH events are for shard Y → crossbeam channel anyway. So reuseport only removes the **accept** bottleneck, not the routing bottleneck.

**Verdict.** **Useful but not transformative.** Adopt in v2 if accept shows up in profiles. Otherwise defer.

**Rust.** `socket2` crate (maintained) exposes `set_reuse_port`.

**Prior art.** nginx (since 1.9.1), HAProxy, Envoy, every modern HTTP server.

### D.2 `TCP_NODELAY` / `TCP_QUICKACK` / `TCP_CORK`

- **`TCP_NODELAY`** — disable Nagle's algorithm. **Required** for Tally's small-frame protocol, assumed already set (verify).
- **`TCP_QUICKACK`** — disable delayed ACKs. Combined with `TCP_NODELAY` gives sub-100 µs RTTs reliably. Must be set repeatedly because Linux auto-clears it after some time. MED confidence win for Tally.
- **`TCP_CORK`** — batch small writes until uncorked. Opposite of `TCP_NODELAY`. Useful for response batching: cork, write all N response frames, uncork → one packet. Worth using for Tally's batched `push_many` responses.

**Rust.** `socket2` + `nix::sys::socket::sockopt` expose all three.

**Target phase.** v1.4 audit.

### D.3 `SO_BUSY_POLL` — poll NIC directly from recv path

**Sockopt.** `setsockopt(sock, SOL_SOCKET, SO_BUSY_POLL, &usec, 4)`. Linux 3.11+.

**What it does.** When the socket recv queue is empty, busy-poll the NIC for up to `usec` microseconds instead of parking the thread. **Saves ~5 µs** of wakeup latency on each small-frame receive, by skipping the NIC interrupt → softirq → socket wake → scheduler path.

**Caveat.** The NIC driver and kernel must support it. Intel drivers do; most do. Requires `net.core.busy_poll` sysctl > 0 as well. Actively uses the CPU while polling — fine on pinned shard cores, bad otherwise.

**For Tally.** MED confidence **~5 µs tail improvement** on incoming PUSH. Aligns well with the "dedicated shard core" model.

**Rust.** `setsockopt` via `nix`. No ecosystem wrapper.

**Prior art.** Aeron, Chronicle, high-frequency-trading stacks.

**Target phase.** v2 alongside io_uring networking.

### D.4 `SO_TIMESTAMPING` — hardware RX timestamps

**Sockopt.** `setsockopt(sock, SOL_SOCKET, SO_TIMESTAMPING, &flags, 4)` with `SOF_TIMESTAMPING_RX_HARDWARE` etc.

**What it does.** The NIC stamps each incoming packet with a hardware timestamp at wire-arrival time. The app reads it via `recvmsg` `cmsg`.

**For Tally's Phase 10.2 LatencyTracker** — measuring "time from wire to response" is currently measured from "time read returned in our code", which misses scheduler/IRQ latency. With `SO_TIMESTAMPING`, Tally's latency histogram becomes **true end-to-end** rather than "after Linux handed us the bytes."

**Cost.** ~100 ns per read to fetch the cmsg. NIC must support it (modern Intel/Mellanox do).

**Rust.** `nix::sys::socket::recvmsg` exposes cmsg. No high-level wrapper. Would require code plumbing.

**Target phase.** v2 observability improvement.

### D.5 `MSG_ZEROCOPY` / `SO_ZEROCOPY`

**Sockopt + flag:** `setsockopt(SO_ZEROCOPY)`, then `send(..., MSG_ZEROCOPY)`.

**What it does.** The kernel pins the user buffer and DMAs directly from it to the NIC, no memcpy. **Only a win for sends > ~8 KB.** Completion signals via `MSG_ERRQUEUE` tell you when it's safe to reuse the buffer.

**For Tally.** Useful for large responses (snapshot-over-HTTP, bulk MGET responses). Not useful for small PUSH responses.

**Verdict.** **Defer until bulk responses matter.**

**Target phase.** v2 HTTP API path.

### D.6 XDP / AF_XDP — kernel-bypass receive

**What it is.** eBPF program attached to the NIC driver, runs before the kernel network stack. Can redirect packets directly to a userspace ring (AF_XDP). Full bypass of the TCP/IP stack.

**For Tally.** Tally speaks TCP, not raw frames. Using AF_XDP means reimplementing TCP in userspace. This is **DPDK territory**, and Tally rejected DPDK.

**Verdict.** **Anti-philosophy. Skip.** See §H.

### D.7 `io_uring` socket ops — replace tokio epoll?

**Relevant SQEs.**
- `IORING_OP_ACCEPT` — async accept
- `IORING_OP_RECV` / `IORING_OP_SEND` — async recv / send
- `IORING_OP_RECVMSG` / `IORING_OP_SENDMSG`
- `IORING_OP_SEND_ZC` / `IORING_OP_RECV_ZC` — zero-copy variants (6.0+)
- Multi-shot accept and recv (6.0+) — one SQE receives N connections or N messages with no resubmission

**What it replaces.** Tokio uses epoll under the hood. For small-frame TCP workloads like Tally, io_uring socket ops aren't dramatically faster than epoll in terms of raw throughput (tokio is already excellent). The wins are:
- Unified I/O path — same ring for storage + network
- Fewer syscalls per operation (tokio issues 2 syscalls per readable edge: `epoll_wait` + `read`)
- Zero-copy option for large sends

**For Tally.** If §1 (storage io_uring) lands, extending to network io_uring is natural. Otherwise, epoll is fine.

**Rust crates.** `tokio-uring`, `compio`, `glommio` all cover this. See `HORIZON-STORAGE-IO.md` §1.2 for the crate comparison.

**Target phase.** v2 if the storage io_uring work lands.

### D.8 kTLS — kernel TLS offload

**What it is.** Linux offloads TLS symmetric encrypt/decrypt into the kernel (and optionally NIC hardware). `setsockopt(TCP_ULP, "tls")` enables it. Combined with `sendfile` it gives true zero-copy TLS.

**For Tally.** Tally uses plain TCP today. If TLS becomes a requirement for v2+, kTLS is how to do it without paying 30 % CPU to encryption.

**Rust.** `tokio-rustls` + kernel TLS experiments — no production wrapper. Ecosystem gap.

**Target phase.** v2+ if/when TLS is in scope.

---

## §E Observability and profiling for tail latency

### E.1 `perf` / `perf c2c` for false sharing

**Tool.** `perf c2c record` + `perf c2c report` — Cache-to-Cache profiling. Shows cache lines that are ping-ponging between cores due to false sharing.

**Tally relevance.** Post v1.3, sharding means each shard is supposed to be isolated. If a shared atomic (e.g., metrics counter, throughput tracker) sits in a cache line shared across shards, false sharing tanks tail latency. v1.3 PITFALL C-6 already flags this as a bench gate. `perf c2c` is how we **actually verify** the prevention.

**Recommendation.** Make `perf c2c` part of v1.3 acceptance testing. Also v1.4 and beyond.

**Prior art.** Standard technique; every shared-nothing codebase uses it.

### E.2 `perf stat -e L1-dcache-load-misses` as cache locality proxy

Standard PMU event. High L1 miss rate on a shard worker means bad cache layout. Tally should include a `scripts/perf-profile.sh` that collects these counters.

**Target phase.** v1.4 developer experience.

### E.3 eBPF tracing (bpftrace, libbpf-rs)

**What it is.** Attach trace programs to kernel tracepoints, kprobes, or uprobes. Runtime, no restart, no rebuild. Collect anything.

**For Tally.** Production tail-latency debugging. Example bpftrace one-liners:
- Per-syscall latency histogram on a Tally process
- Scheduler wakeup delay for shard workers (`sched:sched_wakeup` → `sched:sched_switch`)
- Page fault rate per thread
- `fsync` latency histogram

**Rust ecosystem.** `libbpf-rs` (maintained, Meta/bpf team involvement). `aya` (pure-Rust eBPF framework, active). **HIGH** confidence on production readiness.

**For Tally specifically.** Tally could **ship USDT probes** (`<sys/sdt.h>` style static probes) that external bpftrace scripts hook into. Rust crate: `probe` (some maintenance questions), or roll our own via inline asm.

**Target phase.** v1.4 dev tooling; v2 USDT probes.

### E.4 `rdtsc` for sub-µs instrumentation

**Instruction.** `RDTSC` / `RDTSCP` — reads the CPU's TimeStamp Counter, sub-nanosecond resolution.

**Caveat.** On modern Intel/AMD, the TSC is invariant (runs at constant rate regardless of CPU frequency), but comparing TSCs across cores requires the TSC to be synchronized (`constant_tsc` + `nonstop_tsc` flags in /proc/cpuinfo, which is standard post-2010).

**For Tally's LatencyTracker (Phase 10.2).** If it uses `std::time::Instant` today, that's `clock_gettime(CLOCK_MONOTONIC)` under the hood, which is ~20–30 ns per call on Linux (vDSO). RDTSC is ~5 ns. Difference matters only for very tight loops; probably **not worth changing** for Tally's current latency measurements.

**Rust.** `core::arch::x86_64::_rdtsc` (stable). `quanta` crate wraps it cross-platform and handles TSC calibration.

**Verdict.** **Consider `quanta` for LatencyTracker** if `Instant`-overhead shows up. Otherwise keep `Instant`.

### E.5 `clock_gettime` variants

- `CLOCK_MONOTONIC` — steady, adjusts for NTP slew. **Default for latency measurement.** `std::time::Instant` uses this.
- `CLOCK_MONOTONIC_RAW` — like above but ignores NTP. Raw hardware. Slightly cheaper.
- `CLOCK_REALTIME` — wall clock, can jump. **Correct for event timestamps in Tally**, matching what PROJECT.md says about `SystemTime` (already in use).
- `CLOCK_BOOTTIME` — monotonic including suspend time. Not relevant.

**Tally's mix.** `SystemTime` for event timestamps (right), `Instant` for latency measurement (right). **No change needed.**

### E.6 Chrome trace format / perfetto

For visualizing shard worker activity. Tally's debug UI could emit Chrome trace JSON that users pipe into `chrome://tracing` or perfetto.dev.

**Rust.** `tracing` crate + `tracing-chrome` subscriber → directly emits the format.

**Target phase.** v1.4 debug UI enhancement.

### E.7 Spin backoff primitives — recap

Covered in §A.6. `core::hint::spin_loop()` / `_mm_pause` for tight waits; `crossbeam-utils::Backoff` for structured backoff; never `sched_yield` on hot paths.

---

## §F Deterministic tail-latency — jitter sources

**Sources of jitter on a shard worker core, ordered by impact:**

| Jitter source | Typical cost | Mitigation |
|---|---|---|
| Kernel preemption (another `SCHED_OTHER` task) | 10–100 µs | `isolcpus` + affinity + optional `SCHED_FIFO` |
| Scheduler tick (1000 Hz) | ~1 µs/tick | `nohz_full` |
| NIC interrupts | 3–5 µs | IRQ affinity off hot cores |
| NVMe completion interrupts | 3–5 µs | IRQ affinity off hot cores |
| Timer interrupts (high-res timers) | ~1 µs | `nohz_full` |
| RCU callbacks | < 1 µs but bursty | `rcu_nocbs` |
| TLB shootdowns (other cores' mmap/munmap IPIs) | ~1 µs | Avoid mmap churn; huge pages reduce frequency |
| Page faults (minor) | ~1 µs | `mlockall` or never evict |
| Page faults (major, disk read) | 50–100+ µs | Never — never mmap write path |
| Cache pollution from co-located threads | Variable | Pin + isolate |
| THP compaction stalls | Bursty, ms-scale | Use `madvise` mode THP, not `always` |
| Allocator slow paths | ~1–10 µs | mimalloc / arena allocator |

**Combined:** a fully-tuned Scylla shard on `isolcpus + nohz_full + rcu_nocbs + mlockall + IRQ affinity + huge pages + mimalloc` gets **sub-20 µs p99.999** on point operations. That's the theoretical ceiling for Tally — assuming the operator does the tuning.

**Recommendation.** Document all of this in a "tuned host" guide. Tally code does what it can (mlockall, mimalloc, huge-pages-via-madvise, shard pinning, IRQ-aware defaults). Operator does the rest on dedicated hosts.

---

## §G Container and deployment realities

### G.1 Kubernetes throttling

`cpu.max` in cgroups v2 (or `cpu.cfs_quota_us` in v1) throttles a container's CPU share per period. The throttler runs on a 100 ms period by default. **This is a tail-latency catastrophe for Tally** — a shard worker busy-waiting at the end of a period gets paused for tens of ms until the next period starts.

**Mitigation options:**
1. **Set CPU limits generously** — give Tally more than it needs. "Don't throttle me."
2. **Use CPU requests without limits** (Kubernetes pattern). Scheduler allocates but kernel doesn't throttle.
3. **Use static CPU manager policy** — Kubernetes pins whole cores to the pod, kernel uses `sched_setaffinity` equivalent.
4. **Use cgroups v2 `cpuset.cpus`** directly for bare-metal-like pinning inside a container.

**Recommendation.** Document these in the K8s deployment guide. Tally code: nothing to change. Target phase: docs v1.4.

### G.2 Network stack overhead in containers

Going through a veth bridge adds ~5–10 µs per packet compared to host networking. For Tally's <100 µs p99 target, that's 5–10 % of the budget consumed.

**Mitigation:**
- **host network mode** (`hostNetwork: true` in K8s) — eliminates the veth. But port namespacing goes away; conflicts with other host-network pods.
- **MACVLAN / IPVLAN** — assigns real MAC/IP to the container, no bridge. More complex but keeps isolation.
- **Cilium (eBPF-based)** — replaces iptables with eBPF for service routing, faster than kube-proxy. **MED confidence ~2–3 µs tail improvement** over standard kube-proxy.

**Target phase.** v1.4 docs.

### G.3 "install-optimized.sh" — the grand unification

The recipe for a fully-tuned Tally host:
```
# Kernel command line
isolcpus=4-15 nohz_full=4-15 rcu_nocbs=4-15
# sysctl
vm.swappiness=1
vm.nr_hugepages=2048
net.core.busy_poll=50
kernel.sched_rt_runtime_us=-1
# Runtime
swapoff -a
systemctl disable irqbalance
echo 0 > /sys/class/net/eth0/device/sriov_numvfs
```

**Should Tally ship this script?** The gstack philosophy says zero-ops wins over peak perf. **Don't ship a mandatory script**. Do ship:
- A documented recipe in `docs/deployment/tuned-host.md`
- A `tally doctor --check-tuning` command that reports what's suboptimal
- Runtime detection that adapts to whatever's there

**Target phase.** v1.4 docs; v2 `tally doctor` command.

---

## §H Anti-philosophy items — scoped and deprioritized

Each item answers: why skip in v1.4–v2, what would make us revisit in v3+.

### H.1 Kernel-bypass networking (DPDK, AF_XDP)

**Why skip.** Reimplements TCP in userspace. Breaks "runs anywhere" — needs specific NIC support, dedicates the NIC, requires privileged operation. Tally's gains from io_uring network ops (§D.7) are already 70 % of what DPDK offers at 5 % of the operator cost.
**Revisit in v3 when.** A customer legitimately needs 10M packets/sec/node on a single Tally process — which is far beyond the "single-binary zero-ops" target.

### H.2 Real-time kernel (PREEMPT_RT / RHEL RT)

**Why skip.** Requires a custom kernel. Complicates every package/distro/cloud deployment. Benefit (~µs tail improvement on ~µs jitter sources) is smaller than the cost.
**Revisit when.** Tally becomes the platform for literal hard-real-time work (trading, telecom RAN). Unlikely product direction.

### H.3 Custom kernel modules

**Why skip.** Violates every zero-ops premise. No Rust ecosystem. DKMS maintenance burden.
**Revisit when.** Never.

### H.4 `isolcpus` + kernel cmdline as a **hard requirement**

**Why skip.** Requires operator to reboot with kernel parameters — anti-philosophy.
**Soft-ship.** Document as best practice; runtime-detect and adapt. Already in recommendation above.

### H.5 Persistent-memory code paths (`clwb` / `sfence`)

**Why skip.** Optane is dead. CXL PMEM is 2027+ mainstream. Code complexity for unvalidated hardware.
**Revisit when.** CXL 3.0 persistent memory lands in the hardware Tally runs on and demonstrates a clear durability win.

### H.6 SPDK userspace NVMe

**Why skip.** Dedicates the drive. No filesystem. Breaks every operational tool. Duplicate of HORIZON-STORAGE-IO §8.1 verdict.
**Revisit when.** Kernel path is measurably the storage bottleneck post-io_uring — unlikely on 2026+ NVMe.

### H.7 Kernel TLS offload

**Why skip for v1.4-v2.** Tally speaks plain TCP; TLS is not on the milestone roadmap until a customer requires it.
**Revisit when.** TLS gets promoted from "out of scope" to "required."

### H.8 Fork-based BGSAVE (Redis model)

**Why skip.** Incompatible with thread-per-shard + parking_lot. Phase 15 off-thread snapshot is the correct answer.
**Never revisit.**

---

## §I Per-hack decision matrix

Legend: Lat win = effect on PUSH p99 tail. Mem win = RSS / density effect. Complexity is implementation effort. Maturity H/M/L.

| # | Hack | Syscall / primitive | Rust crate | Prior art | Lat win | Mem win | Complexity | Maturity | Phase |
|---|---|---|---|---|---|---|---|---|---|
| A1 | Pin all Tally threads (including housekeeping) | `sched_setaffinity` | `core_affinity` | Scylla | Small (µs) | 0 | Low | H | v1.3/v1.4 |
| A2 | `SCHED_FIFO` for shard workers (opt-in) | `sched_setscheduler` | `thread-priority`, `nix` | Chronicle, Aeron | Medium (10 µs tail) | 0 | Med (config + doc) | H | v2 opt-in |
| A3 | `SCHED_DEADLINE` | `sched_setattr` | `nix` raw | academic | Small | 0 | High | M | **skip** |
| A4 | `isolcpus + nohz_full + rcu_nocbs` | kernel cmdline | N/A | Scylla, DPDK | **Large (20+ µs tail)** | 0 | Docs only | H | **v1.4 docs** |
| A5 | IRQ affinity off shard cores | `/proc/irq/*/smp_affinity` | N/A | Every low-latency shop | Medium (5 µs tail) | 0 | Docs only | H | **v1.4 docs** |
| A6 | Spin-wait with `_mm_pause` (replace sched_yield) | `core::hint::spin_loop` | `crossbeam-utils::Backoff` | std | Small | 0 | Trivial | H | **v1.3 cleanup** |
| B1 | `MADV_HUGEPAGE` on shard state | `madvise` | `nix`, alloc integration | Scylla, DragonflyDB | Medium (3–8 %) | 0 | Low | H | **v1.4** (with mimalloc) |
| B2 | `hugetlbfs` explicit huge pages via `memfd(HUGETLB)` | `memfd_create`, `mmap` | `memfd`, `nix` | Scylla, DPDK | Medium | 0 | Med (custom alloc) | M | v2 |
| B3 | `mlockall(MCL_CURRENT\|MCL_FUTURE)` | `mlockall` | `nix` | Redis, Elasticsearch | Small | 0 | Low | H | v1.4 |
| B4 | NUMA-local allocation per shard | `set_mempolicy` | `libnuma-sys` (unmaintained) / mimalloc NUMA | Scylla | **Large on 2-socket (10–30 %)** | 0 | Med (alloc integration) | M | v2 |
| B5 | `vm.swappiness=1`, related sysctls | sysctl | N/A | Redis, Kafka, etc. | Small-medium | 0 | Docs only | H | **v1.4 docs** |
| B6 | mimalloc allocator swap | `#[global_allocator]` | `mimalloc` | DragonflyDB | 3–8 % | ~5 % RSS | Trivial | H | **v1.4** (NEXT-STEPS #6) |
| B7 | snmalloc alternative | same | `snmalloc-rs` | research | Similar | Similar | Trivial | H | **fallback for B6** |
| B8 | Per-shard bump arena | custom | custom | Scylla reactor | 3–10 % (alloc removal) | Can save RSS | High (lifetimes) | M | v2 |
| D1 | `SO_REUSEPORT` for shard accept | sockopt | `socket2` | nginx, Envoy | Small (accept path) | 0 | Med | H | v2 |
| D2 | `TCP_NODELAY` + `TCP_QUICKACK` + `TCP_CORK` | sockopt | `socket2`, `nix` | every low-latency server | Small (µs) | 0 | Trivial | H | **v1.4 audit** |
| D3 | `SO_BUSY_POLL` on incoming socket | sockopt | `nix` | Aeron, HFT stacks | Medium (~5 µs) | 0 | Low | M | v2 |
| D4 | `SO_TIMESTAMPING` for LatencyTracker accuracy | sockopt + cmsg | `nix` raw | observability stacks | 0 (measurement accuracy) | 0 | Med | H | v2 |
| D5 | `MSG_ZEROCOPY` for large HTTP responses | send flag | `nix` | nginx, Envoy | 0 hot; mgmt path faster | 0 | Low | H | v2 |
| D6 | XDP / AF_XDP | eBPF program | `aya`, `libbpf-rs` | Cilium | Large (10+ µs) | 0 | Very high | M | **skip (H.1)** |
| D7 | `io_uring` network ops | `IORING_OP_RECV/SEND` | `compio`/`tokio-uring` | DragonflyDB, Envoy | Small-medium | 0 | High | H | v2 (tied to STORAGE-IO §1) |
| D8 | kTLS | `setsockopt(TCP_ULP, "tls")` | ecosystem gap | nginx | TLS path | 0 | High | M | **skip until TLS in scope** |
| E1 | `perf c2c` as bench gate | profiling tool | N/A | standard | Diagnostic | 0 | Low | H | **v1.3 gate** |
| E2 | `perf stat` L1 miss as health metric | profiling tool | N/A | standard | Diagnostic | 0 | Low | H | v1.4 dev tool |
| E3 | eBPF USDT probes shipped by Tally | static probes | `probe` (LOW), roll own | Java stacks use, Rust ecosystem thin | Diagnostic | 0 | Med | M | v2 |
| E4 | `RDTSC` / `quanta` in LatencyTracker | `_rdtsc` | `quanta` | HFT | Small (measurement overhead) | 0 | Low | H | v1.4 polish if needed |
| E5 | Chrome trace format via `tracing-chrome` | - | `tracing-chrome` | standard | Diagnostic | 0 | Low | H | v1.4 debug UI |
| F1 | "Tuned host" docs w/ all jitter mitigations | docs | N/A | Scylla, Aeron | **Large combined** | 0 | Docs | H | **v1.4 docs** |
| G1 | K8s CPU-limits-or-requests guidance | docs | N/A | every K8s shop | Large (avoid throttle) | 0 | Docs | H | **v1.4 docs** |
| G2 | Host network / MACVLAN recipes | docs | N/A | every low-latency shop | Medium (5–10 µs) | 0 | Docs | H | **v1.4 docs** |
| G3 | `tally doctor --check-tuning` command | read /proc | stdlib | — | 0 direct, great DX | 0 | Med | H | v2 |
| H1 | DPDK | userspace NIC | ecosystem gap | openvswitch, high-freq | — | — | Very high | — | **skip (H.1)** |
| H2 | PREEMPT_RT kernel | kernel build | N/A | telco | Small | — | Huge operator burden | — | **skip (H.2)** |
| H3 | `clwb` / `sfence` PMEM | x86 instructions | `pmemobj-rs` (unmaintained) | Intel PMDK | — | — | High | L | **skip until v3 CXL PMEM** |

### Tier recommendations

**v1.3 tail (days):** A6 (audit spin paths), E1 (`perf c2c` as bench gate).
**v1.4 cheap wins:** A1, B1, B3, B5 (docs), B6 (mimalloc), D2, G1 (docs), G2 (docs), A4 (docs), A5 (docs), F1 (docs), E5.
**v1.4 stretch:** E4 (quanta), E2 (dev tooling).
**v2 meaningful:** A2 (SCHED_FIFO opt-in), B2 (hugetlbfs), B4 (NUMA), B8 (arena), D1 (reuseport), D3 (busy_poll), D4 (timestamping), D5 (MSG_ZEROCOPY), D7 (io_uring net), E3 (USDT), G3 (tally doctor).
**v3 speculative / track only:** H3 (CXL PMEM).
**Skip:** A3, D6, D8, H1, H2.

### Sources & citations

- Linux kernel docs: `man 2 sched_setaffinity`, `man 2 madvise`, `man 2 mlockall`, `man 7 sched`, `man 2 io_uring_setup`.
- Crotty, Leis, Pavlo. *Are You Sure You Want to Use MMAP in Your Database Management System?* CIDR 2022.
- Linux `io_uring` Jens Axboe LWN articles (2019–2024).
- Scylla docs (scylladb.com/docs): CPU assignment, NUMA, isolcpus, `SO_BUSY_POLL`, huge pages.
- DragonflyDB blog posts: Helio, io_uring, allocator choice, NUMA.
- Meta TMO (ATC 2022, Weiner et al.), TPP (OSDI 2023/2024, Zhong et al.) — memory tiering and MADV_COLD.
- Chronicle Queue / Aeron docs — SCHED_FIFO + affinity + busy_poll.
- Cilium documentation for eBPF service routing latency.
- NVMe spec 1.4 / 2.0.
- mimalloc repo (microsoft/mimalloc) — NUMA / huge pages design doc.
- Tokio `io_uring` integration notes (tokio-uring, compio, glommio repos).
- `aya` eBPF framework, `libbpf-rs` — Rust eBPF ecosystem.
- Kubernetes CPU manager static policy (kubernetes.io/docs).
- systemd `LimitMEMLOCK`, sysctl reference.
- Andrea Arcangeli's THP design notes (Red Hat blog, 2011–2014, still canonical).

---

*Written 2026-04-11. Sibling: `HORIZON-STORAGE-IO.md` for storage / snapshot / event-log I/O specifics.*
