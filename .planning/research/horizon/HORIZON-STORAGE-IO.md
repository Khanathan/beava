# Horizon Research — Storage & I/O Hacks

**Date:** 2026-04-11
**Scope:** Everything between Tally's Rust code and the NVMe/SSD. Narrow, deep. Long horizon (v1.4 → v2 → v3).
**Companion to:** `HORIZON-SURVEY.md` (do not duplicate §4 — this document extends it with syscall-level detail), `HORIZON-COMBINATIONS.md`, `HORIZON-NEXT-STEPS.md`.
**Confidence scale:** HIGH = authoritative kernel/crate docs; MED = benchmarks/blog posts; LOW = extrapolation.

---

## Framing: what Tally does with the disk today

From `src/state/snapshot.rs` and `src/state/event_log.rs` (read 2026-04-11):

- **Event log** — per-stream `BufWriter<File>` opened `O_APPEND`, `write_all` on every PUSH (~100–300 ns memcpy into the BufWriter). fsync done periodically on a background timer. Compaction rewrites the file from scratch, excluding entries older than `history_ttl` (default 72 h).
- **Snapshots** — clone state on main thread, `spawn_blocking` a worker that writes `tmp → rename` via stdlib `File::create` + `postcard::to_io_writer`, then `sync_all`. Format v6 with base + delta (incremental). v1.3 Phase 15 moves the clone off the main thread too, via a manifest-based v7 format.
- **Restore** — `File::open` + stdlib `BufReader`, reads the full snapshot on startup. Single-pass, single-threaded.
- **Runtime** — `#[tokio::main(flavor = "current_thread")]` until v1.3; after v1.3, thread-per-shard with `parking_lot::Mutex<ShardStore>` and shard-local event log directories `events/shard-N/`.

**The gap this document fills.** None of the above touches io_uring, `O_DIRECT`, `fallocate`, `fadvise`, `madvise`, `splice`, `sync_file_range`, NVMe hints, persistent memory, or kernel-bypass storage. Tally is leaving a lot of kernel-level latency and density on the table. Below is what the state of the art looks like in 2026.

---

## §1 Async I/O subsystem — io_uring and friends

### 1.1 `io_uring` in 2026 — where it actually lives

io_uring went from "experimental Linux 5.1 feature (2019)" to **the default async I/O surface on Linux 6.x** (current LTS 6.6, stable 6.12+). In 2024–2025 it picked up:

- `IORING_SETUP_SQPOLL` — kernel-side poll thread that drains the submission queue without any syscall at all on the hot path. The application writes to a shared ring, the kernel thread picks it up. Zero syscall per I/O once warm. (HIGH confidence — Axboe's talks, kernel docs.)
- `IORING_SETUP_DEFER_TASKRUN` (6.1+) — completion processing happens only on explicit `io_uring_enter`, which means the kernel won't interrupt your worker thread at arbitrary points. Critical for tail latency on shard workers. (HIGH)
- `IORING_REGISTER_BUFFERS` + `IORING_OP_WRITE_FIXED` / `READ_FIXED` — pre-registered DMA-able buffers, avoiding the per-op page-pinning cost. ~15–30 % throughput win on small writes. (MED — Axboe benchmarks)
- `IORING_OP_FSYNC` / `IORING_OP_FDATASYNC` — fsync as an async SQE, no blocking thread.
- `IOSQE_IO_LINK` — chain SQEs so "write → fsync → notify" is a single kernel transition. Exactly the pattern Tally wants for group-commit fsync.
- `IORING_OP_SEND_ZC` / `RECV_ZC` (6.0+) — zero-copy network I/O via `SO_ZEROCOPY`.

**Security note.** Major cloud providers (Google, Docker, a few distros) have restricted io_uring in 2023–2024 due to the CVE stream. It's still available but operators may need `sysctl kernel.io_uring_disabled=0`. HIGH confidence — documented in `kernel.io_uring_disabled` man page.

### 1.2 Rust crate landscape (checked crates.io April 2026 to the best of training data; verify before adoption)

| Crate | Level | Maintenance | Runtime model | Fit for Tally |
|---|---|---|---|---|
| **`io-uring`** (tokio-rs) | Low-level bindings | Active (tokio-rs org, Axboe co-maintainer for a while) | Any | Use as the FFI floor. Still requires a reactor on top. |
| **`tokio-uring`** | High-level over tokio | **Semi-active**, not fully integrated into mainline tokio; separate runtime builder | Own runtime, incompatible with regular `tokio::spawn` tasks | **Medium fit.** Conflicts with v1.3's thread-per-shard + parking_lot design. |
| **`glommio`** | Full thread-per-core runtime | DataDog sponsored; maintenance slowed 2023–2024 | Thread-per-core, shard-local, polled io_uring | **Architecturally closest to v1.3 shape.** But it replaces tokio entirely — huge blast radius for Tally. |
| **`compio`** | Cross-platform io_uring / IOCP / kqueue | Active (2023+), rapidly maturing | Own runtime; compatible with standard futures | **Best fit for a post-v1.3 migration that wants Linux io_uring but portable fallback.** |
| **`rio`** (Tyler Neely / sled) | Minimal io_uring | **Archived / unmaintained.** Tyler's own post says don't use it. | Callback | Skip. |
| **`monoio`** (ByteDance) | Thread-per-core, io_uring | Active | Own runtime, !Send futures | Similar to glommio; Asian fintech production references. |

**Recommendation for Tally.** Don't rip out tokio. Instead use **`io-uring` crate directly for the event-log append path and the snapshot path on a dedicated I/O thread per shard**, while keeping tokio for accept, protocol framing, and the HTTP management API. This is the "io_uring as a co-processor" model that Scylla-without-Seastar systems use.

**Prior art for this hybrid model:** ScyllaDB's Seastar (C++) gives up tokio-equivalents entirely; DragonflyDB uses io_uring via Helio; Envoy added io_uring support as an opt-in extension in 2023 (`envoy.bootstrap.internal_listener` extensions). Vector (Datadog log router) added optional io_uring for sinks in 2024.

### 1.3 Concrete wins for Tally

| Current path | io_uring path | Est. win |
|---|---|---|
| `BufWriter::write_all` per event → epoch fsync via `spawn_blocking` | Pre-registered buffer ring; `IORING_OP_WRITE_FIXED` + linked `IORING_OP_FDATASYNC` on group-commit boundary; SQPOLL drain | **Eliminates `spawn_blocking` cost (~2–5 µs per fsync dispatch today); fsync amortizes over 64–256 events for free** |
| `spawn_blocking` snapshot write (clone + stdlib write + sync_all) — Phase 15 plan | `O_DIRECT` open, `IORING_REGISTER_BUFFERS` for the postcard output ring, chained write chunks + `IORING_OP_FSYNC` as last SQE | **Zero page-cache pollution; snapshot write doesn't evict hot entity state from the kernel cache**. ~2–3× snapshot write throughput on modern NVMe. |
| Snapshot restore: single-threaded BufReader | `IORING_OP_READ_FIXED` with 4–8 in-flight reads into a ring; parallel postcard decode | **~3–5× snapshot restore wall time** on 1 M-entity snapshot |
| Event log replay (backfill path, v1.1): stdlib `BufReader` | Same, multi-reader ring | ~3–5× |

### 1.4 Per-shard ring vs shared ring (post v1.3)

Post-v1.3 Tally has thread-per-shard. Two options:

- **Per-shard io_uring.** Each shard worker owns a ring; SQPOLL kernel thread per ring is expensive (one extra kernel thread per shard). Solution: `IORING_SETUP_ATTACH_WQ` shares the backing kernel worker pool between rings. HIGH confidence — kernel docs.
- **Shared io_uring on a dedicated I/O thread.** Shard workers hand off I/O requests via `crossbeam-channel`; one I/O thread owns the ring. Simpler, adds a hop. MED confidence this is a latency win over per-shard rings.

**Recommendation:** **per-shard io_uring with `IORING_SETUP_ATTACH_WQ`** for SQPOLL pool sharing. Matches Scylla/Seastar's per-reactor-ring model, matches v1.3's shared-nothing philosophy.

### 1.5 Target phase

**v1.4 stretch or v2.** Nontrivial. Requires moving event log + snapshot paths off stdlib. Plan as a dedicated phase after v1.3 ships, with a benchmark gate: show event log append cost drops below 500 ns including fsync-amortized cost, or don't ship.

---

## §2 Direct I/O and page cache control

### 2.1 `O_DIRECT` for snapshot writes

**Syscall:** `open(..., O_DIRECT | O_WRONLY | O_CREAT, ...)`.

**What it does.** Bypasses the Linux page cache entirely. Data goes straight from the userspace buffer to the disk DMA controller. No double-buffering. No page-cache eviction pressure on hot entity state.

**Why it matters for Tally.** Today's snapshot write pollutes the page cache with ~100 MB to several GB of "written once, never read" pages. Those pages compete with hot state (if Tally ever mmaps anything — today it doesn't, but see §5). Even without mmap, the page cache pollution affects *other services on the same host*, which matters in multi-tenant deployments.

**Alignment requirements (HIGH confidence, POSIX documented):**
- Buffer address must be aligned to block size (typically 4096; 512 on some older devices).
- Buffer length must be a multiple of block size.
- File offset must be aligned to block size.

**Implication for postcard.** Postcard produces variable-length output. Options:
1. **Pad the postcard stream to 4 KiB boundaries** at the end of each chunk. ~0.05% overhead amortized.
2. **Double-buffered write pattern** — write into a 1 MiB aligned buffer (`std::alloc::Layout::from_size_align(1<<20, 4096)`), flush when full, rotate. This is the standard pattern.

**Rust crate support.** No dedicated `O_DIRECT` crate needed — `std::os::unix::fs::OpenOptionsExt::custom_flags(libc::O_DIRECT)` is all it takes. The `aligned-buffer` and `aligned-vec` crates (or manual `alloc::alloc`) handle the buffer alignment.

**Prior art.** PostgreSQL added optional `O_DIRECT` for WAL in 2023 (`wal_sync_method=open_direct`). MySQL/InnoDB has supported `innodb_flush_method=O_DIRECT` for ~15 years. ClickHouse uses `O_DIRECT` for large background merges. ScyllaDB uses it exclusively (everything bypasses page cache).

**Target phase.** v1.4 polish. Low integration risk; cheap win.

### 2.2 `posix_fadvise` — tell the kernel what we intend

**Syscall:** `posix_fadvise(fd, offset, len, advice)`.

Relevant `advice` values:

| Advice | Semantics | Tally use |
|---|---|---|
| `POSIX_FADV_DONTNEED` | Evict these pages from the cache now | After snapshot fsync — kick 100 MB of newly-written snapshot pages out of the cache |
| `POSIX_FADV_NOREUSE` | "I'm reading this once" | On old event log segments during compaction read |
| `POSIX_FADV_SEQUENTIAL` | "I'll read sequentially, prefetch aggressively" | Snapshot restore reads, event log replay |
| `POSIX_FADV_RANDOM` | "Don't prefetch" | N/A for Tally — we don't read random from disk |
| `POSIX_FADV_WILLNEED` | "Prefetch now, I'll read soon" | Could be used for predicted warm-tier hydration (§5) |

**The `POSIX_FADV_DONTNEED` after snapshot fsync trick** is well-known in the database world and used by PostgreSQL, MySQL, and pretty much anyone who writes large checkpoint files. **HIGH confidence**. Rust: `nix::fcntl::posix_fadvise` (nix crate is production-stable).

**Caveat:** `POSIX_FADV_DONTNEED` only evicts pages whose writeback has completed. If you call it immediately after `write()` without `fdatasync()`, nothing happens. Pattern: `write → fdatasync → fadvise(DONTNEED)`.

**Target phase.** v1.4 polish. Two-line win.

### 2.3 `madvise` — for anonymous / mmapped memory

**Syscall:** `madvise(addr, len, advice)`. Rust: `nix::sys::mman::madvise`, or `region` crate.

| Advice | Semantics | Tally use |
|---|---|---|
| `MADV_DONTNEED` | Free these pages, re-zero on next touch | TTL eviction path — cheap state purge without unmap |
| `MADV_FREE` (Linux 4.5+) | Pages can be reclaimed under pressure, but keep content if not reclaimed | Same, but "soft" — if memory pressure is low, eviction is free |
| `MADV_COLD` (Linux 5.4+) | "Deprioritize these pages for reclaim" | Warm-tier state that hasn't been hot for N minutes |
| `MADV_PAGEOUT` (Linux 5.4+) | Force immediate swap-out | Aggressive eviction without unmap |
| `MADV_HUGEPAGE` | Enable THP for this range | Per-shard entity map bulk allocation |
| `MADV_NOHUGEPAGE` | Disable THP | Small operator state regions to avoid the "huge page inflation" pitfall |
| `MADV_WILLNEED` | Prefetch | Predicted hot-up of cold entity state |
| `MADV_DONTDUMP` | Exclude from core dumps | Sensitive feature state |

**`MADV_COLD` / `MADV_PAGEOUT`** are the interesting new ones (Linux 5.4, 2019; widely available by 2022). Prior art: Android uses them aggressively for app backgrounding. Meta TMO (ATC 2022) uses them for tiered memory. **MED confidence** they help Tally specifically; would need benchmarking.

**Target phase.** v1.4 for `MADV_DONTNEED` on TTL eviction. v2 for `MADV_COLD` if warm-tier tiering lands (§C2 of combinations).

### 2.4 Kernel-side read-ahead tuning

`posix_fadvise(POSIX_FADV_SEQUENTIAL)` doubles kernel read-ahead window. On NVMe with sequential 4 KB reads, this bumps effective bandwidth by ~30 %. For snapshot restore specifically — the kernel already tries to prefetch, but SEQUENTIAL makes it more aggressive. **HIGH confidence**, standard trick.

Alternative: `blockdev --setra` at the device level (requires root, anti-philosophy; document as operator tunable).

---

## §3 Filesystem-level tricks

### 3.1 `fallocate` — preallocate, punch, collapse

**Syscall:** `fallocate(fd, mode, offset, len)`. Rust: `nix::fcntl::fallocate`.

#### 3.1.1 Preallocation (`mode = 0`)

Reserve blocks without writing them. Prevents filesystem fragmentation when the event log grows. **Standard trick**, used by Kafka, RocksDB WAL, PostgreSQL WAL. **HIGH confidence**, ~10–30 % write throughput win on fragmented ext4/XFS under sustained append workloads.

**Tally application.** On `register_stream`, preallocate the first 256 MB of the event log file. Grow in 256 MB chunks when filled. Eliminates the fragmentation Tally would accumulate at 1 M eps over weeks.

#### 3.1.2 `FALLOC_FL_PUNCH_HOLE`

Reclaims disk blocks in the middle of a file, turning them into a sparse hole. The file length is unchanged. **Exactly what Tally compaction wants**: today compaction rewrites the file from scratch, which means doubling disk space transiently and rewriting all non-expired entries. With punch-hole, you can just free the expired prefix of the event log.

**Caveat:** Doesn't shrink the file head — the file-offset-to-event mapping stays stable, which is actually good (replay offsets don't need remapping).

**Filesystem support:** ext4 (2012+), XFS (2013+), Btrfs (2014+), tmpfs (recent). **HIGH confidence.**

**Prior art.** Kafka uses `PUNCH_HOLE` for log segment cleaning (Kafka KIP-405 tiered storage adjacent). Ceph RADOS uses it.

#### 3.1.3 `FALLOC_FL_COLLAPSE_RANGE`

ext4 / XFS only. Removes a range from the start or middle of a file, shifting subsequent data down. **True log compaction — file actually shrinks.** Range must be block-aligned. File offsets of surviving data change.

Tally's current compaction rewrites the whole file; `COLLAPSE_RANGE` would let us drop the expired prefix in place. But it would invalidate any in-memory offset cursors, which matters for v1.1's offset-based replay. Net: **probably not worth it vs PUNCH_HOLE** for Tally.

#### 3.1.4 `FALLOC_FL_ZERO_RANGE`

Writes zeros without allocating filesystem metadata changes. Useful to wipe stale sections without a full `write(0)` dance. Narrow win for Tally.

### 3.2 `F_SET_FILE_RW_HINT` — NVMe data placement hints

**Syscall:** `fcntl(fd, F_SET_FILE_RW_HINT, &hint)`. Linux 4.13+.

Hint values (`enum rw_hint`):
- `RWH_WRITE_LIFE_NONE`
- `RWH_WRITE_LIFE_SHORT` — ephemeral, expected to be overwritten or deleted soon
- `RWH_WRITE_LIFE_MEDIUM`
- `RWH_WRITE_LIFE_LONG`
- `RWH_WRITE_LIFE_EXTREME` — essentially immortal

**Why it matters.** Modern NVMe SSDs with **multi-stream write** support (NVMe 1.3+ Directives) use these hints to place blocks in different erase groups, dramatically reducing write amplification and extending drive life. Samsung PM1725, Intel DC P4510+, most enterprise NVMe post-2019. **MED confidence** — the feature exists but actual support varies by drive firmware.

**Tally application.**
- Event log tail (actively written, will be compacted out): `RWH_WRITE_LIFE_SHORT`.
- Event log old segments (persisted until TTL compaction): `RWH_WRITE_LIFE_MEDIUM`.
- Snapshot base file: `RWH_WRITE_LIFE_LONG`.
- Snapshot delta files: `RWH_WRITE_LIFE_SHORT` (they get merged into base soon).

**Prior art.** RocksDB (since 2019), ceph-osd, Google's F2FS integration. **HIGH confidence** on usage pattern.

**Rust.** No dedicated crate. Raw `libc::fcntl` + `libc::F_SET_FILE_RW_HINT` via `nix`. Trivial.

**Risk.** If the drive doesn't support it, no-ops silently. No downside.

**Target phase.** v1.4 polish. 50-line change.

### 3.3 `O_TMPFILE` + `linkat` — race-free atomic rename

**Syscalls:** `open(dir, O_TMPFILE | O_WRONLY, 0600)` → `linkat(fd, "", dirfd, "final-name", AT_EMPTY_PATH)`.

**What it does.** Creates a file that doesn't yet have a name — it exists as an anonymous inode. Write to it, sync it, then atomically link it into the directory with its final name. No leftover `.tmp` files on crash. No rename-over-existing edge case.

**Comparison with current Tally pattern.** Today's snapshot code does `write to foo.snapshot.tmp → rename to foo.snapshot`. That's fine, but:
- Crash between write and rename leaves an orphan `.tmp` file that must be cleaned up on startup.
- Rename-over-existing is atomic on POSIX but some kernel + filesystem combos (notably XFS under specific loads) have quirks.

`O_TMPFILE` + `linkat` eliminates both. **HIGH confidence** — kernel docs, and it's how systemd-journald, git, etc. write their data files.

**Rust.** `nix::fcntl::openat` with `O_TMPFILE` flag (nix has the constants). No dedicated crate.

**Target phase.** v1.4 polish alongside §2.1 snapshot refactor.

### 3.4 `renameat2(RENAME_EXCHANGE)` — atomic swap

**Syscall:** `renameat2(olddirfd, oldpath, newdirfd, newpath, RENAME_EXCHANGE)`. Linux 3.15+.

Atomically swaps two files in one operation. Useful if Tally wants to maintain `current.snapshot` and `previous.snapshot` as symlinks and atomically promote a new snapshot to current while demoting current to previous. Cleaner than the manifest approach v7 uses.

**Compared to v7 manifest.** The manifest v7 design uses a JSON manifest file that lists which snapshot files are current. That's fine and more portable. `RENAME_EXCHANGE` is a simpler alternative for 2-slot rotation but the manifest approach scales to N slots (base + N deltas). **v7 manifest is the right call.** Note `RENAME_EXCHANGE` as a simplification we're explicitly not taking.

### 3.5 `copy_file_range` — zero-copy intra-kernel file duplication

**Syscall:** `copy_file_range(fd_in, off_in, fd_out, off_out, len, flags)`. Linux 4.5+.

Kernel-side copy without bouncing through userspace. On CoW filesystems (Btrfs, XFS reflinks), it uses reflink semantics and does no data copy at all — just shared extents. On ext4, it reads into kernel memory and writes out, but skips userspace.

**Tally application.** Snapshot branching for backup / S3 upload staging. Not hot-path.

**Rust.** `nix::fcntl::copy_file_range`. **HIGH** confidence.

**Target phase.** v2 if snapshot-to-S3 lands.

### 3.6 Filesystem choice: ext4 vs XFS vs ZFS vs Btrfs

| FS | Append-only workload | Small-file metadata | fsync latency | Preallocation | Notes |
|---|---|---|---|---|---|
| **ext4** | Good | Good (hashed dirs) | Good (~200 µs p99 on consumer NVMe, ~50 µs on enterprise) | `fallocate` efficient | Tally's baseline. Fine. |
| **XFS** | **Excellent** (was designed for this) | Excellent (B-tree dirs) | **Best** (parallel log) | Best | **Recommended for high-throughput Tally deployments.** |
| **Btrfs** | OK but COW amplifies small appends | Great | Poor (~1 ms p99 not uncommon) | `fallocate` supported but weird COW semantics | Avoid for event log. |
| **ZFS (OpenZFS)** | OK, ZIL helps writes | Great | Depends on ZIL config | N/A (COW) | Works but operator burden. |
| **tmpfs** | **Best** (RAM) | Best | Best (~5 µs) | Free | For snapshot staging — see §6.7 |

**Recommendation.** Tally's docs should say "XFS recommended, ext4 supported, avoid Btrfs/ZFS for the event log path." That's a documentation item, not a code change. Prior art: Scylla docs strongly recommend XFS and explicitly reject ext4 for their commit log (too high fsync tail). For Tally's scale ext4 is fine.

**Target phase.** Documentation item, v1.4.

---

## §4 Zero-copy data paths — `splice`, `sendfile`, `vmsplice`

### 4.1 `splice` and `sendfile`

**Syscalls:** `splice(fd_in, off_in, fd_out, off_out, len, flags)`, `sendfile(out_fd, in_fd, off, count)`.

`sendfile` is the simple case: file → socket, zero-copy via the kernel page cache. `splice` is the general case: any two file descriptors where at least one is a pipe, zero-copy.

**Where this matters for Tally.** Tally's PUSH / GET hot path is **small messages** (tens of bytes to a few KB). Zero-copy only helps for sends > ~8 KB. It's not a hot-path win.

**Where it DOES help:**
- **Snapshot download over HTTP mgmt API.** `GET /debug/snapshot/dump` would do `sendfile(client_socket, snapshot_fd, ...)` instead of reading the snapshot into userspace and writing it back. ~2× throughput on a 1 GB snapshot download, saves CPU.
- **Event log streaming replay.** If Tally ever exposes an "event log tail" streaming endpoint (for operational debugging, or for shipping to S3 backup), `sendfile` or `splice` zero-copies it.
- **Backfill to a secondary Tally** (future HA mode out of scope) would use splice for bulk transfer.

**Linux 5.19+ `IORING_OP_SPLICE`.** Same thing, inside io_uring. If Tally adopts io_uring (§1), splice becomes a natural fit.

**Rust.** `nix::fcntl::splice`, `nix::sys::sendfile::sendfile`. Standard.

**Prior art.** nginx `sendfile on`, Varnish, HAProxy `option splice-*`, Kafka `transferTo` (Java, same underlying syscall).

**Target phase.** v2 when/if HTTP snapshot download or backfill API lands.

### 4.2 `vmsplice` — userspace buffer → kernel pipe, zero-copy

`vmsplice(fd, iovec, count, flags)` maps userspace pages into a pipe. Combined with `splice` from pipe → socket, it gives "userspace → socket, zero-copy."

**Caveat:** User must not modify buffers while in flight. This matches well with an arena-allocated snapshot buffer that's immutable after the snapshot tick.

**Tally application.** Snapshot serialization into an arena → `vmsplice` into a pipe → `splice` to HTTP response socket. Skips the kernel copy of snapshot bytes. Useful for 100 MB+ snapshots shipped over the network. **Niche.**

**Target phase.** v2+ if snapshot-over-network is a feature.

---

## §5 Memory-mapped files — what works, what doesn't

### 5.1 The case against mmap for Tally's hot writes

**Required reading:** *Are You Sure You Want to Use MMAP in Your Database Management System?* — Crotty, Leis, Pavlo, **CIDR 2022** (https://db.cs.cmu.edu/mmap-cidr2022/, cmu.edu). Summarizes why mmap is a trap for databases:

1. **Transactional safety.** Dirty page writeback happens at kernel discretion, not yours. You cannot control fsync boundary.
2. **I/O stalls invisible to scheduler.** A page fault on a cold mmap'd page blocks the faulting thread in the kernel — your tokio/shard worker is gone for 50–100 µs of NVMe read, with no way to yield.
3. **Error handling.** Read errors on mmap'd files become SIGBUS. Good luck recovering in a shard worker.
4. **TLB pressure.** Large mmap regions cause TLB thrashing without huge pages.
5. **Eviction is the kernel's decision, not yours.** LRU inside the page cache is not your LRU.

**Verdict for Tally.** **Never mmap the event log write side. Never mmap the snapshot write side.** Both invariants are already held — keep them.

**Prior art** (all cited in Crotty et al.): LMDB is mmap-based but readonly-hot; SQLite has an optional mmap mode but it's a known footgun; MongoDB's MMAPv1 was deprecated for exactly this reason; InfluxDB moved off mmap in TSI 2018.

### 5.2 Where mmap DOES work for Tally

- **Snapshot restore read path.** Snapshot is immutable; mmap + `MAP_POPULATE` + parallel postcard decode works fine. You still pay page faults on first touch but can prefault with `MAP_POPULATE`.
- **Read-only event log scans during backfill.** Same story.

**Syscall + flags:**
```
mmap(addr, len, PROT_READ, MAP_PRIVATE | MAP_POPULATE, fd, offset)
```
`MAP_POPULATE` prefaults the pages on map. `MAP_HUGETLB` pairs with explicit huge pages. `madvise(MADV_SEQUENTIAL)` after map tells the kernel to prefetch aggressively.

**Rust.** `memmap2` crate (0.9+, maintained by RazrFalcon, production-stable). Alternative: raw `nix::sys::mman`.

**Expected win.** Snapshot restore on a 1 GB snapshot: stdlib `BufReader` is ~1.5 GB/s (limited by 8 KB buffer + memcpy); mmap + MAP_POPULATE is ~4–6 GB/s (limited by disk read, now parallel). **~3–4× faster recovery**, on top of io_uring's parallel reads (§1.3).

### 5.3 `MAP_HUGETLB` and `memfd_create(MFD_HUGETLB)`

**Syscalls:** `memfd_create(name, MFD_HUGETLB | MFD_HUGE_2MB)` then `mmap` on the resulting fd.

Creates an anonymous hugepage-backed memory region. Tally would use this for **per-shard bump allocators** that hold the entity HashMap's backing storage. 2 MB pages → 512× fewer TLB entries for the same RSS → **5–15 % hot-path latency reduction** on cache-heavy state traversal (MED confidence — measured in Scylla, DragonflyDB).

**Prerequisite.** `vm.nr_hugepages` or `hugetlb_cgroup` must be configured. Operator burden. Document as best practice; do not require.

**Rust.** `memfd` crate, or raw `nix::sys::memfd::memfd_create` + `nix::sys::mman::mmap`.

**Target phase.** v2 — bundle with an allocator refactor.

### 5.4 `mremap` for shard resizing

`mremap(old_addr, old_size, new_size, MREMAP_MAYMOVE)` — resize a mapping without reallocating + copying. Useful if a shard's entity map needs to grow. But since Tally uses `hashbrown`, which uses `std::alloc` under the hood, this isn't directly exposed. Skip unless doing a custom arena allocator.

---

## §6 Snapshot write path — concrete hacks

This section is where several of the above primitives compose into a **"best-case snapshot write path"** for v2.

### 6.1 Today (v1.3 Phase 15 plan)

```
1. main thread: grab read-snapshot of state (Arc::clone or per-shard copy_on_write)
2. spawn_blocking worker:
3.   File::create("snap.tmp")
4.   postcard::to_io_writer(file, &state)   // stdlib buffered writes, page cache
5.   file.sync_all()                         // fsync via BLOCKING syscall
6.   fs::rename("snap.tmp", "snap")          // atomic name swap
```

Hot-path cost during snapshot: ~0 (step 1 is copy-on-write or already per-shard). Durability-to-disk cost: ~1–3 s for 1 GB snapshot, fsync stalls the blocking pool thread.

### 6.2 "Best-case v2" snapshot write path

```
1. shard worker: mark dirty range for this cycle, hand to I/O thread
2. I/O thread (owns per-shard io_uring):
3.   open with O_DIRECT | O_TMPFILE
4.   fallocate preallocate expected size
5.   fcntl F_SET_FILE_RW_HINT = RWH_WRITE_LIFE_LONG
6.   postcard-stream into pre-registered aligned 1 MiB buffers in the ring
7.   submit IORING_OP_WRITE_FIXED for each chunk, linked with IOSQE_IO_LINK
8.   final SQE is IORING_OP_FDATASYNC, linked
9.   on completion: linkat the O_TMPFILE fd into final location (atomic)
10.  posix_fadvise(POSIX_FADV_DONTNEED, ...)   // evict from page cache
11.  notify shard worker of completion via completion queue
```

**Wins over current path:**
- No `spawn_blocking` thread cost (currently 1 per write).
- No page-cache pollution (`O_DIRECT` + `POSIX_FADV_DONTNEED` as belt-and-suspenders).
- Postcard buffer copy eliminated (`IORING_REGISTER_BUFFERS`).
- fsync amortized via linked SQE instead of blocking a thread.
- NVMe place-on-long-lived-zone hint improves drive life.
- Atomic rename race-free via `O_TMPFILE` + `linkat`.

**Estimated savings.** Current snapshot write: ~1–3 s wall time for 1 GB (measured elsewhere in Tally perf docs, extrapolated). v2 path: ~400–800 ms wall time, **and** no page-cache impact on neighbors, **and** no blocking-pool cost.

**Maturity.** All individual primitives are HIGH confidence. Integration is MED — debugging io_uring submission failures is unpleasant.

### 6.3 Atomic write primitives — NVMe NAWUN

**NVMe NAWUN (Namespace Atomic Write Unit, Normal).** The SSD guarantees that a write of up to `NAWUN` sectors is atomic across power loss — you either see all or none. Typical values: **16 KB (32 sectors) on enterprise NVMe**. Query via `nvme id-ns` (nvme-cli).

**Implication.** For **writes smaller than NAWUN**, `tmp → rename` is unnecessary. You can write in place and trust the drive's atomicity. For Tally's manifest file (~1 KB), and delta headers, this applies.

**Rust.** No dedicated crate. `nvme-cli` check at startup, store the value, branch on write size.

**Risk.** NAWUN depends on the drive. If Tally runs on a cheap consumer NVMe or a networked block device, NAWUN might be 512 bytes. Fall back to `tmp → rename`. **Advisory win, not a replacement.**

**Target phase.** v2 if manifest becomes a hot rewrite path.

### 6.4 `sync_file_range(SYNC_FILE_RANGE_WRITE)` — fire-and-forget page flush

**Syscall:** `sync_file_range(fd, offset, nbytes, flags)`.

Flags:
- `SYNC_FILE_RANGE_WAIT_BEFORE` — wait for in-flight writeback on this range to finish before starting new writeback.
- `SYNC_FILE_RANGE_WRITE` — initiate writeback on dirty pages in this range, but don't wait.
- `SYNC_FILE_RANGE_WAIT_AFTER` — wait for writeback to complete.

The **"WRITE only"** call starts writeback without blocking. Combined with a later `fdatasync` for true durability, it gives a **two-stage flush**: kick off IO early, sync later. Reduces peak IO bursts.

**Tally application.** Event log group-commit: after each batch of N events (but before the next group commit boundary), `sync_file_range(SYNC_FILE_RANGE_WRITE)` to start writeback. On the actual group commit tick, `fdatasync` to finalize. Smooths IO load and reduces fsync latency.

**Caveat.** `sync_file_range` is **not a replacement for fsync** — it does not flush filesystem metadata. **HIGH confidence**, documented in `man 2 sync_file_range` with a prominent warning.

**Prior art.** Kafka uses this for broker log segments (`log.flush.interval.messages` triggers sync_file_range under the hood in some tunings). PostgreSQL has `sync_file_range` in its backend sync code.

**Rust.** `nix::fcntl::sync_file_range`. Standard.

**Target phase.** v1.4 if event log fsync tail is a problem; otherwise v2.

### 6.5 Redis BGSAVE-via-fork — can Tally fork?

Redis's `BGSAVE` does `fork()` and writes the snapshot from the child, which inherits a copy-on-write view of the data. The parent keeps serving; the child snapshots.

**Can Tally do this?** Post-v1.3, Tally is thread-per-shard with `parking_lot::Mutex<ShardStore>`. `fork()` in a multi-threaded process is **undefined behavior for any non-async-signal-safe code path**, and `parking_lot` is absolutely not fork-safe. The only safe pattern in a multi-threaded process is `fork+exec`, which defeats the purpose.

**Glommio's model might help.** Glommio is thread-per-core *processes*, not threads — each core is its own OS thread within one process, but the runtime is single-threaded per core. Inside a single shard, `fork()` is theoretically safe IF that shard doesn't share mutable state with other shards. Which is exactly v1.3's design! But: `fork()` still has to snapshot the entire virtual memory of the process, including all other shards' state. Unless Tally uses `MAP_PRIVATE` + a per-shard address space (basically a separate process per shard), fork is not a win.

**Verdict.** **Don't fork.** Tally's "off-thread snapshot" (Phase 15 + §6.2) is a better answer. Fork-based snapshot is a Redis idiom that doesn't survive multi-threading.

### 6.6 Snapshot-over-tmpfs + periodic drain

**Idea.** Mount a tmpfs at `/var/lib/tally/snapshot-staging`. Write snapshots there (RAM-speed). Use a background thread to copy completed snapshots to the persistent path.

**Tradeoffs.**
- **Pro:** Snapshot write = RAM write, ~5 GB/s, zero disk stall, no fsync cost.
- **Pro:** Persistent copy can be throttled, happens off hot path entirely.
- **Con:** On crash between tmpfs write and persistent copy, that snapshot is lost. Durability window widens from "30 s (snapshot cadence)" to "snapshot cadence + drain latency".
- **Con:** RAM pressure — a 1 GB snapshot doubles Tally's RSS during the write.
- **Con:** Operator burden — requires a tmpfs mount, which means systemd config or Docker volume. Anti-philosophy.

**Verdict.** **Useful as an advanced tuning, not a default.** Document as "if your fsync latency is a problem, mount /var/lib/tally/snapshot-staging on tmpfs with size=2×snapshot_size." Target phase: documentation, v2.

### 6.7 Streaming zstd into an io_uring write ring

Combined with §2.1 zstd dictionary snapshots (HORIZON-SURVEY.md §4.4):

```
postcard encoder → zstd::stream::Encoder → io_uring write ring
```

The `zstd` Rust crate (0.13+, maintained) supports streaming encode with `Encoder::with_dictionary`. Wire the output into an aligned 1 MiB buffer, submit to io_uring when full.

**Win.** Snapshot writes become **CPU-bound on zstd** (~500 MB/s single-threaded) instead of disk-bound. For 1 GB postcard → ~100 MB compressed → **400 MB of disk IO saved per snapshot cycle.** At 30 s snapshot cadence, that's 13 MB/s of sustained IO avoided. Modest but composes with everything else.

**Target phase.** v1.4 alongside the snapshot-compression piece in NEXT-STEPS Tier 1 item 4.

---

## §7 Event log specifics

### 7.1 Preallocate or grow?

**Current:** event log file grows organically via O_APPEND writes. The filesystem allocates blocks on demand, leading to fragmentation over weeks of sustained 100K+ eps writes.

**Recommendation:** `fallocate` 256 MB chunks when the log crosses 80 % of its current size. Standard Kafka/RocksDB pattern. **HIGH** confidence win. ~5–15 % reduction in write-side fsync tail on ext4/XFS.

### 7.2 Buffered vs `O_DIRECT` event log

**Current:** `BufWriter<File>` with `O_APPEND`. Kernel-side page cache buffers writes, periodic fsync.

**O_DIRECT alternative:** userspace buffer (aligned), direct write per group commit, explicit fdatasync. Skips the page cache.

**Tradeoff.** O_DIRECT saves the page-cache double-buffering cost but requires careful alignment and risks a per-write latency spike if the userspace buffer isn't full when the commit fires. **For small frequent writes (100K+ eps), buffered is generally better**; for large bursty writes, O_DIRECT wins. Tally is squarely in the "small frequent" regime → **keep BufWriter**, add io_uring for the fsync dispatch path.

**Verdict.** **Don't O_DIRECT the event log.** Do O_DIRECT the snapshot.

### 7.3 Group commit cadence

Current: periodic timer fsync. Phase 11 uses ~100 ms cadence (check elsewhere in plan).

**State of the art:** **Adaptive group commit** — fsync when either (a) N events have been written, or (b) T ms have elapsed, or (c) the oldest unfsynced event is older than D ms. Kafka, PostgreSQL, RocksDB all do variations.

**For Tally:** N=1024, T=50 ms, D=10 ms is a reasonable starting point. With io_uring + linked SQE fsync, the dispatch cost of an extra fsync is near-zero, so bias toward frequent small fsyncs. **MED confidence** on specific parameter values — would need benchmarking.

**Target phase.** v2 adaptive commit, after v1.3 measures what real cadence looks like.

### 7.4 LZ4 event log compression

**Idea.** Compress event log entries on the write side. LZ4 is ~2 GB/s single-threaded, fits in the hot-path budget.

**Rust crate.** `lz4_flex` (pure-Rust, production, HIGH confidence, maintained by PSeitz, benchmark-lead-on-crates.io). Streaming API via `lz4_flex::frame::FrameEncoder<W: Write>`.

**Win.** Event log on disk ~2–3× smaller. Compaction costs scale with disk size, so 2–3× less compaction work. Replay bandwidth doubles.

**Cost.** ~200 ns per event on the write side (LZ4 encode). Tally's current per-event cost is ~7 µs → **+3 % budget**, acceptable.

**Risk.** Frame boundaries must align with event boundaries for independent decoding — otherwise replay has to decode everything from the start. Standard pattern: one LZ4 frame per event, with frame headers. Slightly worse compression (frame overhead), but independent decode. Alternative: group N events per frame, scan frame-aligned.

**Target phase.** v1.4 polish or v2.

---

## §8 Kernel bypass for storage — SPDK and friends

### 8.1 SPDK (Storage Performance Development Kit)

**What it is.** Intel-led userspace NVMe driver. Bypasses the kernel block layer entirely. Uses UIO/VFIO to map NVMe registers into userspace. I/O latency drops from ~10 µs (kernel path) to **~5 µs (userspace path)**.

**Prior art.** ScyllaDB (via Seastar, opt-in), Ceph BlueStore (experimental), Aerospike Hybrid Memory (optional), Alibaba PolarDB.

**Rust bindings.** `spdk-sys` exists but is experimental/low-level. No high-level wrapper. `rust-spdk` in WIP state. **LOW confidence** on production readiness.

**For Tally.** Breaks "single binary, runs anywhere" in multiple ways:
- Requires VFIO / UIO kernel modules loaded with specific permissions.
- Requires the NVMe device to be **unbound from the kernel driver** — the OS cannot see it as a block device anymore. No filesystem. No other process sharing the drive. No backup tools. No `ls`.
- Requires hugepage allocation at startup.
- Requires operator to run Tally with CAP_SYS_ADMIN or equivalent.

**Verdict.** **Anti-philosophy. Scope and deprioritize.** Document as "the ceiling, never crossed." Worth revisiting in v3+ if a customer hits the "kernel I/O path is literally the bottleneck" wall. Until then, io_uring gets Tally 80 % of the way there at 5 % of the operator cost.

### 8.2 Aerospike raw device mode

Aerospike's "raw device" mode uses `O_DIRECT` on a whole-disk device (`/dev/nvme0n1`) with no filesystem. Aerospike manages its own on-disk layout.

**Advantages:** No filesystem overhead, no journal, deterministic latency, zero fragmentation.
**Disadvantages:** Same list as SPDK — dedicated disk, operator burden, no cohabitation with other services.

**For Tally.** Same verdict as SPDK: **too hostile to the zero-ops promise**. Keep the filesystem path; use `O_DIRECT` + `fallocate` preallocation to approximate the benefit.

### 8.3 Userspace LSM evaluation (RocksDB as "warm tier on disk")

If v2 lands partial-state tiering (HORIZON-COMBINATIONS C2), the question is: build Tally's own on-disk warm tier, or use RocksDB as the storage engine for cold/warm entities?

**RocksDB (via `rocksdb` Rust crate, 0.22+):**

| Aspect | RocksDB gives us | Cost |
|---|---|---|
| On-disk format | LSM with SST compaction, block cache, bloom filters | Well-tested, Meta-scale |
| Point lookups | ~10–50 µs p99 NVMe, block-cache sensitive | Adds 10–50 µs to cold reads |
| Range scans | Efficient | Not Tally's query shape |
| Configuration | Vast tuning surface | Operator burden |
| Binary size | +20 MB to Tally | Matters for "zero-ops single binary" |
| Compaction background work | Managed by RocksDB | Unpredictable CPU spikes |

**Verdict.** **Tempting but wrong for Tally.** RocksDB is a general-purpose KV and Tally has very specific query patterns (point lookups on dense keys, bulk scan during replay, no ranges, small values). A custom on-disk format aligned with Tally's event log replay path is likely simpler and faster. Prior art: DragonflyDB deliberately did *not* use RocksDB for its cold tier for similar reasons.

**However.** Prototyping the warm tier with RocksDB **as the first iteration** is a reasonable de-risking move. Ship v2 with a RocksDB-backed warm tier behind a feature flag, measure, then decide whether to write a custom replacement.

**Target phase.** v2 warm tier — evaluation phase.

### 8.4 ScyllaDB / Seastar lessons

Seastar (the C++ runtime ScyllaDB is built on) gives these lessons:
1. **Shared-nothing thread-per-core is the biggest win.** Tally v1.3 already has this.
2. **All I/O through io_uring is the second win.** This document's §1–6 agree.
3. **Userspace NVMe via SPDK is the third, smaller win.** Not worth the operator cost for most workloads.
4. **Co-locating compute with I/O on the same reactor is the fourth.** Tally's v1.3 thread-per-shard model already does this.

**Net:** Tally is 1 and 4 already. 2 is the io_uring work in §1. 3 is rejected. **Tally is on the right architectural path without importing any Seastar-specific machinery.**

### 8.5 NVMe-oF (scope only)

NVMe-over-Fabrics is **cluster-mode storage**, which Tally's PROJECT.md says is out of scope. Skip.

---

## §9 Persistent memory legacy

**Context.** Intel Optane DC PMEM is discontinued (mid-2022). Micron exited 3D XPoint. The persistent memory story that VLDB was writing papers about 2017–2021 is dead on the specific hardware.

**What survived.**

### 9.1 CXL 2.0+ memory-semantic devices

CXL.mem devices arriving 2025–2027 have persistence variants (CXL 3.0 "Global Fabric Attached Memory"). The programming model is similar to PMEM: `clwb` + `sfence` for durability, byte-addressable, ~200–400 ns access latency, persistence on power loss.

**`clwb` / `clflushopt` / `sfence` x86 instructions.** Cache line writeback without invalidation. Useful for small durable writes: update an 8-byte word, `clwb`, `sfence`, done. ~50 ns instead of a full fsync.

**`libpmem` / `libpmemobj`** — still maintained (PMDK on GitHub, Intel continues it). Rust: `pmemobj-rs` exists but **unmaintained since 2022** (LOW confidence production-readiness).

**For Tally.** If CXL PMEM lands in 2027+ in servers Tally runs on, **this is the natural persistent-state-without-WAL primitive.** Store operator state in CXL PMEM, `clwb` + `sfence` after each update, no snapshot needed — state is always durable. Latency budget: +50 ns per update, ~0.5 % of the per-event budget.

**Risk.** Hardware dependency. Rust ecosystem not there. **v3 speculation.**

### 9.2 Redis on Optane lessons

Even though the hardware is dead, the Redis-on-Optane research (VLDB 2020, FAST 2021) taught a lasting lesson: **persistent memory durability is cheapest when you design for byte-granular durability from day one, not as a bolt-on to a page-based design.** Tally's per-operator state is already structured this way — the v1.3 shard store is a natural fit for a future PMEM backend.

**Keep this in the design book, don't build for it.** Target phase: v3+.

---

## §10 Per-hack decision matrix

Legend:
- **Lat win** — effect on PUSH/GET p99 latency. Positive = faster. "0" = no hot-path change, may affect snapshot wall time.
- **Mem win** — RSS / cache footprint effect.
- **Phase** — earliest Tally phase this should land.
- **Maturity** — H/M/L for HIGH / MEDIUM / LOW.

| # | Hack | Syscall / primitive | Rust crate | Prior art | Lat win | Mem win | Complexity | Maturity | Phase |
|---|---|---|---|---|---|---|---|---|---|
| 1 | io_uring event log append + linked fsync | `io_uring_*`, `IORING_OP_WRITE_FIXED`, `IOSQE_IO_LINK`, `IORING_OP_FDATASYNC` | `io-uring` | DragonflyDB, ScyllaDB, RocksDB (since 8.x) | **−2 to −5 µs per fsync amortized** | 0 | High | H | v1.4 stretch / v2 |
| 2 | io_uring snapshot write (O_DIRECT + fixed buffers) | same + `O_DIRECT` | `io-uring` | PostgreSQL O_DIRECT WAL | Snapshot wall ~2–3× faster; 0 page-cache pollution | Eliminates snapshot-driven cache evictions | High | H | v2 |
| 3 | `O_DIRECT` snapshot write (without io_uring) | `O_DIRECT` flag | `std`, `nix` | InnoDB, ClickHouse | 0 hot; snapshot ~1.5× | Large (no page-cache pollution) | Low | H | **v1.4 polish** |
| 4 | `POSIX_FADV_DONTNEED` post-snapshot | `posix_fadvise` | `nix` | PostgreSQL checkpoint | 0 | Large (cache hygiene) | Trivial | H | **v1.4 polish** |
| 5 | `POSIX_FADV_SEQUENTIAL` on restore reads | `posix_fadvise` | `nix` | Standard | Restore wall ~1.3× | 0 | Trivial | H | **v1.4 polish** |
| 6 | `madvise(MADV_DONTNEED)` on TTL eviction | `madvise` | `nix`, `memmap2` | Android, TMO | 0 | Real (immediate RSS drop) | Low | H | **v1.4** |
| 7 | `madvise(MADV_COLD)` on warm-tier pages | `madvise` | `nix` | Meta TMO | 0 hot; +~1 µs cold | Real | Med | M | v2 |
| 8 | `fallocate` preallocate event log | `fallocate` | `nix` | Kafka, RocksDB WAL | 5–15 % fewer fsync stalls | 0 | Trivial | H | **v1.4** |
| 9 | `FALLOC_FL_PUNCH_HOLE` for compaction | `fallocate` | `nix` | Kafka log cleaning | Faster compaction, less disk churn | Disk only | Low | H | **v1.4** |
| 10 | `F_SET_FILE_RW_HINT` NVMe placement | `fcntl` | `nix` | RocksDB, F2FS | Slight fsync tail improvement | Drive life (flash wear) | Trivial | M | **v1.4 polish** |
| 11 | `O_TMPFILE` + `linkat` race-free snapshot rename | `openat`, `linkat` | `nix` | systemd-journald, git | 0 | 0 | Low | H | v1.4 polish |
| 12 | `copy_file_range` for snapshot branching | `copy_file_range` | `nix` | cp, Btrfs reflinks | Snapshot-to-S3 prep faster | 0 | Low | H | v2 |
| 13 | `sendfile`/`splice` for HTTP snapshot download | `sendfile`/`splice` | `nix` | nginx, Varnish | Snapshot download 2× | 0 | Low | H | v2 |
| 14 | mmap + `MAP_POPULATE` snapshot restore | `mmap` | `memmap2` | Standard DB restore | Restore wall **3–4×** | 0 | Med | H | v1.4 stretch |
| 15 | Huge-page-backed shard storage (`memfd_create(MFD_HUGETLB)`) | `memfd_create`, `mmap` | `memfd`, `nix` | Scylla, DragonflyDB | 5–15 % random-access speedup | TLB pressure drop | Med | M | v2 |
| 16 | `sync_file_range(WRITE)` early writeback | `sync_file_range` | `nix` | Kafka, PostgreSQL | Smoother fsync tail | 0 | Low | H | v1.4/v2 |
| 17 | NVMe NAWUN atomic write (skip tmp+rename for small writes) | `nvme id-ns`, in-place write | raw | NoSQL stores | 0 hot; simpler manifest path | 0 | Low (+drive detect) | M | v2 |
| 18 | Snapshot staging on tmpfs | mount | N/A (docs) | Redis recipe | Snapshot wall much faster | +RSS during write | Trivial code, operator burden | H | **docs v1.4** |
| 19 | zstd-dict streaming snapshot compression | zstd lib | `zstd` 0.13+ | See HORIZON-SURVEY §4.4 | Snapshot IO ~10× less | Disk only (10× smaller) | Med | H | **v1.4** (NEXT-STEPS item 4) |
| 20 | `lz4_flex` event log compression | — | `lz4_flex` | Kafka | 0 hot; +200 ns | 2–3× disk shrink | Med | H | v1.4/v2 |
| 21 | BGSAVE-via-fork | `fork` | N/A | Redis | — | — | Incompatible with thread-per-shard | — | **skip** |
| 22 | SPDK userspace NVMe | VFIO, libspdk | `spdk-sys` LOW | Scylla opt-in | ~5 µs/op instead of ~10 µs | — | Breaks filesystem + single-binary | L | **skip (v3?)** |
| 23 | RocksDB warm tier | — | `rocksdb` | DragonflyDB evaluated + rejected | +10–50 µs cold | Disk tier | Large (binary, background compaction) | H | v2 prototype gate |
| 24 | PMEM `clwb`/`sfence` byte-durable state | `clwb`, `sfence` instructions; `libpmem` | `pmemobj-rs` unmaintained | Intel PMDK | +50 ns per update | No snapshot needed | — | L | **v3 if CXL PMEM arrives** |

### Tier recommendations

**v1.4 cheap wins (days-scale):** 3, 4, 5, 6, 8, 9, 10, 11, 19 (the 9 pre-commit + zstd snapshot work).
**v1.4 stretch (weeks-scale):** 14, 20.
**v2 architectural:** 1, 2, 7, 12, 15, 16, 17, 18 (docs), 23 (evaluation gate).
**v3 speculative, track only:** 22, 24.
**Skip:** 21 (fork).

### Sources & citations

- Linux kernel `io_uring` docs & Jens Axboe LWN articles — HIGH.
- `io-uring`, `tokio-uring`, `glommio`, `compio`, `monoio`, `rio` — crates.io listings (verify versions before adoption).
- Crotty, Leis, Pavlo. *Are You Sure You Want to Use MMAP in Your Database Management System?* CIDR 2022.
- Gjengset et al. *Noria.* OSDI 2018.
- Meta TPP (OSDI 2024 — Zhong et al.), TMO (ATC 2022 — Weiner et al.) — CXL / memory tiering.
- PostgreSQL `wal_sync_method` docs; MySQL `innodb_flush_method` docs.
- ScyllaDB / Seastar documentation — shared-nothing + io_uring + per-shard reactors.
- RocksDB 8.x io_uring notes (Meta blog 2023).
- Kafka KIP-405 (tiered storage), Kafka log compaction internals.
- DragonflyDB architecture blog posts (2022–2024) — Helio, why-not-RocksDB.
- Aerospike HMA whitepaper.
- NVMe spec 1.4 / 2.0 — NAWUN, Directives (multi-stream write), write hints.
- PMDK (pmem.io) and `libpmem` documentation — still maintained by Intel as of 2024 despite Optane EOL.

---

*Written 2026-04-11. Sibling: `HORIZON-OS-KERNEL.md` for the non-storage OS/kernel/hardware hacks.*
