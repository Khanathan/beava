# S3 Files Feature as Tally State Backend — Horizon Research

**Date:** 2026-04-11
**Status:** Exploratory (no roadmap commitment)
**Author:** Horizon research agent

---

## What "S3 Files" Is

**Amazon S3 Files**, announced **April 7, 2026** and GA in 34 regions. It exposes any general-purpose S3 bucket as an **NFSv4.1+ file system** mountable on EC2, ECS, EKS, and Lambda. Built on top of Amazon EFS as the front-end cache, with S3 as the system of record.

Primary sources:

- AWS What's New: <https://aws.amazon.com/about-aws/whats-new/2026/04/amazon-s3-files/>
- AWS News Blog (launch post): <https://aws.amazon.com/blogs/aws/launching-s3-files-making-s3-buckets-accessible-as-file-systems/>
- Werner Vogels, *S3 Files and the changing face of S3*: <https://www.allthingsdistributed.com/2026/04/s3-files-and-the-changing-face-of-s3.html>
- Corey Quinn commentary: <https://www.lastweekinaws.com/blog/s3-is-not-a-filesystem-but-now-theres-one-in-front-of-it/>
- Hacker News discussion: <https://news.ycombinator.com/item?id=47680404>

### Key mechanism — "stage and commit"

- Writes land on an **EFS-backed filesystem cache**. Every **~60 seconds** dirty files are pushed to S3 as **whole-object PUTs** (one PUT per changed file).
- **Lazy hydration:** files <128 KB pulled immediately on mount discovery; larger files have metadata only until read.
- **Read bypass:** sequential reads of 1 MiB+ are rerouted directly to S3 via parallel GETs — **~3 GB/s per client**, scales to terabits in aggregate.
- **Consistency:** NFS close-to-open on the file side; S3 atomic-PUT on the object side. Concurrent modifications → S3 wins, filesystem copy moves to `lost+found` + CloudWatch event.
- **Eviction:** files untouched for 30 days drop out of the cache view (still in S3).
- **Known sharp edges:** directory renames cost O(objects-in-prefix) (each rename = copy + delete in S3). Warning at mounts >50M objects. POSIX-invalid keys don't appear.

### Pricing (reported, verify before committing)

- **~$0.30/GB-month** on the *cached working set* (not total bucket — this is the big cost win vs EFS Standard).
- **$0.03/GB reads, $0.06/GB writes** through the file interface.
- Large sequential reads that take the read-bypass path stream from S3 at standard S3 GET rates with **no S3 Files surcharge**.
- Underlying S3 storage billed normally ($0.023/GB-month S3 Standard).

Source: <https://computingforgeeks.com/amazon-s3-files-pricing-cost-calculator/>, <https://lushbinary.com/blog/amazon-s3-files-guide-pricing-use-cases-efs-fsx-comparison/>

---

## Feature Characteristics That Matter for Tally

| Dimension | S3 Files | Implication for Tally |
|---|---|---|
| Write-visibility latency to S3 | ~60 s batched | Any role that needs real-time S3 durability is disqualified |
| Local write latency (EFS cache) | ~1-3 ms (NFS over EFS) | **Blocks the <100 µs PUSH hot path entirely.** Never on hot path. |
| Sequential read throughput | 3 GB/s / client (read bypass) | Excellent for snapshot restore and backfill replay |
| Random small-file read | NFS RTT, ~500 µs–1 ms warm cache | Fine for snapshot discovery, not for per-event lookups |
| API | POSIX file ops (open/read/write/fsync/rename) | **No SDK integration needed** — stdlib `File` + existing `snapshot.rs` code "just works" |
| Consistency on conflict | S3 wins, filesystem version → lost+found | Multi-writer on same file = **silent data loss** unless Tally coordinates |
| Cost on working set only | $0.30/GB-mo cached | Big win if most keys are cold; loses to EFS if everything stays hot |
| Directory rename | O(objects in prefix) | Tally uses monotonic filenames + manifest rotation — no directory renames. **Compatible.** |
| Mount size warning | 50M objects | Tally writes 1 manifest + N base + M delta per cycle. At 30 s cadence × 30 days × 13 shards = ~1.1M files/year. **Fine.** |

---

## The Six Roles S3 Files Could Play in Tally

### Verdict Table

| # | Role | Verdict | Effort | Why |
|---|---|---|---|---|
| 1 | Snapshot persistence (base + delta) | **VIABLE** | S | POSIX drop-in; existing `spawn_blocking` write path works unchanged |
| 2 | Event log segments (rotated to cold) | **CONDITIONAL** | M | 60 s batch window = 60 s durability loss on segments in flight; only viable for *sealed* rotated segments |
| 3 | Cold tier for TTL-evicted state | **BLOCKED** | — | NFS round-trip on cache-miss revive = 500 µs–several ms; breaks <100 µs PUSH |
| 4 | Cross-instance state handoff | **VIABLE** | M | Read-bypass makes 1 GB base snapshot restore feasible in seconds; new instance mounts same bucket |
| 5 | Serverless deployment (Fargate/Fly/Lambda) | **VIABLE** | M | Solves "no local disk" for scale-to-zero; Tally boots, hydrates from mount, runs |
| 6 | Backfill source for Phase 8 | **VIABLE** | S | Read-bypass 3 GB/s sequential matches exactly the backfill replay access pattern |

### Per-role detail

**Role 1 — Snapshot persistence. VIABLE, ship this first.**
Tally's v7 manifest layout (`tally.snapshot.manifest.NNN`, `.base.NNN.shard-XX`, `.delta.NNN.shard-XX`) is monotonic, never renames directories, never overwrites files in-place. Writes are whole-file tempfile → atomic rename within the same directory (file rename is cheap in S3 Files — just the target object). The 60 s stage-and-commit window widens the crash durability gap from "30 s (snapshot cadence)" to "up to 90 s" (snapshot cycle + worst-case commit delay). For a feature server that already tolerates 30 s loss, this is marginal. The `posix_fadvise(DONTNEED)` and `O_DIRECT` hacks in HORIZON-STORAGE-IO.md §2 become **no-ops** on NFS — those optimizations only apply to a local-disk variant, document as such.

**Role 2 — Event log rotation. CONDITIONAL.**
Active event log must stay on local NVMe (Tally group-commits fsyncs every few ms; NFS fsync is ~1 ms minimum and 60 s to actual S3 durability is unacceptable for the active tail). But *sealed* segments can be copied to the S3 Files mount after rotation, freeing local disk. This turns "event log on SSD" into "event log on SSD + archive on S3 Files." Condition: Tally must keep enough local segments to cover the 60 s commit window.

**Role 3 — Cold tier for evicted TTL state. BLOCKED.**
The naive idea: on TTL eviction, serialize entity state to a file on the S3 Files mount; on next PUSH for that key, stat + read + deserialize and revive. Math: NFS stat + open + read + close for a small file = 2–4 RTTs @ 0.5 ms = **2–4 ms**, ~20-40× the <100 µs hot-path budget. Even a warm EFS cache hit is ~300-500 µs — still **3-5× over budget**. Additionally, file-per-key at 1M keys × 30-day retention ≈ hitting the 50M object warning. **Do not build this.** If cold-tier is wanted, do it async on a background thread via an explicit "hydrate" call that the client opts into.

**Role 4 — Cross-instance state handoff. VIABLE.**
Two Tally instances can mount the same bucket. New instance starts, reads the latest manifest, loads base + deltas from S3 Files (read-bypass path, 3 GB/s). On a 1 GB shard state, that's sub-second restore — **faster than current local-disk BufReader restore** (HORIZON-STORAGE-IO.md §5.2 benchmarks BufReader at ~1.5 GB/s). Blue/green and region failover become: mount → read manifest → restore → accept traffic. **Caveat:** only one instance may write the manifest at a time. Tally needs a simple lease file (separate S3 atomic-PUT with If-None-Match) to enforce single-writer. This is the **killer feature** — the one that actually extends Tally's reach, not just relocates its files.

**Role 5 — Serverless deployment. VIABLE.**
Fargate / Fly Machines / Lambda containers have no persistent local disk. Today this blocks Tally entirely (snapshots die with the container). Mount an S3 Files bucket at `/var/lib/tally` at container start: snapshots persist across restarts, scale-to-zero works. Read-bypass keeps boot-time state restore fast. This composes with Role 4 for rolling deploys. **Effort driver:** not the mount integration (it's POSIX), but the lease/single-writer coordination to prevent two instances from corrupting each other's manifests during a restart race.

**Role 6 — Backfill source. VIABLE.**
Phase 8 (historical backfill) replays sealed event log segments. If segments live on the S3 Files mount (Role 2), backfill is `sendfile`/`splice` over read-bypass at 3 GB/s — already faster than the event log can be replayed CPU-side. Zero extra effort beyond Role 2. Note the read-bypass trigger requires sequential reads ≥1 MiB, which matches segment file sizes.

### Zero-ops compatibility

New failure modes S3 Files introduces vs local disk:

- **60 s write-commit window** — widens crash durability gap to ~90 s worst case. Document; don't hide.
- **`lost+found` on concurrent modification** — if two Tally instances write the same manifest, one's changes silently move to lost+found with only a CloudWatch event to notify. **Tally must emit a startup warning if lost+found is non-empty** and must enforce single-writer via a lease file.
- **Mount disappearance on network partition** — NFS hard-mount hangs forever, soft-mount returns `EIO`. Tally's snapshot writer must handle `EIO` as "skip this cycle, log, retry next tick" — not crash.
- **Region tied to the bucket** — cross-region failover needs bucket-level replication, which adds its own lag.

None of these are dealbreakers for the "zero-ops" thesis *as long as Tally is explicit about them in docs* and provides a local-disk fallback. The promise is "you don't have to tune anything," not "nothing can go wrong."

---

## Rust Crate Recommendation

**Recommendation: use nothing. Keep stdlib `std::fs`.**

The whole point of S3 Files is that it's POSIX. Tally's existing `src/state/snapshot.rs` (1,284 lines of `File::create`, `BufWriter`, `rename`, `fsync`) works unchanged against a mounted S3 Files path. The only config change is `snapshot_dir = "/mnt/s3files/tally"` instead of `/var/lib/tally`.

Alternatives considered and rejected:

- **`aws-sdk-s3`** — would bypass S3 Files and talk native S3 object API. ~5 MB added binary size, adds tokio dependencies Tally already has, but reintroduces SDK-shaped error paths and async-only I/O. **Only worth it if Tally ever wants to write the event log as direct S3 multipart uploads without the file-system layer** — a different project.
- **`object_store`** (Apache Arrow) — cleaner abstraction, but Tally already has `snapshot.rs` concretely implemented against `std::fs::File`. Rewriting to `object_store` costs ~2 weeks and buys portability to GCS/Azure nobody has asked for.
- **`mountpoint-s3`** — different product (Mountpoint for S3, a read-optimized FUSE filesystem). Not needed; S3 Files handles the mounting via the kernel NFS client.

**Single-binary philosophy stays intact.** Deploy guidance: "install `nfs-common` on the host, mount the bucket in your systemd unit before launching `tally`, point `--snapshot-dir` at the mount." That's it. No new Rust deps.

---

## Cost Model

**Workload A — Tally with 1M keys, 30 s snapshot cadence, 30-day retention.**

- Shard state: ~5 KB/key × 1M = **5 GB** per full base snapshot.
- Base snapshot cadence: every 30 min (Phase 9 incremental model). Delta snapshots every 30 s, ~1% of keys dirty = ~50 MB each.
- Per day: 48 base × 5 GB = 240 GB of base writes, 2,880 deltas × 50 MB = 144 GB of delta writes. **~384 GB/day written** to S3 Files.
- Retention of N=3 bases + rolling deltas between: working set at any time ≈ 15 GB (3 bases) + 1.5 GB (30 deltas) ≈ **~17 GB cached**.
- S3 Files cache cost: **17 GB × $0.30 = $5.10/month**.
- Underlying S3 storage (if nothing is deleted): 384 GB/day × 30 days = 11.5 TB; with N=3 retention + GC of old bases, **~20-50 GB persisted** = $0.46-$1.15/month S3 Standard.
- Write access: 384 GB/day × 30 days × $0.06 = **$691/month**. **This dominates.**
- Read access: only on restart / backfill. Negligible day-to-day.

**Total: ~$700/month for the snapshot workload of a 1M-key instance.** Overwhelmingly driven by the $0.06/GB write charge.

**Mitigation:** Tally's v1.4 zstd-dict compression (HORIZON-STORAGE-IO.md §6.8, NEXT-STEPS item 4) compresses postcard snapshots ~10×. Post-compression: **~38 GB/day written** → **$69/month**. This is the difference between S3 Files being viable and not — **zstd compression is a prerequisite**, not a nice-to-have.

**Workload B — Event log rotation to S3 Files (Role 2).**
At 100K events/sec × 200 B/event × 86,400 s = **1.7 TB/day** of event log. At $0.06/GB write that's **$3,060/month**. **Not viable at that scale**. Only works for lower-throughput users (<5K eps → ~$150/month) or after event log compaction.

**Workload shape that makes S3 Files cheap vs expensive:**
- **Cheap:** bursty writes with long idle periods; large read working set; low write-to-read ratio. Tally is **none of these** natively.
- **Expensive:** sustained high write rate with small working set. Tally's snapshot pattern is exactly this.

**Conclusion:** S3 Files is cost-viable for Tally **only if combined with aggressive compression**. This is an argument to prioritize the zstd-dict work in v1.4 regardless, and then unlock S3 Files in v1.5.

---

## Proposed Operational Modes

### Mode 1: `snapshot_backend = "local"` (default, unchanged)
Today's behavior. Local disk.

### Mode 2: `snapshot_backend = "s3files"` (v1.5 candidate)
```toml
[snapshot]
backend = "s3files"
dir = "/mnt/tally-state"          # NFS mount path
lease_file = "/mnt/tally-state/.lease"
instance_id = "prod-us-east-1a"
commit_window_s = 60              # document; don't try to tune below S3 Files floor
```
Tally acquires the lease on startup (atomic write + If-None-Match), refreshes every 15 s, releases on SIGTERM. Fail-fast if lease held by another instance. Snapshot writer uses the mount as if it were local disk.

### Mode 3: `snapshot_backend = "hybrid"` (v1.6 candidate)
```toml
[snapshot]
backend = "hybrid"
hot_dir = "/var/lib/tally"        # local NVMe, authoritative
cold_dir = "/mnt/tally-archive"   # S3 Files, async copy after fsync
retention = { hot = "1h", cold = "30d" }
```
Local NVMe is the source of truth; a background thread copies sealed snapshot files to the S3 Files mount. Recovery tries hot first, falls back to cold. This is the "best of both" but also the most code.

---

## Prior Art Comparison

| System | Object-storage persistence model | Compare to S3 Files |
|---|---|---|
| **Redis (RDB → S3)** | RDB file written locally; separate sidecar uploads via `aws s3 cp`. Manual. Async. | S3 Files replaces the sidecar with a kernel mount. Less moving parts. |
| **DragonflyDB** | Same as Redis plus native `SAVE DF` to S3 via aws-sdk-cpp (added 2024). Built-in. | S3 Files is a file-system alternative — DragonflyDB's native path avoids the 60 s commit window and pays per-PUT instead. For bursty snapshots Dragonfly's approach is cheaper; for continuous write streams S3 Files bundles PUTs. |
| **ClickHouse** | MergeTree-on-S3: data parts written directly as S3 objects via native SDK. Separates "hot" metadata from "cold" data. | ClickHouse's path is native S3 API with Tally-irrelevant Parquet/MergeTree structure. S3 Files would be a step backward for ClickHouse; it's a step forward for Tally because Tally's snapshot format is already file-shaped. |
| **Materialize** | Persist layer writes shard data to S3 as immutable blobs via aws-sdk. Uses CRDTs / versioned consensus for metadata. | Materialize solved the "multi-writer safety" problem with its own protocol (persistent consensus). Tally would need the same thing (the lease file) or be single-writer-by-policy. |
| **Mountpoint for S3** (2023) | Read-optimized FUSE filesystem over S3. Best-effort writes, no rename, no fsync semantics. | S3 Files is **Mountpoint's better-behaved sibling**: full POSIX, real writes, at the cost of the 60 s commit window and the EFS cache bill. Tally wants S3 Files, not Mountpoint. |

**One-liner:** Tally + S3 Files is structurally closest to **DragonflyDB + S3 snapshots**, minus DragonflyDB's need to statically link the AWS SDK.

---

## Open Questions for Product Decision

1. **Do we ship the v1.4 zstd-dict snapshot compression *before* or *with* S3 Files?** Cost math shows S3 Files needs ~10× compression to be viable. If we commit to S3 Files in v1.5, zstd-dict is on the critical path for v1.4. If we delay S3 Files, zstd-dict can float.
2. **Who is the target user?** The killer use case is **serverless deployments** (Role 5) and **blue/green/failover** (Role 4). Neither is on the stated v1.x roadmap. If the roadmap stays SMB-on-one-box, S3 Files is optional polish. If Tally wants to be deployable on Fly Machines / Fargate / Lambda SnapStart, S3 Files is the unlock.
3. **Is single-writer enforcement via lease file acceptable**, or do we need something stronger (external coordinator, DynamoDB lock)? The lease file is ~200 lines of code and matches Tally's "zero infrastructure" ethos. A DynamoDB lock is safer but drags in an AWS SDK.
4. **Can we tolerate the 90 s worst-case durability gap** (30 s snapshot cadence + 60 s commit delay)? Default claim today is "30 s loss on crash." Tripling that needs a release-note call-out.
5. **Regional scope.** Mount is region-local. Cross-region DR needs S3 replication layered on top — is that in scope for v1.5?
6. **Fallback semantics.** If the NFS mount returns `EIO` (network blip), do we (a) pause snapshots and keep serving reads/writes, (b) fail-stop, or (c) failover to a local disk backup? Phase planning must pick one.

---

## Recommendation

**Ship posture: prototype in v1.5, do not commit v1.4.**

Concrete sequence:

1. **v1.4** — land zstd-dict snapshot compression (already on the roadmap as NEXT-STEPS item 4). This is the cost precondition for S3 Files *and* a standalone win. No S3 Files work yet.
2. **v1.5 Phase A — S3 Files as snapshot backend (Role 1, Mode 2).** Add `snapshot_backend = "s3files"` config, a lease-file single-writer guard, and docs. Effort: **S (~1 week)** because the snapshot code is already POSIX. Benchmark snapshot write/restore on a mounted bucket vs local disk before declaring it shippable.
3. **v1.5 Phase B — Serverless deployment guide (Role 5).** Publish a Fly Machines / Fargate example with the S3 Files mount. This is the user-visible story that justifies the phase. No new code beyond Phase A plus a Dockerfile and a terraform snippet.
4. **v1.6 or later — hybrid mode (Mode 3)** and **event log archive (Role 2)**. Only if users ask.
5. **Never — cold-tier TTL revive (Role 3).** Don't. If users want this, build an async pre-warm API that doesn't pretend to be on the PUSH path.

**Do not ship S3 Files as the default backend.** Local disk stays the default for the box-on-NVMe user; S3 Files is the opt-in for users who need portability across compute instances.

**Biggest risk:** the $0.06/GB write charge. If zstd-dict under-delivers (say, only 3× compression), the monthly bill at 1M keys goes from $69 to ~$230 and the margin for "zero-ops" feels thinner. **Gate v1.5 S3 Files phase on v1.4 compression measurements hitting ≥8×.**

---
