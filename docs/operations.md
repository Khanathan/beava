# Operations

Everything you need to read before deploying Beava to real infrastructure.
This page pairs with [docs/architecture.md](architecture.md) (which explains
*why* the system is shaped the way it is) and [docs/event-time.md](event-time.md)
(which explains the correctness semantics you will be tuning around).

---

## Sizing

### Memory

Beava keeps all operator state in RAM. Per-entity cost depends on the operator
mix — specifically whether you use probabilistic sketches (HLL++, UDDSketch)
which carry a fixed per-instance overhead.

Back-of-envelope estimates:

| Pipeline shape | Per-entity RAM | 1 M entities |
|----------------|----------------|--------------|
| Session agent (count + last + stddev, no sketches) | ~200 B | ~200 MB |
| Standard aggregation (count + sum + avg + distinct_count HLL) | ~1 KB | ~1 GB |
| Fraud-scoring (47 features, multiple HLL + percentile sketches) | ~616 KB | ~620 GB |

The fraud-scoring estimate comes from the benchmark pipeline at
`benchmark/fraud-pipeline/`; the session-agent estimate is from
`examples/session-features/`.

Set `BEAVA_MEMORY_LIMIT_MB` to trigger soft-warning signals at 85% and 95% of
process RSS (see [Observability](#observability)). The signal does NOT reject
writes; it is an early-warning for the operator to resize before the kernel OOM
killer fires.

### Disk

- **WAL:** grows unbounded by default. The WAL is retained for backfill and fork
  replay; set `BEAVA_EVENT_LOG_MAX_BYTES` to cap it (events beyond the cap are
  purged and unavailable for replay).
- **Snapshots:** on-disk size approximates the in-memory RSS (postcard
  serialization). Expect 1:1 with RAM for a fully-loaded instance.
- **Recovery wall-clock:** scales linearly with snapshot size. Baseline from
  the Phase 47 benchmark (see [`benchmark/`](../benchmark/)):
  **7 s for 4.7 GB / 10.3 M events / 25 K entities** on a 10-core Apple M4
  with NVMe. Plan for ~1.5 s/GB on commodity NVMe.
- **Snapshot load is synchronous** before the listener binds. The
  `/debug/ready` endpoint returns 200 only after recovery completes — use it as
  your readiness probe, not `/health`.

### Ports

| Port | Protocol | Purpose |
|------|----------|---------|
| 6900 | HTTP | Push + read + admin (default) |
| 6400 | TCP | Binary protocol (Python SDK, fork replica) |
| 6901 | HTTP | Public read-only surface (`--public` flag) |

All ports are configurable via environment variables (see [Tuning](#tuning-knobs)).

### CPU

Single-node, multi-threaded. Default worker threads: 4. Hot-key contention is
the main scaling bottleneck — concurrent writers on a single entity key contend
on a per-key lock. The `benchmark/` matrix shows visible p99 degradation with 8
concurrent clients on a 10 K Zipfian key distribution vs a single client.
Vertical scaling (more cores, faster NVMe) is the primary lever today.

---

## Durability

Beava's durability model is **at-least-once**. Exactly-once is not claimed;
idempotency on accumulating operators (count, sum) is the client's
responsibility — use an upstream dedup key if needed.

### WAL fsync policy

Every event is written to the per-stream WAL **before** the engine extracts
features. This write-before-extract ordering means a crash between append and
extraction is safe: recovery replays the event from the log.

| Ingest path | fsync policy |
|-------------|-------------|
| TCP single-event | fsync before client ack — zero data-loss window for in-flight events |
| HTTP single push (`POST /push/{stream}`) | group-commit batched fsync; ~1 ms window |
| HTTP batch push (`POST /push-batch/{stream}`) | per-batch fsync; batch survives or is rejected atomically |
| HTTP NDJSON stream (`POST /push/{stream}/ndjson`) | per-chunk fsync; chunk size = 1000 events |

**`?sync=1` flag:** causes the HTTP handler to wait until the in-memory ingest
queue drains before responding. This makes writes observable immediately on the
next `GET /features` call. Useful in tests and CLI tooling. Expect throughput
to drop from >100 K EPS to ~10-50 K EPS because each request waits for queue
drain. Does not provide a durable fsync guarantee beyond the group-commit window.

A scaffolded `EventLog::append_with_fsync` path exists in `src/state/event_log.rs`
for a future `?durable=1` upgrade; it is not wired to any endpoint in v1.0.
See [docs/event-time.md § Crash-Replay Determinism](event-time.md#crash-replay-determinism).

---

## Crash recovery

### What happens on an ungraceful crash

- **Ungraceful crash (`kill -9`, power loss):** bounded data-loss window equal
  to the delta-snapshot interval (default 30 s) plus the ~1 ms fsync
  group-commit window. No corruption — WAL entries are either fully written
  (CRC-validated) or absent.
- **Graceful shutdown (SIGTERM):** the server drains the ingest queue, flushes
  a final delta snapshot, and closes the WAL cleanly. Recovery from a clean
  shutdown is near-instant.

### Recovery sequence

On startup, Beava runs the following synchronously before binding the listener:

1. Load the latest **base snapshot** from `<data-dir>/`.
2. Apply every **delta snapshot** written after the base.
3. **Replay the WAL** from the resume cursor (the WAL position recorded in
   the last delta) to reconstruct any state not yet captured by snapshots.

The listener binds and `/debug/ready` returns 200 only after step 3 completes.

### Crash-replay determinism

Live-ingest and post-crash-replay produce **bit-identical feature values** for
the same event sequence. This guarantee holds because:

- Replay uses `_event_time` from the event payload as the bucketing clock (not
  the `LogEntry.timestamp` wall-clock field). Events with explicit `_event_time`
  land in the same bucket they occupied during live ingestion.
- Write-before-extract ordering ensures no event is lost between WAL append and
  feature extraction.

See [docs/event-time.md § Crash-Replay Determinism](event-time.md#crash-replay-determinism)
for the full semantics and the ship-gate test that validates this guarantee.

### Crash recovery runbook

```bash
# 1. Confirm the process is down and check the reason
systemctl status tally

# 2. Identify the latest snapshot chain
ls -lh /var/lib/tally/*.snapshot.*

# 3. Start the server; recovery runs synchronously
systemctl start tally

# 4. Poll until recovery completes (readiness probe)
until curl -sf http://localhost:6900/debug/ready; do sleep 1; done

# 5. Check the recovery wall-clock
curl -s http://localhost:6900/metrics | grep beava_boot
```

---

## Snapshot cycle

| Snapshot type | Default cadence | Purpose |
|---------------|----------------|---------|
| Base snapshot | Every 300 s (`BEAVA_FULL_SNAPSHOT_INTERVAL`) | Full state dump — recovery anchor |
| Delta snapshot | Every 30 s (`BEAVA_DELTA_SNAPSHOT_INTERVAL`) | Incremental changes — caps data-loss window |

Snapshots run on `spawn_blocking` so the ingest path is not synchronously
blocked. Snapshot latency under sustained write load is not yet benchmarked;
plan headroom for occasional blocking on the snapshot write path.

Trigger a manual snapshot at any time:

```bash
# Fire-and-forget
curl -X POST http://localhost:6900/snapshot

# Wait for completion (5 s timeout)
curl -X POST "http://localhost:6900/snapshot?wait=true&timeout_ms=5000"
```

---

## Tuning knobs

| Env var | Default | What it controls |
|---------|---------|-----------------|
| `BEAVA_HTTP_PORT` | `6900` | HTTP listen port |
| `BEAVA_TCP_PORT` | `6400` | TCP binary protocol port |
| `BEAVA_DATA_DIR` | `/data` (container) | WAL + snapshot directory |
| `BEAVA_ADMIN_TOKEN` | _(unset)_ | Bearer token required for non-loopback admin calls |
| `BEAVA_FULL_SNAPSHOT_INTERVAL` | `300` (s) | Base snapshot cadence |
| `BEAVA_DELTA_SNAPSHOT_INTERVAL` | `30` (s) | Delta snapshot cadence (data-loss window) |
| `BEAVA_WORKER_THREADS` | `4` | Tokio worker thread count |
| `BEAVA_MEMORY_LIMIT_MB` | _(unset)_ | Soft RSS cap; drives 85%/95% warnings at `/debug/warnings` |
| `BEAVA_HTTP_MAX_BODY` | `2097152` (2 MiB) | Per-request body cap for JSON endpoints |
| `BEAVA_EVENT_LOG_MAX_BYTES` | _(unset)_ | WAL size cap; older events purged when exceeded |
| `BEAVA_TCP_BIND` | `127.0.0.1` | TCP bind address (set `0.0.0.0` to accept remote SDK connections) |

**Per-stream watermark lateness** is not an env var — it is set per stream in
Python (`@bv.stream(watermark_lateness="10m")`) or in the pipeline registration
payload. See [docs/event-time.md § Per-stream override](event-time.md#per-stream-override).

---

## Observability

### Prometheus metrics (`/metrics`)

No auth required. Exposes Prometheus text format.

Key metrics for operations:

| Metric | Type | What to alert on |
|--------|------|-----------------|
| `beava_events_total{proto="http"}` | counter | Sudden drop → ingest path broken |
| `beava_events_total{proto="tcp"}` | counter | Sudden drop → SDK / replica broken |
| `beava_keys_total` | gauge | Unbounded growth → missing TTL or key cardinality explosion |
| `beava_ring_buffer_drops_total{stream,operator_kind,reason}` | counter | Any nonzero → late events or window too small |
| `beava_late_events_dropped_total` | counter | Any nonzero → watermark lateness too tight |
| `beava_snapshot_duration_seconds` | gauge | Exceeds delta interval → snapshots piling up |
| `beava_snapshots_skipped_total` | counter | Nonzero → snapshot can't keep up with write rate |
| `beava_memory_bytes` | gauge | Track alongside RSS; difference indicates sketch overhead |
| `beava_boot_recovery_seconds` | gauge | Recovery wall-clock; compare across restarts |

`beava_ring_buffer_drops_total` and `beava_late_events_dropped_total` are
mutually exclusive: ring-buffer drops are events that fit within the lateness
window but exceeded the ring buffer capacity; late-event drops are events older
than `observed_max - watermark_lateness`.

### Admin probes

| Endpoint | Auth | Purpose |
|----------|------|---------|
| `GET /health` | None | Liveness — returns 200 if the process is running |
| `GET /debug/ready` | None | Readiness — returns 200 only after recovery completes |
| `GET /debug/warnings` | None | RSS alerts, watermark lateness warnings, skipped-snapshot alerts |
| `GET /debug/memory` | Token/loopback | Memory breakdown by stream and entity count |
| `GET /debug/topology` | Token/loopback | Pipeline DAG and topological execution order |
| `GET /debug/throughput` | Token/loopback | Per-stream EWMA throughput (5 s, 1 min, 5 min windows) |

Use `/health` as the **liveness probe** and `/debug/ready` as the **readiness
probe** in Kubernetes or systemd `ExecStartPost=` scripts.

### Structured logs

All log output is JSON on stdout. Fields: `timestamp`, `level`, `message`,
`stream` (when applicable), `entity_count`, `duration_ms`. Forward to your
aggregator of choice.

---

## Scaling posture

Beava is a **single-node** server today. It is designed for one box: NVMe,
4-16 cores, 8-64 GB RAM.

- **Vertical scaling:** add CPU, RAM, and NVMe bandwidth. The primary
  bottleneck is per-key lock contention under concurrent writes; more cores
  help up to ~16 threads before lock saturation.
- **Horizontal scaling:** deferred to Beava Cloud (Q4 2026 roadmap). The
  design path is thread-per-core runtime (v1.2) then multi-node via Kafka
  (v1.3+).
- **HA today:** cold standby with periodic snapshot rsync. Automated failover
  is Cloud.
- **Fork read scaling:** forks are read-only replicas that run locally and pull
  from the production server. They do not add write load to the primary.

---

## Security baseline (self-hosted)

- All write endpoints and admin reads require `BEAVA_ADMIN_TOKEN` bearer auth
  on non-loopback origins. Requests from `127.0.0.1` / `::1` are
  unconditionally trusted — do not expose the TCP port to untrusted networks.
- `/health`, `/metrics`, and `/debug/ready` are unauthenticated by design;
  safe to expose via a public load-balancer health probe.
- Terminate TLS at Caddy, nginx, or your cloud edge (Fly.io, Railway). Beava
  does not terminate TLS in v1.0.
- Filesystem encryption (LUKS, dm-crypt, ZFS native encryption) at rest is the
  operator's choice.

Roadmap: built-in TLS on ingest (v1.x), mTLS + RBAC + audit log (v1.0 polish),
AES-256 at rest + SOC 2 Type II + HIPAA BAA (Beava Cloud, Q4 2026).

---

## Deployment

- **Docker (recommended for evaluation):** `docker run -p 6900:6900 beavadb/beava:latest`
- **Docker Compose (persistent dev):** see [`examples/docker-compose.yml`](../examples/docker-compose.yml)
  — mounts a named volume at `/data`, exposes both ports.
- **systemd (bare-metal / VM):** see [`deploy/`](../deploy/) for the unit file,
  Caddyfile, and provision script.
- **Kubernetes:** not officially supported. If you must, use a StatefulSet with
  a single replica and a PersistentVolume claim mounted at `/data`. Set
  `livenessProbe` to `/health` and `readinessProbe` to `/debug/ready`.

---

---

## Shard Sizing & Hot-Shard Diagnosis

This section covers the v1.2+ thread-per-core (TPC) shard model. Read
[docs/architecture-tpc.md](architecture-tpc.md) first for the full design rationale.

### Choosing BEAVA_SHARDS

- Release builds default to `num_cpus::get_physical()` (all physical cores). Debug builds
  default to `1`.
- **8-core box:** `BEAVA_SHARDS=4` is a safe starting point. It leaves 4 cores available
  for the OS, network I/O, and background work. Raise to 8 once the workload is
  characterized.
- **16+ core boxes:** use the full physical core count. Each shard is single-threaded and
  benefits from exclusive L1/L2 cache residency.
- **Power-of-2 recommendation:** set `BEAVA_SHARDS` to a power of 2 where possible.
  `ahash mod N` is consistent regardless of N, but powers of 2 make key-distribution
  reasoning easier (half the keys go to the lower half, etc.).
- **Shard count mismatch on boot:** if the on-disk snapshot records a different shard
  count, the server refuses to boot with an actionable error. Run `tally reshard` before
  restarting with a new count (see below).

### Metrics to Watch

| Metric | Alert condition |
|--------|----------------|
| `beava_shard_keys_owned{shard}` | Any shard > 2× the fleet mean indicates a hot-shard condition |
| `beava_shard_reactor_utilization{shard}` | Sustained values > 0.85 indicate shard saturation |
| `shard_probe` `cross_shard_fraction` | Sustained values > 40% indicate `shard_key=` misalignment |
| `beava_shard_inbox_full_total{shard}` | Any nonzero → shard falling behind, clients receiving 503s |
| `beava_shard_inbox_depth{shard}` | Nonzero steady-state → shard consistently behind ingest rate |

`beava_shard_keys_owned` imbalance > 2× mean is the clearest hot-shard signal. Check this
before tuning `BEAVA_HOT_SHARD_THRESHOLD`.

### BEAVA_HOT_SHARD_THRESHOLD Tuning

Default: **1.5×** fleet mean `keys_owned`. When any shard exceeds this ratio, the
`/debug/shards` endpoint includes a `hot_shards` array and a warning log is emitted at
the `WARN` level.

| Setting | When to use |
|---------|-------------|
| 1.2× | Latency-sensitive workloads; catch hot shards early |
| 1.5× (default) | Balanced workloads; reasonable warning sensitivity |
| 2.0× | Naturally skewed key distributions where the operator explicitly accepts imbalance |

Raise the threshold only after confirming via `reactor_utilization` that the hot shard is
not actually saturated.

### Hot-Shard Diagnosis Flow

Follow these steps when you suspect a hot-shard condition:

1. **Inspect `/debug/shards`:**
   ```bash
   curl -s http://localhost:6900/debug/shards | jq '.hot_shards'
   ```
   A non-empty `hot_shards` array identifies the affected shard IDs.

2. **Run `shard_probe` on the hot shard:**
   ```bash
   curl -s "http://localhost:6900/debug/shard_probe?shard=N"
   ```
   Observe `cross_shard_fraction` and `keys_owned`.

3. **If hot-shard persists > 5 minutes:** inspect stream `shard_key=` declarations.
   The two most common causes:
   - Missing `shard_key=` declaration causes all events to route to shard 0.
   - Misaligned join declarations (both streams must use the same `shard_key=`).

4. **If the workload is inherently skewed** (e.g., Zipf key distribution, celebrity
   accounts receiving disproportionate event volume): increasing `BEAVA_SHARDS` spreads
   the total key space but does not eliminate a single hyper-popular key. In that case,
   consider application-level key salting. Increasing shard count still reduces contention
   on all other keys.

### Reshard Workflow

When the shard count needs to change:

1. Stop the server.
2. Run the reshard tool:
   ```bash
   tally reshard --from N --to K \
     --data-dir /var/lib/beava \
     --output /var/lib/beava-new \
     --replace
   ```
3. Restart with `BEAVA_SHARDS=K`.

Downtime equals the tool's runtime (primarily NVMe I/O). Online reshard is not available
in v1.2; it is deferred to v1.3+.

See [docs/architecture-tpc.md § Reshard Workflow](architecture-tpc.md#reshard-workflow)
for the full CLI reference and atomic swap semantics.

**Important (D-02):** do not manually write files into `data/logs/`. This directory is
managed exclusively by Beava's migration tooling. The legacy `data/logs/` layout is
emptied during the v8 snapshot migration and removed on the first clean shutdown after
migration. Write all data to the server via the HTTP or TCP ingest endpoints; the server
writes to `data/shard-N/streams/` internally.

### Ship-Gate Criteria as Production Health Indicators

These three criteria were validated before v1.2 shipped. They serve as ongoing health
baselines — run the benchmark suite against any major configuration change to verify they
still hold.

| Criterion | Threshold | What it means in production |
|-----------|-----------|----------------------------|
| N=1 throughput vs v1.1 baseline | Within −5% | No regression for single-shard deployments; TPC overhead is within budget |
| `complex-c8-x8` at N=CPU_COUNT | ≥ 3× baseline | Multi-core scaling is delivering; bottleneck is not the routing layer |
| `pareto-c8-x8` `cross_shard_fraction` | < 40% | Hot-key (Zipf) workloads are handled safely; routing skew is not overwhelming a shard |

If `cross_shard_fraction` exceeds 40% on your workload, inspect `shard_key=` declarations
and consider whether the application's key distribution is compatible with the current
shard count. See
[benchmark/pareto-c8-x8/README.md](../benchmark/pareto-c8-x8/README.md) for how to run
this cell against your configuration.

---

## Next reading

- [Architecture](architecture.md) — storage layout, WAL format, fork-replica
  design, and why Beava is single-node.
- [TPC Architecture](architecture-tpc.md) — thread-per-core shard model, routing,
  joins, recovery, reshard, fork/replica, and ship-gate rationale.
- [Event time](event-time.md) — bucket-boundary math, watermark lateness,
  crash-replay determinism, and TTL eviction semantics.
- [HTTP API](http-api.md) — full endpoint reference, authentication model, and
  `?sync=1` semantics.
- [Benchmark](../benchmark/) — reproducible performance numbers (9-cell matrix,
  TCP push, recovery, fork-replay).
