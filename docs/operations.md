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

## Next reading

- [Architecture](architecture.md) — storage layout, WAL format, fork-replica
  design, and why Beava is single-node.
- [Event time](event-time.md) — bucket-boundary math, watermark lateness,
  crash-replay determinism, and TTL eviction semantics.
- [HTTP API](http-api.md) — full endpoint reference, authentication model, and
  `?sync=1` semantics.
- [Benchmark](../benchmark/) — reproducible performance numbers (9-cell matrix,
  TCP push, recovery, fork-replay).
