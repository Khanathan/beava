# Beava HTTP Management API Reference

## Overview

Beava exposes a secondary HTTP API on port **6401** for management, monitoring, and debugging. This API is separate from the primary binary TCP protocol on port 6400 that handles the hot path (PUSH, GET, SET, MSET). The HTTP API is not designed for high-throughput event ingestion -- use the TCP protocol for that.

All responses are JSON unless otherwise noted. The API is built with Axum and runs on its own Tokio listener.

## Configuration

| Environment Variable | Default | Description |
|---|---|---|
| `BEAVA_HTTP_PORT` | `6401` | Port the HTTP management API listens on |

The server binds to `0.0.0.0:${BEAVA_HTTP_PORT}`.

---

## Endpoints

### GET /health

Health check endpoint. Returns immediately with a static response. Suitable for load balancer health probes.

**Response**

```json
{"status": "ok"}
```

**Status Codes**

| Code | Meaning |
|---|---|
| 200 | Server is running |

**Example**

```bash
curl http://localhost:6401/health
```

---

### GET /metrics

Prometheus-compatible metrics in text exposition format. Suitable for scraping by Prometheus or compatible monitoring systems.

**Response** (Content-Type: `text/plain; version=0.0.4`)

```
# HELP beava_keys_total Number of entity keys in memory
# TYPE beava_keys_total gauge
beava_keys_total 14523
# HELP beava_events_total Total events processed
# TYPE beava_events_total counter
beava_events_total 892041
# HELP beava_push_latency_seconds Last observed PUSH latency
# TYPE beava_push_latency_seconds gauge
beava_push_latency_seconds 0.000042
# HELP beava_snapshot_duration_seconds Last snapshot write duration
# TYPE beava_snapshot_duration_seconds gauge
beava_snapshot_duration_seconds 0.312
# HELP beava_memory_bytes Estimated memory usage
# TYPE beava_memory_bytes gauge
beava_memory_bytes 29741056
# HELP beava_snapshots_skipped_total Snapshot cycles skipped due to in-progress write
# TYPE beava_snapshots_skipped_total counter
beava_snapshots_skipped_total 0
```

**Metrics Reference**

| Metric | Type | Description |
|---|---|---|
| `beava_keys_total` | gauge | Number of entity keys currently in the in-memory store |
| `beava_events_total` | counter | Total events processed since server start |
| `beava_push_latency_seconds` | gauge | Last observed PUSH command latency |
| `beava_snapshot_duration_seconds` | gauge | Duration of the most recent snapshot write |
| `beava_memory_bytes` | gauge | Estimated memory usage (~2KB per entity key) |
| `beava_snapshots_skipped_total` | counter | Snapshot cycles skipped because a previous write was still in progress |

**Status Codes**

| Code | Meaning |
|---|---|
| 200 | Metrics returned |

**Example**

```bash
curl http://localhost:6401/metrics
```

---

### POST /pipelines

Register a new stream or view pipeline definition. The request body is the same JSON format produced by the Python SDK's serialization. Streams and views are distinguished by the `definition_type` field.

**Request Body**

```json
{
  "name": "Transactions",
  "key_field": "user_id",
  "definition_type": "stream",
  "features": [
    {"name": "tx_count_1h", "type": "count", "window": "1h", "bucket": "1m"},
    {"name": "tx_sum_1h", "type": "sum", "field": "amount", "window": "1h", "bucket": "1m"},
    {"name": "velocity_spike", "type": "derive", "expr": "(tx_count_1h / 1) / (tx_count_24h / 24)"}
  ]
}
```

For views, set `"definition_type": "view"`:

```json
{
  "name": "UserRisk",
  "key_field": "user_id",
  "definition_type": "view",
  "features": [
    {"name": "tx_to_login_ratio", "type": "derive", "expr": "Transactions.tx_count_1h / Logins.login_count_1h"},
    {"name": "merchant_chargebacks", "type": "lookup", "target": "MerchantActivity.chargeback_count_24h", "on": "merchant_id"}
  ]
}
```

**Response (success)**

```json
{"status": "ok"}
```

**Response (error)**

```json
{"error": "invalid request: missing field `name`"}
```

**Status Codes**

| Code | Meaning |
|---|---|
| 200 | Pipeline registered successfully |
| 400 | Invalid request body or validation error |

**Example**

```bash
curl -X POST http://localhost:6401/pipelines \
  -H "Content-Type: application/json" \
  -d '{
    "name": "Transactions",
    "key_field": "user_id",
    "definition_type": "stream",
    "features": [
      {"name": "tx_count_1h", "type": "count", "window": "1h", "bucket": "1m"},
      {"name": "tx_sum_1h", "type": "sum", "field": "amount", "window": "1h", "bucket": "1m"}
    ]
  }'
```

---

### GET /pipelines

List all registered pipeline names.

**Response**

```json
{
  "pipelines": ["Transactions", "Logins", "MerchantActivity"]
}
```

**Status Codes**

| Code | Meaning |
|---|---|
| 200 | List returned |

**Example**

```bash
curl http://localhost:6401/pipelines
```

---

### GET /pipelines/:name

Get the full definition of a registered pipeline, including all feature definitions with their configuration.

**Response (success)**

```json
{
  "name": "Transactions",
  "key_field": "user_id",
  "features": [
    {"name": "tx_count_1h", "type": "count", "window_secs": 3600, "bucket_secs": 60},
    {"name": "tx_sum_1h", "type": "sum", "field": "amount", "window_secs": 3600, "bucket_secs": 60, "optional": false},
    {"name": "avg_amount_1h", "type": "avg", "field": "amount", "window_secs": 3600, "bucket_secs": 60, "optional": false},
    {"name": "max_amount_24h", "type": "max", "field": "amount", "window_secs": 86400, "bucket_secs": 60, "optional": false},
    {"name": "unique_merchants", "type": "distinct_count", "field": "merchant_id", "window_secs": 86400, "bucket_secs": 60, "optional": false},
    {"name": "last_country", "type": "last", "field": "country", "optional": false},
    {"name": "velocity_spike", "type": "derive"}
  ]
}
```

Supported feature types in the response: `count`, `sum`, `avg`, `min`, `max`, `distinct_count`, `last`, `first`, `derive`, `lag`, `ema`, `last_n`, `stddev`, `percentile`, `exact_min`, `exact_max`.

**Response (not found)**

```json
{"error": "pipeline 'NonExistent' not found"}
```

**Status Codes**

| Code | Meaning |
|---|---|
| 200 | Pipeline definition returned |
| 404 | Pipeline not found |

**Example**

```bash
curl http://localhost:6401/pipelines/Transactions
```

---

### DELETE /pipelines/:name

Remove a registered pipeline. Also deregisters the stream from the event log.

**Response (success)**

```json
{"status": "ok"}
```

**Response (not found)**

```json
{"error": "pipeline 'NonExistent' not found"}
```

**Status Codes**

| Code | Meaning |
|---|---|
| 200 | Pipeline removed |
| 404 | Pipeline not found |

**Example**

```bash
curl -X DELETE http://localhost:6401/pipelines/Transactions
```

---

### POST /snapshot

Trigger a manual snapshot write. Writes a full base snapshot to disk. The snapshot includes all entity state, pipeline definitions, and backfill completion records.

**Query Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `wait` | bool | `false` | If `true`, block until the snapshot write completes before responding |
| `timeout_ms` | u64 | none | Maximum milliseconds to wait when `wait=true`. Returns 408 on timeout. |

**Response (success)**

```json
{
  "status": "ok",
  "bytes": 1048576,
  "duration_ms": 312
}
```

**Response (snapshots disabled)**

```json
{"error": "snapshots disabled"}
```

**Response (already in progress)**

```json
{"error": "snapshot cycle already in progress"}
```

**Response (timeout)**

```json
{"error": "snapshot timed out"}
```

**Status Codes**

| Code | Meaning |
|---|---|
| 200 | Snapshot written successfully |
| 404 | Snapshots are disabled on this server |
| 408 | Snapshot write timed out (only when `wait=true` with `timeout_ms`) |
| 409 | A snapshot is already in progress |
| 500 | Snapshot write failed (I/O error) |

**Examples**

```bash
# Fire-and-forget snapshot
curl -X POST http://localhost:6401/snapshot

# Wait for completion
curl -X POST "http://localhost:6401/snapshot?wait=true"

# Wait with a 5-second timeout
curl -X POST "http://localhost:6401/snapshot?wait=true&timeout_ms=5000"
```

---

### GET /debug/key/:key

Inspect the full internal state for an entity key. Returns live operator states, static features (from SET/MSET), computed feature values, and the last event timestamp.

**Response (success)**

```json
{
  "key": "u123",
  "live_operators": [
    {
      "name": "tx_count_1h",
      "stream": "Transactions",
      "state": "Counter { buckets: RingBuffer { ... } }"
    },
    {
      "name": "tx_sum_1h",
      "stream": "Transactions",
      "state": "Sum { buckets: RingBuffer { ... } }"
    }
  ],
  "static_features": {
    "lifetime_value": 4500.0,
    "segment": "high_value"
  },
  "computed_features": {
    "Transactions.tx_count_1h": 7,
    "Transactions.tx_sum_1h": 350.0,
    "Transactions.velocity_spike": 2.3,
    "lifetime_value": 4500.0
  },
  "last_event_at": 1712966400
}
```

**Response (not found)**

```json
{"error": "key 'u999' not found"}
```

**Status Codes**

| Code | Meaning |
|---|---|
| 200 | Key state returned |
| 404 | Key not found in the store |

**Example**

```bash
curl http://localhost:6401/debug/key/u123
```

---

### GET /debug/memory

Memory usage breakdown. Shows total entity count, registered stream count, an estimated byte total, and per-stream/view breakdowns.

**Response**

```json
{
  "entity_count": 14523,
  "stream_count": 3,
  "estimated_bytes": 29743104,
  "per_stream": [
    {
      "name": "Transactions",
      "kind": "stream",
      "key_count": 12000,
      "estimated_bytes": 24576000
    },
    {
      "name": "Logins",
      "kind": "stream",
      "key_count": 8500,
      "estimated_bytes": 17408000
    },
    {
      "name": "UserRisk",
      "kind": "view",
      "key_count": 0,
      "estimated_bytes": 0
    }
  ]
}
```

The `estimated_bytes` values are computed from the actual operator state in memory (ring buffer sizes, HLL register bytes, per-value overhead). Each stream entry also includes `operator_breakdown` (bytes by operator type) and `features` (per-feature byte totals). Views show `key_count: 0` because they are computed on read, not stored.

**Status Codes**

| Code | Meaning |
|---|---|
| 200 | Memory breakdown returned |

**Example**

```bash
curl http://localhost:6401/debug/memory
```

---

### GET /debug/topology

Pipeline DAG topology. Returns all registered streams and views as nodes, with edges representing cascade dependencies (between streams) and lookup dependencies (from views to streams). Includes the cached topological execution order.

**Response**

```json
{
  "nodes": [
    {
      "name": "Transactions",
      "kind": "stream",
      "key_field": "user_id",
      "features": ["tx_count_1h", "tx_sum_1h", "velocity_spike"],
      "operators": [
        {"name": "tx_count_1h", "op": "count", "window": "1h", "bucket": "1m"},
        {"name": "tx_sum_1h", "op": "sum", "field": "amount", "window": "1h", "bucket": "1m"},
        {"name": "velocity_spike", "op": "derive", "expr": "(tx_count_1h / 1) / (tx_count_24h / 24)"}
      ],
      "depends_on": []
    },
    {
      "name": "UserRisk",
      "kind": "view",
      "key_field": "user_id",
      "features": ["tx_to_login_ratio", "merchant_chargebacks"],
      "operators": [
        {"name": "tx_to_login_ratio", "op": "derive", "expr": "Transactions.tx_count_1h / Logins.login_count_1h"},
        {"name": "merchant_chargebacks", "op": "lookup", "target": "MerchantActivity.chargeback_count_24h", "on": "merchant_id"}
      ],
      "depends_on": []
    }
  ],
  "edges": [
    {"from": "MerchantActivity", "to": "UserRisk", "kind": "lookup"}
  ],
  "topo_order": ["Transactions", "Logins", "MerchantActivity", "UserRisk"]
}
```

**Node Fields**

| Field | Description |
|---|---|
| `name` | Stream or view name |
| `kind` | `"stream"` or `"view"` |
| `key_field` | The entity key field (may be `null` for keyless streams) |
| `features` | List of feature names defined on this stream/view |
| `operators` | Detailed operator definitions including type, window, field, expressions |
| `depends_on` | Upstream stream names (cascade dependencies). Always empty for views. |

**Edge Kinds**

| Kind | Description |
|---|---|
| `cascade` | Stream depends on another stream (via `depends_on`) |
| `lookup` | View has a lookup feature referencing a target stream |

**Status Codes**

| Code | Meaning |
|---|---|
| 200 | Topology returned |

**Example**

```bash
curl http://localhost:6401/debug/topology
```

---

### GET /debug/throughput

Per-stream throughput metrics using exponentially weighted moving averages (EWMA) over 5-second, 1-minute, and 5-minute windows.

**Response**

```json
{
  "streams": [
    {
      "name": "Transactions",
      "ewma_5s": 1234.5,
      "ewma_1m": 1100.2,
      "ewma_5m": 980.7
    },
    {
      "name": "Logins",
      "ewma_5s": 45.2,
      "ewma_1m": 42.1,
      "ewma_5m": 40.0
    }
  ]
}
```

The EWMA values represent events per second, decayed up to the current instant when the endpoint is called.

**Status Codes**

| Code | Meaning |
|---|---|
| 200 | Throughput data returned |

**Example**

```bash
curl http://localhost:6401/debug/throughput
```

---

### GET /debug/latency

Per-command and per-stream latency histograms. Provides detailed latency distribution data.

**Status Codes**

| Code | Meaning |
|---|---|
| 200 | Latency data returned |

**Example**

```bash
curl http://localhost:6401/debug/latency
```

---

### GET /debug/backfill

Status of active and completed backfill tasks. Shows progress for each backfill operation including the stream, features being backfilled, total events, processed events, and completion status.

**Response**

```json
{
  "backfill_tasks": [
    {
      "stream": "Transactions",
      "features": ["tx_count_1h", "tx_sum_1h"],
      "total_events": 50000,
      "processed_events": 32000,
      "completed": false,
      "status": "running"
    },
    {
      "stream": "Logins",
      "features": ["login_count_1h"],
      "total_events": 10000,
      "processed_events": 10000,
      "completed": true,
      "status": "completed"
    }
  ]
}
```

**Status Codes**

| Code | Meaning |
|---|---|
| 200 | Backfill status returned |

**Example**

```bash
curl http://localhost:6401/debug/backfill
```

---

### GET /

Debug UI index page. Serves the built-in web dashboard for visual inspection of pipelines, topology, throughput, and key state.

### GET /static/*file

Static assets for the debug UI (JavaScript, CSS, etc.).

---

## Debug Endpoints Guide

The debug endpoints are designed for troubleshooting production issues. Here is how to use them effectively.

### Inspecting a Specific Entity

Use `GET /debug/key/:key` to see everything Beava knows about an entity:

```bash
curl -s http://localhost:6401/debug/key/u123 | jq .
```

**What to look for:**

- **`live_operators`** -- The raw operator state including internal ring buffer contents. The `state` field is a Rust debug representation of the operator. Look for unexpected zero values or stale bucket data.
- **`static_features`** -- Features written via SET/MSET. If a feature you expect from a streaming pipeline shows up here, something wrote to it directly and may be overriding live values.
- **`computed_features`** -- The final feature values as a GET command would return them. Compare these against `live_operators` to verify derive expressions are computing correctly.
- **`last_event_at`** -- Unix timestamp of the most recent event for this key. If this is old, the entity may be approaching TTL eviction.

### Understanding Memory Usage

Use `GET /debug/memory` to identify which streams are consuming the most memory:

```bash
curl -s http://localhost:6401/debug/memory | jq .
```

**What to look for:**

- **`entity_count`** vs individual stream `key_count` -- A single entity can have state in multiple streams. The total entity count may be lower than the sum of per-stream key counts.
- **Views show `key_count: 0`** -- This is expected. Views are computed on read and do not store per-key state.
- **`estimated_bytes`** -- This is a rough estimate (~2KB per entity-stream pair). Actual memory depends on the number and type of operators. Streams with many windowed operators or HyperLogLog (distinct_count) features will use more.

### Visualizing the Pipeline DAG

Use `GET /debug/topology` to understand how streams and views are connected:

```bash
curl -s http://localhost:6401/debug/topology | jq .
```

**What to look for:**

- **`edges`** -- Cascade edges show stream-to-stream dependencies. Lookup edges show which streams a view reads from. Missing edges may indicate a misconfigured pipeline.
- **`topo_order`** -- The execution order Beava uses when processing events. Streams earlier in this list are evaluated first. If a derive expression references a stream that appears later, the value may be stale by one event.
- **`operators`** -- Per-node operator details including window sizes, fields, filter expressions, and derive formulas. Useful for verifying that the Python SDK serialized the pipeline correctly.

### Monitoring Throughput

Use `GET /debug/throughput` to see real-time event rates per stream:

```bash
# Watch throughput every 2 seconds
watch -n 2 'curl -s http://localhost:6401/debug/throughput | jq .'
```

**What to look for:**

- **`ewma_5s`** -- Most responsive to bursts. Good for detecting sudden traffic spikes.
- **`ewma_5m`** -- Smoothed long-term rate. Good for capacity planning.
- **Streams with zero throughput** -- May indicate a misconfigured producer or a stream that is registered but not receiving events.

### Monitoring Backfill Progress

Use `GET /debug/backfill` to track long-running backfill operations:

```bash
curl -s http://localhost:6401/debug/backfill | jq '.backfill_tasks[] | select(.status == "running")'
```

**What to look for:**

- **`processed_events` vs `total_events`** -- Progress indicator. If `processed_events` stops advancing, the backfill may be stuck.
- **Multiple running tasks** -- Backfills run concurrently. Watch for resource contention if many are active simultaneously.

### Checking Snapshot Health

Use `POST /snapshot` with `wait=true` to verify snapshots are working:

```bash
curl -s -X POST "http://localhost:6401/snapshot?wait=true&timeout_ms=10000" | jq .
```

**What to look for:**

- **`duration_ms`** -- If this is growing over time, the state store is getting larger. Consider TTL eviction settings.
- **`bytes`** -- Snapshot size on disk. Monitor for unexpected growth.
- **409 Conflict** -- A snapshot is already in progress. Check `beava_snapshots_skipped_total` in `/metrics` to see if periodic snapshots are being skipped due to slow writes.
