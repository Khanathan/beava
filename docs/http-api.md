# Beava HTTP API

## Quickstart

Register a stream and push events from the command line in under 60 seconds:

```bash
# 1. Register a stream with windowed features
curl -X POST http://127.0.0.1:6401/pipelines \
  -H 'Content-Type: application/json' \
  -H "Authorization: Bearer ${BEAVA_ADMIN_TOKEN}" \
  -d '{"name":"transactions","key_field":"user","definition_type":"stream","features":[{"name":"tx_count_1h","type":"count","window":"1h","bucket":"1m"}]}'

# 2. Push an event
curl -X POST http://127.0.0.1:6401/push/transactions \
  -H 'Content-Type: application/json' \
  -H "Authorization: Bearer ${BEAVA_ADMIN_TOKEN}" \
  -d '{"user":"alice","amount":10.5}'

# 3. Read features back
curl http://127.0.0.1:6401/features/alice \
  -H "Authorization: Bearer ${BEAVA_ADMIN_TOKEN}"
```

---

## Event Ingest Endpoints

### POST /push/{stream}

Push a single JSON event to a registered stream. The event is accepted into the
in-memory ingest queue and acknowledged immediately (buffer-accept semantics).

**Path parameters**

| Param    | Description                             |
| -------- | --------------------------------------- |
| `stream` | Name of the registered stream to target |

**Query parameters**

| Param    | Default | Description                                                                                                  |
| -------- | ------- | ------------------------------------------------------------------------------------------------------------ |
| `sync`   | `0`     | Set to `1` to wait until the in-memory ingest queue drains before responding. Useful for tests and CLI tooling. **Durable-ack (fsync) is deferred to Phase 46.** See [Durability Semantics](#durability-semantics). |

**Request body**

A single JSON object. Arbitrary payload fields are accepted. The optional
`_event_time` field pins the event timestamp; omitting it uses server wall-clock.

```json
{"user": "alice", "amount": 10.5, "_event_time": 1700000000000}
```

`_event_time` may be:
- An integer (Unix milliseconds): `1700000000000`
- A string (RFC 3339): `"2023-11-14T22:13:20Z"`

**Response (200 — accepted)**

```json
{"ok": true}
```

**Response (400 — schema error)**

```json
{
  "ok": false,
  "error": {
    "code": "stream_not_registered",
    "message": "unknown stream: transactions"
  }
}
```

**Status codes**

| Code | Meaning                                                               |
| ---- | --------------------------------------------------------------------- |
| 200  | Event accepted into the ingest queue                                  |
| 400  | Invalid JSON body, schema mismatch, or stream not registered          |
| 401  | Missing or invalid `Authorization: Bearer <token>` header             |
| 413  | Body exceeds `BEAVA_HTTP_MAX_BODY` limit (default 16 MiB)             |
| 408  | Handler did not complete within the server-side timeout (30s default) |

---

#### curl

```bash
curl -X POST http://localhost:6401/push/transactions \
  -H 'Content-Type: application/json' \
  -H "Authorization: Bearer ${BEAVA_ADMIN_TOKEN}" \
  -d '{"user":"alice","amount":10.5,"_event_time":1700000000000}'
# → 200 {"ok":true}
```

#### Go (net/http)

```go
package main

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"os"
)

func main() {
	body := []byte(`{"user":"alice","amount":10.5,"_event_time":1700000000000}`)
	req, _ := http.NewRequest("POST", "http://localhost:6401/push/transactions", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+os.Getenv("BEAVA_ADMIN_TOKEN"))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	fmt.Printf("%d %s\n", resp.StatusCode, b)
}
```

#### Node (fetch)

```javascript
const body = { user: 'alice', amount: 10.5, _event_time: 1700000000000 };
const res = await fetch('http://localhost:6401/push/transactions', {
  method: 'POST',
  headers: {
    'Content-Type': 'application/json',
    'Authorization': `Bearer ${process.env.BEAVA_ADMIN_TOKEN}`,
  },
  body: JSON.stringify(body),
});
console.log(res.status, await res.text());
// → 200 {"ok":true}
```

---

### POST /push-batch/{stream}

Push a JSON array of events to a registered stream in one request. Each event's
`_event_time` is captured individually for per-event watermark gating.

> **Phase 46 note:** Per-event event-time bucketing (`push_batch_with_cascade_no_features`
> signature change) is a Phase 46 correctness fix. Phase 45 captures per-event
> timestamps in `PendingAsync.now`; Phase 46 drops the internal wrapping.

**Path parameters**

| Param    | Description                             |
| -------- | --------------------------------------- |
| `stream` | Name of the registered stream to target |

**Query parameters**

| Param  | Default | Description                                                          |
| ------ | ------- | -------------------------------------------------------------------- |
| `sync` | `0`     | Set to `1` to wait for in-memory queue drain. See [Durability Semantics](#durability-semantics). |

**Request body**

A JSON array of event objects. Maximum size is `BEAVA_HTTP_MAX_BODY` (default 16 MiB).
Each event follows the same schema as the single-event endpoint.

```json
[
  {"user": "alice", "amount": 5.0},
  {"user": "bob",   "amount": 20.0},
  {"user": "alice", "amount": 7.5}
]
```

**Response (200 — D-12 summary-only)**

```json
{
  "ok": true,
  "data": {
    "accepted": 3,
    "rejected": 0,
    "first_error": null
  }
}
```

If some events are rejected (e.g., schema mismatch), the rest are still accepted.
`first_error` contains the first failure's code and message.

**Status codes**

| Code | Meaning                                                   |
| ---- | --------------------------------------------------------- |
| 200  | Batch processed; see `accepted`/`rejected` in response    |
| 400  | Body is not a valid JSON array                            |
| 401  | Missing or invalid `Authorization: Bearer <token>` header |
| 413  | Body exceeds `BEAVA_HTTP_MAX_BODY` limit (default 16 MiB) |

---

#### curl

```bash
curl -X POST "http://localhost:6401/push-batch/transactions?sync=1" \
  -H 'Content-Type: application/json' \
  -H "Authorization: Bearer ${BEAVA_ADMIN_TOKEN}" \
  -d '[{"user":"alice","amount":5},{"user":"bob","amount":20},{"user":"alice","amount":7.5}]'
# → 200 {"ok":true,"data":{"accepted":3,"rejected":0,"first_error":null}}
```

#### Go (net/http)

```go
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
)

func main() {
	events := []map[string]interface{}{
		{"user": "alice", "amount": 5.0},
		{"user": "bob", "amount": 20.0},
		{"user": "alice", "amount": 7.5},
	}
	payload, _ := json.Marshal(events)
	req, _ := http.NewRequest("POST", "http://localhost:6401/push-batch/transactions?sync=1", bytes.NewReader(payload))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+os.Getenv("BEAVA_ADMIN_TOKEN"))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	fmt.Printf("%d %s\n", resp.StatusCode, b)
}
```

#### Node (fetch)

```javascript
const events = [
  { user: 'alice', amount: 5.0 },
  { user: 'bob',   amount: 20.0 },
  { user: 'alice', amount: 7.5 },
];
const res = await fetch('http://localhost:6401/push-batch/transactions?sync=1', {
  method: 'POST',
  headers: {
    'Content-Type': 'application/json',
    'Authorization': `Bearer ${process.env.BEAVA_ADMIN_TOKEN}`,
  },
  body: JSON.stringify(events),
});
const data = await res.json();
console.log(res.status, data);
// → 200 { ok: true, data: { accepted: 3, rejected: 0, first_error: null } }
```

---

### POST /push/{stream}/ndjson

Push events as newline-delimited JSON (NDJSON). Each line is a separate event
object. The server uses `axum-extra::JsonLines` for line-by-line streaming parse —
no full-array allocation in memory. Events are flushed to the ingest engine in
chunks of 1000.

Use this endpoint for:
- Backfill operations accumulating thousands of events per call
- Streaming producers that generate events one-per-line
- Large historical loads where the 16 MiB body limit would otherwise apply

**Path parameters**

| Param    | Description                             |
| -------- | --------------------------------------- |
| `stream` | Name of the registered stream to target |

**Request headers**

`Content-Type: application/x-ndjson`

**Request body**

One JSON object per line. Malformed lines are counted as `rejected`; the stream
is NOT aborted on a bad line.

```
{"user":"alice","amount":1.0}
{"user":"alice","amount":2.0}
{"user":"carol","amount":100.0}
{"user":"bob","amount":3.0}
{"user":"alice","amount":4.0}
```

**Response (200 — D-13 summary-only)**

```json
{
  "ok": true,
  "data": {
    "accepted": 5,
    "rejected": 0,
    "chunks": 1,
    "first_error": null
  }
}
```

`chunks` is the number of 1000-event flush batches sent to the engine.

**Status codes**

| Code | Meaning                                                   |
| ---- | --------------------------------------------------------- |
| 200  | All parseable lines processed; see `accepted`/`rejected`  |
| 401  | Missing or invalid `Authorization: Bearer <token>` header |

> Note: body limit does not apply to NDJSON — the stream is parsed line-by-line
> without buffering the full body. This makes NDJSON suitable for large backfills
> that exceed the 16 MiB JSON-array limit.

---

#### curl

```bash
printf '%s\n' \
  '{"user":"alice","amount":1}' \
  '{"user":"alice","amount":2}' \
  '{"user":"carol","amount":100}' \
  '{"user":"bob","amount":3}' \
  '{"user":"alice","amount":4}' \
  | curl -X POST http://localhost:6401/push/transactions/ndjson \
      -H 'Content-Type: application/x-ndjson' \
      -H "Authorization: Bearer ${BEAVA_ADMIN_TOKEN}" \
      --data-binary @-
# → 200 {"ok":true,"data":{"accepted":5,"rejected":0,"chunks":1,"first_error":null}}
```

#### Go (net/http)

```go
package main

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
)

func main() {
	ndjson := strings.Join([]string{
		`{"user":"alice","amount":1}`,
		`{"user":"alice","amount":2}`,
		`{"user":"carol","amount":100}`,
		`{"user":"bob","amount":3}`,
		`{"user":"alice","amount":4}`,
	}, "\n")
	req, _ := http.NewRequest("POST", "http://localhost:6401/push/transactions/ndjson", strings.NewReader(ndjson))
	req.Header.Set("Content-Type", "application/x-ndjson")
	req.Header.Set("Authorization", "Bearer "+os.Getenv("BEAVA_ADMIN_TOKEN"))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	fmt.Printf("%d %s\n", resp.StatusCode, b)
}
```

#### Node (fetch)

```javascript
const lines = [
  { user: 'alice', amount: 1 },
  { user: 'alice', amount: 2 },
  { user: 'carol', amount: 100 },
  { user: 'bob',   amount: 3 },
  { user: 'alice', amount: 4 },
].map(e => JSON.stringify(e)).join('\n');

const res = await fetch('http://localhost:6401/push/transactions/ndjson', {
  method: 'POST',
  headers: {
    'Content-Type': 'application/x-ndjson',
    'Authorization': `Bearer ${process.env.BEAVA_ADMIN_TOKEN}`,
  },
  body: lines,
});
console.log(res.status, await res.text());
// → 200 {"ok":true,"data":{"accepted":5,"rejected":0,"chunks":1,"first_error":null}}
```

---

## Read Endpoints

### GET /features/{key}

Return the current computed feature values for an entity key. By default, returns
all tables. Use `?table=X` to filter to one table.

Read endpoints are available on the **admin router** by default. When the server is
started with `--public`, they move to the public router (no token required).
See [Authentication](#authentication).

**Path parameters**

| Param | Description                              |
| ----- | ---------------------------------------- |
| `key` | Entity key value (e.g., user ID, device) |

**Query parameters**

| Param   | Default | Description                                     |
| ------- | ------- | ----------------------------------------------- |
| `table` | (all)   | Filter results to a single table (stream name)  |

**Response (200)**

```json
{
  "ok": true,
  "data": {
    "key": "alice",
    "tables": {
      "transactions": {
        "tx_count_1h": 12,
        "tx_sum_1h": 455.5,
        "velocity_spike": 2.1
      }
    }
  }
}
```

**Response (404 — key not found)**

```json
{
  "ok": false,
  "error": {
    "code": "key_not_found",
    "message": "no entity for key alice"
  }
}
```

**Status codes**

| Code | Meaning                                            |
| ---- | -------------------------------------------------- |
| 200  | Feature values returned                            |
| 401  | Missing or invalid `Authorization` (non-public)    |
| 404  | No entity found for the given key                  |

---

#### curl

```bash
# All tables
curl http://localhost:6401/features/alice \
  -H "Authorization: Bearer ${BEAVA_ADMIN_TOKEN}"

# Single table filter
curl "http://localhost:6401/features/alice?table=transactions" \
  -H "Authorization: Bearer ${BEAVA_ADMIN_TOKEN}"
```

#### Go (net/http)

```go
package main

import (
	"fmt"
	"io"
	"net/http"
	"os"
)

func main() {
	req, _ := http.NewRequest("GET", "http://localhost:6401/features/alice", nil)
	req.Header.Set("Authorization", "Bearer "+os.Getenv("BEAVA_ADMIN_TOKEN"))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	fmt.Printf("%d %s\n", resp.StatusCode, b)
}
```

#### Node (fetch)

```javascript
const res = await fetch('http://localhost:6401/features/alice', {
  headers: {
    'Authorization': `Bearer ${process.env.BEAVA_ADMIN_TOKEN}`,
  },
});
const data = await res.json();
console.log(res.status, JSON.stringify(data, null, 2));
```

---

### GET /streams

List all registered streams with their names and current watermarks.

**Response (200)**

```json
{
  "ok": true,
  "data": {
    "streams": [
      {"name": "transactions", "watermark_ms": 1700000042000},
      {"name": "logins",       "watermark_ms": 1700000038000}
    ]
  }
}
```

`watermark_ms` is the Unix millisecond timestamp of the latest event processed by
that stream. `null` if no events have been processed yet.

**Status codes**

| Code | Meaning                                            |
| ---- | -------------------------------------------------- |
| 200  | Stream list returned                               |
| 401  | Missing or invalid `Authorization` (non-public)    |

---

#### curl

```bash
curl http://localhost:6401/streams \
  -H "Authorization: Bearer ${BEAVA_ADMIN_TOKEN}"
```

#### Go (net/http)

```go
package main

import (
	"fmt"
	"io"
	"net/http"
	"os"
)

func main() {
	req, _ := http.NewRequest("GET", "http://localhost:6401/streams", nil)
	req.Header.Set("Authorization", "Bearer "+os.Getenv("BEAVA_ADMIN_TOKEN"))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	fmt.Printf("%d %s\n", resp.StatusCode, b)
}
```

#### Node (fetch)

```javascript
const res = await fetch('http://localhost:6401/streams', {
  headers: {
    'Authorization': `Bearer ${process.env.BEAVA_ADMIN_TOKEN}`,
  },
});
console.log(res.status, await res.text());
```

---

### GET /streams/{name}

Return details for a single registered stream: name, watermark, and the list of
feature definitions.

**Path parameters**

| Param  | Description                     |
| ------ | ------------------------------- |
| `name` | Name of the registered stream   |

**Response (200)**

```json
{
  "ok": true,
  "data": {
    "name": "transactions",
    "watermark_ms": 1700000042000,
    "features": [
      {"name": "tx_count_1h", "type": "Count { window_secs: 3600, bucket_secs: 60 }"},
      {"name": "tx_sum_1h",   "type": "Sum { field: \"amount\", window_secs: 3600 }"}
    ]
  }
}
```

> Note: the `type` field is the Rust debug representation of the `FeatureDef` variant.
> This will be replaced with a structured schema in Phase 47.

**Response (404 — stream not found)**

```json
{
  "ok": false,
  "error": {
    "code": "stream_not_found",
    "message": "stream transactions not registered"
  }
}
```

**Status codes**

| Code | Meaning                                            |
| ---- | -------------------------------------------------- |
| 200  | Stream detail returned                             |
| 401  | Missing or invalid `Authorization` (non-public)    |
| 404  | No stream registered under that name               |

---

#### curl

```bash
curl http://localhost:6401/streams/transactions \
  -H "Authorization: Bearer ${BEAVA_ADMIN_TOKEN}"
```

#### Go (net/http)

```go
package main

import (
	"fmt"
	"io"
	"net/http"
	"os"
)

func main() {
	req, _ := http.NewRequest("GET", "http://localhost:6401/streams/transactions", nil)
	req.Header.Set("Authorization", "Bearer "+os.Getenv("BEAVA_ADMIN_TOKEN"))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	fmt.Printf("%d %s\n", resp.StatusCode, b)
}
```

#### Node (fetch)

```javascript
const res = await fetch('http://localhost:6401/streams/transactions', {
  headers: {
    'Authorization': `Bearer ${process.env.BEAVA_ADMIN_TOKEN}`,
  },
});
const data = await res.json();
console.log(res.status, JSON.stringify(data, null, 2));
```

---

## Authentication

All write endpoints (`POST /push/*`) and, by default, all read endpoints
(`GET /features/*`, `GET /streams*`) require authentication.

**Token authentication**

Pass a Bearer token in the `Authorization` header:

```
Authorization: Bearer <token>
```

The token must match the `BEAVA_ADMIN_TOKEN` environment variable set when the
server started.

**Loopback bypass**

Requests originating from `127.0.0.1` or `::1` (localhost) are automatically
authenticated — no token required. This allows local tooling and scripts to omit
the header.

**Public read endpoints**

Start the server with `--public` to serve the three read endpoints
(`/features/*`, `/streams`, `/streams/*`) on the public router without
authentication. Write endpoints always require authentication regardless of
`--public`.

```bash
beava serve --public
```

**401 Unauthorized**

When authentication fails, the server responds with HTTP 401 and a JSON body:

```json
{
  "ok": false,
  "error": {
    "code": "unauthorized",
    "message": "missing or invalid authorization token"
  }
}
```

> **Phase 45 note:** The status code for auth failures is `401 Unauthorized`
> (not 403). This is an intentional change from older Beava versions (orchestrator
> decision A4).

---

## Durability Semantics

### Default: buffer-accept

By default, `POST /push/{stream}` and `POST /push-batch/{stream}` return `200`
after the event is accepted into the **in-memory ingest queue**. The response does
NOT wait for the event log to be flushed to disk (`fsync`).

This gives maximum throughput (>100 K EPS on `/push-batch`). Events in the
in-memory queue are durable to clean shutdowns but not to hard crashes.

### `?sync=1`: wait for in-memory drain

Adding `?sync=1` to a push request causes the handler to wait until the in-memory
ingest queue drains before responding. This makes writes observable immediately on
the next `GET /features` call without a race:

```bash
# Push event, then immediately read features with no race
curl -X POST "http://localhost:6401/push/transactions?sync=1" \
  -H 'Content-Type: application/json' \
  -H "Authorization: Bearer ${BEAVA_ADMIN_TOKEN}" \
  -d '{"user":"alice","amount":10.5}'

curl http://localhost:6401/features/alice \
  -H "Authorization: Bearer ${BEAVA_ADMIN_TOKEN}"
```

`?sync=1` is useful for tests and CLI tooling. Expect a throughput drop (from
>100 K EPS to ~10-50 K EPS) because each request now waits for queue drain.

### Durable-ack (fsync) — Phase 46

**Durable write-acknowledgment (waiting for `fsync` to the SSD event log) is
deferred to Phase 46.** When Phase 46 ships, a new `?durable=1` query param (or a
separate endpoint) will provide crash-safe write acknowledgment. Until then, `?sync=1`
is the strongest guarantee available.

> Orchestrator decision A7: in-memory sync semantics in Phase 45; durable-ack in Phase 46.

---

## Observability

### Prometheus metrics

The `/metrics` endpoint (no auth required) exposes Prometheus text format:

```bash
curl http://localhost:6401/metrics
```

**Event counters**

```
# HELP beava_events_total Total events processed
# TYPE beava_events_total counter
beava_events_total 892041
beava_events_total{proto="http"} 102000
beava_events_total{proto="tcp"}  790041
```

> **Deprecation notice:** The unlabeled `beava_events_total` counter (no `proto`
> label) is **deprecated and will be removed in Phase 47**. Consumers should
> migrate to `beava_events_total{proto="http"}` and `beava_events_total{proto="tcp"}`
> to distinguish ingest path throughput.

**Other metrics**

| Metric                            | Type    | Description                                     |
| --------------------------------- | ------- | ----------------------------------------------- |
| `beava_keys_total`                | gauge   | Number of entity keys in the in-memory store    |
| `beava_push_latency_seconds`      | gauge   | Last observed PUSH latency                      |
| `beava_snapshot_duration_seconds` | gauge   | Duration of the most recent snapshot write      |
| `beava_memory_bytes`              | gauge   | Estimated memory usage (~2 KB per entity key)   |
| `beava_snapshots_skipped_total`   | counter | Snapshot cycles skipped (previous still running)|

---

## Body Limits

| Source                 | Default | Override                                           |
| ---------------------- | ------- | -------------------------------------------------- |
| Single event / batch   | 16 MiB  | `BEAVA_HTTP_MAX_BODY=<bytes>` environment variable |
| NDJSON streaming       | Unlimited (line-by-line parse) | N/A                     |

```bash
# Start server with a 64 MiB body limit
BEAVA_HTTP_MAX_BODY=67108864 beava serve
```

When the body limit is exceeded, the server returns HTTP 413 with a JSON envelope
before the handler runs:

```json
{
  "ok": false,
  "error": {
    "code": "body_limit_exceeded",
    "message": "request body too large"
  }
}
```

---

## Configuration Reference

| Variable             | Default | Description                                                  |
| -------------------- | ------- | ------------------------------------------------------------ |
| `BEAVA_HTTP_PORT`    | `6401`  | HTTP API listen port                                         |
| `BEAVA_ADMIN_TOKEN`  | (none)  | Bearer token required for authenticated endpoints            |
| `BEAVA_HTTP_MAX_BODY`| `16777216` | Maximum request body in bytes for JSON endpoints (16 MiB) |

---

# Appendix: Legacy Management Endpoints

The following endpoints were part of the original Beava HTTP management API.
They remain fully supported and have not changed. They are demoted to the appendix
because Phase 45 adds higher-throughput ingest + read paths that are the primary
developer surface.

---

### GET /health

Health check. Returns immediately; suitable for load-balancer probes.

```json
{"status": "ok"}
```

```bash
curl http://localhost:6401/health
```

---

### GET /metrics

Prometheus metrics in text exposition format (Content-Type: `text/plain; version=0.0.4`).

```bash
curl http://localhost:6401/metrics
```

See [Observability](#observability) above for the full metric reference.

---

### POST /pipelines

Register a new stream or view pipeline definition. Used by the Python SDK
`tl.register_remote()` call internally.

**Request body**

```json
{
  "name": "Transactions",
  "key_field": "user_id",
  "definition_type": "stream",
  "features": [
    {"name": "tx_count_1h", "type": "count", "window": "1h", "bucket": "1m"},
    {"name": "tx_sum_1h",   "type": "sum",   "field": "amount", "window": "1h", "bucket": "1m"}
  ]
}
```

For views, set `"definition_type": "view"`.

**Response (success)**

```json
{"status": "ok"}
```

**Status codes:** 200 (success), 400 (invalid body or validation error).

```bash
curl -X POST http://localhost:6401/pipelines \
  -H "Content-Type: application/json" \
  -d '{"name":"Transactions","key_field":"user_id","definition_type":"stream","features":[]}'
```

---

### GET /pipelines

List all registered pipeline names.

```json
{"pipelines": ["Transactions", "Logins", "MerchantActivity"]}
```

```bash
curl http://localhost:6401/pipelines
```

---

### GET /pipelines/:name

Get the full definition of a registered pipeline.

```bash
curl http://localhost:6401/pipelines/Transactions
```

---

### DELETE /pipelines/:name

Remove a registered pipeline and deregister its stream from the event log.

```bash
curl -X DELETE http://localhost:6401/pipelines/Transactions
```

---

### POST /snapshot

Trigger a manual snapshot write to disk.

**Query parameters:** `wait=true` to block until complete; `timeout_ms=<ms>` limits wait time.

```bash
# Fire-and-forget
curl -X POST http://localhost:6401/snapshot

# Wait for completion with 5s timeout
curl -X POST "http://localhost:6401/snapshot?wait=true&timeout_ms=5000"
```

**Status codes:** 200 (written), 404 (snapshots disabled), 408 (timeout), 409 (already in progress), 500 (I/O error).

---

### GET /debug/key/:key

Inspect the full internal state for an entity key: live operator states, static
features, computed feature values, last event timestamp.

```bash
curl http://localhost:6401/debug/key/u123
```

---

### GET /debug/memory

Memory usage breakdown by stream, including entity count and estimated byte totals.

```bash
curl http://localhost:6401/debug/memory
```

---

### GET /debug/topology

Pipeline DAG topology: nodes (streams + views), edges (cascade + lookup
dependencies), and the cached topological execution order.

```bash
curl http://localhost:6401/debug/topology
```

---

### GET /debug/throughput

Per-stream throughput using exponentially weighted moving averages (EWMA) over
5-second, 1-minute, and 5-minute windows. Values are events per second.

```bash
# Watch throughput every 2 seconds
watch -n 2 'curl -s http://localhost:6401/debug/throughput | jq .'
```

---

### GET /debug/latency

Per-command and per-stream latency histograms.

```bash
curl http://localhost:6401/debug/latency
```

---

### GET /debug/backfill

Status of active and completed backfill tasks.

```bash
curl http://localhost:6401/debug/backfill
```

---

### GET / and GET /static/*file

Debug UI index page and static assets for the built-in web dashboard.

---

## Debug Endpoints Guide

### Inspecting a Specific Entity

```bash
curl -s http://localhost:6401/debug/key/u123 | jq .
```

Look for `computed_features` to see final feature values, `live_operators` for raw
ring buffer state, and `last_event_at` for freshness.

### Understanding Memory Usage

```bash
curl -s http://localhost:6401/debug/memory | jq .
```

Views show `key_count: 0` (computed on read, not stored). Streams with HLL
(`distinct_count`) operators will have higher per-key memory than average.

### Visualizing the Pipeline DAG

```bash
curl -s http://localhost:6401/debug/topology | jq .
```

`topo_order` is the execution order — streams earlier in the list are evaluated
before streams that appear later. Derive expressions referencing a later stream
may see stale values by one event.

### Monitoring Throughput

```bash
watch -n 2 'curl -s http://localhost:6401/debug/throughput | jq .'
```

### Monitoring Backfill Progress

```bash
curl -s http://localhost:6401/debug/backfill | jq '.backfill_tasks[] | select(.status == "running")'
```

### Checking Snapshot Health

```bash
curl -s -X POST "http://localhost:6401/snapshot?wait=true&timeout_ms=10000" | jq .
```

A 409 response means a snapshot is already in progress. Monitor
`beava_snapshots_skipped_total` in `/metrics` if periodic snapshots are being skipped.
