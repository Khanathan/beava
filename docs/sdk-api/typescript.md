# Beava TypeScript SDK

> **Status:** Authoritative for v0. Documents the **post-13.6 target** TS SDK
> shape — Phase 13.6 implements the port. Cross-language semantics live in
> [shared.md](shared.md); wire-level body shapes live in
> [docs/wire-spec.md](../wire-spec.md). Python is the canonical reference —
> see [python.md](python.md).
> **Last reviewed:** 2026-05-03 (Phase 13.0).

## Overview

The Beava TypeScript SDK ships as the npm package `@beava/sdk`. It mirrors
the [Python SDK](python.md) surface 1:1 at the wire-contract level (per
[shared.md](shared.md)) but uses idiomatic JavaScript / TypeScript
conventions where the language demands them:

- **camelCase** for multi-word identifiers (e.g., `app.batchGet(...)`,
  `eventName: string`, `withColumns({...})`). The transport layer
  translates wire JSON `snake_case` keys to camelCase on response and
  back to `snake_case` on requests.
- **Promise-based** API — every wire-bound method returns a `Promise<T>`.
  Synchronous SDK is reserved for v0.1+.
- **Builder-pattern feature declaration** — TypeScript stage-3 decorators
  (TS 5.0+) ship in v0.1+ per
  [`.planning/ideas/v0.1-deferrals.md`](../../.planning/ideas/v0.1-deferrals.md).
  v0 uses explicit builder calls (`bv.event({...})`, `bv.table({...})`).
- **53 op functions** matching the Python catalogue, named per
  [ADR-002](../../.planning/decisions/ADR-002-polars-op-rename.md) Polars
  conventions — `bv.mean`, `bv.var`, `bv.std`, `bv.nUnique`, `bv.quantile`.
  No deprecation aliases on the TS side; v0 is unreleased and TS users
  start with the new names directly.

> **npm package:** `@beava/sdk`. Install via `npm install @beava/sdk` or
> `yarn add @beava/sdk` or `pnpm add @beava/sdk`. The SDK targets Node.js
> 20+ (LTS) and modern browsers via the `"browser"` package export
> condition.

## Module structure

```
@beava/sdk/
├── index.ts             # public exports: BeavaApp, event, table, col, lit, count, sum, mean, ...
├── app.ts               # BeavaApp class
├── events.ts            # event() builder + EventDescriptor
├── table.ts             # table() builder + TableDescriptor (per ADR-001)
├── agg.ts               # 53 op functions (count, sum, mean, ..., nUnique, quantile)
├── col.ts               # col(...), lit(...), expression overloading
├── errors.ts            # RegistrationError, BinaryNotFoundError
├── types.ts             # Optional, Field, type vocab
├── wire.ts              # frame codec, opcodes (CT_JSON only in v0)
├── transport.ts         # HTTP transport (TCP fast-path is v0.1+ on TS — node-only initially)
└── test/                # test fixtures (sub-export: @beava/sdk/test)
    ├── fixture.ts       # spawn embed-mode app
    ├── replay.ts        # replay events for deterministic tests
    └── assertFeaturesEq.ts
```

## BeavaApp class

```typescript
import { BeavaApp } from "@beava/sdk";
import * as bv from "@beava/sdk";

class BeavaApp {
  constructor(url?: string, options?: { timeout?: number });

  // Wire-mapped methods (each returns a Promise<T>)
  register(...descriptors: Descriptor[]): Promise<RegisterResult>;
  register(
    descriptors: Descriptor[],
    opts: { force?: boolean; dryRun?: boolean }
  ): Promise<RegisterResult>;
  push(eventName: string, fields: Record<string, unknown>): Promise<PushResult>;
  get(table: string, key: string | (string | number | boolean)[]): Promise<Record<string, unknown>>;
  batchGet(
    requests: Array<{ table: string; key: string | (string | number | boolean)[]; features?: string[] }>
  ): Promise<Record<string, unknown>[]>;
  reset(): Promise<void>;
  ping(): Promise<{ serverVersion: string; registryVersion: number }>;
  close(): Promise<void>;

  // Async-disposable for `using` / `await using` ergonomics (Node 22+ / TS 5.2+)
  [Symbol.asyncDispose](): Promise<void>;
}
```

Each public method maps 1:1 to a wire opcode:

| Method | Wire opcode | Wire spec section |
|--------|-------------|-------------------|
| `app.register(...)` | `OP_REGISTER` (`0x0001`) | [wire-spec § OP_REGISTER](../wire-spec.md#op_register-0x0001) |
| `app.push(...)` | `OP_PUSH` (`0x0010`) | [wire-spec § OP_PUSH](../wire-spec.md#op_push-0x0010) |
| `app.get(...)` | `OP_GET` (`0x0020`) | [wire-spec § OP_GET](../wire-spec.md#op_get-0x0020) |
| `app.batchGet(...)` | `OP_BATCH_GET` (`0x0024`) | [wire-spec § OP_BATCH_GET](../wire-spec.md#op_batch_get-0x0024) |
| `app.reset()` | `OP_RESET` (`0x0040`) | [wire-spec § OP_RESET](../wire-spec.md#op_reset-0x0040) |
| `app.ping()` | `OP_PING` (`0x0000`) | [wire-spec § OP_PING](../wire-spec.md#op_ping-0x0000) |
| `app.close()` | (lifecycle) | n/a — closes transport + terminates embed subprocess. |

### Constructor

`new BeavaApp(url?, options?)` — URL controls transport selection per
[shared.md § Wire transports](shared.md#wire-transports):

- `"http://..."` / `"https://..."` → HTTP/JSON transport.
- `"tcp://..."` → custom-framed TCP transport (Node.js only in v0; browser
  builds reject the `tcp://` scheme).
- `undefined` (default) → embed mode; spawns local `beava` binary on
  ephemeral ports. Node.js only — browsers cannot spawn subprocesses.

`options.timeout` is a transport-level I/O timeout in milliseconds (default
`30_000`).

**Embed mode + the explicit-disposal pattern:**

```typescript
import { BeavaApp } from "@beava/sdk";
import * as bv from "@beava/sdk";

await using app = new BeavaApp();   // Node 22+ / TS 5.2+ auto-cleanup
await app.register(Txn, UserFeatures);
await app.push("Txn", { user_id: "alice", amount: 42.50 });
console.log(await app.get("UserFeatures", "alice"));
// `app[Symbol.asyncDispose]()` runs at scope exit
```

For older runtimes:

```typescript
const app = new BeavaApp();
try {
  await app.register(Txn, UserFeatures);
  // ...
} finally {
  await app.close();
}
```

### `app.register(...descriptors, opts?)`

**Wire opcode:** `OP_REGISTER` (`0x0001`).

Validates the descriptor list locally (DAG / schema checks; zero network
I/O), topo-sorts upstreams before dependents, compiles the JSON payload,
and dispatches.

**Args:**

- `...descriptors`: variadic descriptor objects (returned by `bv.event(...)`,
  `bv.table(...)`, or chain expressions).
- `opts.force` (camelCase): if `true`, accept destructive schema changes.
  Default `false` — destructive changes throw `RegistrationError` with
  `code === "registration_conflict"`.
- `opts.dryRun` (camelCase): if `true`, return the diff without applying.
  `registryVersion` is unchanged.

**Returns:** `Promise<RegisterResult>` carrying
`{ status, registryVersion, added, removed?, changed?, diff? }`. Note the
camelCase `registryVersion` (wire JSON `registry_version` is translated by
the transport).

**Throws (rejects):** `RegistrationError` carrying `code`, `path`,
`message`, and `errors: ValidationError[]` for the full list when the
server returns multiple problems.

**Variadic vs array form:** the variadic form (`app.register(d1, d2)`) is
the syntactic sugar; the array form (`app.register([d1, d2], { force: true })`)
is required when passing options. The signature accepts both via TS
function overloads.

### `app.push(eventName, fields)`

**Wire opcode:** `OP_PUSH` (`0x0010`).

Push a single event into a registered event source.

**Args:**

- `eventName`: string matching a registered `bv.event(...)` source's name.
- `fields`: object matching the schema (field names use snake_case to
  match the registered schema; the SDK passes them through as-is).

**Returns:** `Promise<PushResult>` carrying `{ ackLsn, registryVersion }`.

**Throws:** `RegistrationError` with `code` of `schema_mismatch`,
`missing_field`, or `unknown_event`.

### `app.get(table, key)`

**Wire opcode:** `OP_GET` (`0x0020`).

Single-row feature read.

**Args:**

- `table`: name of a registered table.
- `key`: `string` (single-key) or `(string | number | boolean)[]`
  (composite-key, in declaration order).

**Returns:** `Promise<Record<string, unknown>>` — row-shape feature dict.
**Cold-start** returns `{}` — not an error.

**Throws:** `RegistrationError` with `code` of `unknown_table`,
`feature_not_in_table`, or `key_shape_mismatch`.

#### Generic typed result

```typescript
type UserTxnRow = {
  tx_count_1h: number;
  tx_sum_1h: number;
  tx_p99_1h: number;
};

const row = await app.get<UserTxnRow>("UserTxnFeatures", "alice");
// row is typed as Partial<UserTxnRow> (cold-start is {})
```

The `<T>` overload is purely a TS-level affordance; runtime behavior is
identical. `Partial<T>` is used because cold-start returns `{}` and a
specific `features` filter omits unrequested fields.

### `app.batchGet(requests)`

**Wire opcode:** `OP_BATCH_GET` (`0x0024`).

Heterogeneous batch lookup.

**Args:**

- `requests`: array of `{ table, key, features? }` objects. Different
  `table` values may appear in the same batch.

**Returns:** `Promise<Record<string, unknown>[]>` — array of row-shape
dicts in request order. Per-entry cold-start is `{}`.

**Throws:** same set as `app.get(...)`. v0 has **no partial success** —
any single bad entry rejects the entire batch.

### `app.reset()`

**Wire opcode:** `OP_RESET` (`0x0040`).

Wipe state + WAL. **Destructive — only call on a beava instance bound to
test data.**

**Returns:** `Promise<void>`.

### `app.ping()`

**Wire opcode:** `OP_PING` (`0x0000`).

Health probe + version discovery.

**Returns:** `Promise<{ serverVersion: string; registryVersion: number }>`.

### `app.close()`

Close the underlying transport (idempotent). For embed-mode apps, also
terminates the subprocess.

`Symbol.asyncDispose` calls `close()` automatically when used with the
`using` / `await using` syntax (TS 5.2+ / Node 22+).

## Builder API (event + table)

TypeScript v0 uses explicit builder calls — decorators are deferred to
v0.1+ per
[`.planning/ideas/v0.1-deferrals.md`](../../.planning/ideas/v0.1-deferrals.md).
The builder pattern is the canonical surface for TS users.

> **No decorators in v0.** TS stage-3 decorators (TS 5.0+) are deferred to
> v0.1+. The builder-pattern API documented here is the canonical surface.

### Event source

```typescript
import * as bv from "@beava/sdk";

const Txn = bv.event("Txn", {
  user_id: "str",
  amount: "f64",
  merchant: "str",
  ip: bv.optional("str"),     // nullable per shared.md § Field types
})
  .keepEventsFor("30d")
  .coldAfter("1d")
  .dedupeKey("trace_id", "5m");
```

`bv.event(name, schema)` returns an `EventDescriptor` with chainable
configuration methods:

| Method | Args | Description |
|--------|------|-------------|
| `.keepEventsFor(window)` | duration string | Event-retention TTL. |
| `.coldAfter(window)` | duration string | Per-source cold-entity TTL per V0-MEM-GOV-01. |
| `.dedupeKey(field, window)` | field name + duration | Idempotent-replay configuration. |

`bv.optional("type")` produces a nullable field type marker.

### Event derivation (function form)

```typescript
const BigTxn = bv.event("BigTxn", { upstream: Txn })
  .filter(bv.col("amount").gt(100));
```

When the schema argument is `{ upstream: <EventDescriptor> }`, the
builder creates a derivation node carrying the upstream's schema. Chain
ops compose against the upstream shape.

### Table (aggregation-output, per ADR-001)

```typescript
const UserTxnFeatures = bv.table({
  name: "UserTxnFeatures",
  key: "user_id",
  source: Txn,
  agg: {
    tx_count_1h: bv.count({ window: "1h" }),
    tx_sum_1h: bv.sum("amount", { window: "1h" }),
    tx_p99_1h: bv.quantile("amount", { q: 0.99, window: "1h" }),
    tx_unique_merchants_1h: bv.nUnique("merchant", { window: "1h" }),
  },
});

await app.register(Txn, UserTxnFeatures);
```

The `bv.table({...})` builder declares an aggregation-output table per
[ADR-001](../../.planning/decisions/ADR-001-bv-table-partial-overturn.md).
Mutation paths (`upsert` / `delete` / `retract`) are NOT exposed in v0;
tables are populated only by upstream aggregation pipelines.

**Args:**

- `name`: table name (string).
- `key`: string (single-key) or `string[]` (composite-key).
- `source`: upstream `EventDescriptor` or `EventDerivation`.
- `agg`: object mapping feature names to op descriptors (returned by
  `bv.count(...)`, `bv.sum(...)`, etc.).

The builder compiles to the same wire-level derivation node with
`output_kind: "table"` that the Python `@bv.table(key=...)` decorator
produces. Wire parity is the contract; the API surface is per-language.

## Pipeline DSL (chained methods)

```typescript
const BigTxn = Txn
  .filter(bv.col("amount").gt(100))
  .select("user_id", "amount", "merchant");

const UserBigTxn = bv.table({
  name: "UserBigTxn",
  key: "user_id",
  source: BigTxn,
  agg: {
    count_big: bv.count({ window: "1h" }),
  },
});
```

camelCase chain methods on event descriptors and event derivations:

| Method | Returns | Description |
|--------|---------|-------------|
| `events.filter(expr)` | `EventDerivation` | Keep rows where `expr` is True. |
| `events.select(...cols)` | `EventDerivation` | Keep only the named fields. |
| `events.drop(...cols)` | `EventDerivation` | Remove the named fields. |
| `events.rename(mapping)` | `EventDerivation` | Rename fields per object mapping. |
| `events.withColumns(mapping)` | `EventDerivation` | Add or overwrite derived fields. |
| `events.map(mapping)` | `EventDerivation` | Alias for `withColumns`. |
| `events.cast(typeMap)` | `EventDerivation` | Change field types. |
| `events.fillna(defaults)` | `EventDerivation` | Replace null values. |
| `events.groupBy(...keys)` | `GroupBy` | Start an aggregation pipeline. |
| `groupBy.agg(features)` | derivation | Emit named aggregation features. |

Note the camelCase: `withColumns`, `groupBy`, `batchGet` (NOT
`with_columns` / `group_by` / `batch_get`).

## Expression DSL (bv.col)

TypeScript does NOT support operator overloading, so the expression DSL
uses **method chaining**:

```typescript
bv.col("amount").gt(100)                                 // amount > 100
bv.col("amount").lt(50)                                  // amount < 50
bv.col("user_id").eq("alice")                            // user_id == 'alice'
bv.col("amount").gt(100).and(bv.col("status").eq("ok"))  // (amount > 100) and (status == 'ok')
bv.col("amount").gt(100).or(bv.col("vip"))               // ... or vip
bv.col("flag").not()                                     // (not flag)
bv.col("amount").isnull()                                // (amount == null)
bv.col("status").cast("int")                             // cast(status, int)
bv.col("a").add(bv.col("b")).mul(2)                      // (a + b) * 2
bv.lit(42)                                               // literal value
```

Method-name reference:

| Method | Wire op | Equivalent Python |
|--------|---------|-------------------|
| `.gt(other)` | `>` | `> other` |
| `.ge(other)` | `>=` | `>= other` |
| `.lt(other)` | `<` | `< other` |
| `.le(other)` | `<=` | `<= other` |
| `.eq(other)` | `==` | `== other` |
| `.ne(other)` | `!=` | `!= other` |
| `.add(other)` | `+` | `+ other` |
| `.sub(other)` | `-` | `- other` |
| `.mul(other)` | `*` | `* other` |
| `.div(other)` | `/` | `/ other` |
| `.and(other)` | `and` | `& other` |
| `.or(other)` | `or` | `\| other` |
| `.not()` | `not` | `~` |
| `.isnull()` | `(x == null)` | `.isnull()` |
| `.cast(type)` | `cast(x, type)` | `.cast(type)` |
| `.alias(name)` | column-rename | `.alias(name)` |

`bv.col(name)` → expression node. `bv.lit(value)` → literal node.

The TS spelling differs from Python (operator overloading) but produces
the **same wire-level expression string** per
[`docs/pipeline-dsl/expressions.md`](../pipeline-dsl/expressions.md) (Plan
13.0-12 — forward reference).

## bv.sum signature (Q1 Path B locked)

```typescript
function sum(
  field: string,
  opts?: { window?: string; where?: Expr }
): AggDescriptor;
```

> **Locked per Q1 Path B** ([13.0-CONTEXT.md](../../.planning/phases/13.0-design-contract-spec-docs/13.0-CONTEXT.md)).
> The TS `bv.sum(field: string, ...)` signature accepts a string column
> name **only**. Inline expressions are **FORBIDDEN**.

```typescript
// FORBIDDEN — inline expression as the field arg.
bv.sum(bv.col("flag").cast("int"), { window: "1h" });    // ✗ TypeScript compile error (string expected)

// RECOMMENDED — two-stage withColumns + sum:
const UserFraudCounts = bv.table({
  name: "UserFraudCounts",
  key: "user_id",
  source: Txn.withColumns({ flag_int: bv.col("is_fraud").cast("int") }),
  agg: { c: bv.sum("flag_int", { window: "1h" }) },
});
```

This narrowing applies symmetrically across the
[Python SDK](python.md#bvsum-signature-q1-path-b-locked) and the
[Go SDK](go.md). All three SDKs use string-only field args for `bv.sum`.

> **See:** [`docs/pipeline-dsl/compilation-rules.md`](../pipeline-dsl/compilation-rules.md)
> § Boolean-sum recipe (Plan 13.0-12 — forward reference).

## Public expression literals (`bv.lit`) — per ADR-003

Per [ADR-003](../../.planning/decisions/ADR-003-global-aggregation-and-bv-lit.md), `bv.lit(value)` is exposed as a public factory function in the `bv` namespace:

```typescript
function lit(value: number | string | boolean | null): Expr;
```

Use cases (mirror Python):

```typescript
// Constant column
events.withColumns({ source: bv.lit("web") });

// Force float division
events.withColumns({ rate: bv.col("count").div(bv.lit(60.0)) });

// Explicit literal in filter
events.filter(bv.col("amount").gt(bv.lit(100)));
```

Implementation lands in Phase 13.6. Wire-level: literals are serialized via the existing expression-string path; no wire change.

## Global aggregation — per ADR-003

Per [ADR-003](../../.planning/decisions/ADR-003-global-aggregation-and-bv-lit.md), TypeScript ships first-class **global aggregation** mirroring the Python surface. Declare a global table by calling `bv.table` without a `key` field, or use the `events.agg(...)` shorthand directly:

```typescript
const Click = bv.event({
  name: "Click",
  schema: { user_id: "string", page: "string" }
});

const TotalClicks = bv.table({
  name: "TotalClicks",
  // no `key` field → global table
  source: Click,
  agg: { total: bv.count({ window: "forever" }) }
});

const app = new BeavaApp();
await app.register(Click, TotalClicks);
await app.push("Click", { user_id: "alice", page: "/home" });
await app.push("Click", { user_id: "bob",   page: "/home" });

await app.get("TotalClicks");  // → { total: 2 }, no entity arg
```

**Three equivalent forms** (all compile to wire-level `key: []`):

```typescript
clicks.agg({ total: bv.count(...) })                      // shortest
clicks.groupBy().agg({ total: bv.count(...) })            // explicit empty groupBy
bv.table({ name: "Foo", source: c, agg: { ... } })        // no `key` field
```

**`app.get` arity contract:**

| Table type | Call shape |
|---|---|
| Per-entity | `await app.get(tableName, key)` (2 args required) |
| Global | `await app.get(tableName)` (1 arg required) |

TypeScript enforces the arity at the type level via overloaded signatures — the wrong-arity call is a compile-time error, not a runtime exception. (See `app.get` signature overload in [BeavaApp class](#app-get-table-key) — Phase 13.6 lands the overload.)

Implementation deferred to Phase 13.6 (~75 LOC: `bv.lit` factory + `events.groupBy()` empty allowance + `events.agg(...)` direct + table-builder no-`key` form + `app.get` overload).

## Operator catalog

The 53 op functions match the Python catalogue (per
[ADR-002](../../.planning/decisions/ADR-002-polars-op-rename.md) Polars
naming). One-line family table:

| Family | Ops (camelCase TS spelling) | Doc |
|--------|------|-----|
| Core (8) | count, sum, mean, min, max, var, std, ratio | [docs/operators/core/](../operators/core/) |
| Sketch (5) | nUnique, quantile, topK, bloomMember, entropy | [docs/operators/sketch/](../operators/sketch/) |
| Point/ordinal (5) | first, last, firstN, lastN, lag | [docs/operators/point-ordinal/](../operators/point-ordinal/) |
| Recency (10) | firstSeen, lastSeen, age, hasSeen, timeSince, timeSinceLastN, streak, maxStreak, negativeStreak, firstSeenInWindow | [docs/operators/recency/](../operators/recency/) |
| Decay (6) | ewma (alias ema), ewvar, ewZscore, decayedSum, decayedCount, twa | [docs/operators/decay/](../operators/decay/) |
| Velocity (9) | rateOfChange, interArrivalStats, burstCount, deltaFromPrev, trend, trendResidual, outlierCount, valueChangeCount, zScore | [docs/operators/velocity/](../operators/velocity/) |
| Bounded-buffer (7) | histogram, hourOfDayHistogram, dowHourHistogram, seasonalDeviation, eventTypeMix, mostRecentN, reservoirSample | [docs/operators/buffer-geo/](../operators/buffer-geo/) |
| Geo (4) | geoVelocity, geoDistance, geoSpread, distanceFromHome | [docs/operators/buffer-geo/](../operators/buffer-geo/) |

Total: 8+5+5+10+6+9+7+4 = **54** entries (53 unique + `ema` alias).

Each op accepts an options object — `bv.count({ window: "1h" })`,
`bv.quantile("amount", { q: 0.99, window: "1h" })`,
`bv.ewma("amount", { halfLife: "5m" })`. Required positional args
(typically `field` for non-count ops) come first; everything else is
camelCase keys in the options object.

> **No deprecation aliases in TS.** v0 is unreleased and TS users start
> with the new Polars names directly. (Python keeps `bv.avg` etc. as
> deprecation aliases for the v0.0.x line per ADR-002.)

## Errors

```typescript
class RegistrationError extends Error {
  code: string;                          // structured error code
  path: string;                          // JSON-pointer path
  errors: ValidationError[];             // all validation errors
  constructor(opts: { code: string; path?: string; message: string; errors?: ValidationError[] });
}

class BinaryNotFoundError extends Error {
  searched: string[];                    // paths attempted by binary discovery
}

interface ValidationError {
  kind: string;                          // one of 9 ValidationError kinds
  path: string;
  message: string;
}
```

The 9 valid `ValidationError.kind` values are documented in
[shared.md § ValidationError envelope](shared.md#validationerror-envelope).

## Test fixtures (`@beava/sdk/test`)

```typescript
import { describe, it, beforeEach, afterEach } from "vitest";
import { fixture, assertFeaturesEq } from "@beava/sdk/test";
import * as bv from "@beava/sdk";

describe("counts per user", () => {
  let app: BeavaApp;

  beforeEach(async () => {
    app = await fixture({ resetEach: true });
  });

  afterEach(async () => {
    await app.close();
  });

  it("counts events per user", async () => {
    const Txn = bv.event("Txn", { user_id: "str" });
    const Counts = bv.table({
      name: "Counts",
      key: "user_id",
      source: Txn,
      agg: { c: bv.count({ window: "1h" }) },
    });
    await app.register(Txn, Counts);

    await app.push("Txn", { user_id: "alice" });
    await app.push("Txn", { user_id: "alice" });

    assertFeaturesEq(await app.get("Counts", "alice"), { c: 2 });
  });
});
```

`fixture({ resetEach })`:

- Returns a `Promise<BeavaApp>` configured for embed mode.
- If `resetEach=true` (default), calls `app.reset()` between tests via
  `OP_RESET` to clear in-memory state.
- The caller is responsible for `await app.close()` in `afterEach`.

`assertFeaturesEq(got, want)` — assertion helper using vitest / jest
expectations. Tolerant of float near-equality (relative tolerance `1e-9`)
for sketch-based ops like `quantile` and `nUnique`.

## TypeScript int64 caveat

JavaScript has no native 64-bit integer. Per
[shared.md § Field types](shared.md#field-types):

| Wire | TS native | Safe range |
|------|-----------|-----------|
| `i64` | `number` | `Number.MAX_SAFE_INTEGER` (`2^53 - 1`) |

For values exceeding `Number.MAX_SAFE_INTEGER`, a future minor release
may switch the surface to `bigint`. v0 ships with `number` and a
runtime warning when an `i64` field deserialises to a value at the
edge of the safe range. Most fraud / ad-tech / behavioral workloads
fit comfortably within `2^53 - 1` (counters, amounts, IDs); operators
who need full `int64` should send IDs as strings.

## Versioning + compatibility

- **TypeScript versions:** TS 5.2+ (for `using` / `await using` and the
  `Symbol.asyncDispose` spec).
- **Node.js versions:** Node 20+ LTS. Embed mode requires `child_process`
  spawn capability (Node-only).
- **Browser support:** HTTP transport only; embed mode and `tcp://`
  scheme are Node-only.
- **API stability:** the public surface is **frozen for v0**. Adding new
  optional fields is non-breaking.

## Plan-level traceability

This document is authored by Plan 13.0-04 (Wave 1). Downstream consumers:

- **Phase 13.6** — TS SDK port reads this doc as the canonical surface;
  lands the v0-target shape (`@beava/sdk` published to npm).
- [shared.md](shared.md) + [python.md](python.md) — cross-language parity
  references.

For the full Phase 13.0 plan tree, see
[`.planning/phases/13.0-design-contract-spec-docs/13.0-PLAN.md`](../../.planning/phases/13.0-design-contract-spec-docs/13.0-PLAN.md).
