# Beava Go SDK

> **Status:** Authoritative for v0. Documents the **post-13.6 target** Go SDK
> shape — Phase 13.6 implements the port. Cross-language semantics live in
> [shared.md](shared.md); wire-level body shapes live in
> [docs/wire-spec.md](../wire-spec.md). Python is the canonical reference —
> see [python.md](python.md).
> **Last reviewed:** 2026-05-03 (Phase 13.0).

## Overview

The Beava Go SDK ships as the module `github.com/beava-io/beava-go`. It
mirrors the [Python SDK](python.md) wire contract 1:1 (per
[shared.md](shared.md)) but uses idiomatic Go conventions where the
language demands them:

- **Context-aware methods** — every wire-bound method takes
  `ctx context.Context` as its first argument. Cancellation propagates
  to the transport.
- **Explicit `error` returns** — the second return value of every wire
  method. No exceptions, no panics.
- **Functional options** — register/option flags (`force=true`,
  `dry_run=true`, `WithTimeout`, etc.) are passed via variadic
  `...Option`-style arguments rather than option-struct fields. Standard
  Go pattern; matches `grpc-go`, `cobra`, `chi`, and similar widely-used
  Go libraries.
- **Struct-tag field mapping** — event-source schemas declared as Go
  structs with `beava:"<wire_field_name>"` tags.
- **53 op functions** in PascalCase per Go convention (`beava.Count`,
  `beava.Sum`, `beava.Mean`, `beava.NUnique`, `beava.Quantile`) per
  [ADR-002](../../.planning/decisions/ADR-002-polars-op-rename.md).

> **Module:** `github.com/beava-io/beava-go`. Install via
> `go get github.com/beava-io/beava-go`. Import as
> `import beava "github.com/beava-io/beava-go"`. The SDK targets Go
> 1.22+ (modern context handling, slog).

## Module structure

```
github.com/beava-io/beava-go/
├── beava.go             # public types: App, EventDescriptor, TableDescriptor, FeatureResult
├── app.go               # App struct + methods
├── events.go            # event builder + EventDescriptor + struct-tag parser
├── table.go             # table builder + TableDescriptor (per ADR-001)
├── agg.go               # 53 op functions
├── col.go               # Col expression builder
├── errors.go            # RegistrationError, BinaryNotFoundError
├── wire.go              # frame codec, opcodes (CT_JSON only in v0)
├── transport.go         # HTTP / TCP / Embed transports + URL-scheme dispatch
├── beavatest/           # test helpers
│   ├── fixture.go       # spawn embed-mode app
│   └── assert.go        # AssertFeaturesEq
└── go.mod
```

## App struct

```go
package beava

import "context"

type App struct {
    // unexported fields
}

func NewApp(ctx context.Context, url string, opts ...AppOption) (*App, error)

// Wire-mapped methods
func (a *App) Register(ctx context.Context, descriptors []Descriptor, opts ...RegisterOption) (*RegisterResult, error)
func (a *App) Push(ctx context.Context, eventName string, fields map[string]any) (*PushResult, error)
func (a *App) Get(ctx context.Context, table string, key any) (FeatureResult, error)
func (a *App) BatchGet(ctx context.Context, requests []GetRequest) ([]FeatureResult, error)
func (a *App) Reset(ctx context.Context) error
func (a *App) Ping(ctx context.Context) (*PingResult, error)
func (a *App) Close(ctx context.Context) error
```

Each public method maps 1:1 to a wire opcode:

| Method | Wire opcode | Wire spec section |
|--------|-------------|-------------------|
| `App.Register(...)` | `OP_REGISTER` (`0x0001`) | [wire-spec § OP_REGISTER](../wire-spec.md#op_register-0x0001) |
| `App.Push(...)` | `OP_PUSH` (`0x0010`) | [wire-spec § OP_PUSH](../wire-spec.md#op_push-0x0010) |
| `App.Get(...)` | `OP_GET` (`0x0020`) | [wire-spec § OP_GET](../wire-spec.md#op_get-0x0020) |
| `App.BatchGet(...)` | `OP_BATCH_GET` (`0x0024`) | [wire-spec § OP_BATCH_GET](../wire-spec.md#op_batch_get-0x0024) |
| `App.Reset(...)` | `OP_RESET` (`0x0040`) | [wire-spec § OP_RESET](../wire-spec.md#op_reset-0x0040) |
| `App.Ping(...)` | `OP_PING` (`0x0000`) | [wire-spec § OP_PING](../wire-spec.md#op_ping-0x0000) |
| `App.Close(...)` | (lifecycle) | n/a — closes transport + terminates embed subprocess. |

### `NewApp(ctx, url, opts...)`

Constructor. URL controls transport selection per
[shared.md § Wire transports](shared.md#wire-transports):

- `"http://..."` / `"https://..."` → HTTP/JSON transport.
- `"tcp://..."` → custom-framed TCP transport.
- `""` (empty string) → embed mode; spawns local `beava` binary on
  ephemeral ports.

The `ctx` here governs **construction** (binary discovery, embed-mode
startup, initial connection); subsequent wire calls take their own
context.

**Functional options:**

```go
type AppOption func(*appConfig)

func WithTimeout(d time.Duration) AppOption
func WithBinaryPath(p string) AppOption     // override embed-mode binary discovery
```

**Returns:** `(*App, error)`. Errors during embed-mode startup
(`BinaryNotFoundError`, transport connect failure) are surfaced here.

**Lifecycle pattern:**

```go
ctx := context.Background()
app, err := beava.NewApp(ctx, "")
if err != nil {
    log.Fatal(err)
}
defer app.Close(ctx)

// ...
```

`Close(ctx)` is idempotent. For embed-mode apps, `Close` also terminates
the subprocess (SIGTERM, then SIGKILL after 5 seconds).

### `App.Register(ctx, descriptors, opts...)`

**Wire opcode:** `OP_REGISTER` (`0x0001`).

Validates the descriptor list locally, topo-sorts, and dispatches.

**Args:**

- `ctx`: standard `context.Context` for cancellation / timeouts.
- `descriptors`: slice of `Descriptor` interface values (returned by
  `beava.NewEvent[T](...)`, `beava.NewTable(...)`, or chain expressions).
- `opts ...RegisterOption`: functional options for `force` and `dry_run`.

**Functional options:**

```go
func WithForce() RegisterOption
func WithDryRun() RegisterOption
```

Usage:

```go
result, err := app.Register(ctx, descriptors, beava.WithForce(), beava.WithDryRun())
```

**Returns:** `(*RegisterResult, error)` carrying
`{Status, RegistryVersion, Added, Removed, Changed, Diff}` (all
PascalCase Go field names; the transport translates wire JSON
`snake_case` to PascalCase via field tags).

```go
type RegisterResult struct {
    Status          string   `json:"status"`
    RegistryVersion int64    `json:"registry_version"`
    Added           []string `json:"added,omitempty"`
    Removed         []string `json:"removed,omitempty"`
    Changed         []string `json:"changed,omitempty"`
}
```

**Errors:** `*RegistrationError` carrying `Code`, `Path`, `Message`,
`Errors []ValidationError`.

### `App.Push(ctx, eventName, fields)`

**Wire opcode:** `OP_PUSH` (`0x0010`).

**Args:**

- `ctx`: context.
- `eventName`: string matching a registered event source.
- `fields`: `map[string]any`. Field types must match the registered
  schema; the SDK serialises into the wire-level JSON form.

**Returns:** `(*PushResult, error)` carrying `AckLsn` and
`RegistryVersion`.

### `App.Get(ctx, table, key)`

**Wire opcode:** `OP_GET` (`0x0020`).

**Args:**

- `ctx`: context.
- `table`: name of a registered table.
- `key`: `any` — string for single-key tables; `[]any` containing
  `[string | int64 | float64 | bool]` items for composite-key tables.

**Returns:** `(FeatureResult, error)` where
`FeatureResult` is `map[string]any`. **Cold-start** returns an empty map
(`map[string]any{}`) — not an error.

```go
row, err := app.Get(ctx, "UserTxnFeatures", "alice")
// row is map[string]any{"tx_count_1h": float64(7), "tx_sum_1h": 312.45, ...}
```

For typed access, the user dereferences the map keys with type
assertions. v0.1+ may add a generic codegen path that produces
strongly-typed result structs.

### `App.BatchGet(ctx, requests)`

**Wire opcode:** `OP_BATCH_GET` (`0x0024`).

```go
type GetRequest struct {
    Table    string   `json:"table"`
    Key      any      `json:"key"`
    Features []string `json:"features,omitempty"`
}

func (a *App) BatchGet(ctx context.Context, requests []GetRequest) ([]FeatureResult, error)
```

**Returns:** `([]FeatureResult, error)`. Per-entry cold-start is
`map[string]any{}`. v0 has **no partial success** — any single bad
entry returns the whole frame as an error.

### `App.Reset(ctx)`

**Wire opcode:** `OP_RESET` (`0x0040`).

Wipe state + WAL. **Destructive — only call on a beava instance bound to
test data.**

**Returns:** `error`.

### `App.Ping(ctx)`

**Wire opcode:** `OP_PING` (`0x0000`).

```go
type PingResult struct {
    ServerVersion   string `json:"server_version"`
    RegistryVersion int64  `json:"registry_version"`
}
```

### `App.Close(ctx)`

Close transport (idempotent). For embed-mode apps, terminates the
subprocess.

## Builder API (event + table)

### Event source

```go
type Txn struct {
    UserID   string  `beava:"user_id"`
    Amount   float64 `beava:"amount"`
    Merchant string  `beava:"merchant"`
    IP       *string `beava:"ip"`              // nullable per shared.md § Field types
}

txnDesc := beava.NewEvent[Txn]("Txn",
    beava.KeepEventsFor("30d"),
    beava.ColdAfter("1d"),
    beava.DedupeKey("trace_id", "5m"),
)
```

`beava.NewEvent[T]` is a generic constructor that reflects on `T` to
extract the wire schema. The `beava:"<wire_field_name>"` struct tag
overrides the default `snake_case`-of-`PascalCase` mapping; without the
tag, `UserID` would default to `user_i_d` (Go's stdlib snake_case is
imperfect for all-caps acronyms), so the explicit tag is recommended for
fields with multi-letter abbreviations.

Pointer types (`*string`, `*int64`) declare nullable fields per
[shared.md § Field types](shared.md#field-types) — `Optional[T]`
semantics.

**Functional options:**

| Option | Description |
|--------|-------------|
| `beava.KeepEventsFor(window)` | Event-retention TTL (duration string). |
| `beava.ColdAfter(window)` | Per-source cold-entity TTL per V0-MEM-GOV-01. |
| `beava.DedupeKey(field, window)` | Idempotent-replay configuration. |

Reflection happens at descriptor-construction time, so any unsupported
field type produces an error from `NewEvent` (returned via the chain;
fluent-style `descriptor.Err()` accessor or `panic` — the choice is left
to the 13.6 implementation).

### Event derivation (function form)

```go
bigTxn := txnDesc.Filter(beava.Col("amount").Gt(100))
                 .Select("user_id", "amount", "merchant")
```

Method-receiver chains compose against the upstream descriptor.

### Table (aggregation-output, per ADR-001)

```go
userFeatures := beava.NewTable("UserTxnFeatures",
    beava.WithKey("user_id"),
    beava.WithUpstream(txnDesc),
    beava.WithAgg(map[string]beava.AggOp{
        "tx_count_1h":              beava.Count(beava.Window("1h")),
        "tx_sum_1h":                beava.Sum("amount", beava.Window("1h")),
        "tx_p99_1h":                beava.Quantile("amount", 0.99, beava.Window("1h")),
        "tx_unique_merchants_1h":   beava.NUnique("merchant", beava.Window("1h")),
    }),
)

_, err := app.Register(ctx, []beava.Descriptor{txnDesc, userFeatures})
```

`beava.NewTable(name, opts...)` returns a `TableDescriptor` populated
only by upstream aggregation derivations per
[ADR-001](../../.planning/decisions/ADR-001-bv-table-partial-overturn.md).
Mutation paths (`Upsert` / `Delete` / `Retract`) are NOT exposed in v0.

**Functional options:**

| Option | Description |
|--------|-------------|
| `beava.WithKey(string \| []string)` | Single-key or composite-key. |
| `beava.WithUpstream(Descriptor)` | Upstream event source or derivation. |
| `beava.WithAgg(map[string]AggOp)` | Named aggregation features. |

The composite-key form passes a slice:
`beava.WithKey([]string{"user_id", "device_id"})`.

## Pipeline DSL

Method-chained API on `EventDescriptor` and `EventDerivation`:

```go
bigTxn := txnDesc.Filter(beava.Col("amount").Gt(100))
                 .Select("user_id", "amount", "merchant")

userBig := bigTxn.GroupBy("user_id").Agg(map[string]beava.AggOp{
    "count_big": beava.Count(beava.Window("1h")),
})
```

PascalCase chain methods:

| Method | Returns | Description |
|--------|---------|-------------|
| `events.Filter(expr)` | `EventDerivation` | Keep rows where `expr` is True. |
| `events.Select(cols ...string)` | `EventDerivation` | Keep only the named fields. |
| `events.Drop(cols ...string)` | `EventDerivation` | Remove the named fields. |
| `events.Rename(mapping map[string]string)` | `EventDerivation` | Rename fields. |
| `events.WithColumns(mapping map[string]Expr)` | `EventDerivation` | Add or overwrite derived fields. |
| `events.Map(mapping map[string]Expr)` | `EventDerivation` | Alias for `WithColumns`. |
| `events.Cast(typeMap map[string]string)` | `EventDerivation` | Change field types. |
| `events.Fillna(defaults map[string]any)` | `EventDerivation` | Replace null values. |
| `events.GroupBy(keys ...string)` | `*GroupBy` | Start an aggregation pipeline. |
| `groupBy.Agg(features map[string]AggOp)` | `Descriptor` | Emit named aggregation features. |

## Expression DSL

```go
beava.Col("amount").Gt(100)                                 // amount > 100
beava.Col("user_id").Eq("alice")                            // user_id == 'alice'
beava.Col("amount").Gt(100).And(beava.Col("status").Eq("ok"))
beava.Col("amount").Gt(100).Or(beava.Col("vip"))
beava.Col("flag").Not()
beava.Col("amount").IsNull()
beava.Col("status").Cast("int")
beava.Col("a").Add(beava.Col("b")).Mul(beava.Lit(2))
beava.Lit(42)
```

PascalCase method names:

| Method | Wire op | Equivalent Python |
|--------|---------|-------------------|
| `.Gt(other)` | `>` | `> other` |
| `.Ge(other)` | `>=` | `>= other` |
| `.Lt(other)` | `<` | `< other` |
| `.Le(other)` | `<=` | `<= other` |
| `.Eq(other)` | `==` | `== other` |
| `.Ne(other)` | `!=` | `!= other` |
| `.Add(other)` | `+` | `+ other` |
| `.Sub(other)` | `-` | `- other` |
| `.Mul(other)` | `*` | `* other` |
| `.Div(other)` | `/` | `/ other` |
| `.And(other)` | `and` | `& other` |
| `.Or(other)` | `or` | `\| other` |
| `.Not()` | `not` | `~` |
| `.IsNull()` | `(x == null)` | `.isnull()` |
| `.Cast(type)` | `cast(x, type)` | `.cast(type)` |
| `.Alias(name)` | column-rename | `.alias(name)` |

`beava.Col(name)` returns a `*Col` expression node;
`beava.Lit(value)` returns a literal node. All chain methods compile to
the same wire-level expression string per
[`docs/pipeline-dsl/expressions.md`](../pipeline-dsl/expressions.md)
(Plan 13.0-12 — forward reference).

## bv.sum signature (Q1 Path B locked)

```go
func Sum(field string, opts ...SumOption) AggOp

func Window(s string) SumOption       // duration-string window
func Where(expr Expr) SumOption       // optional filter expression
```

> **Locked per Q1 Path B** ([13.0-CONTEXT.md](../../.planning/phases/13.0-design-contract-spec-docs/13.0-CONTEXT.md)).
> The Go `beava.Sum(field string, ...)` signature accepts a string column
> name **only**. Inline expressions are **FORBIDDEN**.

```go
// FORBIDDEN — Sum's first param is string, not Expr; this is a compile error.
beava.Sum(beava.Col("flag").Cast("int"), beava.Window("1h"))   // ✗ does not compile

// RECOMMENDED — two-stage WithColumns + Sum:
userFraudCounts := beava.NewTable("UserFraudCounts",
    beava.WithKey("user_id"),
    beava.WithUpstream(txnDesc.WithColumns(map[string]beava.Expr{
        "flag_int": beava.Col("is_fraud").Cast("int"),
    })),
    beava.WithAgg(map[string]beava.AggOp{
        "c": beava.Sum("flag_int", beava.Window("1h")),
    }),
)
```

This narrowing applies symmetrically across the
[Python SDK](python.md#bvsum-signature-q1-path-b-locked) and the
[TypeScript SDK](typescript.md#bvsum-signature-q1-path-b-locked). All
three SDKs use string-only field args for `bv.sum` / `beava.Sum`.

> **See:** [`docs/pipeline-dsl/compilation-rules.md`](../pipeline-dsl/compilation-rules.md)
> § Boolean-sum recipe (Plan 13.0-12 — forward reference).

## Operator catalog

The 53 op functions match the Python catalogue in PascalCase per Go
convention (per [ADR-002](../../.planning/decisions/ADR-002-polars-op-rename.md)
Polars naming):

| Family | Ops (Go PascalCase) | Doc |
|--------|------|-----|
| Core (8) | Count, Sum, Mean, Min, Max, Var, Std, Ratio | [docs/operators/core/](../operators/core/) |
| Sketch (5) | NUnique, Quantile, TopK, BloomMember, Entropy | [docs/operators/sketch/](../operators/sketch/) |
| Point/ordinal (5) | First, Last, FirstN, LastN, Lag | [docs/operators/point-ordinal/](../operators/point-ordinal/) |
| Recency (10) | FirstSeen, LastSeen, Age, HasSeen, TimeSince, TimeSinceLastN, Streak, MaxStreak, NegativeStreak, FirstSeenInWindow | [docs/operators/recency/](../operators/recency/) |
| Decay (6) | Ewma (alias Ema), Ewvar, EwZscore, DecayedSum, DecayedCount, Twa | [docs/operators/decay/](../operators/decay/) |
| Velocity (9) | RateOfChange, InterArrivalStats, BurstCount, DeltaFromPrev, Trend, TrendResidual, OutlierCount, ValueChangeCount, ZScore | [docs/operators/velocity/](../operators/velocity/) |
| Bounded-buffer (7) | Histogram, HourOfDayHistogram, DowHourHistogram, SeasonalDeviation, EventTypeMix, MostRecentN, ReservoirSample | [docs/operators/buffer-geo/](../operators/buffer-geo/) |
| Geo (4) | GeoVelocity, GeoDistance, GeoSpread, DistanceFromHome | [docs/operators/buffer-geo/](../operators/buffer-geo/) |

Total: 8+5+5+10+6+9+7+4 = **54** entries (53 unique + `Ema` alias).

Each op accepts variadic functional options for kwargs:

- `beava.Window(string)` — duration-string window for windowed ops.
- `beava.Where(Expr)` — optional filter expression.
- `beava.HalfLife(string)` — for decay ops.
- `beava.SubWindow(string)` — for `BurstCount`.
- `beava.Sigma(float64)` — for `OutlierCount`.

Required positional args (typically `field` and op-specific params like
`q` for `Quantile`) come first; everything else is functional options.

> **No deprecation aliases in Go.** v0 is unreleased and Go users start
> with the new Polars names directly.

## Errors

```go
type RegistrationError struct {
    Code    string
    Path    string
    Message string
    Errors  []ValidationError
}

func (e *RegistrationError) Error() string {
    return fmt.Sprintf("[%s] %s: %s", e.Code, e.Path, e.Message)
}

type BinaryNotFoundError struct {
    Searched []string
}

func (e *BinaryNotFoundError) Error() string {
    return fmt.Sprintf("beava binary not found in: %v", e.Searched)
}

type ValidationError struct {
    Kind    string
    Path    string
    Message string
}
```

The 9 valid `ValidationError.Kind` values are documented in
[shared.md § ValidationError envelope](shared.md#validationerror-envelope).

Standard Go error-handling idioms apply:

```go
result, err := app.Register(ctx, descriptors)
if err != nil {
    var regErr *beava.RegistrationError
    if errors.As(err, &regErr) {
        // structured handling
        log.Printf("registration failed: code=%s path=%s", regErr.Code, regErr.Path)
        for _, ve := range regErr.Errors {
            log.Printf("  - %s", ve)
        }
    }
    return err
}
```

## Test helpers (`beavatest`)

```go
package mypackage_test

import (
    "context"
    "testing"

    beava "github.com/beava-io/beava-go"
    "github.com/beava-io/beava-go/beavatest"
)

type Txn struct {
    UserID string `beava:"user_id"`
}

func TestCountPerUser(t *testing.T) {
    ctx := context.Background()
    app := beavatest.Fixture(t, beavatest.WithResetEach(true))

    txnDesc := beava.NewEvent[Txn]("Txn")
    counts := beava.NewTable("Counts",
        beava.WithKey("user_id"),
        beava.WithUpstream(txnDesc),
        beava.WithAgg(map[string]beava.AggOp{
            "c": beava.Count(beava.Window("1h")),
        }),
    )

    if _, err := app.Register(ctx, []beava.Descriptor{txnDesc, counts}); err != nil {
        t.Fatal(err)
    }

    if _, err := app.Push(ctx, "Txn", map[string]any{"user_id": "alice"}); err != nil {
        t.Fatal(err)
    }

    row, err := app.Get(ctx, "Counts", "alice")
    if err != nil {
        t.Fatal(err)
    }

    beavatest.AssertFeaturesEq(t, row, beava.FeatureResult{"c": int64(1)})
}
```

`beavatest.Fixture(t, opts...)`:

- Returns a `*beava.App` configured for embed mode.
- Registers `t.Cleanup(func() { app.Close(ctx) })` so the subprocess
  terminates when the test ends.
- `beavatest.WithResetEach(true)` — call `app.Reset(ctx)` between tests
  (standard Go testing pattern uses subtests + cleanup; this option
  pairs with table-driven tests).

`beavatest.AssertFeaturesEq(t, got, want)` — assertion helper using
`testing.T.Errorf`. Tolerant of float near-equality (relative tolerance
`1e-9`) for sketch-based ops like `Quantile` and `NUnique`.

## Versioning + compatibility

- **Go versions:** Go 1.22+ (generics + modern context handling).
- **Wire compatibility:** v0 SDK targets v0 server.
- **API stability:** the public surface is **frozen for v0**. Adding new
  optional fields / functional options is non-breaking.

## Plan-level traceability

This document is authored by Plan 13.0-04 (Wave 1). Downstream consumers:

- **Phase 13.6** — Go SDK port reads this doc as the canonical surface;
  lands the v0-target shape (`github.com/beava-io/beava-go` v0.0.0).
- [shared.md](shared.md) + [python.md](python.md) — cross-language parity
  references.

For the full Phase 13.0 plan tree, see
[`.planning/phases/13.0-design-contract-spec-docs/13.0-PLAN.md`](../../.planning/phases/13.0-design-contract-spec-docs/13.0-PLAN.md).
