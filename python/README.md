# beava — Python SDK

Real-time feature server, Python SDK.

## Install

```
pip install beava
```

## Quickstart — HTTP

```python
import beava as bv

@bv.event
class Transaction:
    user_id: str
    amount: float
    event_time: int

@bv.table(key="user_id")
class UserProfile:
    user_id: str
    balance: float

with bv.App("http://localhost:7379") as app:
    resp = app.register(Transaction, UserProfile)
    print("registry_version:", resp["registry_version"])
```

## Quickstart — TCP (faster path)

```python
with bv.App("tcp://localhost:7380") as app:
    resp = app.register(Transaction, UserProfile)
    print("registry_version:", resp["registry_version"])
```

## Quickstart — Embed mode (no separate server)

```python
with bv.App() as app:
    resp = app.register(Transaction, UserProfile)
    print("registry_version:", resp["registry_version"])
```

Embed mode auto-spawns a local `beava` binary on ephemeral ports. Requires
the binary to be on `PATH` (`brew install beava` or `docker pull beava/beava`)
or set `BEAVA_BINARY=/path/to/beava`.

## Validate without network I/O

```python
errs = bv.App("http://localhost:7379").validate(Transaction, UserProfile)
assert errs == []
```

`validate` returns `list[beava.ValidationError]` — empty on success.

## Expression DSL

```python
expr = bv.col("amount") > 100
print(expr.to_expr_string())  # "(amount > 100)"

compound = (bv.col("amount") > 0) & (bv.col("balance") < 1000)
print(compound.to_expr_string())  # "((amount > 0) and (balance < 1000))"
```

Expressions are compiled at register time (Phase 4+). Phase 3 accepts them
in derivation definitions; server-side evaluation ships in Phase 4.
