# Beava Quickstart

> `pip install beava` -> first feature in 60 seconds.

Beava is a real-time feature server. You declare aggregations in plain Python,
push events over HTTP, and query computed features by entity key --
sub-millisecond -- with `curl` alone or any HTTP client.

## Install

```bash
pip install beava
```

The pip package ships the Python SDK. The `beava` server binary is bundled and
discovered automatically by `bv.App()` (no separate install). For production
deployment use the Docker image (Phase 13.8 release).

## First feature in 60 seconds

```python
import beava as bv

# Define an event source.
@bv.event
class Impression:
    campaign_id: str
    bid: float

# Define an aggregation table.
@bv.table(key="campaign_id")
def CampaignStats(imp: Impression):
    return imp.group_by("campaign_id").agg(
        impressions_1h=bv.count(window="1h"),
        bid_sum_1h=bv.sum("bid", window="1h"),
        bid_mean_1h=bv.mean("bid", window="1h"),
    )

# Run an embedded local server (no separate install needed).
with bv.App() as app:
    app.register(Impression, CampaignStats)

    # Push events.
    for camp_id, bid in [("c1", 0.50), ("c1", 0.75), ("c2", 0.40)]:
        app.push("Impression", {"campaign_id": camp_id, "bid": bid})

    # Query computed features.
    print(app.get("CampaignStats", "c1"))
    # -> {"impressions_1h": 2, "bid_sum_1h": 1.25, "bid_mean_1h": 0.625}
```

That's it. No external storage, no separate server install, no SDK ceremony.
Beava's [embed mode](./concepts/embed-mode.md) spawns a local `beava` binary
on ephemeral ports -- the same binary you'd run in production for HTTP/TCP
feature serving.

## bv.demo()

For a self-contained tour with realistic-shape data:

```python
import beava as bv

bv.demo("adtech")     # ad-impression / click-rate aggregations
bv.demo("fraud")      # high-cardinality velocity + sketch
bv.demo("ecommerce")  # purchase / basket aggregations
```

Each demo registers descriptors, pushes ~10 events, and queries the resulting
features. See
[examples/python/adtech.py](../examples/python/adtech.py),
[examples/python/fraud.py](../examples/python/fraud.py), and
[examples/python/ecommerce.py](../examples/python/ecommerce.py)
for the full source. Equivalent demos exist for
[TypeScript](../examples/typescript/) and [Go](../examples/go/) -- see
[docs/sdk-api/typescript.md](./sdk-api/typescript.md) +
[docs/sdk-api/go.md](./sdk-api/go.md).

## Next steps

- **API reference:** [docs/sdk-api/python.md](./sdk-api/python.md) -- full
  Python SDK surface (App, decorators, expressions, op helpers)
- **Operator catalog:** [docs/operators/index.md](./operators/index.md) --
  all 54 op pages (`count`, `sum`, `mean`, `n_unique`, `quantile`, `ewma`,
  ...)
- **Wire contract:** [docs/wire-spec.md](./wire-spec.md) -- frame format +
  JSON Schema 2020-12 contracts (for porting to other languages)
- **Pipeline DSL:** [docs/pipeline-dsl/overview.md](./pipeline-dsl/overview.md)
  -- `@bv.event`, `@bv.table`, chain methods, expressions
- **Architecture:** [docs/architecture/](./architecture/) -- single-thread
  apply + mio data plane + WAL/snapshot durability + memory budget

For production deployment + scaling guidance see the docs site (Phase 13.7).
