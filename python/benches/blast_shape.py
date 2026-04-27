"""Four-shape body-pool builder for the Phase 19 Python bench harness.

Mirrors ``crates/beava-bench/src/blast_shape.rs`` semantics:

  - ``fixed`` — one body, reused N times (cache-warm marketing peak)
  - ``uniform`` — ``user_id`` rolls evenly over ``cardinality`` keys (cache-pessimistic)
  - ``zipfian`` — Zipfian distribution over rank with alpha=1.0 default (realistic fraud)
  - ``mixed`` — round-robin over ``mixed_event_count`` distinct event names

REVISED D-09 (commit 88f1161) per CONTEXT.md: the pool stores Python ``dict`` bodies
(plus an event-name string for ``mixed``), NOT pre-encoded frame bytes. Encoding
happens INSIDE ``transport.send_push()`` (TCP) or ``transport._client.post()`` (HTTP)
per call — this is the SDK overhead the bench is honestly measuring; matches what
a real ``app.push()`` user would see when SDK-APP-04 lands.

Raw ``socket.create_connection + sock.sendall(pre_encoded_bytes)`` is FORBIDDEN.
"""

from __future__ import annotations

import random
from dataclasses import dataclass, field
from typing import Any, Literal

BlastShapeName = Literal["fixed", "uniform", "zipfian", "mixed"]
WireFormatName = Literal["json", "msgpack"]
TransportName = Literal["http", "tcp"]


@dataclass
class PoolConfig:
    """Configuration for a per-worker pool of event bodies."""

    shape: BlastShapeName
    wire_format: WireFormatName
    transport: TransportName
    cardinality: int
    zipf_alpha: float
    mixed_event_count: int
    seed: int
    pipeline_event_name: str
    pipeline_key_field: str
    pipeline_extra_fields: dict[str, str]
    # For mixed shape: synthesized event-name list. If pipeline only registers
    # one event the harness falls back to suffixed clones of the primary name.
    mixed_event_names: list[str] = field(default_factory=list)


@dataclass
class PoolItem:
    """One event in the pool: which event name to push + the body dict."""

    event_name: str
    body: dict[str, Any]


def _build_body(
    cfg: PoolConfig,
    key_idx: int,
    seq: int,
    rng: random.Random,
) -> dict[str, Any]:
    """Build one event body with key + event_time + scalar extra fields."""
    body: dict[str, Any] = {
        cfg.pipeline_key_field: f"k{key_idx:08d}",
        "event_time": 1_000_000 + seq,
    }
    for fld, ty in cfg.pipeline_extra_fields.items():
        if ty == "f64":
            body[fld] = round(rng.uniform(0.0, 1000.0), 4)
        elif ty == "i64":
            body[fld] = rng.randint(0, 999_999)
        elif ty == "str":
            body[fld] = f"s{rng.randint(0, 999)}"
        else:
            # Unknown type: stash 0 so the server's schema check still passes for
            # numeric primitives. Anything truly exotic should be added explicitly.
            body[fld] = 0
    return body


def _zipf_zeta(n: int, alpha: float) -> float:
    """Zeta value used by the rank-based Zipfian sampler."""
    total: float = 0.0
    for i in range(1, n + 1):
        total += 1.0 / (i**alpha)
    return total


class _ZipfianSampler:
    """Rank-based Zipfian sampler.

    Mirrors the recipe in ``rand_distr::Zipf`` semantics for alpha != 1: classic
    Gray et al. rejection sampler over rank ``r in [0, k)`` with
    ``P(r) ∝ 1/(r+1)^alpha``. Pure Python; not tuned for absolute speed, but
    fast enough for pool sizes up to a few million.
    """

    def __init__(self, k: int, alpha: float, seed: int) -> None:
        if k <= 0:
            raise ValueError(f"k must be > 0, got {k}")
        if alpha <= 0:
            raise ValueError(f"alpha must be > 0, got {alpha}")
        if alpha == 1.0:
            # alpha=1 hits a singularity in the eta formula below. Use a slightly
            # offset alpha to keep the closed-form sampler well-defined; the
            # statistical difference at alpha=1.0 vs 1.0001 is negligible for
            # bench purposes and matches Gray et al.'s original published recipe.
            alpha = 1.0001
        self.k = k
        self.alpha = alpha
        self.rng = random.Random(seed)
        self.zetan = _zipf_zeta(k, alpha)
        zeta2 = _zipf_zeta(2, alpha)
        # eta as defined in Gray et al. for the rank-based rejection sampler.
        self.eta = (
            (1.0 - (2.0 / k) ** (1.0 - alpha))
            / (1.0 - zeta2 / self.zetan)
        )

    def sample(self) -> int:
        """Return a rank in ``[0, k)`` distributed Zipf(alpha)."""
        u = self.rng.random()
        uz = u * self.zetan
        if uz < 1.0:
            return 0
        if uz < 1.0 + 0.5**self.alpha:
            return 1
        v = self.rng.random()
        rank = int(
            self.k * ((self.eta * v - self.eta + 1.0) ** (1.0 / (1.0 - self.alpha)))
        )
        return min(max(rank, 0), self.k - 1)


def build_pool(cfg: PoolConfig, n: int) -> list[PoolItem]:
    """Build N pool items matching the requested shape.

    REVISED D-09: each item is ``PoolItem(event_name, body_dict)``. Encoding
    happens inside the transport.send_push / transport._client.post call site
    in blast.py. Raw frame bytes are NOT pre-built here.

    Args:
        cfg: PoolConfig with shape + wire_format + transport + RNG seed.
        n: Number of items to build.

    Returns:
        list[PoolItem] of length ``n``.
    """
    if n <= 0:
        return []

    rng = random.Random(cfg.seed)
    pool: list[PoolItem] = []

    if cfg.shape == "fixed":
        # ONE pre-built body, reused N times. Cache-warm marketing peak.
        body = _build_body(cfg, key_idx=0, seq=0, rng=rng)
        item = PoolItem(event_name=cfg.pipeline_event_name, body=body)
        return [item] * n

    if cfg.shape == "uniform":
        # user_id rolls evenly over `cardinality` keys.
        cardinality = max(cfg.cardinality, 1)
        for seq in range(n):
            key_idx = rng.randrange(0, cardinality)
            body = _build_body(cfg, key_idx, seq, rng)
            pool.append(PoolItem(event_name=cfg.pipeline_event_name, body=body))
        return pool

    if cfg.shape == "zipfian":
        cardinality = max(cfg.cardinality, 1)
        sampler = _ZipfianSampler(cardinality, cfg.zipf_alpha, cfg.seed ^ 0xDEAD)
        for seq in range(n):
            key_idx = sampler.sample()
            body = _build_body(cfg, key_idx, seq, rng)
            pool.append(PoolItem(event_name=cfg.pipeline_event_name, body=body))
        return pool

    if cfg.shape == "mixed":
        m = max(cfg.mixed_event_count, 1)
        if len(cfg.mixed_event_names) >= m:
            names = list(cfg.mixed_event_names[:m])
        else:
            base = list(cfg.mixed_event_names) if cfg.mixed_event_names else [
                cfg.pipeline_event_name
            ]
            # Pad with suffixed clones of the primary name. A single-event
            # config is the common case; the suffixed names will not exist on
            # the server, but the harness itself is exercised.
            while len(base) < m:
                base.append(f"{cfg.pipeline_event_name}_{len(base)}")
            names = base[:m]

        cardinality = max(cfg.cardinality, 1)
        for seq in range(n):
            ev = names[seq % m]
            key_idx = rng.randrange(0, cardinality)
            body = _build_body(cfg, key_idx, seq, rng)
            pool.append(PoolItem(event_name=ev, body=body))
        return pool

    raise ValueError(f"unknown shape: {cfg.shape!r}")
