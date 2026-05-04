"""53 aggregation op helpers — Phase 13.5 Plan 04.

Each helper returns an :class:`AggDescriptor` with ``to_dict()`` emitting
wire-shape JSON consumed by ``crates/beava-core`` register-time compilation.

Names follow Polars conventions per ADR-002 (``mean`` / ``var`` / ``std`` /
``n_unique`` / ``quantile``). The old SQL-prose names (``avg`` / ``variance``
/ ``stddev`` / ``count_distinct`` / ``percentile``) ship as deprecation
aliases that emit ``DeprecationWarning`` referencing the new name.

Q1 Path B is enforced on every field-bearing op: passing an ``_Expr``
instance (e.g. ``bv.col("x") * 2``) where a column-name string is required
raises ``RegistrationError(code='schema_mismatch')`` at decorator-time. Use
a two-stage chain instead::

    events.with_columns(doubled=bv.col("x") * 2).group_by(...).agg(
        s=bv.sum("doubled", window="1h"),
    )

Family breakdown (53 unique + 1 ``ema`` alias):

- core (8): ``count`` ``sum`` ``mean`` ``min`` ``max`` ``var`` ``std`` ``ratio``
- sketch (5): ``n_unique`` ``quantile`` ``top_k`` ``bloom_member`` ``entropy``
- point/ordinal (5): ``first`` ``last`` ``first_n`` ``last_n`` ``lag``
- recency (10): ``first_seen`` ``last_seen`` ``age`` ``has_seen`` ``time_since``
  ``time_since_last_n`` ``streak`` ``max_streak`` ``negative_streak``
  ``first_seen_in_window``
- decay (6 + ``ema`` alias): ``ewma`` ``ewvar`` ``ew_zscore`` ``decayed_sum``
  ``decayed_count`` ``twa``
- velocity (9): ``rate_of_change`` ``inter_arrival_stats`` ``burst_count``
  ``delta_from_prev`` ``trend`` ``trend_residual`` ``outlier_count``
  ``value_change_count`` ``z_score``
- bounded buffers (7): ``histogram`` ``hour_of_day_histogram``
  ``dow_hour_histogram`` ``seasonal_deviation`` ``event_type_mix``
  ``most_recent_n`` ``reservoir_sample``
- geo (4): ``geo_velocity`` ``geo_distance`` ``geo_spread`` ``distance_from_home``
"""
from __future__ import annotations

import re
import warnings
from dataclasses import dataclass, field as _dc_field
from typing import Any

from beava._col import _Expr
from beava._errors import RegistrationError

_WINDOW_PATTERN = re.compile(r"^\d+(ms|s|m|h|d)$|^forever$")


def _validate_window(
    window: str | None, op: str, *, required: bool = True
) -> None:
    if window is None:
        if required:
            raise ValueError(f"{op} requires a window= kwarg")
        return
    if not _WINDOW_PATTERN.match(window):
        raise ValueError(
            f"{op}: invalid window {window!r} — must match \\d+(ms|s|m|h|d) "
            f"or 'forever'"
        )


def _validate_half_life(half_life: str, op: str) -> None:
    if not _WINDOW_PATTERN.match(half_life):
        raise ValueError(f"{op}: invalid half_life {half_life!r}")


def _enforce_field_str(field_arg: Any, op: str) -> str:
    """Q1 Path B: ``field`` arg must be a string column name, not an ``_Expr``."""
    if isinstance(field_arg, _Expr):
        raise RegistrationError(
            code="schema_mismatch",
            path=op,
            message=(
                f"bv.{op}(field=...) requires a string column name, not an "
                f"expression. Use a two-stage chain: "
                f"events.with_columns(<derived>=<expr>).group_by(...).agg("
                f"{op}=bv.{op}('<derived>', ...)) — see "
                f"docs/sdk-api/python.md § bv.sum signature."
            ),
            errors=[],
        )
    if not isinstance(field_arg, str):
        raise RegistrationError(
            code="schema_mismatch",
            path=op,
            message=f"bv.{op}(field) must be a string; got {type(field_arg).__name__}",
            errors=[],
        )
    return field_arg


def _serialize_where(where: Any) -> str | None:
    if where is None:
        return None
    if isinstance(where, _Expr):
        return where.to_expr_string()
    raise TypeError(
        f"where= must be an _Expr or None; got {type(where).__name__}"
    )


@dataclass(frozen=True)
class AggDescriptor:
    """The artifact returned by every op helper.

    ``to_dict()`` renders to wire-shape JSON. The ``extras`` dict carries
    op-specific kwargs (``q``, ``k``, ``n``, ``threshold``,
    ``baseline_window``, ``sub_window``, ``buckets``, ``lat_field``,
    ``lon_field``) so each helper need not subclass.
    """

    op: str
    field: str | None = None
    window: str | None = None
    half_life: str | None = None
    extras: dict[str, Any] = _dc_field(default_factory=dict)
    where: str | None = None

    def to_dict(self) -> dict[str, Any]:
        d: dict[str, Any] = {"op": self.op}
        if self.field is not None:
            d["field"] = self.field
        if self.window is not None:
            d["window"] = self.window
        if self.half_life is not None:
            d["half_life"] = self.half_life
        if self.where is not None:
            d["where"] = self.where
        d.update(self.extras)
        return d


# ── Core (8) ───────────────────────────────────────────────────────────────


def count(*, window: str | None = None, where: Any = None) -> AggDescriptor:
    """Count of events in window."""
    _validate_window(window, "count", required=False)
    return AggDescriptor(op="count", window=window, where=_serialize_where(where))


def sum(  # noqa: A001 — shadows builtin intentionally per docs/sdk-api/python.md
    field: Any, *, window: str | None = None, where: Any = None
) -> AggDescriptor:
    """Sum of ``field`` over window. Q1 Path B: ``field`` must be a column-name string."""
    _enforce_field_str(field, "sum")
    _validate_window(window, "sum", required=False)
    return AggDescriptor(
        op="sum", field=field, window=window, where=_serialize_where(where)
    )


def mean(
    field: Any, *, window: str | None = None, where: Any = None
) -> AggDescriptor:
    """Polars-style ``mean`` (ADR-002 rename of ``avg``)."""
    _enforce_field_str(field, "mean")
    _validate_window(window, "mean", required=False)
    return AggDescriptor(
        op="mean", field=field, window=window, where=_serialize_where(where)
    )


def min(  # noqa: A001
    field: Any, *, window: str | None = None, where: Any = None
) -> AggDescriptor:
    _enforce_field_str(field, "min")
    _validate_window(window, "min", required=False)
    return AggDescriptor(
        op="min", field=field, window=window, where=_serialize_where(where)
    )


def max(  # noqa: A001
    field: Any, *, window: str | None = None, where: Any = None
) -> AggDescriptor:
    _enforce_field_str(field, "max")
    _validate_window(window, "max", required=False)
    return AggDescriptor(
        op="max", field=field, window=window, where=_serialize_where(where)
    )


def var(
    field: Any, *, window: str | None = None, where: Any = None
) -> AggDescriptor:
    """Polars-style ``var`` (ADR-002 rename of ``variance``)."""
    _enforce_field_str(field, "var")
    _validate_window(window, "var", required=False)
    return AggDescriptor(
        op="var", field=field, window=window, where=_serialize_where(where)
    )


def std(
    field: Any, *, window: str | None = None, where: Any = None
) -> AggDescriptor:
    """Polars-style ``std`` (ADR-002 rename of ``stddev``)."""
    _enforce_field_str(field, "std")
    _validate_window(window, "std", required=False)
    return AggDescriptor(
        op="std", field=field, window=window, where=_serialize_where(where)
    )


def ratio(*, window: str | None = None, where: Any = None) -> AggDescriptor:
    """Server computes ratio = matched / total over window; ``where`` filters numerator."""
    _validate_window(window, "ratio", required=False)
    return AggDescriptor(op="ratio", window=window, where=_serialize_where(where))


# ── Sketch (5) ─────────────────────────────────────────────────────────────


def n_unique(
    field: Any, *, window: str | None = None, where: Any = None
) -> AggDescriptor:
    """Polars-style ``n_unique`` (ADR-002 rename of ``count_distinct``)."""
    _enforce_field_str(field, "n_unique")
    _validate_window(window, "n_unique", required=False)
    return AggDescriptor(
        op="n_unique", field=field, window=window, where=_serialize_where(where)
    )


def quantile(
    field: Any,
    *,
    q: float,
    window: str | None = None,
    where: Any = None,
) -> AggDescriptor:
    """Polars-style ``quantile`` (ADR-002 rename of ``percentile``).

    ``q`` is in the open interval ``(0, 1)``.
    """
    _enforce_field_str(field, "quantile")
    if not (0.0 < q < 1.0):
        raise ValueError(f"quantile q must be in (0, 1); got {q}")
    _validate_window(window, "quantile", required=False)
    return AggDescriptor(
        op="quantile",
        field=field,
        window=window,
        extras={"q": q},
        where=_serialize_where(where),
    )


def top_k(
    field: Any,
    *,
    k: int,
    window: str | None = None,
    where: Any = None,
) -> AggDescriptor:
    """Top-k most frequent values (server-side count-min sketch + heap)."""
    _enforce_field_str(field, "top_k")
    if k < 1:
        raise ValueError(f"top_k k must be >= 1; got {k}")
    _validate_window(window, "top_k", required=False)
    return AggDescriptor(
        op="top_k",
        field=field,
        window=window,
        extras={"k": k},
        where=_serialize_where(where),
    )


def bloom_member(
    field: Any, *, window: str | None = None, where: Any = None
) -> AggDescriptor:
    _enforce_field_str(field, "bloom_member")
    _validate_window(window, "bloom_member", required=False)
    return AggDescriptor(
        op="bloom_member",
        field=field,
        window=window,
        where=_serialize_where(where),
    )


def entropy(
    field: Any, *, window: str | None = None, where: Any = None
) -> AggDescriptor:
    _enforce_field_str(field, "entropy")
    _validate_window(window, "entropy", required=False)
    return AggDescriptor(
        op="entropy",
        field=field,
        window=window,
        where=_serialize_where(where),
    )


# ── Point/ordinal (5) ──────────────────────────────────────────────────────


def first(field: Any) -> AggDescriptor:
    _enforce_field_str(field, "first")
    return AggDescriptor(op="first", field=field)


def last(field: Any) -> AggDescriptor:
    _enforce_field_str(field, "last")
    return AggDescriptor(op="last", field=field)


def first_n(field: Any, *, n: int) -> AggDescriptor:
    _enforce_field_str(field, "first_n")
    if n < 1:
        raise ValueError(f"first_n n must be >= 1; got {n}")
    return AggDescriptor(op="first_n", field=field, extras={"n": n})


def last_n(field: Any, *, n: int) -> AggDescriptor:
    _enforce_field_str(field, "last_n")
    if n < 1:
        raise ValueError(f"last_n n must be >= 1; got {n}")
    return AggDescriptor(op="last_n", field=field, extras={"n": n})


def lag(field: Any, *, n: int = 1) -> AggDescriptor:
    _enforce_field_str(field, "lag")
    if n < 1:
        raise ValueError(f"lag n must be >= 1; got {n}")
    return AggDescriptor(op="lag", field=field, extras={"n": n})


# ── Recency (10) ───────────────────────────────────────────────────────────


def first_seen() -> AggDescriptor:
    return AggDescriptor(op="first_seen")


def last_seen() -> AggDescriptor:
    return AggDescriptor(op="last_seen")


def age() -> AggDescriptor:
    return AggDescriptor(op="age")


def has_seen(field: Any) -> AggDescriptor:
    _enforce_field_str(field, "has_seen")
    return AggDescriptor(op="has_seen", field=field)


def time_since() -> AggDescriptor:
    return AggDescriptor(op="time_since")


def time_since_last_n(*, n: int) -> AggDescriptor:
    if n < 1:
        raise ValueError(f"time_since_last_n n must be >= 1; got {n}")
    return AggDescriptor(op="time_since_last_n", extras={"n": n})


def streak(field: Any) -> AggDescriptor:
    _enforce_field_str(field, "streak")
    return AggDescriptor(op="streak", field=field)


def max_streak(field: Any) -> AggDescriptor:
    _enforce_field_str(field, "max_streak")
    return AggDescriptor(op="max_streak", field=field)


def negative_streak(field: Any) -> AggDescriptor:
    _enforce_field_str(field, "negative_streak")
    return AggDescriptor(op="negative_streak", field=field)


def first_seen_in_window(field: Any, *, window: str) -> AggDescriptor:
    _enforce_field_str(field, "first_seen_in_window")
    _validate_window(window, "first_seen_in_window", required=True)
    return AggDescriptor(op="first_seen_in_window", field=field, window=window)


# ── Decay (6 + ema alias) ──────────────────────────────────────────────────


def ewma(
    field: Any, *, half_life: str, where: Any = None
) -> AggDescriptor:
    _enforce_field_str(field, "ewma")
    _validate_half_life(half_life, "ewma")
    return AggDescriptor(
        op="ewma",
        field=field,
        half_life=half_life,
        where=_serialize_where(where),
    )


def ema(
    field: Any, *, half_life: str, where: Any = None
) -> AggDescriptor:
    """``ema`` is an alias of ``ewma`` per docs/sdk-api/python.md § Operator catalog."""
    return ewma(field, half_life=half_life, where=where)


def ewvar(
    field: Any, *, half_life: str, where: Any = None
) -> AggDescriptor:
    _enforce_field_str(field, "ewvar")
    _validate_half_life(half_life, "ewvar")
    return AggDescriptor(
        op="ewvar",
        field=field,
        half_life=half_life,
        where=_serialize_where(where),
    )


def ew_zscore(
    field: Any, *, half_life: str, where: Any = None
) -> AggDescriptor:
    _enforce_field_str(field, "ew_zscore")
    _validate_half_life(half_life, "ew_zscore")
    return AggDescriptor(
        op="ew_zscore",
        field=field,
        half_life=half_life,
        where=_serialize_where(where),
    )


def decayed_sum(
    field: Any, *, half_life: str, where: Any = None
) -> AggDescriptor:
    _enforce_field_str(field, "decayed_sum")
    _validate_half_life(half_life, "decayed_sum")
    return AggDescriptor(
        op="decayed_sum",
        field=field,
        half_life=half_life,
        where=_serialize_where(where),
    )


def decayed_count(*, half_life: str, where: Any = None) -> AggDescriptor:
    _validate_half_life(half_life, "decayed_count")
    return AggDescriptor(
        op="decayed_count",
        half_life=half_life,
        where=_serialize_where(where),
    )


def twa(field: Any, *, window: str, where: Any = None) -> AggDescriptor:
    _enforce_field_str(field, "twa")
    _validate_window(window, "twa", required=True)
    return AggDescriptor(
        op="twa",
        field=field,
        window=window,
        where=_serialize_where(where),
    )


# ── Velocity (9) ───────────────────────────────────────────────────────────


def rate_of_change(
    field: Any, *, window: str, where: Any = None
) -> AggDescriptor:
    _enforce_field_str(field, "rate_of_change")
    _validate_window(window, "rate_of_change", required=True)
    return AggDescriptor(
        op="rate_of_change",
        field=field,
        window=window,
        where=_serialize_where(where),
    )


def inter_arrival_stats(*, window: str, where: Any = None) -> AggDescriptor:
    _validate_window(window, "inter_arrival_stats", required=True)
    return AggDescriptor(
        op="inter_arrival_stats",
        window=window,
        where=_serialize_where(where),
    )


def burst_count(
    *, window: str, sub_window: str, where: Any = None
) -> AggDescriptor:
    _validate_window(window, "burst_count", required=True)
    _validate_window(sub_window, "burst_count.sub_window", required=True)
    return AggDescriptor(
        op="burst_count",
        window=window,
        extras={"sub_window": sub_window},
        where=_serialize_where(where),
    )


def delta_from_prev(field: Any, *, where: Any = None) -> AggDescriptor:
    _enforce_field_str(field, "delta_from_prev")
    return AggDescriptor(
        op="delta_from_prev",
        field=field,
        where=_serialize_where(where),
    )


def trend(field: Any, *, window: str, where: Any = None) -> AggDescriptor:
    _enforce_field_str(field, "trend")
    _validate_window(window, "trend", required=True)
    return AggDescriptor(
        op="trend",
        field=field,
        window=window,
        where=_serialize_where(where),
    )


def trend_residual(
    field: Any, *, window: str, where: Any = None
) -> AggDescriptor:
    _enforce_field_str(field, "trend_residual")
    _validate_window(window, "trend_residual", required=True)
    return AggDescriptor(
        op="trend_residual",
        field=field,
        window=window,
        where=_serialize_where(where),
    )


def outlier_count(
    field: Any,
    *,
    window: str,
    threshold: float = 3.0,
    where: Any = None,
) -> AggDescriptor:
    _enforce_field_str(field, "outlier_count")
    _validate_window(window, "outlier_count", required=True)
    return AggDescriptor(
        op="outlier_count",
        field=field,
        window=window,
        extras={"threshold": threshold},
        where=_serialize_where(where),
    )


def value_change_count(
    field: Any, *, window: str, where: Any = None
) -> AggDescriptor:
    _enforce_field_str(field, "value_change_count")
    _validate_window(window, "value_change_count", required=True)
    return AggDescriptor(
        op="value_change_count",
        field=field,
        window=window,
        where=_serialize_where(where),
    )


def z_score(
    field: Any, *, baseline_window: str, where: Any = None
) -> AggDescriptor:
    _enforce_field_str(field, "z_score")
    _validate_window(baseline_window, "z_score.baseline_window", required=True)
    return AggDescriptor(
        op="z_score",
        field=field,
        extras={"baseline_window": baseline_window},
        where=_serialize_where(where),
    )


# ── Bounded buffers (7) ────────────────────────────────────────────────────


def histogram(
    field: Any,
    *,
    buckets: int,
    window: str,
    where: Any = None,
) -> AggDescriptor:
    _enforce_field_str(field, "histogram")
    if buckets < 1:
        raise ValueError(f"histogram buckets must be >= 1; got {buckets}")
    _validate_window(window, "histogram", required=True)
    return AggDescriptor(
        op="histogram",
        field=field,
        window=window,
        extras={"buckets": buckets},
        where=_serialize_where(where),
    )


def hour_of_day_histogram(*, window: str, where: Any = None) -> AggDescriptor:
    _validate_window(window, "hour_of_day_histogram", required=True)
    return AggDescriptor(
        op="hour_of_day_histogram",
        window=window,
        where=_serialize_where(where),
    )


def dow_hour_histogram(*, window: str, where: Any = None) -> AggDescriptor:
    _validate_window(window, "dow_hour_histogram", required=True)
    return AggDescriptor(
        op="dow_hour_histogram",
        window=window,
        where=_serialize_where(where),
    )


def seasonal_deviation(
    field: Any, *, window: str, where: Any = None
) -> AggDescriptor:
    _enforce_field_str(field, "seasonal_deviation")
    _validate_window(window, "seasonal_deviation", required=True)
    return AggDescriptor(
        op="seasonal_deviation",
        field=field,
        window=window,
        where=_serialize_where(where),
    )


def event_type_mix(
    field: Any, *, window: str, where: Any = None
) -> AggDescriptor:
    _enforce_field_str(field, "event_type_mix")
    _validate_window(window, "event_type_mix", required=True)
    return AggDescriptor(
        op="event_type_mix",
        field=field,
        window=window,
        where=_serialize_where(where),
    )


def most_recent_n(field: Any, *, n: int) -> AggDescriptor:
    _enforce_field_str(field, "most_recent_n")
    if n < 1:
        raise ValueError(f"most_recent_n n must be >= 1; got {n}")
    return AggDescriptor(op="most_recent_n", field=field, extras={"n": n})


def reservoir_sample(field: Any, *, k: int) -> AggDescriptor:
    _enforce_field_str(field, "reservoir_sample")
    if k < 1:
        raise ValueError(f"reservoir_sample k must be >= 1; got {k}")
    return AggDescriptor(op="reservoir_sample", field=field, extras={"k": k})


# ── Geo (4) ────────────────────────────────────────────────────────────────


def geo_velocity(
    lat_field: str, lon_field: str, *, window: str, where: Any = None
) -> AggDescriptor:
    _validate_window(window, "geo_velocity", required=True)
    return AggDescriptor(
        op="geo_velocity",
        window=window,
        extras={"lat_field": lat_field, "lon_field": lon_field},
        where=_serialize_where(where),
    )


def geo_distance(
    lat_field: str, lon_field: str, *, where: Any = None
) -> AggDescriptor:
    return AggDescriptor(
        op="geo_distance",
        extras={"lat_field": lat_field, "lon_field": lon_field},
        where=_serialize_where(where),
    )


def geo_spread(
    lat_field: str, lon_field: str, *, window: str, where: Any = None
) -> AggDescriptor:
    _validate_window(window, "geo_spread", required=True)
    return AggDescriptor(
        op="geo_spread",
        window=window,
        extras={"lat_field": lat_field, "lon_field": lon_field},
        where=_serialize_where(where),
    )


def distance_from_home(
    lat_field: str, lon_field: str, *, window: str, where: Any = None
) -> AggDescriptor:
    _validate_window(window, "distance_from_home", required=True)
    return AggDescriptor(
        op="distance_from_home",
        window=window,
        extras={"lat_field": lat_field, "lon_field": lon_field},
        where=_serialize_where(where),
    )


# ── ADR-002 deprecation aliases (5) ───────────────────────────────────────


def avg(field: Any, **kw: Any) -> AggDescriptor:
    """DEPRECATED: use ``bv.mean`` per ADR-002."""
    warnings.warn(
        "bv.avg is deprecated; use bv.mean per ADR-002",
        DeprecationWarning,
        stacklevel=2,
    )
    return mean(field, **kw)


def variance(field: Any, **kw: Any) -> AggDescriptor:
    """DEPRECATED: use ``bv.var`` per ADR-002."""
    warnings.warn(
        "bv.variance is deprecated; use bv.var per ADR-002",
        DeprecationWarning,
        stacklevel=2,
    )
    return var(field, **kw)


def stddev(field: Any, **kw: Any) -> AggDescriptor:
    """DEPRECATED: use ``bv.std`` per ADR-002."""
    warnings.warn(
        "bv.stddev is deprecated; use bv.std per ADR-002",
        DeprecationWarning,
        stacklevel=2,
    )
    return std(field, **kw)


def count_distinct(field: Any, **kw: Any) -> AggDescriptor:
    """DEPRECATED: use ``bv.n_unique`` per ADR-002."""
    warnings.warn(
        "bv.count_distinct is deprecated; use bv.n_unique per ADR-002",
        DeprecationWarning,
        stacklevel=2,
    )
    return n_unique(field, **kw)


def percentile(field: Any, *, p: float, **kw: Any) -> AggDescriptor:
    """DEPRECATED: use ``bv.quantile(q=...)`` per ADR-002."""
    warnings.warn(
        "bv.percentile is deprecated; use bv.quantile(q=...) per ADR-002",
        DeprecationWarning,
        stacklevel=2,
    )
    return quantile(field, q=p, **kw)
