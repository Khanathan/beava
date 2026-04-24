"""Aggregation helpers and GroupBy builder for the Beava Python SDK.

Requirements: SDK-AGG-01, SDK-AGG-02, SDK-AGG-03, SDK-AGG-04, SDK-AGG-05, SDK-AGG-06

Public API (re-exported from beava.__init__):
  - AggDescriptor: frozen dataclass returned by every helper
  - GroupBy: builder returned by EventSource/EventDerivation.group_by()
  - count, sum, avg, min, max, variance, stddev, ratio: module-level helpers

NOTE: ``sum``, ``min``, and ``max`` shadow Python builtins at module scope.
This is intentional — the beava namespace is a DSL, not a stdlib replacement.
Users who need the builtins should access them via ``builtins.sum`` etc.

Window string format (SDK-AGG-06):
  Accepted:  \\d+(ms|s|m|h|d)  or  "forever"
  Examples:  "5m", "1h", "30s", "100ms", "7d", "forever"
  Rejected:  "5seconds", "1hour", "5", ""  → ValueError at call time
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

__all__ = [
    "AggDescriptor",
    "GroupBy",
    # Phase 5 core
    "count",
    "sum",
    "avg",
    "min",
    "max",
    "variance",
    "stddev",
    "ratio",
    # Phase 9 decay (AGG-DECAY-01..06)
    "ewma",
    "ema",
    "ewvar",
    "ew_zscore",
    "decayed_sum",
    "decayed_count",
    "twa",
    # Phase 9 velocity (AGG-VEL-01..08)
    "rate_of_change",
    "inter_arrival_stats",
    "burst_count",
    "delta_from_prev",
    "trend",
    "trend_residual",
    "outlier_count",
    "value_change_count",
    # Phase 9 entity z-score (AGG-Z-01)
    "z_score",
]

# ---------------------------------------------------------------------------
# Window string validation (SDK-AGG-06)
# ---------------------------------------------------------------------------

# CR-01: leading digit must be 1-9 to reject zero-value windows like "0ms".
_WINDOW_PATTERN = re.compile(r"^(?:[1-9]\d*(?:ms|s|m|h|d)|forever)$")


def _validate_window(window: str | None, op: str, requires_window: bool) -> None:
    """Validate the window= argument at helper-call time (SDK-AGG-06).

    Args:
        window:          The window string to validate, or None.
        op:              Operator name (for the error message).
        requires_window: True for sum/avg/min/max/variance/stddev.

    Raises:
        ValueError: If window is required but absent, or present but malformed.
    """
    if window is None:
        if requires_window:
            raise ValueError(
                f"window is required for {op!r} operator; "
                "provide a duration string e.g. '5m', '1h', or 'forever'"
            )
        return
    if not _WINDOW_PATTERN.match(window):
        raise ValueError(
            f"window={window!r} must match regex \\d+(ms|s|m|h|d) or 'forever'; "
            "examples: '5m', '1h', '30s', '100ms', '7d', 'forever'"
        )


def _serialize_where(where: Any) -> str | None:
    """Serialize the where= predicate to its canonical expression string.

    Accepts any object with a ``to_expr_string()`` method (duck-typed so that
    the Phase 4 _ExprAST hierarchy works without a hard import cycle).

    Args:
        where: An ``_ExprAST`` expression node, or None.

    Returns:
        The serialized expression string, or None if where is None.

    Raises:
        TypeError: If where is not None and has no ``to_expr_string()`` method.
    """
    if where is None:
        return None
    if not hasattr(where, "to_expr_string"):
        raise TypeError(
            f"where= must be a bv.col(...) expression; got {type(where).__name__!r}. "
            "Example: where=bv.col('status') == 'ok'"
        )
    result: str = where.to_expr_string()
    return result


# ---------------------------------------------------------------------------
# AggDescriptor frozen dataclass
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class AggDescriptor:
    """Returned by bv.count() / bv.sum() / ... / bv.ratio().

    Consumed by GroupBy.agg() to build REGISTER JSON.

    Attributes:
        op:         Operator name: count|sum|avg|min|max|variance|stddev|ratio or
                    Phase 9 names (ewma, ewvar, ew_zscore, decayed_sum,
                    decayed_count, twa, rate_of_change, inter_arrival_stats,
                    burst_count, delta_from_prev, trend, trend_residual,
                    outlier_count, value_change_count, z_score).
        field:      Column name (None for count/ratio/inter_arrival_stats/burst_count).
        window:     Duration string, or None (lifetime).
        where:      Serialized predicate string (from _ExprAST.to_expr_string()), or None.
        half_life:  Duration string for decay ops (Phase 9 AGG-DECAY).
        sub_window: Duration string for burst_count (Phase 9 AGG-VEL-03).
        sigma:      Float threshold for outlier_count (Phase 9 AGG-VEL-07); default 3.0.
    """

    op: str
    field: str | None = None
    window: str | None = None
    where: str | None = None
    half_life: str | None = None
    sub_window: str | None = None
    sigma: float | None = None

    def to_agg_spec(self) -> dict[str, Any]:
        """Return the wire-JSON AggSpec for this descriptor.

        Only non-None values are included in params to keep the wire payload
        minimal; the Rust server treats absent keys the same as null.
        """
        params: dict[str, Any] = {}
        if self.field is not None:
            params["field"] = self.field
        if self.window is not None:
            params["window"] = self.window
        if self.where is not None:
            params["where"] = self.where
        if self.half_life is not None:
            params["half_life"] = self.half_life
        if self.sub_window is not None:
            params["sub_window"] = self.sub_window
        if self.sigma is not None:
            params["sigma"] = self.sigma
        return {"op": self.op, "params": params}


# ---------------------------------------------------------------------------
# 8 module-level helper functions
# ---------------------------------------------------------------------------


def count(*, window: str | None = None, where: Any = None) -> AggDescriptor:
    """Count of events in the window (or lifetime if window is omitted).

    AGG-CORE-09: window is optional; omitting it means lifetime count.
    SDK-AGG-06:  if supplied, window must match \\d+(ms|s|m|h|d) or 'forever'.

    Args:
        window: Optional duration string, e.g. ``"5m"``, ``"1h"``, ``"forever"``.
        where:  Optional ``bv.col(...)`` predicate — count only matching events.

    Returns:
        AggDescriptor(op='count', field=None, window=..., where=...)
    """
    _validate_window(window, "count", requires_window=False)
    return AggDescriptor(op="count", field=None, window=window, where=_serialize_where(where))


def sum(field: str, *, window: str, where: Any = None) -> AggDescriptor:  # noqa: A001
    """Sum of *field* over *window*.

    Args:
        field:  Name of the numeric field to sum.
        window: Required duration string (SDK-AGG-06).
        where:  Optional predicate — sum only matching events.

    Returns:
        AggDescriptor(op='sum', field=field, window=window, where=...)
    """
    _validate_window(window, "sum", requires_window=True)
    return AggDescriptor(op="sum", field=field, window=window, where=_serialize_where(where))


def avg(field: str, *, window: str, where: Any = None) -> AggDescriptor:
    """Arithmetic mean of *field* over *window*.

    Args:
        field:  Name of the numeric field.
        window: Required duration string (SDK-AGG-06).
        where:  Optional predicate.

    Returns:
        AggDescriptor(op='avg', field=field, window=window, where=...)
    """
    _validate_window(window, "avg", requires_window=True)
    return AggDescriptor(op="avg", field=field, window=window, where=_serialize_where(where))


def min(field: str, *, window: str, where: Any = None) -> AggDescriptor:  # noqa: A001
    """Minimum value of *field* over *window*. Preserves field type.

    Args:
        field:  Name of the field.
        window: Required duration string (SDK-AGG-06).
        where:  Optional predicate.

    Returns:
        AggDescriptor(op='min', field=field, window=window, where=...)
    """
    _validate_window(window, "min", requires_window=True)
    return AggDescriptor(op="min", field=field, window=window, where=_serialize_where(where))


def max(field: str, *, window: str, where: Any = None) -> AggDescriptor:  # noqa: A001
    """Maximum value of *field* over *window*. Preserves field type.

    Args:
        field:  Name of the field.
        window: Required duration string (SDK-AGG-06).
        where:  Optional predicate.

    Returns:
        AggDescriptor(op='max', field=field, window=window, where=...)
    """
    _validate_window(window, "max", requires_window=True)
    return AggDescriptor(op="max", field=field, window=window, where=_serialize_where(where))


def variance(field: str, *, window: str, where: Any = None) -> AggDescriptor:
    """Sample variance of *field* over *window* (Welford / Bessel-corrected).

    Args:
        field:  Name of the numeric field.
        window: Required duration string (SDK-AGG-06).
        where:  Optional predicate.

    Returns:
        AggDescriptor(op='variance', field=field, window=window, where=...)
    """
    _validate_window(window, "variance", requires_window=True)
    return AggDescriptor(
        op="variance", field=field, window=window, where=_serialize_where(where)
    )


def stddev(field: str, *, window: str, where: Any = None) -> AggDescriptor:
    """Sample standard deviation of *field* over *window* (sqrt of sample variance).

    Args:
        field:  Name of the numeric field.
        window: Required duration string (SDK-AGG-06).
        where:  Optional predicate.

    Returns:
        AggDescriptor(op='stddev', field=field, window=window, where=...)
    """
    _validate_window(window, "stddev", requires_window=True)
    return AggDescriptor(
        op="stddev", field=field, window=window, where=_serialize_where(where)
    )


def ratio(*, window: str | None = None, where: Any = None) -> AggDescriptor:
    """Ratio of where-matching events to total events in the window.

    AGG-CORE-09: window is optional; omitting it means lifetime ratio.
    SDK-AGG-06:  if supplied, window must match \\d+(ms|s|m|h|d) or 'forever'.

    Args:
        window: Optional duration string.
        where:  Optional predicate — numerator filter.

    Returns:
        AggDescriptor(op='ratio', field=None, window=..., where=...)
    """
    _validate_window(window, "ratio", requires_window=False)
    return AggDescriptor(op="ratio", field=None, window=window, where=_serialize_where(where))


# ---------------------------------------------------------------------------
# Phase 9 — Decay helpers (AGG-DECAY-01..06)
# ---------------------------------------------------------------------------


def _validate_half_life(half_life: str, op: str) -> None:
    """Validate half_life duration (must be finite, positive). AGG-DECAY-07."""
    if not isinstance(half_life, str) or not half_life:
        raise ValueError(
            f"{op}(): half_life must be a non-empty duration string e.g. '5m', '1h'"
        )
    # Allow any \\d+(ms|s|m|h|d), reject "forever".
    if not re.match(r"^[1-9]\d*(?:ms|s|m|h|d)$", half_life):
        raise ValueError(
            f"{op}(): half_life={half_life!r} must match \\d+(ms|s|m|h|d); "
            "'forever' is not allowed for decay ops"
        )


def ewma(field: str, *, half_life: str, where: Any = None) -> AggDescriptor:
    """AGG-DECAY-01: Exponentially-weighted moving average.

    Args:
        field:     Numeric field to track.
        half_life: Duration string, e.g. ``"5m"``. Must be positive and finite.
        where:     Optional predicate.

    Returns:
        AggDescriptor(op='ewma', ...).
    """
    _validate_half_life(half_life, "ewma")
    return AggDescriptor(
        op="ewma",
        field=field,
        half_life=half_life,
        where=_serialize_where(where),
    )


def ema(field: str, *, half_life: str, where: Any = None) -> AggDescriptor:
    """SDK alias for :func:`ewma` (same server-side op)."""
    return ewma(field, half_life=half_life, where=where)


def ewvar(field: str, *, half_life: str, where: Any = None) -> AggDescriptor:
    """AGG-DECAY-02: Exponentially-weighted variance."""
    _validate_half_life(half_life, "ewvar")
    return AggDescriptor(
        op="ewvar",
        field=field,
        half_life=half_life,
        where=_serialize_where(where),
    )


def ew_zscore(field: str, *, half_life: str, where: Any = None) -> AggDescriptor:
    """AGG-DECAY-03: Current event z-score vs EW baseline."""
    _validate_half_life(half_life, "ew_zscore")
    return AggDescriptor(
        op="ew_zscore",
        field=field,
        half_life=half_life,
        where=_serialize_where(where),
    )


def decayed_sum(field: str, *, half_life: str, where: Any = None) -> AggDescriptor:
    """AGG-DECAY-04: Forward-decay sum (Cormode)."""
    _validate_half_life(half_life, "decayed_sum")
    return AggDescriptor(
        op="decayed_sum",
        field=field,
        half_life=half_life,
        where=_serialize_where(where),
    )


def decayed_count(*, half_life: str, where: Any = None) -> AggDescriptor:
    """AGG-DECAY-05: Forward-decay count."""
    _validate_half_life(half_life, "decayed_count")
    return AggDescriptor(
        op="decayed_count",
        field=None,
        half_life=half_life,
        where=_serialize_where(where),
    )


def twa(field: str, *, window: str, where: Any = None) -> AggDescriptor:
    """AGG-DECAY-06: Time-weighted average (gauge fields)."""
    _validate_window(window, "twa", requires_window=True)
    return AggDescriptor(
        op="twa", field=field, window=window, where=_serialize_where(where)
    )


# ---------------------------------------------------------------------------
# Phase 9 — Velocity helpers (AGG-VEL-01..08)
# ---------------------------------------------------------------------------


def rate_of_change(field: str, *, window: str, where: Any = None) -> AggDescriptor:
    """AGG-VEL-01: rate of change across consecutive events within window."""
    _validate_window(window, "rate_of_change", requires_window=True)
    return AggDescriptor(
        op="rate_of_change",
        field=field,
        window=window,
        where=_serialize_where(where),
    )


def inter_arrival_stats(*, window: str, where: Any = None) -> AggDescriptor:
    """AGG-VEL-02: mean inter-arrival (ms). v0 emits mean only."""
    _validate_window(window, "inter_arrival_stats", requires_window=True)
    return AggDescriptor(
        op="inter_arrival_stats",
        field=None,
        window=window,
        where=_serialize_where(where),
    )


def burst_count(*, window: str, sub_window: str, where: Any = None) -> AggDescriptor:
    """AGG-VEL-03: max events observed in any sub-window."""
    _validate_window(window, "burst_count", requires_window=True)
    if not isinstance(sub_window, str) or not re.match(
        r"^[1-9]\d*(?:ms|s|m|h|d)$", sub_window
    ):
        raise ValueError(
            f"burst_count(): sub_window={sub_window!r} must match \\d+(ms|s|m|h|d)"
        )
    return AggDescriptor(
        op="burst_count",
        field=None,
        window=window,
        sub_window=sub_window,
        where=_serialize_where(where),
    )


def delta_from_prev(field: str, *, where: Any = None) -> AggDescriptor:
    """AGG-VEL-04: current value - previous value."""
    return AggDescriptor(
        op="delta_from_prev", field=field, where=_serialize_where(where)
    )


def trend(field: str, *, window: str, where: Any = None) -> AggDescriptor:
    """AGG-VEL-05: slope of online linear regression."""
    _validate_window(window, "trend", requires_window=True)
    return AggDescriptor(
        op="trend", field=field, window=window, where=_serialize_where(where)
    )


def trend_residual(field: str, *, window: str, where: Any = None) -> AggDescriptor:
    """AGG-VEL-06: current_value - trend-predicted value."""
    _validate_window(window, "trend_residual", requires_window=True)
    return AggDescriptor(
        op="trend_residual",
        field=field,
        window=window,
        where=_serialize_where(where),
    )


def outlier_count(
    field: str,
    *,
    window: str,
    sigma: float = 3.0,
    where: Any = None,
) -> AggDescriptor:
    """AGG-VEL-07: count of events with |x - mean| > sigma * stddev."""
    _validate_window(window, "outlier_count", requires_window=True)
    if not isinstance(sigma, (int, float)) or sigma <= 0:
        raise ValueError(f"outlier_count(): sigma={sigma!r} must be positive float")
    return AggDescriptor(
        op="outlier_count",
        field=field,
        window=window,
        sigma=float(sigma),
        where=_serialize_where(where),
    )


def value_change_count(field: str, *, window: str, where: Any = None) -> AggDescriptor:
    """AGG-VEL-08: count of value flips."""
    _validate_window(window, "value_change_count", requires_window=True)
    return AggDescriptor(
        op="value_change_count",
        field=field,
        window=window,
        where=_serialize_where(where),
    )


# ---------------------------------------------------------------------------
# Phase 9 — Entity z-score (AGG-Z-01)
# ---------------------------------------------------------------------------


def z_score(field: str, *, baseline_window: str, where: Any = None) -> AggDescriptor:
    """AGG-Z-01: (current - mean) / stddev over baseline_window."""
    _validate_window(baseline_window, "z_score", requires_window=True)
    return AggDescriptor(
        op="z_score",
        field=field,
        window=baseline_window,
        where=_serialize_where(where),
    )


# ---------------------------------------------------------------------------
# GroupBy builder
# ---------------------------------------------------------------------------


class GroupBy:
    """Returned by EventSource/EventDerivation.group_by(*keys).

    Call ``.agg(**named_features)`` to produce a TableDerivation.
    """

    def __init__(self, upstream: Any, keys: list[str]) -> None:
        self._upstream = upstream
        self._keys = keys

    def agg(self, **named_features: AggDescriptor) -> Any:
        """Build a TableDerivation from named aggregation descriptors.

        Each keyword argument name becomes a feature column in the output table.
        The value must be an AggDescriptor (from bv.count/sum/avg/etc.).

        Args:
            **named_features: Mapping of output feature name → AggDescriptor.

        Returns:
            TableDerivation with output_kind="table", primary_key=group_keys,
            and a ``group_by`` op-node appended to the ops list.

        Raises:
            TypeError: If any value is not an AggDescriptor.
        """
        # Validate: every value must be an AggDescriptor (T-05-07-02)
        for name, desc in named_features.items():
            if not isinstance(desc, AggDescriptor):
                raise TypeError(
                    f"agg(...) kwarg {name!r} must be an AggDescriptor "
                    f"(from bv.count/sum/avg/min/max/variance/stddev/ratio); "
                    f"got {type(desc).__name__!r}"
                )

        # Build the REGISTER JSON GroupBy op-node
        agg_map = {name: desc.to_agg_spec() for name, desc in named_features.items()}
        op_node: dict[str, Any] = {
            "op": "group_by",
            "keys": list(self._keys),
            "agg": agg_map,
        }

        # Construct the TableDerivation — import inside method to avoid circular deps
        from ._schema import FieldSpec  # noqa: PLC0415
        from ._tables import TableDerivation  # noqa: PLC0415

        upstream_name: str = getattr(self._upstream, "_name", None)  # type: ignore[assignment]
        if upstream_name is None:
            raise TypeError(
                "group_by() upstream must have a _name attribute "
                "(EventSource / EventDerivation expected)"
            )

        existing_ops: list[Any] = list(getattr(self._upstream, "_ops", []))
        upstream_schema: dict[str, Any] = getattr(self._upstream, "_schema", {})

        # Build output schema: group-by keys (from upstream) + aggregated features.
        # count/ratio → int; all other ops → float.
        output_schema: dict[str, FieldSpec] = {}
        for key in self._keys:
            if key in upstream_schema:
                output_schema[key] = upstream_schema[key]
            else:
                # Key not found in upstream — fall back to str (server will validate)
                output_schema[key] = FieldSpec(name=key, py_type=str)
        for feat_name, desc in named_features.items():
            if desc.op in ("count",):
                output_schema[feat_name] = FieldSpec(name=feat_name, py_type=int)
            else:
                output_schema[feat_name] = FieldSpec(name=feat_name, py_type=float)

        return TableDerivation(
            name=f"{upstream_name}_by_{'_'.join(self._keys)}",
            schema=output_schema,
            upstreams=[upstream_name],
            ops=[*existing_ops, op_node],
            output_kind="table",
            table_primary_key=list(self._keys),
            upstream=self._upstream,
        )
