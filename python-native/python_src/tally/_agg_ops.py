"""Aggregation operator descriptors for the v0 SDK (Plan 21-03).

Each descriptor is a pure-Python spec object — no execution. Phase 22 wires
these into the Rust engine via the REGISTER JSON ``aggregation.features[]``
payload (see :mod:`tally._serialize`).

Each subclass declares:

  * ``supports_retraction`` (class attr) — whether the engine can decrement
    this operator when an upstream event is retracted.
  * ``requires_window`` (class attr) — windowed ops need a ``window=`` kwarg;
    point-in-time / ordinal ops (``first``, ``last``, ``first_n``, ``last_n``,
    ``ema``, ``lag``) set this to False.
  * ``hybrid_params`` (dict, optional) — exact→approximate transition
    parameters for sketch-backed operators (percentile/count_distinct/top_k).
  * ``output_type_for(schema)`` — schema inference.
  * ``to_json(name)`` — emits the engine JSON fragment that Phase 22 consumes.

The lowercase module-level aliases (``count``, ``sum``, …) are re-exported on
the public ``tally`` namespace by :mod:`tally.__init__`. Collisions with
Python builtins are intentional — ``tl.sum`` is a spec constructor, not the
builtin.
"""

from __future__ import annotations

import re
from typing import Any

from tally._types_core import FieldSpec


# ---------------------------------------------------------------------------
# Base
# ---------------------------------------------------------------------------


_WINDOW_RE = re.compile(r"^\d+(ms|s|m|h|d)$")


def _validate_window(window: Any, op_name: str) -> str:
    if not isinstance(window, str) or not _WINDOW_RE.match(window):
        raise ValueError(
            f"{op_name}: window must be a duration string like '30m' / '1h' / "
            f"'24h'; got {window!r}"
        )
    return window


def _validate_half_life(value: Any, op_name: str) -> str:
    if not isinstance(value, str) or not _WINDOW_RE.match(value):
        raise ValueError(
            f"{op_name}: half_life must be a duration string like '30m' / "
            f"'1h'; got {value!r}"
        )
    return value


class AggOp:
    """Base class for aggregation operator descriptors.

    Subclasses MUST set ``op_type`` (str, the engine-side operator tag) and
    SHOULD override ``output_type_for`` if the output type depends on the
    input field's type.
    """

    # Class-level defaults — subclasses override.
    op_type: str = ""
    supports_retraction: bool = False
    requires_window: bool = True

    # Populated by subclass constructors.
    field: str | None = None
    window: str | None = None
    where: str | None = None
    bucket: str | None = None

    # Hybrid transition parameters — subset used by sketch-backed ops.
    hybrid_params: dict[str, Any] | None = None

    # ---- schema inference ----
    def output_type_for(self, schema: dict[str, FieldSpec]) -> type:
        """Compute the Python output type for this op given the input schema.

        Default implementation returns ``float`` — most bucketed aggregates
        produce floats. Subclasses override when the output type depends on
        the referenced field's type (min/max/first/last/lag) or is a
        container (first_n/last_n/top_k) or fixed (count/count_distinct → int).
        """
        return float

    # ---- serialization ----
    def to_json(self, name: str) -> dict[str, Any]:
        """Emit the engine JSON fragment for this aggregation.

        The shape matches Phase 22's consumer contract (documented in
        ``21-03-PLAN.md``). Optional keys are omitted when ``None``.
        """
        d: dict[str, Any] = {
            "name": name,
            "type": self.op_type,
            "supports_retraction": self.supports_retraction,
        }
        if self.field is not None:
            d["field"] = self.field
        if self.window is not None:
            d["window"] = self.window
        if self.where is not None:
            d["where"] = self.where
        if self.bucket is not None:
            d["bucket"] = self.bucket
        if self.hybrid_params:
            # Flatten hybrid params as top-level keys (engine contract).
            for k, v in self.hybrid_params.items():
                d[k] = v
        # Subclass-specific extras (set by constructor).
        for extra in ("n", "quantile", "half_life", "k"):
            if hasattr(self, extra) and getattr(self, extra) is not None:
                d[extra] = getattr(self, extra)
        return d

    def _validate_field_ref(
        self, schema: dict[str, FieldSpec], op_name: str
    ) -> None:
        """Raise if ``self.field`` isn't in ``schema``. Caller context prefix."""
        if self.field is None:
            return
        if self.field not in schema:
            from tally._schema_v0 import schema_mismatch_error
            raise TypeError(
                schema_mismatch_error(self.field, schema, op_name)
            )


# ---------------------------------------------------------------------------
# Bucketed numeric aggregates
# ---------------------------------------------------------------------------


class _Count(AggOp):
    op_type = "count"
    supports_retraction = True
    requires_window = True

    def __init__(
        self,
        *,
        window: str,
        where: str | None = None,
        bucket: str | None = None,
    ) -> None:
        self.field = None
        self.window = _validate_window(window, "count")
        self.where = where
        self.bucket = bucket

    def output_type_for(self, schema: dict[str, FieldSpec]) -> type:
        return int


class _Sum(AggOp):
    op_type = "sum"
    supports_retraction = True
    requires_window = True

    def __init__(
        self,
        field: str,
        *,
        window: str,
        where: str | None = None,
        bucket: str | None = None,
    ) -> None:
        if not isinstance(field, str) or not field:
            raise TypeError("sum(field, ...) requires a non-empty field name")
        self.field = field
        self.window = _validate_window(window, "sum")
        self.where = where
        self.bucket = bucket

    def output_type_for(self, schema: dict[str, FieldSpec]) -> type:
        return float


class _Avg(AggOp):
    op_type = "avg"
    supports_retraction = True
    requires_window = True

    def __init__(
        self,
        field: str,
        *,
        window: str,
        bucket: str | None = None,
    ) -> None:
        if not isinstance(field, str) or not field:
            raise TypeError("avg(field, ...) requires a non-empty field name")
        self.field = field
        self.window = _validate_window(window, "avg")
        self.bucket = bucket

    def output_type_for(self, schema: dict[str, FieldSpec]) -> type:
        return float


class _Min(AggOp):
    op_type = "min"
    supports_retraction = False  # bucketed min can't decrement (spec §3.3)
    requires_window = True

    def __init__(
        self,
        field: str,
        *,
        window: str,
        bucket: str | None = None,
    ) -> None:
        if not isinstance(field, str) or not field:
            raise TypeError("min(field, ...) requires a non-empty field name")
        self.field = field
        self.window = _validate_window(window, "min")
        self.bucket = bucket

    def output_type_for(self, schema: dict[str, FieldSpec]) -> type:
        spec = schema.get(self.field) if self.field else None
        return spec.py_type if spec else float


class _Max(AggOp):
    op_type = "max"
    supports_retraction = False
    requires_window = True

    def __init__(
        self,
        field: str,
        *,
        window: str,
        bucket: str | None = None,
    ) -> None:
        if not isinstance(field, str) or not field:
            raise TypeError("max(field, ...) requires a non-empty field name")
        self.field = field
        self.window = _validate_window(window, "max")
        self.bucket = bucket

    def output_type_for(self, schema: dict[str, FieldSpec]) -> type:
        spec = schema.get(self.field) if self.field else None
        return spec.py_type if spec else float


class _Variance(AggOp):
    op_type = "variance"
    supports_retraction = True  # Welford
    requires_window = True

    def __init__(
        self,
        field: str,
        *,
        window: str,
        bucket: str | None = None,
    ) -> None:
        if not isinstance(field, str) or not field:
            raise TypeError("variance(field, ...) requires a non-empty field name")
        self.field = field
        self.window = _validate_window(window, "variance")
        self.bucket = bucket

    def output_type_for(self, schema: dict[str, FieldSpec]) -> type:
        return float


class _Stddev(AggOp):
    op_type = "stddev"
    supports_retraction = True
    requires_window = True

    def __init__(
        self,
        field: str,
        *,
        window: str,
        bucket: str | None = None,
    ) -> None:
        if not isinstance(field, str) or not field:
            raise TypeError("stddev(field, ...) requires a non-empty field name")
        self.field = field
        self.window = _validate_window(window, "stddev")
        self.bucket = bucket

    def output_type_for(self, schema: dict[str, FieldSpec]) -> type:
        return float


# ---------------------------------------------------------------------------
# Sketch-backed aggregates (hybrid exact→approximate)
# ---------------------------------------------------------------------------


class _Percentile(AggOp):
    op_type = "percentile"
    supports_retraction = False  # UDDSketch is not retractable
    requires_window = True

    def __init__(
        self,
        field: str,
        quantile: float,
        *,
        window: str,
        bucket: str | None = None,
        exact_threshold: int = 256,
        hybrid_alpha: float = 0.01,
    ) -> None:
        if not isinstance(field, str) or not field:
            raise TypeError(
                "percentile(field, quantile, ...) requires a non-empty field name"
            )
        if not isinstance(quantile, (int, float)) or not (0.0 <= float(quantile) <= 1.0):
            raise ValueError(
                f"percentile: quantile must be in [0, 1]; got {quantile!r}"
            )
        if not isinstance(exact_threshold, int) or exact_threshold <= 0:
            raise ValueError(
                f"percentile: exact_threshold must be positive int; got {exact_threshold!r}"
            )
        if not isinstance(hybrid_alpha, (int, float)) or not (0.0 < float(hybrid_alpha) < 1.0):
            raise ValueError(
                f"percentile: hybrid_alpha must be in (0, 1); got {hybrid_alpha!r}"
            )
        self.field = field
        self.quantile = float(quantile)
        self.window = _validate_window(window, "percentile")
        self.bucket = bucket
        self.hybrid_params = {
            "exact_threshold": exact_threshold,
            "hybrid_alpha": float(hybrid_alpha),
        }

    def output_type_for(self, schema: dict[str, FieldSpec]) -> type:
        return float


class _CountDistinct(AggOp):
    op_type = "count_distinct"
    supports_retraction = False  # HLL decrement is fuzzy
    requires_window = True

    def __init__(
        self,
        field: str,
        *,
        window: str,
        bucket: str | None = None,
        exact_threshold: int = 1024,
        hybrid_precision: int = 14,
    ) -> None:
        if not isinstance(field, str) or not field:
            raise TypeError(
                "count_distinct(field, ...) requires a non-empty field name"
            )
        if not isinstance(exact_threshold, int) or exact_threshold <= 0:
            raise ValueError(
                f"count_distinct: exact_threshold must be positive int; got {exact_threshold!r}"
            )
        if not isinstance(hybrid_precision, int) or not (4 <= hybrid_precision <= 16):
            raise ValueError(
                f"count_distinct: hybrid_precision must be in [4, 16]; got {hybrid_precision!r}"
            )
        self.field = field
        self.window = _validate_window(window, "count_distinct")
        self.bucket = bucket
        self.hybrid_params = {
            "exact_threshold": exact_threshold,
            "hybrid_precision": hybrid_precision,
        }

    def output_type_for(self, schema: dict[str, FieldSpec]) -> type:
        return int


class _TopK(AggOp):
    op_type = "top_k"
    supports_retraction = False
    requires_window = True

    def __init__(
        self,
        field: str,
        k: int,
        *,
        window: str,
        bucket: str | None = None,
        exact_threshold: int = 1024,
        hybrid_width: int = 2048,
        hybrid_depth: int = 4,
    ) -> None:
        if not isinstance(field, str) or not field:
            raise TypeError("top_k(field, k, ...) requires a non-empty field name")
        if not isinstance(k, int) or k <= 0:
            raise ValueError(f"top_k: k must be a positive int; got {k!r}")
        if not isinstance(exact_threshold, int) or exact_threshold <= 0:
            raise ValueError(
                f"top_k: exact_threshold must be positive int; got {exact_threshold!r}"
            )
        if not isinstance(hybrid_width, int) or hybrid_width <= 0:
            raise ValueError(
                f"top_k: hybrid_width must be positive int; got {hybrid_width!r}"
            )
        if not isinstance(hybrid_depth, int) or hybrid_depth <= 0:
            raise ValueError(
                f"top_k: hybrid_depth must be positive int; got {hybrid_depth!r}"
            )
        self.field = field
        self.k = k
        self.window = _validate_window(window, "top_k")
        self.bucket = bucket
        self.hybrid_params = {
            "exact_threshold": exact_threshold,
            "hybrid_width": hybrid_width,
            "hybrid_depth": hybrid_depth,
        }

    def output_type_for(self, schema: dict[str, FieldSpec]) -> type:
        return list


# ---------------------------------------------------------------------------
# Point-in-time / ordinal operators (no window)
# ---------------------------------------------------------------------------


class _First(AggOp):
    op_type = "first"
    supports_retraction = False
    requires_window = False

    def __init__(self, field: str) -> None:
        if not isinstance(field, str) or not field:
            raise TypeError("first(field) requires a non-empty field name")
        self.field = field

    def output_type_for(self, schema: dict[str, FieldSpec]) -> type:
        spec = schema.get(self.field) if self.field else None
        return spec.py_type if spec else object


class _Last(AggOp):
    op_type = "last"
    supports_retraction = False
    requires_window = False

    def __init__(self, field: str) -> None:
        if not isinstance(field, str) or not field:
            raise TypeError("last(field) requires a non-empty field name")
        self.field = field

    def output_type_for(self, schema: dict[str, FieldSpec]) -> type:
        spec = schema.get(self.field) if self.field else None
        return spec.py_type if spec else object


class _FirstN(AggOp):
    op_type = "first_n"
    supports_retraction = False
    requires_window = False

    def __init__(self, field: str, n: int) -> None:
        if not isinstance(field, str) or not field:
            raise TypeError("first_n(field, n) requires a non-empty field name")
        if not isinstance(n, int) or n <= 0:
            raise ValueError(f"first_n: n must be a positive int; got {n!r}")
        self.field = field
        self.n = n

    def output_type_for(self, schema: dict[str, FieldSpec]) -> type:
        return list


class _LastN(AggOp):
    op_type = "last_n"
    supports_retraction = False
    requires_window = False

    def __init__(self, field: str, n: int) -> None:
        if not isinstance(field, str) or not field:
            raise TypeError("last_n(field, n) requires a non-empty field name")
        if not isinstance(n, int) or n <= 0:
            raise ValueError(f"last_n: n must be a positive int; got {n!r}")
        self.field = field
        self.n = n

    def output_type_for(self, schema: dict[str, FieldSpec]) -> type:
        return list


class _EMA(AggOp):
    op_type = "ema"
    supports_retraction = False
    requires_window = False

    def __init__(self, field: str, half_life: str) -> None:
        if not isinstance(field, str) or not field:
            raise TypeError("ema(field, half_life) requires a non-empty field name")
        self.field = field
        self.half_life = _validate_half_life(half_life, "ema")

    def output_type_for(self, schema: dict[str, FieldSpec]) -> type:
        return float


class _Lag(AggOp):
    op_type = "lag"
    supports_retraction = False
    requires_window = False

    def __init__(self, field: str, n: int) -> None:
        if not isinstance(field, str) or not field:
            raise TypeError("lag(field, n) requires a non-empty field name")
        if not isinstance(n, int) or n <= 0:
            raise ValueError(f"lag: n must be a positive int; got {n!r}")
        self.field = field
        self.n = n

    def output_type_for(self, schema: dict[str, FieldSpec]) -> type:
        spec = schema.get(self.field) if self.field else None
        return spec.py_type if spec else object


# ---------------------------------------------------------------------------
# Public lowercase aliases (tl.count, tl.sum, …)
# ---------------------------------------------------------------------------


count = _Count
sum = _Sum  # shadows builtins.sum at module scope — accessed as tl.sum
avg = _Avg
min = _Min  # shadows builtins.min — accessed as tl.min
max = _Max  # shadows builtins.max — accessed as tl.max
variance = _Variance
stddev = _Stddev
percentile = _Percentile
count_distinct = _CountDistinct
top_k = _TopK
first = _First
last = _Last
first_n = _FirstN
last_n = _LastN
ema = _EMA
lag = _Lag


ALL_AGG_OPS: tuple[type[AggOp], ...] = (
    _Count, _Sum, _Avg, _Min, _Max, _Variance, _Stddev,
    _Percentile, _CountDistinct, _TopK,
    _First, _Last, _FirstN, _LastN, _EMA, _Lag,
)


__all__ = [
    "AggOp",
    "count", "sum", "avg", "min", "max", "variance", "stddev",
    "percentile", "count_distinct", "top_k",
    "first", "last", "first_n", "last_n", "ema", "lag",
    "ALL_AGG_OPS",
]
