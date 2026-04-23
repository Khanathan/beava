"""Aggregation helpers and GroupBy builder for the Beava Python SDK.

Requirements: SDK-AGG-01, SDK-AGG-02, SDK-AGG-03, SDK-AGG-04, SDK-AGG-05, SDK-AGG-06

Public API (re-exported from beava.__init__):
  - AggDescriptor: frozen dataclass returned by every helper
  - GroupBy: builder returned by EventSource/EventDerivation.group_by()
  - count, sum, avg, min, max, variance, stddev, ratio: module-level helpers

NOTE: ``sum``, ``min``, and ``max`` shadow Python builtins at module scope.
This is intentional — the beava namespace is a DSL, not a stdlib replacement.
Users who need the builtins should access them via ``builtins.sum`` etc.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    pass

__all__ = [
    "AggDescriptor",
    "GroupBy",
    "count",
    "sum",
    "avg",
    "min",
    "max",
    "variance",
    "stddev",
    "ratio",
]


@dataclass(frozen=True)
class AggDescriptor:
    """Returned by bv.count() / bv.sum() / ... / bv.ratio().

    Consumed by GroupBy.agg() to build REGISTER JSON.

    Attributes:
        op:     Operator name: one of count|sum|avg|min|max|variance|stddev|ratio
        field:  Column name (None for count/ratio which don't require a field)
        window: Duration string e.g. "5m", "1h", "forever", or None (lifetime)
        where:  Serialized predicate string (from _ExprAST.to_expr_string()), or None
    """

    op: str
    field: str | None = None
    window: str | None = None
    where: str | None = None

    def to_agg_spec(self) -> dict[str, Any]:
        """Return {'op': <op>, 'params': {'field'?: ..., 'window'?: ..., 'where'?: ...}}."""
        raise NotImplementedError


def count(*, window: str | None = None, where: Any = None) -> AggDescriptor:
    """Count of events in the window (or lifetime if window=None). SDK-AGG-06."""
    raise NotImplementedError


def sum(field: str, *, window: str, where: Any = None) -> AggDescriptor:  # noqa: A001
    """Sum of *field* over *window*. window is required. SDK-AGG-06."""
    raise NotImplementedError


def avg(field: str, *, window: str, where: Any = None) -> AggDescriptor:
    """Arithmetic mean of *field* over *window*. window is required. SDK-AGG-06."""
    raise NotImplementedError


def min(field: str, *, window: str, where: Any = None) -> AggDescriptor:  # noqa: A001
    """Minimum of *field* over *window*. window is required. SDK-AGG-06."""
    raise NotImplementedError


def max(field: str, *, window: str, where: Any = None) -> AggDescriptor:  # noqa: A001
    """Maximum of *field* over *window*. window is required. SDK-AGG-06."""
    raise NotImplementedError


def variance(field: str, *, window: str, where: Any = None) -> AggDescriptor:
    """Sample variance of *field* over *window*. window is required. SDK-AGG-06."""
    raise NotImplementedError


def stddev(field: str, *, window: str, where: Any = None) -> AggDescriptor:
    """Sample standard deviation of *field* over *window*. window is required. SDK-AGG-06."""
    raise NotImplementedError


def ratio(*, window: str | None = None, where: Any = None) -> AggDescriptor:
    """Ratio of where-matching events / total events. window optional. SDK-AGG-06."""
    raise NotImplementedError


class GroupBy:
    """Returned by EventSource/EventDerivation.group_by(*keys).

    Call .agg(**named_features) to produce a TableDerivation.
    """

    def __init__(self, upstream: Any, keys: list[str]) -> None:
        self._upstream = upstream
        self._keys = keys

    def agg(self, **named_features: AggDescriptor) -> Any:
        """Build a TableDerivation from named aggregation descriptors."""
        raise NotImplementedError
