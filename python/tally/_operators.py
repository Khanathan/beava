"""Operator descriptor classes for the Tally declarative DSL.

Each operator stores its configuration and serializes to a JSON dict matching
the Rust ``FeatureDefRequest`` schema (see ``src/server/protocol.rs``).

Public API (re-exported from ``tally.__init__`` as lowercase aliases)::

    import tally as st
    st.count(window="30m")
    st.sum("amount", window="1h")
    st.derive("failed / total")
"""

from __future__ import annotations


class OperatorBase:
    """Base class for all operator descriptors.

    Subclasses must implement ``to_json(name) -> dict``.
    """

    def to_json(self, name: str) -> dict:
        raise NotImplementedError


class Count(OperatorBase):
    """Count events in a sliding window.

    Args:
        window: Duration string (e.g. "30m", "1h", "24h"). Required.
        where: Optional filter expression (e.g. "status == 'failed'").
        bucket: Optional bucket granularity (e.g. "1m").
        backfill: If True, replay from event log on registration.
    """

    def __init__(self, *, window: str, where: str | None = None, bucket: str | None = None, backfill: bool = False) -> None:
        self.window = window
        self.where_clause = where
        self.bucket = bucket
        self.backfill = backfill

    def to_json(self, name: str) -> dict:
        d: dict = {"name": name, "type": "count", "window": self.window}
        if self.where_clause is not None:
            d["where"] = self.where_clause
        if self.bucket is not None:
            d["bucket"] = self.bucket
        if self.backfill:
            d["backfill"] = True
        return d


class Sum(OperatorBase):
    """Sum a numeric field in a sliding window.

    Args:
        field: Name of the event field to sum. Required (positional).
        window: Duration string. Required.
        optional: If True, missing field values are skipped instead of erroring.
        bucket: Optional bucket granularity.
        backfill: If True, replay from event log on registration.
    """

    def __init__(self, field: str, *, window: str, optional: bool = False, bucket: str | None = None, backfill: bool = False) -> None:
        self.field = field
        self.window = window
        self.optional = optional
        self.bucket = bucket
        self.backfill = backfill

    def to_json(self, name: str) -> dict:
        d: dict = {"name": name, "type": "sum", "field": self.field, "window": self.window}
        if self.optional:
            d["optional"] = True
        if self.bucket is not None:
            d["bucket"] = self.bucket
        if self.backfill:
            d["backfill"] = True
        return d


class Avg(OperatorBase):
    """Average a numeric field in a sliding window.

    Args:
        field: Name of the event field. Required (positional).
        window: Duration string. Required.
        optional: If True, missing field values are skipped.
        bucket: Optional bucket granularity.
        backfill: If True, replay from event log on registration.
    """

    def __init__(self, field: str, *, window: str, optional: bool = False, bucket: str | None = None, backfill: bool = False) -> None:
        self.field = field
        self.window = window
        self.optional = optional
        self.bucket = bucket
        self.backfill = backfill

    def to_json(self, name: str) -> dict:
        d: dict = {"name": name, "type": "avg", "field": self.field, "window": self.window}
        if self.optional:
            d["optional"] = True
        if self.bucket is not None:
            d["bucket"] = self.bucket
        if self.backfill:
            d["backfill"] = True
        return d


class Min(OperatorBase):
    """Minimum value of a field in a sliding window.

    Args:
        field: Name of the event field. Required (positional).
        window: Duration string. Required.
        bucket: Optional bucket granularity.
        backfill: If True, replay from event log on registration.
    """

    def __init__(self, field: str, *, window: str, bucket: str | None = None, backfill: bool = False) -> None:
        self.field = field
        self.window = window
        self.bucket = bucket
        self.backfill = backfill

    def to_json(self, name: str) -> dict:
        d: dict = {"name": name, "type": "min", "field": self.field, "window": self.window}
        if self.bucket is not None:
            d["bucket"] = self.bucket
        if self.backfill:
            d["backfill"] = True
        return d


class Max(OperatorBase):
    """Maximum value of a field in a sliding window.

    Args:
        field: Name of the event field. Required (positional).
        window: Duration string. Required.
        bucket: Optional bucket granularity.
        backfill: If True, replay from event log on registration.
    """

    def __init__(self, field: str, *, window: str, bucket: str | None = None, backfill: bool = False) -> None:
        self.field = field
        self.window = window
        self.bucket = bucket
        self.backfill = backfill

    def to_json(self, name: str) -> dict:
        d: dict = {"name": name, "type": "max", "field": self.field, "window": self.window}
        if self.bucket is not None:
            d["bucket"] = self.bucket
        if self.backfill:
            d["backfill"] = True
        return d


class DistinctCount(OperatorBase):
    """Approximate unique count of a field in a sliding window (HyperLogLog).

    Args:
        field: Name of the event field. Required (positional).
        window: Duration string. Required.
        bucket: Optional bucket granularity.
        backfill: If True, replay from event log on registration.
    """

    def __init__(self, field: str, *, window: str, bucket: str | None = None, backfill: bool = False) -> None:
        self.field = field
        self.window = window
        self.bucket = bucket
        self.backfill = backfill

    def to_json(self, name: str) -> dict:
        d: dict = {"name": name, "type": "distinct_count", "field": self.field, "window": self.window}
        if self.bucket is not None:
            d["bucket"] = self.bucket
        if self.backfill:
            d["backfill"] = True
        return d


class Last(OperatorBase):
    """Most recent value of a field (no window).

    Args:
        field: Name of the event field. Required (positional).
        backfill: If True, replay from event log on registration.
    """

    def __init__(self, field: str, *, backfill: bool = False) -> None:
        self.field = field
        self.backfill = backfill

    def to_json(self, name: str) -> dict:
        d: dict = {"name": name, "type": "last", "field": self.field}
        if self.backfill:
            d["backfill"] = True
        return d


class Derive(OperatorBase):
    """Expression computed over other features (evaluated on read, no state).

    Args:
        expr: Expression string (e.g. "failed_tx_30m / tx_count_30m"). Required (positional).
    """

    def __init__(self, expr: str) -> None:
        self.expr = expr

    def to_json(self, name: str) -> dict:
        return {"name": name, "type": "derive", "expr": self.expr}


class Lookup(OperatorBase):
    """Cross-key feature reference (looks up a feature from another stream's key).

    Args:
        target: Reference to a feature on another stream, as a string
            like "MerchantActivity.chargeback_count_24h". Required (positional).
        on: The key field name to use for the lookup (e.g. "merchant_id"). Required.
    """

    def __init__(self, target: str, *, on: str) -> None:
        self.target = target
        self.on = on

    def to_json(self, name: str) -> dict:
        return {"name": name, "type": "lookup", "target": self.target, "on": self.on}
