"""Operator descriptor classes for the Beava declarative DSL.

Each operator stores its configuration and serializes to a JSON dict matching
the Rust ``FeatureDefRequest`` schema (see ``src/server/protocol.rs``).

Public API (re-exported from ``beava.__init__`` as lowercase aliases)::

    import beava as st
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


class Stddev(OperatorBase):
    """Standard deviation of a numeric field in a sliding window.

    Args:
        field: Name of the event field. Required (positional).
        window: Duration string. Required.
        optional: If True, missing field values are skipped.
        where: Optional filter expression.
        bucket: Optional bucket granularity.
        backfill: If True, replay from event log on registration.
    """

    def __init__(self, field: str, *, window: str, optional: bool = False, where: str | None = None, bucket: str | None = None, backfill: bool = False) -> None:
        self.field = field
        self.window = window
        self.optional = optional
        self.where_clause = where
        self.bucket = bucket
        self.backfill = backfill

    def to_json(self, name: str) -> dict:
        d: dict = {"name": name, "type": "stddev", "field": self.field, "window": self.window}
        if self.optional:
            d["optional"] = True
        if self.where_clause is not None:
            d["where"] = self.where_clause
        if self.bucket is not None:
            d["bucket"] = self.bucket
        if self.backfill:
            d["backfill"] = True
        return d


class Percentile(OperatorBase):
    """Percentile of a numeric field in a sliding window.

    Args:
        field: Name of the event field. Required (positional).
        quantile: Quantile value between 0.0 and 1.0 (e.g. 0.95 for p95). Required.
        window: Duration string. Required.
        optional: If True, missing field values are skipped.
        where: Optional filter expression.
        bucket: Optional bucket granularity.
        backfill: If True, replay from event log on registration.
    """

    def __init__(self, field: str, quantile: float, *, window: str, optional: bool = False, where: str | None = None, bucket: str | None = None, backfill: bool = False) -> None:
        self.field = field
        self.quantile = quantile
        self.window = window
        self.optional = optional
        self.where_clause = where
        self.bucket = bucket
        self.backfill = backfill

    def to_json(self, name: str) -> dict:
        d: dict = {"name": name, "type": "percentile", "field": self.field, "quantile": self.quantile, "window": self.window}
        if self.optional:
            d["optional"] = True
        if self.where_clause is not None:
            d["where"] = self.where_clause
        if self.bucket is not None:
            d["bucket"] = self.bucket
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


class Lag(OperatorBase):
    """Return the value from N events ago (event-count-based, no window).

    Args:
        field: Name of the event field. Required (positional).
        n: Number of events to lag by. Required.
        optional: If True, missing field values are skipped.
        backfill: If True, replay from event log on registration.
    """

    def __init__(self, field: str, *, n: int, optional: bool = False, backfill: bool = False) -> None:
        self.field = field
        self.n = n
        self.optional = optional
        self.backfill = backfill

    def to_json(self, name: str) -> dict:
        d: dict = {"name": name, "type": "lag", "field": self.field, "n": self.n}
        if self.optional:
            d["optional"] = True
        if self.backfill:
            d["backfill"] = True
        return d


class Ema(OperatorBase):
    """Exponential moving average with time-based decay (no window).

    Args:
        field: Name of the numeric event field. Required (positional).
        half_life: Duration string for the EMA half-life (e.g. "30m", "1h"). Required.
        optional: If True, missing field values are skipped.
        backfill: If True, replay from event log on registration.
    """

    def __init__(self, field: str, *, half_life: str, optional: bool = False, backfill: bool = False) -> None:
        self.field = field
        self.half_life = half_life
        self.optional = optional
        self.backfill = backfill

    def to_json(self, name: str) -> dict:
        d: dict = {"name": name, "type": "ema", "field": self.field, "half_life": self.half_life}
        if self.optional:
            d["optional"] = True
        if self.backfill:
            d["backfill"] = True
        return d


class LastN(OperatorBase):
    """Store the last N values of a field, returned as a JSON array string.

    Args:
        field: Name of the event field. Required (positional).
        n: Number of recent values to keep. Required.
        optional: If True, missing field values are skipped.
        backfill: If True, replay from event log on registration.
    """

    def __init__(self, field: str, *, n: int, optional: bool = False, backfill: bool = False) -> None:
        self.field = field
        self.n = n
        self.optional = optional
        self.backfill = backfill

    def to_json(self, name: str) -> dict:
        d: dict = {"name": name, "type": "last_n", "field": self.field, "n": self.n}
        if self.optional:
            d["optional"] = True
        if self.backfill:
            d["backfill"] = True
        return d


class First(OperatorBase):
    """Store the first value ever seen for an entity key (no window, never overwrites).

    Args:
        field: Name of the event field. Required (positional).
        optional: If True, missing field on first event is skipped.
        backfill: If True, replay from event log on registration.
    """

    def __init__(self, field: str, *, optional: bool = False, backfill: bool = False) -> None:
        self.field = field
        self.optional = optional
        self.backfill = backfill

    def to_json(self, name: str) -> dict:
        d: dict = {"name": name, "type": "first", "field": self.field}
        if self.optional:
            d["optional"] = True
        if self.backfill:
            d["backfill"] = True
        return d


class ExactMin(OperatorBase):
    """Exact retractable minimum in a sliding window (BTreeMap-based).

    Args:
        field: Name of the numeric event field. Required (positional).
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
        d: dict = {"name": name, "type": "exact_min", "field": self.field, "window": self.window}
        if self.bucket is not None:
            d["bucket"] = self.bucket
        if self.backfill:
            d["backfill"] = True
        return d


class ExactMax(OperatorBase):
    """Exact retractable maximum in a sliding window (BTreeMap-based).

    Args:
        field: Name of the numeric event field. Required (positional).
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
        d: dict = {"name": name, "type": "exact_max", "field": self.field, "window": self.window}
        if self.bucket is not None:
            d["bucket"] = self.bucket
        if self.backfill:
            d["backfill"] = True
        return d


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
