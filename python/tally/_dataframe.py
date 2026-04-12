"""DataFrame-style API for Tally pipeline definitions.

Provides ``Stream``, ``Table``, ``GroupBy``, and ``JoinedTable`` classes
that build an internal DAG and compile to the same JSON registration format
that the ``@st.stream``/``@st.view`` decorator API produces.

Usage::

    import tally as st

    app = st.App("localhost:6400")
    raw = app.source("transactions_raw")
    enriched = raw.map(amount_usd=raw["amount"] * raw["fx_rate"])
    user_features = enriched.group_by("user_id").agg(
        tx_count_1h=st.count(window="1h"),
        tx_sum_1h=st.sum("amount_usd", window="1h"),
    )
    app.serve(user_features)
    app.register_all()
"""

from __future__ import annotations

from collections import OrderedDict
from typing import Any

from tally._expr import Column, EventProxy, Expr, Ref, _wrap
from tally._operators import Count, Derive, Lookup, OperatorBase


# ---------------------------------------------------------------------------
# Dataset base
# ---------------------------------------------------------------------------


class Dataset:
    """Base class for all pipeline datasets (Stream, Table, JoinedTable).

    Provides the ``_to_register_json()`` protocol that ``App.register()``
    and ``App.register_all()`` use.
    """

    _name: str

    def _to_register_json(self) -> dict:
        raise NotImplementedError

    def _collect_registrations(self) -> list[dict]:
        """Return all JSON registrations needed (self + transitive deps).

        Subclasses override to include upstream datasets.
        """
        return [self._to_register_json()]


# ---------------------------------------------------------------------------
# Stream (keyless event flow)
# ---------------------------------------------------------------------------


class Stream(Dataset):
    """A keyless event stream.

    Created via ``app.source(name)`` or ``stream.map()``/``stream.filter()``.

    Supports:
    - ``stream["col"]`` -- column reference (returns Column proxy)
    - ``stream.map(**kwargs)`` -- stateless column transforms
    - ``stream.filter(expr)`` -- filter events
    - ``stream.group_by("key").agg(...)`` -- aggregate into a Table
    """

    def __init__(
        self,
        name: str,
        *,
        parent: Stream | None = None,
        derives: dict[str, str] | None = None,
        filter_expr: str | None = None,
    ) -> None:
        self._name = name
        self._parent = parent
        self._derives = derives or {}
        self._filter_expr = filter_expr

    def __getitem__(self, col_name: str) -> Column:
        """Return a Column proxy for a field in this stream's events."""
        return Column(self, col_name)

    def map(self, **kwargs: Any) -> Stream:
        """Create a derived stream with computed columns.

        Each kwarg is ``new_col_name=expr``. Expressions can be Expr objects
        (from Column operator overloading) or raw strings.

        Returns a new Stream that depends on this one.
        """
        derives = {}
        for col_name, expr in kwargs.items():
            if isinstance(expr, Expr):
                derives[col_name] = expr.to_expr_string()
            elif isinstance(expr, str):
                derives[col_name] = expr
            else:
                raise TypeError(
                    f"map() value for {col_name!r} must be Expr or str, "
                    f"got {type(expr).__name__}"
                )
        child_name = f"{self._name}__mapped"
        return Stream(child_name, parent=self, derives=derives)

    def filter(self, expr: Expr | str) -> Stream:
        """Create a filtered stream.

        Returns a new Stream that depends on this one with a filter applied.
        """
        if isinstance(expr, Expr):
            filter_str = expr.to_expr_string()
        else:
            filter_str = expr
        child_name = f"{self._name}__filtered"
        return Stream(child_name, parent=self, filter_expr=filter_str)

    def group_by(self, key: str) -> GroupBy:
        """Group this stream by a key field, returning a GroupBy handle.

        Call ``.agg(...)`` on the result to produce a Table.
        """
        return GroupBy(source=self, key=key)

    def _to_register_json(self) -> dict:
        d: dict[str, Any] = {
            "name": self._name,
            "key_field": None,
            "features": [],
        }
        # Add derive features for map() transforms
        for feat_name, expr_str in self._derives.items():
            d["features"].append(Derive(expr_str).to_json(feat_name))
        if self._parent is not None:
            d["depends_on"] = [self._parent._name]
        if self._filter_expr is not None:
            d["filter"] = self._filter_expr
        return d

    def _collect_registrations(self) -> list[dict]:
        result = []
        if self._parent is not None:
            result.extend(self._parent._collect_registrations())
        result.append(self._to_register_json())
        return result

    def __repr__(self) -> str:
        return f"Stream({self._name!r})"


# ---------------------------------------------------------------------------
# GroupBy (intermediate)
# ---------------------------------------------------------------------------


class GroupBy:
    """Intermediate object from ``stream.group_by("key")``.

    Call ``.agg(**kwargs)`` to produce a Table.
    """

    def __init__(self, source: Stream | Table, key: str) -> None:
        self._source = source
        self._key = key

    def agg(self, **kwargs: OperatorBase) -> Table:
        """Aggregate the grouped stream/table into a new Table.

        Each kwarg is ``feature_name=operator`` where operator is an
        OperatorBase instance (e.g., ``st.count(window="1h")``).

        Returns a Table keyed by the group_by key.
        """
        # Auto-name: use the source stream name + key
        table_name = f"{self._source._name}_by_{self._key}"
        table = Table(table_name, key=self._key, source=self._source)
        for feat_name, op in kwargs.items():
            if not isinstance(op, OperatorBase):
                raise TypeError(
                    f"agg() value for {feat_name!r} must be an OperatorBase, "
                    f"got {type(op).__name__}"
                )
            table._features[feat_name] = op
        return table

    def __repr__(self) -> str:
        return f"GroupBy({self._source._name!r}, key={self._key!r})"


# ---------------------------------------------------------------------------
# Table (keyed dataset)
# ---------------------------------------------------------------------------


class Table(Dataset):
    """A keyed dataset (materialized entity state).

    Created via ``GroupBy.agg()`` or directly. Supports:
    - ``table["col"]`` -- Column proxy (event field or defined feature)
    - ``table["new_col"] = expr`` -- add derived feature
    - ``table.count(window=...)``, ``table["col"].sum(window=...)`` -- aggregation
    - ``table.join(other, on=, how=)`` -- join with another table
    - ``table.lookup(col, on=)`` -- cross-key lookup
    - ``table.event["field"]`` -- raw event field access
    """

    def __init__(
        self,
        name: str,
        *,
        key: str,
        source: Stream | Table | None = None,
        entity_ttl: str | None = None,
        history_ttl: str | None = None,
    ) -> None:
        self._name = name
        self._key = key
        self._source = source
        self._features: OrderedDict[str, OperatorBase] = OrderedDict()
        self._entity_ttl = entity_ttl
        self._history_ttl = history_ttl

    def __getitem__(self, col_name: str) -> Column:
        """Return a Column proxy for a feature or event field."""
        return Column(self, col_name)

    def __setitem__(self, name: str, value: OperatorBase | Expr) -> None:
        """Register a feature definition.

        If ``value`` is an Expr (from operator overloading), it is converted
        to a ``Derive`` operator. If it is an OperatorBase, it is stored directly.
        """
        if isinstance(value, Expr):
            self._features[name] = Derive(value.to_expr_string())
        elif isinstance(value, OperatorBase):
            self._features[name] = value
        else:
            raise TypeError(
                f"Cannot assign {type(value).__name__} as a feature; "
                f"expected Expr or OperatorBase"
            )

    @property
    def event(self) -> EventProxy:
        """Access raw event fields via ``table.event["field"]``."""
        return EventProxy(self)

    # --- Table-level aggregation shortcuts ---

    def count(self, *, window: str, where: str | None = None, **kwargs: Any) -> OperatorBase:
        """Count all events in a sliding window."""
        return Count(window=window, where=where, **kwargs)

    def filter(self, expr: Expr | str) -> Table:
        """Create a filtered variant of this table.

        Returns a new Table with the same key that depends on this table's
        source stream with an additional filter.
        """
        if isinstance(expr, Expr):
            filter_str = expr.to_expr_string()
        else:
            filter_str = expr
        filtered_name = f"{self._name}__filtered"
        # Create a filtered stream from the source, then a new table
        if self._source is not None:
            filtered_source = Stream(
                f"{self._source._name}__filtered",
                parent=self._source if isinstance(self._source, Stream) else None,
                filter_expr=filter_str,
            )
        else:
            filtered_source = Stream(
                f"{self._name}__source_filtered",
                filter_expr=filter_str,
            )
        return Table(filtered_name, key=self._key, source=filtered_source)

    def join(
        self,
        other: Table,
        on: str | None = None,
        how: str = "left",
    ) -> JoinedTable:
        """Join with another table.

        Args:
            other: The right-side table to join.
            on: Join key. Defaults to ``self._key`` for same-key joins.
                For cross-key joins, specify the foreign key field.
            how: Join type (``"left"``, ``"inner"``). Default ``"left"``.

        Returns a JoinedTable (view) that includes features from both tables.
        """
        join_key = on or self._key
        return JoinedTable(
            left=self,
            right=other,
            join_key=join_key,
            how=how,
        )

    def lookup(self, column: Column, on: str) -> OperatorBase:
        """Cross-key lookup of a feature from another table.

        Args:
            column: A Column reference on another table (e.g., ``merchants["cbacks"]``).
            on: The foreign key field in events (e.g., ``"merchant_id"``).

        Returns a Lookup operator.
        """
        target = f"{column.table._name}.{column.name}"
        return Lookup(target=target, on=on)

    def group_by(self, key: str) -> GroupBy:
        """Re-aggregate on a different key, producing a new Table."""
        return GroupBy(source=self, key=key)

    # --- Table-level DataFrame operations ---

    def select(self, columns: list[str]) -> Table:
        """Select only the specified features (projection)."""
        new = Table(f"{self._name}__select", key=self._key, source=self._source)
        for name in columns:
            if name in self._features:
                new._features[name] = self._features[name]
        return new

    def drop(self, columns: list[str]) -> Table:
        """Drop the specified features."""
        new = Table(f"{self._name}__drop", key=self._key, source=self._source)
        for name, op in self._features.items():
            if name not in columns:
                new._features[name] = op
        return new

    def rename(self, mapping: dict[str, str]) -> Table:
        """Rename features. mapping = {old_name: new_name}."""
        new = Table(f"{self._name}__rename", key=self._key, source=self._source)
        for name, op in self._features.items():
            new_name = mapping.get(name, name)
            new._features[new_name] = op
        return new

    def assign(self, **kwargs: Any) -> Table:
        """Add multiple derived features at once. Returns a new Table."""
        new = Table(f"{self._name}__assign", key=self._key, source=self._source)
        new._features = dict(self._features)
        for name, value in kwargs.items():
            from tally._expr import Expr, _wrap
            from tally._operators import Derive, OperatorBase
            if isinstance(value, Expr):
                new._features[name] = Derive(value.to_expr_string())
            elif isinstance(value, OperatorBase):
                new._features[name] = value
            else:
                new._features[name] = Derive(_wrap(value).to_expr_string())
        return new

    def _to_register_json(self) -> dict:
        d: dict[str, Any] = {
            "name": self._name,
            "key_field": self._key,
            "features": [
                op.to_json(feat_name)
                for feat_name, op in self._features.items()
            ],
        }
        if self._source is not None:
            d["depends_on"] = [self._source._name]
        if self._entity_ttl is not None:
            d["entity_ttl"] = self._entity_ttl
        if self._history_ttl is not None:
            d["history_ttl"] = self._history_ttl
        return d

    def _collect_registrations(self) -> list[dict]:
        result = []
        if self._source is not None:
            result.extend(self._source._collect_registrations())
        result.append(self._to_register_json())
        return result

    # Metadata for App compatibility
    @property
    def _tally_stream_name(self) -> str:
        return self._name

    def __repr__(self) -> str:
        return f"Table({self._name!r}, key={self._key!r})"


# ---------------------------------------------------------------------------
# JoinedTable (view from a join)
# ---------------------------------------------------------------------------


class JoinedTable(Dataset):
    """Result of a table join. Compiles to a view registration.

    Supports ``__getitem__``/``__setitem__`` like Table for adding
    derived features on the joined result. Features from the right table
    are automatically included as lookups.
    """

    def __init__(
        self,
        left: Table,
        right: Table,
        join_key: str,
        how: str = "left",
    ) -> None:
        self._left = left
        self._right = right
        self._join_key = join_key
        self._how = how
        self._name = f"{left._name}__{right._name}"
        self._extra_features: OrderedDict[str, OperatorBase] = OrderedDict()

    def __getitem__(self, col_name: str) -> Column:
        """Return a Column proxy for a feature in the joined view."""
        return Column(self, col_name)

    def __setitem__(self, name: str, value: OperatorBase | Expr) -> None:
        """Add a derived feature to the joined view."""
        if isinstance(value, Expr):
            self._extra_features[name] = Derive(value.to_expr_string())
        elif isinstance(value, OperatorBase):
            self._extra_features[name] = value
        else:
            raise TypeError(
                f"Cannot assign {type(value).__name__} as a feature; "
                f"expected Expr or OperatorBase"
            )

    @property
    def event(self) -> EventProxy:
        """Access raw event fields."""
        return EventProxy(self)

    def _to_register_json(self) -> dict:
        """Compile to a view registration.

        If left and right share the same key, this is a same-key view
        with derive features referencing both tables.

        If they differ, right-side features are compiled as lookups.
        """
        features: list[dict] = []

        same_key = (self._left._key == self._right._key == self._join_key)

        if same_key:
            # Same-key join: view references features from both tables
            # by their qualified names (TableName.feature)
            pass  # No automatic lookups needed for same-key views
        else:
            # Cross-key join: right-side features become lookups
            for feat_name, op in self._right._features.items():
                lookup_target = f"{self._right._name}.{feat_name}"
                features.append(
                    Lookup(target=lookup_target, on=self._join_key).to_json(feat_name)
                )

        # Add any extra derived features
        for feat_name, op in self._extra_features.items():
            features.append(op.to_json(feat_name))

        d: dict[str, Any] = {
            "name": self._name,
            "key_field": self._left._key,
            "features": features,
            "type": "view",
        }
        return d

    def _collect_registrations(self) -> list[dict]:
        result = []
        result.extend(self._left._collect_registrations())
        result.extend(self._right._collect_registrations())
        result.append(self._to_register_json())
        return result

    # Metadata for App compatibility
    @property
    def _tally_stream_name(self) -> str:
        return self._name

    def __repr__(self) -> str:
        return (
            f"JoinedTable({self._left._name!r} JOIN {self._right._name!r} "
            f"ON {self._join_key!r})"
        )
