"""@tl.dataset decorator, group_by, union, and related types for the v2.0 API.

Provides the core pipeline definition pattern::

    import tally as tl
    from tally._source import source
    from tally._dataset import dataset, group_by, union

    @source
    class RawTxns:
        pass

    @dataset(depends_on=[RawTxns])
    class UserTxns:
        features = group_by("user_id").agg(
            tx_count=tl.count(window="1h"),
            tx_sum=tl.sum("amount", window="1h"),
        )
        failure_rate = tl.derive("failed / total")
"""

from __future__ import annotations

from tally._operators import OperatorBase


class GroupedDataset:
    """Intermediate result of ``group_by("key").agg(...)``.

    Holds the aggregation key and feature operator definitions.
    Not directly registered -- consumed by ``@dataset`` to build a DatasetDef.
    """

    def __init__(self, key: str, features: dict[str, OperatorBase] | None = None) -> None:
        self._key = key
        self._features: dict[str, OperatorBase] = features or {}

    def agg(self, **kwargs: OperatorBase) -> GroupedDataset:
        """Define aggregation features.

        Each kwarg is ``feature_name=operator_instance``.
        Returns a new GroupedDataset with the features set.
        """
        for name, op in kwargs.items():
            if not isinstance(op, OperatorBase):
                raise TypeError(
                    f"agg() value for {name!r} must be an OperatorBase, "
                    f"got {type(op).__name__}"
                )
        new_features = dict(self._features)
        new_features.update(kwargs)
        return GroupedDataset(key=self._key, features=new_features)

    def __repr__(self) -> str:
        return f"GroupedDataset(key={self._key!r}, features={list(self._features)})"


def group_by(key: str) -> GroupedDataset:
    """Create a GroupedDataset with the given key.

    Used inside a ``@dataset`` class body::

        @dataset(depends_on=[source])
        class MyDataset:
            features = group_by("user_id").agg(tx_count=tl.count(window="1h"))
    """
    return GroupedDataset(key=key)


class UnionSource:
    """Represents the union of multiple sources.

    Created by ``tl.union(source_a, source_b)``. When used in
    ``depends_on``, flattens to a list of source names.
    """

    def __init__(self, *sources: object) -> None:
        self._sources = list(sources)

    def _get_depends_on_names(self) -> list[str]:
        """Resolve each source to its name string."""
        names = []
        for src in self._sources:
            if hasattr(src, "_name"):
                names.append(src._name)
            else:
                names.append(str(src))
        return names

    def __repr__(self) -> str:
        return f"UnionSource({self._get_depends_on_names()})"


def union(*sources: object) -> UnionSource:
    """Union multiple sources together.

    The result can be passed in ``depends_on`` of a ``@dataset``::

        @dataset(depends_on=[union(source_a, source_b)])
        class Combined:
            features = group_by("key").agg(...)
    """
    return UnionSource(*sources)


class DatasetDef:
    """A dataset definition produced by the ``@dataset`` decorator.

    Compiles to a keyed stream registration with features from
    ``group_by().agg()`` plus any additional derive operators.
    Compatible with ``App.register()`` and ``App.register_all()``.
    """

    def __init__(
        self,
        name: str,
        depends_on: list,
        grouped_dataset: GroupedDataset | None,
        extra_features: dict[str, OperatorBase] | None = None,
        event_schema: type | None = None,
        entity_ttl: str | None = None,
        history_ttl: str | None = None,
    ) -> None:
        self._name = name
        self._depends_on = depends_on
        self._grouped_dataset = grouped_dataset
        self._extra_features = extra_features or {}
        self._event_schema = event_schema
        self._entity_ttl = entity_ttl
        self._history_ttl = history_ttl

    @property
    def _tally_stream_name(self) -> str:
        """Compatibility with App.register()."""
        return self._name

    def _resolve_depends_on(self) -> list[str]:
        """Resolve depends_on list to string names, flattening UnionSource."""
        names: list[str] = []
        for dep in self._depends_on:
            if isinstance(dep, UnionSource):
                names.extend(dep._get_depends_on_names())
            elif hasattr(dep, "_name"):
                names.append(dep._name)
            else:
                names.append(str(dep))
        return names

    def _compile(self) -> dict:
        """Compile to a RegisterRequest JSON dict."""
        # Build features list from grouped dataset + extra derives
        features: list[dict] = []
        if self._grouped_dataset is not None:
            for feat_name, op in self._grouped_dataset._features.items():
                features.append(op.to_json(feat_name))

        for feat_name, op in self._extra_features.items():
            features.append(op.to_json(feat_name))

        key_field = self._grouped_dataset._key if self._grouped_dataset else None

        d: dict = {
            "name": self._name,
            "key_field": key_field,
            "features": features,
        }

        depends_on_names = self._resolve_depends_on()
        if depends_on_names:
            d["depends_on"] = depends_on_names

        if self._entity_ttl is not None:
            d["entity_ttl"] = self._entity_ttl
        if self._history_ttl is not None:
            d["history_ttl"] = self._history_ttl

        return d

    def _to_register_json(self) -> dict:
        """App.register() compatibility."""
        return self._compile()

    def _collect_registrations(self) -> list[dict]:
        """Collect registrations for self and all upstream dependencies.

        Walks depends_on, collecting from each SourceDef/DatasetDef,
        deduplicating by name, and appending self last.
        """
        seen: set[str] = set()
        result: list[dict] = []

        for dep in self._depends_on:
            if isinstance(dep, UnionSource):
                for src in dep._sources:
                    if hasattr(src, "_collect_registrations"):
                        for reg in src._collect_registrations():
                            if reg["name"] not in seen:
                                seen.add(reg["name"])
                                result.append(reg)
            elif hasattr(dep, "_collect_registrations"):
                for reg in dep._collect_registrations():
                    if reg["name"] not in seen:
                        seen.add(reg["name"])
                        result.append(reg)

        # Append self
        result.append(self._compile())
        return result

    def __repr__(self) -> str:
        return f"DatasetDef({self._name!r})"


def dataset(
    *,
    depends_on: list,
    entity_ttl: str | None = None,
    history_ttl: str | None = None,
):
    """Decorator that creates a DatasetDef from a class.

    Usage::

        @dataset(depends_on=[raw_source])
        class UserTxns:
            features = group_by("user_id").agg(
                tx_count=tl.count(window="1h"),
            )
            failure_rate = tl.derive("failed / total")
    """

    def decorator(cls: type) -> DatasetDef:
        # Extract grouped dataset from 'features' attribute
        grouped = getattr(cls, "features", None)
        if grouped is not None and not isinstance(grouped, GroupedDataset):
            grouped = None

        # Extract event schema from 'event' attribute
        event_schema = None
        event_attr = getattr(cls, "event", None)
        if event_attr is not None and isinstance(event_attr, type):
            from tally._schema import EventSet
            if issubclass(event_attr, EventSet):
                event_schema = event_attr

        # Scan class body for additional OperatorBase instances (e.g. derive features)
        extra_features: dict[str, OperatorBase] = {}
        for attr_name, attr_val in cls.__dict__.items():
            if attr_name.startswith("_"):
                continue
            if attr_name == "features":
                continue
            if attr_name == "event":
                continue
            if isinstance(attr_val, OperatorBase):
                extra_features[attr_name] = attr_val

        return DatasetDef(
            name=cls.__name__,
            depends_on=depends_on,
            grouped_dataset=grouped,
            extra_features=extra_features,
            event_schema=event_schema,
            entity_ttl=entity_ttl,
            history_ttl=history_ttl,
        )

    return decorator
