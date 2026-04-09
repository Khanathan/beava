"""@stream decorator and StreamMeta metaclass for declarative stream definitions.

Usage::

    import tally as st

    @st.stream(key="user_id")
    class Transactions:
        tx_count_30m = st.count(window="30m")
        tx_sum_1h    = st.sum("amount", window="1h")
        rate         = st.derive("tx_sum_1h / tx_count_30m")

The metaclass collects OperatorBase descriptors from the class body and
all base classes (mixins), validates constraints, and attaches metadata
for later serialization to the Rust RegisterRequest JSON format.
"""

from __future__ import annotations

from tally._operators import Derive, Lookup, OperatorBase


class StreamMeta(type):
    """Metaclass that collects operator descriptors from the class body and bases.

    Sets:
        cls._tally_features:    dict[str, OperatorBase] — collected features
        cls._tally_key_field:   str — the entity key field name
        cls._tally_stream_name: str — the class (stream) name
        cls._tally_is_view:     bool — True for @view, False for @stream
    """

    def __new__(
        mcs,
        name: str,
        bases: tuple[type, ...],
        namespace: dict,
        *,
        key: str | None = None,
        _is_view: bool = False,
        entity_ttl: str | None = None,
        history_ttl: str | None = None,
    ) -> StreamMeta:
        # Collect operator descriptors from all bases (mixin support).
        # Walk bases in reverse order so later bases override earlier ones,
        # consistent with Python MRO expectations for multiple inheritance.
        features: dict[str, OperatorBase] = {}
        for base in reversed(bases):
            for attr_name, attr_val in vars(base).items():
                if isinstance(attr_val, OperatorBase):
                    features[attr_name] = attr_val

        # Class body overrides anything from bases.
        for attr_name, attr_val in namespace.items():
            if isinstance(attr_val, OperatorBase):
                features[attr_name] = attr_val

        # Validate view restriction: views cannot have TTL fields.
        if _is_view and (entity_ttl is not None or history_ttl is not None):
            raise TypeError(
                f"view '{name}' cannot have entity_ttl or history_ttl; "
                "views have no state to evict"
            )

        # Validate view restriction: only Derive and Lookup allowed.
        if _is_view:
            for feat_name, feat_op in features.items():
                if not isinstance(feat_op, (Derive, Lookup)):
                    raise TypeError(
                        f"view '{name}' feature '{feat_name}' is a "
                        f"{type(feat_op).__name__}; views only allow derive and lookup operators"
                    )

        cls = super().__new__(mcs, name, bases, namespace)
        cls._tally_features = features
        cls._tally_key_field = key
        cls._tally_stream_name = name
        cls._tally_is_view = _is_view
        cls._tally_entity_ttl = entity_ttl
        cls._tally_history_ttl = history_ttl
        return cls

    def _to_register_json(cls) -> dict:
        """Build the RegisterRequest dict matching the Rust DTO schema.

        Returns::

            {
                "name": "Transactions",
                "key_field": "user_id",
                "features": [
                    {"name": "tx_count", "type": "count", "window": "30m"},
                    ...
                ]
            }
        """
        d = {
            "name": cls._tally_stream_name,
            "key_field": cls._tally_key_field,
            "features": [
                op.to_json(feat_name)
                for feat_name, op in cls._tally_features.items()
            ],
        }
        if cls._tally_is_view:
            d["type"] = "view"
        if cls._tally_entity_ttl is not None:
            d["entity_ttl"] = cls._tally_entity_ttl
        if cls._tally_history_ttl is not None:
            d["history_ttl"] = cls._tally_history_ttl
        return d


def stream(
    *,
    key: str,
    entity_ttl: str | None = None,
    history_ttl: str | None = None,
):
    """Decorator that creates a stream class with the given key field.

    Usage::

        @stream(key="user_id", entity_ttl="5m", history_ttl="72h")
        class Transactions:
            tx_count = Count(window="30m")
    """

    def decorator(cls: type) -> StreamMeta:
        # Collect the class body namespace (exclude dunder attributes).
        namespace = {
            k: v for k, v in cls.__dict__.items() if not k.startswith("__")
        }
        return StreamMeta(
            cls.__name__,
            cls.__bases__,
            namespace,
            key=key,
            entity_ttl=entity_ttl,
            history_ttl=history_ttl,
        )

    return decorator
