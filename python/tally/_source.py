"""@tl.source decorator for declaring event sources in the v2.0 API.

A source is a keyless event stream -- events flow in but no aggregation
is performed. Sources serve as inputs to ``@tl.dataset`` pipelines.

Usage::

    import tally as tl
    from tally._schema import EventSet, Field
    from tally._source import source

    class TxnEvent(EventSet):
        user_id: str = Field()
        amount: float = Field()

    @source
    class Transactions:
        event = TxnEvent

    # Or with options:
    @source(entity_ttl="5m", history_ttl="72h")
    class Transactions:
        event = TxnEvent
"""

from __future__ import annotations

from tally._schema import EventSet


class SourceDef:
    """A source definition produced by the ``@source`` decorator.

    Compiles to a keyless stream registration (key_field=None, features=[]).
    Compatible with ``App.register()`` and ``App.register_all()``.
    """

    def __init__(
        self,
        name: str,
        event_schema: type | None = None,
        entity_ttl: str | None = None,
        history_ttl: str | None = None,
    ) -> None:
        self._name = name
        self._event_schema = event_schema
        self._entity_ttl = entity_ttl
        self._history_ttl = history_ttl

    @property
    def _tally_stream_name(self) -> str:
        """Compatibility with App.register() which checks for this attribute."""
        return self._name

    def _compile(self) -> dict:
        """Compile to a RegisterRequest JSON dict."""
        d: dict = {
            "name": self._name,
            "key_field": None,
            "features": [],
        }
        if self._entity_ttl is not None:
            d["entity_ttl"] = self._entity_ttl
        if self._history_ttl is not None:
            d["history_ttl"] = self._history_ttl
        return d

    def _to_register_json(self) -> dict:
        """App.register() compatibility."""
        return self._compile()

    def _collect_registrations(self) -> list[dict]:
        """App.register_all() compatibility."""
        return [self._compile()]

    def __repr__(self) -> str:
        return f"SourceDef({self._name!r})"


def source(cls=None, *, entity_ttl: str | None = None, history_ttl: str | None = None):
    """Decorator that creates a SourceDef from a class.

    Supports both bare and parameterized usage::

        @source
        class Transactions: ...

        @source(entity_ttl="5m")
        class Transactions: ...
    """

    def _wrap(cls: type) -> SourceDef:
        # Extract event schema if present
        event_schema = None
        event_attr = getattr(cls, "event", None)
        if event_attr is not None and isinstance(event_attr, type) and issubclass(event_attr, EventSet):
            event_schema = event_attr

        return SourceDef(
            name=cls.__name__,
            event_schema=event_schema,
            entity_ttl=entity_ttl,
            history_ttl=history_ttl,
        )

    if cls is not None:
        # Called as @source (no parentheses)
        return _wrap(cls)

    # Called as @source(...) (with parentheses)
    return _wrap
