"""``@tl.stream`` decorator + Stream / StreamSource runtime types.

Class-form only in Plan 21-01. The function-form decorator (which captures
upstream dependencies via parameter annotations) ships in Plan 21-02 and
raises ``NotImplementedError`` here.
"""

from __future__ import annotations

from typing import Any

from tally._describe import format_describe
from tally._schema_v0 import extract_schema
from tally._types_core import FieldSpec


class Stream:
    """Marker / runtime type for streaming inputs.

    Both :class:`StreamSource` (external, declared via ``@tl.stream class``)
    and future ``StreamDerivation`` (declared via ``@tl.stream def`` in
    Plan 21-02) are subclasses. User-facing function signatures in later
    phases will use ``Stream`` for parameter / return annotations.
    """


class StreamSource(Stream):
    """An external stream source — the node the engine ingests events into.

    Carries the declared schema plus stream-level options (``history_ttl``
    for Phase 25 watermark/retention work). Exposes the App-register compat
    protocol (``_tally_stream_name``, ``_compile``, ``_to_register_json``,
    ``_collect_registrations``).
    """

    def __init__(
        self,
        name: str,
        schema: dict[str, FieldSpec],
        *,
        history_ttl: str | None = None,
    ) -> None:
        self._name = name
        self._schema = schema
        self._history_ttl = history_ttl

    # --- public introspection ---
    def describe(self) -> dict[str, Any]:
        return format_describe(
            name=self._name,
            kind="stream",
            key=None,
            mode=None,
            schema=self._schema,
            history_ttl=self._history_ttl,
        )

    # --- App.register compat ---
    @property
    def _tally_stream_name(self) -> str:
        return self._name

    def _compile(self) -> dict[str, Any]:
        """Compile to a RegisterRequest JSON dict (keyless stream source)."""
        d: dict[str, Any] = {
            "name": self._name,
            "key_field": None,
            "features": [],
            "fields": {
                fname: {
                    "type": spec.py_type.__name__,
                    "optional": spec.optional,
                }
                for fname, spec in self._schema.items()
            },
        }
        if self._history_ttl is not None:
            d["history_ttl"] = self._history_ttl
        return d

    def _to_register_json(self) -> dict[str, Any]:
        return self._compile()

    def _collect_registrations(self) -> list[dict[str, Any]]:
        return [self._compile()]

    def __repr__(self) -> str:
        return f"StreamSource({self._name!r})"


def stream(cls: type | None = None, *, history_ttl: str | None = None):
    """Decorator that declares a Stream.

    Class form (this plan)::

        @tl.stream
        class Clicks:
            user_id: str
            url: str

        @tl.stream(history_ttl="90d")
        class Logins:
            user_id: str

    Function form (Plan 21-02) — currently raises NotImplementedError.
    """

    def _wrap(target: Any) -> StreamSource:
        if not isinstance(target, type):
            raise NotImplementedError(
                "@tl.stream function form ships in Plan 21-02; "
                "use @tl.stream class form until then"
            )
        schema = extract_schema(target)
        return StreamSource(
            name=target.__name__,
            schema=schema,
            history_ttl=history_ttl,
        )

    if cls is not None:
        # Bare usage: @tl.stream class X: ...
        return _wrap(cls)

    # Parameterized usage: @tl.stream(history_ttl="90d")
    return _wrap


__all__ = ["stream", "Stream", "StreamSource"]
