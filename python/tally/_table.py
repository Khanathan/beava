"""``@tl.table`` decorator + Table / TableSource runtime types.

Class-form only in Plan 21-01. ``mode="changelog"`` is reserved for v0.1
and raises ``NotImplementedError``. Function-form Tables ship in Plan 21-02.
"""

from __future__ import annotations

from typing import Any

from tally._describe import format_describe
from tally._schema_v0 import extract_schema, schema_mismatch_error
from tally._types_core import FieldSpec


class Table:
    """Marker / runtime type for tabular inputs.

    Both :class:`TableSource` (external, via ``@tl.table class``) and future
    ``TableDerivation`` (via ``@tl.table def`` in Plan 21-02) subclass Table.
    """


class TableSource(Table):
    """An external table source (CDC-style ingest).

    ``key`` is always normalised to ``list[str]`` (composite or single).
    ``mode`` is ``"append"`` in v0; ``"changelog"`` ships in v0.1.
    """

    def __init__(
        self,
        name: str,
        schema: dict[str, FieldSpec],
        key: list[str],
        *,
        mode: str = "append",
        ttl: str | None = None,
    ) -> None:
        self._name = name
        self._schema = schema
        self._key = key
        self._mode = mode
        self._ttl = ttl

    # --- public introspection ---
    def describe(self) -> dict[str, Any]:
        return format_describe(
            name=self._name,
            kind="table",
            key=list(self._key),
            mode=self._mode,
            schema=self._schema,
            ttl=self._ttl,
        )

    # --- App.register compat ---
    @property
    def _tally_stream_name(self) -> str:
        return self._name

    def _compile(self) -> dict[str, Any]:
        """Compile to a RegisterRequest JSON dict.

        Tables with a single key field write ``key_field`` as a string (matches
        the existing engine contract); composite keys use ``key_fields``.
        """
        d: dict[str, Any] = {
            "name": self._name,
            "features": [],
            "fields": {
                fname: {
                    "type": spec.py_type.__name__,
                    "optional": spec.optional,
                }
                for fname, spec in self._schema.items()
            },
            "mode": self._mode,
        }
        if len(self._key) == 1:
            d["key_field"] = self._key[0]
        else:
            d["key_field"] = None
            d["key_fields"] = list(self._key)
        if self._ttl is not None:
            d["entity_ttl"] = self._ttl
        return d

    def _to_register_json(self) -> dict[str, Any]:
        return self._compile()

    def _collect_registrations(self) -> list[dict[str, Any]]:
        return [self._compile()]

    def __repr__(self) -> str:
        return f"TableSource({self._name!r}, key={list(self._key)!r})"


def table(
    cls: type | None = None,
    *,
    key: str | list[str] | None = None,
    ttl: str | None = None,
    mode: str = "append",
):
    """Decorator that declares a Table.

    Usage::

        @tl.table(key="user_id")
        class Users:
            user_id: str
            name: str

        @tl.table(key=["user_id", "merchant_id"], ttl="30d")
        class UM:
            user_id: str
            merchant_id: str
            score: float
    """
    if key is None:
        raise TypeError("@tl.table requires key=... (str or list[str])")

    # Normalize key to list[str]
    if isinstance(key, str):
        key_list = [key]
    elif isinstance(key, (list, tuple)) and all(isinstance(k, str) for k in key):
        key_list = list(key)
    else:
        raise TypeError(
            f"@tl.table key must be str or list[str], got {type(key).__name__}"
        )
    if not key_list:
        raise TypeError("@tl.table key must not be empty")

    # Validate mode
    if mode == "changelog":
        raise NotImplementedError(
            "mode='changelog' ships in v0.1; use mode='append' (default)"
        )
    if mode not in ("append",):
        raise ValueError(
            f"@tl.table mode must be 'append' or 'changelog', got {mode!r}"
        )

    def _wrap(target: Any) -> TableSource:
        if not isinstance(target, type):
            raise NotImplementedError(
                "@tl.table function form ships in Plan 21-02; "
                "use @tl.table class form until then"
            )
        schema = extract_schema(target)

        # Every key field must be declared in the schema.
        for k in key_list:
            if k not in schema:
                raise TypeError(
                    schema_mismatch_error(k, schema, f"{target.__name__} schema")
                )

        return TableSource(
            name=target.__name__,
            schema=schema,
            key=key_list,
            mode=mode,
            ttl=ttl,
        )

    # Table always uses keyword arguments, so we never hit the bare form.
    # But support it defensively in case someone writes `@tl.table` with no parens.
    if cls is not None:
        return _wrap(cls)
    return _wrap


__all__ = ["table", "Table", "TableSource"]
