"""``@tl.table`` decorator + Table / TableSource / TableDerivation runtime types.

Plan 21-01 shipped the class form. Plan 21-02 adds:
  * :class:`StatelessOpsMixin` on :class:`Table` — stateless ops on Tables.
  * :class:`TableDerivation` — returned by ops + function-form derivations.
  * Function-form ``@tl.table(key=...) def X(...) -> Table:`` — the function
    is invoked once at registration with upstream descriptors; the returned
    Table is renamed to the function name and its upstreams captured.
"""

from __future__ import annotations

import inspect
import typing
from types import FunctionType
from typing import Any

from tally._describe import format_describe
from tally._schema_v0 import extract_schema, schema_mismatch_error
from tally._stateless_ops import StatelessOpsMixin
from tally._types_core import FieldSpec


class Table(StatelessOpsMixin):
    """Marker / runtime type for tabular inputs.

    Both :class:`TableSource` and :class:`TableDerivation` subclass Table.
    The :class:`StatelessOpsMixin` provides the 8 per-row ops; Table's
    ``_derive`` cascades key-field renames through the derivation chain.
    """

    # Phase 24-02: Dispatch marker used by :meth:`tally.App.push` and
    # :meth:`tally.App.delete` to route into the OP_PUSH_TABLE /
    # OP_DELETE_TABLE wire opcodes. Every Table subclass inherits it.
    _tally_kind: str = "table"

    _key: list[str]

    def _derive(
        self,
        *,
        schema: dict[str, FieldSpec],
        op: dict[str, Any],
    ) -> "TableDerivation":
        return TableDerivation(
            name=self._name,
            schema=schema,
            key=list(self._key),
            mode=getattr(self, "_mode", "append"),
            ttl=getattr(self, "_ttl", None),
            ops=list(self._ops) + [op],
            upstream=self,
            upstreams=[self],
        )

    def group_by(self, *keys: str) -> Any:
        """Table aggregation is NOT supported in v0 — rejected at registration.

        Tables are current-state-only in v0. To aggregate related data,
        model it as a Stream source. Table aggregation ships in v0.1
        (requires retraction propagation — see the v0 restructure spec).
        """
        raise RuntimeError(
            f"Cannot aggregate over Table {self._name!r}. "
            f"Tables are current-state-only in v0; Table aggregation ships "
            f"in v0.1. To aggregate related data, model it as a Stream source."
        )

    def join(
        self,
        other: Any,
        *,
        on: "str | list[str]",
        type: str = "inner",
        within: str | None = None,
    ) -> "Table":
        """Join this Table with another Table (same full-key required in v0).

        Table↔Table joins do not support ``within=`` (no event-time window).
        For Stream↔Table enrichment, call ``.join`` from the Stream side.
        Execution lands in Phase 23.
        """
        if within is not None:
            raise TypeError(
                "Table↔Table join does not accept within=...; "
                "within is only valid for Stream↔Stream windowed joins"
            )
        # Dispatch based on other's type.
        from tally._join import table_join
        # Import Table lazily here to avoid a self-import; isinstance check
        # on the local Table class works fine.
        if not isinstance(other, Table):
            raise TypeError(
                f"Table {self._name!r} can only join another Table; "
                f"for Stream↔Table enrichment call .join from the Stream side. "
                f"Got {other.__class__.__name__}"
            )
        return table_join(self, other, on=on, type_=type)


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
        self._ops: list[dict[str, Any]] = []
        self._upstreams: list[Table] = []

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
        """Compile to a RegisterRequest JSON dict via the shared serializer."""
        from tally._serialize import compile_to_register_json
        return compile_to_register_json(self)

    def _to_register_json(self) -> dict[str, Any]:
        return self._compile()

    def _collect_registrations(self) -> list[dict[str, Any]]:
        from tally._serialize import collect_registrations
        return collect_registrations(self)

    def __repr__(self) -> str:
        return f"TableSource({self._name!r}, key={list(self._key)!r})"


class TableDerivation(Table):
    """A Table produced by stateless ops or by ``@tl.table def ... -> Table``.

    Carries the key list (which may have been rewritten by ``.rename``), the
    linear op chain, and the parameter-declared upstreams for DAG build.
    """

    def __init__(
        self,
        *,
        name: str,
        schema: dict[str, FieldSpec],
        key: list[str],
        mode: str = "append",
        ttl: str | None = None,
        ops: list[dict[str, Any]],
        upstream: Table | None,
        upstreams: list[Any],
        func: FunctionType | None = None,
        type_hints: dict[str, Any] | None = None,
    ) -> None:
        self._name = name
        self._schema = schema
        self._key = key
        self._mode = mode
        self._ttl = ttl
        self._ops = ops
        self._upstream = upstream
        self._upstreams = upstreams
        self._func = func
        self._type_hints = type_hints or {}

    def describe(self) -> dict[str, Any]:
        return format_describe(
            name=self._name,
            kind="table",
            key=list(self._key),
            mode=self._mode,
            schema=self._schema,
            ttl=self._ttl,
        )

    @property
    def _tally_stream_name(self) -> str:
        return self._name

    def _compile(self) -> dict[str, Any]:
        from tally._serialize import compile_to_register_json
        return compile_to_register_json(self)

    def _to_register_json(self) -> dict[str, Any]:
        return self._compile()

    def _collect_registrations(self) -> list[dict[str, Any]]:
        from tally._serialize import collect_registrations
        return collect_registrations(self)

    def __repr__(self) -> str:
        return (
            f"TableDerivation({self._name!r}, key={self._key!r}, "
            f"ops={len(self._ops)})"
        )


# ---------------------------------------------------------------------------
# Decorator
# ---------------------------------------------------------------------------


def _build_table_derivation_from_func(
    func: FunctionType,
    *,
    key_list: list[str],
    ttl: str | None,
    mode: str,
) -> TableDerivation:
    """Invoke a derivation function once to build its TableDerivation."""
    from tally._stream import _resolve_func_hints
    hints = _resolve_func_hints(func)

    if "return" not in hints:
        raise TypeError(
            f"@tl.table function {func.__name__!r} must declare a "
            f"return type annotation (``-> Table``)"
        )
    ret = hints["return"]
    if not (isinstance(ret, type) and issubclass(ret, Table)):
        raise TypeError(
            f"@tl.table function {func.__name__!r} must return Table; "
            f"annotation was {ret!r}"
        )

    sig = inspect.signature(func)
    params = list(sig.parameters.values())
    if not params:
        raise TypeError(
            f"derivation function {func.__name__!r} has no upstreams; "
            f"annotate parameters with your Stream/Table types"
        )
    upstreams: list[Any] = []
    for p in params:
        if p.name not in hints:
            raise TypeError(
                f"derivation function {func.__name__!r} parameter {p.name!r} "
                f"has no type annotation"
            )
        upstreams.append(hints[p.name])

    result = func(*upstreams)
    if not isinstance(result, Table):
        raise TypeError(
            f"derivation function {func.__name__!r} annotated -> Table but "
            f"returned {type(result).__name__}"
        )

    # If upstreams include something with a key field and the derivation
    # passes through / transforms the key, we enforce the declared key still
    # matches the result's actual key field list.
    result_key = getattr(result, "_key", key_list)

    if isinstance(result, TableDerivation):
        # Enforce declared key matches the derivation's current key list.
        if list(result_key) != list(key_list):
            raise TypeError(
                f"@tl.table(key={key_list!r}) function {func.__name__!r} "
                f"returned a Table with key {list(result_key)!r}; rename or "
                f"project so the output key matches the declared key"
            )
        result._name = func.__name__
        result._upstreams = list(upstreams)
        result._func = func
        result._type_hints = hints
        result._mode = mode
        result._ttl = ttl
        return result

    # result is a TableSource — wrap as a passthrough TableDerivation.
    if list(result_key) != list(key_list):
        raise TypeError(
            f"@tl.table(key={key_list!r}) function {func.__name__!r} "
            f"returned a Table with key {list(result_key)!r}"
        )
    return TableDerivation(
        name=func.__name__,
        schema=dict(result._schema),
        key=list(key_list),
        mode=mode,
        ttl=ttl,
        ops=[],
        upstream=result,
        upstreams=list(upstreams),
        func=func,
        type_hints=hints,
    )


def table(
    cls: type | FunctionType | None = None,
    *,
    key: str | list[str] | None = None,
    ttl: str | None = None,
    mode: str = "append",
):
    """Decorator that declares a Table (class or function form).

    Class form::

        @tl.table(key="user_id")
        class Users:
            user_id: str
            name: str

    Function form (Plan 21-02)::

        @tl.table(key="user_id")
        def UserLast(clicks: Clicks) -> tl.Table:
            return clicks  # until aggregation lands in 21-03
    """
    if key is None:
        raise TypeError("@tl.table requires key=... (str or list[str])")

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

    if mode == "changelog":
        raise NotImplementedError(
            "mode='changelog' ships in v0.1; use mode='append' (default)"
        )
    if mode not in ("append",):
        raise ValueError(
            f"@tl.table mode must be 'append' or 'changelog', got {mode!r}"
        )

    def _wrap(target: Any) -> Table:
        if isinstance(target, FunctionType):
            return _build_table_derivation_from_func(
                target, key_list=key_list, ttl=ttl, mode=mode
            )
        if isinstance(target, type):
            schema = extract_schema(target)
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
        raise TypeError(
            f"@tl.table must be applied to a class or function, got "
            f"{type(target).__name__}"
        )

    if cls is not None:
        return _wrap(cls)
    return _wrap


__all__ = ["table", "Table", "TableSource", "TableDerivation"]
