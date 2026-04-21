"""``@bv.table`` decorator + Table / TableSource / TableDerivation runtime types.

Plan 21-01 shipped the class form. Plan 21-02 adds:
  * :class:`StatelessOpsMixin` on :class:`Table` — stateless ops on Tables.
  * :class:`TableDerivation` — returned by ops + function-form derivations.
  * Function-form ``@bv.table(key=...) def X(...) -> Table:`` — the function
    is invoked once at registration with upstream descriptors; the returned
    Table is renamed to the function name and its upstreams captured.
"""

from __future__ import annotations

import inspect
from types import FunctionType
from typing import Any

from beava._describe import format_describe
from beava._schema_v0 import extract_schema, schema_mismatch_error
from beava._stateless_ops import StatelessOpsMixin
from beava._types_core import FieldSpec


# Phase 25-02: client-side duration-string validator. Mirrors the server's
# parse_duration_str in src/server/protocol.rs so bad values fail at
# decorator time rather than after a network round-trip. Accepts ms/s/m/h/d
# suffixes plus the "forever" and "0" sentinels.
_DURATION_SUFFIXES = ("ms", "s", "m", "h", "d")


def _validate_duration_str(s: str, *, field: str) -> None:
    if not isinstance(s, str):
        raise ValueError(
            f"{field} must be a string duration (e.g. '30d', 'forever'), "
            f"got {type(s).__name__}"
        )
    st = s.strip()
    if not st:
        raise ValueError(f"{field} must not be empty")
    if st.lower() == "forever":
        return
    if st == "0":
        return
    # ms first (two-char suffix)
    if st.endswith("ms"):
        num = st[:-2]
    else:
        num = None
        for suf in ("s", "m", "h", "d"):
            if st.endswith(suf):
                num = st[: -len(suf)]
                break
        if num is None:
            raise ValueError(
                f"{field}: invalid duration '{s}'. Use a number followed by "
                f"one of {_DURATION_SUFFIXES} (e.g. '30d') or 'forever' / '0'"
            )
    try:
        int(num)
    except ValueError as e:
        raise ValueError(
            f"{field}: invalid duration '{s}': non-numeric magnitude"
        ) from e


class Table(StatelessOpsMixin):
    """Marker / runtime type for tabular inputs.

    Both :class:`TableSource` and :class:`TableDerivation` subclass Table.
    The :class:`StatelessOpsMixin` provides the 8 per-row ops; Table's
    ``_derive`` cascades key-field renames through the derivation chain.
    """

    # Phase 24-02: Dispatch marker used by :meth:`beava.App.push` and
    # :meth:`beava.App.delete` to route into the OP_PUSH_TABLE /
    # OP_DELETE_TABLE wire opcodes. Every Table subclass inherits it.
    _beava_kind: str = "table"

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
        from beava._join import table_join
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
    def _beava_stream_name(self) -> str:
        return self._name

    def _compile(self) -> dict[str, Any]:
        """Compile to a RegisterRequest JSON dict via the shared serializer."""
        from beava._serialize import compile_to_register_json
        return compile_to_register_json(self)

    def _to_register_json(self) -> dict[str, Any]:
        return self._compile()

    def _collect_registrations(self) -> list[dict[str, Any]]:
        from beava._serialize import collect_registrations
        return collect_registrations(self)

    def __repr__(self) -> str:
        return f"TableSource({self._name!r}, key={list(self._key)!r})"


class TableDerivation(Table):
    """A Table produced by stateless ops or by ``@bv.table def ... -> Table``.

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
    def _beava_stream_name(self) -> str:
        return self._name

    def _compile(self) -> dict[str, Any]:
        from beava._serialize import compile_to_register_json
        return compile_to_register_json(self)

    def _to_register_json(self) -> dict[str, Any]:
        return self._compile()

    def _collect_registrations(self) -> list[dict[str, Any]]:
        from beava._serialize import collect_registrations
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
    from beava._stream import _resolve_func_hints
    hints = _resolve_func_hints(func)

    if "return" not in hints:
        raise TypeError(
            f"@bv.table function {func.__name__!r} must declare a "
            f"return type annotation (``-> Table``)"
        )
    ret = hints["return"]
    if not (isinstance(ret, type) and issubclass(ret, Table)):
        raise TypeError(
            f"@bv.table function {func.__name__!r} must return Table; "
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
                f"@bv.table(key={key_list!r}) function {func.__name__!r} "
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
            f"@bv.table(key={key_list!r}) function {func.__name__!r} "
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

        @bv.table(key="user_id")
        class Users:
            user_id: str
            name: str

    Function form (Plan 21-02)::

        @bv.table(key="user_id")
        def UserLast(clicks: Clicks) -> bv.Table:
            return clicks  # until aggregation lands in 21-03
    """
    if key is None:
        raise TypeError("@bv.table requires key=... (str or list[str])")

    # Phase 25-02: validate ttl string client-side. Mirrors the server's
    # parse_duration_str: ms/s/m/h/d + "forever"/"0" sentinels.
    if ttl is not None:
        _validate_duration_str(ttl, field="ttl")

    if isinstance(key, str):
        key_list = [key]
    elif isinstance(key, (list, tuple)) and all(isinstance(k, str) for k in key):
        key_list = list(key)
    else:
        raise TypeError(
            f"@bv.table key must be str or list[str], got {type(key).__name__}"
        )
    if not key_list:
        raise TypeError("@bv.table key must not be empty")

    if mode == "changelog":
        raise NotImplementedError(
            "mode='changelog' ships in v0.1; use mode='append' (default)"
        )
    if mode not in ("append",):
        raise ValueError(
            f"@bv.table mode must be 'append' or 'changelog', got {mode!r}"
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
            f"@bv.table must be applied to a class or function, got "
            f"{type(target).__name__}"
        )

    if cls is not None:
        return _wrap(cls)
    return _wrap


# ---------------------------------------------------------------------------
# Phase 55-02 Task 3 (TPC-SOURCE-01): @bv.source_table decorator + SourceTable
# ---------------------------------------------------------------------------


class SourceTable(TableSource):
    """A CDC-source Table whose writes arrive via UPSERT_TABLE_ROW /
    DELETE_TABLE_ROW opcodes (not PUSH). Echoes ``source_lsn`` on ack for
    resumable replication.

    Phase 55 source tables are **passive enrichment targets**: they cannot
    ``group_by``/aggregate — cascades do NOT fire on source-table writes
    (D-B6). Phase 57 consumes the per-DELETE PendingRetraction marker to
    drive downstream retraction.

    Phase 59.5: SourceTable defaults to **replicated** (writes fan out to
    all shards; enrichment reads are local). Pass ``sharded=True`` to the
    ``@bv.source_table`` decorator for tables too large to replicate —
    that preserves Phase 56 partition-by-key + cross-shard ReadEntityAt
    behavior.
    """

    _beava_kind: str = "source_table"
    # Phase 59.5: per-instance override. False (default) = replicated;
    # True = partitioned by `key` and looked up via cross-shard ReadEntityAt.
    _beava_sharded: bool = False

    def group_by(self, *keys: str) -> Any:
        raise RuntimeError(
            f"Cannot group_by a @bv.source_table ({self._name!r}); "
            f"source tables are passive enrichment targets in Phase 55. "
            f"Aggregate a @bv.stream source instead."
        )

    def filter(self, *_args, **_kwargs) -> Any:
        raise RuntimeError(
            f"Cannot filter a @bv.source_table ({self._name!r}); "
            f"source tables are passive enrichment targets."
        )

    def __repr__(self) -> str:
        return f"SourceTable({self._name!r}, key={list(self._key)!r})"


def source_table(
    cls: type | None = None,
    *,
    key: str | list[str] | None = None,
    entity_ttl: str | None = None,
    sharded: bool = False,
):
    """Declare a CDC source table (Phase 55-02, TPC-SOURCE-01). Example::

        @bv.source_table(key="country_code")
        class Countries:
            country_code: str
            name: str
            currency: str

    Writes land via ``client.upsert_table_row(Countries, "US", {...},
    source_lsn=42)`` — not via ``push()``. DELETEs are hard-delete +
    PendingRetraction marker (Phase 57 consumer).

    Phase 59.5: source tables default to **replicated** — every shard gets
    a full copy of the table, writes fan out to all N shards, and
    enrichment reads are local (no cross-shard traffic). This is the
    correct architecture for small dimension tables (the 95% case).
    Pass ``sharded=True`` for tables too large to replicate — that
    preserves Phase 56 partition-by-key + cross-shard ReadEntityAt
    behavior at the cost of per-event round-trip latency.

    Args:
        key: required column name(s) that identifies the row. ``str`` for
            single-key, ``list[str]`` for composite.
        entity_ttl: optional row-level TTL (e.g. ``"30d"``).
        sharded: if True, the table is partitioned by ``key`` and
            enrichment lookups on non-owner shards trigger a cross-shard
            ``ShardOp::ReadEntityAt``. Default ``False`` = replicated
            (fanout on write, local read on enrichment).

    Raises:
        TypeError: if ``key`` is omitted (``"requires key"``) or
            ``sharded`` is not a bool.
    """
    if key is None:
        raise TypeError("@bv.source_table requires key=... (str or list[str])")

    if isinstance(key, str):
        key_list = [key]
    elif isinstance(key, (list, tuple)) and all(isinstance(k, str) for k in key):
        key_list = list(key)
    else:
        raise TypeError(
            f"@bv.source_table key must be str or list[str], got {type(key).__name__}"
        )
    if not key_list:
        raise TypeError("@bv.source_table key must not be empty")

    # Phase 59.5: validate sharded. bool subclasses of int must be rejected
    # so `sharded=1` doesn't silently slip through — same pattern as
    # _validate_salt rejecting bool-shaped ints.
    if not isinstance(sharded, bool):
        raise TypeError(
            f"@bv.source_table sharded must be bool, got {type(sharded).__name__}"
        )

    if entity_ttl is not None:
        _validate_duration_str(entity_ttl, field="entity_ttl")

    def _wrap(target: Any) -> SourceTable:
        if not isinstance(target, type):
            raise TypeError(
                f"@bv.source_table must be applied to a class, got "
                f"{type(target).__name__}"
            )
        schema = extract_schema(target)
        for k in key_list:
            if k not in schema:
                raise TypeError(
                    schema_mismatch_error(k, schema, f"{target.__name__} schema")
                )
        instance = SourceTable(
            name=target.__name__,
            schema=schema,
            key=key_list,
            mode="append",
            ttl=entity_ttl,
        )
        # Stamp the sharded flag on the instance so _serialize reads it
        # per-descriptor rather than from the class default.
        instance._beava_sharded = sharded
        return instance

    if cls is not None:
        return _wrap(cls)
    return _wrap


__all__ = [
    "table",
    "Table",
    "TableSource",
    "TableDerivation",
    # Phase 55-02 Task 3
    "SourceTable",
    "source_table",
]
