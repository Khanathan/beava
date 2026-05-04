"""@bv.table decorator — Phase 13.5 Plan 03.

Per ADR-001 (locked 2026-05-03 partial overturn): ``@bv.table`` is revived
in v0 as an aggregation-output decorator only. There is **no** ``app.upsert``,
``app.delete``, or ``app.retract`` path in v0 — the decorator simply
declares the keyed materialization of an event-driven aggregation chain.

Per ADR-003 (locked 2026-05-03): ``@bv.table`` accepts three call shapes:

- ``@bv.table(key="user_id")`` — keyed (single key column)
- ``@bv.table(key=["user_id", "page"])`` — keyed (composite key)
- ``@bv.table`` (no parens) or ``@bv.table()`` (parens, no kwarg) — *global*
  table per ADR-003 Decision B; ``key_cols=[]``; the aggregation produces a
  single row addressed by an empty-string entity key.

Function form is the only supported shape in v0; class form is deferred to
v0.1+.
"""
from __future__ import annotations

import inspect
from typing import Any, Callable, get_type_hints

from beava._events import EventDerivation


class TableDescriptor:
    """The artifact produced by ``@bv.table`` — opaque to user code.

    Carries enough state to be serialized into the wire-JSON registry
    payload at ``app.register(...)`` time.
    """

    def __init__(
        self,
        name: str,
        key_cols: list[str],
        chain: list[dict[str, Any]],
        parent: Any,
    ) -> None:
        self._name = name
        self._key_cols = key_cols
        self._chain = chain
        self._parent = parent
        self._kind = "table"


def _resolve_upstream_proxies(fn: Callable[..., Any]) -> list[Any]:
    """Resolve PEP-563 string annotations to their actual classes.

    Mirrors ``_events._make_event_derivation`` so @bv.table works under
    ``from __future__ import annotations``.
    """
    sig = inspect.signature(fn)
    params = list(sig.parameters.values())
    if not params:
        raise TypeError(
            f"@bv.table function {fn.__name__!r} must take at least one parameter"
        )
    try:
        resolved = get_type_hints(fn)
    except Exception:
        resolved = {}
    proxies: list[Any] = []
    for p in params:
        ann = resolved.get(p.name, p.annotation)
        if isinstance(ann, str):
            ann = fn.__globals__.get(ann, ann)
        proxies.append(ann)
    return proxies


def _make_table(
    fn: Callable[..., Any], key_cols: list[str]
) -> TableDescriptor:
    proxies = _resolve_upstream_proxies(fn)
    result = fn(*proxies)
    if not isinstance(result, EventDerivation):
        raise TypeError(
            f"@bv.table function {fn.__name__!r} must return a chain "
            f"(events.group_by(...).agg(...) or events.agg(...)); "
            f"got {type(result).__name__}"
        )
    return TableDescriptor(
        name=fn.__name__,
        key_cols=key_cols,
        chain=result._chain,
        parent=result._parent,
    )


def table(fn_or_none: Any = None, /, *, key: Any = None) -> Any:
    """``@bv.table`` decorator — three call shapes.

    Examples
    --------
    Keyed::

        @bv.table(key="user_id")
        def UserClicks(click: Click):
            return click.group_by("user_id").agg(c=bv.count(window="1h"))

    Composite-key::

        @bv.table(key=["user_id", "page"])
        def UserPageClicks(click: Click):
            return click.group_by("user_id", "page").agg(c=bv.count(window="1h"))

    Global (per ADR-003)::

        @bv.table
        def TotalClicks(click: Click):
            return click.agg(total=bv.count(window="forever"))
    """
    # Form 1: bare @bv.table (no parens) → fn_or_none is the function.
    if fn_or_none is not None and key is None:
        if not callable(fn_or_none):
            raise TypeError(
                "@bv.table without parens requires a function below it"
            )
        return _make_table(fn_or_none, key_cols=[])
    # Form 2 + 3: @bv.table(key=...) or @bv.table() → return a decorator.
    if key is None:
        # @bv.table() (parens, no key) is global per ADR-003.
        def _decorate_global(fn: Callable[..., Any]) -> TableDescriptor:
            return _make_table(fn, key_cols=[])

        return _decorate_global

    # Normalize key arg.
    if isinstance(key, str):
        key_cols = [key]
    elif isinstance(key, (list, tuple)):
        key_cols = list(key)
    else:
        raise TypeError(
            f"@bv.table key= must be str, list, or tuple; got {type(key).__name__}"
        )

    def _decorate_keyed(fn: Callable[..., Any]) -> TableDescriptor:
        return _make_table(fn, key_cols=key_cols)

    return _decorate_keyed
