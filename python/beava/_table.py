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


def _collect_closure_cells(fn: Callable[..., Any]) -> dict[str, Any]:
    """Return a {name: value} map of ``fn``'s lexical closure cells.

    Pairs ``fn.__code__.co_freevars`` (the names) with ``fn.__closure__``
    (the cell objects). A cell with an empty / unset value is skipped.
    """
    cells: dict[str, Any] = {}
    code = getattr(fn, "__code__", None)
    closure = getattr(fn, "__closure__", None)
    if code is None or closure is None:
        return cells
    freevars = getattr(code, "co_freevars", ())
    for name, cell in zip(freevars, closure):
        try:
            cells[name] = cell.cell_contents
        except ValueError:
            # Cell exists but has no contents yet (rare); skip silently.
            continue
    return cells


_TABLE_MODULE_FILE = __file__


def _collect_caller_frame_locals() -> dict[str, Any]:
    """Return merged ``f_locals`` from user-code frames above ``_table.py``.

    Walks back through the call stack and merges ``f_locals`` from every
    frame that is NOT inside ``_table.py`` (this module). User-code
    frames are walked outward; first-seen wins, so a name in the closest
    user-code frame (e.g. an inner factory fn) shadows a same-name
    binding in an outer frame (e.g. the pytest test fn).

    The Plan 07e documented contract pins this behavior:
      - Boundary detection is by FILE IDENTITY (not by frame-depth count),
        so the resolver stays robust under decorator-stack wrappers
        (``functools.lru_cache``, etc.) that change the count.
      - User-code frames are merged in proximity order — closest wins.
      - The walk terminates at a depth bound (32 frames) to avoid
        runaway in pathological recursive setups.

    See ``docs/sdk-api/python.md`` § "Supported @bv.event declaration
    sites" for the user-facing contract.
    """
    merged: dict[str, Any] = {}
    frame = inspect.currentframe()
    try:
        if frame is None:
            return merged
        frame = frame.f_back  # skip this helper
        depth = 0
        while frame is not None and depth < 32:
            code = frame.f_code
            if getattr(code, "co_filename", None) != _TABLE_MODULE_FILE:
                # User-code frame — merge first-seen-wins (closer frames
                # take priority over outer frames).
                for name, val in frame.f_locals.items():
                    if name not in merged:
                        merged[name] = val
            frame = frame.f_back
            depth += 1
        return merged
    finally:
        del frame  # break the CPython reference cycle


def _resolve_upstream_proxies(fn: Callable[..., Any]) -> list[Any]:
    """Resolve PEP-563 string annotations to their actual classes.

    Mirrors ``_events._make_event_derivation`` so @bv.table works under
    ``from __future__ import annotations``.

    Per Phase 13.5.1 D-01 (USER-LOCKED): raises ``TypeError`` if any decorated
    parameter is missing an annotation — predictable, mypy-friendly, mirrors
    the existing ``@bv.event`` convention. Silent fallback to
    ``inspect.Parameter.empty`` (which surfaced as ``AttributeError`` in
    user code) is forbidden.

    **Phase 13.5.1 Plan 07e — documented declaration-site contract.** The
    resolver tries name-resolution sources in this fixed order and stops at
    the first hit:

      1. ``fn.__globals__`` (canonical, mypy-friendly module-level
         declarations).
      2. Enclosing closure cells (``fn.__closure__`` paired with
         ``fn.__code__.co_freevars``) — captures inner-class / lru_cache
         factory patterns.
      3. Caller-frame ``f_locals`` (one frame back, bounded) — captures
         the pytest-fixture pattern (``@bv.event class Foo: ...`` inside
         a test fn body).

    See ``docs/sdk-api/python.md`` § "Supported @bv.event declaration sites"
    for the user-facing contract; see ``13.5.1-07e-PLAN.md`` for the
    rationale (this replaces the Plan 05 8-frame magic walk, which broke
    on decorator-stack wrappers because frame depth shifted).
    """
    sig = inspect.signature(fn)
    params = list(sig.parameters.values())
    if not params:
        raise TypeError(
            f"@bv.table function {fn.__name__!r} must take at least one parameter"
        )
    # Combine localns sources in priority order: closure cells, then a
    # single caller-frame snapshot. fn.__globals__ is passed separately to
    # get_type_hints. First-seen wins on key collisions inside `localns`
    # (closure cells take priority over caller f_locals — closure cells
    # are tied to the decorator-fn definition, caller f_locals is the
    # invocation site; the former is more specific).
    closure_cells = _collect_closure_cells(fn)
    # Walk back to the first non-_table.py frame: that's the user-code
    # frame that invoked @bv.table (whether directly via the bare form or
    # via the inner _decorate_keyed / _decorate_global closure). The
    # boundary detection is by file identity, not by depth count, so it
    # stays robust under decorator-stack wrappers.
    caller_locals = _collect_caller_frame_locals()
    localns: dict[str, Any] = dict(caller_locals)  # lower priority
    localns.update(closure_cells)  # higher priority overlays
    try:
        resolved = get_type_hints(fn, globalns=fn.__globals__, localns=localns)
    except Exception:
        resolved = {}
    proxies: list[Any] = []
    for p in params:
        ann = resolved.get(p.name, p.annotation)
        if ann is inspect.Parameter.empty:
            raise TypeError(
                f"@bv.table function {fn.__name__!r} parameter {p.name!r} "
                f"must be annotated with the upstream event class — "
                f"e.g. def {fn.__name__}({p.name}: Click): ..."
            )
        if isinstance(ann, str):
            # Try sources in documented contract order: globals, then
            # closure cells, then caller f_locals.
            ann = (
                fn.__globals__.get(
                    ann,
                    closure_cells.get(ann, caller_locals.get(ann, ann)),
                )
            )
        # Phase 13.5.2 D-02 (USER-LOCKED): reject raw EventDerivation
        # instances. Legitimate @bv.event def outputs carry the
        # `_is_bv_event_function` marker landed in `_make_event_derivation`.
        if isinstance(ann, EventDerivation) and not getattr(
            ann, "_is_bv_event_function", False
        ):
            raise TypeError(
                f"@bv.table function {fn.__name__!r} parameter {p.name!r} "
                f"annotation resolves to an EventDerivation instance (a raw "
                f"chain). Annotate with an @bv.event-decorated class or function "
                f"instead — e.g.\n"
                f"    @bv.event\n"
                f"    def Tagged(click: Click): return click.with_columns(...)\n"
                f"    @bv.table(key='user_id')\n"
                f"    def {fn.__name__}({p.name}: Tagged): "
                f"return {p.name}.group_by('user_id').agg(...)"
            )
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
