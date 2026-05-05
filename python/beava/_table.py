"""``@bv.table`` decorator.

Per the ADR-001 partial-overturn (locked 2026-05-03): ``@bv.table`` is the
aggregation-output decorator. v0 has no ``app.upsert`` / ``app.delete`` /
``app.retract`` paths — the decorator simply declares the keyed
materialization of an event-driven aggregation chain.

The decorator accepts three call shapes:

- ``@bv.table(key="user_id")`` — keyed (single key column).
- ``@bv.table(key=["user_id", "page"])`` — keyed (composite key).
- ``@bv.table`` (no parens) or ``@bv.table()`` (parens, no kwarg) —
  *global* table; ``key_cols=[]``; the aggregation produces a single
  row addressed by an empty-string entity key.

Only the function form is supported in v0; class form is deferred to v0.1+.
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
    """Return a ``{name: value}`` map of ``fn``'s lexical closure cells.

    Pairs ``fn.__code__.co_freevars`` (the names) with ``fn.__closure__``
    (the cell objects). Cells with no contents yet (rare; happens during
    forward-reference resolution at module import) are skipped silently.
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
            continue
    return cells


_TABLE_MODULE_FILE = __file__


def _collect_caller_frame_locals() -> dict[str, Any]:
    """Return merged ``f_locals`` from user-code frames above ``_table.py``.

    Walks back through the call stack and merges ``f_locals`` from every
    frame that is NOT inside ``_table.py``. Closest user-code frame wins
    on key collisions (so an inner factory function shadows the outer
    pytest fn binding of the same name).

    Boundary detection is by FILE IDENTITY rather than by frame-depth
    count, so the resolver stays robust under decorator-stack wrappers
    (``functools.lru_cache``, etc.) that shift the depth. The walk
    terminates at 32 frames to avoid runaway under pathological recursion.
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
                for name, val in frame.f_locals.items():
                    if name not in merged:
                        merged[name] = val
            frame = frame.f_back
            depth += 1
        return merged
    finally:
        # Break the CPython reference cycle (frames hold refs to locals
        # which can hold refs back to the frame).
        del frame


def _resolve_upstream_proxies(fn: Callable[..., Any]) -> list[Any]:
    """Resolve PEP-563 string annotations to their actual classes.

    Mirrors :func:`_events._make_event_derivation` so ``@bv.table`` works
    under ``from __future__ import annotations``.

    A missing annotation raises ``TypeError`` rather than falling through to
    ``inspect.Parameter.empty``. The strict error is the contract: silent
    fallback used to surface as ``AttributeError`` deep inside user code
    when the empty annotation reached a chain method, which is much harder
    to diagnose than a sharp decorator-time error pointing at the
    parameter.

    Resolution sources are tried in this fixed priority order and the first
    hit wins:

      1. ``fn.__globals__`` — the canonical, mypy-friendly module-level
         declaration site.
      2. Enclosing closure cells — captures inner-class / ``lru_cache``
         factory patterns where the upstream class is defined inside
         another function.
      3. Caller-frame ``f_locals`` (one frame back, bounded) — captures
         the pytest-fixture pattern of declaring ``@bv.event class Foo:
         ...`` inside a test function body.
    """
    sig = inspect.signature(fn)
    params = list(sig.parameters.values())
    if not params:
        raise TypeError(
            f"@bv.table function {fn.__name__!r} must take at least one parameter"
        )
    closure_cells = _collect_closure_cells(fn)
    caller_locals = _collect_caller_frame_locals()
    # Closure cells outrank caller-frame locals on key collisions: the
    # closure is tied to the decorator-fn definition site, while the
    # caller frame is the invocation site, and the former is the more
    # specific scope.
    localns: dict[str, Any] = dict(caller_locals)
    localns.update(closure_cells)
    try:
        resolved = get_type_hints(fn, globalns=fn.__globals__, localns=localns)
    except Exception:
        resolved = {}
    proxies: list[Any] = []
    for p in params:
        ann = resolved.get(p.name, p.annotation)
        if ann is inspect.Parameter.empty:
            # Strict contract: a missing annotation is rejected at
            # decorator time rather than falling through to a generic
            # proxy. The fallback used to surface as `AttributeError`
            # deep inside user code; the sharp `TypeError` here points
            # readers straight at the unannotated parameter.
            raise TypeError(
                f"@bv.table function {fn.__name__!r} parameter {p.name!r} "
                f"must be annotated with the upstream event class — "
                f"e.g. def {fn.__name__}({p.name}: Click): ..."
            )
        if isinstance(ann, str):
            ann = (
                fn.__globals__.get(
                    ann,
                    closure_cells.get(ann, caller_locals.get(ann, ann)),
                )
            )
        # Reject raw EventDerivation instances. Legitimate @bv.event def
        # outputs carry the `_is_bv_event_function` marker set by
        # `_make_event_derivation`; raw chain expressions don't.
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

    Global::

        @bv.table
        def TotalClicks(click: Click):
            return click.agg(total=bv.count(window="forever"))
    """
    # Bare ``@bv.table`` (no parens): the wrapped function is positional.
    if fn_or_none is not None and key is None:
        if not callable(fn_or_none):
            raise TypeError(
                "@bv.table without parens requires a function below it"
            )
        return _make_table(fn_or_none, key_cols=[])
    # ``@bv.table(...)`` returns a decorator; ``key=None`` is global.
    if key is None:
        def _decorate_global(fn: Callable[..., Any]) -> TableDescriptor:
            return _make_table(fn, key_cols=[])

        return _decorate_global

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
