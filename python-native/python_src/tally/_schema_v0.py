"""Schema extraction + Levenshtein suggestions for the v0 SDK.

Exposes:
  - ``extract_schema(cls)``: reads class annotations + ``Field()`` markers,
    returns an ordered ``dict[str, FieldSpec]``. Rejects class bodies that
    define methods (user is pointed at the function-form decorator).
  - ``suggest(needle, haystack)``: pure-Python Levenshtein; returns the
    closest match when distance ≤ 2, else ``None``.
  - ``schema_mismatch_error(...)``: shared surgical error builder.
"""

from __future__ import annotations

import typing
from typing import Any

from tally._types_core import (
    MISSING,
    FieldSpec,
    Optional as _OptionalMarker_instance,
    _FieldMarker,
    _OptionalSpec,
)

# Primitive types we accept as field annotations. Extended as needed.
_ALLOWED_PRIMITIVES: tuple[type, ...] = (int, float, str, bool, bytes)


def _is_supported_type(t: Any) -> bool:
    """Is ``t`` a schema-supported Python type?"""
    if t in _ALLOWED_PRIMITIVES:
        return True
    # Allow datetime / date (commonly used; stringify on serialize)
    try:
        import datetime as _dt

        if t in (_dt.datetime, _dt.date, _dt.time):
            return True
    except Exception:  # pragma: no cover
        pass
    return False


def _type_name(t: Any) -> str:
    return getattr(t, "__name__", repr(t))


def extract_schema(cls: type) -> dict[str, FieldSpec]:
    """Build an ordered FieldSpec map from a class body.

    Rules:
      * Every annotated attribute becomes a FieldSpec (in declaration order).
      * ``Optional[T]`` marker → ``optional=True`` with ``py_type=T``.
      * ``= Field(desc=..., default=...)`` assigns metadata.
      * Any callable in the class body (excluding dunders) raises TypeError
        directing the user at the function-form decorator.
      * Unsupported types (generics like ``list[dict]``, arbitrary objects)
        raise TypeError naming the offending field.
    """
    # Reject methods / other callables — class form is schema-only.
    for attr_name, attr_val in cls.__dict__.items():
        if attr_name.startswith("__") and attr_name.endswith("__"):
            continue
        if callable(attr_val) and not isinstance(attr_val, _FieldMarker):
            raise TypeError(
                f"@tl.stream classes declare schema only — found method "
                f"{attr_name!r} on {cls.__name__!r}; move derivations to a "
                f"@tl.stream function"
            )

    # Pull type hints honouring the tally Optional marker. We deliberately
    # avoid typing.get_type_hints because it resolves typing.Optional to
    # Union[T, None] which we don't want to conflate with tl.Optional.
    raw_annotations: dict[str, Any] = {}
    for base in reversed(cls.__mro__):
        raw_annotations.update(getattr(base, "__annotations__", {}))

    schema: dict[str, FieldSpec] = {}
    for fname, ftype in raw_annotations.items():
        if fname.startswith("_"):
            continue

        optional = False
        if isinstance(ftype, _OptionalSpec):
            optional = True
            ftype = ftype.inner

        # Resolve forward references as strings via eval. We layer three
        # namespaces: builtins/primitives, datetime types, and the class's
        # defining module — so ``ts: datetime`` works regardless of whether
        # the test module imported ``datetime`` at module level.
        if isinstance(ftype, str):
            ns: dict[str, Any] = {
                "int": int, "float": float, "str": str, "bool": bool,
                "bytes": bytes, "Optional": _OptionalMarker_instance,
            }
            import datetime as _dt_mod
            ns.update({"datetime": _dt_mod.datetime, "date": _dt_mod.date, "time": _dt_mod.time})
            try:
                mod = __import__(cls.__module__, fromlist=["*"])
                ns.update(mod.__dict__)
            except Exception:
                pass
            ns.update(vars(cls))
            try:
                ftype = eval(ftype, ns)
            except Exception as e:
                raise TypeError(
                    f"Could not resolve forward-ref type for field "
                    f"{fname!r} on {cls.__name__!r}: {ftype!r} ({e})"
                ) from e
            if isinstance(ftype, _OptionalSpec):
                optional = True
                ftype = ftype.inner

        if not _is_supported_type(ftype):
            raise TypeError(
                f"Field {fname!r} on {cls.__name__!r} has unsupported type "
                f"{ftype!r}; only primitives (int/float/str/bool/bytes) and "
                f"datetime types are allowed in v0 schemas"
            )

        desc: str | None = None
        default: Any = MISSING
        marker = cls.__dict__.get(fname)
        if isinstance(marker, _FieldMarker):
            desc = marker.desc
            default = marker.default

        schema[fname] = FieldSpec(
            name=fname,
            py_type=ftype,
            optional=optional,
            desc=desc,
            default=default,
        )

    return schema


# ---------------------------------------------------------------------------
# Levenshtein suggestion helper
# ---------------------------------------------------------------------------


def _levenshtein(a: str, b: str) -> int:
    """Classic iterative DP Levenshtein distance. O(len(a)*len(b)) time,
    O(len(b)) space. No external dependency."""
    if a == b:
        return 0
    if not a:
        return len(b)
    if not b:
        return len(a)
    prev = list(range(len(b) + 1))
    for i, ca in enumerate(a, 1):
        curr = [i] + [0] * len(b)
        for j, cb in enumerate(b, 1):
            cost = 0 if ca == cb else 1
            curr[j] = min(
                curr[j - 1] + 1,      # insert
                prev[j] + 1,          # delete
                prev[j - 1] + cost,   # replace
            )
        prev = curr
    return prev[-1]


def suggest(needle: str, haystack: list[str], *, max_distance: int = 2) -> str | None:
    """Return the closest match in ``haystack`` with distance ≤ ``max_distance``.

    Ties are broken by first appearance in ``haystack`` (stable).
    Returns ``None`` when no candidate falls within the threshold.
    """
    best: tuple[int, str] | None = None
    for candidate in haystack:
        d = _levenshtein(needle, candidate)
        if d <= max_distance and (best is None or d < best[0]):
            best = (d, candidate)
    return best[1] if best is not None else None


def schema_mismatch_error(
    field: str,
    schema: dict[str, FieldSpec] | list[str],
    context: str,
) -> str:
    """Build the surgical mismatch message used across the SDK.

    Example::

        "field 'amout' not in Purchases; did you mean 'amount'?
         available: [amount, user_id, status]"
    """
    if isinstance(schema, dict):
        names = list(schema.keys())
    else:
        names = list(schema)
    hint = suggest(field, names)
    lines = [f"field {field!r} not in {context}"]
    if hint is not None:
        lines[-1] += f"; did you mean {hint!r}?"
    lines.append(f"available: [{', '.join(names)}]")
    return "\n".join(lines)
