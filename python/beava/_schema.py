"""Schema extraction from Python type annotations for the beava SDK.

Public API:
  - FieldSpec: dataclass representing a single schema field
  - extract_schema(cls) -> dict[str, FieldSpec]: extract schema from a class
  - validate_duration_string(s): validate duration string shape only
  - duration_to_ms(s) -> int: convert duration string to milliseconds

Schema extraction is stdlib-only (inspect + typing). No pydantic, no attrs.
"""

from __future__ import annotations

import re
import typing
from dataclasses import dataclass, field
from typing import Any, Union

from ._types import MISSING, _FieldMarker, _OptionalSpec, py_type_to_field_type

__all__ = ["FieldSpec", "extract_schema", "validate_duration_string", "duration_to_ms"]

# ---------------------------------------------------------------------------
# FieldSpec dataclass
# ---------------------------------------------------------------------------


@dataclass
class FieldSpec:
    """Normalized schema entry for a single field.

    Attributes:
        name:     Field name as declared in the class.
        py_type:  Inner Python type (e.g. str, int, datetime.datetime).
                  Always the unwrapped type — not _OptionalSpec.
        optional: True if field was annotated with bv.Optional[T].
        desc:     Optional human-readable description from bv.Field(desc=...).
        default:  Default value from bv.Field(default=...) or MISSING if none.
    """

    name: str
    py_type: type
    optional: bool = False
    desc: str | None = None
    default: Any = field(default_factory=lambda: MISSING)


# ---------------------------------------------------------------------------
# Duration string helpers
# ---------------------------------------------------------------------------

_DURATION_RE = re.compile(r"^\d+(ms|s|m|h|d)$")

_UNIT_TO_MS: dict[str, int] = {
    "ms": 1,
    "s": 1_000,
    "m": 60_000,
    "h": 3_600_000,
    "d": 86_400_000,
}


def validate_duration_string(s: str) -> None:
    """Validate that *s* is a correctly formatted duration string.

    Accepted: ``"forever"`` or ``<number><unit>`` where unit is one of
    ``ms``, ``s``, ``m``, ``h``, ``d``.

    Args:
        s: The string to validate.

    Raises:
        TypeError: If *s* does not match the expected pattern.
    """
    if s == "forever":
        return
    if not _DURATION_RE.match(s):
        raise TypeError(
            f"invalid duration string {s!r}; expected a number followed by a unit "
            f"(ms, s, m, h, d) or the literal 'forever'; e.g. '5s', '24h', '7d', '100ms'"
        )


def duration_to_ms(s: str) -> int:
    """Convert a duration string to milliseconds.

    Args:
        s: A valid duration string like ``"5s"``, ``"24h"``, ``"7d"``, ``"100ms"``.
           The string ``"forever"`` is NOT convertible — it raises ValueError.

    Returns:
        Integer number of milliseconds.

    Raises:
        TypeError: If *s* is not a valid duration string.
        ValueError: If *s* is ``"forever"`` (no finite ms representation).
    """
    validate_duration_string(s)  # raises TypeError if invalid
    if s == "forever":
        raise ValueError(
            "'forever' has no finite millisecond equivalent; "
            "use ttl_ms=None to represent no expiry"
        )
    # Extract trailing unit; number is everything before the unit
    for unit in ("ms", "s", "m", "h", "d"):
        if s.endswith(unit):
            number_part = s[: -len(unit)]
            return int(number_part) * _UNIT_TO_MS[unit]
    # Unreachable given _DURATION_RE, but satisfies type checkers
    raise TypeError(f"invalid duration string {s!r}")  # pragma: no cover


# ---------------------------------------------------------------------------
# Schema extraction
# ---------------------------------------------------------------------------


def extract_schema(cls: type) -> dict[str, FieldSpec]:
    """Extract a beava schema from a plain Python class's type annotations.

    Walks ``cls.__annotations__`` (preserves declaration order) and resolves
    types via ``typing.get_type_hints()``. Handles ``bv.Optional[T]`` nullable
    markers and ``bv.Field(desc=..., default=...)`` metadata.

    Args:
        cls: The class to inspect. Must be a type (not a function or instance).

    Returns:
        An ordered ``dict[name, FieldSpec]`` matching declaration order.

    Raises:
        TypeError: If *cls* is not a class.
        TypeError: If a field is annotated with ``typing.Optional[T]`` instead
                   of ``bv.Optional[T]``.
        TypeError: If a field type is not one of: str, int, float, bool, bytes,
                   datetime.datetime.
    """
    if not isinstance(cls, type):
        raise TypeError(f"extract_schema() expects a class, got {type(cls).__name__!r}")

    # Resolve forward references; include_extras=False strips Annotated wrappers.
    try:
        hints = typing.get_type_hints(cls, include_extras=False)
    except Exception as exc:
        raise TypeError(f"failed to resolve type hints for {cls.__name__!r}: {exc}") from exc

    # Collect annotation order from the full MRO (base classes first, excluding
    # object) so that inherited fields are included.  typing.get_type_hints()
    # already merges the MRO; we must mirror that here so annotation_order and
    # hints stay in sync.  Python 3.7+ dicts preserve insertion order.
    annotation_order: list[str] = []
    seen_ann: set[str] = set()
    for klass in reversed(cls.__mro__):
        if klass is object:
            continue
        for field_name in getattr(klass, "__annotations__", {}):
            if field_name not in seen_ann:
                annotation_order.append(field_name)
                seen_ann.add(field_name)

    result: dict[str, FieldSpec] = {}
    for name in annotation_order:
        if name not in hints:
            continue
        py_type = hints[name]

        optional = False

        # Handle bv.Optional[T] — our nullable marker
        if isinstance(py_type, _OptionalSpec):
            optional = True
            py_type = py_type.inner

        # Reject typing.Optional[T] (Union[T, None]) — guide user to bv.Optional
        elif (
            typing.get_origin(py_type) is Union
            and type(None) in typing.get_args(py_type)
        ):
            raise TypeError(
                f"field {name!r}: use bv.Optional[T] not typing.Optional[T]; "
                f"bv.Optional is the Beava nullable marker that avoids Union ambiguity"
            )

        # Validate that the (unwrapped) type is supported; propagates TypeError
        py_type_to_field_type(py_type)

        # Look for bv.Field(...) class-attribute metadata
        desc: str | None = None
        default: Any = MISSING
        attr = cls.__dict__.get(name)
        if isinstance(attr, _FieldMarker):
            desc = attr.desc
            default = attr.default

        result[name] = FieldSpec(
            name=name,
            py_type=py_type,
            optional=optional,
            desc=desc,
            default=default,
        )

    return result
