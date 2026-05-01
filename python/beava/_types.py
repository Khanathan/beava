"""Type-mapping primitives for the beava SDK.

Public exports (re-exported from beava.__init__):
  - Optional: subscript target producing _OptionalSpec markers (distinct from typing.Optional)
  - Field: factory returning _FieldMarker instances carrying per-field metadata
  - MISSING: singleton sentinel for "no default specified"

Module-level helpers (for internal use by decorators / schema extraction):
  - py_type_to_field_type: maps Python types to server FieldType strings
"""

from __future__ import annotations

from typing import Any


class _Missing:
    """Singleton sentinel for 'no default value specified'.

    Distinct from None so that ``Field(default=None)`` (nullable default)
    can be distinguished from ``Field()`` (no default at all).

    ``bool(MISSING)`` is False.
    """

    _instance: "_Missing | None" = None

    def __new__(cls) -> "_Missing":
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __repr__(self) -> str:  # pragma: no cover
        return "MISSING"

    def __bool__(self) -> bool:
        return False


MISSING = _Missing()


class _OptionalSpec:
    """Wraps a Python type to mark it nullable in a beava schema.

    Produced by ``Optional[T]``. Nested ``Optional[Optional[T]]`` collapses
    to ``Optional[T]`` so users can safely compose without double-wrapping.
    """

    __slots__ = ("inner",)
    inner: Any  # the wrapped Python type or _OptionalSpec (before collapse)

    def __init__(self, inner: Any) -> None:
        # Collapse nested Optionals: Optional[Optional[int]] -> Optional[int]
        if isinstance(inner, _OptionalSpec):
            inner = inner.inner
        self.inner = inner

    def __repr__(self) -> str:
        inner_name = getattr(self.inner, "__name__", repr(self.inner))
        return f"Optional[{inner_name}]"

    def __eq__(self, other: object) -> bool:
        return isinstance(other, _OptionalSpec) and other.inner == self.inner

    def __hash__(self) -> int:
        return hash(("_OptionalSpec", self.inner))


class _OptionalMarker:
    """Subscript target: ``bv.Optional[int]`` → ``_OptionalSpec(int)``.

    This is distinct from ``typing.Optional`` (which expands to ``Union[T, None]``)
    so beava can precisely distinguish nullable-annotated fields from plain Python
    type unions.
    """

    def __getitem__(self, inner: Any) -> _OptionalSpec:
        return _OptionalSpec(inner)

    def __repr__(self) -> str:  # pragma: no cover
        return "beava.Optional"


Optional = _OptionalMarker()


class _FieldMarker:
    """Class-attribute marker carrying per-field metadata.

    Users write::

        class Clicks:
            user_id: str = bv.Field(desc="who clicked")
            amount: float = bv.Field(desc="charge in USD", default=0.0)

    Schema extraction reads ``cls.__dict__`` for ``_FieldMarker`` instances and
    merges the metadata into the resulting FieldSpec.
    """

    __slots__ = ("desc", "default")

    def __init__(self, desc: str | None = None, default: Any = MISSING) -> None:
        self.desc = desc
        self.default = default

    def __repr__(self) -> str:  # pragma: no cover
        return f"Field(desc={self.desc!r}, default={self.default!r})"


def Field(*, desc: str | None = None, default: Any = MISSING) -> _FieldMarker:
    """Factory for per-field metadata attached as a class-attribute default.

    Args:
        desc: Human-readable description of the field shown in registry + error messages.
        default: Default value for the field. Omit (or pass ``MISSING``) to mark required.

    Returns:
        A ``_FieldMarker`` instance; schema extraction reads it from the class dict.
    """
    return _FieldMarker(desc=desc, default=default)


# ---------------------------------------------------------------------------
# Field type mapping: Python type → server FieldType string
# ---------------------------------------------------------------------------
#
# Must be imported by _events.py at decoration time.
# Raises TypeError on unsupported types with a helpful message.
# ---------------------------------------------------------------------------

_PY_TYPE_TO_FIELD_TYPE: dict[type, str] = {
    str: "str",
    float: "f64",
    int: "i64",
    bool: "bool",
    bytes: "bytes",
}


def py_type_to_field_type(py_type: type) -> str:
    """Map a Python built-in type to the server's FieldType string.

    Args:
        py_type: A Python type (str, int, float, bool, bytes, datetime.datetime).

    Returns:
        The server FieldType string: "str", "i64", "f64", "bool", "bytes", or "datetime".

    Raises:
        TypeError: If ``py_type`` is not one of the supported types.
            Message includes the type name and the full supported list.
    """
    import datetime as _dt

    if py_type is _dt.datetime:
        return "datetime"
    if py_type in _PY_TYPE_TO_FIELD_TYPE:
        return _PY_TYPE_TO_FIELD_TYPE[py_type]
    type_name = getattr(py_type, "__name__", repr(py_type))
    raise TypeError(
        f"unsupported type {type_name!r}; supported: str, int, float, bool, bytes, datetime"
    )
