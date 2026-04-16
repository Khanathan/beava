"""Schema primitives for the v0 SDK.

Provides:
  - ``Optional[T]``: a Beava-owned nullable marker (distinct from ``typing.Optional``).
  - ``Field(desc=..., default=...)``: per-field metadata attached as a class-attribute value.
  - ``FieldSpec``: normalized schema entry consumed by Stream/Table descriptors.
  - ``MISSING``: sentinel distinguishing "no default" from "default is None".

``typing.Optional[T]`` is *not* used because it's ambiguous in the Beava schema
model — we want users to explicitly opt into nullability via ``bv.Optional``.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


class _Missing:
    """Sentinel class — single instance exposed as ``MISSING``."""

    _instance: "_Missing | None" = None

    def __new__(cls) -> "_Missing":
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __repr__(self) -> str:  # pragma: no cover - cosmetic
        return "MISSING"

    def __bool__(self) -> bool:
        return False


MISSING = _Missing()


class _OptionalSpec:
    """Wraps a Python type to mark it nullable in a Beava schema.

    Produced by ``Optional[T]``. ``_OptionalSpec(_OptionalSpec(T))`` collapses
    to ``_OptionalSpec(T)`` so nesting is idempotent.
    """

    __slots__ = ("inner",)

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
    """Subscript target: ``Optional[int]`` → ``_OptionalSpec(int)``."""

    def __getitem__(self, inner: Any) -> _OptionalSpec:
        return _OptionalSpec(inner)

    def __repr__(self) -> str:  # pragma: no cover
        return "beava.Optional"


Optional = _OptionalMarker()


class _FieldMarker:
    """Class-attribute marker carrying per-field metadata.

    Users write::

        class Clicks:
            user_id: str = Field(desc="who clicked")

    ``extract_schema`` looks up ``cls.__dict__`` for this marker and merges
    its metadata into the resulting :class:`FieldSpec`.
    """

    __slots__ = ("desc", "default")

    def __init__(self, desc: str | None = None, default: Any = MISSING) -> None:
        self.desc = desc
        self.default = default

    def __repr__(self) -> str:  # pragma: no cover
        return f"Field(desc={self.desc!r}, default={self.default!r})"


def Field(*, desc: str | None = None, default: Any = MISSING) -> _FieldMarker:
    """Attach metadata to a schema field.

    Usage::

        class Users:
            user_id: str = Field(desc="primary key")
            nickname: str = Field(desc="display name", default="anon")
    """
    return _FieldMarker(desc=desc, default=default)


@dataclass
class FieldSpec:
    """Normalized schema entry for a single field.

    ``py_type`` is always the *inner* Python type; nullability is tracked
    separately via ``optional``. ``default`` is ``MISSING`` when no default
    was supplied (distinct from ``default=None``).
    """

    name: str
    py_type: type
    optional: bool = False
    desc: str | None = None
    default: Any = MISSING
