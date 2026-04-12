"""EventSet, FeatureSet, and Field types for the v2.0 API.

Provides typed schema definitions using PEP 681 ``@dataclass_transform``
so that IDEs provide autocomplete and type checking for user-defined
event and feature schemas.

Usage::

    import tally as tl
    from tally._schema import EventSet, FeatureSet, Field

    class TxnEvent(EventSet):
        user_id: str = Field()
        amount: float = Field()
        merchant_id: str = Field()

    class TxnFeatures(FeatureSet):
        tx_count: int = Field()
        avg_amount: float = Field()
"""

from __future__ import annotations

import typing

_SENTINEL = ...  # Ellipsis as "required" sentinel


class Field:
    """Descriptor for a field in an EventSet or FeatureSet.

    Args:
        dtype: Explicit type. If None, inferred from annotation.
        description: Human-readable description.
        default: Default value. Ellipsis (...) means required.
    """

    def __init__(
        self,
        *,
        dtype: type | None = None,
        description: str = "",
        default: object = _SENTINEL,
    ) -> None:
        self.dtype = dtype
        self.description = description
        self.default = default


def _collect_fields(cls: type) -> dict[str, Field]:
    """Collect Field descriptors from class annotations and attributes.

    For each annotated name:
    - If the class attribute is a Field instance, use it.
    - If no Field is present, create a default Field() (required).

    Then infer dtype from annotation if Field.dtype is None.
    """
    fields: dict[str, Field] = {}

    # Resolve type hints (handles `from __future__ import annotations`)
    try:
        hints = typing.get_type_hints(cls)
    except Exception:
        hints = getattr(cls, "__annotations__", {})

    for name in hints:
        if name.startswith("_"):
            continue
        attr = cls.__dict__.get(name)
        if isinstance(attr, Field):
            field = attr
        else:
            field = Field()
        # Infer dtype from annotation if not explicitly set
        if field.dtype is None:
            field.dtype = hints.get(name)
        fields[name] = field

    return fields


def _make_init(fields: dict[str, Field]) -> None:
    """Generate an __init__ method for the given fields using exec().

    Required fields (default is ...) come first, optional fields after.
    """
    required = [(n, f) for n, f in fields.items() if f.default is _SENTINEL]
    optional = [(n, f) for n, f in fields.items() if f.default is not _SENTINEL]

    params = []
    body_lines = []

    for name, _field in required:
        params.append(name)
        body_lines.append(f"    self.{name} = {name}")

    for name, field in optional:
        # Use a unique default sentinel name to avoid closure issues
        default_name = f"_default_{name}"
        params.append(f"{name}={default_name}")
        body_lines.append(f"    self.{name} = {name}")

    if not params:
        params_str = "self"
    else:
        params_str = "self, " + ", ".join(params)

    body = "\n".join(body_lines) if body_lines else "    pass"
    code = f"def __init__({params_str}):\n{body}"

    # Build locals with default values
    local_ns: dict = {}
    for name, field in optional:
        local_ns[f"_default_{name}"] = field.default

    exec(code, local_ns)
    return local_ns["__init__"]


@typing.dataclass_transform(field_specifiers=(Field,))
class EventSet:
    """Base class for event schemas.

    Subclasses define fields via annotations and Field() descriptors::

        class TxnEvent(EventSet):
            user_id: str = Field()
            amount: float = Field()
    """

    _fields: dict[str, Field]

    def __init_subclass__(cls, **kwargs: object) -> None:
        super().__init_subclass__(**kwargs)
        cls._fields = _collect_fields(cls)
        cls.__init__ = _make_init(cls._fields)


@typing.dataclass_transform(field_specifiers=(Field,))
class FeatureSet:
    """Base class for feature schemas.

    Subclasses define fields via annotations and Field() descriptors::

        class TxnFeatures(FeatureSet):
            tx_count: int = Field()
            avg_amount: float = Field()
    """

    _fields: dict[str, Field]

    def __init_subclass__(cls, **kwargs: object) -> None:
        super().__init_subclass__(**kwargs)
        cls._fields = _collect_fields(cls)
        cls.__init__ = _make_init(cls._fields)
