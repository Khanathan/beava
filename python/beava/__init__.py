"""beava — Python SDK for the Beava real-time feature server.

Public API (Phase 3 Plan 02 baseline):
  - Optional: nullable field marker (distinct from typing.Optional)
  - Field: per-field metadata factory
  - ValidationError: frozen dataclass for schema/DAG validation errors
  - RegistrationError: Exception for registration failures
  - BinaryNotFoundError: Exception for missing beava binary (embed mode)
  - col: expression DSL constructor (Plan 03-02)
  - Col: _ExprAST base class for isinstance checks (Plan 03-02)
  - event, table, App: stubs filled in by later plans in Phase 3
"""

from ._col import Col, col
from ._errors import (
    BinaryNotFoundError,
    RegistrationError,
    ValidationError,
)
from ._types import Field, Optional

# Phase 3 stubs — filled in by later plans; kept as module attributes so
# import-time discovery (hasattr, dir) works from Plan 03-01 onwards.


def _stub_event(*_args: object, **_kwargs: object) -> object:
    raise NotImplementedError("@bv.event lands in Plan 03-03")


def _stub_table(*_args: object, **_kwargs: object) -> object:
    raise NotImplementedError("@bv.table lands in Plan 03-03")


class _AppStub:
    def __init__(self, *_args: object, **_kwargs: object) -> None:
        raise NotImplementedError("bv.App lands in Plan 03-04+05")


event = _stub_event
table = _stub_table
App = _AppStub

__all__ = [
    "event",
    "table",
    "col",
    "Col",
    "App",
    "Optional",
    "Field",
    "ValidationError",
    "RegistrationError",
    "BinaryNotFoundError",
]
