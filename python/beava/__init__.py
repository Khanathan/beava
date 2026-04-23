"""beava — Python SDK for the Beava real-time feature server.

Public API (Phase 3 Plan 01 baseline):
  - Optional: nullable field marker (distinct from typing.Optional)
  - Field: per-field metadata factory
  - ValidationError: frozen dataclass for schema/DAG validation errors
  - RegistrationError: Exception for registration failures
  - BinaryNotFoundError: Exception for missing beava binary (embed mode)
  - event, table, col, App: stubs filled in by later plans in Phase 3
"""

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


def _stub_col(*_args: object, **_kwargs: object) -> object:
    raise NotImplementedError("bv.col lands in Plan 03-02")


class _AppStub:
    def __init__(self, *_args: object, **_kwargs: object) -> None:
        raise NotImplementedError("bv.App lands in Plan 03-04+05")


event = _stub_event
table = _stub_table
col = _stub_col
App = _AppStub

__all__ = [
    "event",
    "table",
    "col",
    "App",
    "Optional",
    "Field",
    "ValidationError",
    "RegistrationError",
    "BinaryNotFoundError",
]
