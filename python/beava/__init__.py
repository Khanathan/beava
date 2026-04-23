"""beava — Python SDK for the Beava real-time feature server.

Public API (Phase 3 Plan 03-05):
  - Optional: nullable field marker (distinct from typing.Optional)
  - Field: per-field metadata factory
  - ValidationError: frozen dataclass for schema/DAG validation errors
  - RegistrationError: Exception for registration failures
  - BinaryNotFoundError: Exception for missing beava binary (embed mode)
  - col: expression DSL constructor (Plan 03-02)
  - Col: _ExprAST base class for isinstance checks (Plan 03-02)
  - event: @bv.event decorator (class + function form) (Plan 03-03)
  - table: @bv.table decorator (class + function form) (Plan 03-03)
  - parse_url_to_transport: URL-scheme dispatch (Plan 03-04)
  - App: sync client — register, validate, ping (Plan 03-05)
"""

from ._agg import (
    AggDescriptor,
    GroupBy,
    avg,
    count,
    max,
    min,
    ratio,
    stddev,
    sum,
    variance,
)
from ._app import App
from ._col import Col, col
from ._errors import (
    BinaryNotFoundError,
    RegistrationError,
    ValidationError,
)
from ._events import event
from ._tables import table
from ._transport import parse_url_to_transport
from ._types import Field, Optional

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
    "parse_url_to_transport",
    # Aggregation helpers (SDK-AGG-01..06)
    "AggDescriptor",
    "GroupBy",
    "count",
    "sum",
    "avg",
    "min",
    "max",
    "variance",
    "stddev",
    "ratio",
]
