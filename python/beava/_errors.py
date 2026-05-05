"""Error types for the Beava SDK.

Public exports (re-exported from :mod:`beava`):

- :class:`ValidationError` — frozen dataclass for client- and server-side
  validation errors.
- :class:`RegistrationError` — raised when registration fails (locally or
  on the server).
- :class:`BinaryNotFoundError` — raised when the beava binary cannot be
  located in embed mode.
"""

from __future__ import annotations

from dataclasses import dataclass

# Per `project_redis_shaped_no_event_time_ever` (locked 2026-04-30), the SDK
# is processing-time only in v0; @bv.event rejects `event_time` field
# declarations at decorator time, so the matching validation kind is absent
# from this set on purpose.
VALIDATION_ERROR_KINDS: frozenset[str] = frozenset({
    "cycle",
    "missing_upstream",
    "schema_mismatch",
    "bad_return_type",
    "unknown_field_type",
    "table_key_invalid",
    "registration_conflict",
    "duplicate_name",
})


@dataclass(frozen=True)
class ValidationError:
    """Structured validation error produced by local or server-side schema checks.

    Attributes:
        kind: One of the kinds in VALIDATION_ERROR_KINDS.
        path: JSON-pointer-style path to the offending field, e.g. "Transaction.event_time".
        message: Human-readable description of the problem.
    """

    kind: str
    path: str
    message: str

    def __str__(self) -> str:
        return f"[{self.kind}] {self.path}: {self.message}"


class RegistrationError(Exception):
    """Raised when registration fails — either locally (DAG/schema) or on the server (409).

    Attributes:
        code: Machine-readable error code, typically one of VALIDATION_ERROR_KINDS.
        path: Path to the offending descriptor or field.
        message: Human-readable description.
        errors: Full list of ValidationError entries when the server returns multiple errors.
    """

    def __init__(
        self,
        *,
        code: str,
        path: str = "",
        message: str = "",
        errors: list[ValidationError] | None = None,
    ) -> None:
        self.code = code
        self.path = path
        self.message = message
        self.errors: list[ValidationError] = errors if errors is not None else []
        if path:
            super().__init__(f"[{code}] {path}: {message}")
        else:
            super().__init__(f"[{code}] {message}")


class BinaryNotFoundError(Exception):
    """Raised by embed mode (_embed.py) when the beava binary cannot be located.

    Discovery order:
      1. BEAVA_BINARY env var
      2. 'beava' on PATH
      3. ./target/debug/beava (dev-loop convenience)
      4. This exception with an install-guidance message.
    """

    pass
