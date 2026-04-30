"""Error types for the beava SDK.

Public exports (re-exported from beava.__init__):
  - ValidationError: frozen dataclass for client-side and server-side validation errors
  - RegistrationError: Exception raised when registration fails (local or server)
  - BinaryNotFoundError: Exception raised when the beava binary cannot be found (embed mode)

Module-level:
  - VALIDATION_ERROR_KINDS: frozenset of all valid ValidationError.kind values
"""

from __future__ import annotations

from dataclasses import dataclass

# Plan 12.6-08 (no-event-time pivot, 2026-04-30): event_time_field_invalid
# kind removed; the SDK no longer issues it because the @bv.event decorator
# rejects event_time field declarations at decorator time (TypeError).
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
