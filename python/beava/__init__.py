"""Beava Python SDK — Phase 13.5 Plan 01 minimal foundation.

The five-module surface (`__init__`, `_wire`, `_transport`, `_errors`, `_embed`)
is intentionally bare after Plan 01 deletes the stale pre-Phase-13.0 surface.

Plans 02-07 re-populate the public namespace:
  - Plan 02: bv.App core + URL-scheme dispatch + test_mode kwarg
  - Plan 03: pipeline DSL — bv.col, bv.lit, @bv.event, @bv.table
  - Plan 04: 53 op helpers + ADR-002 deprecation aliases
  - Plan 05: PEP 563 fix + bv.demo loader + beava.test/cli submodules
  - Plan 06: in-package demo datasets
  - Plan 07: beava.test fixtures + replay + MockApp

v0 ships events-only per `project_v0_events_only_scope` (locked 2026-04-30,
ADR-001 partial overturn 2026-05-03 revives @bv.table for aggregation-output).
"""

from __future__ import annotations

# Re-exports from kept modules only:
from beava._app import App  # noqa: F401
from beava._col import col, lit  # noqa: F401
from beava._errors import (  # noqa: F401
    BinaryNotFoundError,
    RegistrationError,
    ValidationError,
)

__all__ = [
    "App",
    "RegistrationError",
    "BinaryNotFoundError",
    "ValidationError",
    "col",
    "lit",
]
