"""beava.test — pytest fixtures and assertion helpers (Plan 07 populates).

Phase 13.5 Plan 05 ships this empty submodule so import order does not
break. Plan 07 adds:

- ``fixture()`` — pytest fixture factory yielding a configured ``bv.App``
- ``replay(app, events)`` — replay helper for deterministic re-runs
- ``assert_features_eq(a, b)`` — cross-language feature equality
- ``MockApp`` — in-memory App stub for unit tests
"""
from __future__ import annotations

# Plan 07 will populate:
# from beava.test._fixtures import fixture, replay, assert_features_eq, MockApp

__all__: list[str] = []
