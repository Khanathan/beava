"""beava.test — pytest fixtures and assertion helpers for Beava users.

Public surface (per docs/sdk-api/python.md § bv.test fixtures):

- :func:`fixture` — pytest-shaped fixture yielding ``bv.App`` (embed mode,
  ``test_mode=True`` default per Phase 13.5 D-05).
- :func:`replay` — feed a list of event dicts into ``App.push`` in order.
- :func:`assert_features_eq` — feature-dict comparison with float tolerance.
- :class:`MockApp` — in-memory test double of ``bv.App``.
"""
from __future__ import annotations

from beava.test._assertions import assert_features_eq
from beava.test._fixtures import fixture
from beava.test._mock import MockApp
from beava.test._replay import replay

__all__ = ["fixture", "replay", "assert_features_eq", "MockApp"]
