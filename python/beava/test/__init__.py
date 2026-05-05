"""``beava.test`` — pytest fixtures and assertion helpers.

Public surface:

- :func:`fixture` — pytest-shaped fixture yielding a ``bv.App`` (embed
  mode, ``test_mode=True`` by default).
- :func:`replay` — feed a list of event dicts into ``App.push`` in order.
- :func:`assert_features_eq` — feature-dict comparison with float
  tolerance.
- :class:`MockApp` — in-memory test double of ``bv.App``.
"""
from __future__ import annotations

from beava.test._assertions import assert_features_eq
from beava.test._fixtures import fixture
from beava.test._mock import MockApp
from beava.test._replay import replay

__all__ = ["fixture", "replay", "assert_features_eq", "MockApp"]
