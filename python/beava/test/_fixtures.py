"""``beava.test.fixture`` — pytest-shaped fixture for embed-mode App.

Defaults to ``test_mode=True`` so :meth:`bv.App.reset` is callable inside
the test body.

Usage::

    import pytest
    from beava.test import fixture

    @pytest.fixture
    def app():
        yield from fixture(reset_each=True)
"""
from __future__ import annotations

from collections.abc import Generator

import beava as bv


def fixture(
    *,
    reset_each: bool = True,
    test_mode: bool = True,
    url: str | None = None,
    timeout: float = 30.0,
) -> Generator[bv.App, None, None]:
    """Pytest-fixture-shaped generator yielding a ``bv.App``.

    Default behavior: spawns an embed-mode binary with ``test_mode=True`` so
    OP_RESET is allowed. If ``reset_each=True``, calls ``app.reset()`` after
    entering the context-manager so each test starts with a clean slate.
    """
    with bv.App(url=url, timeout=timeout, test_mode=test_mode) as app:
        if reset_each:
            try:
                app.reset()
            except (RuntimeError, Exception):
                # Fresh embed instances may not need explicit reset; ignore.
                pass
        yield app
