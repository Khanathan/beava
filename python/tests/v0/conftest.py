"""Pytest fixtures for v0 user-facing operator integration tests.

Provides:
  - app fixture: yields a fresh ``bv.App()`` embed-mode instance per test.
  - _engine_available() helper: returns False until both Phase 13.4 (engine
    prep — output_kind=table support, Polars-renamed ops) AND Phase 13.5
    (Python SDK rewrite — full 53-op helper surface) ship. Used by every
    test_*.py file's module-level skipif marker so 13.0 pytest collection
    succeeds without ImportError but no test executes.

Skip detection contract (per Plan 13.0-16):
  An "engine available" environment must satisfy ALL of:
    1. The bv module exposes the Polars-renamed core helpers (mean / var /
       std / n_unique / quantile) — Phase 13.4 + 13.5 deliverable.
    2. ``bv.table`` is exposed in the public namespace (ADR-001 partial
       overturn — aggregation-output decorator only) — Phase 13.5 deliverable.
    3. The beava server binary is on PATH (or BEAVA_BINARY env var is set)
       so embed mode can spawn the subprocess — Phase 13.4 deliverable.

If any condition is unmet, _engine_available() returns False and every test
in tests/v0/ is skipped at collection time.

Cold-start equivalence:
  ``app.get(table, "unknown_entity") == {}`` is the contract for
  "no state for this key". v0 may surface QueryNotFound as either an empty
  dict ``{}`` or ``None`` depending on transport (see _app.py::App.get).
  Tests use the cold_start_equivalent helper to accept either shape.
"""
from __future__ import annotations

import os
import shutil
from pathlib import Path
from typing import Any, Generator

import pytest


def _engine_available() -> bool:
    """Return True iff the v0 engine + SDK are ready to run these tests.

    Returns False until Phase 13.4 + 13.5 ship. Used as the skipif condition
    for every test_*.py file in this directory.
    """
    # Condition 1 + 2: Polars-renamed helpers + bv.table must be in the public namespace.
    try:
        import beava as bv  # noqa: PLC0415
    except Exception:
        return False
    required_helpers = ("mean", "var", "std", "n_unique", "quantile", "table")
    for name in required_helpers:
        if not hasattr(bv, name):
            return False

    # Condition 3: beava binary discoverable (BEAVA_BINARY env var, on PATH,
    # or in target/debug under the repo root).
    if os.environ.get("BEAVA_BINARY"):
        candidate = Path(os.environ["BEAVA_BINARY"])
        if not (candidate.is_file() and os.access(candidate, os.X_OK)):
            return False
    elif shutil.which("beava") is None:
        # Fallback: dev-loop convenience — walk upward looking for target/debug/beava.
        found = False
        for parent in [Path.cwd(), *Path.cwd().parents]:
            cand = parent / "target" / "debug" / "beava"
            if cand.is_file() and os.access(cand, os.X_OK):
                found = True
                break
        if not found:
            return False

    return True


def cold_start_equivalent(value: Any) -> bool:
    """Return True iff *value* represents a cold-start (no state) result.

    v0 transports may surface QueryNotFound as either {} or None depending on
    the codepath (msgpack/JSON/embed). Both are valid cold-start shapes for
    a per-entity table; tests treat them identically.
    """
    return value == {} or value is None


@pytest.fixture
def app() -> Generator[Any, None, None]:
    """Yield a fresh ``bv.App(test_mode=True)`` embed-mode instance per test.

    Per Phase 13.5.1 D-05 (USER-LOCKED): every v0 acceptance test runs against
    a real spawned subprocess with BEAVA_TEST_MODE=1, so app.reset() is callable
    and OP_RESET frames are accepted by the engine. NO mock-object against the
    Transport surface — that anti-pattern masked the 0/68 deficit at Phase 13.5
    Plan 11 close.

    The fixture spawns a local beava subprocess on ephemeral ports via the
    no-URL ``bv.App(test_mode=True)`` embed-mode path (uses python/beava/_embed.py
    machinery; passes test_mode through to spawn_embedded_server which sets
    env["BEAVA_TEST_MODE"]="1"), yields the entered context manager, and tears
    down on test exit.

    Tests should NOT close the app explicitly — the fixture handles teardown.
    """
    import beava as bv  # noqa: PLC0415

    with bv.App(test_mode=True) as instance:
        yield instance


# ---------------------------------------------------------------------------
# Pytest configuration
# ---------------------------------------------------------------------------


def pytest_configure(config: pytest.Config) -> None:
    """Register custom markers used across tests/v0/."""
    config.addinivalue_line(
        "markers",
        "high_volume: tests that push >=500 events per test "
        "(every test in tests/v0/ qualifies)",
    )


# Per-test timeout: each test pushes hundreds-to-thousands of events; 30s is
# generous against the real engine post-13.4. Tests skip during 13.0 so this
# only matters once execution is enabled.
def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    """Apply the high_volume marker + 30s timeout to every test in tests/v0/."""
    high_volume = pytest.mark.high_volume
    for item in items:
        if "tests/v0/" in str(item.fspath) or "tests\\v0\\" in str(item.fspath):
            item.add_marker(high_volume)
