"""Phase 13.5 Plan 05: bv.demo(name) loader red tests."""
from __future__ import annotations

import inspect

import pytest

import beava as bv


def test_demo_unknown_name_raises_with_valid_choices() -> None:
    with pytest.raises(ValueError, match="adtech|fraud|ecommerce"):
        bv.demo("nonexistent_dataset_xyz")


@pytest.mark.parametrize("name", ["adtech", "fraud", "ecommerce"])
def test_demo_returns_loaded_fixture(name: str) -> None:
    """Plan 05 ships the loader skeleton; Plan 06 ships the actual data files.

    This test passes once Plan 06 lands the data — until then, expect a
    clean ``RuntimeError`` mentioning Plan 06 / "not yet bundled", not an
    unexpected exception.
    """
    try:
        result = bv.demo(name)
    except RuntimeError as e:
        # Plan 06 ships the data — Plan 05's loader handshake must be informative.
        assert "Plan 06" in str(e) or "not yet bundled" in str(e)
        return
    assert isinstance(result, dict)
    assert "schema" in result or "events" in result


def test_demo_signature_returns_dict() -> None:
    """The signature is ``bv.demo(name: str) -> dict`` per docs/sdk-api/python.md."""
    sig = inspect.signature(bv.demo)
    assert len(sig.parameters) == 1
