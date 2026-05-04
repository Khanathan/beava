"""Phase 13.5 Plan 04: ADR-002 deprecation alias regression tests.

The 5 deprecation aliases (``avg`` / ``variance`` / ``stddev`` /
``count_distinct`` / ``percentile``) MUST emit a ``DeprecationWarning``
referencing the new Polars-style name and MUST forward to the renamed
helper such that the AggDescriptor's ``op`` field equals the new name.
"""
from __future__ import annotations

import warnings

import pytest

import beava as bv


@pytest.mark.parametrize(
    "alias_name,new_name",
    [
        ("avg", "mean"),
        ("variance", "var"),
        ("stddev", "std"),
        ("count_distinct", "n_unique"),
    ],
)
def test_alias_emits_deprecation_warning(
    alias_name: str, new_name: str
) -> None:
    alias_fn = getattr(bv, alias_name)
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        result = alias_fn("amount", window="1h")
    deprecation = [w for w in caught if issubclass(w.category, DeprecationWarning)]
    assert deprecation, f"{alias_name} did not emit DeprecationWarning"
    msgs = [str(w.message) for w in deprecation]
    assert any(new_name in m for m in msgs), (
        f"DeprecationWarning for bv.{alias_name} must reference new name "
        f"{new_name!r}; got messages={msgs!r}"
    )
    # Forwards to the renamed helper.
    assert result.to_dict()["op"] == new_name


def test_percentile_alias_to_quantile() -> None:
    """bv.percentile uses ``p=`` (old SQL convention); bv.quantile uses ``q=``."""
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        d = bv.percentile("amount", p=0.99, window="1h")
    deprecation = [w for w in caught if issubclass(w.category, DeprecationWarning)]
    assert deprecation, "bv.percentile did not emit DeprecationWarning"
    out = d.to_dict()
    assert out["op"] == "quantile"
    assert out["q"] == 0.99
