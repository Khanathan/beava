"""beava.test.assert_features_eq — feature-dict comparison with float tolerance.

Sketch / decay / EWMA ops are not bitwise stable across runs (insertion
order, floating-point accumulation), so feature-by-feature equality uses
``math.isclose`` with a tight relative tolerance.
"""
from __future__ import annotations

import math
from typing import Any


def assert_features_eq(
    got: dict[str, Any],
    want: dict[str, Any],
    *,
    rel_tol: float = 1e-9,
    abs_tol: float = 1e-12,
) -> None:
    """Assert two feature dicts are equal. Floats compared with ``math.isclose``."""
    got_keys = set(got)
    want_keys = set(want)
    if got_keys != want_keys:
        missing = want_keys - got_keys
        extra = got_keys - want_keys
        raise AssertionError(
            f"feature dicts differ — missing keys: {missing}; extra keys: {extra}"
        )
    for k in want:
        gv = got[k]
        wv = want[k]
        if isinstance(gv, float) or isinstance(wv, float):
            if not math.isclose(
                float(gv), float(wv), rel_tol=rel_tol, abs_tol=abs_tol
            ):
                raise AssertionError(
                    f"feature {k!r} differ: got={gv!r} want={wv!r} "
                    f"(rel_tol={rel_tol}, abs_tol={abs_tol})"
                )
        else:
            if gv != wv:
                raise AssertionError(
                    f"feature {k!r} differ: got={gv!r} want={wv!r}"
                )
