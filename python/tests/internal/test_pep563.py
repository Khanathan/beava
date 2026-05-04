"""Phase 13.5 Plan 05: PEP 563 (deferred annotation evaluation) regression test.

When the user writes ``from __future__ import annotations``, the class body's
annotations are strings. ``@bv.event`` must call ``typing.get_type_hints()``
so the schema dict contains real types — not bare strings or ``ForwardRef``.
"""
from __future__ import annotations

from typing import Optional

import beava as bv


def test_event_class_under_future_annotations_extracts_real_types() -> None:
    """Schema dict must contain real ``str``/``float`` types, not raw strings."""

    @bv.event
    class _PEP563Test:
        user_id: str
        amount: float
        merchant: str

    schema = _PEP563Test._schema
    # Must not be the raw string forms.
    for k, v in schema.items():
        assert not isinstance(v, str), (
            f"Schema field {k!r} is a string — PEP 563 fix not applied. "
            f"Switch _make_event_source to use typing.get_type_hints()."
        )
    # The 3 fields must have their real types.
    assert schema["user_id"] is str
    assert schema["amount"] is float
    assert schema["merchant"] is str


def test_event_class_with_optional_field() -> None:
    """``Optional[str]`` resolves under PEP-563 deferred evaluation."""

    @bv.event
    class _PEP563Optional:
        user_id: str
        ip: Optional[str]  # nullable per shared.md § Field types

    schema = _PEP563Optional._schema
    assert "ip" in schema


# Module-level event source for the function-form derivation test below.
# (Function-form @bv.event resolves parameter annotations via fn.__globals__,
# which only sees module-scope names — not class/function-local scope. Real
# user code follows this pattern, so the test does too.)
@bv.event
class _PEPSrcModule:
    user_id: str
    amount: float


@bv.event
def _PEPDerivModule(src: _PEPSrcModule):
    return src.filter(bv.col("amount") > 0)


def test_event_function_form_under_future_annotations() -> None:
    """Function-form @bv.event resolves the parameter annotation under PEP 563."""
    assert _PEPDerivModule._chain[-1]["op"] == "filter"
    assert _PEPDerivModule._name == "_PEPDerivModule"
