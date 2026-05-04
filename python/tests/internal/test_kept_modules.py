"""Phase 13.5 Plan 01: kept-module + opcode regression tripwire.

After the deletion sweep, these imports must succeed and the OP_PUSH wire opcode
must equal 0x0010 (matches docs/wire-spec.md § OP_PUSH and crates/beava-server src
dispatch table).
"""
from __future__ import annotations

import importlib

import pytest


def test_op_push_opcode_value() -> None:
    """OP_PUSH must equal 0x0010 (Phase 13.5 D-CARRYOVER bug fix from prior surface)."""
    from beava import _wire

    assert _wire.OP_PUSH == 0x0010, (
        f"OP_PUSH must be 0x0010 per docs/wire-spec.md, got {_wire.OP_PUSH:#06x}"
    )


def test_kept_modules_import() -> None:
    """The 4 modules NOT deleted in Plan 01 must continue to import cleanly."""
    for mod_name in ["beava._wire", "beava._transport", "beava._errors", "beava._embed"]:
        importlib.import_module(mod_name)


@pytest.mark.parametrize(
    "deleted",
    [
        # ``_col`` and ``_events`` are reintroduced by Plan 03 (rewritten DSL);
        # ``_agg`` is reintroduced by Plan 04 (53 op helpers). These must stay
        # deleted: legacy schema/validate/eval_reference modules whose surface
        # is replaced by the new pipeline DSL.
        "beava._schema",
        "beava._validate",
        "beava._eval_reference",
    ],
)
def test_deleted_modules_raise(deleted: str) -> None:
    """Plan 01 deletes these. Importing them must raise ModuleNotFoundError."""
    with pytest.raises(ModuleNotFoundError):
        importlib.import_module(deleted)
