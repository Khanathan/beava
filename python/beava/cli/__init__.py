"""beava.cli — CLI entry point (Plan 08 wires ``beava bench`` subcommand).

Phase 13.5 Plan 05 ships this empty submodule so the entry-point ID is
reserved and Plan 08 can drop in the argparse subcommand graph without
restructuring.
"""
from __future__ import annotations


def main() -> None:
    """Placeholder. Plan 08 wires the actual entry point + argparse subcommands."""
    raise SystemExit(
        "beava.cli main() is not wired in this plan; Plan 08 adds "
        "`beava bench` subcommands."
    )


__all__ = ["main"]
