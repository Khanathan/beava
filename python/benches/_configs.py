"""Pipeline-config loaders for the Phase 19 Python bench harness.

The Python harness POSTs the same JSON the Rust bench uses
(``crates/beava-bench/configs/*.json``) verbatim to ``/register`` so there is
zero semantic drift between the two harnesses. Translating the configs to
``@bv.event`` / ``@bv.table`` decorators would risk drift; reusing the JSON
keeps both harnesses on the same registration payload.

Per CONTEXT.md D-09: "the Python harness wants to drive the SAME register
payload the Rust harness uses, NOT translate it through Python decorators".
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

# python/benches/_configs.py → tally/python/benches/_configs.py → repo root is two parents up.
_REPO_ROOT = Path(__file__).resolve().parents[2]
_CONFIGS_DIR = _REPO_ROOT / "crates" / "beava-bench" / "configs"


def load_pipeline_config(name: str) -> dict[str, Any]:
    """Load a pipeline JSON config.

    Accepts a short name (small / medium / large / large_phase9) which resolves to
    ``crates/beava-bench/configs/{name}.json``, OR a full path to a JSON file.

    Args:
        name: Short config name or absolute/relative JSON path.

    Returns:
        Parsed JSON config dict with keys ``register``, ``event_name``,
        ``key_field``, ``extra_fields``, ``features``.

    Raises:
        FileNotFoundError: Neither ``name`` nor ``configs/{name}.json`` exists.
    """
    path = Path(name)
    if not path.is_file():
        path = _CONFIGS_DIR / f"{name}.json"
    if not path.is_file():
        raise FileNotFoundError(
            f"pipeline config {name!r} not found at {path}"
        )
    with open(path, encoding="utf-8") as f:
        result: dict[str, Any] = json.load(f)
    return result


def event_name(cfg: dict[str, Any]) -> str:
    """Return the primary event name (key into ``event_name`` field of config)."""
    return str(cfg["event_name"])


def key_field(cfg: dict[str, Any]) -> str:
    """Return the entity-key field name (``user_id`` for the v0 configs)."""
    return str(cfg["key_field"])


def extra_fields(cfg: dict[str, Any]) -> dict[str, str]:
    """Return the non-key, non-event_time scalar field types as ``{name: type}``.

    Example for ``small.json``: ``{"amount": "f64"}``.
    """
    return dict(cfg["extra_fields"])


def register_payload(cfg: dict[str, Any]) -> bytes:
    """The exact JSON bytes to POST to ``/register`` — verbatim Rust-harness payload."""
    return json.dumps(cfg["register"], ensure_ascii=False).encode("utf-8")
