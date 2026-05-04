"""bv.demo(name) — Phase 13.5 Plan 05.

Loads bundled demo datasets (``adtech`` / ``fraud`` / ``ecommerce``) shipped
at ``python/beava/demos/<name>/{schema.json, events.jsonl}``. Plan 06
generates the actual dataset files; Plan 05 ships the loader skeleton so
that import order does not break and the user-facing error path is
informative when the data files are absent.
"""
from __future__ import annotations

import importlib.resources
import json
from typing import Any

_VALID_DEMOS = ("adtech", "fraud", "ecommerce")


def demo(name: str) -> dict[str, Any]:
    """Load a bundled demo dataset.

    Returns
    -------
    ``{"name": <name>, "schema": <list of descriptors>, "events": <list of events>}``

    Raises
    ------
    ValueError
        On unknown ``name``; message lists the 3 valid choices.
    RuntimeError
        If dataset files are not bundled (Plan 06 ships them).
    """
    if name not in _VALID_DEMOS:
        raise ValueError(
            f"Unknown demo {name!r}. Valid: {_VALID_DEMOS}. "
            f"See docs/sdk-api/python.md § bv.demo for what each dataset exercises."
        )
    try:
        ref = importlib.resources.files("beava.demos") / name
        schema_path = ref / "schema.json"
        events_path = ref / "events.jsonl"
        if not schema_path.is_file() or not events_path.is_file():
            raise RuntimeError(
                f"Demo {name!r} not yet bundled — Plan 06 ships the actual data files."
            )
        with schema_path.open("r") as f:
            schema = json.load(f)
        events: list[dict[str, Any]] = []
        with events_path.open("r") as f:
            for raw in f:
                line = raw.strip()
                if line:
                    events.append(json.loads(line))
        return {"name": name, "schema": schema, "events": events}
    except FileNotFoundError as e:
        raise RuntimeError(
            f"Demo {name!r} not yet bundled (Plan 06 deliverable): {e}"
        ) from e
    except ModuleNotFoundError as e:
        # ``beava.demos`` package not yet a package on disk
        raise RuntimeError(
            f"Demo {name!r} not yet bundled (Plan 06 deliverable; "
            f"beava.demos package missing): {e}"
        ) from e
