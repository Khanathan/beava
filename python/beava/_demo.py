"""``bv.demo(name)`` — bundled demo dataset loader.

Loads bundled demo datasets (``adtech`` / ``fraud`` / ``ecommerce``)
shipped at ``python/beava/demos/<name>/{schema.json, events.jsonl}``.
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
        On unknown ``name``; the message lists the valid choices.
    RuntimeError
        Dataset files are not bundled in this install.
    """
    if name not in _VALID_DEMOS:
        raise ValueError(
            f"Unknown demo {name!r}. Valid: {_VALID_DEMOS}."
        )
    try:
        ref = importlib.resources.files("beava.demos") / name
        schema_path = ref / "schema.json"
        events_path = ref / "events.jsonl"
        if not schema_path.is_file() or not events_path.is_file():
            raise RuntimeError(
                f"Demo {name!r} dataset files not bundled in this install."
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
            f"Demo {name!r} dataset files not bundled: {e}"
        ) from e
    except ModuleNotFoundError as e:
        raise RuntimeError(
            f"Demo {name!r} not bundled (beava.demos package missing): {e}"
        ) from e
