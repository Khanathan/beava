"""beava.test.replay — feed a list of events into App.push in order."""
from __future__ import annotations

from typing import Any


def replay(app: Any, events: list[dict[str, Any]]) -> None:
    """For each event dict in ``events``, call ``app.push(<event_name>, <fields>)``.

    Each event dict must have an ``"_event"`` key naming the event source.
    The remaining keys form the fields payload.
    """
    for ev in events:
        if "_event" not in ev:
            raise ValueError(f"replay event missing '_event' key: {ev!r}")
        name = ev["_event"]
        fields = {k: v for k, v in ev.items() if k != "_event"}
        app.push(name, fields)
