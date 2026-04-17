"""Shared pipeline definition for the fork-replay bench.

Kept deliberately small — this bench measures how quickly a fork can
ingest events, not the operator cost under the complex fraud pipeline.
A single stream with one rolling-count table gives a clean per-event
signal and keeps state memory bounded.
"""

from __future__ import annotations

import os
import sys

_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(_HERE, "..", "..", "python"))

import beava as bv  # noqa: E402


@bv.stream
class Event:
    user_id: str
    amount: float


@bv.table(key="user_id")
def UserCounts(e: Event) -> bv.Table:
    return e.group_by("user_id").agg(
        count_1h=bv.count(window="1h"),
    )
