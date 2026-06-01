"""TDD red — time-builtin Python sugar (PR 3 Step 1).

What this file checks: ``bv.hour_of_day(ts)`` produces ``hour_of_day(ts)``
on the wire.

Status today: fails because ``bv.hour_of_day`` doesn't exist yet.
Step 5 of PR 3 adds it.
"""
from __future__ import annotations

import beava as bv


def test_hour_of_day_with_column() -> None:
    """Standard case: extract the hour (0-23) from a timestamp column.
    Used in features like "clicks_per_hour" or "is_business_hours"."""
    expr = bv.hour_of_day(bv.col("ts"))
    assert expr.to_expr_string() == "hour_of_day(ts)"
