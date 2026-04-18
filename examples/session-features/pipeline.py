"""
Session features — last-N-click + count + sum per session.

This is the simplest working Beava pipeline: one stream, one table, a few
windowed aggregations. Read this in under 2 minutes and you have understood
the core model.

Usage:
    pip install requests
    python pipeline.py      # register the pipeline (TCP 6400)
    python push.py          # push 1000 synthetic click events (HTTP 6900)
    curl http://localhost:6900/features/session-001
"""
import os
import sys

# Allow running directly from the repo without `pip install beava`
_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(_HERE, "..", "..", "python"))

import beava as bv  # noqa: E402


# ---------------------------------------------------------------------------
# Stream definition
# ---------------------------------------------------------------------------

@bv.stream
class Click:
    """A single page-click event in a user session."""
    session_id: str
    page: str
    duration_ms: int


# ---------------------------------------------------------------------------
# Table definition
# ---------------------------------------------------------------------------

@bv.table(key="session_id", ttl="30m")
def SessionFeatures(c: Click) -> bv.Table:
    """
    Per-session feature table. Evicts sessions idle for 30 minutes.

    Features:
        clicks_5m           — click count in the past 5 minutes
        total_duration_5m   — total time-on-page in the past 5 minutes (ms)
        last_3_pages        — the 3 most recent pages visited (ordinal)
        first_page          — the very first page in this session (ordinal)
    """
    return c.group_by("session_id").agg(
        clicks_5m=bv.count(window="5m"),
        total_duration_5m=bv.sum("duration_ms", window="5m"),
        last_3_pages=bv.last_n("page", n=3),
        first_page=bv.first("page"),
    )


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    tcp_host = os.environ.get("BEAVA_TCP_HOST", "localhost:6400")
    app = bv.App(tcp_host)
    app.register(Click, SessionFeatures)
    print(f"Registered: Click → SessionFeatures (TTL 30m, keyed by session_id)")
    print(f"  Features: clicks_5m, total_duration_5m, last_3_pages, first_page")
