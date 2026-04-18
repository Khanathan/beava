"""
Fraud-scoring pipeline (HTTP variant of benchmark/fraud-pipeline/).

Registers 3 streams + 2 tables with representative fraud features. Designed to
run against a fresh `docker run -p 6900:6900 -p 6400:6400 beavadb/beava:latest`.

This is the HTTP-first variant: pipeline registration uses the Python SDK over
TCP (port 6400), while event ingest and feature reads use HTTP (port 6900).
See push_synthetic.py for the HTTP ingest path.

Usage:
    pip install requests
    python pipeline.py           # register pipeline via Python SDK (TCP 6400)
    python push_synthetic.py     # push 10k synthetic events via HTTP 6900
    curl http://localhost:6900/features/u0001
"""
import os
import sys

# Allow running directly from the repo without `pip install beava`
_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(_HERE, "..", "..", "python"))

import beava as bv  # noqa: E402


# ---------------------------------------------------------------------------
# Stream definitions
# ---------------------------------------------------------------------------

@bv.stream
class Transaction:
    """A payment transaction event."""
    user_id: str
    amount: float
    merchant: str
    # _event_time is auto-detected from the payload; omit for server wall-clock


@bv.stream
class Device:
    """A device-association event (user linked to a device + IP)."""
    user_id: str
    device_id: str
    ip: str


@bv.stream
class Login:
    """A login attempt event."""
    user_id: str
    success: bool
    ip: str


# ---------------------------------------------------------------------------
# Table definitions
# ---------------------------------------------------------------------------

@bv.table(key="user_id")
def UserFraudScore(t: Transaction) -> bv.Table:
    """25+ fraud features keyed by user_id: velocity, volume, diversity."""
    return t.group_by("user_id").agg(
        tx_count_30m=bv.count(window="30m"),
        tx_count_1h=bv.count(window="1h"),
        tx_count_24h=bv.count(window="24h"),
        tx_sum_1h=bv.sum("amount", window="1h"),
        tx_sum_24h=bv.sum("amount", window="24h"),
        tx_avg_1h=bv.avg("amount", window="1h"),
        tx_avg_24h=bv.avg("amount", window="24h"),
        tx_max_1h=bv.max("amount", window="1h"),
        tx_max_24h=bv.max("amount", window="24h"),
        tx_min_24h=bv.min("amount", window="24h"),
        tx_stddev_24h=bv.stddev("amount", window="24h"),
        unique_merchants_1h=bv.count_distinct("merchant", window="1h"),
        unique_merchants_24h=bv.count_distinct("merchant", window="24h"),
        last_merchant=bv.last("merchant"),
        last_amount=bv.last("amount"),
    )


@bv.table(key="user_id")
def UserLoginPattern(l: Login) -> bv.Table:
    """Login velocity features keyed by user_id."""
    return l.group_by("user_id").agg(
        login_count_10m=bv.count(window="10m"),
        login_count_1h=bv.count(window="1h"),
        failed_count_10m=bv.count(window="10m"),
        last_ip=bv.last("ip"),
    )


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    tcp_host = os.environ.get("BEAVA_TCP_HOST", "localhost:6400")
    app = bv.App(tcp_host)
    app.register(Transaction, Device, Login, UserFraudScore, UserLoginPattern)
    print(f"Pipeline registered on {tcp_host}")
    print("Streams: Transaction, Device, Login")
    print("Tables:  UserFraudScore (15 features), UserLoginPattern (4 features)")
