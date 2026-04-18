"""
Push 10k synthetic fraud-scoring events via HTTP /push-batch/{stream}.

No Python SDK required — only the `requests` library. This demonstrates
the language-agnostic HTTP ingest path that any client can use.

Usage:
    pip install requests
    python push_synthetic.py
    # Or override the server URL:
    HTTP_BASE=http://localhost:6900 python push_synthetic.py
"""
import os
import random
import time

import requests

HTTP = os.environ.get("HTTP_BASE", "http://localhost:6900")
N_TRANSACTIONS = 10_000
N_DEVICES = 500
N_LOGINS = 1_000
N_USERS = 100
BATCH_SIZE = 500

MERCHANTS = [
    "amazon", "uber", "doordash", "shell_gas", "walmart",
    "target", "starbucks", "netflix", "apple_store", "airbnb",
]


def user_id(i: int) -> str:
    return f"u{i:04d}"


def push_batch(stream: str, events: list) -> None:
    """Push a list of events in BATCH_SIZE chunks."""
    for i in range(0, len(events), BATCH_SIZE):
        chunk = events[i:i + BATCH_SIZE]
        resp = requests.post(
            f"{HTTP}/push-batch/{stream}",
            json=chunk,
            timeout=30,
        )
        resp.raise_for_status()
    print(f"  Pushed {len(events)} events to {stream}")


def main() -> None:
    now_ms = int(time.time() * 1000)

    # -- Transaction events --------------------------------------------------
    transactions = []
    for i in range(N_TRANSACTIONS):
        uid = user_id(i % N_USERS)
        et = now_ms - random.randint(0, 3_600_000)  # within the past hour
        transactions.append({
            "user_id": uid,
            "amount": round(random.expovariate(1 / 50), 2),
            "merchant": random.choice(MERCHANTS),
            "_event_time": et,
        })

    # -- Device events -------------------------------------------------------
    devices = []
    for i in range(N_DEVICES):
        uid = user_id(i % N_USERS)
        devices.append({
            "user_id": uid,
            "device_id": f"dev-{random.randint(1, 200):04d}",
            "ip": f"10.{random.randint(0,255)}.{random.randint(0,255)}.{random.randint(1,254)}",
            "_event_time": now_ms - random.randint(0, 86_400_000),
        })

    # -- Login events --------------------------------------------------------
    logins = []
    for i in range(N_LOGINS):
        uid = user_id(i % N_USERS)
        logins.append({
            "user_id": uid,
            "success": random.random() > 0.2,
            "ip": f"10.{random.randint(0,255)}.{random.randint(0,255)}.{random.randint(1,254)}",
            "_event_time": now_ms - random.randint(0, 600_000),  # past 10 min
        })

    print(f"Pushing synthetic events to {HTTP} ...")
    push_batch("Transaction", transactions)
    push_batch("Device", devices)
    push_batch("Login", logins)

    print()
    print("Done. Sample feature query:")
    print(f"  curl {HTTP}/features/u0001")


if __name__ == "__main__":
    main()
