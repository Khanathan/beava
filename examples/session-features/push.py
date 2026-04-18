"""
Push 1000 synthetic click events to http://localhost:6900/push-batch/Click.

No Python SDK required — only the `requests` library.

Usage:
    pip install requests
    python push.py
    # Or override server URL:
    HTTP_BASE=http://localhost:6900 python push.py
"""
import os
import random
import time

import requests

HTTP = os.environ.get("HTTP_BASE", "http://localhost:6900")
N_EVENTS = 1_000
N_SESSIONS = 20
PAGES = ["/home", "/search", "/product/42", "/cart", "/checkout", "/confirm"]
BATCH_SIZE = 200


def main() -> None:
    now_ms = int(time.time() * 1000)
    events = []
    for i in range(N_EVENTS):
        events.append({
            "session_id": f"session-{i % N_SESSIONS:03d}",
            "page": random.choice(PAGES),
            "duration_ms": random.randint(50, 5_000),
            "_event_time": now_ms - random.randint(0, 300_000),  # past 5 min
        })

    # Push in batches
    for i in range(0, len(events), BATCH_SIZE):
        chunk = events[i:i + BATCH_SIZE]
        resp = requests.post(
            f"{HTTP}/push-batch/Click",
            json=chunk,
            timeout=30,
        )
        resp.raise_for_status()

    print(f"Pushed {N_EVENTS} click events across {N_SESSIONS} sessions.")
    print(f"Try: curl {HTTP}/features/session-001")


if __name__ == "__main__":
    main()
