"""
Registers the Transactions stream with windowed count + sum features via the
Beava HTTP /pipelines endpoint.

This script intentionally uses the HTTP management API (not the TCP SDK
App.register path) because:
  - The HTTP endpoint accepts the v2.0 flat JSON pipeline definition directly.
  - It works without the Python SDK installed — only the stdlib `urllib` is used.
  - It matches the pattern that run.sh uses as its fallback.

Usage:
    python3 sample-pipeline.py [http_port]

Default HTTP port: 6401.
"""

import json
import sys
import urllib.request

PIPELINE = {
    "name": "Transactions",
    "key_field": "user",
    "definition_type": "stream",
    "features": [
        {"name": "tx_count_1h", "type": "count",  "window": "1h", "bucket": "1m"},
        {"name": "tx_sum_1h",   "type": "sum",  "field": "amount", "window": "1h", "bucket": "1m"},
    ],
}

if __name__ == "__main__":
    http_port = int(sys.argv[1]) if len(sys.argv) > 1 else 6401
    token = sys.argv[2] if len(sys.argv) > 2 else "test-admin"
    url = f"http://127.0.0.1:{http_port}/pipelines"
    data = json.dumps(PIPELINE).encode()
    req = urllib.request.Request(
        url,
        data=data,
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {token}",
        },
        method="POST",
    )
    with urllib.request.urlopen(req) as resp:
        body = resp.read().decode()
    print(f"registered Transactions on localhost:{http_port}: {body}")
