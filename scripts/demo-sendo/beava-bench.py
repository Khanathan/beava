#!/usr/bin/env python3
"""
beava-bench.py — async HTTP load generator for the Sendo demo (Scene 4).

Reads JSONL from stdin, posts in batches to /push-batch/{stream} at a target
rate, prints a live one-line counter, and exits with a p50/p99 summary.

Usage:
    cat otto_events.jsonl \\
      | python scripts/demo-sendo/beava-bench.py \\
          --rate 10000 \\
          --to http://localhost:6900/push-batch/events \\
          --duration 60

The URL points at the batch endpoint because that is how 10K EPS is
sustainable on the HTTP path in practice. The voiceover still says
"events come in over HTTP" — which is true.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import statistics
import sys
import time
from typing import List

try:
    import httpx
except ImportError:
    sys.stderr.write(
        "beava-bench requires httpx. Run: pip install httpx  "
        "(or use the venv created by setup.sh)\n"
    )
    sys.exit(2)


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser()
    ap.add_argument("--rate", type=int, required=True,
                    help="target events/second")
    ap.add_argument("--to", required=True,
                    help="target URL, e.g. http://localhost:6900/push-batch/events")
    ap.add_argument("--duration", type=int, default=60,
                    help="seconds to run (default 60)")
    ap.add_argument("--batch", type=int, default=500,
                    help="events per HTTP request (default 500)")
    ap.add_argument("--concurrency", type=int, default=8,
                    help="parallel HTTP workers (default 8)")
    ap.add_argument("--token", default=os.environ.get("BEAVA_ADMIN_TOKEN", "test-admin"),
                    help="bearer token (default test-admin or $BEAVA_ADMIN_TOKEN)")
    return ap.parse_args()


async def worker(name: int, queue: asyncio.Queue, url: str, token: str,
                 latencies_ms: List[float], sent_counter: List[int]) -> None:
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    async with httpx.AsyncClient(timeout=10.0) as client:
        while True:
            batch = await queue.get()
            if batch is None:
                queue.task_done()
                return
            payload = json.dumps(batch).encode()
            t0 = time.perf_counter()
            try:
                r = await client.post(url, content=payload, headers=headers)
                r.raise_for_status()
            except Exception as e:
                sys.stderr.write(f"worker {name}: {e}\n")
            else:
                latencies_ms.append((time.perf_counter() - t0) * 1000.0)
                sent_counter[0] += len(batch)
            finally:
                queue.task_done()


async def event_feeder(queue: asyncio.Queue, rate: int, batch_size: int,
                       duration: int) -> int:
    """Read JSONL from stdin, enqueue batches at ~rate events/sec."""
    stdin = sys.stdin.buffer
    batch: List[dict] = []
    total = 0
    started = time.perf_counter()
    next_emit = started
    per_batch_interval = batch_size / rate

    for line in stdin:
        line = line.strip()
        if not line:
            continue
        try:
            ev = json.loads(line)
        except json.JSONDecodeError:
            continue
        batch.append(ev)
        if len(batch) < batch_size:
            continue

        # Token-bucket pacing
        now = time.perf_counter()
        if now < next_emit:
            await asyncio.sleep(next_emit - now)
        next_emit += per_batch_interval

        await queue.put(batch)
        total += len(batch)
        batch = []

        if time.perf_counter() - started >= duration:
            break

    if batch:
        await queue.put(batch)
        total += len(batch)
    return total


async def printer(sent_counter: List[int], duration: int) -> None:
    started = time.perf_counter()
    last_sent = 0
    last_t = started
    while True:
        await asyncio.sleep(1.0)
        now = time.perf_counter()
        elapsed = now - started
        cur = sent_counter[0]
        rate = (cur - last_sent) / max(now - last_t, 1e-6)
        sys.stderr.write(
            f"\r[{elapsed:5.1f}s] sent={cur:>9d}  rate={rate:>8.0f}/s  "
        )
        sys.stderr.flush()
        last_sent = cur
        last_t = now
        if elapsed >= duration + 2:
            return


async def main() -> int:
    args = parse_args()
    queue: asyncio.Queue = asyncio.Queue(maxsize=args.concurrency * 4)
    latencies_ms: List[float] = []
    sent_counter = [0]

    workers = [
        asyncio.create_task(worker(i, queue, args.to, args.token,
                                   latencies_ms, sent_counter))
        for i in range(args.concurrency)
    ]
    printer_task = asyncio.create_task(printer(sent_counter, args.duration))

    total = await event_feeder(queue, args.rate, args.batch, args.duration)

    for _ in workers:
        await queue.put(None)
    await queue.join()
    await asyncio.gather(*workers)
    printer_task.cancel()

    sys.stderr.write("\n")
    if latencies_ms:
        latencies_ms.sort()
        p50 = latencies_ms[len(latencies_ms) // 2]
        p99 = latencies_ms[min(len(latencies_ms) - 1, int(len(latencies_ms) * 0.99))]
        mean = statistics.fmean(latencies_ms)
        print(f"events sent : {sent_counter[0]}")
        print(f"source rows : {total}")
        print(f"batch latency: p50={p50:.1f}ms  p99={p99:.1f}ms  mean={mean:.1f}ms")
    else:
        print("no batches sent")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
