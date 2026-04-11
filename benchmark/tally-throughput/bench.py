#!/usr/bin/env python3
"""
Tally throughput benchmark — sync PUSH over the real SDK.

Measures events/sec and per-event latency (p50/p95/p99) for three pipeline
shapes: small, medium, large. Writes timestamped JSON results to ./results/.

Usage:
    python3 bench.py --events 100000 --clients 1 --pipeline medium
    python3 bench.py --events 50000 --clients 4 --pipeline large
"""

import argparse
import json
import os
import random
import statistics
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

# Path hack so the bench can run without installing the SDK
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'python'))

import tally as st  # noqa: E402


# ---------------------------------------------------------------------------
# Pipeline definitions
# ---------------------------------------------------------------------------

def define_small():
    """Single stream, 5 features, no cascade."""
    @st.stream(key='user_id')
    class Transactions:
        tx_count_1h = st.count(window='1h')
        tx_sum_1h = st.sum('amount', window='1h')
        avg_amount_1h = st.avg('amount', window='1h')
        max_amount_24h = st.max('amount', window='24h')
        min_amount_24h = st.min('amount', window='24h')
    return [Transactions], Transactions


def define_medium():
    """2 streams + 1 view. Fan-out across user_id and merchant_id."""
    @st.stream(key='user_id')
    class Transactions:
        tx_count_1h = st.count(window='1h')
        tx_sum_1h = st.sum('amount', window='1h')
        avg_amount_1h = st.avg('amount', window='1h')
        max_amount_24h = st.max('amount', window='24h')
        failed_count_30m = st.count(window='30m', where="status == 'failed'")
        failure_rate = st.derive('failed_count_30m / tx_count_1h')

    @st.stream(key='merchant_id')
    class MerchantActivity:
        merchant_tx_count = st.count(window='1h')
        merchant_sum = st.sum('amount', window='1h')

    @st.view(key='user_id')
    class UserRisk:
        is_high_volume = st.derive('Transactions.tx_count_1h > 10')

    return [Transactions, MerchantActivity, UserRisk], Transactions


def define_large():
    """3 streams + 2 views with cascade + fan-out + distinct_count."""
    @st.stream(key='user_id')
    class Transactions:
        tx_count_1h = st.count(window='1h')
        tx_sum_1h = st.sum('amount', window='1h')
        avg_amount_1h = st.avg('amount', window='1h')
        max_amount_24h = st.max('amount', window='24h')
        min_amount_24h = st.min('amount', window='24h')
        failed_count_30m = st.count(window='30m', where="status == 'failed'")
        unique_merchants_24h = st.distinct_count('merchant_id', window='24h')
        failure_rate = st.derive('failed_count_30m / tx_count_1h')

    @st.stream(key='merchant_id')
    class MerchantActivity:
        merchant_tx_count_1h = st.count(window='1h')
        merchant_sum_1h = st.sum('amount', window='1h')
        merchant_unique_users = st.distinct_count('user_id', window='24h')

    @st.stream(key='device_id')
    class DeviceActivity:
        device_tx_count_1h = st.count(window='1h')
        device_unique_users = st.distinct_count('user_id', window='1h')

    @st.view(key='user_id')
    class UserRisk:
        is_high_volume = st.derive('Transactions.tx_count_1h > 10')
        suspicious = st.derive('Transactions.failure_rate > 0.2')

    @st.view(key='user_id')
    class UserSummary:
        total_tx = st.derive('Transactions.tx_count_1h')
        total_amount = st.derive('Transactions.tx_sum_1h')

    return [Transactions, MerchantActivity, DeviceActivity, UserRisk, UserSummary], Transactions


PIPELINES = {
    'small': define_small,
    'medium': define_medium,
    'large': define_large,
}


# ---------------------------------------------------------------------------
# Event generator
# ---------------------------------------------------------------------------

def make_event(i, user_pool=1000, merchant_pool=100, device_pool=500):
    """Generate a realistic-looking transaction event."""
    return {
        'user_id': f'user_{i % user_pool}',
        'merchant_id': f'merchant_{i % merchant_pool}',
        'device_id': f'device_{i % device_pool}',
        'amount': round(random.uniform(1.0, 1000.0), 2),
        'status': 'success' if i % 10 != 0 else 'failed',
        'country': random.choice(['US', 'UK', 'DE', 'FR', 'JP']),
    }


# ---------------------------------------------------------------------------
# Benchmark runners
# ---------------------------------------------------------------------------

def run_single_client_sync(primary_cls, events_per_client, client_id, warmup=1000):
    """Sync-mode runner: per-event round-trip via push_sync.

    Returns (latencies_us, wall_seconds). Latencies are measured per event.
    """
    app = st.App('localhost:6400')
    latencies = []

    # Warmup
    for i in range(warmup):
        app.push_sync(primary_cls, make_event(i + client_id * 1000000))

    # Measured run
    t_start = time.perf_counter()
    for i in range(events_per_client):
        ev = make_event(i + client_id * 1000000)
        t0 = time.perf_counter_ns()
        app.push_sync(primary_cls, ev)
        dt = time.perf_counter_ns() - t0
        latencies.append(dt / 1000.0)  # ns → us
    wall = time.perf_counter() - t_start

    return latencies, wall


def run_single_client_async(primary_cls, events_per_client, client_id, warmup=1000):
    """Async-mode runner (Phase 11): fire-and-forget loop + trailing flush.

    Returns ([], wall_seconds). Per-event latency is not collected — the
    async push returns immediately, so individual "latency" would be
    meaningless. Only throughput is reported.
    """
    app = st.App('localhost:6400')

    # Warmup in async mode; flush to exclude warmup from measured wall
    for i in range(warmup):
        app.push(primary_cls, make_event(i + client_id * 1000000))
    app.flush()

    t_start = time.perf_counter()
    for i in range(events_per_client):
        ev = make_event(i + client_id * 1000000)
        app.push(primary_cls, ev)
    app.flush()
    wall = time.perf_counter() - t_start
    return [], wall


def run_single_client(primary_cls, events_per_client, client_id, warmup=1000):
    """Back-compat wrapper — sync mode (used by any external callers)."""
    latencies, _ = run_single_client_sync(primary_cls, events_per_client, client_id, warmup)
    return latencies


def percentile(values, p):
    if not values:
        return 0.0
    s = sorted(values)
    idx = max(0, min(len(s) - 1, int(p / 100.0 * len(s))))
    return s[idx]


def run_benchmark(args):
    print(f'\n=== Tally Throughput Benchmark ===')
    print(f'Mode:     {args.mode}')
    print(f'Pipeline: {args.pipeline}')
    print(f'Events:   {args.events:,}')
    print(f'Clients:  {args.clients}')
    print()

    # Register pipeline
    streams, primary = PIPELINES[args.pipeline]()
    app = st.App('localhost:6400')
    app.register(*streams)
    print(f'Registered {len(streams)} streams/views. Primary: {primary.__name__}')

    # Warmup ping (always sync so we know the server is alive before the loop)
    app.push_sync(primary, make_event(0))
    print('Warmup ping OK')

    events_per_client = args.events // args.clients

    # Run
    print(f'\nRunning {events_per_client:,} events × {args.clients} client(s) in {args.mode} mode...')

    all_latencies = []
    if args.mode == 'sync':
        if args.clients == 1:
            all_latencies, t_elapsed = run_single_client_sync(primary, events_per_client, 0)
        else:
            t_start = time.perf_counter()
            with ThreadPoolExecutor(max_workers=args.clients) as pool:
                futures = [
                    pool.submit(run_single_client_sync, primary, events_per_client, cid)
                    for cid in range(args.clients)
                ]
                for f in as_completed(futures):
                    lat, _ = f.result()
                    all_latencies.extend(lat)
            t_elapsed = time.perf_counter() - t_start
    else:  # async
        if args.clients == 1:
            _, t_elapsed = run_single_client_async(primary, events_per_client, 0)
        else:
            # For multi-client async, run clients concurrently and take the max
            # wall time (that's when all events are acknowledged via flush).
            with ThreadPoolExecutor(max_workers=args.clients) as pool:
                futures = [
                    pool.submit(run_single_client_async, primary, events_per_client, cid)
                    for cid in range(args.clients)
                ]
                walls = []
                for f in as_completed(futures):
                    _, wall = f.result()
                    walls.append(wall)
                t_elapsed = max(walls) if walls else 0.0

    total_events = events_per_client * args.clients
    throughput = total_events / t_elapsed if t_elapsed > 0 else 0.0

    print(f'\n=== Results ({args.mode} mode) ===')
    print(f'Total events:  {total_events:,}')
    print(f'Wall time:     {t_elapsed:.2f}s')
    print(f'Throughput:    {throughput:,.0f} events/sec')

    latency_block = None
    if args.mode == 'sync' and all_latencies:
        p50 = percentile(all_latencies, 50)
        p95 = percentile(all_latencies, 95)
        p99 = percentile(all_latencies, 99)
        p999 = percentile(all_latencies, 99.9)
        mean = statistics.mean(all_latencies)
        print(f'Per-event latency (us):')
        print(f'  mean:  {mean:7.2f}')
        print(f'  p50:   {p50:7.2f}')
        print(f'  p95:   {p95:7.2f}')
        print(f'  p99:   {p99:7.2f}')
        print(f'  p99.9: {p999:7.2f}')
        latency_block = {
            'mean': round(mean, 2),
            'p50': round(p50, 2),
            'p95': round(p95, 2),
            'p99': round(p99, 2),
            'p999': round(p999, 2),
        }
    elif args.mode == 'async':
        print('(per-event latency not measured in async mode — only throughput is meaningful)')

    # Write JSON result
    results_dir = os.path.join(os.path.dirname(__file__), 'results')
    os.makedirs(results_dir, exist_ok=True)
    ts = datetime.now().strftime('%Y%m%d-%H%M%S')
    result = {
        'timestamp': ts,
        'mode': args.mode,
        'pipeline': args.pipeline,
        'total_events': total_events,
        'clients': args.clients,
        'events_per_client': events_per_client,
        'wall_seconds': round(t_elapsed, 3),
        'throughput_eps': round(throughput, 1),
        'latency_us': latency_block,
    }
    out_path = os.path.join(results_dir, f'{ts}-{args.pipeline}-{args.clients}c-{args.mode}.json')
    with open(out_path, 'w') as fh:
        json.dump(result, fh, indent=2)
    print(f'\nWrote {out_path}')

    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--events', type=int, default=50000,
                        help='Total events across all clients (default: 50000)')
    parser.add_argument('--clients', type=int, default=1,
                        help='Number of parallel SDK connections (default: 1)')
    parser.add_argument('--pipeline', choices=list(PIPELINES.keys()), default='medium',
                        help='Pipeline shape (default: medium)')
    parser.add_argument('--mode', choices=['sync', 'async'], default='sync',
                        help='Push mode: sync = per-event push_sync round-trip (v1.1 behavior), '
                             'async = fire-and-forget push with trailing flush (Phase 11)')
    args = parser.parse_args()
    run_benchmark(args)


if __name__ == '__main__':
    main()
