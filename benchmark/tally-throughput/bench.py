#!/usr/bin/env python3
"""
Tally throughput benchmark — sync PUSH over the real SDK.

Measures events/sec and per-event latency (p50/p95/p99) for three pipeline
shapes: small, medium, large. Writes timestamped JSON results to ./results/.

Usage:
    python3 bench.py --events 100000 --clients 1 --pipeline medium
    python3 bench.py --events 50000 --clients 4 --pipeline large
    python3 bench.py --matrix --clients 1 --events 60000           # Phase 12 gate
    python3 bench.py --pipeline medium --mode mixed --events 20000 # sync p99 under async saturation
"""

import argparse
import json
import os
import random
import statistics
import sys
import threading
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
    app = st.App('localhost:6400', timeout=30.0)
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


def run_single_client_async(primary_cls, events_per_client, client_id, warmup=1000, sample_latency=False):
    """Async-mode runner (Phase 11): fire-and-forget loop + trailing flush.

    Returns (latencies_us_or_empty, wall_seconds).

    Phase 12: When sample_latency=True, we sample per-`push()` call time
    on every Nth event (N=8) to avoid penalizing throughput measurement.
    This captures the SDK-side enqueue / write time — the metric directly
    affected by server-side coalescing's batch deadline from the caller's
    perspective, and what ROADMAP criterion #10 (async p50 impact)
    documents. The final flush() is NOT included in the per-event
    latencies (it's a one-shot fence).

    v1.2 baseline (142k eps on medium) measured throughput WITHOUT any
    per-event timing calls. To keep gate comparisons fair, the sampling
    path now uses stride-based sampling (1-in-8) so the hot loop spends
    most of its iterations on the fast non-sampling code path. Wall time
    (and thus throughput) is measured over the entire run.
    """
    app = st.App('localhost:6400', timeout=30.0)

    # Warmup in async mode; flush to exclude warmup from measured wall
    for i in range(warmup):
        app.push(primary_cls, make_event(i + client_id * 1000000))
    app.flush()

    latencies = []
    t_start = time.perf_counter()
    if sample_latency:
        # Stride-based sampling: measure every 8th push to keep throughput
        # measurement fair while still collecting representative latency data.
        SAMPLE_STRIDE = 8
        for i in range(events_per_client):
            ev = make_event(i + client_id * 1000000)
            if i % SAMPLE_STRIDE == 0:
                t0 = time.perf_counter_ns()
                app.push(primary_cls, ev)
                dt = time.perf_counter_ns() - t0
                latencies.append(dt / 1000.0)
            else:
                app.push(primary_cls, ev)
        app.flush()
    else:
        for i in range(events_per_client):
            ev = make_event(i + client_id * 1000000)
            app.push(primary_cls, ev)
        app.flush()
    wall = time.perf_counter() - t_start
    return latencies, wall


def run_single_client_async_batch(primary_cls, events_per_client, client_id,
                                   warmup=1000, batch_size=1000):
    """Async-batch mode runner (Phase 13): push_many batch frames + trailing flush.

    Returns ([], wall_seconds). No per-event latency sampling — pure throughput.
    """
    app = st.App('localhost:6400', timeout=30.0)

    # Warmup
    warmup_events = [make_event(i + client_id * 1000000) for i in range(warmup)]
    for i in range(0, len(warmup_events), batch_size):
        app.push_many(primary_cls, warmup_events[i:i + batch_size])
    app.flush()

    # Pre-generate all events to exclude generation time from measurement
    events = [make_event(i + client_id * 1000000) for i in range(events_per_client)]

    # Timed run
    t_start = time.perf_counter()
    for i in range(0, len(events), batch_size):
        app.push_many(primary_cls, events[i:i + batch_size])
    app.flush()
    wall = time.perf_counter() - t_start

    eps = events_per_client / wall if wall > 0 else 0.0
    print(f'  [client-{client_id}] async-batch: {events_per_client} events in {wall:.3f}s = {eps:.0f} eps (batch_size={batch_size})')
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


# ---------------------------------------------------------------------------
# Standard benchmark (single scenario — existing behavior, extended for async
# latency sampling so --matrix and the default CLI both surface percentiles)
# ---------------------------------------------------------------------------

def run_benchmark(args, sample_async_latency=True, quiet=False):
    if not quiet:
        print(f'\n=== Tally Throughput Benchmark ===')
        print(f'Mode:     {args.mode}')
        print(f'Pipeline: {args.pipeline}')
        print(f'Events:   {args.events:,}')
        print(f'Clients:  {args.clients}')
        print()

    # Register pipeline
    streams, primary = PIPELINES[args.pipeline]()
    app = st.App('localhost:6400', timeout=30.0)
    app.register(*streams)
    if not quiet:
        print(f'Registered {len(streams)} streams/views. Primary: {primary.__name__}')

    # Warmup ping (always sync so we know the server is alive before the loop)
    app.push_sync(primary, make_event(0))
    if not quiet:
        print('Warmup ping OK')

    events_per_client = args.events // args.clients

    if not quiet:
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
    elif args.mode == 'async-batch':
        batch_size = getattr(args, 'batch_size', 1000)
        if args.clients == 1:
            all_latencies, t_elapsed = run_single_client_async_batch(
                primary, events_per_client, 0, batch_size=batch_size,
            )
        else:
            with ThreadPoolExecutor(max_workers=args.clients) as pool:
                futures = [
                    pool.submit(
                        run_single_client_async_batch,
                        primary, events_per_client, cid, 1000, batch_size,
                    )
                    for cid in range(args.clients)
                ]
                walls = []
                for f in as_completed(futures):
                    lat, wall = f.result()
                    walls.append(wall)
                    all_latencies.extend(lat)
                t_elapsed = max(walls) if walls else 0.0
    else:  # async
        if args.clients == 1:
            all_latencies, t_elapsed = run_single_client_async(
                primary, events_per_client, 0, sample_latency=sample_async_latency
            )
        else:
            # For multi-client async, run clients concurrently and take the max
            # wall time (that's when all events are acknowledged via flush).
            with ThreadPoolExecutor(max_workers=args.clients) as pool:
                futures = [
                    pool.submit(
                        run_single_client_async,
                        primary, events_per_client, cid, 1000, sample_async_latency,
                    )
                    for cid in range(args.clients)
                ]
                walls = []
                for f in as_completed(futures):
                    lat, wall = f.result()
                    walls.append(wall)
                    all_latencies.extend(lat)
                t_elapsed = max(walls) if walls else 0.0

    total_events = events_per_client * args.clients
    throughput = total_events / t_elapsed if t_elapsed > 0 else 0.0

    if not quiet:
        print(f'\n=== Results ({args.mode} mode) ===')
        print(f'Total events:  {total_events:,}')
        print(f'Wall time:     {t_elapsed:.2f}s')
        print(f'Throughput:    {throughput:,.0f} events/sec')

    latency_block = None
    if all_latencies:
        p50 = percentile(all_latencies, 50)
        p95 = percentile(all_latencies, 95)
        p99 = percentile(all_latencies, 99)
        p999 = percentile(all_latencies, 99.9)
        mean = statistics.mean(all_latencies)
        if not quiet:
            label = 'per-event sync latency' if args.mode == 'sync' else 'per-push async enqueue latency'
            print(f'{label} (us):')
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
    if not quiet:
        print(f'\nWrote {out_path}')

    return result


# ---------------------------------------------------------------------------
# Phase 12: --matrix runner (6 scenarios × 5-run median, σ<10% gate)
# ---------------------------------------------------------------------------

class _Args:
    """Minimal shim so the matrix runner can drive run_benchmark without touching argparse."""
    def __init__(self, mode, pipeline, events, clients, batch_size=1000):
        self.mode = mode
        self.pipeline = pipeline
        self.events = events
        self.clients = clients
        self.batch_size = batch_size


def run_matrix(clients, events_budget):
    """Run the 6-scenario Phase 12 gate matrix.

    Phase 12 D-17/D-18: small/medium/large × sync/async, 5-run median per
    scenario with σ/median < 10% rejection.

    - events_budget: the --events CLI value. Sync scenarios run at 30% of
      that budget (sync is ~10x slower so we keep wall-time bounded) and
      async scenarios run at the full budget.
    """
    scenarios = [
        ('small', 'sync'),
        ('small', 'async'),
        ('medium', 'sync'),
        ('medium', 'async'),
        ('large', 'sync'),
        ('large', 'async'),
    ]

    results = {}
    all_fail = False
    print('\n=== Phase 12 Matrix Gate (6 scenarios × 5-run median, σ<10%) ===')
    print(f'Budget: events={events_budget} (sync runs use 30% of budget), clients={clients}')
    print()

    for pipeline, mode in scenarios:
        scenario_key = f'{pipeline}_{mode}'
        events = events_budget if mode == 'async' else max(5000, events_budget * 3 // 10)
        run_eps = []
        run_p50 = []
        run_p95 = []
        run_p99 = []
        print(f'>>> {pipeline:<6s} × {mode:<5s}  (events={events}, 5 runs)')
        for run_idx in range(5):
            args = _Args(mode=mode, pipeline=pipeline, events=events, clients=clients)
            # Throughput measurement must NOT include per-event latency
            # sampling overhead. The v1.2 baseline (142k medium async) was
            # measured without any per-push timing calls; sampling every
            # push added ~15% Python-side overhead that inflated the
            # apparent regression. Stride-based sampling (1-in-8) in
            # run_single_client_async still captures representative
            # latency percentiles for async p50 impact reporting without
            # penalizing the throughput number.
            r = run_benchmark(args, sample_async_latency=(mode == 'async'), quiet=True)
            run_eps.append(r['throughput_eps'])
            if r['latency_us'] is not None:
                run_p50.append(r['latency_us']['p50'])
                run_p95.append(r['latency_us']['p95'])
                run_p99.append(r['latency_us']['p99'])
            print(f'    run {run_idx + 1}/5: {r["throughput_eps"]:>10,.0f} eps'
                  + (f", p50={r['latency_us']['p50']}µs, p99={r['latency_us']['p99']}µs"
                     if r['latency_us'] is not None else ''))
            # Small settle gap between runs so the server's internal state is not hot-polluted.
            time.sleep(0.1)

        median_eps = statistics.median(run_eps)
        sigma_eps = statistics.stdev(run_eps) if len(run_eps) >= 2 else 0.0
        sigma_pct = (sigma_eps / median_eps) if median_eps > 0 else 0.0
        gate_ok = sigma_pct <= 0.10
        median_p50 = statistics.median(run_p50) if run_p50 else None
        median_p95 = statistics.median(run_p95) if run_p95 else None
        median_p99 = statistics.median(run_p99) if run_p99 else None

        if not gate_ok:
            all_fail = True
            print(f'    MATRIX FAIL: σ/median = {sigma_pct * 100:.1f}% > 10% gate')

        results[scenario_key] = {
            'pipeline': pipeline,
            'mode': mode,
            'runs': 5,
            'median_eps': round(median_eps, 1),
            'sigma_eps': round(sigma_eps, 1),
            'sigma_pct': round(sigma_pct * 100, 2),
            'p50_us': round(median_p50, 2) if median_p50 is not None else None,
            'p95_us': round(median_p95, 2) if median_p95 is not None else None,
            'p99_us': round(median_p99, 2) if median_p99 is not None else None,
            'gate': 'ok' if gate_ok else 'FAIL',
        }

    # Summary table
    print('\n=== Matrix summary ===')
    print(f'{"scenario":<16s} {"runs":<5s} {"median eps":>12s} {"σ/median":>10s} {"p50":>8s} {"p99":>8s} {"gate":>6s}')
    for key in ['small_sync', 'small_async', 'medium_sync', 'medium_async', 'large_sync', 'large_async']:
        r = results[key]
        label = f"{r['pipeline']:<6s} {r['mode']:<5s}"
        p50 = f"{r['p50_us']}µs" if r['p50_us'] is not None else '—'
        p99 = f"{r['p99_us']}µs" if r['p99_us'] is not None else '—'
        print(f'{label:<16s} {r["runs"]:<5d} {r["median_eps"]:>12,.0f} {r["sigma_pct"]:>9.2f}% {p50:>8s} {p99:>8s} {r["gate"]:>6s}')

    if all_fail:
        print('\nMATRIX FAIL: at least one scenario exceeded σ<10% gate — results NOT trustworthy')
    else:
        print('\nMATRIX OK: all 6 scenarios under σ<10%')

    # Write JSON file
    results_dir = os.path.join(os.path.dirname(__file__), 'results')
    os.makedirs(results_dir, exist_ok=True)
    ts = datetime.now().strftime('%Y%m%d-%H%M%S')
    out_path = os.path.join(results_dir, f'{ts}-matrix-{clients}c.json')
    with open(out_path, 'w') as fh:
        json.dump(
            {
                'timestamp': ts,
                'clients': clients,
                'events_budget': events_budget,
                'scenarios': results,
                'matrix_ok': not all_fail,
            },
            fh,
            indent=2,
        )
    print(f'\nWrote {out_path}')
    return results


# ---------------------------------------------------------------------------
# Phase 12: --mode mixed (async saturator + sync sampler in parallel)
# ---------------------------------------------------------------------------

def run_mixed(pipeline_name, events_budget):
    """Mixed-workload harness for sync p99 under async saturation.

    Thread A opens one connection and pushes `events_budget` OP_PUSH_ASYNC
    frames as fast as possible, then flushes. Concurrently thread B opens a
    separate connection and pushes one sync event every 500µs for the
    duration of thread A, measuring per-push latency.

    Output: async aggregate eps, sync p50/p95/p99, and a SYNC-P99 GATE line
    testing the D-10 constraint that sync p99 stays within ±5% of 87µs.
    """
    streams, primary = PIPELINES[pipeline_name]()
    app_reg = st.App('localhost:6400', timeout=30.0)
    app_reg.register(*streams)
    app_reg.push_sync(primary, make_event(0))  # warmup

    a_result = {'eps': 0.0, 'wall': 0.0, 'events': 0}
    b_result = {'latencies': [], 'count': 0}
    stop_sampler = threading.Event()

    def saturator():
        app = st.App('localhost:6400', timeout=30.0)
        # Warmup
        for i in range(500):
            app.push(primary, make_event(i + 90000000))
        app.flush()
        t0 = time.perf_counter()
        for i in range(events_budget):
            app.push(primary, make_event(i + 91000000))
        app.flush()
        wall = time.perf_counter() - t0
        a_result['wall'] = wall
        a_result['events'] = events_budget
        a_result['eps'] = events_budget / wall if wall > 0 else 0.0
        stop_sampler.set()

    def sampler():
        app = st.App('localhost:6400', timeout=30.0)
        # Warmup
        for i in range(100):
            app.push_sync(primary, make_event(i + 92000000))
        latencies = []
        i = 0
        while not stop_sampler.is_set():
            ev = make_event(i + 93000000)
            t0 = time.perf_counter_ns()
            try:
                app.push_sync(primary, ev)
            except Exception:
                break
            dt = time.perf_counter_ns() - t0
            latencies.append(dt / 1000.0)
            i += 1
            # 500µs pacing between samples (D-10 plan spec)
            time.sleep(0.0005)
        b_result['latencies'] = latencies
        b_result['count'] = len(latencies)

    print(f'\n=== Mixed workload: {pipeline_name} pipeline ===')
    print(f'Saturator: {events_budget} OP_PUSH_ASYNC frames')
    print(f'Sampler:   sync push every 500µs (concurrent)')
    print()

    t_a = threading.Thread(target=saturator)
    t_b = threading.Thread(target=sampler)
    t_b.start()
    time.sleep(0.05)  # let sampler warm up before saturator lands
    t_a.start()
    t_a.join()
    t_b.join(timeout=5.0)

    lats = b_result['latencies']
    if not lats:
        print('Sampler collected no samples — aborting mixed run')
        return {'ok': False}

    sync_p50 = percentile(lats, 50)
    sync_p95 = percentile(lats, 95)
    sync_p99 = percentile(lats, 99)
    sync_mean = statistics.mean(lats)

    print(f'Saturator (async): {a_result["eps"]:,.0f} eps over {a_result["wall"]:.2f}s ({a_result["events"]:,} events)')
    print(f'Sampler (sync): {b_result["count"]} samples')
    print(f'  mean:  {sync_mean:7.2f} µs')
    print(f'  p50:   {sync_p50:7.2f} µs')
    print(f'  p95:   {sync_p95:7.2f} µs')
    print(f'  p99:   {sync_p99:7.2f} µs')

    # D-10 gate: ±5% of 87µs → [82.65, 91.35]
    lo, hi = 82.65, 91.35
    if lo <= sync_p99 <= hi:
        gate_line = f'SYNC-P99 GATE: PASS (p99={sync_p99:.2f}µs in [{lo}, {hi}])'
    else:
        gate_line = f'SYNC-P99 GATE: FAIL (got {sync_p99:.2f}µs, allowed [{lo}, {hi}])'
    print(gate_line)

    result = {
        'pipeline': pipeline_name,
        'mode': 'mixed',
        'saturator_events': a_result['events'],
        'saturator_wall': round(a_result['wall'], 3),
        'saturator_eps': round(a_result['eps'], 1),
        'sampler_count': b_result['count'],
        'sampler_p50_us': round(sync_p50, 2),
        'sampler_p95_us': round(sync_p95, 2),
        'sampler_p99_us': round(sync_p99, 2),
        'sampler_mean_us': round(sync_mean, 2),
        'gate_low': lo,
        'gate_high': hi,
        'gate_pass': lo <= sync_p99 <= hi,
    }

    results_dir = os.path.join(os.path.dirname(__file__), 'results')
    os.makedirs(results_dir, exist_ok=True)
    ts = datetime.now().strftime('%Y%m%d-%H%M%S')
    out_path = os.path.join(results_dir, f'{ts}-{pipeline_name}-mixed.json')
    with open(out_path, 'w') as fh:
        json.dump(result, fh, indent=2)
    print(f'Wrote {out_path}')
    return result


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--events', type=int, default=50000,
                        help='Total events across all clients (default: 50000)')
    parser.add_argument('--clients', type=int, default=1,
                        help='Number of parallel SDK connections (default: 1)')
    parser.add_argument('--pipeline', choices=list(PIPELINES.keys()), default='medium',
                        help='Pipeline shape (default: medium)')
    parser.add_argument('--mode', choices=['sync', 'async', 'mixed', 'async-batch'], default='sync',
                        help='Push mode: sync = per-event push_sync round-trip, '
                             'async = fire-and-forget + trailing flush, '
                             'mixed = async saturator + sync sampler in parallel (Phase 12 D-10), '
                             'async-batch = push_many batch frames (Phase 13 D-15)')
    parser.add_argument('--batch-size', type=int, default=1000,
                        help='Events per push_many call in async-batch mode (default: 1000)')
    parser.add_argument('--matrix', action='store_true',
                        help='Phase 12 D-17 gate: run small/medium/large x sync/async '
                             '(6 scenarios x 5-run median, sigma<10%% rejection)')
    args = parser.parse_args()

    if args.matrix:
        run_matrix(clients=args.clients, events_budget=args.events)
    elif args.mode == 'mixed':
        run_mixed(pipeline_name=args.pipeline, events_budget=args.events)
    else:
        run_benchmark(args)


if __name__ == '__main__':
    main()
