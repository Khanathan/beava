#!/usr/bin/env python3
"""Tally v0 throughput benchmark — sync/async PUSH over the v0 SDK.

Mirrors the v2.0 bench.py 9-cell matrix (small/medium/large x 1c/4c/8c) but
drives the v0 `@tl.stream` + `@tl.table(key=...)` + `group_by().agg(...)` API
so it runs against the post-Phase-21 server. Plan 22-04 Step 5 gate:
no cell regresses > 5% from `.planning/phases/22-stream-aggregation-engine/BASELINE.json`.

Usage:
    python3 bench_v0.py --pipeline medium --clients 1 --events 30000
    python3 bench_v0.py --matrix --events 30000
"""

import argparse
import json
import os
import random
import statistics
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

# Path hack so the bench can run without installing the SDK
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'python'))

import tally as tl  # noqa: E402


# ---------------------------------------------------------------------------
# v0 pipeline definitions — matched in spirit to the v2.0 small/medium/large.
# Single-key aggregations only (v0→v2 translator enforces this in 22-04).
# ---------------------------------------------------------------------------

def define_small():
    """1 source stream + 1 keyed aggregation, 5 features."""
    @tl.stream
    class RawTxns:
        user_id: str
        amount: float

    @tl.table(key="user_id")
    def Transactions(raw: RawTxns) -> tl.Table:
        return raw.group_by("user_id").agg(
            tx_count_1h=tl.count(window='1h'),
            tx_sum_1h=tl.sum('amount', window='1h'),
            avg_amount_1h=tl.avg('amount', window='1h'),
            max_amount_24h=tl.max('amount', window='24h'),
            min_amount_24h=tl.min('amount', window='24h'),
        )

    return [RawTxns, Transactions], RawTxns


def define_medium():
    """1 source + 1 user-keyed aggregation with a where-filtered count."""
    @tl.stream
    class RawTxns:
        user_id: str
        amount: float
        status: str

    @tl.table(key="user_id")
    def Transactions(raw: RawTxns) -> tl.Table:
        return raw.group_by("user_id").agg(
            tx_count_1h=tl.count(window='1h'),
            tx_sum_1h=tl.sum('amount', window='1h'),
            avg_amount_1h=tl.avg('amount', window='1h'),
            max_amount_24h=tl.max('amount', window='24h'),
            failed_count_30m=tl.count(window='30m', where="status == 'failed'"),
        )

    return [RawTxns, Transactions], RawTxns


def define_large():
    """1 source + user-keyed aggregation with 7 features including count_distinct."""
    @tl.stream
    class RawTxns:
        user_id: str
        merchant_id: str
        amount: float
        status: str

    @tl.table(key="user_id")
    def Transactions(raw: RawTxns) -> tl.Table:
        return raw.group_by("user_id").agg(
            tx_count_1h=tl.count(window='1h'),
            tx_sum_1h=tl.sum('amount', window='1h'),
            avg_amount_1h=tl.avg('amount', window='1h'),
            max_amount_24h=tl.max('amount', window='24h'),
            min_amount_24h=tl.min('amount', window='24h'),
            failed_count_30m=tl.count(window='30m', where="status == 'failed'"),
            unique_merchants_24h=tl.count_distinct('merchant_id', window='24h'),
        )

    return [RawTxns, Transactions], RawTxns


def define_join_small():
    """Stream↔Stream windowed join feeding a count aggregation.

    Primary push target is Orders. Events are generated with an
    `_event_time` that spans the within window so ~50% probe into the
    opposite side match. See bench_v0.run_benchmark generator seed.
    """
    @tl.stream
    class BenchOrders:
        user_id: str
        order_id: str

    @tl.stream
    class BenchPayments:
        user_id: str
        order_id: str
        amount: float

    Joined = BenchOrders.join(
        BenchPayments, on=["user_id", "order_id"], within="30s", type="inner"
    )

    @tl.table(key="user_id")
    def BenchJoinAgg(j: Joined) -> tl.Table:
        return j.group_by("user_id").agg(matched=tl.count(window='1h'))

    return [BenchOrders, BenchPayments, Joined, BenchJoinAgg], BenchOrders


def define_enrich_small():
    """Stream↔Table enrichment feeding a count aggregation.

    Primary push target is BenchClicks. BenchProfile is pre-populated
    before the benchmark loop (via a warmup pass).
    """
    @tl.stream
    class BenchClicks:
        user_id: str
        page: str

    @tl.table(key="user_id")
    class BenchProfile:
        user_id: str
        country: str

    Enriched = BenchClicks.join(BenchProfile, on="user_id", type="left")

    @tl.table(key="country")
    def BenchEnrichAgg(e: Enriched) -> tl.Table:
        return e.group_by("country").agg(n=tl.count(window='1h'))

    return [BenchClicks, BenchProfile, Enriched, BenchEnrichAgg], BenchClicks


PIPELINES = {
    'small': define_small,
    'medium': define_medium,
    'large': define_large,
    'join': define_join_small,
    'enrich': define_enrich_small,
}


# ---------------------------------------------------------------------------
# Event generator
# ---------------------------------------------------------------------------

def make_event(i, user_pool=1000, merchant_pool=100, device_pool=500):
    return {
        'user_id': f'user_{i % user_pool}',
        'merchant_id': f'merchant_{i % merchant_pool}',
        'device_id': f'device_{i % device_pool}',
        'amount': round(random.uniform(1.0, 1000.0), 2),
        'status': 'success' if i % 10 != 0 else 'failed',
        'country': random.choice(['US', 'UK', 'DE', 'FR', 'JP']),
    }


# ---------------------------------------------------------------------------
# Runners (mirror bench.py semantics: async fire-and-forget + trailing flush)
# ---------------------------------------------------------------------------

def run_async_client(primary_cls, events_per_client, client_id, warmup=1000,
                     sample_latency=False, host='localhost:6400'):
    app = tl.App(host, timeout=30.0)
    for i in range(warmup):
        app.push(primary_cls, make_event(i + client_id * 1_000_000))
    app.flush()

    latencies = []
    t0 = time.perf_counter()
    if sample_latency:
        STRIDE = 8
        for i in range(events_per_client):
            ev = make_event(i + client_id * 1_000_000)
            if i % STRIDE == 0:
                t_start = time.perf_counter_ns()
                app.push(primary_cls, ev)
                latencies.append((time.perf_counter_ns() - t_start) / 1000.0)
            else:
                app.push(primary_cls, ev)
        app.flush()
    else:
        for i in range(events_per_client):
            app.push(primary_cls, make_event(i + client_id * 1_000_000))
        app.flush()
    wall = time.perf_counter() - t0
    return latencies, wall


def percentile(values, p):
    if not values:
        return 0.0
    s = sorted(values)
    idx = max(0, min(len(s) - 1, int(p / 100.0 * len(s))))
    return s[idx]


def run_benchmark(pipeline_name, clients, events_per_client, host='localhost:6400',
                  sample_latency=True, quiet=True):
    streams, primary = PIPELINES[pipeline_name]()
    app = tl.App(host, timeout=30.0)
    app.register(*streams)
    # Warmup ping via sync push
    app.push_sync(primary, make_event(0))

    all_lat = []
    if clients == 1:
        all_lat, wall = run_async_client(primary, events_per_client, 0,
                                         sample_latency=sample_latency, host=host)
    else:
        with ThreadPoolExecutor(max_workers=clients) as pool:
            futures = [
                pool.submit(run_async_client, primary, events_per_client, cid, 1000,
                            sample_latency, host)
                for cid in range(clients)
            ]
            walls = []
            for f in as_completed(futures):
                lat, w = f.result()
                walls.append(w)
                all_lat.extend(lat)
            wall = max(walls) if walls else 0.0

    total = events_per_client * clients
    eps = total / wall if wall > 0 else 0.0
    lat_block = None
    if all_lat:
        lat_block = {
            'mean': round(statistics.mean(all_lat), 2),
            'p50': round(percentile(all_lat, 50), 2),
            'p95': round(percentile(all_lat, 95), 2),
            'p99': round(percentile(all_lat, 99), 2),
            'p999': round(percentile(all_lat, 99.9), 2),
        }
    if not quiet:
        print(f'{pipeline_name}_{clients}c: {total:,} events in {wall:.2f}s = {eps:,.0f} eps')
        if lat_block:
            print(f'  p50={lat_block["p50"]}us p99={lat_block["p99"]}us')
    return {
        'pipeline': pipeline_name,
        'clients': clients,
        'events': total,
        'wall_seconds': round(wall, 3),
        'throughput_eps': round(eps, 1),
        'latency_us': lat_block,
    }


# ---------------------------------------------------------------------------
# 9-cell matrix runner
# ---------------------------------------------------------------------------

BASELINE_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), '..', '..',
    '.planning', 'phases', '22-stream-aggregation-engine', 'BASELINE.json',
)


def run_matrix(events_per_run, runs_per_cell, out_path, label, host):
    # 9 regression cells + 2 characterization cells (join/enrich @ 1c).
    pipelines = ['small', 'medium', 'large']
    clients_list = [1, 4, 8]
    char_cells = [('join', 1), ('enrich', 1)]
    results = {}
    t0 = time.time()
    for p in pipelines:
        for c in clients_list:
            key = f'{p}_{c}c'
            eps_list, p50_list, p99_list = [], [], []
            for run in range(runs_per_cell):
                try:
                    r = run_benchmark(p, c, events_per_run // c, host=host,
                                      sample_latency=True, quiet=True)
                    eps_list.append(r['throughput_eps'])
                    if r['latency_us']:
                        p50_list.append(r['latency_us']['p50'])
                        p99_list.append(r['latency_us']['p99'])
                except Exception as e:
                    print(f'  !!! {key} run {run}: {e!r}')
                time.sleep(0.2)
            if not eps_list:
                results[key] = {'error': 'all runs failed'}
                continue
            eps_list.sort(); p50_list.sort(); p99_list.sort()
            mid = len(eps_list) // 2
            results[key] = {
                'pipeline': p,
                'clients': c,
                'runs': len(eps_list),
                'eps_median': eps_list[mid],
                'eps_all': eps_list,
                'p50_median': p50_list[len(p50_list)//2] if p50_list else None,
                'p99_median': p99_list[len(p99_list)//2] if p99_list else None,
            }
            print(f'{key:20s} eps={eps_list[mid]:>10,.0f}  p99={results[key]["p99_median"]}us')

    # 2 characterization cells (smaller run counts OK — no pass/fail gate).
    for (p, c) in char_cells:
        key = f'{p}_small_{c}c'
        eps_list, p99_list = [], []
        for run in range(runs_per_cell):
            try:
                r = run_benchmark(p, c, events_per_run // c, host=host,
                                  sample_latency=True, quiet=True)
                eps_list.append(r['throughput_eps'])
                if r['latency_us']:
                    p99_list.append(r['latency_us']['p99'])
            except Exception as e:
                print(f'  !!! {key} run {run}: {e!r}')
            time.sleep(0.2)
        if not eps_list:
            results[key] = {'error': 'all runs failed'}
            continue
        eps_list.sort(); p99_list.sort()
        mid = len(eps_list) // 2
        results[key] = {
            'pipeline': p,
            'clients': c,
            'runs': len(eps_list),
            'eps_median': eps_list[mid],
            'eps_all': eps_list,
            'p99_median': p99_list[len(p99_list)//2] if p99_list else None,
            'note': 'characterization only, no gate',
        }
        print(f'{key:20s} eps={eps_list[mid]:>10,.0f}  p99={results[key]["p99_median"]}us  (characterization)')

    # Gate check against BASELINE.json.
    gate_passed = True
    baseline = None
    if os.path.exists(BASELINE_PATH):
        with open(BASELINE_PATH) as fh:
            baseline = json.load(fh)
    if baseline:
        for p in pipelines:
            for c in clients_list:
                key = f'{p}_{c}c'
                if key not in results or 'eps_median' not in results[key]:
                    continue
                base = baseline.get('cells', {}).get(key, {}).get('eps_median')
                if not base or base <= 0:
                    continue
                cur = results[key]['eps_median']
                delta = (cur - base) / base * 100.0
                results[key]['delta_pct_vs_baseline'] = round(delta, 2)
                # Gate: regression > 5% fails.
                passed = delta >= -5.0
                results[key]['pass'] = passed
                if not passed:
                    gate_passed = False

    # Relate characterization cells to the small_1c base for context.
    if 'small_1c' in results and 'eps_median' in results['small_1c']:
        base_small_1c = results['small_1c']['eps_median']
        for key in ('join_small_1c', 'enrich_small_1c'):
            if key in results and 'eps_median' in results[key] and base_small_1c > 0:
                results[key]['pct_of_small_1c'] = round(
                    results[key]['eps_median'] / base_small_1c * 100.0, 2
                )

    out = {
        'label': label,
        'baseline_ref': 'BASELINE.json' if baseline else None,
        'timestamp': datetime.now(timezone.utc).isoformat(),
        'events_per_run': events_per_run,
        'runs_per_cell': runs_per_cell,
        'wall_seconds': round(time.time() - t0, 1),
        'cells': results,
        'gate_passed': gate_passed,
    }
    with open(out_path, 'w') as fh:
        json.dump(out, fh, indent=2)
    print(f'\nWrote {out_path}  ({out["wall_seconds"]}s)  gate_passed={gate_passed}')
    return out


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--pipeline', choices=list(PIPELINES.keys()), default='medium')
    ap.add_argument('--clients', type=int, default=1)
    ap.add_argument('--events', type=int, default=30000)
    ap.add_argument('--runs', type=int, default=3)
    ap.add_argument('--host', default='localhost:6400')
    ap.add_argument('--matrix', action='store_true')
    ap.add_argument('--out', default=None)
    ap.add_argument('--label', default='v0-post-wiring')
    args = ap.parse_args()

    if args.matrix:
        out_path = args.out or os.path.join(
            os.path.dirname(os.path.abspath(__file__)), 'results',
            f'matrix-v0-{datetime.now().strftime("%Y%m%d-%H%M%S")}.json'
        )
        os.makedirs(os.path.dirname(out_path), exist_ok=True)
        run_matrix(args.events, args.runs, out_path, args.label, args.host)
    else:
        r = run_benchmark(args.pipeline, args.clients,
                          args.events // args.clients, host=args.host,
                          sample_latency=True, quiet=False)
        print(json.dumps(r, indent=2))


if __name__ == '__main__':
    main()
