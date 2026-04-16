#!/usr/bin/env python3
"""Multi-stream throughput benchmark — Phase 40 validation.

Registers N independent source streams and pushes events round-robin across
them from C client threads. The whole point is to prove that the per-stream
event-log writer locks (Phase 40 refactor of `EventLog`) actually let pushes
to different streams progress in parallel under high `TALLY_WORKER_THREADS`.

Before Phase 40 every PUSH serialized through one global mutex, so throughput
capped at whatever a single writer thread could sustain no matter how many
workers or client threads were thrown at it.

Usage:
    # Baseline: 1 stream, 8 clients — all clients fight for one writer lock.
    python3 bench_multistream.py --streams 1 --clients 8 --events 30000

    # Scaling: 4 streams, 8 clients — expect ~3-4× baseline throughput.
    python3 bench_multistream.py --streams 4 --clients 8 --events 30000

    # Stress: 8 streams, 8 clients — one client per stream, minimum contention.
    python3 bench_multistream.py --streams 8 --clients 8 --events 30000
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
# Dynamic stream factory
# ---------------------------------------------------------------------------

def make_stream_classes(n_streams):
    """Return a list of N distinct @tl.stream classes + their matching
    @tl.table aggregations. Each stream has the same small shape as
    bench_v0's `define_small` so results are comparable.

    The decorators stamp metadata keyed on the class/function name, so
    we build each pair via exec() inside a fresh namespace to get N
    uniquely-named definitions without copy-pasting.
    """
    classes = []
    aggs = []
    for idx in range(n_streams):
        stream_name = f'BenchMultiS{idx}'
        agg_name = f'BenchMultiAgg{idx}'
        ns = {'tl': tl, 'str': str, 'float': float}
        exec(
            f"@tl.stream\n"
            f"class {stream_name}:\n"
            f"    user_id: str\n"
            f"    amount: float\n"
            f"\n"
            f"@tl.table(key='user_id')\n"
            f"def {agg_name}(raw: {stream_name}) -> tl.Table:\n"
            f"    return raw.group_by('user_id').agg(\n"
            f"        tx_count_1h=tl.count(window='1h'),\n"
            f"        tx_sum_1h=tl.sum('amount', window='1h'),\n"
            f"    )\n",
            ns,
        )
        classes.append(ns[stream_name])
        aggs.append(ns[agg_name])
    return classes, aggs


# ---------------------------------------------------------------------------
# Event generator (shared with bench_v0 in spirit)
# ---------------------------------------------------------------------------

def make_event(i, user_pool=1000):
    return {
        'user_id': f'user_{i % user_pool}',
        'amount': round(random.uniform(1.0, 1000.0), 2),
    }


# ---------------------------------------------------------------------------
# Client runner: each client picks an assigned stream (round-robin by client
# id modulo n_streams) and pushes events exclusively to that stream.
#
# This gives the cleanest scaling signal: N streams × (C/N) clients each.
# Alternative (one client rotating across all streams) was considered but
# pulls SDK-internal PUSH-async coalescing costs into the measurement.
# ---------------------------------------------------------------------------

def run_client(stream_cls, events_per_client, client_id,
               warmup=1000, sample_latency=False, host='localhost:6400'):
    app = tl.App(host, timeout=30.0)
    for i in range(warmup):
        app.push(stream_cls, make_event(i + client_id * 1_000_000))
    app.flush()

    latencies = []
    t0 = time.perf_counter()
    if sample_latency:
        STRIDE = 8
        for i in range(events_per_client):
            ev = make_event(i + client_id * 1_000_000)
            if i % STRIDE == 0:
                t_start = time.perf_counter_ns()
                app.push(stream_cls, ev)
                latencies.append((time.perf_counter_ns() - t_start) / 1000.0)
            else:
                app.push(stream_cls, ev)
        app.flush()
    else:
        for i in range(events_per_client):
            app.push(stream_cls, make_event(i + client_id * 1_000_000))
        app.flush()
    wall = time.perf_counter() - t0
    return latencies, wall


def percentile(values, p):
    if not values:
        return 0.0
    s = sorted(values)
    idx = max(0, min(len(s) - 1, int(p / 100.0 * len(s))))
    return s[idx]


def run_scenario(n_streams, n_clients, events_per_client,
                 host='localhost:6400', sample_latency=True, quiet=False):
    streams, aggs = make_stream_classes(n_streams)
    app = tl.App(host, timeout=30.0)
    app.register(*streams, *aggs)

    # Warmup ping for every stream (so the server pre-creates writers +
    # the SDK's pooled connections stay hot).
    for s in streams:
        app.push_sync(s, make_event(0))

    # Assign each client to one stream round-robin.
    assignments = [streams[cid % n_streams] for cid in range(n_clients)]

    all_lat = []
    if n_clients == 1:
        all_lat, wall = run_client(assignments[0], events_per_client, 0,
                                   sample_latency=sample_latency, host=host)
    else:
        with ThreadPoolExecutor(max_workers=n_clients) as pool:
            futures = [
                pool.submit(run_client, assignments[cid], events_per_client, cid,
                            sample_latency=sample_latency, host=host)
                for cid in range(n_clients)
            ]
            walls = []
            for f in as_completed(futures):
                lat, w = f.result()
                walls.append(w)
                all_lat.extend(lat)
            wall = max(walls) if walls else 0.0

    total = events_per_client * n_clients
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
        print(f'streams={n_streams} clients={n_clients}: '
              f'{total:,} events in {wall:.2f}s = {eps:,.0f} eps')
        if lat_block:
            print(f'  p50={lat_block["p50"]}us p99={lat_block["p99"]}us')
    return {
        'streams': n_streams,
        'clients': n_clients,
        'events': total,
        'wall_seconds': round(wall, 3),
        'throughput_eps': round(eps, 1),
        'latency_us': lat_block,
    }


def run_matrix(events_per_run, host, label, out_path, runs_per_cell=3):
    scenarios = [
        (1, 8),
        (4, 8),
        (8, 8),
    ]
    t0 = time.time()
    cells = {}
    for (n_streams, n_clients) in scenarios:
        key = f's{n_streams}_c{n_clients}'
        eps_list, p50_list, p99_list = [], [], []
        for run in range(runs_per_cell):
            try:
                r = run_scenario(n_streams, n_clients, events_per_run // n_clients,
                                 host=host, sample_latency=True, quiet=True)
                eps_list.append(r['throughput_eps'])
                if r['latency_us']:
                    p50_list.append(r['latency_us']['p50'])
                    p99_list.append(r['latency_us']['p99'])
            except Exception as e:
                print(f'  !!! {key} run {run}: {e!r}')
            time.sleep(0.3)
        if not eps_list:
            cells[key] = {'error': 'all runs failed'}
            continue
        eps_list.sort()
        mid = len(eps_list) // 2
        cells[key] = {
            'streams': n_streams,
            'clients': n_clients,
            'runs': len(eps_list),
            'eps_median': eps_list[mid],
            'eps_all': eps_list,
            'p50_median': sorted(p50_list)[len(p50_list) // 2] if p50_list else None,
            'p99_median': sorted(p99_list)[len(p99_list) // 2] if p99_list else None,
        }
        print(f'{key:10s} streams={n_streams} clients={n_clients} '
              f'eps={eps_list[mid]:>10,.0f}  p99={cells[key]["p99_median"]}us')

    # Scaling ratio vs s1_c8 baseline.
    base = cells.get('s1_c8', {}).get('eps_median')
    if base and base > 0:
        for key, cell in cells.items():
            if 'eps_median' in cell:
                cell['scaling_factor_vs_s1c8'] = round(cell['eps_median'] / base, 2)

    out = {
        'label': label,
        'timestamp': datetime.now(timezone.utc).isoformat(),
        'events_per_run': events_per_run,
        'runs_per_cell': runs_per_cell,
        'wall_seconds': round(time.time() - t0, 1),
        'cells': cells,
    }
    with open(out_path, 'w') as fh:
        json.dump(out, fh, indent=2)
    print(f'\nWrote {out_path}  ({out["wall_seconds"]}s)')
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--streams', type=int, default=4,
                    help='Number of independent streams (default: 4)')
    ap.add_argument('--clients', type=int, default=8,
                    help='Number of concurrent client threads (default: 8)')
    ap.add_argument('--events', type=int, default=30000,
                    help='Total events across all clients (default: 30000)')
    ap.add_argument('--host', default='localhost:6400')
    ap.add_argument('--label', default='phase40-multistream')
    ap.add_argument('--out', '--output', dest='out', default=None)
    ap.add_argument('--matrix', action='store_true',
                    help='Run the 3-cell scaling matrix (s1c8, s4c8, s8c8).')
    ap.add_argument('--runs', type=int, default=3,
                    help='Runs per cell in matrix mode (default: 3)')
    args = ap.parse_args()

    if args.matrix:
        out_path = args.out or os.path.join(
            os.path.dirname(os.path.abspath(__file__)), 'results',
            f'multistream-{datetime.now().strftime("%Y%m%d-%H%M%S")}.json'
        )
        os.makedirs(os.path.dirname(out_path), exist_ok=True)
        run_matrix(args.events, args.host, args.label, out_path,
                   runs_per_cell=args.runs)
    else:
        r = run_scenario(args.streams, args.clients,
                         args.events // args.clients, host=args.host,
                         sample_latency=True, quiet=False)
        if args.out:
            with open(args.out, 'w') as fh:
                json.dump({'label': args.label, 'result': r}, fh, indent=2)
            print(f'Wrote {args.out}')
        else:
            print(json.dumps(r, indent=2))


if __name__ == '__main__':
    main()
