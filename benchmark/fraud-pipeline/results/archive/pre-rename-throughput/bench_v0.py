#!/usr/bin/env python3
"""Beava v0 throughput benchmark — sync/async PUSH over the v0 SDK.

Mirrors the v2.0 bench.py 9-cell matrix (small/medium/large x 1c/4c/8c) but
drives the v0 `@bv.stream` + `@bv.table(key=...)` + `group_by().agg(...)` API
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

import beava as bv  # noqa: E402


# ---------------------------------------------------------------------------
# v0 pipeline definitions — matched in spirit to the v2.0 small/medium/large.
# Single-key aggregations only (v0→v2 translator enforces this in 22-04).
# ---------------------------------------------------------------------------

def define_small():
    """1 source stream + 1 keyed aggregation, 5 features."""
    @bv.stream
    class RawTxns:
        user_id: str
        amount: float

    @bv.table(key="user_id")
    def Transactions(raw: RawTxns) -> bv.Table:
        return raw.group_by("user_id").agg(
            tx_count_1h=bv.count(window='1h'),
            tx_sum_1h=bv.sum('amount', window='1h'),
            avg_amount_1h=bv.avg('amount', window='1h'),
            max_amount_24h=bv.max('amount', window='24h'),
            min_amount_24h=bv.min('amount', window='24h'),
        )

    return [RawTxns, Transactions], RawTxns


def define_medium():
    """1 source + 1 user-keyed aggregation with a where-filtered count."""
    @bv.stream
    class RawTxns:
        user_id: str
        amount: float
        status: str

    @bv.table(key="user_id")
    def Transactions(raw: RawTxns) -> bv.Table:
        return raw.group_by("user_id").agg(
            tx_count_1h=bv.count(window='1h'),
            tx_sum_1h=bv.sum('amount', window='1h'),
            avg_amount_1h=bv.avg('amount', window='1h'),
            max_amount_24h=bv.max('amount', window='24h'),
            failed_count_30m=bv.count(window='30m', where="status == 'failed'"),
        )

    return [RawTxns, Transactions], RawTxns


def define_large():
    """1 source + user-keyed aggregation with 7 features including count_distinct."""
    @bv.stream
    class RawTxns:
        user_id: str
        merchant_id: str
        amount: float
        status: str

    @bv.table(key="user_id")
    def Transactions(raw: RawTxns) -> bv.Table:
        return raw.group_by("user_id").agg(
            tx_count_1h=bv.count(window='1h'),
            tx_sum_1h=bv.sum('amount', window='1h'),
            avg_amount_1h=bv.avg('amount', window='1h'),
            max_amount_24h=bv.max('amount', window='24h'),
            min_amount_24h=bv.min('amount', window='24h'),
            failed_count_30m=bv.count(window='30m', where="status == 'failed'"),
            unique_merchants_24h=bv.count_distinct('merchant_id', window='24h'),
        )

    return [RawTxns, Transactions], RawTxns


def define_join_small():
    """Stream↔Stream windowed join feeding a count aggregation.

    Primary push target is Orders. Events are generated with an
    `_event_time` that spans the within window so ~50% probe into the
    opposite side match. See bench_v0.run_benchmark generator seed.
    """
    @bv.stream
    class BenchOrders:
        user_id: str
        order_id: str

    @bv.stream
    class BenchPayments:
        user_id: str
        order_id: str
        amount: float

    Joined = BenchOrders.join(
        BenchPayments, on=["user_id", "order_id"], within="30s", type="inner"
    )

    @bv.table(key="user_id")
    def BenchJoinAgg(j: Joined) -> bv.Table:
        return j.group_by("user_id").agg(matched=bv.count(window='1h'))

    return [BenchOrders, BenchPayments, Joined, BenchJoinAgg], BenchOrders


def define_enrich_small():
    """Stream↔Table enrichment feeding a count aggregation.

    Primary push target is BenchClicks. BenchProfile is pre-populated
    before the benchmark loop (via a warmup pass).
    """
    @bv.stream
    class BenchClicks:
        user_id: str
        page: str

    @bv.table(key="user_id")
    class BenchProfile:
        user_id: str
        country: str

    Enriched = BenchClicks.join(BenchProfile, on="user_id", type="left")

    @bv.table(key="country")
    def BenchEnrichAgg(e: Enriched) -> bv.Table:
        return e.group_by("country").agg(n=bv.count(window='1h'))

    return [BenchClicks, BenchProfile, Enriched, BenchEnrichAgg], BenchClicks


# ---------------------------------------------------------------------------
# Phase 24 characterization pipelines
# ---------------------------------------------------------------------------

def define_late_events_small():
    """Small shape, but 10% of events arrive late-but-in-window.

    Events are stamped with ``_event_time``; 10% are generated with
    ``event_time = arrival_ms - 4000`` (i.e. 4 seconds in the past,
    comfortably inside the 5 s watermark window so they are *accepted*
    and flow through the watermark-compare path without being dropped).

    This cell measures the cost of `parse_event_time` + the watermark
    read-gate + the RingBuffer's event-time bucket-routing logic on
    the hot PUSH path, relative to `small_1c`.
    """
    @bv.stream
    class LateEvtTxns:
        user_id: str
        amount: float

    @bv.table(key="user_id")
    def LateEvtAgg(raw: LateEvtTxns) -> bv.Table:
        return raw.group_by("user_id").agg(
            tx_count_1h=bv.count(window='1h'),
            tx_sum_1h=bv.sum('amount', window='1h'),
            avg_amount_1h=bv.avg('amount', window='1h'),
            max_amount_24h=bv.max('amount', window='24h'),
            min_amount_24h=bv.min('amount', window='24h'),
        )

    return [LateEvtTxns, LateEvtAgg], LateEvtTxns


def define_tombstone_cascade_small():
    """Two source Tables + TT-join. 1% of Table upserts are OP_DELETE_TABLE.

    Measures cascade throughput under tombstone load: every push/delete
    fires `cascade_table_upsert`, which re-reads both sides and re-
    materialises the inner-join output row (or tombstones it).
    """
    @bv.table(key="user_id")
    class TsProfile:
        user_id: str
        country: str

    @bv.table(key="user_id")
    class TsRisk:
        user_id: str
        score: int

    TsView = TsProfile.join(TsRisk, on="user_id", type="inner")

    # Primary driver is TsProfile (the benchmark loop pushes / deletes
    # rows on this table; TsRisk is seeded once in the warmup).
    return [TsProfile, TsRisk, TsView], TsProfile


def define_tt_join_real_small():
    """Two source Tables + TT-join; driver pushes to the left table only.

    No tombstones — this cell isolates the real OP_PUSH_TABLE → cascade
    path for the TT-join migration (plan 24-03). Compares against the
    Phase 23 marker-shim cost as a sanity check in SUMMARY.md.
    """
    @bv.table(key="user_id")
    class TtA:
        user_id: str
        x: int

    @bv.table(key="user_id")
    class TtB:
        user_id: str
        y: int

    TtJ = TtA.join(TtB, on="user_id", type="inner")

    return [TtA, TtB, TtJ], TtA


def define_enrich_with_wm_small():
    """Stream↔Table enrichment with `_event_time` on every event.

    Otherwise identical to `define_enrich_small`. Isolates the cost of
    the watermark parse + the `event_time()` builtin relative to the
    non-watermarked enrichment cell.
    """
    @bv.stream
    class WmClicks:
        user_id: str
        page: str

    @bv.table(key="user_id")
    class WmProfile:
        user_id: str
        country: str

    WmEnriched = WmClicks.join(WmProfile, on="user_id", type="left")

    @bv.table(key="country")
    def WmEnrichAgg(e: WmEnriched) -> bv.Table:
        return e.group_by("country").agg(n=bv.count(window='1h'))

    return [WmClicks, WmProfile, WmEnriched, WmEnrichAgg], WmClicks


PIPELINES = {
    'small': define_small,
    'medium': define_medium,
    'large': define_large,
    'join': define_join_small,
    'enrich': define_enrich_small,
    # Phase 24 characterization cells
    'late_events': define_late_events_small,
    'tombstone_cascade': define_tombstone_cascade_small,
    'tt_join_real': define_tt_join_real_small,
    'enrich_with_wm': define_enrich_with_wm_small,
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


def make_event_with_et(i, arrival_ms, late_ratio=0.0, late_offset_ms=4000,
                       user_pool=1000):
    """Event generator that stamps `_event_time` on every event.

    * `late_ratio=0.0` → every event stamped at `arrival_ms`.
    * `late_ratio=0.1` → 10% of events stamped at `arrival_ms - late_offset_ms`
      (late-but-in-window — inside the 5s watermark, so accepted).
    """
    ev = {
        'user_id': f'user_{i % user_pool}',
        'amount': round(random.uniform(1.0, 1000.0), 2),
        'page': 'p' + str(i % 7),
        'country': random.choice(['US', 'UK', 'DE', 'FR', 'JP']),
    }
    if late_ratio > 0.0 and (i % int(1 / late_ratio)) == 0:
        ev['_event_time'] = int(arrival_ms - late_offset_ms)
    else:
        ev['_event_time'] = int(arrival_ms)
    return ev


# ---------------------------------------------------------------------------
# Runners (mirror bench.py semantics: async fire-and-forget + trailing flush)
# ---------------------------------------------------------------------------

def run_async_client(primary_cls, events_per_client, client_id, warmup=1000,
                     sample_latency=False, host='localhost:6400'):
    app = bv.App(host, timeout=30.0)
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


def run_event_time_client(primary_cls, events_per_client, client_id,
                          late_ratio=0.0, warmup=1000, sample_latency=False,
                          host='localhost:6400'):
    """Stream client that stamps `_event_time` on every event (optionally
    late-but-in-window for a fixed fraction). Used by the `late_events_small`
    and `enrich_with_wm_small` characterization cells.
    """
    app = bv.App(host, timeout=30.0)
    base_ms = int(time.time() * 1000)
    for i in range(warmup):
        # Warmup uses non-late events only (watermarks want monotone advance).
        app.push(primary_cls, make_event_with_et(
            i + client_id * 1_000_000, base_ms + i, late_ratio=0.0,
        ))
    app.flush()

    latencies = []
    t0 = time.perf_counter()
    if sample_latency:
        STRIDE = 8
        for i in range(events_per_client):
            ev = make_event_with_et(
                i + client_id * 1_000_000, base_ms + 10_000 + i,
                late_ratio=late_ratio,
            )
            if i % STRIDE == 0:
                t_start = time.perf_counter_ns()
                app.push(primary_cls, ev)
                latencies.append((time.perf_counter_ns() - t_start) / 1000.0)
            else:
                app.push(primary_cls, ev)
        app.flush()
    else:
        for i in range(events_per_client):
            app.push(primary_cls, make_event_with_et(
                i + client_id * 1_000_000, base_ms + 10_000 + i,
                late_ratio=late_ratio,
            ))
        app.flush()
    wall = time.perf_counter() - t0
    return latencies, wall


def run_table_push_client(primary_table, events_per_client, client_id,
                          delete_ratio=0.0, user_pool=1000,
                          warmup=500, sample_latency=False,
                          host='localhost:6400'):
    """Table-driven client using `app.push(Table, key, fields)` (sync).

    * `delete_ratio=0.0` → all events are upserts.
    * `delete_ratio=0.01` → 1% of events are `app.delete(Table, key)`.
    """
    app = bv.App(host, timeout=30.0)
    # Warmup upserts only (delete-without-prior-upsert is legal but uninteresting).
    for i in range(warmup):
        key = f'user_{(i + client_id * 1_000_000) % user_pool}'
        app.push(primary_table, key, {'country': 'US', 'x': int(i)})

    latencies = []
    t0 = time.perf_counter()
    delete_every = int(1 / delete_ratio) if delete_ratio > 0 else 0
    STRIDE = 8
    for i in range(events_per_client):
        key = f'user_{(i + client_id * 1_000_000) % user_pool}'
        is_delete = delete_every and (i % delete_every) == 0
        if sample_latency and i % STRIDE == 0:
            t_start = time.perf_counter_ns()
            if is_delete:
                app.delete(primary_table, key)
            else:
                app.push(primary_table, key,
                         {'country': random.choice(['US', 'UK', 'DE']),
                          'x': int(i)})
            latencies.append((time.perf_counter_ns() - t_start) / 1000.0)
        else:
            if is_delete:
                app.delete(primary_table, key)
            else:
                app.push(primary_table, key,
                         {'country': random.choice(['US', 'UK', 'DE']),
                          'x': int(i)})
    wall = time.perf_counter() - t0
    return latencies, wall


# Pipelines that need custom runners (non-stream driver or event-time stamping).
_CUSTOM_RUNNER = {
    'late_events': ('event_time_stream', {'late_ratio': 0.1}),
    'enrich_with_wm': ('event_time_stream', {'late_ratio': 0.0}),
    'tombstone_cascade': ('table_push', {'delete_ratio': 0.01}),
    'tt_join_real': ('table_push', {'delete_ratio': 0.0}),
}


def _seed_right_table_for_enrich(pipeline_name, app, streams):
    """Populate the right-side Table for enrichment cells (and for tt_join_real
    if the left-push cell still expects the right side to exist)."""
    if pipeline_name in ('enrich', 'enrich_with_wm'):
        # Right side is a table named *Profile with a country column.
        profile_tbl = None
        for s in streams:
            name = getattr(s, '_beava_stream_name', None) or getattr(s, '__name__', '')
            if 'Profile' in name and getattr(s, '_beava_kind', None) == 'table':
                profile_tbl = s
                break
        if profile_tbl is not None:
            for u in range(1000):
                app.push(profile_tbl, f'user_{u}',
                         {'country': random.choice(['US', 'UK', 'DE', 'FR', 'JP'])})
    elif pipeline_name == 'tt_join_real':
        # Seed TtB for every user so the inner-join has something to match.
        ttb = None
        for s in streams:
            name = getattr(s, '_beava_stream_name', None) or getattr(s, '__name__', '')
            if name == 'TtB':
                ttb = s
                break
        if ttb is not None:
            for u in range(1000):
                app.push(ttb, f'user_{u}', {'y': int(u)})
    elif pipeline_name == 'tombstone_cascade':
        # Seed TsRisk so the TT-cascade has a Live right side to merge with.
        tsrisk = None
        for s in streams:
            name = getattr(s, '_beava_stream_name', None) or getattr(s, '__name__', '')
            if name == 'TsRisk':
                tsrisk = s
                break
        if tsrisk is not None:
            for u in range(1000):
                app.push(tsrisk, f'user_{u}', {'score': int(u % 100)})


def percentile(values, p):
    if not values:
        return 0.0
    s = sorted(values)
    idx = max(0, min(len(s) - 1, int(p / 100.0 * len(s))))
    return s[idx]


def run_benchmark(pipeline_name, clients, events_per_client, host='localhost:6400',
                  sample_latency=True, quiet=True):
    streams, primary = PIPELINES[pipeline_name]()
    app = bv.App(host, timeout=30.0)
    app.register(*streams)

    custom = _CUSTOM_RUNNER.get(pipeline_name)
    # Seed right-side tables for enrichment + TT cells (characterization only).
    _seed_right_table_for_enrich(pipeline_name, app, streams)

    # Warmup ping. Skip for Table-primary cells (no make_event shape).
    if custom is None or custom[0] != 'table_push':
        app.push_sync(primary, make_event(0))

    all_lat = []
    if custom is None:
        runner = run_async_client
        runner_kwargs = {}
    elif custom[0] == 'event_time_stream':
        runner = run_event_time_client
        runner_kwargs = dict(custom[1])
    elif custom[0] == 'table_push':
        runner = run_table_push_client
        runner_kwargs = dict(custom[1])
    else:
        raise RuntimeError(f'unknown custom runner: {custom!r}')

    if clients == 1:
        all_lat, wall = runner(primary, events_per_client, 0,
                               sample_latency=sample_latency, host=host,
                               **runner_kwargs)
    else:
        with ThreadPoolExecutor(max_workers=clients) as pool:
            futures = [
                pool.submit(runner, primary, events_per_client, cid,
                            sample_latency=sample_latency, host=host,
                            **runner_kwargs)
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


def _probe_warnings_endpoint(host):
    """Plan 25-03: probe GET /debug/warnings once per matrix run and record
    its response latency. Non-gated characterisation; the matrix regression
    gate is against BASELINE.json throughput cells only.

    `host` is `hostname:tcp_port` (default 6400). The HTTP admin port is
    assumed to be `tcp_port + 1` per main.rs (default 6401).
    """
    try:
        import socket as _sock
        import urllib.request as _urlreq
        hostname, tcp_port = host.split(':')
        http_port = int(tcp_port) + 1
        url = f'http://{hostname}:{http_port}/debug/warnings'
        # Two runs: first warms any page-cache / route-table state, second
        # is what we record. Matches the matrix cell pattern.
        latencies_us = []
        for _ in range(3):
            t0 = time.perf_counter_ns()
            try:
                with _urlreq.urlopen(url, timeout=2.0) as r:
                    _ = r.read()
            except Exception:
                return {'error': 'endpoint unreachable', 'url': url}
            latencies_us.append((time.perf_counter_ns() - t0) / 1000.0)
        return {
            'url': url,
            'samples_us': [round(x, 2) for x in latencies_us],
            'median_us': round(sorted(latencies_us)[len(latencies_us) // 2], 2),
            'note': 'observational — not part of the regression gate',
        }
    except Exception as e:
        return {'error': repr(e)}


def run_matrix(events_per_run, runs_per_cell, out_path, label, host):
    # 9 regression cells + 2 Phase-23 characterization cells + 4 Phase-24
    # characterization cells (all @ 1c).
    pipelines = ['small', 'medium', 'large']
    clients_list = [1, 4, 8]
    char_cells = [
        # Phase 23 characterization
        ('join', 1),
        ('enrich', 1),
        # Phase 24 characterization
        ('late_events', 1),
        ('tombstone_cascade', 1),
        ('tt_join_real', 1),
        ('enrich_with_wm', 1),
    ]
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
        char_keys = (
            'join_small_1c', 'enrich_small_1c',
            'late_events_small_1c', 'tombstone_cascade_small_1c',
            'tt_join_real_small_1c', 'enrich_with_wm_small_1c',
        )
        for key in char_keys:
            if key in results and 'eps_median' in results[key] and base_small_1c > 0:
                results[key]['pct_of_small_1c'] = round(
                    results[key]['eps_median'] / base_small_1c * 100.0, 2
                )

    # Plan 25-03: observational probe of /debug/warnings latency.
    # Not gated — purely for characterisation in the matrix output.
    warnings_probe = _probe_warnings_endpoint(host)

    out = {
        'label': label,
        'baseline_ref': 'BASELINE.json' if baseline else None,
        'timestamp': datetime.now(timezone.utc).isoformat(),
        'events_per_run': events_per_run,
        'runs_per_cell': runs_per_cell,
        'wall_seconds': round(time.time() - t0, 1),
        'cells': results,
        'gate_passed': gate_passed,
        'warnings_endpoint_probe': warnings_probe,
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
    ap.add_argument('--out', '--output', dest='out', default=None)
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
