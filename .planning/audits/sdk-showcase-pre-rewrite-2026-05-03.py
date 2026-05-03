"""beava Python SDK — top-to-bottom code-review showcase (audit artifact).

Purpose: read this file cover-to-cover to SEE the current state of the Python
SDK at v0 launch readiness. Not a tutorial; not a test — a structured walk
through every public surface with broken/missing pieces flagged inline.

How to read: top-down. Each section opens with a prose intro, exercises that
surface, then annotates outcomes:
    # ✅ WORKS                  — call succeeds, behaviour matches docs
    # 🚧 GAP: <description>     — call broken or surface entirely missing
    # ⏳ DEFERRED-V0.1+: <feat>  — intentionally out-of-scope per project memory
Every 🚧 GAP carries fix-scope + (where relevant) a workaround. Consolidated
GAP SUMMARY table at the bottom of the file.

Importable, not runnable: the file imports cleanly (``python
examples/sdk_showcase.py`` will not error). Broken calls are commented out
behind their 🚧 GAP marker. Working calls — class decls, op chaining,
expressions, descriptors — execute eagerly and serve as live documentation.
Server-touching calls are gated behind ``if RUN_LIVE`` (env
``BEAVA_SHOWCASE_LIVE=1``; default off so the file works in any checkout).

Headline findings (full GAP table at the bottom of the file):
  1. BLOCKER — ``GroupBy.agg()`` raises RuntimeError unconditionally
     (python/beava/_agg.py:539-556). Python users cannot declare ANY
     aggregations from the SDK today.
  2. MAJOR — Only 22 of the ~53 server-side AggKind operators have Python
     helpers exported from the ``beava`` namespace. Phase 8 (15 ops),
     Phase 10 sketches (5 ops), and Phase 11 buffer/geo (11 ops) have no
     Python callable.
  3. MAJOR — ``App.push()`` does not exist; users must reach into the
     transport directly (``app._transport.send_push(...)``).
  4. MAJOR — ``App.deregister()`` does not exist anywhere (no SDK or server
     opcode).
  5. MINOR — Schema upgrades work by re-calling ``app.register()``; no
     explicit upgrade/migrate API and no version-pinning.

Cross-references
----------------
* Server op enum:  crates/beava-core/src/agg_op.rs:50  (AggKind, 53 variants)
* Op-name parser:  crates/beava-core/src/agg_compile.rs:253 (parse_agg_kind)
* SDK helpers:     python/beava/_agg.py                  (22 helpers exported)
* Public API:      python/beava/__init__.py              (__all__ list)
* Op shape reference: crates/beava-bench/configs/fraud-team.json
"""

import os
from typing import Any

# NOTE: deliberately NO `from __future__ import annotations` here. Function-form
# `@bv.event` reads the LIVE class object out of the parameter annotation
# (python/beava/_events.py::_decorate_event_function reads `param.annotation`
# directly, not via typing.get_type_hints). PEP 563 stringified annotations
# break that path; this file's BigTxn derivation requires real annotations.

# Import the public surface explicitly — source-of-truth for `from beava import *`.
import beava as bv
from beava import App, Col, Field, Optional, RegistrationError, ValidationError, col, event
# Aggregation helpers — note carefully which names are missing below.
# (sum/min/max shadow builtins — intentional, see python/beava/_agg.py module docstring.)
from beava import (
    AggDescriptor, GroupBy,
    # Phase 5 core (8)
    count, sum, avg, min, max, variance, stddev, ratio,
    # Phase 9 decay (7) — ema is an SDK alias for ewma server-side
    ewma, ema, ewvar, ew_zscore, decayed_sum, decayed_count, twa,
    # Phase 9 velocity (8)
    rate_of_change, inter_arrival_stats, burst_count, delta_from_prev,
    trend, trend_residual, outlier_count, value_change_count,
    # Phase 9 z-score (1)
    z_score,
)

# Live-mode flag — flip via env var so the file imports/runs everywhere.
RUN_LIVE: bool = os.environ.get("BEAVA_SHOWCASE_LIVE") == "1"


# ─── Section 1: Imports + setup ─────────────────────────────────────────────
#
# Status: ✅ WORKS (imports), 🚧 GAP (operator coverage)
# Coverage: python/beava/__init__.py::__all__ vs server AggKind enum
#
# This section verifies that everything in the public namespace actually
# imports. The narrative finding: imports succeed, but the namespace is too
# small. Counting the helpers below against server crates/beava-core/src/agg_op.rs
# produces:
#
#     server AggKind variants ........................ 53
#     beava.* aggregation helpers exported ........... 22
#     coverage ratio ................................. 41 %
#
# The 31 missing helpers fall into Phase 8 (15), Phase 10 sketches (5), and
# Phase 11 buffer/geo (11). They are reachable today only via the raw
# AggDescriptor constructor — but even that lands you in Section 7's blocker
# (GroupBy.agg() raises), so today there is NO supported Python path to
# register a Phase 8/10/11 aggregation.

# ✅ WORKS — namespace imports verified above all resolve
_PUBLIC_NAMES = sorted(name for name in dir(bv) if not name.startswith("_"))

# 🚧 GAP: 31 server-side AggKinds have no Python helper.
# Workaround: AggDescriptor(op="<server_name>", field=..., window=...) — but
#             see Section 7: GroupBy.agg() raises so the descriptor is unsendable.
# Fix scope: ~150 LOC in python/beava/_agg.py (one helper per op + __init__ export).
SERVER_AGG_OPS_MISSING_FROM_SDK = [
    # Phase 8: point/ordinal (5)
    "first", "last", "first_n", "last_n", "lag",
    # Phase 8: recency markers (6)
    "first_seen", "last_seen", "age", "has_seen", "time_since", "time_since_last_n",
    # Phase 8: streaks (3)
    "streak", "max_streak", "negative_streak",
    # Phase 8: windowed recency (1)
    "first_seen_in_window",
    # Phase 10: sketches (5)
    "count_distinct", "percentile", "top_k", "bloom_member", "entropy",
    # Phase 11: bounded-buffer + geo (11)
    "histogram", "hour_of_day_histogram", "dow_hour_histogram", "seasonal_deviation",
    "event_type_mix", "most_recent_n", "reservoir_sample",
    "geo_velocity", "geo_distance", "geo_spread", "distance_from_home",
]
assert len(SERVER_AGG_OPS_MISSING_FROM_SDK) == 31, "audit math drift"


# ─── Section 2: App connection ──────────────────────────────────────────────
#
# Status: ✅ WORKS (constructor + URL dispatch); skipped on import
# Coverage: bv.App, parse_url_to_transport, embed mode, http://, tcp://
#
# All four App construction paths exist and dispatch to the right transport.
# Live mode is gated so this file imports without a running server.

# ✅ WORKS — three URL-dispatched modes + embed
_HTTP_APP_FACTORY = lambda: bv.App("http://localhost:7379")
_TCP_APP_FACTORY = lambda: bv.App("tcp://localhost:7380")
_EMBED_APP_FACTORY = lambda: bv.App()  # embed: spawns local binary

if RUN_LIVE:
    # ✅ WORKS — explicit URL mode does not require a context manager
    http_app = _HTTP_APP_FACTORY()
    print("HTTP ping:", "n/a (HTTP transport has no /ping in v0)")
    http_app.close()

    # ✅ WORKS — TCP transport has /ping
    with _TCP_APP_FACTORY() as tcp_app:
        print("TCP ping:", tcp_app.ping())

    # ✅ WORKS — embed mode REQUIRES context manager (raises otherwise)
    with _EMBED_APP_FACTORY() as embed_app:
        print("Embed ping:", embed_app.ping())


# ─── Section 3: Event source declaration ────────────────────────────────────
#
# Status: ✅ WORKS — class form, function form, all decorator kwargs
# Coverage: @bv.event, Field, Optional, keep_events_for, dedupe_*, cold_after
#
# Decorator + descriptor construction is fully working. Field types map
# correctly (str→str, int→i64, float→f64, bool→bool, bytes→bytes, datetime→
# datetime). Reasonable error messages on misuse (unsupported types, dedupe_key
# not in schema, etc).

# ✅ WORKS — class form (an EventSource descriptor)
@bv.event
class Transaction:
    user_id: str
    amount: float
    mcc: str
    merchant_id: str
    declined: int
    lat: float
    lon: float


# ✅ WORKS — class form with all decorator kwargs
@bv.event(
    keep_events_for="7d",       # event retention TTL
    dedupe_key="event_id",      # idempotency on event_id
    dedupe_window="24h",        # within a 24h sliding window
    cold_after="30d",           # Phase 12.8: per-source cold-entity TTL
)
class Login:
    event_id: str
    user_id: str
    device_id: str
    ip_address: str
    success: int
    user_agent: str
    lat: float
    lon: float


# ✅ WORKS — Field() carries description + default for nicer error messages
@bv.event
class CardAdd:
    user_id: str = bv.Field(desc="hashed user id")
    card_fp: str = bv.Field(desc="card fingerprint")
    bin: str = bv.Field(desc="card BIN", default="")  # default makes it optional-like
    success: int = bv.Field(desc="1 if added, 0 if declined", default=0)


# ✅ WORKS — bv.Optional[T] marks a nullable field (distinct from typing.Optional)
@bv.event
class Refund:
    user_id: str
    card_fp: str
    amount: float
    is_chargeback: int
    reason_code: bv.Optional[str]  # may be null


# ✅ WORKS — function form: derivation referencing an upstream source
@bv.event
def BigTxn(source: Transaction) -> Any:
    """Function-form @bv.event — the function is invoked once at decoration
    time with the upstream descriptor as a placeholder argument. Whatever it
    returns becomes the body of the derivation."""
    return source.filter(bv.col("amount") > 100.0)


# ─── Section 4: Stateless op chaining ───────────────────────────────────────
#
# Status: ✅ WORKS — all 8 ops exist on the mixin
# Coverage: filter, select, drop, rename, with_columns, map, cast, fillna
#
# Every op returns a NEW EventDerivation (never mutates self), as documented
# in _events.py::_EventOpsMixin. Ops compose via fluent chaining.

# ✅ WORKS — each op standalone
_ex_filter = Transaction.filter(bv.col("amount") > 100.0)
_ex_select = Transaction.select("user_id", "amount", "mcc")
_ex_drop = Transaction.drop("lat", "lon")  # drop the columns we don't want
_ex_rename = Transaction.rename(amount="value")  # NOTE: rename DOES exist (contra audit q)
_ex_with_columns = Transaction.with_columns(big=bv.col("amount") > 1000.0)
_ex_map = Transaction.map(amt_x2=bv.col("amount") * 2)  # map is alias for with_columns
_ex_cast = Transaction.cast(amount="float", declined="bool")
_ex_fillna = Transaction.fillna(merchant_id="UNKNOWN", mcc="0000")

# ✅ WORKS — chained pipeline
_chained = (
    Transaction
    .filter(bv.col("amount") > 0.0)                  # drop refunds
    .with_columns(amount_usd=bv.col("amount"))       # rename via derived column
    .drop("lat", "lon")                              # ignore geo
    .cast(amount_usd="float")                        # ensure float
    .fillna(mcc="0000")                              # default MCC
)


# ─── Section 5: Expression DSL (bv.col) ─────────────────────────────────────
#
# Status: ✅ WORKS for arithmetic/comparison/boolean/null/cast
#         🚧 GAP for bv.lit (no public literal helper)
#         🚧 GAP for string functions, datetime functions
# Coverage: python/beava/_col.py
#
# bv.col() returns an _ExprAST that overloads Python operators. Every binary
# op is parenthesized in the serialized form (see _col.py docstring D-08).

# ✅ WORKS — arithmetic operators: + - * /
_ex_add = bv.col("amount") + bv.col("fee")
_ex_sub = bv.col("amount") - 10
_ex_mul = bv.col("amount") * 1.05
_ex_div = bv.col("amount") / bv.col("count")
assert _ex_add.to_expr_string() == "(amount + fee)"

# ✅ WORKS — comparison operators: == != < > <= >=
_ex_eq = bv.col("status") == "approved"
_ex_ne = bv.col("status") != "declined"
_ex_lt = bv.col("amount") < 100
_ex_gt = bv.col("amount") > 1000
_ex_le = bv.col("amount") <= 50.0
_ex_ge = bv.col("amount") >= 0.0
# String literal escaping is automatic (T-03-02-01 mitigation):
assert _ex_eq.to_expr_string() == "(status == 'approved')"

# ✅ WORKS — boolean combinators: & | ~ (not Python's `and`/`or`/`not` —
# those keywords cannot be overloaded in Python).
_ex_and = (bv.col("amount") > 100) & (bv.col("declined") == 0)
_ex_or = (bv.col("mcc") == "5411") | (bv.col("mcc") == "5812")
_ex_not = ~(bv.col("declined") == 1)

# ✅ WORKS — .isnull() shorthand for `(x == null)`
_ex_null = bv.col("merchant_id").isnull()

# ✅ WORKS — .cast(type) renders as `cast(x, float)` (bare ident, not quoted)
_ex_cast_expr = bv.col("amount").cast("float")

# 🚧 GAP: `bv.lit(...)` is NOT exported — there is no public literal constructor.
# Workaround: pass plain Python scalars on the RHS of an op; the wrapper
#             auto-promotes them via _wrap() → _Literal. This works for
#             comparisons (`bv.col("x") > 100`) but cannot represent a bare
#             literal expression like `bv.lit(0)` standalone.
# Fix scope: ~10 LOC in python/beava/_col.py + __init__ export. Trivial:
#            `def lit(value: Any) -> _ExprAST: return _Literal(value)`.
# bv.lit(0)  # would NameError today

# 🚧 GAP: No string functions (lower, upper, contains, starts_with, regex_match).
# Workaround: pre-process the field client-side, or use .with_columns() with
#             arithmetic-only derivations. Substring/regex predicates are
#             unreachable from Python.
# Fix scope: needs API design — must coordinate with server expression
#            evaluator (crates/beava-core/src/expr.rs). Not in v0 scope.

# 🚧 GAP: No datetime functions (hour_of, day_of_week, epoch_ms, etc).
# Workaround: cast(timestamp, int) and do arithmetic with epoch ms manually.
# Fix scope: needs API design + server expression-eval support. Not in v0.


# ─── Section 6: ALL 53 aggregations ─────────────────────────────────────────
#
# Status: ✅ WORKS for 22 helpers; 🚧 GAP for the remaining 31
# Coverage: every variant of crates/beava-core/src/agg_op.rs::AggKind
#
# Phase 5 core (8 ops) — every helper exists and is callable.
# Phase 8 (15 ops)     — ZERO Python helpers; 31 server-side ops are unreachable
# Phase 9 (15 ops)     — every helper exists
# Phase 10 (5 ops)     — ZERO Python helpers
# Phase 11 (11 ops)    — ZERO Python helpers
#
# All op constants and parameter shapes below are cross-referenced against
# crates/beava-bench/configs/fraud-team.json so the workarounds mirror a real
# fraud workload. fraud-team is the locked tuning benchmark per
# project_fraud_team_primary_bench (2026-04-27).

# ── Phase 5 core (8 ops) ──────────────────────────────────────────────────
# All helpers present and exported.
_p5_count = bv.count(window="5m")
_p5_count_lifetime = bv.count()  # AGG-CORE-09: window omitted = lifetime
_p5_sum = bv.sum("amount", window="1h")
_p5_avg = bv.avg("amount", window="24h")
_p5_min = bv.min("amount", window="1h")
_p5_max = bv.max("amount", window="1h")
_p5_variance = bv.variance("amount", window="24h")
_p5_stddev = bv.stddev("amount", window="24h")
_p5_ratio = bv.ratio(where=bv.col("declined") == 1, window="1h")  # decline rate

assert _p5_count.op == "count"
assert _p5_sum.to_agg_spec() == {
    "op": "sum",
    "params": {"field": "amount", "window": "1h"},
}

# ── Phase 8 ordinal/recency/streak (15 ops) — 🚧 ALL HELPERS MISSING ──────
#
# Two sub-flavours of gap:
#   (a) Helper-only gap: AggDescriptor(op=...) constructs the descriptor; the
#       only thing missing is the friendly Python wrapper.
#   (b) Helper + AggDescriptor field gap: descriptor has NO slot for the
#       parameter (n, sketch_params, ext params). Today these ops are
#       unreachable from Python at any layer.
#
# Workarounds shown for (a) only — see fraud-team.json for canonical wire shape.
# 🚧 GAP: bv.first(field, window)         — (a) AggDescriptor(op="first", field=..., window=...)
# 🚧 GAP: bv.last(field, window)          — (a) AggDescriptor(op="last", field=..., window=...)
# 🚧 GAP: bv.first_n(field, n, window)    — (b) AggDescriptor.n field missing
# 🚧 GAP: bv.last_n(field, n, window)     — (b) same
# 🚧 GAP: bv.lag(field, n)                — (b) same
# 🚧 GAP: bv.first_seen()                 — (a) AggDescriptor(op="first_seen")
# 🚧 GAP: bv.last_seen()                  — (a) AggDescriptor(op="last_seen")
# 🚧 GAP: bv.age()                        — (a) AggDescriptor(op="age")
# 🚧 GAP: bv.has_seen(where=...)          — (a) AggDescriptor(op="has_seen", where=...)
# 🚧 GAP: bv.time_since(field)            — (a) AggDescriptor(op="time_since")
# 🚧 GAP: bv.time_since_last_n(n)         — (b) AggDescriptor.n field missing
# 🚧 GAP: bv.streak()                     — (a) AggDescriptor(op="streak")
# 🚧 GAP: bv.max_streak()                 — (a) AggDescriptor(op="max_streak")
# 🚧 GAP: bv.negative_streak(field)       — (a) AggDescriptor(op="negative_streak", field=...)
# 🚧 GAP: bv.first_seen_in_window(window) — (a) AggDescriptor(op="first_seen_in_window", window=...)
# Fix scope: ~150 LOC (15 helpers + __all__ exports + extend AggDescriptor
#            with `n: int | None = None` + update to_agg_spec()).
_workaround_first = AggDescriptor(op="first", field="merchant_id", window="24h")
_workaround_first_seen = AggDescriptor(op="first_seen")
_workaround_streak = AggDescriptor(op="streak")

# ── Phase 9 decay/velocity/z (15 ops) — ✅ ALL HELPERS EXIST ──────────────
# decay (7): ewma, ema-alias, ewvar, ew_zscore, decayed_sum, decayed_count, twa
_p9_ewma = bv.ewma("amount", half_life="1h")
_p9_ema = bv.ema("amount", half_life="1h")
_p9_ewvar = bv.ewvar("amount", half_life="1h")
_p9_ew_zscore = bv.ew_zscore("amount", half_life="1h")
_p9_decayed_sum = bv.decayed_sum("amount", half_life="2h")
_p9_decayed_count = bv.decayed_count(half_life="2h")
_p9_twa = bv.twa("amount", window="24h")
# velocity (8): rate_of_change, inter_arrival_stats, burst_count, delta_from_prev,
#               trend, trend_residual, outlier_count, value_change_count
_p9_rate = bv.rate_of_change("amount", window="1h")
_p9_iat = bv.inter_arrival_stats(window="5m")
_p9_burst = bv.burst_count(window="5m", sub_window="10s")
_p9_delta = bv.delta_from_prev("amount")  # windowless OK
_p9_trend = bv.trend("amount", window="24h")
_p9_trend_resid = bv.trend_residual("amount", window="24h")
_p9_outlier = bv.outlier_count("amount", window="1h", sigma=3.0)
_p9_value_change = bv.value_change_count("device_id", window="24h")
# z-score (1):
_p9_z = bv.z_score("amount", baseline_window="7d")

# ── Phase 10 sketches (5 ops) — 🚧 ALL HELPERS MISSING ────────────────────
#
# 🚧 GAP: bv.count_distinct(field, window) — (a) HyperLogLog cardinality.
#         Default HLL precision used because AggDescriptor.sketch_params absent.
# 🚧 GAP: bv.percentile(field, q, window)  — (b) t-digest; q param has no slot.
# 🚧 GAP: bv.top_k(field, k, window)       — (b) k param has no slot.
# 🚧 GAP: bv.bloom_member(field)           — (a) windowless-only per AggKind comment.
# 🚧 GAP: bv.entropy(field, window)        — (a) Shannon entropy.
# Fix scope: ~80 LOC (5 helpers + __all__ + extend AggDescriptor.sketch_params).
_workaround_count_distinct = AggDescriptor(op="count_distinct", field="merchant_id", window="24h")
_workaround_bloom = AggDescriptor(op="bloom_member", field="device_id")
_workaround_entropy = AggDescriptor(op="entropy", field="merchant_id", window="24h")

# ── Phase 11 buffer + geo (11 ops) — 🚧 ALL HELPERS MISSING ───────────────
#
# 🚧 GAP: bv.histogram(field, buckets, window)   — (b) buckets list has no slot.
# 🚧 GAP: bv.hour_of_day_histogram()             — (a) no params per fraud-team.
# 🚧 GAP: bv.dow_hour_histogram()                — (a) no params.
# 🚧 GAP: bv.seasonal_deviation(field)           — (a) field-only.
# 🚧 GAP: bv.event_type_mix(field)               — (a) field-only.
# 🚧 GAP: bv.most_recent_n(field, n)             — (b) n has no slot.
# 🚧 GAP: bv.reservoir_sample(field, samples)    — (b) samples has no slot.
# 🚧 GAP: bv.geo_velocity(lat="lat", lon="lon")  — (b) lat/lon FIELD-NAMES, no slot.
# 🚧 GAP: bv.geo_distance(lat=..., lon=...)      — (b) same.
# 🚧 GAP: bv.geo_spread(lat=..., lon=..., window="24h") — (b) same.
# 🚧 GAP: bv.distance_from_home(lat=..., lon=...) — (b) same.
# Fix scope: ~200 LOC (11 helpers + __all__ + extend AggDescriptor with
#            buckets, lat_field, lon_field, samples, n).
_workaround_hour_hist = AggDescriptor(op="hour_of_day_histogram")
_workaround_seasonal_dev = AggDescriptor(op="seasonal_deviation", field="amount")
_workaround_event_mix = AggDescriptor(op="event_type_mix", field="mcc")

# Section-6 helper-gap tally: 15 (Phase 8) + 5 (Phase 10) + 11 (Phase 11) = 31.


# ─── Section 7: Aggregation pipeline (group_by().agg()) ─────────────────────
#
# Status: 🚧 BLOCKER GAP — GroupBy.agg() raises RuntimeError unconditionally.
# Coverage: python/beava/_agg.py:539-556
#
# This is the showstopper. The ENTIRE Python aggregation surface is dead in
# v0 because Plan 12.7-06 stripped the table-derivation backing. The raise
# explicitly cites project_v0_events_only_scope, but that memory only drops
# TABLES — not stateful event-aggregation. There is a thinking-gap between
# "v0 is events-only" and "Python users cannot register a single windowed
# feature." Event-output aggregation IS what fraud-team.json exercises (see
# its derivation node `TxnByUser` with `group_by` + `agg` ops).

_gb = Transaction.group_by("user_id")
assert isinstance(_gb, GroupBy)  # ✅ WORKS — builder constructs

# 🚧 BLOCKER GAP: GroupBy.agg() raises RuntimeError unconditionally.
# Workaround: NONE — even raw AggDescriptor objects have no sink.
# Fix scope: ~40 LOC — restore GroupBy.agg() to wrap descriptors in an
#            EventDerivation that emits the `agg` op shape on the wire.
#
# What it SHOULD look like (mirrors fraud-team.json::TxnByUser shape):
#     TxnByUser = (
#         Transaction
#         .group_by("user_id")
#         .agg(
#             cnt_5m=bv.count(window="5m"),
#             cnt_1h=bv.count(window="1h"),
#             sum_amt_24h=bv.sum("amount", window="24h"),
#             decline_rate_1h=bv.ratio(where=bv.col("declined") == 1, window="1h"),
#         )
#         .named("TxnByUser")
#     )
#     app.register(Transaction, TxnByUser)
#
# Today this raises immediately on .agg(). Left commented so file imports clean:
# _ = _gb.agg(cnt_5m=bv.count(window="5m"))  # raises RuntimeError


# ─── Section 8: Windowed aggregations ───────────────────────────────────────
#
# Status: ✅ WORKS at descriptor level (window=str kwarg)
#         🚧 BLOCKED downstream (Section 7)
# Coverage: window="5m" kwarg threading through helpers
#
# The SDK uses ``window=`` consistently. There is NO ``windowed=`` kwarg —
# the audit prompt asked which one is used; the answer is ``window=``. See
# python/beava/_agg.py::count, sum, avg, etc. all taking ``window: str``.

# ✅ WORKS — descriptor carries window correctly
_w_count = bv.count(window="5m")
_w_sum = bv.sum("amount", window="1h")
_w_p99 = AggDescriptor(op="percentile", field="amount", window="24h")  # workaround

assert _w_count.window == "5m"
# Wire-shape sanity:
assert _w_sum.to_agg_spec()["params"]["window"] == "1h"

# Validation (✅ all WORK): regex \d+(ms|s|m|h|d)|forever; leading-0 rejected;
# sub-second OK; required for sum/avg/min/max/variance/stddev; optional for
# count/ratio (omit = lifetime).
for bad in ("5seconds", "0ms"):
    try:
        bv.sum("amount", window=bad); raise AssertionError("expected ValueError")
    except ValueError:
        pass


# ─── Section 9: Where-clause aggregations ───────────────────────────────────
#
# Status: ✅ WORKS at descriptor level
#         🚧 BLOCKED downstream (Section 7)
# Coverage: where= kwarg accepts a bv.col(...) AST
#
# The where= predicate is serialized via _serialize_where() at descriptor
# construction time. Anything with .to_expr_string() works (bv.col duck-type).

# ✅ WORKS — predicate threading
_decline_count = bv.count(where=bv.col("declined") == 1, window="1h")
_high_value_sum = bv.sum("amount", where=bv.col("amount") > 1000.0, window="24h")
_decline_rate = bv.ratio(where=bv.col("declined") == 1, window="1h")

# Round-trip the predicate to verify escape rules survived:
assert _decline_count.where == "(declined == 1)"


# ─── Section 10: Lifetime aggregations ──────────────────────────────────────
#
# Status: ✅ WORKS — omit window= for forever-window
#         🚧 BLOCKED downstream (Section 7)
# Coverage: AGG-CORE-09; Phase 12.8 lifetime memory contract
#
# Phase 12.8 closed the lifetime-bound contract: ops without a window are O(1)
# memory per entity (no event buffer). Python expression: omit `window=` on
# count/ratio (others REQUIRE a window).

# ✅ WORKS — count() / ratio() lifetime form
_life_count = bv.count()  # window=None == lifetime
_life_ratio = bv.ratio()
assert _life_count.window is None

# 🚧 GAP: lifetime-bound ops in Phase 8 (first_seen, last_seen, age, has_seen,
# time_since, streak, max_streak, negative_streak, first_seen_in_window if
# windowless) all lack helpers (Section 6).


# ─── Section 11: Cold-entity TTL ────────────────────────────────────────────
#
# Status: ✅ WORKS — cold_after kwarg lands on the wire
# Coverage: Phase 12.8 D-01; @bv.event(cold_after="...")
#
# Per-source opt-in. When an entity's last_seen is older than
# now - cold_after, all its state is cleared on the next event (Redis TTL
# pattern). Range [1s, 365d]; 'forever' is rejected explicitly.

# ✅ WORKS — cold_after parses + lands in cold_after_ms on the descriptor
@bv.event(cold_after="30d")
class TxnWithTTL:
    user_id: str
    amount: float

assert TxnWithTTL._cold_after_ms == 30 * 86_400_000

# Validation (✅ all WORK):
try:
    @bv.event(cold_after="forever")  # explicitly rejected
    class _BadForever:
        x: str
except TypeError:
    pass

try:
    @bv.event(cold_after="500ms")  # below 1s minimum
    class _BadTooShort:
        x: str
except TypeError:
    pass


# ─── Section 12: Register pipeline ──────────────────────────────────────────
#
# Status: ✅ WORKS for event sources + raw derivations (no agg)
#         🚧 INDIRECT GAP — see Section 7; aggregation derivations cannot be
#         constructed today, so register() can only land schema-only events.
# Coverage: App.register, validate_descriptors, topo_sort
#
# Pipeline: validate (zero I/O) → topo_sort → JSON-encode → transport.send_register

# ✅ WORKS — pure-validation path
errs = bv.App("http://localhost:7379").validate(Transaction, BigTxn)
# Note: this constructs the App but does NOT call the network — validate is
# local. errs is a list[ValidationError].
assert isinstance(errs, list)

if RUN_LIVE:
    # ✅ WORKS — register an event source + a function-form derivation
    with bv.App("tcp://localhost:7380") as app:
        try:
            response = app.register(Transaction, BigTxn, Login, Refund, CardAdd)
            print("registered, registry_version =", response.get("registry_version"))
        except RegistrationError as exc:
            print(f"register failed: code={exc.code} path={exc.path} msg={exc.message}")
        except ValidationError as exc:
            print("local validation failed:", exc)


# ─── Section 13: Push events ────────────────────────────────────────────────
#
# Status: 🚧 MAJOR GAP — App.push() does not exist; users reach into
#         App._transport. For the most-called SDK API on a "Redis for streaming
#         features" product, this is the most awkward, not the most polished.
# Coverage: python/beava/_app.py vs python/beava/_transport.py
#
# 🚧 GAP: bv.App has no .push() method.
# Workaround: app._transport.send_push(event_name, body_dict, wire_format=...)
#             — works on TcpTransport + EmbedTransport (NOT HttpTransport,
#             which has no send_push at all).
# Fix scope: ~30 LOC App.push() dispatch + ~20 LOC HttpTransport.send_push.
#
# 🚧 GAP: No batch-push helper. Server has OP_PUSH_MANY (0x0012) reserved but
#         apply_shard.rs has no dispatch arm. HTTP /push-batch IS routed
#         (server.rs:1733). Fix scope: ~50 LOC SDK + finish server arm.
#
# 🚧 GAP: No async / fire-and-forget push (README promises "fire-and-forget
#         Python SDK"). Needs API design (asyncio? threadpool?). Defer v0.1+.
#
# 🚧 GAP: No /push-sync helper (acks=all). HttpPushSync routed (server.rs:1730);
#         OP_PUSH_SYNC=0x0011 returns op_not_implemented (apply_shard.rs:529).
#         Fix scope: ~20 LOC SDK once App.push() exists.


# ─── Section 14: Get features ───────────────────────────────────────────────
#
# Status: ✅ WORKS — single feature single key (App.get)
#         🚧 GAP — no batch-get / multi-key helper on App
# Coverage: App.get, transport.tcp_get_single, transport.http_get_single
#
# App.get() dispatches based on transport: msgpack-default on TCP/embed,
# JSON-only on HTTP per locked decision D-D. Both paths return the unwrapped
# value field (not the {"value": ...} envelope).

if RUN_LIVE:
    with bv.App("tcp://localhost:7380") as app:
        # ✅ WORKS — single feature, single key
        v = app.get("cnt_5m", "alice")
        print("cnt_5m for alice:", v)

# 🚧 GAP: No batch-get helper on App.
# Workaround: build the OP_MGET (0x0021) or OP_GET_MULTI (0x0022) frame
#             manually and call transport._ensure_connected().sendall(frame).
#             See crates/beava-server/src/server.rs::WireRequest::HttpGetMulti
#             for the HTTP route shape (POST /get-multi with JSON body).
# Fix scope: ~80 LOC — add App.get_many(features, keys) wrapping OP_GET_MULTI;
#            HttpTransport.http_get_multi for HTTP path; mirror the JSON body
#            shape the server's get_multi handler expects.


# ─── Section 15: Deregister ─────────────────────────────────────────────────
#
# Status: 🚧 MAJOR GAP — entirely missing (no SDK, no server opcode).
# Coverage: nothing exists
#
# Grep for OP_DEREGISTER, OP_UNREGISTER, deregister, unregister across
# crates/beava-core, crates/beava-server, python/beava — zero hits.
#
# 🚧 GAP: bv.App.deregister() does not exist; server has no opcode.
# Workaround: stop server, rm -rf .beava/wal .beava/snapshots, restart.
#             Surgical — destroys ALL state, not just one descriptor.
# Suggested API (post-v0):
#     app.deregister("Transaction")           # remove ONE event source
#     app.deregister("Transaction", "Login")  # remove several
#     app.deregister(all=True)                # nuke everything (with consent)
# Fix scope: ~300 LOC server (new opcode + WAL record + recovery skip) +
#            ~30 LOC SDK. Ship-without alternative: document rm-WAL escape
#            in operator runbook; v0 positioning ("size your box") allows it.


# ─── Section 16: Schema upgrade / version bump ──────────────────────────────
#
# Status: ✅ WORKS implicitly (re-call register)
#         🚧 MINOR GAP — no explicit upgrade API; no version pinning
# Coverage: App.register again with additive changes
#
# Upgrades today: re-call app.register() with new descriptors. Server compares
# against existing registry — additive-only changes (new sources, new
# derivations, new aggregations on existing sources) bump registry_version;
# breaking changes (removed field, type change) return a registration error.

if RUN_LIVE:
    with bv.App("tcp://localhost:7380") as app:
        v1 = app.register(Transaction, Login)
        v2 = app.register(Transaction, Login, Refund)  # additive, OK
        assert v2["registry_version"] > v1["registry_version"]

# 🚧 MINOR GAP: No explicit .upgrade() / .migrate() API. Re-call register()
#               works. Fix scope: ~5 LOC alias for user clarity.
# 🚧 MINOR GAP: No registry_version pinning on push (no expect_registry_version
#               kwarg). Fix scope: ~20 LOC + server rejection arm.
# ⏳ DEFERRED-V0.1+: No WAL forward-rewrite migration tool. v0 contract:
#                    "size your box, don't break the schema."


# ─── Section 17: Cleanup ────────────────────────────────────────────────────
#
# Status: ✅ WORKS — context manager + idempotent close()
# Coverage: App.__enter__, App.__exit__, App.close, App.__del__
#
# close() is idempotent. __del__ provides a safety net for forgotten closes
# but is not guaranteed to run (per Python language spec). Embed mode
# REQUIRES the context-manager pattern — calling register() before __enter__
# raises RuntimeError.

# ✅ WORKS — context manager
if RUN_LIVE:
    with bv.App("tcp://localhost:7380") as app:
        app.ping()
    # transport closed automatically on __exit__

# ✅ WORKS — manual close (explicit URL mode)
if RUN_LIVE:
    app = bv.App("tcp://localhost:7380")
    app.ping()
    app.close()
    app.close()  # idempotent — safe to call twice


# ─── GAP SUMMARY ────────────────────────────────────────────────────────────
#
# Severity legend:
#   BLOCKER — v0 ship is impossible without fixing
#   MAJOR   — v0 ships, but a top-3 user-visible feature is unreachable
#   MINOR   — v0 ships, polish item
#   DEFER   — explicitly out-of-scope per project memory; do nothing
#
#  #  SEV       GAP                                                LOCATION                          FIX SCOPE
# --- --------- -------------------------------------------------- --------------------------------- -----------------------------
#   1 BLOCKER   GroupBy.agg() raises RuntimeError                  python/beava/_agg.py:539-556      ~40 LOC restore EventDerivation wrap
#   2 MAJOR     Phase 8 ops have ZERO Python helpers (15 ops:      python/beava/_agg.py + __init__   ~150 LOC (15 helpers + __all__
#               first/last/streak/recency/lag)                                                       + AggDescriptor.n field)
#   3 MAJOR     Phase 10 sketches: ZERO Python helpers (5 ops:     python/beava/_agg.py + __init__   ~80 LOC (5 helpers +
#               count_distinct, percentile, top_k, bloom, entropy)                                   AggDescriptor.sketch_params)
#   4 MAJOR     Phase 11 buffer+geo: ZERO Python helpers (11 ops:  python/beava/_agg.py + __init__   ~200 LOC (11 helpers +
#               histogram, hist-of-{hour,dow}, seasonal_dev,                                         AggDescriptor.{buckets,
#               event_type_mix, most_recent_n, reservoir,                                            lat_field, lon_field,
#               geo_velocity/distance/spread, dist_from_home)                                        samples})
#   5 MAJOR     App.push() does not exist (users reach into        python/beava/_app.py              ~30 LOC + 20 LOC
#               App._transport)                                                                      HttpTransport.send_push
#   6 MAJOR     App.deregister() missing AND no server opcode      everywhere                        ~300 LOC server + 30 LOC SDK;
#                                                                                                    can DEFER w/ rm-WAL doc
#   7 MAJOR     No batch get on App (OP_MGET/OP_GET_MULTI server-  python/beava/_app.py              ~80 LOC (App.get_many +
#               side complete)                                                                       HttpTransport.get_multi)
#   8 MINOR     No batch push (OP_PUSH_MANY=0x0012 reserved        python/beava/_transport.py +      ~50 LOC SDK + finish
#               but server dispatch unimplemented)                 apply_shard.rs                    OP_PUSH_MANY server arm
#   9 MINOR     No /push-sync helper (HttpPushSync route exists;   python/beava/_app.py              ~20 LOC after #5
#               TCP returns op_not_implemented apply_shard.rs:529)
#  10 MINOR     No bv.lit(value) helper                            python/beava/_col.py              ~10 LOC
#  11 MINOR     No string functions in expr DSL                    python/beava/_col.py +            Needs API design;
#               (lower, upper, contains, starts_with, regex)       crates/beava-core/src/expr.rs     DEFER to v0.1+
#  12 MINOR     No datetime functions in expr DSL                  python/beava/_col.py +            Needs API design;
#               (hour_of, dow, epoch_ms)                           crates/beava-core/src/expr.rs     DEFER to v0.1+
#  13 MINOR     No async / fire-and-forget push (README promises)  python/beava/_app.py              Needs API design (asyncio?)
#  14 MINOR     No explicit schema upgrade API (re-call register)  python/beava/_app.py              ~5 LOC alias; cosmetic
#  15 MINOR     No registry_version pinning on push                python/beava/_app.py +            ~20 LOC + server rejection
#                                                                  apply_shard.rs                    arm
#  16 DEFER     No tables / table-aggregation                      everywhere                        project_v0_events_only_scope
#  17 DEFER     No bv.fork / playground                            everywhere                        same
#  18 DEFER     No session windows                                 everywhere                        same
#  19 DEFER     No event-time / watermarks / joins / PIT           everywhere                        project_redis_shaped_no_event_time_ever
#  20 DEFER     No schema migration tool (forward-rewrite WAL)     n/a                               "size your box" v0 contract
#
# Counts: BLOCKER=1 (#1), MAJOR=6 (#2-7), MINOR=8 (#8-15), DEFER=5 (#16-20). TOTAL=20.
# (Per-op gaps inside #2/#3/#4 expand to 31 individual server ops with no Python
# helper — see SERVER_AGG_OPS_MISSING_FROM_SDK list at the top of the file.)
#
# Recommended v0 launch slice (smallest patch, biggest blast-radius):
#   1. Fix #1 (BLOCKER) — restore GroupBy.agg() → ~40 LOC. UNBLOCKS EVERYTHING.
#   2. Fix #5 (App.push) — ~50 LOC. Most-used API; removes the awkward
#      "reach into _transport" pattern.
#   3. Fix #2 + #3 + #4 (op helpers) — ~430 LOC. Without these the README's
#      "40+ purpose-built aggregation primitives" is a marketing lie since
#      Python users see only 22.
#   4. Fix #7 (batch get) — ~80 LOC. Sub-millisecond batch-get is in the
#      core value prop ("batch lookup for sub-millisecond fraud-decisioning").
#
# Total v0 fix budget: ~600 LOC across python/beava/_agg.py + _app.py +
# small _transport.py addition. NONE of this touches the Rust hot path.
# Defer #6/#8/#9/#13/#14/#15 to v0.1; document rm-WAL in the operator runbook.
#

if __name__ == "__main__":
    # When run as a script, just print the headline finding.
    print("beava SDK showcase — see file body for the full audit.")
    print(
        f"Public namespace exports {len(_PUBLIC_NAMES)} names; "
        f"{len(SERVER_AGG_OPS_MISSING_FROM_SDK)} server AggKinds have NO Python helper."
    )
    print(
        "Top blocker: GroupBy.agg() raises RuntimeError unconditionally "
        "(python/beava/_agg.py:539-556). v0 launch is gated on this fix."
    )
