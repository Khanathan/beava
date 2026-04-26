"""Phase 24-04 Task 3: end-to-end watermark semantics from the Python SDK.

Runs against the session-scoped ``tally_server`` fixture; drives REGISTER
and PUSH through the SDK with `_event_time` on the event payload and
asserts on the server-side watermark / late-drop behaviour via the admin
HTTP endpoints (`/metrics`, `/debug/streams/:name`).

Covered behaviours:

  * test_event_time_populated_by_user_lands_in_correct_bucket
  * test_event_time_absent_uses_wall_clock
  * test_late_event_increments_counter
  * test_debug_streams_endpoint_shows_watermark
"""

from __future__ import annotations

import time
import urllib.request
import urllib.error
import uuid

import tally as tl


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def _http_get(http_port: int, path: str) -> tuple[int, str]:
    """Minimal HTTP GET against the admin server on 127.0.0.1."""
    url = f"http://127.0.0.1:{http_port}{path}"
    try:
        with urllib.request.urlopen(url, timeout=5) as resp:
            return resp.status, resp.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode("utf-8", errors="replace")


def _unique_name(prefix: str) -> str:
    """Stable, unique class / stream name per test — the session-scoped
    server is shared so names must not collide across tests."""
    return f"{prefix}{uuid.uuid4().hex[:8]}"


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_event_time_populated_by_user_lands_in_correct_bucket(app, tally_server):
    """Push 3 events OOO within 5s and verify all 3 count in a 1h window."""
    _host, _tcp, http_port = tally_server

    stream_name = _unique_name("WmClicks_")
    agg_name = _unique_name("WmClicksAgg_")

    # Build a stream + count_1h aggregation programmatically via the v0 DSL.
    stream_cls = type(
        stream_name,
        (),
        {"__annotations__": {"user_id": str}},
    )
    stream_cls = tl.stream(stream_cls)

    # Table-with-aggregation via the function-form.
    def _agg(s: stream_cls) -> tl.Table:
        return s.group_by("user_id").agg(clicks_1h=tl.count(window="1h"))

    _agg.__name__ = agg_name
    Agg = tl.table(key="user_id")(_agg)

    app.register(stream_cls, Agg)

    # Pick an event-time in the live window. The server will use this
    # timestamp for bucket routing and watermark tracking.
    t0 = int(time.time())
    user = _unique_name("wm_ooo_")
    # Push three events OOO within 5s.
    app.push_sync(stream_cls, {"user_id": user, "_event_time": t0})
    app.push_sync(stream_cls, {"user_id": user, "_event_time": t0 - 2})
    app.push_sync(stream_cls, {"user_id": user, "_event_time": t0 - 4})
    app.flush()

    # All three fall inside the 1h window and within 5s of t0 (so none
    # are past-watermark). Expect clicks_1h == 3.
    row = app.get(user).to_dict()
    assert row.get("clicks_1h") == 3, f"expected clicks_1h=3, got {row!r}"


def test_event_time_absent_uses_wall_clock(app, tally_server):
    """Events without `_event_time` flow normally and never trigger late-drop."""
    _host, _tcp, http_port = tally_server

    stream_name = _unique_name("WmNoEt_")
    agg_name = _unique_name("WmNoEtAgg_")

    stream_cls = type(
        stream_name,
        (),
        {"__annotations__": {"user_id": str}},
    )
    stream_cls = tl.stream(stream_cls)

    def _agg(s: stream_cls) -> tl.Table:
        return s.group_by("user_id").agg(n=tl.count(window="1h"))

    _agg.__name__ = agg_name
    Agg = tl.table(key="user_id")(_agg)

    app.register(stream_cls, Agg)

    user = _unique_name("wm_no_et_")
    for _ in range(3):
        app.push_sync(stream_cls, {"user_id": user})
    app.flush()

    row = app.get(user).to_dict()
    assert row.get("n") == 3, f"expected n=3 when `_event_time` absent, got {row!r}"

    # And the /debug/streams endpoint surfaces zero drops for this stream.
    status, body = _http_get(http_port, f"/debug/streams/{stream_name}")
    assert status == 200, f"expected 200 for /debug/streams/{stream_name}, got {status}"
    assert '"late_events_dropped":0' in body.replace(" ", ""), body


def test_late_event_increments_counter(app, tally_server):
    """Push t=now then t=now-10s (> 5s late) → late-drop counter bumps."""
    _host, _tcp, http_port = tally_server

    stream_name = _unique_name("WmLate_")

    # Keyless stream works for this test — we only care about the
    # stream-level watermark and late-drop counter, not aggregation output.
    stream_cls = type(
        stream_name,
        (),
        {"__annotations__": {"user_id": str}},
    )
    stream_cls = tl.stream(stream_cls)

    # Need an aggregation on it so REGISTER keeps the stream alive as a
    # source. Minimal agg to anchor the stream.
    agg_name = _unique_name("WmLateAgg_")

    def _agg(s: stream_cls) -> tl.Table:
        return s.group_by("user_id").agg(n=tl.count(window="1h"))

    _agg.__name__ = agg_name
    Agg = tl.table(key="user_id")(_agg)
    app.register(stream_cls, Agg)

    t0 = int(time.time())
    user = _unique_name("wm_late_")
    # Seed watermark at t0 → wm = t0 − 5.
    app.push_sync(stream_cls, {"user_id": user, "_event_time": t0})
    # Push at t0 − 10 (10s past) → event_time < watermark → dropped.
    app.push_sync(stream_cls, {"user_id": user, "_event_time": t0 - 10})
    app.flush()

    # Check /metrics for the counter.
    status, body = _http_get(http_port, "/metrics")
    assert status == 200, f"metrics HTTP status: {status}"
    needle = f'tally_late_events_dropped_total{{stream="{stream_name}"}} 1'
    assert needle in body, (
        f"expected `{needle}` in /metrics body; got:\n"
        + "\n".join(
            line for line in body.splitlines() if "late_events_dropped" in line
        )
    )


def test_debug_streams_endpoint_shows_watermark(app, tally_server):
    """GET /debug/streams/:name returns the per-stream watermark JSON."""
    _host, _tcp, http_port = tally_server

    stream_name = _unique_name("WmDbg_")
    agg_name = _unique_name("WmDbgAgg_")

    stream_cls = type(
        stream_name,
        (),
        {"__annotations__": {"user_id": str}},
    )
    stream_cls = tl.stream(stream_cls)

    def _agg(s: stream_cls) -> tl.Table:
        return s.group_by("user_id").agg(n=tl.count(window="1h"))

    _agg.__name__ = agg_name
    Agg = tl.table(key="user_id")(_agg)
    app.register(stream_cls, Agg)

    t0 = int(time.time())
    user = _unique_name("wm_dbg_")
    app.push_sync(stream_cls, {"user_id": user, "_event_time": t0})
    app.flush()

    status, body = _http_get(http_port, f"/debug/streams/{stream_name}")
    assert status == 200, f"expected 200, got {status}: {body}"
    import json
    data = json.loads(body)
    assert data["name"] == stream_name, data
    # watermark_ms = (t0 − 5) * 1000
    expected_wm_ms = (t0 - 5) * 1000
    assert data["watermark_ms"] == expected_wm_ms, data
    assert data["observed_max_ms"] == t0 * 1000, data
    assert data["lateness_seconds"] == 5, data
    assert data["late_events_dropped"] == 0, data

    # 404 for unknown stream.
    status_404, body_404 = _http_get(http_port, "/debug/streams/definitely-does-not-exist")
    assert status_404 == 404, f"expected 404 for unknown stream, got {status_404}: {body_404}"
