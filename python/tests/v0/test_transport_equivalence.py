"""Cross-transport equivalence smoke (Phase 13.5.1 D-05 + D-02 + D-03).

These tests parameterise across the three v0 client-transport surfaces
(``embed`` / ``http`` / ``tcp``) and assert that a canonical fraud-team-shaped
flow produces identical results regardless of transport.

Coverage matrix: 7 tests × 3 transports = 21 parameterised cases.

D-03 features-filter coverage (USER-LOCKED, Plan 13.5.1-CONTEXT.md):
  - Test 4  (`test_get_features_filter_equivalent`)        — App.get(features=[...])
  - Test 4b (`test_batch_get_features_filter_equivalent`)  — App.batch_get per-entry
                                                              features=[...] (heterogeneous)

These two tests jointly catch the Plan 13.5.1-05 sibling blocker — features
plumbing on EITHER App.get OR App.batch_get alone is insufficient.

Anti-pattern guard (D-05, USER-LOCKED): NO mock-object references against
the Transport surface. The 0/68 acceptance-test deficit at Phase 13.5 Plan 11
close was masked precisely because internal tests used mock transports —
every test here hits a real engine via ``bv.App(test_mode=True)`` (embed)
or ``bv.App(url=...)`` against a ``spawn_embedded_server(test_mode=True)``
subprocess (http/tcp).

This file follows the canonical relative-import pattern every other v0 test
file uses (relative ``_helpers`` import — see line below); it does NOT
import from the absolute ``python.tests.v0.conftest`` path (no
``__init__.py`` in ``python/`` — that path is not a valid module).

RED state at HEAD (this commit):
  - HttpTransport.send_push/get/batch_get/reset → NotImplementedError
  - TcpTransport.send_get/batch_get/reset → NotImplementedError
  - EmbedTransport.send_push/get/batch_get/reset → NotImplementedError
  - App.get does not yet forward features= to send_get (Plan 05 amendment)
  - App.batch_get does not yet accept per-entry features (Plan 05 amendment)

Plan 13.5.1-05's GREEN commit lights up all 21 cases.
"""
from __future__ import annotations

from typing import Any, Generator

import pytest

import beava as bv

from ._helpers import _engine_available, cold_start_equivalent

pytestmark = pytest.mark.skipif(
    not _engine_available(),
    reason="requires Phase 13.4 engine + Phase 13.5 SDK rewrite + Phase 13.5.1 transport-impl",
)


# ---------------------------------------------------------------------------
# Parameterised fixture: one App per transport variant per test invocation.
# ---------------------------------------------------------------------------


@pytest.fixture(params=["embed", "http", "tcp"])
def transport_app(request: pytest.FixtureRequest) -> Generator[Any, None, None]:
    """Yield an App configured for one of the three transports.

    embed → bv.App(test_mode=True) — spawns its own subprocess.
    http  → spawn an embed server, connect bv.App(url=http_url).
    tcp   → spawn an embed server, connect bv.App(url=tcp_url).

    For http/tcp the subprocess teardown is wrapped in try/finally so a
    failing test still releases the spawned binary.
    """
    if request.param == "embed":
        with bv.App(test_mode=True) as a:
            yield a
    else:
        from beava._embed import spawn_embedded_server, teardown_process

        proc, http_url, tcp_url, _env = spawn_embedded_server(test_mode=True)
        url = http_url if request.param == "http" else tcp_url
        try:
            with bv.App(url=url) as a:
                yield a
        finally:
            teardown_process(proc)


# ---------------------------------------------------------------------------
# Test 1: register equivalence
# ---------------------------------------------------------------------------


def test_register_equivalent_across_transports(transport_app: Any) -> None:
    """Same register payload yields ``status=ok`` + integer registry_version
    on all three transports (compare on shape/types, not exact integer)."""

    @bv.event
    class Txn:
        user_id: str
        amount: float

    @bv.table(key="user_id")
    def UserTxn(txn: Txn):
        return txn.group_by("user_id").agg(c=bv.count(window="forever"))

    result = transport_app.register(Txn, UserTxn)
    assert result["status"] == "ok"
    assert isinstance(result.get("registry_version"), int)


# ---------------------------------------------------------------------------
# Test 2: push → get equivalence
# ---------------------------------------------------------------------------


def test_push_then_get_equivalent(transport_app: Any) -> None:
    """Push 10 events for one key, get the row, expect ``{"c": 10}`` on every transport."""

    @bv.event
    class Txn:
        user_id: str
        amount: float

    @bv.table(key="user_id")
    def UserTxn(txn: Txn):
        return txn.group_by("user_id").agg(c=bv.count(window="forever"))

    transport_app.register(Txn, UserTxn)
    for _ in range(10):
        transport_app.push("Txn", {"user_id": "alice", "amount": 1.0})

    assert transport_app.get("UserTxn", "alice") == {"c": 10}


# ---------------------------------------------------------------------------
# Test 3: batch_get equivalence (tuple-of-2 shape, no features filter)
# ---------------------------------------------------------------------------


def test_batch_get_equivalent(transport_app: Any) -> None:
    """Heterogeneous-key batch_get returns identical rows on every transport."""

    @bv.event
    class Txn:
        user_id: str
        amount: float

    @bv.table(key="user_id")
    def UserTxn(txn: Txn):
        return txn.group_by("user_id").agg(c=bv.count(window="forever"))

    transport_app.register(Txn, UserTxn)
    for user in ("alice", "bob", "carol"):
        for _ in range(5):
            transport_app.push("Txn", {"user_id": user, "amount": 1.0})

    results = transport_app.batch_get(
        [("UserTxn", "alice"), ("UserTxn", "bob"), ("UserTxn", "carol")]
    )
    assert results == [{"c": 5}, {"c": 5}, {"c": 5}]


# ---------------------------------------------------------------------------
# Test 4: D-03 PART 1 — App.get features filter
# ---------------------------------------------------------------------------


def test_get_features_filter_equivalent(transport_app: Any) -> None:
    """D-03 features filter on App.get narrows the response to listed keys.

    Plan 13.5.1-05 plumbs ``features=[...]`` through App.get → send_get →
    HTTP body / TCP frame body / embed delegate. Pre-Plan-05 (HEAD),
    App.get does not yet accept ``features=`` so this test fails with
    TypeError or NotImplementedError.
    """

    @bv.event
    class Txn:
        user_id: str
        amount: float

    @bv.table(key="user_id")
    def UserTxn(txn: Txn):
        return txn.group_by("user_id").agg(
            c=bv.count(window="forever"),
            s=bv.sum("amount", window="forever"),
        )

    transport_app.register(Txn, UserTxn)
    for _ in range(4):
        transport_app.push("Txn", {"user_id": "alice", "amount": 2.5})

    # Full row baseline — both columns present.
    full = transport_app.get("UserTxn", "alice")
    assert set(full.keys()) == {"c", "s"}, f"baseline must return full row; got {full!r}"
    assert full["c"] == 4
    assert abs(full["s"] - 10.0) < 1e-9

    # D-03: features=["c"] narrows the response to just {"c": ...}.
    narrowed = transport_app.get("UserTxn", "alice", features=["c"])
    assert set(narrowed.keys()) == {"c"}, (
        f"D-03 features filter must narrow App.get response to ['c'] only; "
        f"got {narrowed!r}"
    )
    assert narrowed["c"] == 4


# ---------------------------------------------------------------------------
# Test 4b: D-03 PART 2 — App.batch_get per-entry features filter
# ---------------------------------------------------------------------------


def test_batch_get_features_filter_equivalent(transport_app: Any) -> None:
    """D-03 per-entry features filter on App.batch_get across HTTP/TCP/Embed.

    Wire-spec lockdown (docs/wire-spec.md:54 + docs/http-api.md:317):
        OP_BATCH_GET body shape — {"requests":[{"table","key","features"?}, ...]}
    examples/wire/batch_get-heterogeneous.request.json shows a heterogeneous
    batch where one entry filters with ``features`` and another does not.

    We probe the dict-shape per-entry form first (matches the wire JSON
    Schema exactly). If Plan 13.5.1-05 ships the tuple-of-3 form instead,
    the dict path raises TypeError/ValueError; we fall back to the tuple-of-3
    path so EITHER Plan-05 design lands green here. At HEAD (pre-Plan-05),
    BOTH paths fail with NotImplementedError or TypeError — that's the RED
    state this test enforces.
    """

    @bv.event
    class Txn:
        user_id: str
        amount: float

    @bv.table(key="user_id")
    def UserTxn(txn: Txn):
        return txn.group_by("user_id").agg(
            c=bv.count(window="forever"),
            s=bv.sum("amount", window="forever"),
        )

    transport_app.register(Txn, UserTxn)
    for user in ("alice", "bob"):
        for _ in range(3):
            transport_app.push("Txn", {"user_id": user, "amount": 4.0})

    # Heterogeneous batch — matches examples/wire/batch_get-heterogeneous.request.json:
    #   - First entry: full row for alice (NO features filter)
    #   - Second entry: narrowed to ["c"] for bob (WITH features filter)
    try:
        results = transport_app.batch_get(
            [
                {"table": "UserTxn", "key": "alice"},
                {"table": "UserTxn", "key": "bob", "features": ["c"]},
            ]
        )
    except (TypeError, ValueError):
        # Fallback path: Plan 05 may ship the tuple-of-3 form instead of dict-shape.
        results = transport_app.batch_get(
            [
                ("UserTxn", "alice"),
                ("UserTxn", "bob", ["c"]),
            ]
        )

    assert len(results) == 2
    # First entry: full row, both feature columns present.
    assert set(results[0].keys()) == {"c", "s"}, (
        f"entry 0 (no features filter) must return full row; got {results[0]!r}"
    )
    assert results[0]["c"] == 3
    # Second entry: narrowed to ["c"] only.
    assert set(results[1].keys()) == {"c"}, (
        f"D-03 per-entry features filter on batch_get must narrow entry 1 "
        f"to ['c'] only; got {results[1]!r}"
    )
    assert results[1]["c"] == 3


# ---------------------------------------------------------------------------
# Test 5: ADR-003 global aggregation equivalence (no key)
# ---------------------------------------------------------------------------


def test_global_aggregation_equivalent(transport_app: Any) -> None:
    """ADR-003 global form: bare ``@bv.table`` with no key= yields a single global row."""

    @bv.event
    class Txn:
        user_id: str
        amount: float

    @bv.table
    def GlobalTxn(txn: Txn):
        return txn.agg(c=bv.count(window="forever"))

    transport_app.register(Txn, GlobalTxn)
    for _ in range(7):
        transport_app.push("Txn", {"user_id": "alice", "amount": 1.0})

    # ADR-003 sentinel routing: get("TableName") with no key → global row.
    assert transport_app.get("GlobalTxn") == {"c": 7}


# ---------------------------------------------------------------------------
# Test 6: D-04 reset equivalence (test_mode-gated)
# ---------------------------------------------------------------------------


def test_reset_equivalent(transport_app: Any) -> None:
    """app.reset() wipes state on all three transports (test_mode=True only)."""
    from beava._errors import RegistrationError  # noqa: PLC0415

    @bv.event
    class Txn:
        user_id: str
        amount: float

    @bv.table(key="user_id")
    def UserTxn(txn: Txn):
        return txn.group_by("user_id").agg(c=bv.count(window="forever"))

    transport_app.register(Txn, UserTxn)
    for _ in range(5):
        transport_app.push("Txn", {"user_id": "alice", "amount": 1.0})
    assert transport_app.get("UserTxn", "alice") == {"c": 5}

    transport_app.reset()

    # After reset: state is wiped. The exact registry-state semantics differ
    # by transport (some wipe the registry too); we only assert the row is
    # cold-start-equivalent ({} or None per cold_start_equivalent), OR the
    # server returns ``unknown_table`` (registry was wiped — per Phase 13.4
    # OP_RESET D-03 USER-LOCKED, reset clears state + registry on the v0
    # default path; v0.1+ may add a state-only reset variant).
    try:
        post = transport_app.get("UserTxn", "alice")
        assert cold_start_equivalent(post), (
            f"after reset(), get must return cold-start ({{}} or None); got {post!r}"
        )
    except RegistrationError as exc:
        assert exc.code == "unknown_table", (
            f"after reset(), get must return cold-start OR raise unknown_table; "
            f"got RegistrationError(code={exc.code!r})"
        )
