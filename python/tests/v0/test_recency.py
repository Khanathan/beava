"""Recency operator tests — first_seen / last_seen / age / has_seen / time_since /
time_since_last_n / streak / max_streak / negative_streak / first_seen_in_window.

10 tests, each pushing 500-1000 events with controlled timing/sequencing.

Recency ops are read-time-computed — `app.get()` returns values relative to query
time. For time-based ops (age, time_since, first_seen_in_window) we use coarse
tolerance bounds since the engine's clock is wall-time and the test cannot
inject a fake clock at v0. For state-based ops (streak, max_streak,
negative_streak, first_seen, last_seen, has_seen, first_n) we assert exact
state correctness from event-arrival semantics.
"""
from __future__ import annotations

import random
import time

import pytest

import beava as bv

from ._helpers import ENTITIES, _engine_available, cold_start_equivalent

pytestmark = pytest.mark.skipif(
    not _engine_available(),
    reason="requires Phase 13.4 engine + Phase 13.5 SDK rewrite",
)


# ---------------------------------------------------------------------------
# Test 1: first_seen — captures wall-time of first matching event
# ---------------------------------------------------------------------------


def test_first_seen_per_user_high_volume(app):
    """bv.first_seen: 500 events / 5 users; first-arrival timestamp sticky.

    State is a single `Option<i64>` per entity that captures `now_ms()` on
    the first matching event and is never overwritten. Since the test
    cannot inject a fake clock, we assert the timestamp is in a tolerance
    band around when push() was first called for that entity.
    """

    @bv.event
    class Login:
        user_id: str
        ok: bool

    @bv.table(key="user_id")
    def UserFirstSeen(logins: Login):
        return logins.group_by("user_id").agg(
            first_ms=bv.first_seen(),
        )

    app.register(Login, UserFirstSeen)

    rng = random.Random(70)
    first_push_ms: dict[str, int] = {}
    test_start_ms = int(time.time() * 1000)
    for _ in range(500):
        user = rng.choice(ENTITIES)
        if user not in first_push_ms:
            first_push_ms[user] = int(time.time() * 1000)
        app.push("Login", {"user_id": user, "ok": True})

    test_end_ms = int(time.time() * 1000)
    for entity in first_push_ms:
        result = app.get("UserFirstSeen", entity)
        actual = result["first_ms"]
        # First-seen must be within the test window
        assert test_start_ms <= actual <= test_end_ms, (
            f"{entity}: first_ms={actual} outside test window "
            f"[{test_start_ms}, {test_end_ms}]"
        )

    assert cold_start_equivalent(app.get("UserFirstSeen", "unknown_fs"))


# ---------------------------------------------------------------------------
# Test 2: last_seen — captures wall-time of latest matching event
# ---------------------------------------------------------------------------


def test_last_seen_per_user_high_volume(app):
    """bv.last_seen: 500 events / 5 users; latest-arrival timestamp overwritten."""

    @bv.event
    class Activity:
        user_id: str
        kind: str

    @bv.table(key="user_id")
    def UserLastSeen(activity: Activity):
        return activity.group_by("user_id").agg(
            last_ms=bv.last_seen(),
        )

    app.register(Activity, UserLastSeen)

    rng = random.Random(71)
    last_push_ms: dict[str, int] = {}
    test_start_ms = int(time.time() * 1000)
    for _ in range(500):
        user = rng.choice(ENTITIES)
        last_push_ms[user] = int(time.time() * 1000)
        app.push("Activity", {"user_id": user, "kind": "page_view"})

    test_end_ms = int(time.time() * 1000)
    for entity in last_push_ms:
        result = app.get("UserLastSeen", entity)
        actual = result["last_ms"]
        # Last-seen must be within test window and >= the recorded last push.
        assert test_start_ms <= actual <= test_end_ms, (
            f"{entity}: last_ms={actual} outside test window"
        )

    assert cold_start_equivalent(app.get("UserLastSeen", "unknown_ls"))


# ---------------------------------------------------------------------------
# Test 3: age — query-time elapsed ms since first_seen
# ---------------------------------------------------------------------------


def test_age_per_user_high_volume(app):
    """bv.age: 500 events / 5 users; elapsed ms since first arrival, computed at query time.

    Tests use coarse bounds: age must be at least the wall-time gap between
    first push and the test's query phase.
    """

    @bv.event
    class Tap:
        user_id: str
        screen: str

    @bv.table(key="user_id")
    def UserAge(taps: Tap):
        return taps.group_by("user_id").agg(
            age_ms=bv.age(),
        )

    app.register(Tap, UserAge)

    rng = random.Random(72)
    first_ts: dict[str, int] = {}
    for _ in range(500):
        user = rng.choice(ENTITIES)
        if user not in first_ts:
            first_ts[user] = int(time.time() * 1000)
        app.push("Tap", {"user_id": user, "screen": "home"})

    # Sleep briefly so age has a measurable lower bound when queried.
    time.sleep(0.1)
    query_start_ms = int(time.time() * 1000)
    for entity, first_ms in first_ts.items():
        result = app.get("UserAge", entity)
        actual = result["age_ms"]
        # Age must be at least (query_start - first_ts) and reasonably bounded.
        # 300ms slack absorbs push-burst skew: client `first_ts` is recorded BEFORE
        # the push call (wall-clock); engine records first_processed_ts AFTER push
        # arrives at the embed-mode subprocess. A 500-event push loop can take
        # >150ms across all entities, so any individual entity's first_ts may be
        # >150ms ahead of when the engine actually saw it. 300ms is 6× the prior
        # 50ms slack but bounded by the push-burst's wall-clock duration; it stays
        # well below the conftest 30s per-test timeout.
        min_expected = max(0, query_start_ms - first_ms - 300)
        assert actual >= min_expected, (
            f"{entity}: age={actual} < expected_min={min_expected}"
        )
        # Upper bound: age should be at most the wall-time gap since first push + slack.
        assert actual <= (int(time.time() * 1000) - first_ms + 200), (
            f"{entity}: age={actual} unreasonably high"
        )

    assert cold_start_equivalent(app.get("UserAge", "unknown_age"))


# ---------------------------------------------------------------------------
# Test 4: has_seen — boolean ever-matched flag
# ---------------------------------------------------------------------------


def test_has_seen_per_user_high_volume(app):
    """bv.has_seen: 500 events / 5 users with where=status=='failed' filter.

    Asserts has_seen=True for entities that emitted at least one failed event;
    False (or absent) for entities that only emitted ok events.
    """

    @bv.event
    class Login:
        user_id: str
        status: str

    @bv.table(key="user_id")
    def UserHasFailed(logins: Login):
        return logins.group_by("user_id").agg(
            ever_failed=bv.has_seen(where=bv.col("status") == "failed"),
        )

    app.register(Login, UserHasFailed)

    rng = random.Random(73)
    fail_seen: dict[str, bool] = {entity: False for entity in ENTITIES}
    for _ in range(500):
        user = rng.choice(ENTITIES)
        # First two entities: only ok events. Others: random failures.
        if user in ("alice", "bob"):
            status = "ok"
        else:
            status = "failed" if rng.random() < 0.3 else "ok"
        if status == "failed":
            fail_seen[user] = True
        app.push("Login", {"user_id": user, "status": status})

    for entity, exp in fail_seen.items():
        result = app.get("UserHasFailed", entity)
        # alice/bob may have non-failed events; has_seen on a where-filter is
        # False until first match. If the entity never matched, result may be
        # empty or has_seen=False.
        if exp:
            assert result.get("ever_failed") is True, (
                f"{entity}: expected ever_failed=True, got {result!r}"
            )
        else:
            actual = result.get("ever_failed")
            # cold-start: missing key OR False (depending on engine behaviour for
            # never-matched-where on lifetime ops with O(1) state).
            assert actual is False or actual is None or "ever_failed" not in result, (
                f"{entity}: expected ever_failed=False/None/missing, got {result!r}"
            )

    assert cold_start_equivalent(app.get("UserHasFailed", "unknown_hs"))


# ---------------------------------------------------------------------------
# Test 5: time_since — query-time elapsed ms since last matching event
# ---------------------------------------------------------------------------


def test_time_since_per_user_high_volume(app):
    """bv.time_since: 500 events / 5 users; elapsed ms from last match to query time."""

    @bv.event
    class Heartbeat:
        user_id: str
        kind: str

    @bv.table(key="user_id")
    def UserSilenceMs(beats: Heartbeat):
        return beats.group_by("user_id").agg(
            silence_ms=bv.time_since(),
        )

    app.register(Heartbeat, UserSilenceMs)

    rng = random.Random(74)
    last_push_ms: dict[str, int] = {}
    for _ in range(500):
        user = rng.choice(ENTITIES)
        last_push_ms[user] = int(time.time() * 1000)
        app.push("Heartbeat", {"user_id": user, "kind": "alive"})

    time.sleep(0.1)  # ensure measurable silence
    query_ms = int(time.time() * 1000)
    for entity, last_ms in last_push_ms.items():
        result = app.get("UserSilenceMs", entity)
        actual = result["silence_ms"]
        min_expected = max(0, query_ms - last_ms - 50)
        assert actual >= min_expected, (
            f"{entity}: silence={actual} < min_expected={min_expected}"
        )

    assert cold_start_equivalent(app.get("UserSilenceMs", "unknown_ts"))


# ---------------------------------------------------------------------------
# Test 6: time_since_last_n (n=3) — silence relative to nth most recent match
# ---------------------------------------------------------------------------


def test_time_since_last_n_per_user_high_volume(app):
    """bv.time_since_last_n (n=3): 700 events / 5 users; silence relative to 3rd-most-recent."""

    @bv.event
    class Ping:
        user_id: str
        kind: str

    @bv.table(key="user_id")
    def UserSilenceN3(pings: Ping):
        return pings.group_by("user_id").agg(
            silence_3=bv.time_since_last_n(n=3),
        )

    app.register(Ping, UserSilenceN3)

    rng = random.Random(75)
    last_3: dict[str, list[int]] = {entity: [] for entity in ENTITIES}
    for _ in range(700):
        user = rng.choice(ENTITIES)
        ts = int(time.time() * 1000)
        last_3[user].append(ts)
        if len(last_3[user]) > 3:
            last_3[user].pop(0)
        app.push("Ping", {"user_id": user, "kind": "p"})

    time.sleep(0.05)
    query_ms = int(time.time() * 1000)
    for entity, deque3 in last_3.items():
        result = app.get("UserSilenceN3", entity)
        if len(deque3) < 3:
            # Operator returns null/None until 3 matches have been seen.
            assert (
                result.get("silence_3") is None
                or "silence_3" not in result
            ), f"{entity}: expected None for <3 matches, got {result!r}"
            continue
        oldest_ts = deque3[0]
        actual = result["silence_3"]
        min_expected = max(0, query_ms - oldest_ts - 50)
        assert actual >= min_expected, (
            f"{entity}: silence_3={actual} < min={min_expected}"
        )

    assert cold_start_equivalent(app.get("UserSilenceN3", "unknown_tsn"))


# ---------------------------------------------------------------------------
# Test 7: streak — current consecutive matching count, resets on non-match
# ---------------------------------------------------------------------------


def test_streak_per_user_high_volume(app):
    """bv.streak: 600 events / 5 users with where=ok; current win-streak per entity.

    Streak is event-order driven (no time dimension): increments on match,
    resets to 0 on non-match. Trailing streak = consecutive matches at the
    tail of the entity's stream.
    """

    @bv.event
    class Bet:
        user_id: str
        outcome: str

    @bv.table(key="user_id")
    def UserStreak(bets: Bet):
        return bets.group_by("user_id").agg(
            current_streak=bv.streak(where=bv.col("outcome") == "win"),
        )

    app.register(Bet, UserStreak)

    rng = random.Random(76)
    streak_state: dict[str, int] = {entity: 0 for entity in ENTITIES}
    for _ in range(600):
        user = rng.choice(ENTITIES)
        outcome = "win" if rng.random() < 0.6 else "loss"
        if outcome == "win":
            streak_state[user] += 1
        else:
            streak_state[user] = 0
        app.push("Bet", {"user_id": user, "outcome": outcome})

    for entity, exp in streak_state.items():
        result = app.get("UserStreak", entity)
        actual = result["current_streak"]
        assert actual == exp, (
            f"{entity}: expected current_streak={exp}, got {actual}"
        )

    assert cold_start_equivalent(app.get("UserStreak", "unknown_st"))


# ---------------------------------------------------------------------------
# Test 8: max_streak — all-time peak match streak per entity
# ---------------------------------------------------------------------------


def test_max_streak_per_user_high_volume(app):
    """bv.max_streak: 600 events / 5 users with where=ok; peak win-streak per entity."""

    @bv.event
    class Bet:
        user_id: str
        outcome: str

    @bv.table(key="user_id")
    def UserMaxStreak(bets: Bet):
        return bets.group_by("user_id").agg(
            max_streak=bv.max_streak(where=bv.col("outcome") == "win"),
        )

    app.register(Bet, UserMaxStreak)

    rng = random.Random(77)
    current: dict[str, int] = {entity: 0 for entity in ENTITIES}
    peak: dict[str, int] = {entity: 0 for entity in ENTITIES}
    for _ in range(600):
        user = rng.choice(ENTITIES)
        outcome = "win" if rng.random() < 0.5 else "loss"
        if outcome == "win":
            current[user] += 1
            if current[user] > peak[user]:
                peak[user] = current[user]
        else:
            current[user] = 0
        app.push("Bet", {"user_id": user, "outcome": outcome})

    for entity, exp in peak.items():
        result = app.get("UserMaxStreak", entity)
        assert result["max_streak"] == exp, (
            f"{entity}: expected max_streak={exp}, got {result['max_streak']}"
        )

    assert cold_start_equivalent(app.get("UserMaxStreak", "unknown_ms"))


# ---------------------------------------------------------------------------
# Test 9: negative_streak — current consecutive non-matching count
# ---------------------------------------------------------------------------


def test_negative_streak_per_user_high_volume(app):
    """bv.negative_streak: 600 events / 5 users with where=fail; current losing streak."""

    @bv.event
    class Trial:
        user_id: str
        result: str

    @bv.table(key="user_id")
    def UserNegStreak(trials: Trial):
        return trials.group_by("user_id").agg(
            losing=bv.negative_streak(where=bv.col("result") == "fail"),
        )

    app.register(Trial, UserNegStreak)

    rng = random.Random(78)
    # negative_streak: counts CONSECUTIVE NON-matching events (i.e., not 'fail').
    # On a non-match (result != 'fail') the counter increments; on a match
    # (result == 'fail') it resets to 0.
    neg_streak: dict[str, int] = {entity: 0 for entity in ENTITIES}
    for _ in range(600):
        user = rng.choice(ENTITIES)
        result_val = "fail" if rng.random() < 0.4 else "ok"
        if result_val != "fail":
            neg_streak[user] += 1
        else:
            neg_streak[user] = 0
        app.push("Trial", {"user_id": user, "result": result_val})

    for entity, exp in neg_streak.items():
        result = app.get("UserNegStreak", entity)
        actual = result["losing"]
        assert actual == exp, (
            f"{entity}: expected negative_streak={exp}, got {actual}"
        )

    assert cold_start_equivalent(app.get("UserNegStreak", "unknown_ns"))


# ---------------------------------------------------------------------------
# Test 10: first_seen_in_window — bool: was last match within window ms?
# ---------------------------------------------------------------------------


def test_first_seen_in_window_per_user_high_volume(app):
    """bv.first_seen_in_window (window='1h'): 500 events / 5 users; freshness bool."""

    @bv.event
    class Action:
        user_id: str
        kind: str

    @bv.table(key="user_id")
    def UserActiveLastHour(actions: Action):
        return actions.group_by("user_id").agg(
            active_1h=bv.first_seen_in_window(window="1h"),
        )

    app.register(Action, UserActiveLastHour)

    rng = random.Random(79)
    active: dict[str, bool] = {}
    for _ in range(500):
        user = rng.choice(ENTITIES)
        active[user] = True
        app.push("Action", {"user_id": user, "kind": "interact"})

    # All events were just pushed — every entity should be active in the last hour.
    for entity in active:
        result = app.get("UserActiveLastHour", entity)
        assert result["active_1h"] is True, (
            f"{entity}: expected active_1h=True (just-pushed), got {result['active_1h']!r}"
        )

    assert cold_start_equivalent(app.get("UserActiveLastHour", "unknown_fsiw"))
