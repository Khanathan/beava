"""Phase 25-02 Task 1: Python SDK TTL decorator defaults + validation."""
from __future__ import annotations

import pytest
import tally as tl


# ----------------------------------------------------------------------------
# Decorator defaults: absent fields are NOT serialized — server applies them.
# Present fields ARE serialized and flow through REGISTER JSON.
# ----------------------------------------------------------------------------


def test_table_without_ttl_omits_field_from_register_json():
    @tl.table(key="user_id")
    class Users:
        user_id: str
        name: str

    payload = Users._to_register_json()
    # When absent, the server applies the 30d default — SDK must NOT emit.
    assert "entity_ttl" not in payload
    assert payload["kind"] == "table"


def test_table_with_explicit_ttl_serializes_field():
    @tl.table(key="user_id", ttl="60d")
    class Users:
        user_id: str
        name: str

    payload = Users._to_register_json()
    assert payload.get("entity_ttl") == "60d"


def test_stream_without_history_ttl_omits_field():
    @tl.stream
    class Clicks:
        user_id: str
        url: str

    payload = Clicks._to_register_json()
    assert "history_ttl" not in payload
    assert payload["kind"] == "stream"


def test_stream_with_explicit_history_ttl_serializes_field():
    @tl.stream(history_ttl="90d")
    class Logins:
        user_id: str

    payload = Logins._to_register_json()
    assert payload.get("history_ttl") == "90d"


# ----------------------------------------------------------------------------
# Invalid duration strings fail at decorator time with ValueError.
# ----------------------------------------------------------------------------


def test_table_rejects_invalid_ttl():
    with pytest.raises(ValueError):
        @tl.table(key="user_id", ttl="garbage")
        class Users:
            user_id: str


def test_table_rejects_empty_ttl():
    with pytest.raises(ValueError):
        @tl.table(key="user_id", ttl="")
        class Users:
            user_id: str


def test_table_rejects_non_string_ttl():
    with pytest.raises(ValueError):
        @tl.table(key="user_id", ttl=30)  # type: ignore[arg-type]
        class Users:
            user_id: str


def test_stream_rejects_invalid_history_ttl():
    with pytest.raises(ValueError):
        @tl.stream(history_ttl="banana")
        class Clicks:
            user_id: str


# ----------------------------------------------------------------------------
# Sentinels: "forever" and "0" are accepted.
# ----------------------------------------------------------------------------


def test_table_accepts_forever_sentinel():
    @tl.table(key="user_id", ttl="forever")
    class Users:
        user_id: str

    payload = Users._to_register_json()
    assert payload.get("entity_ttl") == "forever"


def test_table_accepts_zero_sentinel():
    @tl.table(key="user_id", ttl="0")
    class Users:
        user_id: str

    payload = Users._to_register_json()
    assert payload.get("entity_ttl") == "0"


def test_table_accepts_hour_and_minute_units():
    @tl.table(key="user_id", ttl="2h")
    class A:
        user_id: str

    @tl.table(key="user_id", ttl="15m")
    class B:
        user_id: str

    assert A._to_register_json()["entity_ttl"] == "2h"
    assert B._to_register_json()["entity_ttl"] == "15m"
